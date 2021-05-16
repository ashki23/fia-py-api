#!/usr/bin/env python

import os
import sys
import time
import json

## Time
time_pt = time.strftime("%Y%m%d-%H%M%S")
time_ptr = time.strftime("%Y-%m-%d-%H:%M", time.strptime(time_pt,"%Y%m%d-%H%M%S"))

## Open JSON inputs
config = json.load(open(sys.argv[1]))
tol = config['tolerance']
maxj = config['job_number_max']
attribute = json.load(open(sys.argv[2]))
input_data = json.load(open(sys.argv[3]))
file_name = sys.argv[3].split('.')[0]

## Create a new directory and and remove log files
os.system(f"""
install -dvp ${{FIA}}/json/{file_name}_{time_pt}
install -dvp ${{PROJ_HOME}}/job_out_{file_name}/archive/{time_pt}
rm ${{FIA}}/job-{file_name}-*
rm ${{PROJ_HOME}}/jobid-{file_name}.log
rm ${{PROJ_HOME}}/serial-{file_name}-log.out
mv ${{PROJ_HOME}}/job_out_{file_name}/*.* ${{PROJ_HOME}}/job_out_{file_name}/archive/{time_pt}
""")

## Calculating optimal batch size
num_query = len(config['attribute_cd']) * len(config['year']) * len(input_data)
batch_size = max(round(num_query / maxj), 1)
if batch_size < len(input_data):
    batch_size += len(input_data) % batch_size + 1

## Create batch files to download FIA JSON queries
for att_cd in config['attribute_cd']:
    att = attribute[str(att_cd)]
    for year in config['year']:
        i = 0
        nrow = len(input_data)
        select_row = input_data[i:i + batch_size]
        while i < nrow:
            print('*************', year, att, 'row: ', i, '-', len(select_row), '***************')
            batch = open(f"./fia_data/job-{file_name}-{att_cd}-{year}-{i}.sh",'w')
            batch.write(f"""#!/bin/bash

#SBATCH --job-name={file_name}-{att_cd}-{year}-{i}
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --partition={config['partition']}
#SBATCH --time={config['job_time_hr']}:00:00
#SBATCH --output ./job_out_{file_name}/{file_name}-{att_cd}-{year}-{i}_%j.out
            """)
            lnum = 0
            for l in select_row:
                states_all = [l['state']] + l['neighbors']
                states_cd = [l['state_cd']] + l['neighbors_cd']
                
                st_invyr = {}
                for st in states_all:
                    invyr_id = os.popen(f"""
                    if [ ! -f ./fia_data/survey/{st}_POP_STRATUM.csv ]; then
                    wget -c -nv --tries=2 https://apps.fs.usda.gov/fia/datamart/CSV/{st}_POP_STRATUM.csv -P ./fia_data/survey
                    fi
                    awk -F , '{{print $4}}' ./fia_data/survey/{st}_POP_STRATUM.csv | grep ".*01$" | sort | uniq
                    """).read()[:-1].split('\n')
                    
                    if len(invyr_id[0]) == 6:
                        in_yr = [x[2:-2] for x in invyr_id]
                    else:
                        in_yr = [x[1:-2] for x in invyr_id]
                    
                    invyr = [f"20{x}" for x in in_yr if int(x) < int(time_pt[2:4])] + [f"19{x}" for x in in_yr if int(x) > int(time_pt[2:4])]
                    st_invyr[states_cd[states_all.index(st)]] = invyr
                    
                if tol == 0:
                    if str(year) in st_invyr[l['state_cd']]:
                        yr = year
                        cd_yr = [f"{x}{yr}" for x in states_cd]
                    else:
                        print(f"\n-------- Warning: Estimate not available for state {i} for year {year} --------\n")
                        continue
                else:
                    cd_yr = []
                    yr = []
                    for si in st_invyr.keys():
                        diff = [abs(int(x) - year) for x in st_invyr[si]]
                        indx = diff.index(min(diff))
                        yr_i = st_invyr[si][indx]
                        cd_yr.append(f"{si}{yr_i}")
                        yr.append(yr_i)
                    yr = yr[0]
                
                lnum += 1
                file_path = f"${{FIA}}/json/{file_name}_{time_pt}/{att_cd}_{year}_id{l['unit_id']}"
                batch.write(f"""
echo "----------------------- {file_name}-{att_cd}-{year}-{i} | {lnum} out of {len(select_row)}"
if [ ! -f {file_path}_{yr}.json ]; then
wget -c --tries=2 --random-wait "https://apps.fs.usda.gov/Evalidator/rest/Evalidator/fullreport?reptype=Circle&lat={l['lat']}&lon={l['lon']}&radius={l['radius']}&snum={att}&sdenom=No denominator - just produce estimates&wc={','.join(cd_yr)}&pselected=None&rselected=All live stocking&cselected=None&ptime=Current&rtime=Current&ctime=Current&wf=&wnum=&wnumdenom=&FIAorRPA=FIADEF&outputFormat=JSON&estOnly=Y&schemaName=FS_FIADB." -O {file_path}_{yr}.json
fi
                """)
            batch.close()
            
            ## Submit the batch file
            os.system(f"""
            if [ {maxj} -gt 1 ]; then
            JID=$(sbatch --parsable ${{FIA}}/job-{file_name}-{att_cd}-{year}-{i}.sh)
            echo ${{JID}} >> ${{PROJ_HOME}}/jobid-{file_name}.log
            else . ${{FIA}}/job-{file_name}-{att_cd}-{year}-{i}.sh > ${{PROJ_HOME}}/job_out_{file_name}/{file_name}-{att_cd}-{year}-{i}.out
            fi
            """)
            i += batch_size

## Scritp to extract information and generate outputs
job = open(f"./job-{file_name}.py",'w')
job.write(f"""#!/usr/bin/env python

import re
import os
import sys
import csv
import json
import glob
import prep_data
import collections

config = json.load(open(sys.argv[1]))
json_files = glob.glob('./fia_data/json/{file_name}_{time_pt}/*.json')
att_coordinate = collections.defaultdict(dict)

for i in json_files:
    try:
        with open(i) as jf:
            js_data = json.load(jf)
    except json.decoder.JSONDecodeError:
        print(f"Warning: {i} is not a vaild JSON input.")
        continue

    n = os.path.basename(i)
    year = re.findall('(?<=_)\d{{4}}(?=_.)', i)[0]
    unit_id = re.findall('(?<=id)\d*(?=_)', i)[0]
    lat = js_data['EVALIDatorOutput']['circleLatitude']
    lon = js_data['EVALIDatorOutput']['circleLongitude']
    radius = js_data['EVALIDatorOutput']['circleRadiusMiles']
    att_cd = js_data['EVALIDatorOutput']['numeratorAttributeNumber']
    state_inv = list(js_data['EVALIDatorOutput']['selectedInventories']['stateInventory'])[0].split()
    state = state_inv[0].capitalize()
    state_cd = state_inv[1][:-4]
    year_survey = state_inv[1][2:]
    try:
        value = round(js_data['EVALIDatorOutput']['row'][0]['column'][0]['cellValueNumerator'])
    except (TypeError, KeyError):
        print(f"Warning: {i} does not have a vaild key or type data.")
        continue
    att_coordinate[unit_id].update({{'unit_id': unit_id, 'state': state, 'state_cd': state_cd, 'lat': lat, 'lon': lon, 'radius': radius, f"{{att_cd}}_{{year}}": value}})

## JSON output
with open('./outputs/{file_name}-{time_pt}.json', 'w') as fj:
    json.dump(att_coordinate, fj)

## CSV output
list_coordinate = [x for x in att_coordinate.values()]
lk = len(config['attribute_cd']) * len(config['year'])
if len(list_coordinate) > 0:
    keys = list(list_coordinate[0].keys())[:-lk]
    with open('./outputs/{file_name}-panel-{time_pt}.csv', 'w') as fp:
        prep_data.list_dict_panel(list_coordinate,keys,config,fp)
    
    for x in config['attribute_cd']:
        keys.extend([f"{{x}}_{{y}}" for y in config['year']])
    
    with open('./outputs/{file_name}-{time_pt}.csv', 'w') as fc:
        prep_data.list_dict_csv(list_coordinate,keys,fc)
""")
job.close()

## Create a batch file to run the Python job
batch = open(f"./job-{file_name}.sh",'w')
batch.write(f"""#!/bin/bash

#SBATCH --job-name=Output-{file_name}
#SBATCH --mem=8G
#SBATCH --partition={config['partition']}
#SBATCH --output ./job_out_{file_name}/output_%j.out

python job-{file_name}.py config.json
""")
batch.close()

## Submit the batch file
os.system(f"""
sleep 3
if [ {maxj} -gt 1 ]; then
JOBID=$(cat ${{PROJ_HOME}}/jobid-{file_name}.log | tr '\n' ',' | grep -Po '.*(?=,$)')
JID=$(sbatch --parsable --dependency=afterok:$(echo ${{JOBID}}) ${{PROJ_HOME}}/job-{file_name}.sh)
echo ${{JID}} >> ${{PROJ_HOME}}/jobid-{file_name}.log
else . ${{PROJ_HOME}}/job-{file_name}.sh
fi
sleep 3
""")

## Create a batch file to collect reports
report = open(f"./report-{file_name}.sh",'w')
report.write(f"""#!/bin/bash

#SBATCH --job-name=Report-{file_name}
#SBATCH --mem=4G
#SBATCH --partition={config['partition']}
#SBATCH --output ./report-{file_name}-%j.out

## Collect jobs with error
for i in `ls ${{PROJ_HOME}}/job_out_{file_name}/{file_name}-*.out`; do
    if grep -iq "ERROR" $i ; then
        echo $i | grep -Po "(?<=job_out_{file_name}/).*(?=_.*.out$)" >> ${{PROJ_HOME}}/job_out_{file_name}/failed-temp.txt
    fi
done

if [ `jq ."job_number_max" config.json` -gt 1 ]; then
## Collecting failed and timeout Slurm jobs
sacct -XP --state F,TO --noheader --starttime {time_ptr} --format JobName | grep "{file_name}" >> ${{PROJ_HOME}}/job_out_{file_name}/failed-temp.txt
fi

sort ${{PROJ_HOME}}/job_out_{file_name}/failed-temp.txt | uniq > ${{PROJ_HOME}}/job_out_{file_name}/failed.txt
rm ${{PROJ_HOME}}/job_out_{file_name}/failed-temp.txt

## Collect warnings
grep -i "warning" ${{PROJ_HOME}}/job_out_{file_name}/output_*.out > > ${{PROJ_HOME}}/job_out_{file_name}/warning.txt

sleep 3

echo "-----------------------------------------------------------------------------------
Job(s) start time: {time_ptr}
Number of queries: {num_query}
Number of jobs: `ls ${{PROJ_HOME}}/job_out_{file_name}/*.out | wc -l`
Number of failed jobs: `cat ${{PROJ_HOME}}/job_out_{file_name}/failed.txt | wc -l`"
Number of warnings (./job_out_{file_name}/warning.txt): `cat ${{PROJ_HOME}}/job_out_{file_name}/warning.txt | wc -l`"

if [ `cat ${{PROJ_HOME}}/job_out_{file_name}/failed.txt | wc -l` -gt 0 ]; then
echo "
Find failed jobs' name in '${{PROJ_HOME}}/job_out_{file_name}/failed.txt'

Failure can be relared to:

      - EVALIDator and the FIADB may be unavailable during this time
      - Config file inputs may be unvaild
      - Input coordinates may be unvalid (for the 'coodinate' query type)
      - Slurm job failure

If the failure is related to EVALIDator servers or Slurm jobs, consider to run 'rebatch_file.sh' file to resubmit the failed jobs. Otherwise, modify config file and/or input files and resubmit the 'batch_file.sh'."
fi

echo -----------------------------------------------------------------------------------
""")
report.close()

## Submit the batch file
os.system(f"""
sleep 3
if [ {maxj} -gt 1 ]; then
JOBID=$(tail -n 1 ${{PROJ_HOME}}/jobid-{file_name}.log)
sbatch --parsable --dependency=afterok:$(echo ${{JOBID}}) ${{PROJ_HOME}}/report-{file_name}.sh
else . ${{PROJ_HOME}}/report-{file_name}.sh
fi
""")
