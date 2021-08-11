#!/usr/bin/env python

import os
import sys
import time
import json
import math
import prep_data

## Time
time_pt = time.strftime("%Y%m%d-%H%M%S")
time_ptr = time.strftime("%Y-%m-%d-%H:%M", time.strptime(time_pt,"%Y%m%d-%H%M%S"))

## Open JSON inputs
config = json.load(open(sys.argv[1]))
tol = config['tolerance']
max_job = config['job_number_max']
attribute = json.load(open(sys.argv[2]))
input_data = json.load(open(sys.argv[3]))
file_name = sys.argv[3].split('.')[0]

## FIA state codes
with open('./state_codes.csv', 'r') as cd:
    state_cd = prep_data.csv_dict(cd)

## Create a new directory and and remove log files
os.system(f"""
install -dvp ${{FIA}}/json/{file_name}-{time_pt}
install -dvp ${{PROJ_HOME}}/job-out-{file_name}/archive/{time_pt}
rm ${{FIA}}/job-{file_name}-*
rm ${{PROJ_HOME}}/jobid-{file_name}.log
mv ${{PROJ_HOME}}/job-out-{file_name}/*.* ${{PROJ_HOME}}/job-out-{file_name}/archive/{time_pt}
""")

## Calculating optimal batch size
nrow = len(input_data)
num_query = len(config['attribute_cd']) * len(config['year']) * nrow
batch_size = max(math.ceil(num_query / max_job), 1)

## FIA inventory years for each state
input_state = []
for r in input_data:
    input_state.append(r['state'])
    input_state.extend(r['neighbors'])

input_state = list(set(input_state))
if 'DC' in input_state: input_state.remove('DC')

st_invyr = {}
for st in input_state:
    invyr_id = os.popen(f"""
    if [ ! -f ./fia_data/survey/{st}_POP_STRATUM.csv ]; then
    wget -c -nv --tries=2 https://apps.fs.usda.gov/fia/datamart/CSV/{st}_POP_STRATUM.csv -P ./fia_data/survey
    fi
    awk -F , '{{print $4}}' ./fia_data/survey/{st}_POP_STRATUM.csv | grep ".*01$" | sort | uniq
    """).read().strip().split('\n')
    
    if len(invyr_id[0]) == 6:
        in_yr = [x[2:-2] for x in invyr_id]
    else:
        in_yr = [x[1:-2] for x in invyr_id]
    
    invyr = [f"20{x}" for x in in_yr if int(x) < int(time_pt[2:4])] + [f"19{x}" for x in in_yr if int(x) > int(time_pt[2:4])]
    st_invyr[state_cd[st]] = invyr

## Create batch files to download FIA JSON queries
query_list = []
for att_cd in config['attribute_cd']:
    att = attribute[str(att_cd)]
    for year in config['year']:
        for l in input_data:
            query_cd = [l['state_cd']] + l['neighbors_cd']
            if '11' in query_cd: query_cd.remove('11') # DC code is 11
            
            if tol == 0:
                if all(str(year) in st_invyr[x] for x in query_cd):
                    yr = year
                    cd_yr = [f"{x}{yr}" for x in query_cd]
                else:
                    print(f"Warning: Estimate not available for unit id {l['unit_id']} for year {year}.")
                    continue
            else:
                cd_yr = []
                yr = []
                for cd in query_cd:
                    diff = [abs(int(x) - year) for x in st_invyr[cd]]
                    indx = diff.index(min(diff))
                    yr_i = st_invyr[cd][indx]
                    cd_yr.append(f"{cd}{yr_i}")
                    yr.append(yr_i)
                yr = yr[0]
                
            query_dict = {'att_cd': att_cd, 'att': att, 'year': year, 'cd_yr': cd_yr, 'yr': yr, 'unit_id': l['unit_id'], 'lat': l['lat'], 'lon': l['lon'], 'radius': l['radius']}
            query_list.append(query_dict)

for i in range(max_job):
    select_qr = query_list[i * batch_size:(i + 1) * batch_size]
    if len(select_qr) > 0:
        print('*************', file_name, '- batch', i, '***************')
        batch = open(f"./fia_data/job-{file_name}-{i}.sh",'w')
        batch.write(f"""#!/bin/bash

#SBATCH --job-name={file_name}-{i}
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --partition={config['partition']}
#SBATCH --time={config['job_time_hr']}:00:00
#SBATCH --output=./job-out-{file_name}/{file_name}-{i}-%j.out
        """)
        
        rnum = 1
        for q in select_qr:
            file_path = f"${{FIA}}/json/{file_name}-{time_pt}/{q['att_cd']}-{q['year']}-id{q['unit_id']}-batch{i}"
            batch.write(f"""
echo "----------------------- {q['att_cd']}-{q['year']}-id{q['unit_id']} | {rnum} out of {len(select_qr)}"
if [ ! -f {file_path}-{q['yr']}.json ]; then
wget --tries=3 --timeout=180 --random-wait "https://apps.fs.usda.gov/Evalidator/rest/Evalidator/fullreport?reptype=Circle&lat={q['lat']}&lon={q['lon']}&radius={q['radius']}&snum={q['att']}&sdenom=No denominator - just produce estimates&wc={','.join(q['cd_yr'])}&pselected=None&rselected=All live stocking&cselected=None&ptime=Current&rtime=Current&ctime=Current&wf=&wnum=&wnumdenom=&FIAorRPA=FIADEF&outputFormat=JSON&estOnly=Y&schemaName=FS_FIADB." -O {file_path}-{q['yr']}.json
fi
            """)
            rnum += 1
        batch.close()
        
        ## Submit the batch file
        os.system(f"""
        if [ {max_job} -gt 1 ]; then
        JID=$(sbatch --parsable ${{FIA}}/job-{file_name}-{i}.sh)
        echo ${{JID}} >> ${{PROJ_HOME}}/jobid-{file_name}.log
        else . ${{FIA}}/job-{file_name}-{i}.sh > ${{PROJ_HOME}}/job-out-{file_name}/{file_name}-{i}.out
        fi
        """)

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
json_files = glob.glob('./fia_data/json/{file_name}-{time_pt}/*.json')
state_abb = prep_data.csv_dict(open('./state_abb.csv'))
att_coordinate = collections.defaultdict(dict)

for i in json_files:
    try:
        with open(i) as jf:
            js_data = json.load(jf)
    except json.decoder.JSONDecodeError:
        jf_in = open(i).read()
        if re.match("Estimate not available", jf_in):
            print(f"Warning: {{i}} estimate not available.")
        else:
            print(f"Warning: {{i}} is not a vaild JSON input.")
        continue

    n = os.path.basename(i)
    year = re.findall('(?<=-)\d{{4}}(?=-.)', i)[0]
    unit_id = re.findall('(?<=id)\d*(?=-)', i)[0]
    lat = js_data['EVALIDatorOutput']['circleLatitude']
    lon = js_data['EVALIDatorOutput']['circleLongitude']
    radius = js_data['EVALIDatorOutput']['circleRadiusMiles']
    att_cd = js_data['EVALIDatorOutput']['numeratorAttributeNumber']
    try:
        value = round(js_data['EVALIDatorOutput']['row'][0]['column'][0]['cellValueNumerator'])
    except (TypeError, KeyError):
        print(f"Warning: {{i}} does not have a vaild key or type data.")
        continue
    att_coordinate[unit_id].update({{'unit_id': unit_id, 'lat': lat, 'lon': lon, 'radius': radius, f"{{att_cd}}_{{year}}": value}})

## Sorting by keys
att_coordinate = {{k: att_coordinate[k] for k in sorted(att_coordinate.keys())}}

## JSON output
with open('./outputs/{file_name}-{time_pt}.json', 'w') as fj:
    json.dump(att_coordinate, fj)

## CSV output
list_coordinate = [x for x in att_coordinate.values()]
if len(list_coordinate) > 0:
    keys = ['unit_id','lat','lon','radius']
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
#SBATCH --output=./job-out-{file_name}/output-%j.out

python job-{file_name}.py config.json
""")
batch.close()

## Submit the batch file
os.system(f"""
sleep 2
if [ {max_job} -gt 1 ]; then
JOBID=$(cat ${{PROJ_HOME}}/jobid-{file_name}.log | tr '\n' ',' | grep -Po '.*(?=,$)')
JID=$(sbatch --parsable --dependency=afterok:$(echo ${{JOBID}}) ${{PROJ_HOME}}/job-{file_name}.sh)
echo ${{JID}} >> ${{PROJ_HOME}}/jobid-{file_name}.log
else . ${{PROJ_HOME}}/job-{file_name}.sh > ${{PROJ_HOME}}/job-out-{file_name}/output-serial.out
fi
sleep 2
""")

## Create a batch file to collect reports
report = open(f"./report-{file_name}.sh",'w')
report.write(f"""#!/bin/bash

#SBATCH --job-name=Report-{file_name}
#SBATCH --mem=4G
#SBATCH --partition={config['partition']}
#SBATCH --output=./report-{file_name}-%j.out

## Record the time that jobs are started
echo {time_ptr} > ./time_{file_name}

## Collect jobs with error
for i in `ls ${{PROJ_HOME}}/job-out-{file_name}/{file_name}-*.out`; do
    if grep -Piq "giving up|proxy error|failed" $i; then
        echo $i | grep -Po "(?<=job-out-{file_name}/).*(?=-.*.out$)" >> ${{PROJ_HOME}}/job-out-{file_name}/failed-temp.txt
    fi
done

if [ `jq ."job_number_max" config.json` -gt 1 ]; then
## Collecting failed and timeout Slurm jobs
sacct -XP --state F,TO --noheader --starttime $1 --format JobName | grep "{file_name}-" >> ${{PROJ_HOME}}/job-out-{file_name}/failed-temp.txt
fi

## Collect warnings
if [ ! -f ${{PROJ_HOME}}/job-out-{file_name}/output-*.out ]; then
. ${{PROJ_HOME}}/job-{file_name}.sh > ${{PROJ_HOME}}/job-out-{file_name}/output-msc.out
sleep 1
fi
grep -i "warning" ${{PROJ_HOME}}/job-out-{file_name}/output-*.out > ${{PROJ_HOME}}/job-out-{file_name}/warning.txt
sleep 1

## Collect jobs with warning
for w in $(cat ${{PROJ_HOME}}/job-out-{file_name}/warning.txt | grep input | grep -Po "(?<=batch).*(?=-)"); do
echo {file_name}-$w >> ${{PROJ_HOME}}/job-out-{file_name}/failed-temp.txt
done
sleep 1

if [ -f ${{PROJ_HOME}}/job-out-{file_name}/failed-temp.txt ]; then
sort ${{PROJ_HOME}}/job-out-{file_name}/failed-temp.txt | uniq > ${{PROJ_HOME}}/job-out-{file_name}/failed.txt
rm ${{PROJ_HOME}}/job-out-{file_name}/failed-temp.txt
else touch ${{PROJ_HOME}}/job-out-{file_name}/failed.txt
fi
sleep 1

echo "-----------------------------------------------------------------------------------
Job(s) start time: {time_ptr}
Number of queries: {num_query}
Number of jobs: `ls ${{PROJ_HOME}}/job-out-{file_name}/*.out | wc -l`
Number of warnings: `cat ${{PROJ_HOME}}/job-out-{file_name}/warning.txt | wc -l`
Number of failed jobs: `cat ${{PROJ_HOME}}/job-out-{file_name}/failed.txt | wc -l`

Find name of jobs with a warning or failure in:
    - ./job-out-{file_name}/warning.txt
    - ./job-out-{file_name}/failed.txt

Find the CSV and JSON outputs in (when jobs are done with no failure):
    - ./outputs/{file_name}-{time_pt}.csv
    - ./outputs/{file_name}-panel-{time_pt}.csv
    - ./outputs/{file_name}-{time_pt}.json

Failures can be related to:
    - FIADB issues (check FIA alerts at https://www.fia.fs.fed.us/tools-data/)
    - Download failure
    - Slurm jobs failure
    - Invalid configs (review config.json)
    - Invalid coordinates (review coordinate.csv)

If failures are related to FIADB servers, downloading JSON files or Slurm jobs, consider to run 'source rebatch_file.sh' to resubmit the failed jobs. Otherwise, modify config file and/or input files and resubmit the 'batch_file.sh'.
-----------------------------------------------------------------------------------"
""")
report.close()

## Submit the batch file
os.system(f"""
sleep 2
if [ {max_job} -gt 1 ]; then
JOBID=$(tail -qn 1 ${{PROJ_HOME}}/jobid-{file_name}.log)
sbatch --dependency=afterany:$(echo ${{JOBID}}) ${{PROJ_HOME}}/report-{file_name}.sh "{time_ptr}"
else . ${{PROJ_HOME}}/report-{file_name}.sh > ${{PROJ_HOME}}/report-{file_name}-serial.out
fi
""")
