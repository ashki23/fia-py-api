#!/usr/bin/env python

import os
import sys
import csv
import time
import json
import prep_data

## Time
time_pt = time.strftime("%Y%m%d-%H%M%S")
time_ptr = time.strftime("%Y-%m-%d-%H:%M", time.strptime(time_pt,"%Y%m%d-%H%M%S"))

## Open JSON inputs
config = json.load(open(sys.argv[1]))
tol = config['tolerance']
maxj = int(config['job_number_max'])
attribute = json.load(open(sys.argv[2]))
num_query = len(config['attribute_cd']) * len(config['year']) * len(config['state'])

## Create a dictionary of FIA state codes
with open('./state_codes.csv', 'r') as cd:
    state_cd = prep_data.csv_dict(cd)

## Select states from the config
state_cd = prep_data.state_config(state_cd,config)

## Create a new directory and and remove log files
os.system(f"""
install -dvp ${{FIA}}/json/county_{time_pt}
install -dvp ${{PROJ_HOME}}/job_out_county/archive/{time_pt}
rm ${{FIA}}/job-county-*
rm ${{PROJ_HOME}}/jobid-county.log
rm ${{PROJ_HOME}}/serial-county-log.out
mv ${{PROJ_HOME}}/job_out_county/*.* ${{PROJ_HOME}}/job_out_county/archive/{time_pt}
""")

## Create batch files to download FIA JSON queries
for i in state_cd.keys():
    print('************* state:', i, '***************')
    batch = open(f"./fia_data/job-county-{i}.sh",'w')
    batch.write(f"""#!/bin/bash

#SBATCH --job-name=county-{i}
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --partition={config['partition']}
#SBATCH --time={config['job_time_hr']}:00:00
#SBATCH --output ./job_out_county/county-{i}_%j.out
    """)
    for year in config['year']:
        invyr_id = os.popen(f"""
        if [ ! -f ./fia_data/survey/{i}_POP_STRATUM.csv ]; then
        wget -c -nv --tries=2 https://apps.fs.usda.gov/fia/datamart/CSV/{i}_POP_STRATUM.csv -P ./fia_data/survey; fi
        awk -F , '{{print $4}}' ./fia_data/survey/{i}_POP_STRATUM.csv | grep ".*01$" | sort | uniq
        """).read()[:-1].split('\n')
        
        if len(invyr_id[0]) == 6:
            in_yr = [x[2:-2] for x in invyr_id]
        else:
            in_yr = [x[1:-2] for x in invyr_id]
        
        invyr = [f"20{x}" for x in in_yr if int(x) < int(time_pt[2:4])] + [f"19{x}" for x in in_yr if int(x) > int(time_pt[2:4])]
        
        if tol == 0:
            if str(year) in invyr:
                yr = year
                cd_yr = [f"{state_cd[i]}{yr}"]
            else:
                print(f"\n-------- Warning: Estimate not available for state {i} for year {year} --------\n")
                continue
        else:
            cd_yr = []
            diff = [abs(int(x) - year) for x in invyr]
            indx = diff.index(min(diff))
            yr = invyr[indx]
            cd_yr.append(f"{state_cd[i]}{yr}")
            
        for att_cd in config['attribute_cd']:
            att = attribute[str(att_cd)]
            
            file_path = f"${{FIA}}/json/county_{time_pt}/{att_cd}_{year}_{i}"
            batch.write(f"""
echo "----------------------- county-{i} | {year}-{att_cd}"
if [ ! -f {file_path}_{yr}.json ]; then
wget -c --tries=2 --random-wait "https://apps.fs.usda.gov/Evalidator/rest/Evalidator/fullreport?reptype=State&lat=0&lon=0&radius=0&snum={att}&sdenom=No denominator - just produce estimates&wc={','.join(cd_yr)}&pselected=None&rselected=County code and name&cselected=None&ptime=Current&rtime=Current&ctime=Current&wf=&wnum=&wnumdenom=&FIAorRPA=FIADEF&outputFormat=JSON&estOnly=Y&schemaName=FS_FIADB." -O {file_path}_{yr}.json
fi
            """)
    batch.close()
    
    ## Submit the batch file
    os.system(f"""
    if [ {maxj} -gt 1 ]; then
    JID=$(sbatch --parsable ${{FIA}}/job-county-{i}.sh)
    echo ${{JID}} >> ${{PROJ_HOME}}/jobid-county.log
    else . ${{FIA}}/job-county-{i}.sh > ${{PROJ_HOME}}/job_out_county/county-{i}.out
    fi
    """)

## Scritp to extract information and generate outputs
job = open('./job-county.py','w')
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
json_files = glob.glob('./fia_data/json/county_{time_pt}/*.json')

att_county = collections.defaultdict(dict)
att_state = collections.defaultdict(dict)

for i in json_files:
    try:
        with open(i) as jf:
            js_data = json.load(jf)
    except json.decoder.JSONDecodeError:
        print(f"Warning: {{i}} is not a vaild JSON input.")
        continue

    n = os.path.basename(i)
    year = re.findall('(?<=_)\d{{4}}(?=_.)', i)[0]
    state_abb = js_data['EVALIDatorOutput']['row'][1]['content'].split()[1].upper()
    att_cd = js_data['EVALIDatorOutput']['numeratorAttributeNumber']
    state_inv = js_data['EVALIDatorOutput']['selectedInventories']['stateInventory'].split()
    state = state_inv[0].capitalize()
    state_cd = state_inv[1][:-4]
    year_survey = state_inv[1][2:]
    
    for j in js_data['EVALIDatorOutput']['row']:
        content = j['content'].split()
        try:
            value = round(j['column'][0]['cellValueNumerator'])
        except (TypeError, KeyError):
            print(f"Warning: {{i}} does not have a vaild key or type data.")
            continue
        if content[0] == 'Total':
            att_state[state_abb].update({{'state': state_abb, f"{{att_cd}}_{{year}}": value}})
        else:
            county = content[2].capitalize()
            att_county[f"{{county}}_{{state_abb}}"].update({{'county_cd': content[0], 'county': county, 'state': state_abb, f"{{att_cd}}_{{year}}": value}})

## JSON output
with open('./outputs/county-{time_pt}.json', 'w') as fj:
    json.dump(att_county, fj)

with open('./outputs/state-{time_pt}.json', 'w') as fj:
    json.dump(att_state, fj)

## CSV output - county
list_county = [x for x in att_county.values()]
lk = len(config['attribute_cd']) * len(config['year'])
if len(list_county) > 0:
    county_keys = list(list_county[0].keys())[:-lk]
    with open('./outputs/county-panel-{time_pt}.csv', 'w') as fp:
        prep_data.list_dict_panel(list_county,county_keys,config,fp)

    for x in config['attribute_cd']:
        county_keys.extend([f"{{x}}_{{y}}" for y in config['year']])

    with open('./outputs/county-{time_pt}.csv', 'w') as fc:
        prep_data.list_dict_csv(list_county,county_keys,fc)

## CSV output - state
list_state = [x for x in att_state.values()]
if len(list_state) > 0:
    state_keys = list(list_state[0].keys())[:-lk]
    with open('./outputs/state-panel-{time_pt}.csv', 'w') as fp:
        prep_data.list_dict_panel(list_state,state_keys,config,fp)

    for x in config['attribute_cd']:
        state_keys.extend([f"{{x}}_{{y}}" for y in config['year']])

    with open('./outputs/state-{time_pt}.csv', 'w') as fc:
        prep_data.list_dict_csv(list_state,state_keys,fc)
""")
job.close()

## Create a batch file
batch = open('./job-county.sh','w')
batch.write(f"""#!/bin/bash

#SBATCH --job-name=Output-county
#SBATCH --mem=8G
#SBATCH --partition={config['partition']}
#SBATCH --output ./job_out_county/output_%j.out

python job-county.py config.json
""")
batch.close()

## Submit the batch file
os.system(f"""
sleep 3
if [ {maxj} -gt 1 ]; then
JOBID=$(cat ${{PROJ_HOME}}/jobid-county.log | tr '\n' ',' | grep -Po '.*(?=,$)')
JID=$(sbatch --parsable --dependency=afterok:$(echo ${{JOBID}}) ${{PROJ_HOME}}/job-county.sh)
echo ${{JID}} >> ${{PROJ_HOME}}/jobid-county.log
else . ${{PROJ_HOME}}/job-county.sh > ${{PROJ_HOME}}/job_out_county/output_serial.out
fi
sleep 3
""")

## Create a batch file to collect reports
report = open(f"./report-county.sh",'w')
report.write(f"""#!/bin/bash

#SBATCH --job-name=Report-state-county
#SBATCH --mem=4G
#SBATCH --partition={config['partition']}
#SBATCH --output ./report-state-county-%j.out

## Collect jobs with error
for i in `ls ${{PROJ_HOME}}/job_out_county/county-*.out`; do
    if grep -iq "ERROR" $i ; then
        echo $i | grep -Po "(?<=job_out_county/).*(?=_.*.out$)" >> ${{PROJ_HOME}}/job_out_county/failed-temp.txt
    fi
done

if [ `jq ."job_number_max" config.json` -gt 1 ]; then
## Collecting failed and timeout Slurm jobs
sacct -XP --state F,TO --noheader --starttime {time_ptr} --format JobName | grep "county" >> ${{PROJ_HOME}}/job_out_county/failed-temp.txt
fi

sleep 1
sort ${{PROJ_HOME}}/job_out_county/failed-temp.txt | uniq > ${{PROJ_HOME}}/job_out_county/failed.txt
rm ${{PROJ_HOME}}/job_out_county/failed-temp.txt

## Collect warnings
grep -i "warning" ${{PROJ_HOME}}/job_out_county/output_*.out > ${{PROJ_HOME}}/job_out_county/warning.txt
sleep 1

echo "-----------------------------------------------------------------------------------
Job(s) start time: {time_ptr}
Number of queries: {num_query}
Number of jobs: `ls ${{PROJ_HOME}}/job_out_county/*.out | wc -l`
Number of warnings: `cat ${{PROJ_HOME}}/job_out_county/warning.txt | wc -l`
Number of failed jobs: `cat ${{PROJ_HOME}}/job_out_county/failed.txt | wc -l`

Find name of jobs with a warning or failure in:
     - ./job_out_county/warning.txt
     - ./job_out_county/failed.txt

Find the CSV and JSON outputs in (when jobs done with no failure):
      - ./outputs/county-{time_pt}.csv
      - ./outputs/county-panel-{time_pt}.csv
      - ./outputs/county-{time_pt}.json
      - ./outputs/state-{time_pt}.csv
      - ./outputs/state-panel-{time_pt}.csv
      - ./outputs/state-{time_pt}.json

Failures can be related to:

      - EVALIDator and the FIADB may be unavailable during this time
      - Config file inputs may be unvaild
      - Input coordinates may be unvalid (for the 'coodinate' query type)
      - Slurm job failure

If the failure is related to EVALIDator servers or Slurm jobs, consider to run 'rebatch_file.sh' file to resubmit the failed jobs. Otherwise, modify config file and/or input files and resubmit the 'batch_file.sh'.
-----------------------------------------------------------------------------------"
""")
report.close()

## Submit the batch file
os.system(f"""
sleep 3
if [ {maxj} -gt 1 ]; then
JOBID=$(tail -qn 1 ${{PROJ_HOME}}/jobid-county.log)
sbatch --parsable --dependency=afterany:$(echo ${{JOBID}}) ${{PROJ_HOME}}/report-county.sh
else . ${{PROJ_HOME}}/report-county.sh > ${{PROJ_HOME}}/report-state-county-serial.out
fi
""")
