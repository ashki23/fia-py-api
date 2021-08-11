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
max_job = int(config['job_number_max'])
attribute = json.load(open(sys.argv[2]))

## Create a dictionary of FIA state codes
with open('./state_codes.csv', 'r') as cd:
    state_cd = prep_data.csv_dict(cd)

## Select states from the config
state_cd = prep_data.state_config(state_cd,config)
input_state = list(state_cd.keys())
if 'DC' in input_state: input_state.remove('DC')

## Create a new directory and and remove log files
os.system(f"""
install -dvp ${{FIA}}/json/state-county-{time_pt}
install -dvp ${{PROJ_HOME}}/job-out-state-county/archive/{time_pt}
rm ${{FIA}}/job-state-county-*
rm ${{PROJ_HOME}}/jobid-state-county.log
mv ${{PROJ_HOME}}/job-out-state-county/*.* ${{PROJ_HOME}}/job-out-state-county/archive/{time_pt}
""")

## Calculating optimal batch size
nrow = len(input_state)
num_query = len(config['attribute_cd']) * len(config['year']) * nrow
batch_size = max(math.ceil(num_query / max_job), 1)

## FIA inventory years for each state
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
        for st in input_state:
            cd = state_cd[st]
            if tol == 0:
                if str(year) in st_invyr[cd]:
                    yr = year
                    cd_yr = f"{cd}{yr}"
                else:
                    print(f"Warning: Estimate not available for state {st} for year {year}")
                    continue
            else:
                diff = [abs(int(x) - year) for x in st_invyr[cd]]
                indx = diff.index(min(diff))
                yr = st_invyr[cd][indx]
                cd_yr = f"{cd}{yr}"
                
            query_dict = {'att_cd': att_cd, 'att': att, 'year': year, 'cd_yr': cd_yr, 'yr': yr, 'st': st}
            query_list.append(query_dict)

for i in range(max_job):
    select_qr = query_list[i * batch_size:(i + 1) * batch_size]
    if len(select_qr) > 0:
        print('*************', 'state/county - batch', i, '***************')
        batch = open(f"./fia_data/job-state-county-{i}.sh",'w')
        batch.write(f"""#!/bin/bash

#SBATCH --job-name=state-county-{i}
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --partition={config['partition']}
#SBATCH --time={config['job_time_hr']}:00:00
#SBATCH --output=./job-out-state-county/state-county-{i}-%j.out
        """)
        
        rnum = 1
        for q in select_qr:
            file_path = f"${{FIA}}/json/state-county-{time_pt}/{q['att_cd']}-{q['year']}-{q['st']}-batch{i}"
            batch.write(f"""
echo "----------------------- {q['att_cd']}-{q['year']}-{q['st']} | {rnum} out of {len(select_qr)}"
if [ ! -f {file_path}-{q['yr']}.json ]; then
wget --tries=3 --timeout=180 --random-wait "https://apps.fs.usda.gov/Evalidator/rest/Evalidator/fullreport?reptype=State&lat=0&lon=0&radius=0&snum={q['att']}&sdenom=No denominator - just produce estimates&wc={q['cd_yr']}&pselected=None&rselected=County code and name&cselected=None&ptime=Current&rtime=Current&ctime=Current&wf=&wnum=&wnumdenom=&FIAorRPA=FIADEF&outputFormat=JSON&estOnly=Y&schemaName=FS_FIADB." -O {file_path}-{q['yr']}.json
fi
            """)
            rnum += 1
        batch.close()
        
        ## Submit the batch file
        os.system(f"""
        if [ {max_job} -gt 1 ]; then
        JID=$(sbatch --parsable ${{FIA}}/job-state-county-{i}.sh)
        echo ${{JID}} >> ${{PROJ_HOME}}/jobid-state-county.log
        else . ${{FIA}}/job-state-county-{i}.sh > ${{PROJ_HOME}}/job-out-state-county/state-county-{i}.out
        fi
        """)

## Scritp to extract information and generate outputs
job = open('./job-state-county.py','w')
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
json_files = glob.glob('./fia_data/json/state-county-{time_pt}/*.json')
att_county = collections.defaultdict(dict)
att_state = collections.defaultdict(dict)

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
    state_abb = js_data['EVALIDatorOutput']['row'][1]['content'].split()[1].upper()
    att_cd = js_data['EVALIDatorOutput']['numeratorAttributeNumber']

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
            county = content[2:]
            county = " ".join([x.capitalize() for x in county])
            att_county[f"{{state_abb}}_{{county}}"].update({{'county_cd': content[0], 'county': county, 'state': state_abb, f"{{att_cd}}_{{year}}": value}})

## Sorting by keys
att_county = {{k: att_county[k] for k in sorted(att_county.keys())}}
att_state = {{k: att_state[k] for k in sorted(att_state.keys())}}

## JSON output
with open('./outputs/county-{time_pt}.json', 'w') as fj:
    json.dump(att_county, fj)

with open('./outputs/state-{time_pt}.json', 'w') as fj:
    json.dump(att_state, fj)

## CSV output - county
list_county = [x for x in att_county.values()]
if len(list_county) > 0:
    county_keys = ['county_cd','county','state']
    with open('./outputs/county-panel-{time_pt}.csv', 'w') as fp:
        prep_data.list_dict_panel(list_county,county_keys,config,fp)

    for x in config['attribute_cd']:
        county_keys.extend([f"{{x}}_{{y}}" for y in config['year']])

    with open('./outputs/county-{time_pt}.csv', 'w') as fc:
        prep_data.list_dict_csv(list_county,county_keys,fc)

## CSV output - state
list_state = [x for x in att_state.values()]
if len(list_state) > 0:
    state_keys = ['state']
    with open('./outputs/state-panel-{time_pt}.csv', 'w') as fp:
        prep_data.list_dict_panel(list_state,state_keys,config,fp)

    for x in config['attribute_cd']:
        state_keys.extend([f"{{x}}_{{y}}" for y in config['year']])

    with open('./outputs/state-{time_pt}.csv', 'w') as fc:
        prep_data.list_dict_csv(list_state,state_keys,fc)
""")
job.close()

## Create a batch file
batch = open('./job-state-county.sh','w')
batch.write(f"""#!/bin/bash

#SBATCH --job-name=Output-state-county
#SBATCH --mem=8G
#SBATCH --partition={config['partition']}
#SBATCH --output=./job-out-state-county/output-%j.out

python job-state-county.py config.json
""")
batch.close()

## Submit the batch file
os.system(f"""
sleep 2
if [ {max_job} -gt 1 ]; then
JOBID=$(cat ${{PROJ_HOME}}/jobid-state-county.log | tr '\n' ',' | grep -Po '.*(?=,$)')
JID=$(sbatch --parsable --dependency=afterok:$(echo ${{JOBID}}) ${{PROJ_HOME}}/job-state-county.sh)
echo ${{JID}} >> ${{PROJ_HOME}}/jobid-state-county.log
else . ${{PROJ_HOME}}/job-state-county.sh > ${{PROJ_HOME}}/job-out-state-county/output-serial.out
fi
sleep 2
""")

## Create a batch file to collect reports
report = open(f"./report-state-county.sh",'w')
report.write(f"""#!/bin/bash

#SBATCH --job-name=Report-state-county
#SBATCH --mem=4G
#SBATCH --partition={config['partition']}
#SBATCH --output=./report-state-county-%j.out

## Record the time that jobs are started
echo {time_ptr} > ./time_state_county

## Collect jobs with error
for i in `ls ${{PROJ_HOME}}/job-out-state-county/state-county-*.out`; do
    if grep -Piq "giving up|proxy error|failed" $i; then
        echo $i | grep -Po "(?<=job-out-state-county/).*(?=-.*.out$)" >> ${{PROJ_HOME}}/job-out-state-county/failed-temp.txt
    fi
done

if [ `jq ."job_number_max" config.json` -gt 1 ]; then
## Collecting failed and timeout Slurm jobs
sacct -XP --state F,TO --noheader --starttime $1 --format JobName | grep "state-county-" >> ${{PROJ_HOME}}/job-out-state-county/failed-temp.txt
fi

## Collect warnings
if [ ! -f ${{PROJ_HOME}}/job-out-state-county/output-*.out ]; then
. ${{PROJ_HOME}}/job-state-county.sh > ${{PROJ_HOME}}/job-out-state-county/output-msc.out
sleep 1
fi
grep -i "warning" ${{PROJ_HOME}}/job-out-state-county/output-*.out > ${{PROJ_HOME}}/job-out-state-county/warning.txt
sleep 1

## Collect jobs with warning
for w in $(cat ${{PROJ_HOME}}/job-out-state-county/warning.txt | grep input | grep -Po "(?<=batch).*(?=-)"); do
echo state-county-$w >> ${{PROJ_HOME}}/job-out-state-county/failed-temp.txt
done
sleep 1

if [ -f ${{PROJ_HOME}}/job-out-state-county/failed-temp.txt ]; then
sort ${{PROJ_HOME}}/job-out-state-county/failed-temp.txt | uniq > ${{PROJ_HOME}}/job-out-state-county/failed.txt
rm ${{PROJ_HOME}}/job-out-state-county/failed-temp.txt
else touch ${{PROJ_HOME}}/job-out-state-county/failed.txt
fi
sleep 1

echo "-----------------------------------------------------------------------------------
Job(s) start time: {time_ptr}
Number of queries: {num_query}
Number of jobs: `ls ${{PROJ_HOME}}/job-out-state-county/*.out | wc -l`
Number of warnings: `cat ${{PROJ_HOME}}/job-out-state-county/warning.txt | wc -l`
Number of failed jobs: `cat ${{PROJ_HOME}}/job-out-state-county/failed.txt | wc -l`

Find name of jobs with a warning or failure in:
    - ./job-out-state-county/warning.txt
    - ./job-out-state-county/failed.txt

Find the CSV and JSON outputs in (when jobs are done with no failure):
    - ./outputs/county-{time_pt}.csv
    - ./outputs/county-panel-{time_pt}.csv
    - ./outputs/county-{time_pt}.json
    - ./outputs/state-{time_pt}.csv
    - ./outputs/state-panel-{time_pt}.csv
    - ./outputs/state-{time_pt}.json

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
JOBID=$(tail -qn 1 ${{PROJ_HOME}}/jobid-state-county.log)
sbatch --dependency=afterany:$(echo ${{JOBID}}) ${{PROJ_HOME}}/report-state-county.sh "{time_ptr}"
else . ${{PROJ_HOME}}/report-state-county.sh > ${{PROJ_HOME}}/report-state-county-serial.out
fi
""")
