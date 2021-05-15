#!/usr/bin/env python

import os
import sys
import csv
import time
import json
import prep_data

## Time
time = time.strftime("%Y%m%d-%H%M%S")

## Open JSON inputs
config = json.load(open(sys.argv[1]))
tol = config['tolerance']
maxj = int(config['job_number_max'])
attribute = json.load(open(sys.argv[2]))

## Create a dictionary of FIA state codes
with open('./state_codes.csv', 'r') as cd:
    state_cd = prep_data.csv_dict(cd)

## Select states from the config
state_cd = prep_data.state_config(state_cd,config)
            
## Create a new directory and and remove log files
os.system(f"""
install -dvp ${{FIA}}/json/county_{time}
find ${{FIA}}/json/county_{time} -size 0 -delete")
rm ${{FIA}}/job-county-*
rm ${{PROJ_HOME}}/jobid-county.log
rm ${{PROJ_HOME}}/serial-county-log.out
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
    """)
    for year in config['year']:
        invyr_id = os.popen(f"""
        if [ ! -f ./fia_data/survey/{i}_POP_STRATUM.csv ]; then
        wget -c -nv --tries=2 https://apps.fs.usda.gov/fia/datamart/CSV/{i}_POP_STRATUM.csv -P ./fia_data/survey; fi
        cat ./fia_data/survey/{i}_POP_STRATUM.csv | awk -F , '{{print $4}}' | grep ".*01$" | sort | uniq
        """).read()[:-1].split('\n')
        
        if len(invyr_id[0]) == 6:
            in_yr = [x[2:-2] for x in invyr_id]
        else:
            in_yr = [x[1:-2] for x in invyr_id]
        
        invyr = [f"20{x}" for x in in_yr if int(x) < int(time[2:4])] + [f"19{x}" for x in in_yr if int(x) > int(time[2:4])]
        
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
            
            file_path = f"${{FIA}}/json/county_{time}/{att_cd}_{year}_{i}"
            batch.write(f"""
echo "---------------- county-{i}-{year}-{att_cd}"
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
    else . ${{FIA}}/job-county-{i}.sh >> ${{PROJ_HOME}}/serial-county-log.out
    fi
    """)

## Scritp to extract information and generate outputs
job = open('./job_county.py','w')
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
os.system("find ./fia_data/json/county_{time} -size 0 -delete")
json_files = glob.glob('./fia_data/json/county_{time}/*.json')

att_county = collections.defaultdict(dict)
att_state = collections.defaultdict(dict)

for i in json_files:
    try:
        with open(i) as jf:
            js_data = json.load(jf)
    except json.decoder.JSONDecodeError:
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
        except TypeError:
            continue
        if content[0] == 'Total':
            att_state[state_abb].update({{'state': state_abb, f"{{att_cd}}_{{year}}": value}})
        else:
            county = content[2].capitalize()
            att_county[f"{{county}}_{{state_abb}}"].update({{'county_cd': content[0], 'county': county, 'state': state_abb, f"{{att_cd}}_{{year}}": value}})

## JSON output
with open('./outputs/county-{time}.json', 'w') as fj:
    json.dump(att_county, fj)

with open('./outputs/state-{time}.json', 'w') as fj:
    json.dump(att_state, fj)

## CSV output - county
list_county = [x for x in att_county.values()]
lk = len(config['attribute_cd']) * len(config['year'])
if len(list_county) > 0:
    county_keys = list(list_county[0].keys())[:-lk]
    with open('./outputs/county-panel-{time}.csv', 'w') as fp:
        prep_data.list_dict_panel(list_county,county_keys,config,fp)

    for x in config['attribute_cd']:
        county_keys.extend([f"{{x}}_{{y}}" for y in config['year']])

    with open('./outputs/county-{time}.csv', 'w') as fc:
        prep_data.list_dict_csv(list_county,county_keys,fc)

## CSV output - state
list_state = [x for x in att_state.values()]
if len(list_state) > 0:
    state_keys = list(list_state[0].keys())[:-lk]
    with open('./outputs/state-panel-{time}.csv', 'w') as fp:
        prep_data.list_dict_panel(list_state,state_keys,config,fp)

    for x in config['attribute_cd']:
        state_keys.extend([f"{{x}}_{{y}}" for y in config['year']])

    with open('./outputs/state-{time}.csv', 'w') as fc:
        prep_data.list_dict_csv(list_state,state_keys,fc)
""")
job.close()

## Create a batch file
batch = open('./job_county.sh','w')
batch.write(f"""#!/bin/bash

#SBATCH --job-name=Output
#SBATCH --mem=8G
#SBATCH --partition={config['partition']}

python job_county.py config.json
""")
batch.close()

## Submit the batch file
os.system(f"""
sleep 5
if [ {maxj} -gt 1 ]; then
JOBID=$(cat ${{PROJ_HOME}}/jobid-county.log | tr '\n' ',' | grep -Po '.*(?=,)')
JID=$(sbatch --parsable --dependency=afterok:$(echo ${{JOBID}}) ${{PROJ_HOME}}/job_county.sh)
echo ${{JID}} > ${{PROJ_HOME}}/jobid-county.log
else . ${{PROJ_HOME}}/job_county.sh
fi
sleep 5
""")
