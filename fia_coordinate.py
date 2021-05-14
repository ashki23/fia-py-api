#!/usr/bin/env python

import os
import sys
import time
import json

## Time
time = time.strftime("%Y%m%d-%H%M%S")

## Open JSON inputs
config = json.load(open(sys.argv[1]))
tol = config['tolerance']
maxj = config['job_number_max']
attribute = json.load(open(sys.argv[2]))
input_data = json.load(open(sys.argv[3]))
file_name = sys.argv[3].split('.')[0]

## Archive previous JSONs and remove log files
os.system(f"""
install -dvp ${{FIA}}/json/{file_name}_{time}
rm ${{FIA}}/job-{file_name}-*
rm ${{PROJ_HOME}}/jobid-{file_name}.log
rm ${{PROJ_HOME}}/serial-{file_name}-log.out
""")

## Calculating optimal job size
## opt_job_size find maximum size of each job to satisfy max job num (maxj) 
opt_job_size = (len(config['attribute_cd']) * len(config['year']) * len(input_data)) / maxj
job_size = round(opt_job_size) + 1

## Create batch files to download FIA JSON queries
for att_cd in config['attribute_cd']:
    att = attribute[str(att_cd)]
    for year in config['year']:
        i = 0
        nrow = len(input_data)
        while i < nrow:
            print('*************', year, att, 'row: ', i, '-', i + job_size, '***************')
            batch = open(f"./fia_data/job-{file_name}-{att_cd}-{year}-{i}.sh",'w')
            batch.write(f"""#!/bin/bash

#SBATCH --job-name={file_name}-{att_cd}-{year}-{i}
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --partition={config['partition']}
#SBATCH --time={config['job_time_hr']}:00:00
            """)
            lnum = 1
            for l in input_data[i:i + job_size]:
                states_all = [l['state']] + l['neighbors']
                states_cd = [l['state_cd']] + l['neighbors_cd']
                
                st_invyr = {}
                for st in states_all:
                    invyr_id = os.popen(f"""
                    if [ ! -f ./fia_data/survey/{st}_POP_STRATUM.csv ]; then
                    wget -c -nv --tries=2 https://apps.fs.usda.gov/fia/datamart/CSV/{st}_POP_STRATUM.csv -P ./fia_data/survey
                    fi
                    cat ./fia_data/survey/{st}_POP_STRATUM.csv | awk -F , '{{print $4}}' | grep ".*01$" | sort | uniq
                    """).read()[:-1].split('\n')
                    
                    if len(invyr_id[0]) == 6:
                        in_yr = [x[2:-2] for x in invyr_id]
                    else:
                        in_yr = [x[1:-2] for x in invyr_id]
                    
                    invyr = [f"20{x}" for x in in_yr if int(x) < int(time[2:4])] + [f"19{x}" for x in in_yr if int(x) > int(time[2:4])]
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
                
                file_path = f"${{FIA}}/json/{file_name}_{time}/{att_cd}_{year}_id{l['unit_id']}"
                batch.write(f"""
echo "---------------- {file_name}-{att_cd}-{year}-{i} | {lnum} out of {len(input_data[i:i + job_size])}"
wget -c --tries=2 --random-wait "https://apps.fs.usda.gov/Evalidator/rest/Evalidator/fullreport?reptype=Circle&lat={l['lat']}&lon={l['lon']}&radius={l['radius']}&snum={att}&sdenom=No denominator - just produce estimates&wc={','.join(cd_yr)}&pselected=None&rselected=All live stocking&cselected=None&ptime=Current&rtime=Current&ctime=Current&wf=&wnum=&wnumdenom=&FIAorRPA=FIADEF&outputFormat=JSON&estOnly=Y&schemaName=FS_FIADB." -O {file_path}_{yr}.json
                """)
            batch.close()
            lnum += 1
            
            ## Submit the batch file
            os.system(f"""
            if [ {maxj} -gt 1 ]; then
            JID=$(sbatch --parsable ${{FIA}}/job-{file_name}-{att_cd}-{year}-{i}.sh)
            echo ${{JID}} >> ${{PROJ_HOME}}/jobid-{file_name}.log
            else . ${{FIA}}/job-{file_name}-{att_cd}-{year}-{i}.sh >> ${{PROJ_HOME}}/serial-{file_name}-log.out
            fi
            """)
            i += job_size

## Scritp to extract information and generate outputs
job = open(f"./job_{file_name}.py",'w')
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
json_files = glob.glob('./fia_data/json/{file_name}_{time}/*.json')
att_coordinate = collections.defaultdict(dict)

for i in json_files:
    try:
        with open(i) as jf:
            js_data = json.load(jf)
    except json.decoder.JSONDecodeError:
        continue

    n = os.path.basename(i)
    year = re.findall('(?<=_)\d{{4}}(?=_.)', i)[0]
    lat = js_data['EVALIDatorOutput']['circleLatitude']
    lon = js_data['EVALIDatorOutput']['circleLongitude']
    radius = js_data['EVALIDatorOutput']['circleRadiusMiles']
    att_cd = js_data['EVALIDatorOutput']['numeratorAttributeNumber']
    state_inv = list(js_data['EVALIDatorOutput']['selectedInventories']['stateInventory'])[0].split()
    unit_id = (str(lat).replace('.','') + str(lon).replace('.','').replace('-',''))[:8]
    state = state_inv[0].capitalize()
    state_cd = state_inv[1][:-4]
    year_survey = state_inv[1][2:]
    value = round(js_data['EVALIDatorOutput']['row'][0]['column'][0]['cellValueNumerator'])
    att_coordinate[unit_id].update({{'unit_id': unit_id, 'state': state, 'state_cd': state_cd, 'lat': lat, 'lon': lon, 'radius': radius, f"{{att_cd}}_{{year}}": value}})

## JSON output
with open('./outputs/{file_name}-{time}.json', 'w') as fj:
    json.dump(att_coordinate, fj)

## CSV output
list_coordinate = [x for x in att_coordinate.values()]
lk = len(config['attribute_cd']) * len(config['year'])
if len(list_coordinate) > 0:
    keys = list(list_coordinate[0].keys())[:-lk]
    with open('./outputs/{file_name}-panel-{time}.csv', 'w') as fp:
        prep_data.list_dict_panel(list_coordinate,keys,config,fp)
    
    for x in config['attribute_cd']:
        keys.extend([f"{{x}}_{{y}}" for y in config['year']])
    
    with open('./outputs/{file_name}-{time}.csv', 'w') as fc:
        prep_data.list_dict_csv(list_coordinate,keys,fc)
""")
job.close()

## Create a batch file
job = open(f"./job_{file_name}.sh",'w')
job.write(f"""#!/bin/bash

#SBATCH --job-name=Output
#SBATCH --mem=8G
#SBATCH --partition={config['partition']}

python job_{file_name}.py config.json
""")
job.close()

## Submit the batch file
os.system(f"""
sleep 5
if [ {maxj} -gt 1 ]; then
JOBID=$(cat ${{PROJ_HOME}}/jobid-{file_name}.log | tr '\n' ',' | grep -Po '.*(?=,)')
JID=$(sbatch --parsable --dependency=afterok:$(echo ${{JOBID}}) ${{PROJ_HOME}}/job_{file_name}.sh)
echo ${{JID}} > ${{PROJ_HOME}}/jobid-{file_name}.log
else . ${{PROJ_HOME}}/job_{file_name}.sh
fi
sleep 5
""")
