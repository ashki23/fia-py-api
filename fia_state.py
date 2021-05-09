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
            
## Archive old HTMLs
os.system(f"""
install -dvp ${{FIA}}/html/state_{time}
rm ${{FIA}}/job-state-*
rm ${{PROJ_HOME}}/jobid-state.log
rm ${{PROJ_HOME}}/serial-state-log.out
""")

## Create job files to download FIA HTML queries
for i in state_cd:
    print('************* state:', i, '***************')
    job = open(f"./fia_data/job-state-{i}.sh",'w')
    job.write(f"""#!/bin/bash

#SBATCH --job-name=FIAQuery_{i}
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --partition={config['partition']}
#SBATCH --time=04:00:00
    """)
    for year in config['year']:
        for att_cd in config['attribute_cd']:
            att = attribute[str(att_cd)]
            itr = 0
            n = 2 * tol
            while itr <= n:
                if itr == 0:
                    yl = yh = year
                    cd_yr = [f"{x}{year}" for x in [state_cd[i]]]
                    file_path = f"${{FIA}}/html/state_{time}/{att_cd}_{year}_{i}"
                    job.write(f"""
echo "---------------- {year} - {i} - {att}"
wget -c --tries=2 --random-wait "https://apps.fs.usda.gov/Evalidator/rest/Evalidator/fullreport?reptype=State&lat=0&lon=0&radius=0&snum={att}&sdenom=No denominator - just produce estimates&wc={','.join(cd_yr)}&pselected=None&rselected=All live stocking&cselected=All live stocking&ptime=Current&rtime=Current&ctime=Current&wf=&wnum=&wnumdenom=&FIAorRPA=FIADEF&outputFormat=HTML&estOnly=Y&schemaName=FS_FIADB." -O {file_path}_{year}.html
                    """)
                elif itr % 2 != 0:
                    yl = year - int(itr/2) - 1
                    cd_yr = [f"{x}{yl}" for x in [state_cd[i]]]
                    job.write(f"""
if [ -f {file_path}_{yl+itr}.html ]; then
if [ $(cat {file_path}_{yl+itr}.html | grep -c {state_cd[i]}{yl+itr}) -le 1 ] || [ $(cat {file_path}_{yl+itr}.html | grep -c '>Total<') -le 1 ]; then
rm {file_path}_{yl+itr}.html
wget -c --tries=2 --random-wait "https://apps.fs.usda.gov/Evalidator/rest/Evalidator/fullreport?reptype=State&lat=0&lon=0&radius=0&snum={att}&sdenom=No denominator - just produce estimates&wc={','.join(cd_yr)}&pselected=None&rselected=All live stocking&cselected=All live stocking&ptime=Current&rtime=Current&ctime=Current&wf=&wnum=&wnumdenom=&FIAorRPA=FIADEF&outputFormat=HTML&estOnly=Y&schemaName=FS_FIADB." -O {file_path}_{yl}.html
fi
fi
                    """)
                else:
                    yh = year + int(itr/2)
                    cd_yr = [f"{x}{yh}" for x in [state_cd[i]]]
                    job.write(f"""
if [ -f {file_path}_{yh-itr}.html ]; then
if [ $(cat {file_path}_{yh-itr}.html | grep -c {state_cd[i]}{yh-itr}) -le 1 ] || [ $(cat {file_path}_{yh-itr}.html | grep -c '>Total<') -le 1 ]; then
rm {file_path}_{yh-itr}.html
wget -c --tries=2 --random-wait "https://apps.fs.usda.gov/Evalidator/rest/Evalidator/fullreport?reptype=State&lat=0&lon=0&radius=0&snum={att}&sdenom=No denominator - just produce estimates&wc={','.join(cd_yr)}&pselected=None&rselected=All live stocking&cselected=All live stocking&ptime=Current&rtime=Current&ctime=Current&wf=&wnum=&wnumdenom=&FIAorRPA=FIADEF&outputFormat=HTML&estOnly=Y&schemaName=FS_FIADB." -O {file_path}_{yh}.html
fi
fi
                    """)
                itr += 1
            job.write(f"""
if [ -f {file_path}_{yl}.html ]; then
if [ $(cat {file_path}_{yl}.html | grep -c {state_cd[i]}{yl}) -le 1 ] || [ $(cat {file_path}_{yl}.html | grep -c '>Total<') -le 1 ]; then
rm {file_path}_{yl}.html
echo "ERROR: the FIA dataset does not include records for {att} for state {i} between {yl}-{yh}"
fi
fi

if [ -f {file_path}_{yh}.html ]; then
if [ $(cat {file_path}_{yh}.html | grep -c {state_cd[i]}{yh}) -le 1 ] || [ $(cat {file_path}_{yh}.html | grep -c '>Total<') -le 1 ]; then
rm {file_path}_{yh}.html
echo "ERROR: the FIA dataset does not include records for {att} for state {i} between {yl}-{yh}"
fi
fi
            """)
    job.close()
    
    ## Send the job file to run
    os.system(f"""
if [ {maxj} -gt 1 ]; then
JID=$(sbatch --parsable ${{FIA}}/job-state-{i}.sh)
echo ${{JID}} >> ${{PROJ_HOME}}/jobid-state.log
else . ${{FIA}}/job-state-{i}.sh >> ${{PROJ_HOME}}/serial-state-log.out
fi
    """)

## Create a Bash file to extract level of attributes from FIA HTML files
print('************* Obtain level of attributes ***************')
job = open('./fia_data/job-state-html.sh','w')
job.write(f"""#!/bin/bash

#SBATCH --job-name=Extract
#SBATCH --mem=8G

echo "success: resources has been allocated"
cd ${{FIA}}/html/state_{time}

for i in *.html; do
echo $i | grep -Po '([A-Z]{{2}})' >> ./state.txt
echo $i | grep -Po '(?<=_)\d{{4}}(?=_)' >> ./year.txt
echo $i | grep -Po '^\d*(?=_)' >> ./attribute.txt
cat $i | grep -A 1 'nowrap="nowrap">Total</th>' | tr -d , | grep -Po '\d*' >> ./total_state.txt
done

paste -d _ attribute.txt year.txt > ./attribute_year.txt
paste -d , state.txt year.txt attribute_year.txt total_state.txt > ./att_level_state.csv

cd ${{PROJ_HOME}}
""")
job.close()

## Send the job file to run
os.system(f"""
sleep 5
if [ {maxj} -gt 1 ]; then
JOBID=$(cat ${{PROJ_HOME}}/jobid-state.log | tr '\n' ',' | grep -Po '.*(?=,)')
JID=$(sbatch --parsable --dependency=afterok:$(echo ${{JOBID}}) ${{FIA}}/job-state-html.sh)
echo ${{JID}} > ${{PROJ_HOME}}/jobid-state.log
else . ${{FIA}}/job-state-html.sh
fi
""")

## Create a dictionary of FIA attribute levels for each state
job = open('./job_state.py','w')
job.write(f"""#!/usr/bin/env python

import re
import sys
import csv
import json
import prep_data
import collections

config = json.load(open(sys.argv[1]))
with open('./fia_data/html/state_{time}/att_level_state.csv', 'r') as att:
    att_data_state = att.readlines()

## Convert CSV to ListDict with RE
pattern = re.compile('(.*)[,]' * 3 + '(.*)')
state_data_dict = collections.defaultdict(dict)
for a in att_data_state:
    a = a[:-1]
    t = pattern.search(a)
    if t is None:
        continue
    state_data_dict[t.group(1)].update({{'state': t.group(1), t.group(3): t.group(4)}})

state_data = []
for d in state_data_dict:
    state_data.append(state_data_dict[d])

## JSON output
with open('./outputs/state-{time}.json', 'w') as fj:
    json.dump(state_data, fj)

## CSV output
state_keys = ['state']
with open('./outputs/state-panel-{time}.csv', 'w') as fp:
    prep_data.list_dict_panel(state_data,state_keys,config,fp)

for x in config['attribute_cd']:
    state_keys.extend([f"{{x}}_{{y}}" for y in config['year']])

with open('./outputs/state-{time}.csv', 'w') as fc:
    prep_data.list_dict_csv(state_data,state_keys,fc)

""")
job.close()
    
## Create a job file
job = open('./job_state.sh','w')
job.write("""#!/bin/bash

#SBATCH --job-name=Output
#SBATCH --mem=4G

python job_state.py config.json
""")
job.close()

## Send the last job to run
os.system(f"""
sleep 5
if [ {maxj} -gt 1 ]; then
JID=$(sbatch --parsable --dependency=afterok:$(cat ${{PROJ_HOME}}/jobid-state.log) ${{PROJ_HOME}}/job_state.sh)
echo ${{JID}} > ${{PROJ_HOME}}/jobid-state.log
else . ${{PROJ_HOME}}/job_state.sh
fi
sleep 5
""")
