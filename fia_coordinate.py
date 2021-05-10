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

## Archive previous HTMLs and remove log files
os.system(f"""
install -dvp ${{FIA}}/html/{file_name}_{time}
rm ${{FIA}}/job-{file_name}-*
rm ${{PROJ_HOME}}/jobid-{file_name}.log
rm ${{PROJ_HOME}}/serial-{file_name}-log.out
""")

## Calculating optimal job size
## opt_job_size find maximum size of each job to satisfy max job num (maxj) 
opt_job_size = (len(config['attribute_cd']) * len(config['year']) * len(input_data)) / maxj
job_size = round(opt_job_size) + 1

## Create job files to download FIA HTML queries
for att_cd in config['attribute_cd']:
    att = attribute[str(att_cd)]
    for year in config['year']:
        i = 0
        nrow = len(input_data)
        while i < nrow:
            print('*************', year, att, 'row: ', i, '-', i + job_size, '***************')
            job = open(f"./fia_data/job-{file_name}-{att_cd}-{year}-{i}.sh",'w')
            job.write(f"""#!/bin/bash

#SBATCH --job-name=FIAQuery
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --partition={config['partition']}
#SBATCH --time=08:00:00
            """)
            for l in input_data[i:i + job_size]: # Select based on job_size
                states_all = [l['state']] + l['neighbors']
                states_cd = [l['state_cd']] + l['neighbors_cd']
                st_invyr = {}
                for st in states_all:
                    invyr = os.popen(f"""
                    if [ ! -f ./fia_data/survey/{st}_SURVEY.csv ]; then
                    wget -c -nv --tries=2 https://apps.fs.usda.gov/fia/datamart/CSV/{st}_SURVEY.csv -P ./fia_data/survey
                    fi
                    cat ./fia_data/survey/{st}_SURVEY.csv | awk -F , '{{print $2}}' | tail -n +2 | sort | uniq
                    """).read()[:-1].split('\n')
                    st_invyr[states_cd[states_all.index(st)]] = invyr
                
                if tol == 0:
                    yr = year
                    cd_yr = [f"{x}{yr}" for x in states_cd]
                else:
                    cd_yr = []
                    for si in st_invyr.keys():
                        diff = [abs(int(x) - year) for x in st_invyr[si]]
                        indx = diff.index(min(diff))
                        yr = st_invyr[si][indx]
                        cd_yr.append(f"{si}{yr}")
                
                file_path = f"${{FIA}}/html/{file_name}_{time}/{att_cd}_{year}_id{l['unit_id']}"
                job.write(f"""
echo "---------------- {year} - {att} row {i} - {i + job_size}"
wget -c --tries=2 --random-wait "https://apps.fs.usda.gov/Evalidator/rest/Evalidator/fullreport?reptype=Circle&lat={l['lat']}&lon={l['lon']}&radius={l['radius']}&snum={att}&sdenom=No denominator - just produce estimates&wc={','.join(cd_yr)}&pselected=None&rselected=All live stocking&cselected=All live stocking&ptime=Current&rtime=Current&ctime=Current&wf=&wnum=&wnumdenom=&FIAorRPA=FIADEF&outputFormat=HTML&estOnly=Y&schemaName=FS_FIADB." -O {file_path}_{yr}.html
                """)
            job.close()
            
            ## Send the job file to run
            os.system(f"""
if [ {maxj} -gt 1 ]; then
JID=$(sbatch --parsable ${{FIA}}/job-{file_name}-{att_cd}-{year}-{i}.sh)
echo ${{JID}} >> ${{PROJ_HOME}}/jobid-{file_name}.log
else . ${{FIA}}/job-{file_name}-{att_cd}-{year}-{i}.sh >> ${{PROJ_HOME}}/serial-{file_name}-log.out
fi
        """)
            i += job_size

## Create a Bash file to extract level of attributes from FIA HTML
print('************* Obtain level of attributes ***************')
job = open(f"./fia_data/job-{file_name}-html.sh",'w')
job.write(f"""#!/bin/bash

#SBATCH --job-name=Extract
#SBATCH --mem=16G

echo "success: resources has been allocated"
cd ${{FIA}}/html/{file_name}_{time}

for i in *.html; do
echo $i | grep -Po '^\d*(?=_)' >> ./att.txt
echo $i | grep -Po '(?<=id)\d*(?=_)' >> ./unit_id.txt
echo $i | grep -Po '(?<=_)\d{{4}}(?=_.)' >> ./year.txt
cat $i | grep -A 1 'nowrap="nowrap">Total</th>' | tr -d , | grep -Po '\d*' >> ./att_total.txt 
done

paste -d _ att.txt year.txt > ./att_year.txt
paste -d , unit_id.txt year.txt att_year.txt att_total.txt > ./att_level_{file_name}.csv

cd ${{PROJ_HOME}}
""")
job.close()

## Send the job file to run
os.system(f"""
sleep 5
if [ {maxj} -gt 1 ]; then
JOBID=$(cat ${{PROJ_HOME}}/jobid-{file_name}.log | tr '\n' ',' | grep -Po '.*(?=,)')
JID=$(sbatch --parsable --dependency=afterok:$(echo ${{JOBID}}) ${{FIA}}/job-{file_name}-html.sh)
echo ${{JID}} > ${{PROJ_HOME}}/jobid-{file_name}.log
else . ${{FIA}}/job-{file_name}-html.sh
fi
""")

## Create a dictionary of FIA attribute levels for each coordinate
job = open(f"./job_{file_name}.py",'w')
job.write(f"""#!/usr/bin/env python

import re
import sys
import csv
import json
import prep_data
import collections

config = json.load(open(sys.argv[1]))
{file_name}_data = json.load(open(sys.argv[2]))

## Read level of attributes from CSV
with open('./fia_data/html/{file_name}_{time}/att_level_{file_name}.csv', 'r') as att:
    att_data = att.readlines()

## Convert CSV to ListDict with RE
pattern = re.compile('(.*)[,]' * 3 + '(.*)')
att_{file_name} = collections.defaultdict(dict)
for a in att_data:
    a = a[:-1]
    t = pattern.search(a)
    if t is None:
        continue
    att_{file_name}[t.group(1)].update({{t.group(3): t.group(4)}})

## Add the attribite levels to {file_name}_data
for l in {file_name}_data:
    levels = att_{file_name}[l['unit_id']]
    for k in levels.keys():
        l[k] = levels[k]

## JSON output
with open('./outputs/{file_name}-{time}.json', 'w') as fj:
    json.dump({file_name}_data, fj)

## CSV output
lk = len(config['attribute_cd']) * len(config['year'])
keys = list({file_name}_data[0].keys())[:-lk]
with open('./outputs/{file_name}-panel-{time}.csv', 'w') as fp:
    prep_data.list_dict_panel({file_name}_data,keys,config,fp)

for x in config['attribute_cd']:
    keys.extend([f"{{x}}_{{y}}" for y in config['year']])

with open('./outputs/{file_name}-{time}.csv', 'w') as fc:
    prep_data.list_dict_csv({file_name}_data,keys,fc)

""")
job.close()

## Create a job file
job = open(f"./job_{file_name}.sh",'w')
job.write(f"""#!/bin/bash

#SBATCH --job-name=Output
#SBATCH --mem=8G

python job_{file_name}.py config.json {file_name}.json
""")
job.close()

## Send the last job to run
os.system(f"""
sleep 5
if [ {maxj} -gt 1 ]; then
JID=$(sbatch --parsable --dependency=afterok:$(cat ${{PROJ_HOME}}/jobid-{file_name}.log) ${{PROJ_HOME}}/job_{file_name}.sh)
echo ${{JID}} > ${{PROJ_HOME}}/jobid-{file_name}.log
else . ${{PROJ_HOME}}/job_{file_name}.sh
fi
sleep 5
""")
