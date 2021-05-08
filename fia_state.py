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
maxj = int(config['job_number_max'])
attribute = json.load(open(sys.argv[2]))

## Create a dictionary of FIA state codes
with open('./state_codes.csv', 'r') as cd:
    state_cd = prep_data.csv_dict(cd)
    
## Select states from the config
state_cd = prep_data.state_config(state_cd,config)
            
## Archive old HTMLs
os.system("""
install -dvp ${FIA}/html_state/archived-%s
mv ${FIA}/html_state/*.html ${FIA}/html_state/archived-%s
rm ${FIA}/job-state-*
rm ${PROJ_HOME}/jobid-state.log
""" % (time,time))

## Create job files to download FIA HTML queries
for i in state_cd:
    print('************* state:', i, '***************')
    f = open('./fia_data/job-state-%s.sh' % (i,),'w')
    f.write("""#!/bin/bash

#SBATCH --job-name=FIAQuery_%s
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --partition=%s
#SBATCH --time=04:00:00
    """ % (i,config['partition']))
    for year in config['year']:
        for att_cd in config['attribute_cd']:
            att = attribute['%d' % att_cd]
            itr = 0
            n = 4
            while itr <= n:
                if itr == 0:
                    cd_yr = ['%s%s' % (x,year) for x in [state_cd[i]]]
                    f.write("""
echo "---------------- %s - %s - %s"
wget -c --tries=2 --random-wait "https://apps.fs.usda.gov/Evalidator/rest/Evalidator/fullreport?reptype=State&lat=0&lon=0&radius=0&snum=%s&sdenom=No denominator - just produce estimates&wc=%s&pselected=None&rselected=All live stocking&cselected=All live stocking&ptime=Current&rtime=Current&ctime=Current&wf=&wnum=&wnumdenom=&FIAorRPA=FIADEF&outputFormat=HTML&estOnly=Y&schemaName=FS_FIADB." -O ${FIA}/html_state/%s_%s_%s_%s.html
                    """ % (year,i,att, att,','.join(cd_yr),att_cd,year,i,year))
                elif itr % 2 != 0:
                    yl = year - int(itr/2) - 1
                    #print(yl)
                    cd_yr = ['%s%s' % (x,yl) for x in [state_cd[i]]]
                    f.write("""
if [ -f ${FIA}/html_state/%s_%s_%s_%s.html ]; then
if [ $(cat ${FIA}/html_state/%s_%s_%s_%s.html | grep -c %s%s) -le 1 ] || [ $(cat ${FIA}/html_state/%s_%s_%s_%s.html | grep -c '>Total<') -le 1 ]; then
rm ${FIA}/html_state/%s_%s_%s_%s.html
wget -c --tries=2 --random-wait "https://apps.fs.usda.gov/Evalidator/rest/Evalidator/fullreport?reptype=State&lat=0&lon=0&radius=0&snum=%s&sdenom=No denominator - just produce estimates&wc=%s&pselected=None&rselected=All live stocking&cselected=All live stocking&ptime=Current&rtime=Current&ctime=Current&wf=&wnum=&wnumdenom=&FIAorRPA=FIADEF&outputFormat=HTML&estOnly=Y&schemaName=FS_FIADB." -O ${FIA}/html_state/%s_%s_%s_%s.html
fi
fi
                    """ % (att_cd,year,i,yl+itr, att_cd,year,i,yl+itr, state_cd[i],yl+itr, att_cd,year,i,yl+itr, att_cd,year,i,yl+itr, att,','.join(cd_yr), att_cd,year,i,yl))
                else:
                    yh = year + int(itr/2)
                    #print(yh)
                    cd_yr = ['%s%s' % (x,yh) for x in [state_cd[i]]]
                    f.write("""
if [ -f ${FIA}/html_state/%s_%s_%s_%s.html ]; then
if [ $(cat ${FIA}/html_state/%s_%s_%s_%s.html | grep -c %s%s) -le 1 ] || [ $(cat ${FIA}/html_state/%s_%s_%s_%s.html | grep -c '>Total<') -le 1 ]; then
rm ${FIA}/html_state/%s_%s_%s_%s.html
wget -c --tries=2 --random-wait "https://apps.fs.usda.gov/Evalidator/rest/Evalidator/fullreport?reptype=State&lat=0&lon=0&radius=0&snum=%s&sdenom=No denominator - just produce estimates&wc=%s&pselected=None&rselected=All live stocking&cselected=All live stocking&ptime=Current&rtime=Current&ctime=Current&wf=&wnum=&wnumdenom=&FIAorRPA=FIADEF&outputFormat=HTML&estOnly=Y&schemaName=FS_FIADB." -O ${FIA}/html_state/%s_%s_%s_%s.html
fi
fi
                    """ % (att_cd,year,i,yh-itr, att_cd,year,i,yh-itr, state_cd[i],yh-itr, att_cd,year,i,yh-itr, att_cd,year,i,yh-itr, att,','.join(cd_yr), att_cd,year,i,yh))
                itr += 1
                
            f.write("""
if [ -f ${FIA}/html_state/%s_%s_%s_%s.html ]; then
if [ $(cat ${FIA}/html_state/%s_%s_%s_%s.html | grep -c %s%s) -le 1 ] || [ $(cat ${FIA}/html_state/%s_%s_%s_%s.html | grep -c '>Total<') -le 1 ]; then
rm ${FIA}/html_state/%s_%s_%s_%s.html
echo "ERROR: the FIA dataset does not include records for %s for state %s between %s-%s"
fi
fi

if [ -f ${FIA}/html_state/%s_%s_%s_%s.html ]; then
if [ $(cat ${FIA}/html_state/%s_%s_%s_%s.html | grep -c %s%s) -le 1 ] || [ $(cat ${FIA}/html_state/%s_%s_%s_%s.html | grep -c '>Total<') -le 1 ]; then
rm ${FIA}/html_state/%s_%s_%s_%s.html
echo "ERROR: the FIA dataset does not include records for %s for state %s between %s-%s"
fi
fi
            """ % (att_cd,year,i,yl, att_cd,year,i,yl, state_cd[i],yl, att_cd,year,i,yl, att_cd,year,i,yl, att,i,yl,yh, att_cd,year,i,yh, att_cd,year,i,yh, state_cd[i],yh, att_cd,year,i,yh, att_cd,year,i,yh, att,i,yl,yh))
    f.close()
    
    ## Send the job file to run
    os.system("""
if [ %d -gt 1 ]; then
JID=$(sbatch --parsable ${FIA}/job-state-%s.sh)
echo ${JID} >> ${PROJ_HOME}/jobid-state.log
else . ${FIA}/job-state-%s.sh >> ${PROJ_HOME}/serial_download_log.out
fi
    """ % (maxj,i,i))

## Create a Bash file to extract level of attributes from FIA HTML files
print('************* Obtain level of attributes ***************')
f = open('./fia_data/job-state-html.sh','w')
f.write("""#!/bin/bash

#SBATCH --job-name=Extract
#SBATCH --mem=8G

echo "success: the job has been allocated resources"
cd ${FIA}/html_state
rm *.txt
for i in *.html; do
echo $i | grep -Po '([A-Z]{2})' >> ./state.txt
echo $i | grep -Po '(?<=_)\d{4}(?=_)' >> ./year.txt
echo $i | grep -Po '^\d*(?=_)' >> ./attribute.txt
cat $i | grep -A 1 'nowrap="nowrap">Total</th>' | tr -d , | grep -Po '\d*' >> ./total_state.txt
done

paste -d _ attribute.txt year.txt > ./attribute_year.txt
paste -d , state.txt year.txt attribute_year.txt total_state.txt > ${FIA}/att_level_state.csv

cd ${PROJ_HOME}
""")
f.close()

## Send the job file to run
os.system("""
sleep 5
if [ %d -gt 1 ]; then
JOBID=$(cat ${PROJ_HOME}/jobid-state.log | tr '\n' ',' | grep -Po '.*(?=,)')
JID=$(sbatch --parsable --dependency=afterok:$(echo ${JOBID}) ${FIA}/job-state-html.sh)
echo ${JID} > ${PROJ_HOME}/jobid-state.log
else . ${FIA}/job-state-html.sh
fi
""" % (maxj,))

## Create a dictionary of FIA attribute levels for each state
f = open('./job_state.py','w')
f.write("""#!/usr/bin/env python

import re
import sys
import csv
import json
import prep_data
import collections

config = json.load(open(sys.argv[1]))
with open('./fia_data/att_level_state.csv', 'r') as att:
    att_data_state = att.readlines()

state_keys = ['state']
lk = len(state_keys)
for x in config['attribute_cd']:
    state_keys.extend(['%s_%s' %s (x,y) for y in config['year']])

## Convert CSV to ListDict with RE
pattern = re.compile('(.*)[,]' * 3 + '(.*)')
state_data_dict = collections.defaultdict(dict)
for a in att_data_state:
    a = a[:-1]
    t = pattern.search(a)
    if t is None:
        continue
    state_data_dict[t.group(1)].update({'state': t.group(1), t.group(3): t.group(4)})

state_data = []
for d in state_data_dict:
    state_data.append(state_data_dict[d])

## JSON output
with open('./outputs/state-%s.json', 'w') as fj:
    json.dump(state_data, fj)

## Panel CSV output
with open('./outputs/state-%s.csv', 'w') as fp:
    prep_data.list_dict_panel(state_data,state_keys[:lk],config,fp)

""" % ('%s','%s','%',time,time))
f.close()
    
## Create a job file
f = open('./job_state.sh','w')
f.write("""#!/bin/bash

#SBATCH --job-name=Output
#SBATCH --mem=4G

python3 job_state.py config.json
""")
f.close()

## Send the last job to run
os.system("""
sleep 5
if [ %d -gt 1 ]; then
JID=$(sbatch --parsable --dependency=afterok:$(cat ${PROJ_HOME}/jobid-state.log) ${PROJ_HOME}/job_state.sh)
echo ${JID} > ${PROJ_HOME}/jobid-state.log
else . ${PROJ_HOME}/job_state.sh
fi
sleep 5
""" % (maxj,))
