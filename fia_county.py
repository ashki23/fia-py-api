#!/usr/bin/env python

import os
import sys
import csv
import time
import json
import prep_data

## Time
time = time.strftime("%Y%h%d-%H%M")

## Open JSON inputs
config = json.load(open(sys.argv[1]))
attribute = json.load(open(sys.argv[2]))
year = config['year'][0]
maxj = int(config['job_number_max'])

## Create a dictionary of FIA state codes
with open('./state_codes.csv', 'r') as cd:
    state_cd = prep_data.csv_dict(cd)
    
## Select states from the config
state_cd = prep_data.state_config(state_cd,config)
            
## Archive old HTMLs
os.system("""
install -dvp ${FIA}/html_county/archived-%s
mv ${FIA}/html_county/*.html ${FIA}/html_county/archived-%s
rm ${FIA}/job-county-*
rm ${PROJ_HOME}/jobid-county.log
rm ${PROJ_HOME}/serial_download_log.out
""" % (time,time))

## Create job files to download FIA HTML queries
for i in state_cd:
    print('************* state:', i, '***************')
    f = open('./fia_data/job-county-%s.sh' % (i,),'w')
    f.write("""#!/bin/bash

#SBATCH --job-name=Download
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --partition=%s
#SBATCH --time=04:00:00
    """ % config['partition'])
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
wget -c --tries=2 --random-wait "https://apps.fs.usda.gov/Evalidator/rest/Evalidator/fullreport?reptype=State&lat=0&lon=0&radius=0&snum=%s&sdenom=No denominator - just produce estimates&wc=%s&pselected=None&rselected=County code and name&cselected=All live stocking&ptime=Current&rtime=Current&ctime=Current&wf=&wnum=&wnumdenom=&FIAorRPA=FIADEF&outputFormat=HTML&estOnly=Y&schemaName=FS_FIADB." -O ${FIA}/html_county/%s_%s_%s_%s.html
                    """ % (year,i,att,att,','.join(cd_yr),att_cd,year,i,year))
                elif itr % 2 != 0:
                    yl = year - int(itr/2) - 1
                    #print(yl)
                    cd_yr = ['%s%s' % (x,yl) for x in [state_cd[i]]]
                    f.write("""
if [ -f ${FIA}/html_county/%s_%s_%s_%s.html ]; then
if [ $(cat ${FIA}/html_county/%s_%s_%s_%s.html | grep -c %s%s) -le 1 ] || [ $(cat ${FIA}/html_county/%s_%s_%s_%s.html | grep -c '>Total<') -le 1 ]; then
rm ${FIA}/html_county/%s_%s_%s_%s.html
wget -c --tries=2 --random-wait "https://apps.fs.usda.gov/Evalidator/rest/Evalidator/fullreport?reptype=State&lat=0&lon=0&radius=0&snum=%s&sdenom=No denominator - just produce estimates&wc=%s&pselected=None&rselected=County code and name&cselected=All live stocking&ptime=Current&rtime=Current&ctime=Current&wf=&wnum=&wnumdenom=&FIAorRPA=FIADEF&outputFormat=HTML&estOnly=Y&schemaName=FS_FIADB." -O ${FIA}/html_county/%s_%s_%s_%s.html
fi
fi
                    """ % (att_cd,year,i,yl+itr, att_cd,year,i,yl+itr, state_cd[i],yl+itr, att_cd,year,i,yl+itr, att_cd,year,i,yl+itr, att,','.join(cd_yr), att_cd,year,i,yl))
                else:
                    yh = year + int(itr/2)
                    #print(yh)
                    cd_yr = ['%s%s' % (x,yh) for x in [state_cd[i]]]
                    f.write("""
if [ -f ${FIA}/html_county/%s_%s_%s_%s.html ]; then
if [ $(cat ${FIA}/html_county/%s_%s_%s_%s.html | grep -c %s%s) -le 1 ] || [ $(cat ${FIA}/html_county/%s_%s_%s_%s.html | grep -c '>Total<') -le 1 ]; then
rm ${FIA}/html_county/%s_%s_%s_%s.html
wget -c --tries=2 --random-wait "https://apps.fs.usda.gov/Evalidator/rest/Evalidator/fullreport?reptype=State&lat=0&lon=0&radius=0&snum=%s&sdenom=No denominator - just produce estimates&wc=%s&pselected=None&rselected=County code and name&cselected=All live stocking&ptime=Current&rtime=Current&ctime=Current&wf=&wnum=&wnumdenom=&FIAorRPA=FIADEF&outputFormat=HTML&estOnly=Y&schemaName=FS_FIADB." -O ${FIA}/html_county/%s_%s_%s_%s.html
fi
fi
                    """ % (att_cd,year,i,yh-itr, att_cd,year,i,yh-itr, state_cd[i],yh-itr, att_cd,year,i,yh-itr, att_cd,year,i,yh-itr, att,','.join(cd_yr), att_cd,year,i,yh))
                itr += 1
            f.write("""
if [ -f ${FIA}/html_county/%s_%s_%s_%s.html ]; then
if [ $(cat ${FIA}/html_county/%s_%s_%s_%s.html | grep -c %s%s) -le 1 ] || [ $(cat ${FIA}/html_county/%s_%s_%s_%s.html | grep -c '>Total<') -le 1 ]; then
rm ${FIA}/html_county/%s_%s_%s_%s.html
echo "ERROR: the FIA dataset does not include records for %s for state %s between %s-%s"
fi
fi

if [ -f ${FIA}/html_county/%s_%s_%s_%s.html ]; then
if [ $(cat ${FIA}/html_county/%s_%s_%s_%s.html | grep -c %s%s) -le 1 ] || [ $(cat ${FIA}/html_county/%s_%s_%s_%s.html | grep -c '>Total<') -le 1 ]; then
rm ${FIA}/html_county/%s_%s_%s_%s.html
echo "ERROR: the FIA dataset does not include records for %s for state %s between %s-%s"
fi
fi
            """ % (att_cd,year,i,yl, att_cd,year,i,yl, state_cd[i],yl, att_cd,year,i,yl, att_cd,year,i,yl, att,i,yl,yh, att_cd,year,i,yh, att_cd,year,i,yh, state_cd[i],yh, att_cd,year,i,yh, att_cd,year,i,yh, att,i,yl,yh))
    f.close()
    
    ## Send the job file to run
    os.system("""
if [ %d -gt 1 ]; then
JID=$(sbatch --parsable ${FIA}/job-county-%s.sh)
echo ${JID} >> ${PROJ_HOME}/jobid-county.log 
else . ${FIA}/job-county-%s.sh >> ${PROJ_HOME}/serial_download_log.out
fi
    """ % (maxj,i,i))

## Create a Bash file to extract level of attributes from FIA HTML files
print('************* Obtain level of attributes ***************')
f = open('./fia_data/job-county-html.sh','w')
f.write("""#!/bin/bash

#SBATCH --job-name=Extract
#SBATCH --mem=4G

echo "success: the job has been allocated resources"
cd ${FIA}/html_county
rm *.txt
for i in *.html; do
cat $i | grep -Po '(?<=nowrap="nowrap">)\d.*(?=<)' | grep -Po '([0-9]*)' >> ./county_cd.txt
cat $i | grep -Po '(?<=nowrap="nowrap">)\d.*(?=<)' | grep -Po '([A-Z][a-z].*)' >> ./county.txt
cat $i | grep -Po '(?<=nowrap="nowrap">)\d.*(?=<)' | grep -Po '([A-Z]{2})' >> ./state.txt
cat $i | grep -P -A 1 '(?<=nowrap="nowrap">)\d.*(?=<)' | grep -Po '(?<=align="right">).*(?=<)' | tr -d , >> ./total.txt
for l in $(cat $i | grep -Po '(?<=nowrap="nowrap">)\d.*(?=<)' | grep -Po '([0-9]*)'); do echo $i | grep -Po '^\d*(?=_)'; done >> ./att.txt
for l in $(cat $i | grep -Po '(?<=nowrap="nowrap">)\d.*(?=<)' | grep -Po '([0-9]*)'); do echo $i | grep -Po '(?<=_)\d{4}(?=_)'; done >> ./yr.txt
done

paste -d _ att.txt yr.txt > ./att_yr.txt
paste -d , county_cd.txt county.txt state.txt yr.txt att_yr.txt total.txt > ${FIA}/att_level_county.csv

cd ${PROJ_HOME}
""")
f.close()

## Send the job file to run
os.system("""
sleep 10
if [ %d -gt 1 ]; then
JOBID=$(cat ${PROJ_HOME}/jobid-county.log | tr '\n' ',' | grep -Po '.*(?=,)')
JID=$(sbatch --parsable --dependency=afterok:$(echo ${JOBID}) ${FIA}/job-county-html.sh)
echo ${JID} > ${PROJ_HOME}/jobid-county.log
else . ${FIA}/job-county-html.sh
fi
""" % (maxj,))

## Create a dictionary of FIA attribute levels for each county
f = open('./job_county.py','w')
f.write("""#!/usr/bin/env python

import re
import sys
import csv
import json
import prep_data
import collections

config = json.load(open(sys.argv[1]))
with open('./fia_data/att_level_county.csv', 'r') as att:
    att_data_county = att.readlines()

## Convert CSV to ListDict with RE
pattern = re.compile('(.*)[,]' * 5 + '(.*)')
county_data_dict = collections.defaultdict(dict)
for a in att_data_county:
    a = a[:-1]
    t = pattern.search(a)
    if t is None:
        continue
    county_data_dict[t.group(1)].update({'year': config['year'][0], 'county_cd': t.group(1), 'county': t.group(2), 'state': t.group(3), t.group(5): t.group(6)})

county_data = []
for d in county_data_dict:
    county_data.append(county_data_dict[d])

## JSON output
with open('./outputs/county-%s-%s.json', 'w') as fj:
    json.dump(county_data, fj)

## CSV output
county_keys = ['county_cd','county','state','year'] + ['%s_%s' %s (x,config['year'][0]) for x in config['attribute_cd']]
with open('./outputs/county-%s-%s.csv', 'w') as fc:
    prep_data.list_dict_csv(county_data,county_keys,fc)
    
""" % (year,time,'%s','%s','%',year,time))
f.close()
    
## Create a job file
f = open('./job_county.sh','w')
f.write("""#!/bin/bash

#SBATCH --job-name=Output
#SBATCH --mem=2G

python3 job_county.py config.json
""")
f.close()

## Send the last job to run
os.system("""
sleep 5
if [ %d -gt 1 ]; then
JID=$(sbatch --parsable --dependency=afterok:$(cat ${PROJ_HOME}/jobid-county.log) ${PROJ_HOME}/job_county.sh)
echo ${JID} > ${PROJ_HOME}/jobid-county.log
else . ${PROJ_HOME}/job_county.sh
fi
sleep 5
""" % (maxj,))
