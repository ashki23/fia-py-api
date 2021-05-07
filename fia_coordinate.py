#!/usr/bin/env python

import os
import sys
import time
import json

## Time
time = time.strftime("%Y%m%d-%H%M%S")

## Open JSON inputs
config = json.load(open(sys.argv[1]))
attribute = json.load(open(sys.argv[2]))
coordinate_data = json.load(open(sys.argv[3]))

## Archive previous HTMLs and remove log files
os.system("""
install -dvp ${FIA}/html_coordinate/archived-%s
mv ${FIA}/html_coordinate/*.html ${FIA}/html_coordinate/archived-%s
rm ${FIA}/job-coordinate-*
rm ${PROJ_HOME}/jobid-coordinate.log
""" % (time,time))

## Calculating optimal job size
maxj = config['job_number_max'] # Maximun number of job that is allowed 
## opt_job_size find maximum size of each job to satisfy max job num (maxj) 
opt_job_size = (len(config['attribute_cd']) * len(config['year']) * len(coordinate_data)) / maxj
job_size = max(25, round(opt_job_size) + 1)

## Create job files to download FIA HTML queries
for att_cd in config['attribute_cd']:
    att = attribute['%d' % att_cd]
    for year in config['year']:
        i = 0
        nrow = len(coordinate_data)
        while i < nrow:
            print('*************', att, year, 'row: ', i, '-', i+job_size, '***************')
            f = open('./fia_data/job-coordinate-%s-%s-%s.sh' % (att_cd,year,i),'w')
            f.write("""#!/bin/bash

#SBATCH --job-name=FIAQuery
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --partition=%s
#SBATCH --time=08:00:00
            """ % config['partition'])
            for l in coordinate_data[i:i+job_size]: # Select based on job_size
                states_cd = [l['state_cd']] + l['neighbors_cd']
                itr = 0
                n = 4 # means searching 4 years before and 2 years after the desired year to find closest data if the desired year not available 
                while itr <= n:
                    if itr == 0:
                        #print(year)
                        cd_yr = ['%s%s' % (x,year) for x in states_cd]
                        f.write("""
wget -c --tries=2 --random-wait "https://apps.fs.usda.gov/Evalidator/rest/Evalidator/fullreport?reptype=Circle&lat=%s&lon=%s&radius=%s&snum=%s&sdenom=No denominator - just produce estimates&wc=%s&pselected=None&rselected=All live stocking&cselected=All live stocking&ptime=Current&rtime=Current&ctime=Current&wf=&wnum=&wnumdenom=&FIAorRPA=FIADEF&outputFormat=HTML&estOnly=Y&schemaName=FS_FIADB." -O ${FIA}/html_coordinate/%s_%s_id%s_%s.html
                        """ % ( l['lat'],l['lon'],l['radius'],att,','.join(cd_yr),att_cd,year,l['unit_id'],year))
                    elif itr % 2 != 0:
                        yl = year - int(itr/2) - 1
                        #print(yl)
                        cd_yr = ['%s%s' % (x,yl) for x in states_cd]
                        f.write("""
if [ -f ${FIA}/html_coordinate/%s_%s_id%s_%s.html ]; then
if [ $(cat ${FIA}/html_coordinate/%s_%s_id%s_%s.html | grep -c %s%s) -le 1 ] || [ $(cat ${FIA}/html_coordinate/%s_%s_id%s_%s.html | grep -c '>Total<') -le 1 ]; then
rm ${FIA}/html_coordinate/%s_%s_id%s_%s.html
wget -c --tries=2 --random-wait "https://apps.fs.usda.gov/Evalidator/rest/Evalidator/fullreport?reptype=Circle&lat=%s&lon=%s&radius=%s&snum=%s&sdenom=No denominator - just produce estimates&wc=%s&pselected=None&rselected=All live stocking&cselected=All live stocking&ptime=Current&rtime=Current&ctime=Current&wf=&wnum=&wnumdenom=&FIAorRPA=FIADEF&outputFormat=HTML&estOnly=Y&schemaName=FS_FIADB." -O ${FIA}/html_coordinate/%s_%s_id%s_%s.html
fi
fi
                        """ % (att_cd,year,l['unit_id'],yl+itr, att_cd,year,l['unit_id'],yl+itr, l['state_cd'],yl+itr, att_cd,year,l['unit_id'],yl+itr, att_cd,year,l['unit_id'],yl+itr, l['lat'],l['lon'],l['radius'],att,','.join(cd_yr), att_cd,year,l['unit_id'],yl))
                    else:
                        yh = year + int(itr/2)
                        #print(yh)
                        cd_yr = ['%s%s' % (x,yh) for x in states_cd]
                        f.write("""
if [ -f ${FIA}/html_coordinate/%s_%s_id%s_%s.html ]; then
if [ $(cat ${FIA}/html_coordinate/%s_%s_id%s_%s.html | grep -c %s%s) -le 1 ] || [ $(cat ${FIA}/html_coordinate/%s_%s_id%s_%s.html | grep -c '>Total<') -le 1 ]; then
rm ${FIA}/html_coordinate/%s_%s_id%s_%s.html
wget -c --tries=2 --random-wait "https://apps.fs.usda.gov/Evalidator/rest/Evalidator/fullreport?reptype=Circle&lat=%s&lon=%s&radius=%s&snum=%s&sdenom=No denominator - just produce estimates&wc=%s&pselected=None&rselected=All live stocking&cselected=All live stocking&ptime=Current&rtime=Current&ctime=Current&wf=&wnum=&wnumdenom=&FIAorRPA=FIADEF&outputFormat=HTML&estOnly=Y&schemaName=FS_FIADB." -O ${FIA}/html_coordinate/%s_%s_id%s_%s.html
fi
fi
                        """ % (att_cd,year,l['unit_id'],yh-itr, att_cd,year,l['unit_id'],yh-itr, l['state_cd'],yh-itr, att_cd,year,l['unit_id'],yh-itr, att_cd,year,l['unit_id'],yh-itr, l['lat'],l['lon'],l['radius'],att,','.join(cd_yr), att_cd,year,l['unit_id'],yh))
                    itr += 1
                
                f.write("""
if [ -f ${FIA}/html_coordinate/%s_%s_id%s_%s.html ]; then
if [ $(cat ${FIA}/html_coordinate/%s_%s_id%s_%s.html | grep -c %s%s) -le 1 ] || [ $(cat ${FIA}/html_coordinate/%s_%s_id%s_%s.html | grep -c '>Total<') -le 1 ]; then
rm ${FIA}/html_coordinate/%s_%s_id%s_%s.html
echo "ERROR: the FIA dataset does not include records for %s for state %s between %s-%s"
fi
fi

if [ -f ${FIA}/html_coordinate/%s_%s_id%s_%s.html ]; then
if [ $(cat ${FIA}/html_coordinate/%s_%s_id%s_%s.html | grep -c %s%s) -le 1 ] || [ $(cat ${FIA}/html_coordinate/%s_%s_id%s_%s.html | grep -c '>Total<') -le 1 ]; then
rm ${FIA}/html_coordinate/%s_%s_id%s_%s.html
echo "ERROR: the FIA dataset does not include records for %s for state %s between %s-%s"
fi
fi
                """ % (att_cd,year,l['unit_id'],yl, att_cd,year,l['unit_id'],yl, l['state_cd'],yl, att_cd,year,l['unit_id'],yl, att_cd,year,l['unit_id'],yl, att,l['state_cd'],yl,yh, att_cd,year,l['unit_id'],yh, att_cd,year,l['unit_id'],yh, l['state_cd'],yh, att_cd,year,l['unit_id'],yh, att_cd,year,l['unit_id'],yh, att,l['state_cd'],yl,yh))
            f.close()
            
            ## Send the job file to run
            os.system("""
if [ %d -gt 1 ]; then
JID=$(sbatch --parsable ${FIA}/job-coordinate-%s-%s-%s.sh)
echo ${JID} >> ${PROJ_HOME}/jobid-coordinate.log
else . ${FIA}/job-coordinate-%s-%s-%s.sh >> ${PROJ_HOME}/serial_download_log.out
fi
        """ % (maxj, att_cd,year,i, att_cd,year,i))
            i += job_size

## Create a Bash file to extract level of attributes from FIA HTML
print('************* Obtain level of attributes ***************')
f = open('./fia_data/job-coordinate-html.sh','w')
f.write("""#!/bin/bash

#SBATCH --job-name=Extract
#SBATCH --mem=16G

echo "success: the job has been allocated resources"
cd ${FIA}/html_coordinate
rm *.txt
for i in *.html; do
echo $i | grep -Po '^\d*(?=_)' >> ./att.txt
echo $i | grep -Po '(?<=id)\d*(?=_)' >> ./unit_id.txt
echo $i | grep -Po '(?<=_)\d{4}(?=_.)' >> ./year.txt
cat $i | grep -A 1 'nowrap="nowrap">Total</th>' | tr -d , | grep -Po '\d*' >> ./att_total.txt 
done

paste -d _ att.txt year.txt > ./att_year.txt
paste -d , unit_id.txt year.txt att_year.txt att_total.txt > ${FIA}/att_level_coordinate.csv

cd ${PROJ_HOME}
""")
f.close()

## Send the job file to run
os.system("""
sleep 5
if [ %d -gt 1 ]; then
JOBID=$(cat ${PROJ_HOME}/jobid-coordinate.log | tr '\n' ',' | grep -Po '.*(?=,)')
JID=$(sbatch --parsable --dependency=afterok:$(echo ${JOBID}) ${FIA}/job-coordinate-html.sh)
echo ${JID} > ${PROJ_HOME}/jobid-coordinate.log
else . ${FIA}/job-coordinate-html.sh
fi
""" % (maxj,))

## Create a dictionary of FIA attribute levels for each coordinate
f = open('./job_coordinate.py','w')
f.write("""#!/usr/bin/env python

import re
import sys
import csv
import json
import prep_data
import collections

config = json.load(open(sys.argv[1]))
coordinate_data = json.load(open(sys.argv[2]))

keys = list(coordinate_data[0].keys())
lk = len(keys)
for x in config['attribute_cd']:
    keys.extend(['%s_%s' %s (x,y) for y in config['year']])

## Read level of attributes from CSV
with open('./fia_data/att_level_coordinate.csv', 'r') as att:
    att_data = att.readlines()

## Convert CSV to ListDict with RE
pattern = re.compile('(.*)[,]'*3 + '(.*)')
att_coordinate = collections.defaultdict(dict)
for a in att_data:
    a = a[:-1]
    t = pattern.search(a)
    if t is None:
        continue
    att_coordinate[t.group(1)].update({t.group(3): t.group(4)})

## Add the attribite levels to coordinate_data
for l in coordinate_data:
    levels = att_coordinate[l['unit_id']]
    for k in levels.keys():
        l[k] = levels[k]

## JSON output
with open('./outputs/coordinate-%s.json', 'w') as fj:
    json.dump(coordinate_data, fj)

## Panel CSV output
with open('./outputs/panel-coordinate-%s.csv', 'w') as fp:
    prep_data.list_dict_panel(coordinate_data,keys[:lk],config,fp)

""" % ('%s','%s','%',time,time))
f.close()

## Create a job file
f = open('./job_coordinate.sh','w')
f.write("""#!/bin/bash

#SBATCH --job-name=Output
#SBATCH --mem=8G

python3 job_coordinate.py config.json coordinate_data.json
""")
f.close()

## Send the last job to run
os.system("""
sleep 5
if [ %d -gt 1 ]; then
JID=$(sbatch --parsable --dependency=afterok:$(cat ${PROJ_HOME}/jobid-coordinate.log) ${PROJ_HOME}/job_coordinate.sh)
echo ${JID} > ${PROJ_HOME}/jobid-coordinate.log
else . ${PROJ_HOME}/job_coordinate.sh
fi
sleep 5
""" % (maxj,))
