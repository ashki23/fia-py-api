#!/bin/bash

read -p 'Select the query type (insert one of state, county or coordinate): ' query_name

## Set env variables 
source environment.sh

## Remove log
rm ${PROJ_HOME}/jobid-failed-${query_name}.log

## Collecting failed job's name in the last 24 hours
sacct -XP --state F --noheader --starttime $(date --date='day ago' +"%Y-%m-%d") --format JobName | grep "${query_name}" > ${PROJ_HOME}/failed-jobs-${query_name}.txt

## Resubmit the failed jobs
for j in `cat ${PROJ_HOME}/failed-jobs-${query_name}.txt`; do 
JID=$(sbatch --parseable ${FIA}/job-${j}.sh)
echo ${JID} >> ${PROJ_HOME}/jobid-failed-${query_name}.log
done

## Re-extract informations from HTML files
sleep 3
JOBID=$(cat ${PROJ_HOME}/jobid-failed-${query_name}.log | tr '\n' ',' | grep -Po '.*(?=,)')
JID=$(sbatch --parsable --dependency=afterok:$(echo ${JOBID}) ${FIA}/job-${query_name}-html.sh)
echo ${JID} > ${PROJ_HOME}/jobid-failed-${query_name}.log

## Re-generate the outputs
sleep 3
JID=$(sbatch --parsable --dependency=afterok:$(cat ${PROJ_HOME}/jobid-${query_name}.log) ${PROJ_HOME}/job_${query_name}.sh)
