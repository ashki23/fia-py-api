#!/bin/bash

read -p "Select the query type (i.e. state, county, or coordinate): " query_name

## Set env variables 
source environment.sh
echo ============ Rebatch time: $(date +"%Y-%m-%d-%H:%M")

## Resubmit the failed jobs
for j in $(cat ${PROJ_HOME}/job_out_${query_name}/failed.txt); do
JID=$(sbatch --parseable ${FIA}/job-${j}.sh)
echo ${JID} >> ${PROJ_HOME}/jobid-${query_name}-rebatch.log
done

## Re-extract informations from JSON files
sleep 3
JOBID=$(cat ${PROJ_HOME}/jobid-${query_name}-rebatch.log | tr '\n' ',' | grep -Po '.*(?=,$)')
sbatch --parsable --dependency=afterok:$(echo ${JOBID}) ${PROJ_HOME}/job_${query_name}.sh
