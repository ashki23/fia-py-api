#!/bin/bash

read -p "Select the query type (i.e. state, county or coordinate): " query_name
if [ $query_name = "state" ] || [ $query_name = "county" ]; then query_name="state-county"; echo $query_name
elif [ $query_name = "coordinate" ]; then echo $query_name
else echo "Error: Select state, county or coordinate."; return
fi

## Set env variables
source environment.sh
echo ============ Rebatch time: $(date +"%Y-%m-%d-%H:%M")

## Remove invalid json files
for w in $(cat ${PROJ_HOME}/job-out-${query_name}/warning.txt | grep input | grep -Po "(?<=Warning: ).*(?= is)"); do
rm $w
done

## For serial mode
if [ -z $(which sbatch) ]; then
. ${FIA}/job-${query_name}-0.sh
. ${PROJ_HOME}/job-${query_name}.sh
return
fi

## Remove the old log file
rm ${PROJ_HOME}/jobid-${query_name}-rebatch.log

## For parallel: resubmit the failed jobs
for j in $(cat ${PROJ_HOME}/job-out-${query_name}/failed.txt); do
JID=$(sbatch --parsable ${FIA}/job-${j}.sh)
echo ${JID} >> ${PROJ_HOME}/jobid-${query_name}-rebatch.log
done

## Re-extract informations from JSON files
sleep 3
JOBID=$(cat ${PROJ_HOME}/jobid-${query_name}-rebatch.log | tr '\n' ',' | grep -Po '.*(?=,$)')
sbatch --dependency=afterok:$(echo ${JOBID}) ${PROJ_HOME}/job-${query_name}.sh
