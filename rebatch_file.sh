#!/bin/bash

read -p "Select the query type (i.e. state, county or coordinate): " query_name
if [ $query_name = "state" ] || [ $query_name = "county" ]; then query_name="state-county"; echo $query_name
elif [ $query_name = "coordinate" ]; then echo $query_name
else echo "Error: Select state, county or coordinate."; return
fi

## Set env variables
source environment.sh
rbtim=$(date +"%Y-%m-%d-%H:%M")
dtim=$(date +"%Y%m-%d%H%M")
echo ============ Rebatch time: ${rbtim}
install -dvp  ${PROJ_HOME}/job-out-${query_name}/archive/rebatch-${dtim}

## Remove invalid json files
for w in $(cat ${PROJ_HOME}/job-out-${query_name}/warning.txt | grep input | grep -Po "(?<=Warning: ).*(?= is)"); do
rm -v $w
done

## For serial mode
if [ -z $(which sbatch) ]; then
mv ${PROJ_HOME}/job-out-${query_name}/*.* ${PROJ_HOME}/job-out-${query_name}/archive/rebatch-${dtim}
. ${FIA}/job-${query_name}-0.sh > ${PROJ_HOME}/job-out-${query_name}/${query_name}-0.out
. ${PROJ_HOME}/job-${query_name}.sh > ${PROJ_HOME}/job-out-${query_name}/output-serial.out
. ${PROJ_HOME}/report-${query_name}.sh > ${PROJ_HOME}/report-${query_name}-serial-rebatch.out
return
fi

## Remove the old log file
rm ${PROJ_HOME}/jobid-${query_name}-rebatch.log

## For parallel: resubmit the failed jobs
for j in $(cat ${PROJ_HOME}/job-out-${query_name}/failed.txt); do
mv ${PROJ_HOME}/job-out-${query_name}/${j}-*.out ${PROJ_HOME}/job-out-${query_name}/archive/rebatch-${dtim}
JID=$(sbatch --parsable ${FIA}/job-${j}.sh)
echo ${JID} >> ${PROJ_HOME}/jobid-${query_name}-rebatch.log
done

## Re-extract informations from JSON files
sleep 2
JOBID=$(cat ${PROJ_HOME}/jobid-${query_name}-rebatch.log | tr '\n' ',' | grep -Po '.*(?=,$)')
JID=$(sbatch --parsable --dependency=afterok:$(echo ${JOBID}) ${PROJ_HOME}/job-${query_name}.sh)

## A new report
sleep 2
mv ${PROJ_HOME}/job-out-${query_name}/failed.txt ${PROJ_HOME}/job-out-${query_name}/archive/rebatch-${dtim}
mv ${PROJ_HOME}/job-out-${query_name}/warning.txt ${PROJ_HOME}/job-out-${query_name}/archive/rebatch-${dtim}
mv ${PROJ_HOME}/job-out-${query_name}/output-*.out ${PROJ_HOME}/job-out-${query_name}/archive/rebatch-${dtim}
sbatch --dependency=afterany:$(echo ${JID}) ${PROJ_HOME}/report-${query_name}.sh ${rbtim}
