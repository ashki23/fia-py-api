#!/bin/bash

#SBATCH --job-name=API_Runner
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G

echo ============ Local environments ============ $(hostname) $(date)

source environment.sh

if [ `jq ."job_number_max" config.json` -gt 1 ]; then
if [ -z $(which sbatch) ]; then
echo "'sbatch' command not found. In 'config.json' assign 1 for 'job_number_max' to proceed in serial."; return; fi
fi

echo =============== Preparations =============== $(hostname) $(date)

source download.sh
python prep_data.py config.json || return

echo =============== HTML queries =============== $(hostname) $(date)

## Download level of forest attributes from online source (EVALIDator)
if jq ."query_type" config.json | grep -q "coordinate"; then
python fia_coordinate.py config.json attributes.json coordinate.json || return; fi

if jq ."query_type" config.json | grep -Pq "county|state"; then
python fia_county.py config.json attributes.json || return; fi
