#!/bin/bash

#SBATCH --job-name=API_Biomass
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G

echo ============= Checking configs ============

if ! jq ."query_type" config.json | grep -Pq "state|county|coordinate"; then
echo "Please select one of the available methods for 'query_type'. Available methods are 'state', 'county', 'coordinate'."
return; fi

if jq ."query_type" config.json | grep -q "coordinate"; then
if [ ! -f ./coordinate.csv ]; then
echo "Please place the coordinates in "$PWD". Acceptable file format is CSV and it should be named 'coordinate.csv'."
return; fi
fi

echo ============= Local environments ============ $(hostname) $(date)

source environment.sh

echo ================ Preparations =============== $(hostname) $(date)

source download.sh
python prep_data.py config.json || return

echo ================ HTML queries =============== $(hostname) $(date)

## Download level of forest attributes from online source (EVALIDator)
if jq ."query_type" config.json | grep -q "coordinate"; then
python fia_coordinate.py config.json attributes.json coordinate.json || return; fi

if jq ."query_type" config.json | grep -q "county"; then
python fia_county.py config.json attributes.json || return; fi

if jq ."query_type" config.json | grep -q "state"; then
python fia_state.py config.json attributes.json || return; fi
