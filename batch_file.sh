#!/bin/bash

#SBATCH --job-name=API_Biomass
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G

echo ============= Local environments ============ $(hostname) $(date)

source environment.sh

echo ================ Preparations =============== $(hostname) $(date)

source download.sh
python3 prep_data.py config.json || return;

echo ================ HTML queries =============== $(hostname) $(date)

## Download level of forest attributes from online source (EVALIDator)
if jq ."query_type" config.json | grep -q "state"; then
    python3 fia_state.py config.json attributes.json
fi

if jq ."query_type" config.json | grep -q "county"; then
    python3 fia_county.py config.json attributes.json
fi

if jq ."query_type" config.json | grep -q "coordinate"; then
    if [ -f coordinates.json ]; then
	python3 fia_coordinate.py config.json attributes.json coordinates.json
    else
	echo "Please place the coordinates in "$PWD". Acceptable file format is CSV and the file name should be 'coordinates.csv.'"
    fi
fi

if ! jq ."query_type" config.json | grep -Pq "state|county|coordinate"; then
    echo "Please select one of the available methods for 'query_type'. Available methods are 'state', 'county', 'coordinate'."
fi
