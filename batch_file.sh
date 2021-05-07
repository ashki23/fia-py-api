#!/bin/bash

#SBATCH --job-name=API_Biomass
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G

echo ============= Local environments ============ $(hostname) $(date)

source environment.sh

echo ================ Preparations =============== $(hostname) $(date)

source download.sh
python3 prep_data.py config.json

echo ================ HTML queries =============== $(hostname) $(date)

## Download level of forest attributes from online source (EVALIDator)
if jq ."query_type" config.json | grep -q "state"; then
    python3 fia_state.py config.json attributes.json
elif jq ."query_type" config.json | grep -q "county"; then
    python3 fia_county.py config.json attributes.json
elif jq ."query_type" config.json | grep -q "coordinate"; then
    if [ -f coordinates.json ]; then
	python3 fia_coordinate.py config.json attributes.json coordinates.json
    else
	"Please add place coordinates in this directrory. Acceptable file format is CSV and the file name should be 'coordinates.csv'"
else
    echo "Please select one of the available methods for 'query_type'. Available methods are 'state', 'county', 'coordinate'"
fi
