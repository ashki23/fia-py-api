#!/bin/bash

#SBATCH --job-name=WoodyBiomass
#SBATCH --cpus-per-task=10
#SBATCH --mem=60G

echo ============= Install Miniconda ============ $(hostname) $(date)

## Uncomment the following if miniconda3 is not installed or you want to install it locally
# source bootstrap.sh

echo ============= Local environment ============ $(hostname) $(date)

source environment.sh

echo ============ Download databases ============ $(hostname) $(date)

source other_download.sh
srun python3 prep_data.py config.json

echo =============== HTML queries =============== $(hostname) $(date)

## Download level of forest attributes from online source (EVALIDator)
srun python3 fia_county_html.py config.json attributes.json pellet_data.json

