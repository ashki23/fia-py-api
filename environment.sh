#!/bin/bash

#SBATCH --job-name=Environment
#SBATCH --cpus-per-task=10
#SBATCH --mem=60G

export PROJ_HOME=${PWD}
export OUTPUT=${PWD}/outputs
export OTHER=${PWD}/other_data
export FIA=${PWD}/fia_sqlite
install -dvp ${OUTPUT}
install -dvp ${OTHER}
install -dvp ${FIA}/html_county

## Load Miniconda
if [ -d spack ]; then
source bootstrap.sh; else 
export CONDA_PKGS_DIRS=~/.conda/pkgs
export CONDA_ENVS_PATH=~/.conda/envs
module load miniconda3
fi

## Uncomment the following to remove the local virtual env
# source deactivate
# conda clean --yes --all
# conda env remove --yes --prefix ./app_py_env
# rm -r ./app_py_env

echo -------------------------------------------- $(hostname) $(date)

## Create local environment
if [ ! -d app_py_env ]; then
conda create --yes --prefix ./app_py_env python xlrd shapely fiona
fi

## Activate and update the local env
source activate ./app_py_env
conda update --yes python xlrd shapely fiona

echo -------------------------------------------- $(hostname) $(date)
