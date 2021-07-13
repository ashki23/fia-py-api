#!/bin/bash

export PROJ_HOME=${PWD}
export OUTPUT=${PWD}/outputs
export FIA=${PWD}/fia_data
install -dvp ${OUTPUT}
install -dvp ${FIA}/survey
MYHOME=${HOME}

## Install Miniconda
if [ ! -d miniconda ]; then
source bootstrap.sh
fi

## Initiate conda
export HOME=${PROJ_HOME}
export PATH=${PROJ_HOME}/miniconda/bin/:${PATH}
source ./miniconda/etc/profile.d/conda.sh

## Deactivat active envs
conda deactivate

echo ============ Create Conda envs ============= $(hostname) $(date)

## Create local environments
if [ ! -d api_py_env ]; then
## Including: python jq shapely fiona
conda create --yes --prefix ./api_py_env --file ./api_py_env.txt
conda activate ./api_py_env
pip install geocoder
fi

## Activate Python environment
conda activate ./api_py_env
export HOME=${MYHOME}
