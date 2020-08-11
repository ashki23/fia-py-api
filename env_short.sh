#!/bin/bash

export PROJ_HOME=${PWD}
export OUTPUT=${PWD}/outputs
export OTHER=${PWD}/other_data
export FIA=${PWD}/fia_sqlite
install -dvp ${OUTPUT}
install -dvp ${FIA}/html_state
install -dvp ${FIA}/html_county

## Load Miniconda
if [ -d spack ]; then
source bootstrap.sh; else 
module load miniconda3
fi

## Activate local env
source activate ./app_py_env
