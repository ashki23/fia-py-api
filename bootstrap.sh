#!/bin/bash

echo ============= Install Miniconda ============ $(hostname) $(date) 

## Install Miniconda3
wget -c -nv https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh -b -p ${PROJ_HOME}/miniconda/
