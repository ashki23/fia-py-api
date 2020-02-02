#!/bin/bash

#SBATCH --job-name=Bootstrap
#SBATCH --cpus-per-task=10
#SBATCH --mem=60G

echo -------------------------------------------- $(hostname) $(date)

## Install Spack and Miniconda3 by Spack
git clone https://github.com/spack/spack.git
source spack/share/spack/setup-env.sh
spack install miniconda3

echo -------------------------------------------- $(hostname) $(date)

## Load Miniconda3
source spack/share/spack/setup-env.sh
spack load miniconda3

echo -------------------------------------------- $(hostname) $(date)
