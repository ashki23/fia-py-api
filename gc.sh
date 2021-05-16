#!/bin/bash

read -p "This is the garbage collector. It will remove job file and collected data in './fia_data' and './job_out*' directories. Do you want to proceed (yes/no)? " answer

if [ $answer = "yes" ]; then
    rm -v ./Miniconda3*
    rm -v ./job-*
    rm -v ./jobid-*
    rm -v ./slurm-*.out
    rm -v ./report-*
    rm -r ./job_out_* && echo "'/job_out_*' directories were removed"
    rm -r ./fia_data && echo "'./fia_data' directory was removed"
else
    return
fi
