#!/bin/bash

read -p "This is the garbage collector. It will remove job file and collected data in './fia_data' and './job_out*' directories. Do you want to proceed (yes/no)? " answer

if [ $answer = "yes" ]; then
    rm -v ./Miniconda3*
    rm -v ./job-*
    rm -v ./jobid-*
    rm -v ./report-*
    rm -rv ./job_out_*
    rm -rv ./fia_data
else 
    return
fi
