#!/bin/bash

cd ${OTHER}
wget -c -nv --tries=2 https://apps.fs.usda.gov/fia/datamart/CSV/REF_POP_ATTRIBUTE.csv -O ./attributes_all.csv

cd ${PROJ_HOME}
