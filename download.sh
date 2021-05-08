#!/bin/bash

wget -c -nv --tries=2 https://apps.fs.usda.gov/fia/datamart/CSV/REF_POP_ATTRIBUTE.csv -O ./attributes_all.csv
wget -c -nv --tries=2 https://www2.census.gov/geo/docs/reference/state.txt
wget -c -nv --tries=2 https://gitlab.com/ashki23/rc-biomass/-/raw/main/neighbor_state.csv

cat state.txt | awk -F '|' '{print $3","$2}' | tail -n +2 > state_abb.csv
cat state.txt | awk -F '|' '{print $2","$1}' | tail -n +2 > state_codes.csv
