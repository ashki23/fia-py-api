#!/bin/bash

wget -c -nv --tries=2 https://apps.fs.usda.gov/fia/datamart/CSV/REF_POP_ATTRIBUTE.csv -O ./attributes_all.csv
wget -c -nv --tries=2 https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_state_20m.zip
wget -c -nv --tries=2 https://www2.census.gov/geo/docs/reference/state.txt

unzip -n ./cb*state*zip -d ./shape_state
cat state.txt | awk -F '|' '{print $3","$2}' | tail -n +2 > state_abb.csv
cat state.txt | awk -F '|' '{print $2","$1}' | tail -n +2 > state_codes.csv
rm cb_*_us_state*.zip
