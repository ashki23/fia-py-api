#!/bin/bash

wget -c -nv --tries=2 https://apps.fs.usda.gov/fia/datamart/CSV/REF_POP_ATTRIBUTE.csv -O ./attributes_all.csv
wget -c -nv --tries=2 https://gitlab.com/ashki23/rc-biomass/-/raw/main/state_codes.csv
wget -c -nv --tries=2 https://gitlab.com/ashki23/rc-biomass/-/raw/main/neighbor_state.csv
