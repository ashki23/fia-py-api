#!/usr/bin/env  python

import os
import sys
import json

## Open JSON config and database
with open('./config.json') as conf:
    config = json.load(conf)
year = config['year']

q_type = input('Select the query type (insert one of state, county or coordinate): ').lower().strip()

if q_type not in ['state','county','coordinate']:
    print("Please select one of the available methods for 'query_type'. Available methods are 'state', 'county', 'coordinate'.")

elif q_type == 'coordinate':
    latest = os.popen(f"""
    ls -t ./outputs/coordinate-*.json | head -n 1
    """).read()[:-1]
    
    with open(latest) as db:
        database = json.load(db)
    
    ## Input
    lat_in = input('Insert lat: ').strip()
    lon_in = input('Insert lon: ').strip()
    unit_id = (str(lat_in).replace('.','') + str(lon_in).replace('.','').replace('-',''))[:8] 
    
    ## Query
    if unit_id in database.keys():
        print(database[unit_id])
    else:
        print(f"Sorry! This coordinate is not in the latest database ({latest}).")

elif q_type == 'state':
    latest = os.popen(f"""
    ls -t ./outputs/state-*.json | head -n 1
    """).read()[:-1]
    
    with open(latest) as db:
        database = json.load(db)
    
    ## Input
    state_in = input('Insert state: ').upper().strip()
        
    ## Query
    if state_in in database.keys():
        print(database[state_in])
    else:
        print(f"Sorry! {state_in} is not in the latest database ({latest}).")
    
else:
    latest = os.popen(f"""
    ls -t ./outputs/county-*.json | head -n 1
    """).read()[:-1]

    with open(latest) as db:
        database = json.load(db)
    
    ## Input
    state_in = input('Insert state: ').upper().strip()
    county_in = input('Insert county: ').capitalize().strip()
    unit_id = f"{county_in}_{state_in}"
        
    ## Query
    if unit_id in database.keys():
        print(database[unit_id])
    else:
        print(f"Sorry! {county_in},{state_in} is not in the latest database ({latest}).")