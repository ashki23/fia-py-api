#!/usr/bin/env  python

import os
import sys
import json

## Open JSON config and database
with open('./config.json') as conf:
    config = json.load(conf)
year = config['year']

q_type = input('Select the query type (insert one of state, county or coordinate): ').lower().strip()

if q_type == 'coordinate':
    latest = os.popen(f"""
    ls -t ./outputs/coordinate-*.json | head -n 1
    """).read()[:-1]
    
    with open(latest) as db:
        database = json.load(db)
    
    ## Input
    lat_in = input('Insert lat: ').strip()
    lon_in = input('Insert lon: ').strip()
    unit_id = (str(p['lat']).replace('.','')[::2] + str(p['lon']).replace('.','').replace('-','')[::2])[:8]
    
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
    state_in = input('Insert state (two-letter): ').upper().strip()
        
    ## Query
    if state_in in database.keys():
        print(database[state_in])
    else:
        print(f"Sorry! {state_in} is not in the latest database ({latest}).")
    
elif q_type == 'county':
    latest = os.popen(f"""
    ls -t ./outputs/county-*.json | head -n 1
    """).read()[:-1]

    with open(latest) as db:
        database = json.load(db)
    
    ## Input
    state_in = input('Insert state (two-letter): ').upper().strip()
    county_in = input('Insert county name: ').strip().split()
    county_in = " ".join([x.capitalize() for x in county_in])
    unit_id = f"{state_in}_{county_in}"
        
    ## Query
    if unit_id in database.keys():
        print(database[unit_id])
    else:
        print(f"Sorry! {county_in},{state_in} is not in the latest database ({latest}).")

else:
    print("Please select one of the available methods for 'query_type'. Available methods are 'state', 'county', 'coordinate'.")
