#!/usr/bin/env  python

import os
import sys
import json

## Open JSON config and database
with open('./config.json') as conf:
    config = json.load(conf)
year = config['year']

q_type = input('Select the query type (insert one of state, county or coordinate): ').lower().strip()

if q_type == 'state':
    latest = os.popen(f"""
    ls -t ./outputs/state-*.json | head -n 1
    """).read()[:-1]
    
    with open(latest) as db:
        database = json.load(db)
    
    ## Input
    state_in = input('Insert state: ').upper().strip()
    
    ## Query
    state_ = []
    for db in database:
        state_.append(db['state'])
        if db['state'] == state_in:
                print(db)
    
    if state_in not in set(state_):
        print(f"Sorry! {state_in} is not in the latest database ({latest}).")

elif q_type == 'county':
    latest = os.popen(f"""
    ls -t ./outputs/county-*.json | head -n 1
    """).read()[:-1]
    
    with open(latest) as db:
        database = json.load(db)
    
    ## Input
    state_in = input('Insert state: ').upper().strip()
    county_in = input('Insert county: ').capitalize().strip()
    
    ## Query
    state_ = []
    for db in database:
        state_.append(db['state'])
        if db['state'] == state_in:
            if db['county'] == county_in:
                print(db)
    
    if state_in not in set(state_):
        print(f"Sorry! {state_in} is not in the latest database ({latest}).")

elif q_type == 'coordinate':
    latest = os.popen(f"""
    ls -t ./outputs/coordinate-*.json | head -n 1
    """).read()[:-1]
    
    with open(latest) as db:
        database = json.load(db)
    
    ## Input
    lat_in = input('Insert lat: ').strip()
    lon_in = input('Insert lon: ').strip()
    
    ## Query
    lat_ = []
    for db in database:
        lat_.append(db['lat'])
        if db['lat'] == lat_in:
            if db['lon'] == lon_in:
                print(db)
    
    if lat_in not in set(lat_):
        print(f"Sorry! this coordinate is not in the latest database ({latest}).")

else:
    print("Please select one of the available methods for 'query_type'. Available methods are 'state', 'county', 'coordinate'.")
