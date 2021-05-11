#!/usr/bin/env  python

import os
import sys
import json

## Open JSON config and database
with open('./config.json') as conf:
    config = json.load(conf)
year = config['year']

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

#county_in = open(sys.argv[1]).readline()[:-1].split(',')
#att_in = open(sys.argv[2]).readline()[:-1].split(',')

#for ct in county_in:
#    db = database[int(ct)]
#    for i in att_in:
#        out = {x:int(y) for x,y in db.items() if x in [f"{int(i)}_{j}" for j in range(min(year), max(year) + 1)]}
#        print(out)
#        print(list(out.values()))
