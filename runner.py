#!/usr/bin/python3

import sys
import json

## Open JSON inputs
with open('./config.json') as conf:
    config = json.load(conf)

with open('./outputs/county-2020Feb02-0031.json') as db:
    database = json.load(db)

county_user = open(sys.argv[1]).readline()[:-1]
cty = county_user.split(',')
att_user = open(sys.argv[2]).readline()[:-1]
att = att_user.split(',')
year = config['year']

for i in cty:
    db = database[int(i)]
    for i in att:
        out = {x:int(y) for x,y in db.items() if x in ['%s_%s' % (int(i),j) for j in range(min(year),max(year)+1)]}
        print(out)
        print(list(out.values()))
