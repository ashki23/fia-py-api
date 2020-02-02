#!/usr/bin/python3

import sys
import csv
import json
import xlrd
import collections

def csv_dict(csv_file):
    csv_data = csv.reader(csv_file)
    dict_data = {}
    for row in csv_data:
        dict_data[row[0]] = ','.join([str(x) for x in row[1:]])
    return dict_data

def csv_list_tuple(csv_file):
    csv_data = csv.reader(csv_file)
    list_data = []
    for row in csv_data:
        list_data.append(tuple(row))
    return list_data

def csv_list_dict(csv_file):
    csv_data = csv.reader(csv_file)
    header = next(csv_data)
    assert all([isinstance(x, str) for x in header]), "Header (first row) includes not-string elemnets"
    header = [x.lower() for x in header]
    ncol = len(header)
    list_dict = []
    for row in csv_data:
        mm = {}
        for i in range(ncol):
            mm[header[i]] = row[i]
        list_dict.append(mm)
    return list_dict

def state_config(dict_file,config):
    if config['state'] == ['ALL']:
        return dict_file
    else:
        select = {}
        for st in config:
            select[st] = dict_file[st]
        return select

def select_att_config(dict_,config):
    select = {}
    for i in config['attribute_cd']:
        select[str(i)] = dict_[str(i)]
    return select

def dict_csv(dict_data,csv_output):
    fcsv = csv.writer(csv_output)
    for j in dict_data:
        row = [j] + [dict_data[j]]
        fcsv.writerow(row)

def list_dict_csv(list_dict,keys,csv_output):
    fcsv = csv.writer(csv_output)
    fcsv.writerow(keys)
    for i in list_dict:
        for j in keys:
            if j in i.keys():
                if type(i[j]) is list:
                    i[j] = ','.join(i[j])
            else:
                i[j] = 'NA'
        row = [i[x] for x in keys]
        fcsv.writerow(row)

def list_dict_panel(list_dict,keys,config,csv_output):
    fcsv = csv.writer(csv_output)
    fcsv.writerow(keys + ['year'] + ['att_%s' % x for x in config['attribute_cd']])
    for i in list_dict:
        for j in keys:
            if j in i.keys():
                if type(i[j]) is list:
                    i[j] = ','.join(i[j])
            else:
                i[j] = 'NA'
        row = [i[x] for x in keys]
        for y in config['year']:
            att_yr = []
            att_yr.extend(['%s_%s' % (x,y) for x in config['attribute_cd']])
            fcsv.writerow(row + [y] + [i[x] for x in att_yr])

if __name__=='__main__':
    ## Read config
    config = json.load(open(sys.argv[1]))
    year = config['year']
    
    ## FIA attributes
    with open('./other_data/attributes_all.csv', 'r') as att:
        cd_att_all = csv_list_dict(att)
    
    ## FIA attributes number and name
    cd_att = {}
    for atr in cd_att_all:
        cd_att[atr['attribute_nbr']] = atr['attribute_descr']
    
    ### Uncomment to read FIA attributes number and name directly
    # with open('./attributes_all.csv', 'r') as att:
    #     cd_att = csv_dict(att)
    
    ## JSON and CSV outputs of selected FIA attributes (number and name)
    att_select = select_att_config(cd_att,config)
    with open('attributes.json', 'w') as atj:
        json.dump(att_select, atj) 
    
    with open('attributes.csv', 'w') as atc:
        dict_csv(att_select,atc)
    
    ## FIA state codes
    with open('./state_codes.csv', 'r') as cd:
        state_cd = csv_dict(cd)
    
    ## State neighbors and codes
    with open('./neighbor_state.csv', 'r') as nd:
        ne_data = csv_list_tuple(nd) 
    
    neighbor_state = collections.defaultdict(list)
    for i,j in ne_data:
        neighbor_state[i].append(j)
        neighbor_state[j].append(i)
    
    neighbor_cd = collections.defaultdict(list)
    for i,j in ne_data:
        neighbor_cd[i].append(state_cd[j])
        neighbor_cd[j].append(state_cd[i])
