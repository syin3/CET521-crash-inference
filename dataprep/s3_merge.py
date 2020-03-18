#!/usr/bin/env python
# coding: utf-8

'''
merge.py
reads in NOAA converted coordinates and merge with accident, road, grade, curve files
'''

import os
import pandas as pd
import sqlite3

def read_noaa_coords(yr, directory):
    """
    read from NOAA converted coords

    input
    -----
    yr : int
        year to combine

    directory : string
        where to read NOAA converted files from

    output
    -----
    records : 
        combined coords for the specified year

    test
    -----
        (1) if number of files is greater than zero;
        (2) if records has non-zero length;
        (3) if records' num of columns is three;
    """

    columns = ['ID', 'destLat', 'destLon']
    
    # detect all csv files of the year
    yr_file_list = []
    for file in os.listdir(directory):
        if str(yr) in file:
            yr_file_list.append(file)
    
    # sort so that No.0 file is at the first place
    yr_file_list = sorted(yr_file_list)
    
    # read and append the dataframes
    count = 0
    for file in yr_file_list:
        if count == 0:
            records = pd.read_csv(directory + '/' + file)
            records = records[columns]
        else:
            records = records.append(pd.read_csv(directory + '/' + file)[columns]).reset_index(drop=True)
        
        count += 1
    
    records.columns = ['ID', 'lat', 'lon']
    return records

def acc_merge(acc_file_list, noaa_coords, directory):
    """
    merge accidents with NOAA coords

    input
    -----
    acc_file_list : array_like
        sorted list of accident file names

    noaa_coords : df
        combined NOAA records

    directory : string
        directory of accident files

    output
    -----
    crashes : dic
        dictionary of merged crahses

    test
    -----
        (1) crash dictionary should have 5 key-values, 2013-2017;
        (2) 16 columns in merged datasets;
        (3) weather and light should be in float, but may have NaN;
        (4) crash should not be empty;
    """

    # create dictionary of crashes, key is year
    crashes = {}
    for file in acc_file_list:
        yr = file[2:4]
        acc_file = directory + '/' + file
        tmp = pd.read_csv(acc_file)
        tmp = tmp.dropna(subset=['State_Plane_X', 'State_Plane_Y']).reset_index()
        tmp['ID'] = tmp.index + 1
        crashes[2000+int(yr)] = tmp
    
    # merge noaa records with corresponding year of accidents
    for yr in noaa_coords.keys():
        crashes[yr] = crashes[yr].merge(noaa_coords[yr], on='ID', how='inner')
    
    # specify columns to keep
    columns = ['CASENO', 'FORM_REPT_NO', 'rd_inv', 'milepost', 'RTE_NBR',
           'lat', 'lon', 
           'MONTH', 'DAYMTH', 'WEEKDAY', 
           'RDSURF', 'LIGHT', 'weather', 'rur_urb',
           'REPORT', 'SEVERITY']

    # convert string of light and weather to float, but keep NaN
    for yr in crashes.keys():
        crashes[yr] = crashes[yr][columns]
        crashes[yr].LIGHT = pd.to_numeric(crashes[yr].LIGHT, errors='coerce')
        crashes[yr].weather = pd.to_numeric(crashes[yr].weather, errors='coerce')
    
    return crashes

def detect_files(directory, keyword):
    """
    detect files in specified directory with specified keyword

    input
    -----
    directory : string
        dir to search

    keyword : string
        keyword to look for

    output
    -----
    sorted list of file names

    test
    -----
        (1) if output has larger than length;
    """

    file_list = []
    for file in os.listdir(directory):
        if not (keyword is None):
            if keyword in file:
                file_list.append(file)
        else:
            file_list.append(file)
    
    return sorted(file_list)

def read_files(directory, keyword):
    """
    read files with specified keyword

    input
    -----
    directory : string
        directory to read files from

    keyword : string
        keyword to search for

    output
    -----
    output_dic : dic
        dictionary of datasets

    test
    -----
        (1) output_dic should have length 5, for 2013 - 2017;
        (2) keyword should not be empty;
    """
    output_dic = {}
    file_list = detect_files(directory, keyword)
    for yr in range(2013, 2018):
        output_dic[yr] = pd.read_csv(os.path.join(directory, file_list[yr-2013]))
    return output_dic

def veh_agg(df, crash_year):
    """
    aggregate vehicle info

    input
    -----
    df : pandas dataframe
        df to be summarized

    output
    -----
    df: pandas dataframe
        aggregated df

    """
    def sex(series):
        for ele in series.tolist():
            if ele > 1:
                return True
        return False

    def young(series):
        for ele in series.tolist():
            if ele < 25:
                return True
        return False

    def old(series):
        for ele in series.tolist():
            if ele > 65:
                return True
        return False

    def drink(series):
        for ele in series.tolist():
            if ele == 1.0 or ele == 5.0:
                return True
        return False

    def truck(series):
        for ele in series.tolist():
            if ele > 4:
                return True
        return False
    
    def old_car(series):
        for ele in series.tolist():
            model_year = 1900
            if ele < 10:
                model_year += (100 + ele)
            elif ele < 20:
                model_year += (100 + ele)
            else:
                model_year += (ele)
            
            if crash_year - model_year >= 15:
                return True
        return False
    
    df = df.groupby(['CASENO']).agg({'DRV_SEX': [sex],
                            'DRV_AGE': [young, old],
                            'vehtype': [truck],
                            'vehyr': [old_car],
                            # 'surf_typ': ,
                            # 'drv_actn': ,
                            'intox': [drink]
                           })
    df.columns = df.columns.get_level_values(1)
    df = df.reset_index()
    return df

def meta_merge(**kwargs):
    """
    merge various sources to final accident files

    input
    -----
    crashes : df
        crashes with noaa coords

    curv : string
        curv dictionary with 5 years of data

    grad : string
        grade dictionary with 5 years of data

    occ : string
        occupant dictionary with 5 years of data

    road : string
        road dictionary with 5 years of data

    veh : string
        vehicle dictionary with 5 years of data

    output
    -----
    meta : dic
        dictionary of 5 years of merged meta dataset

    test
    -----
        (1) every input has length 5
        (2) crashes, occ, veh have key 'CASENO'
        (3) 22 columns in output datasets
        (4) output has length 5
        (5) no nan in final output datasets
        (6) -2 column are all string type
    """

    crash = kwargs['crash']
    veh = kwargs['veh']
    peds = kwargs['peds']
    occ = kwargs['occ']
    road = kwargs['road']
    curv = kwargs['curv']
    grad = kwargs['grad']

    acc_drop = kwargs['acc_drop']
    veh_drop = kwargs['veh_drop']
    peds_drop = kwargs['peds_drop']
    occ_drop = kwargs['occ_drop']
    # be careful on the key
    road_drop = kwargs['road_drop']# 'COUNTY', 'DISTRICT', 'FUNC_CLS', 'RD_TYPE', 'RTE_NBR', 'rodwycls'
    curv_drop = kwargs['curv_drop']
    grad_drop = kwargs['grad_drop']
    

    # drop columns in acc first
    for yr in range(2013, 2018):
        crash[yr] = crash[yr].drop(acc_drop, axis=1)
    
    meta = {}
    for yr in range(2013, 2018):
        # veh, needs aggregation
        # options: has_female, has_elder, has_old_car, has_furf_typ, has_drv_actn, has_trf_cntl, has_intox
        veh_aggregated = veh_agg(veh[yr], yr)

        # peds, needs aggregation
        # not every crash has peds involved
        # maybe has_peds?
        # if we only look at veh-veh crashes, then no problem, because no peds info needed
        # peds_cnt = peds[yr]['CASENO'].value_counts().sort_index()
        # peds_cnt = peds_cnt.to_frame().reset_index()
        # peds_cnt.columns = ['CASENO', 'peds_cnt']

        # occs, need aggregation
        # not every crash has occupant info
        # occ_cnt = occ[yr]['CASENO'].value_counts().sort_index()
        # occ_cnt = occ_cnt.to_frame().reset_index()
        # occ_cnt.columns = ['CASENO', 'occ_cnt']

        # if now veh info is one-to-one, then we use inner join
        # if the relationship is one-to-many, then use left
        acc_this_yr = crash[yr].merge(veh_aggregated, on='CASENO')
        # acc_this_yr = acc_this_yr.merge(peds_cnt, on='CASENO', how='left')
        # acc_this_yr = acc_this_yr.merge(occ_cnt, on='CASENO', how='left')
        
        # road
        road_this_yr = road[yr].drop(road_drop, axis=1)
        conn = sqlite3.connect(":memory:")
        acc_this_yr.to_sql("crash", conn, index=False)
        road_this_yr.to_sql("road", conn, index=False)

        query =  "SELECT * FROM crash, road \
        WHERE crash.rd_inv = road.ROAD_INV\
        AND crash.milepost >= road.BEGMP\
        AND crash.milepost <= road.ENDMP"
        records = pd.read_sql_query(query, conn)
        
        ## remove duplicates randomly
        records = records.sample(frac=1).drop_duplicates(subset='CASENO').sort_index()
        # remove duplicate connecting keys
        records = records.drop(['ROAD_INV', 'BEGMP', 'ENDMP'], axis=1)
        
        # curve
        curv_this_yr = curv[yr].drop(curv_drop, axis=1)
        conn = sqlite3.connect(":memory:")
        records.to_sql("records", conn, index=False)
        curv_this_yr.to_sql("curv", conn, index=False)
        
        query = "SELECT * FROM records\
        LEFT JOIN curv ON records.rd_inv = curv.curv_inv \
        AND records.milepost >= curv.begmp \
        AND records.milepost <= curv.endmp"
        records = pd.read_sql_query(query, conn)
        
        ## remove duplicates and drop useless attributes
        records = records.sample(frac=1).drop_duplicates(subset='CASENO').sort_index()
        records = records.drop(['curv_inv', 'begmp', 'endmp'], axis=1)
        
        ## fill NaN curvature with 0
        records = records.fillna(value={'deg_curv': 0})
        
        # grad
        grad_this_yr = grad[yr].drop(grad_drop, axis=1)
        conn = sqlite3.connect(":memory:")
        records.to_sql("records", conn, index=False)
        grad_this_yr.to_sql("grad", conn, index=False)
        
        query = "SELECT * FROM records \
        LEFT JOIN grad ON records.rd_inv = grad.grad_inv\
        AND records.milepost >= grad.begmp\
        AND records.milepost <= grad.endmp"
        
        records = pd.read_sql_query(query, conn)
        
        records = records.sample(frac=1).drop_duplicates(subset='CASENO').sort_index()
        records = records.drop(['grad_inv', 'begmp', 'endmp'], axis=1)
        records = records.fillna(value={'pct_grad': 0})
        
        # columns = ['CASENO', 'FORM_REPT_NO', 'rd_inv', 'milepost', 'RTE_NBR', 'lat', 'lon',\
        #            'MONTH', 'DAYMTH', 'WEEKDAY', 'RDSURF', 'LIGHT', 'weather', 'rur_urb',\
        #            'REPORT', 'veh_count', 'COUNTY', 'AADT', 'mvmt', 'deg_curv', 'dir_grad',\
        #            'pct_grad']

        # can select subset
        if not kwargs['subsets'] is None:
            records = records[kwargs['subsets']]
        
        # dir_grad may have mixed types 0 and NA
        for i in range(records.shape[0]):
            if not isinstance(records.iloc[i, -2], str):
                records.iloc[i, -2] = '0'
    
        # be careful on dropping na, a lot of attributes are N/A
        # records = records.dropna()

        # when everything is done, save
        meta[yr] = records
    return meta

# noaa_coords = {}
# for yr in range(2013, 2018):
#     noaa_coords[yr] = read_noaa_coords(yr, '../../data/coords-noaa')

acc_file_list = detect_files("../hsis-csv", 'acc')
# crash = acc_merge(acc_file_list, noaa_coords, '../../data/hsis-csv')

crash = read_files("../hsis-csv", 'acc')
veh = read_files("../hsis-csv", 'veh')
peds = read_files("../hsis-csv", 'peds')
occ = read_files("../hsis-csv", 'occ')

road = read_files("../hsis-csv", 'road')
curv = read_files("../hsis-csv", 'curv')
grad = read_files("../hsis-csv", 'grad')

# GPS_LAT's are all 0
# xrdclass are all empty
# loc_char most are unspecified '.'
# CITY many urban cases do not have city info
# AC_SRMPI are all empty
acc_drop = ['GPS_LATX', 'GPS_LATY', 'GPS_LATZ', 'xrdclass', 'loc_char', 'CITY', 'AC_SRMPI']
veh_drop = []
peds_drop = []
occ_drop = []
road_drop = ['TRLL_LG1','TRLL_LG2','TRLL_WD1','TRLL_WD2','TRLR_LG1','TRLR_LG2','TRLR_WD1','TRLR_WD2', 'DOMAIN', 'COUNTY', 'RTE_NBR']
# we need to rename some columns to avoid duplicacy
curv_drop = ['seg_lng']
grad_drop = []

met = meta_merge(crash=crash, veh=veh, peds=peds, occ=occ,
    road=road,curv=curv, grad=grad,
    acc_drop=acc_drop, veh_drop=veh_drop, peds_drop=peds_drop, occ_drop=occ_drop,
    road_drop=road_drop, curv_drop=curv_drop, grad_drop=grad_drop, subsets=None)

for yr in range(2013, 2018):
    met[yr].to_csv('../merged/{}.csv'.format(yr), index=False)
    print('finished {}'.format(yr))

