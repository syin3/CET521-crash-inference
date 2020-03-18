#!/usr/bin/env python
# coding: utf-8

'''
s0_copyAttr.py
Copies attributes from Wei's request, e.g. ACCTYPE (accident type)
Sorts dataframe attributes in the pre-defined order
'''

import os
import pandas as pd
from collections import defaultdict
from s0_xlsx2csv import convert_xlsx2csv

def match(first, second):
    """
    Tell if two strings match, regardless of letter capitalization

    input
    -----
    first : string
        first string

    second : string
        second string

    output
    -----
    flag : bool
        if the two strings are approximately the same
    """
    if len(first) != len(second):
        return False

    for s in range(len(first)):
        fir = first[s]
        sec = second[s]
        if fir.lower() != sec and fir.upper() != sec:
            return False
    return True

def find_match(col, ref_list):
    """
    Tell if a string col has an approximate match in ref_list

    input
    -----
    col : string
        the string to be searched for

    ref_list : array_like
        the list where the search takes place

    output
    -----
    If there is a match in the list for the col
    """
    for ref_col in ref_list:
        if match(col, ref_col):
            if col == 'ACCTYPE':
                print(ref_col)
            return True
    return False

def rename_col(col, ref_list):
    """
    find the match and rename the col to the matched in the ref_list

    input
    -----
    col : string
        the string to be searched for

    ref_list : array_like
        the list where the search takes place

    output
    -----
    remapped name
    """
    for ref_col in ref_list:
        if match(col, ref_col):
            return ref_col
    return None

def read_defined_order(request_form='../yin_hsis_data_request.xlsx'):
    """
    Read the request form and the pre-defined ordered list of attributes

    input
    -----
    request_form : string
        the data request form made by Shuyi

    output
    -----
    attributes : dic of array_like
        dictionary of ordered attribute lists for each file
    """
    attributes = {}
    for tab in ['acc', 'curv', 'grad', 'occ', 'peds', 'road', 'veh']:
        curr = pd.read_excel(request_form, tab)
        attributes[tab] = curr['SAS variable name'].tolist()

    return attributes

def search_n_move(attributes, search_directory, ref_directory, save_directory):
    """
    Read ordered attribute lists, search in request directory;
    if not found, go to ref_directory to copy and paste

    input
    -----
    attributes : dic of array_like
        ordered lists of attributes, in dictionary format, keys are acc, peds, etc

    search_directory : string
        directory of files to check for attributes

    ref_directory : string
        directory of files to copy attributes from

    save_directory : string
        directory of csv files to be saved at

    output
    -----
    """

    # check for missing attributes
    # compile the (string, string) tuples into (string, list) tuples
    missing = defaultdict(list)
    for file in ['acc', 'curv', 'grad', 'occ', 'peds', 'road', 'veh']:
        # what has been returned by HSIS
        curr = pd.read_excel(os.path.join(search_directory, 'wa17' + file + '.xlsx'))
        # check in the requested list, which ones are not covered
        for col in attributes[file]:
            if not find_match(col, curr.columns):
                missing[file].append(col)
    
    header = {}
    # go to Wei's data for the missing attributes and copy back
    for key, val in missing.items():

        # missing columns may have different alternatives in Wei's across years
        # cannot rename beforehand, must rename per file per copy

        print(key, val)
        for year in range(17, 12, -1):
            lack_df = pd.read_excel(os.path.join(search_directory, 'wa{}'.format(year) + key + '.xlsx'))
            backup_df = pd.read_excel(os.path.join(ref_directory, 'wa{}'.format(year) + key + '.xlsx'))

            # print(lack_df.index.equals(backup_df.index)) # False
            # print(lack_df.index.intersection(backup_df.index).empty) # True
            val = [rename_col(col, backup_df.columns) for col in val]
            backup_df = backup_df[val]
            assert len(backup_df.shape) == 1 or (len(backup_df.shape) == 2 and backup_df.shape[1] == len(val))
            prev_len, prev_ncol = lack_df.shape
            
            # join Wei's data back to our dataframe
            lack_df = pd.merge(lack_df, backup_df, left_index = True, right_index = True)
            after_len, after_ncol = lack_df.shape
            assert prev_len == after_len
            assert prev_ncol + len(val) == after_ncol
            # lack_df[val] = backup_df[val].to_numpy()
            
            # need to keep attribute names consistent
            # remember header for 2017
            if year == 17:
                header[key] = list(lack_df.columns)
            else:
                lack_df.columns = header[key]
            
            # save to csv directly
            lack_df.to_csv(os.path.join(save_directory, 'wa{}'.format(year) + key + '.csv'), index=False)
    
    # return nothing
    pass

def copy_rest(csv_directory, xlsx_directory):
    """
    Scan the csv directory and check for missing csv files that did not trigger a merge from Wei's
    then converts the xlsx to csv

    input
    -----
    csv_directory : string
        directory of csv's saved

    xlsx_directory : string
        dicrectory of xlsx original data stored

    output
    -----
    """
    # the complete list should be 
    # acc, curv, grad, occ, peds, road, veh
    # 13, 14, 15, 16, 17
    curr_list = set([item.split('.')[0][4:] for item in os.listdir(csv_directory)])
    curr_list.discard('')

    curr_list = set(['acc', 'curv', 'grad', 'occ', 'peds', 'road', 'veh']) - curr_list
 
    convert_list = []
    for year in range(13, 18):
        for item in curr_list:
            convert_list.append('wa{}'.format(year) + item + '.xlsx')
    convert_xlsx2csv(xlsx_directory, csv_directory, convert_list)

    # return nothing
    pass


a = read_defined_order()
search_n_move(a, '../hsis-xlsx', '../../wei/hsis-xlsx', '../hsis-csv')
copy_rest('../hsis-csv', '../hsis-xlsx')
