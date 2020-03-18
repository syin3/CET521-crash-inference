#!/usr/bin/env python
# coding: utf-8

'''
s1_xslx2csv.py
Converts datasets in xslx format to csv in the given directory
'''

import os
import pandas as pd

def find_excel(direct):
    """
    Find all xlsx files in a given directory

    input
    -----
    direct : string
        directory where search will be performed

    output
    -----
    file_list : array_like
        sorted list of xlsx file names

    test
    -----
        (1) if file_list is sorted;
        (2) if all xlsx files are in the list
    """

    # initiate the list
    file_list = []
    
    # iterate through designated directory
    for file in os.listdir(direct):
        if file.endswith(".xlsx"):
            file_list.append(file)

    # sort the file list in reverse order
    file_list = sorted(file_list, reverse=True)
    return file_list

def convert_xlsx2csv(xlsx_direct, csv_direct, file_list):
    """
    Convert excel files in the list to csv files

    input
    -----
    xlsx_direct : string
        directory of xlsx files

    csv_direct : string
        directory of csv files

    file_list : array_like
        list of xlsx files to be converted

    output
    -----
    nothing

    test
    -----
        (1) if column names are the same for the same file over years;
        (2) test if all excel files have one and only one output csv;
    """

    attributes = {}
    # go throuh 2017 files first in the first round
    for file in file_list:
        if '17' in file:
            curr = pd.read_excel(os.path.join(xlsx_direct, file))
            attributes[file.split('.')[0][4:]] = list(curr.columns)
            curr.to_csv(os.path.join(csv_direct, file.split('.')[0] + '.csv'), index=False)

    for file in file_list:
        # we have already passed through 2017 files
        if '17' not in file:
            curr = pd.read_excel(os.path.join(xlsx_direct, file))
            # rename columns in other years' files in 2017 style
            curr.columns = attributes[file.split('.')[0][4:]]
            # save as corresponding csv files in output directory
            curr.to_csv(os.path.join(csv_direct, file.split('.')[0] + '.csv'), index=False)

    # return nothing
    pass

# file_list = find_excel('../hsis-xlsx/')
# convert_xlsx2csv('../hsis-xlsx/', '../hsis-csv/', file_list)
