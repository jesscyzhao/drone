#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import re
import os
import sys
import pandas as pd
from collections import Counter
from xpinyin import Pinyin


sys.getdefaultencoding


BASE_DIRECTORY = '/'.join(['C:', 'Users', 'Chunyi Zhao', 'Projects', 'drone'])
print BASE_DIRECTORY
DS_FILE = '/'.join([BASE_DIRECTORY, 'raw_data', 'bsdata.json'])
print DS_FILE
SS_FILE = '/'.join([BASE_DIRECTORY, 'raw_data', 'ssdata.json'])


def split_string_data(chinese_string, element_sep, equal_sign):
    """
    Generalized function that split each item in the row dictionary from the json file．Uses element_sep to split out
    major metrics string (<'sub_item_name equal_sign sub_item>). Uses equal_sign to split out sub_item_name and
    sub_item. Also handles data cleaning on GPS metrics.
    :param chinese_string: string, item in the row dictionary
    :param element_sep: string, either ' ' or '$' given the current data
    :param equal_sign: string, either '=' or ':' given the current data
    :return: dict, with metric name translated into Pinyin and numeric values for GPS inforamtion.
    """
    # TODO: initials for name with more than 2 characters / use initials for all
    # TODO: handle single numerical value
    p = Pinyin()
    data_entry_dict = {}
    # Initialize some variables for timestamp reformatting
    timestamp = None
    reformat_time = False
    for item in chinese_string.split(element_sep):
        # Identify timestamp and save it to the variable timestamp, make reformat_time True, and continue to the next
        # item.
        if ':' in Counter(item) and Counter(item)[':'] == 2:
            timestamp = item
            reformat_time = True
            continue
        # Split the item further into sub_item_name and sub_item
        item_detail = item.split(equal_sign)
        sub_item_name = str(p.get_pinyin(item_detail[0]))
        sub_item = item_detail[1]
        # Put them in a dict
        data_entry_dict[sub_item_name] = sub_item
        # We will handle 'shi-jian' in the end, so if the current sub_item is 'shi-jian', continue to next item
        if sub_item_name == 'shi-jian':
            continue
        # Keep all the raw data with ':' or '/' as string
        elif ':' in sub_item or '/' in sub_item:
            pass
        # Get numeric values from the rest for future analysis
        else:
            numbers = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d", sub_item)
            if sub_item_name == 'wei-zhi':
                data_entry_dict['north'] = numbers[0]
                data_entry_dict['east'] = numbers[1]
            elif len(numbers) > 0:
                data_entry_dict[sub_item_name] = float(numbers[0])
            else:
                pass

    if reformat_time:
        data_entry_dict['shi-jian'] = pd.to_datetime(data_entry_dict['shi-jian'] + ' ' + timestamp, yearfirst=True)
    print data_entry_dict.keys()
    return data_entry_dict


if __name__ == "__main__":
    records = [json.loads(line) for line in open(DS_FILE)]
    print records[0]['stream.gps_status']
    print records[0]['fpga.alink']

    test_gps = split_string_data(records[0]['stream.gps_status'], ' ', ':')
    test_fpga_alink = split_string_data(records[0]['fpga.alink'], '$', '=')
    test_naklib_ukbuf_stat = split_string_data(records[0]['naklib.ukbuf.stat'], '$', ':')
    print test_gps
    print test_fpga_alink
    print test_naklib_ukbuf_stat

    # TODO: data structure
    # TODO: combine GPS data and xhqd and xzb
    # TODO: comine data sources --> merge two dataframes
    # TODO: Next scrip: plot funciton
    # TODO: Output format
    # TODO: Django
    # counter = 0
    # for i in range(len(records)):
    #     if 'stream.gps_status' not in records[i].keys():
    #         continue

    print '你妈喊你回家吃饭！'