#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import re
import os
import sys
import fnmatch
import datetime
import pandas as pd
from collections import Counter
from xpinyin import Pinyin

BASE_DIRECTORY = os.path.abspath(os.getcwd())


def scrape_directory_for_raw_data_path(dir_path, file_type='*.record'):
    """
    Scrape the given directory and collect the absolute path of files that match the file_type into a list.
    :param dir_path: path string, root directory of the project/data
    :param file_type: string, common expression format of the file type
    :return: list
    """

    raw_data_path_list = []
    for subdir, dirs, files in os.walk(dir_path):
        for this_file in files:
            if fnmatch.fnmatch(this_file, file_type):
                raw_data_path_list.append(os.path.join(subdir, this_file))

    return raw_data_path_list


def load_right_lines(file_path, silence=True):
    """
    Try load each line in a given file, print out error if any
    :param file_path: string
    :return: list of line item in a file
    """

    try:
        records = [json.loads(line) for line in open(file_path)]
    except Exception as err:
        print err
        bad_line = []
        line_count = 0
        records = []
        for line in open(file_path):
            line_count += 1
            try:
                records.append(json.loads(line))
            except Exception as err:
                bad_line.append((line_count, err))
        print 'Loading %s lines with %s lines compromised' % (line_count, len(bad_line))
        if len(bad_line) > 0:
            for log in bad_line:
                print 'Error at line %s : %s; Excluded from raw data' % log
    return records


def extract_gps_and_signal_data(records, name_ext, split_second=False, test=False):
    """
    Extract gps and signal data: east, north (location metric split), 时间，速度，位置，高度，信号强度，信噪比. Return a
    pandas dataframe
    :param records: list of line item
    :param name_ext: int, 0 if bs 1 if ss
    :param split_second: bool, todo, whether to include millisecond data
    :param test: bool, true if under testing, only load in the first 100 records in the file
    :return: pd.DataFrame
    """
    name_ext = '_' + str(name_ext)
    df_list = []
    counter = 0
    p = Pinyin()
    row_range = 100 if test else len(records)
    for i in range(row_range):
        if i % 5000 == 0:
            print '跑完{}行'.format(i)
        if counter % 500 == 0 and counter > 0:
            print '抓取{}行'.format(counter)
        this_record = records[i]
        if not ('stream.gps_status' in this_record and 'osd.measurement' in this_record and
                 p.get_pinyin(this_record['osd.measurement']) != u'mei-you-ke-yong-ce-liang-xin-xi'):
            continue
        gps_info = general_split_string(this_record['stream.gps_status'], ' ', ':')
        signal, snr = measurement_split_string(this_record['osd.measurement'])
        df_list.append(tuple(gps_info.values()) + (signal, snr))
        counter += 1

    df = pd.DataFrame(df_list, columns=['east' + name_ext, 'north' + name_ext,
                                        'shi-jian' + name_ext, 'su-du' + name_ext,
                                        'wei-zhi' + name_ext, 'gao-du' + name_ext,
                                        'xin-hao-qiang-du' + name_ext,
                                        'xin-zao-bi' + name_ext])
    return df


def general_split_string(chinese_string, element_sep, equal_sign):
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
                data_entry_dict[sub_item_name] = None

    if reformat_time:
        data_entry_dict['shi-jian'] = pd.to_datetime(data_entry_dict['shi-jian'] + ' ' + timestamp, yearfirst=True)
    return data_entry_dict


def measurement_split_string(measurement_string):
    """
    Split signal metric.
    :param measurement_string: string that contains measurement metrics
    :return: float, float
    """

    p = Pinyin()
    signal_string, snr_string = [p.get_pinyin(x) for x in measurement_string.split('$')]
    try:
        signal = re.findall('[-+]?\d+[\.]?\d*[eE]?[-+]?\d',
                        str(re.findall("xin-hao-qiang-du-<-\d*dbm", signal_string)[0]))[0]
    except IndexError:
        signal = re.findall('[-+]?\d+[\.]?\d*[eE]?[-+]?\d',
                        str(re.findall("xin-hao-qiang-du--\d*dbm", signal_string)[0]))[0]

    snr = re.findall('\d+[\.]?\d*[eE]?[-+]?\d',
                        str(re.findall("xin-zao-bi-\d*", snr_string)[0]))[0]

    return float(signal), float(snr)


def extract_raw_data_into_csv(data_directory, test=False):
    """
    Process raw data into csv.
    TODO: make it generalizable for all kinds of data output (currenly gps and signal data)
    :param data_directory: root directory of data / project
    :param test: bool, if true then only grab first 100 lines in each file and convert
    :return: none
    """
    raw_data_path = scrape_directory_for_raw_data_path(data_directory)
    print raw_data_path
    for file in raw_data_path:
        record = load_right_lines(file)
        file_name = os.path.basename(file)
        print '\n Processing %s \n' % file_name
        sub_dir = os.path.dirname(file)
        name_ext = 0 if re.search('bs', file_name) else 1
        df = extract_gps_and_signal_data(record, name_ext, test=test)
        print df.head()
        file_ext = '_test.csv' if test else '.csv'
        df.to_csv(os.path.join(sub_dir, file_name.split('.')[0] + file_ext), index=False)
        print os.path.join(sub_dir, file_name.split('.')[0] + file_ext)


if __name__ == "__main__":
    extract_raw_data_into_csv(os.path.join(BASE_DIRECTORY, 'raw_data'), test=True)
    print '你妈喊你回家吃饭！'