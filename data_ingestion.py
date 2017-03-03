#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import re
import sys
import datetime
import pandas as pd
from collections import Counter
from xpinyin import Pinyin



sys.getdefaultencoding


BASE_DIRECTORY = '/'.join(['C:', 'Users', 'Chunyi Zhao', 'Projects', 'drone'])
print BASE_DIRECTORY
BS_FILE = '/'.join([BASE_DIRECTORY, 'raw_data', 'bsdata.json'])
print BS_FILE
SS_FILE = '/'.join([BASE_DIRECTORY, 'raw_data', 'ssdata.json'])


def load_right_lines(file_path):

    try:
        records = [json.loads(line) for line in open(file_path)]
    except Exception as err:
        print err
        bad_line = []
        line_count = 0
        records = []
        for line in open(SS_FILE):
            line_count += 1
            try:
                records.append(json.loads(line))
            except Exception as err:
                bad_line.append(line_count)
    return records


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


def extract_gps_measurement_data(records, name_ext, split_second=False):
    name_ext = '_' + name_ext
    df_list = []
    counter = 0
    p = Pinyin()
    for i in range(len(records)):
        if i % 5000 == 0:
            print '跑完{}行'.format(i)
        if counter % 5000 == 0 and counter > 0:
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

if __name__ == "__main__":

    ss_records = load_right_lines(SS_FILE)
    # bs_records = load_right_lines(BS_FILE)
    # print extract_gps_measurement_data(ss_records[18000:19000], '0')

    # bs_df = extract_gps_measurement_data(bs_records, '2')
    ss_df = extract_gps_measurement_data(ss_records, '0')
    # print bs_df.head()
    # print ss_df.head()
    # bs_df.to_csv('/'.join([BASE_DIRECTORY, 'raw_data', 'bs_gps_signal.csv']))
    ss_df.to_csv('/'.join([BASE_DIRECTORY, 'raw_data', 'ss_gps_signal.csv']), index=False)

    # gps_measure_df = bs_df[['shi-jian_2', 'signal_2', 'snr_2']].merge(ss_df[['shi-jian_0', 'signal_0', 'snr_0']],
    #                                                                   how='left',
    #                                                                   left_on='shi-jian_2', right_on='shi-jian_0',
    #                                                                   sort=True)
    # print gps_measure_df.head()
    # gps_measure_df.to_csv('/'.join([BASE_DIRECTORY, 'raw_data', 'combined_gps_signal.csv']))
    # TODO: data structure
    # TODO: combine GPS data and xhqd and xzb
    # TODO: comine data sources --> merge two dataframes
    print '你妈喊你回家吃饭！'