#!/usr/bin/env python3

"""
@Author: Miro
@Date: 27/12/2022
@Version: 1.1
@Objective: correggere la data di creazione delle anomalie
@TODO:
"""

import xml.etree.ElementTree as et
import numpy as np
import pandas as pd
import datetime


def clear_dataset_from_db(df: pd.DataFrame) -> pd.DataFrame:
    """
        replace the string 'NaT' with Nan in the input dataframe
        :param df: input to clean
        :return: dataframe with 'NaT' replaced with Nan
    """

    df.replace('NaT', np.nan, inplace=True)
    df.replace('None', np.nan, inplace=True)
    df.replace('NA', np.nan, inplace=True)
    df.fillna(value=np.nan, inplace=True)
    return df


def parse_date(date_str: str) -> (int, int):
    """
        parse the input date string and return the month and year parts, two format are supported: 'YYYYMM' and 'MM/YYYY'
        :param date_str: input string to parse
        :return: month, year
    """

    try:
        if '/' in date_str:
            # split the input string into month and year parts separated by '/'
            month_str, year_str = date_str.split('/')
            # convert the month and year strings to integers
            month, year = int(month_str), int(year_str)
        else:
            # extract the year and month parts from the input string
            year, month = int(date_str[:4]), int(date_str[4:])

        # check if the month is valid
        if month < 1 or month > 12:
            raise ValueError(f'>> Invalid month: {month}')

        # check if the year is valid
        if year < 1900 or year > 2100:
            raise ValueError(f'>> Invalid year: {year}')

        return month, year
    except ValueError as e:
        raise ValueError(f'>> Invalid date format: {date_str}') from e


def update_date(input_df: pd.DataFrame, software: str = 'COMPORTAMENT'):
    """
        update the date column of the input dataframe with the date extracted from the XML column.
        :param input_df: input dataframe, it must contain the columns 'XML' and 'DATA'
        :param software: software name, default is 'COMPORTAMENT'
        :return: None
    """

    try:
        comp_df = input_df[input_df['SOFTWARE'] == software]
        for i, row in comp_df.iterrows():
            for simple in et.fromstring(row['XML']).findall('attributes/simple'):
                simple_id = simple.find('id')
                if simple_id is not None and simple_id.text.lower() == 'refmonth':
                    month, year = parse_date(simple.find('value').text)
                    data = datetime.datetime(year=year, month=month, day=1, hour=1, minute=0, second=0, microsecond=0)
                    input_df.at[i, 'DATA'] = data.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    break
    except Exception as e:
        raise ValueError(f'>> Error while updating date: {e}')


def update_target(eval_df: pd.DataFrame) -> pd.DataFrame:
    """
        update the date column of the target dataset with the date extracted from the XML column.
        :param eval_df: input dataframe, it must contain the columns 'XML' and 'DATA'
        :return: updated dataframe
    """

    update_date(eval_df)
    eval_df.drop('XML', axis=1, inplace=True)
    clear_dataset_from_db(eval_df)
    eval_df.sort_values('DATA', inplace=True)
    return eval_df
