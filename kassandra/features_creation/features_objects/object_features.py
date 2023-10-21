#!/usr/bin/env python3

"""
@Author: Miro
@Date: 28/06/2023
@Version: 1.0
@Objective: ObjectFeatures object
@TODO:
"""

import pandas as pd


class ObjectFeatures:
    def __init__(self, dataframe: pd.DataFrame, columns: list, dtype: any):
        """
            :param generic dataframe object
        """
        self._private_dataframe = self._private_preprocess(dataframe, columns, dtype)


    @staticmethod
    def _private_preprocess(dataframe: pd.DataFrame, columns: list, dtype: any) -> pd.DataFrame:
        """
            :param dataframe: subject information (NDG, ...)
            :param columns: columns names
            :param dtype: columns types
            :return: preprocessed subject information
        """

        try:
            # name cols to upper case
            dataframe.columns = dataframe.columns.str.upper()
            dataframe = dataframe[columns]

            if not isinstance(dtype, dict):
                dataframe = dataframe.astype(dtype)
            else:
                for key, value in dtype.items():
                    dataframe.loc[:, key] = dataframe[key].astype(value)
        except Exception as e:
            raise AttributeError(f'>> Error in _private_preprocess: {e}')

        return dataframe