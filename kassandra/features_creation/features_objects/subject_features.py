#!/usr/bin/env python3

"""
@Author: Miro
@Date: 27/06/2023
@Version: 1.0
@Objective: SubjectFeatures object
@TODO:
"""

import logging as log
import pandas as pd
from kassandra.config_module import feature_creation_config as cfg
from datetime import datetime
from kassandra.features_creation.features_objects.object_features import ObjectFeatures
from kassandra.features_creation.features_objects.categorization_data import ListValues


class SubjectFeatures(ObjectFeatures):
    def __init__(self, subjects: pd.DataFrame, list_values: ListValues):
        """
            :param subjects: subject information (NDG, ...)
            :param list_values: list values
        """

        super().__init__(subjects, cfg.subjects_columns, cfg.subjects_dtypes)
        self._private_list_values = list_values.get_list_values()

    def __call__(self, operation_object: ObjectFeatures, matching_column_name: str = 'SIGLA') -> pd.DataFrame:
        """
            :param operation_object: operation object
            :param matching_column_name: column name to be used for matching
            :return: preprocessed subject information
        """

        # disable panda's warning
        pd.options.mode.chained_assignment = None

        # add column for future matching
        self._private_dataframe[matching_column_name] = self._private_dataframe[cfg.residence_province_column].copy()

        # categorize ateco, sae and provinces
        log.debug('>> Categorizing ateco, sae and provinces')
        for row_name, prefix_name in cfg.subjects_columns_infix.items():
            self._private_categorize(row_name, prefix_name)

        # categorize age
        log.debug('>> Categorizing age')
        self._private_categorize_age(operation_object)

        # all the columns to upper case
        self._private_dataframe.columns = [col.upper() for col in self._private_dataframe.columns]

        return self._private_dataframe

    def _private_categorize(self, row_name: str, prefix_name: str,
                            other_suffix: str = 'OTHER', none_suffix: str = 'NONE') -> None:
        """
            common function used for categorized ateco, sae and provinces
            :param row_name: row name to be categorized (e.g. RESIDENCE_PROVINCE)
            :param prefix_name: prefix name for the new column (e.g. PRV is the prefix for PRV_1)
            :param other_suffix: suffix name for the other category (e.g. PRV_OTHER)
            :param none_suffix: suffix name for the none category (e.g. PRV_NONE)
            :return: None
        """

        cat_columns = [col for col in self._private_list_values.columns if col.startswith(prefix_name)]
        categories_df = self._private_list_values[cat_columns]
        categories_dict = {}
        for column in categories_df.columns:
            categories_dict[column] = categories_df[column].dropna().unique().tolist()

        # function for categorize the value
        def categorize_value(value):
            for category, values in categories_dict.items():
                if str(value) in values:
                    return category
            if str(value) in cfg.na_values_list or pd.isna(str(value)):
                return f'{prefix_name.upper()}_{none_suffix}'
            else:
                return f'{prefix_name.upper()}_{other_suffix}'

        # apply the function to the column
        self._private_dataframe[row_name] = self._private_dataframe[row_name].map(categorize_value)

    def _private_categorize_age(self, operation_object: ObjectFeatures, default_name: str = 'NOT_FOUND') -> None:
        """
            categorize age
            :param operation_object: object used for the operation
            :param default_name: default name for the category
            :return: None
        """

        last_operation_df = operation_object.get_last_date()
        self._private_dataframe[cfg.birth_column] = pd.to_datetime(self._private_dataframe[cfg.birth_column], errors='coerce')
        last_operation_df[cfg.end_date_operations] = pd.to_datetime(last_operation_df[cfg.end_date_operations], errors='coerce')

        # calculate age from birth date and last operation date (or last reported target) in years
        last_operation_df = pd.merge(self._private_dataframe, last_operation_df, on=cfg.ndg_name, how='left')
        last_operation_df[cfg.age_column] =(last_operation_df[cfg.end_date_operations] - last_operation_df[cfg.birth_column])
        last_operation_df[cfg.age_column] = last_operation_df[cfg.age_column].apply(lambda x: x.days) / cfg.days_in_year
        last_operation_df = last_operation_df[[cfg.ndg_name, cfg.age_column]]
        self._private_dataframe = pd.merge(self._private_dataframe, last_operation_df, on=cfg.ndg_name, how='left')

        # categorize age into intervals of n years
        bins = range(cfg.min_age, cfg.max_age, cfg.interval_age)
        labels = [f'{i}-{i + cfg.interval_age-1}' for i in bins[:-1]]

        self._private_dataframe[cfg.age_column] = pd.cut(self._private_dataframe[cfg.age_column], bins=bins, labels=labels)
        self._private_dataframe[cfg.age_column] = self._private_dataframe[cfg.age_column] \
            .cat.add_categories([default_name]) \
            .fillna(default_name)
        self._private_dataframe.drop(columns=[cfg.birth_column], inplace=True)
        self._private_dataframe[cfg.age_column] = self._private_dataframe[cfg.age_column].astype(object)
