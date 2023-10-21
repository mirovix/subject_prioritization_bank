#!/usr/bin/env python3

"""
@Author: Miro
@Date: 27/06/2023
@Version: 1.0
@Objective: ProvinceCategorization object, ListValues object, Analysis object
@TODO:
"""

import pandas as pd
from kassandra.config_module import feature_creation_config as cfg


class ProvinceCategorization:
    def __init__(self):
        self._private_province = None
        self._private_province_processed = pd.DataFrame()

    def __call__(self) -> (pd.DataFrame, pd.DataFrame):
        """
            load and process country/province categorization
            :return: province and country categorization
        """

        try:
            # load province/country categorization
            self._private_load_province_list()

            # categorize province
            for column, dict_value_category in cfg.province_categorization_dict:
                self._private_province_categorization(column, dict_value_category)

            # add province that are not in the categorization
            for column in cfg.province_features_to_not_process:
                self._private_province_processed[column.upper()] = self._private_province[column]
        except Exception as e:
            raise ValueError(f'>> Error loading and processing province/country categorization: {e}')

        return self._private_province_processed

    def _private_load_province_list(self) -> None:
        """
            :return: Province/Country categorization dataframe
        """

        self._private_province = pd.read_excel(cfg.country_province_path, sheet_name=cfg.province_sheet_name,
                                               keep_default_na=False, na_values=cfg.na_values_list,
                                               usecols=cfg.province_columns)

    def _private_province_categorization(self, column: str, dict_value_category: dict, default_category: str = 'NONE',
                                         infix_feature_name: str = 'RANGED') -> None:
        """
            Categorize province according to the province categorization
            :param column: column to be categorized (e.g. 'Associazione di tipo mafioso')
            :param dict_value_category: dictionary with value and category (e.g. {'Alto': 0.22})
            :param default_category: default category name in case of no match
            :param infix_feature_name: infix to be added to the feature name (e.g. 'Associazione di tipo mafioso ranged')
            :return: None
        """

        # sort the dictionary by value
        sorted_dict = dict(sorted(dict_value_category.items(), key=lambda x: x[1], reverse=True))

        category_list = []
        for province_value in self._private_province[column]:
            category = default_category
            for key, value in sorted_dict.items():
                if province_value >= value:
                    category = key
                    break
            category_list.append(category)

        self._private_province_processed[column.upper() + ' ' + infix_feature_name] = category_list


class ListValues:
    def __init__(self):
        self._private_list_values = self._private_load_list_values()

    @staticmethod
    def _private_load_list_values() -> pd.DataFrame:
        """
            :return: list values
        """

        return pd.read_csv(cfg.list_values_path, delimiter=cfg.list_values_delimiter,
                           dtype=cfg.list_values_dtype, keep_default_na=False,
                           na_values=cfg.na_values_list)

    def get_list_values(self) -> pd.DataFrame:
        """
            :return: list values dataframe
        """

        return self._private_list_values

    def get_country_risk(self, highest_risk_name: str = 'RISCHIO_PAESE_ALTISSIMO',
                         high_risk_name: str = 'RISCHIO_PAESE_ALTO') -> list:
        """
            :return: list of high risk countries
        """

        # extract the list of high risk countries
        highest_risk = self._private_list_values[highest_risk_name].dropna().values.tolist()
        high_risk = self._private_list_values[high_risk_name].dropna().values.tolist()

        return highest_risk + high_risk


class AnalyticalCausal:
    def __init__(self):
        self._private_analytical_causal = self._private_load_analytical_causal()

    @staticmethod
    def _private_load_analytical_causal() -> pd.DataFrame:
        """
            :return: list values
        """

        return pd.read_csv(cfg.analytical_causal_path, usecols=cfg.analytical_causal_columns,
                           delimiter=cfg.analytical_causal_delimiter, dtype=cfg.analytical_causal_dtype)

    def get_analytical_causal(self) -> pd.DataFrame:
        """
            :return: analytical causal dataframe
        """

        return self._private_analytical_causal