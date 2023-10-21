#!/usr/bin/env python3

"""
@Author: Miro
@Date: 14/06/2023
@Version: 1.0
@Objective: oggetto che contiene le informazioni delle features di un soggetto
@TODO:
"""

import pandas as pd
from kassandra.config_module.prediction_and_loading_config import boolean_features_values, currency_features_values, \
    risk_profile_values, round_percentage, round_contributions


class FeatureCategories:
    def __init__(self, description_features: dict, boolean_features: list, binary_features: list,
                 numerical_features: list, currency_features: list, risk_profile_name: str = 'RISK_PROFILE'):
        """
            :param description_features: dictionary that contains the description of the features
            :param boolean_features: list of boolean features
            :param binary_features: list of binary features
            :param numerical_features: list of numerical features
            :param currency_features: list of currency features
            :param risk_profile_name: name of the risk profile feature
        """

        self.description_features = description_features
        self.boolean_features = boolean_features
        self.binary_features = binary_features
        self.numerical_features = numerical_features
        self.currency_features = currency_features
        self.risk_profile_name = risk_profile_name


class Features:
    def __init__(self, features_values: pd.DataFrame, features_contribution: pd.Series,
                 features_name: FeatureCategories, max_features: int, min_contribution: float = 0.0):
        """
            :param features_values: subject features value
            :param features_contribution: subject features contribution to the prediction
            :param features_name: object that contains the names of the features
            :param max_features: max number of features to show
            :param min_contribution: min contribution of the features to show
        """

        self.list_features = []

        if isinstance(features_values, pd.Series):
            features_values = features_values.to_frame().T
        self.features_values = features_values

        self.features_contribution = features_contribution
        self.features_name = features_name
        self.max_features = max_features
        self.min_contribution = min_contribution

    def __call__(self):
        self._private_process_features()

    def _private_process_features(self) -> None:
        """
            process the features and create a list of Feature objects
        """

        # make positive values
        self.features_contribution = self.features_contribution.abs()

        # pair columns with values
        count_features = 0
        for column in self.features_contribution.index:
            value = round(abs(self.features_contribution[column]), round_contributions)
            if value <= self.min_contribution: continue

            contribution_str = self._private_get_features_contribution_str(column)
            if contribution_str is None: continue

            percentage = round((value / self.features_contribution.sum()) * 100, round_percentage)

            self.list_features.append(Feature(column, value, percentage, contribution_str))

            # sort features by percentage
            self._private_sort_features_by_percentage()

            count_features += 1

            if count_features >= self.max_features: break

    def _private_get_features_contribution_str(self, name: str) -> any:
        """
            define the string that will be used for printing in the xml
            :param name: feature name
            :return: string that will be used for printing in the xml
        """

        if name in self.features_name.boolean_features:
            if int(self.features_values[name].iloc[0]) == 0: return None
            return boolean_features_values[int(self.features_values[name].iloc[0])]
        elif name in self.features_name.binary_features:
            return str(self.features_values[name].iloc[0].round(1))
        elif name in self.features_name.numerical_features:
            if int(self.features_values[name].iloc[0]) == 0: return None
            return str(int(self.features_values[name].iloc[0]))
        elif name in self.features_name.currency_features:
            if int(self.features_values[name].iloc[0]) == 0: return None
            return currency_features_values + f"{self.features_values[name].iloc[0]:.2f}"
        elif name == self.features_name.risk_profile_name:
            return risk_profile_values[int(self.features_values[name].iloc[0])]
        else:
            raise ValueError(f">> Feature {name} not found")

    def _private_sort_features_by_percentage(self) -> None:
        """
            sort features by percentage
        """

        self.list_features = sorted(self.list_features, key=lambda f: f.percentage_contribution, reverse=True)


class Feature:
    def __init__(self, name: str, contribution: float, percentage_contribution: float, contribution_str: str):
        """
            :param name: feature name
            :param contribution: feature contribution
            :param percentage_contribution: feature percentage contribution
            :param contribution_str: feature that will be used for printing in the xml
        """

        self.name = name
        self.contribution = contribution
        self.percentage_contribution = percentage_contribution
        self.contribution_str = contribution_str
