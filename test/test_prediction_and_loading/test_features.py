#!/usr/bin/env python3

"""
@Author: Miro
@Date: 16/06/2022
@Version: 1.0
@Objective: test per della classe Features, Feature e FeatureCategories
@TODO:
"""

import unittest
from unittest.mock import patch
import pandas as pd
from kassandra.prediction_and_loading.features import FeatureCategories, Features
from test.test_prediction_and_loading import subjects_path


class TestFeatures(unittest.TestCase):
    def setUp(self) -> None:
        self.subjects_df = pd.read_csv(subjects_path)

        # define the features name
        empty_features_categories = FeatureCategories(description_features={},
                                                      boolean_features=[],
                                                      binary_features=[],
                                                      numerical_features=[],
                                                      currency_features=[])

        # define the features
        self.features = Features(features_values=self.subjects_df,
                                 features_contribution=pd.Series(),
                                 features_name=empty_features_categories,
                                 max_features=5)

    @patch('kassandra.prediction_and_loading.features.Features._private_get_features_contribution_str')
    def test_process_features(self, mock_get_features_contribution_str):
        """
            test for the function _private_process_features
        """

        mock_get_features_contribution_str.return_value = 'test'

        # define the contribution series
        contribution = pd.Series()
        for cols in self.subjects_df.columns:
            contribution[cols] = 1.0

        # call the function
        self.features()

        for feature in self.features.list_features:
            self.assertEqual(feature.contribution_str, 'test')
            self.assertEqual(feature.contribution, 1.0)
            self.assertEqual(feature.percentage_contribution, 3.57)
            self.assertEqual(feature.name in self.subjects_df.columns, True)

    def test_get_features_contribution_str(self):
        """
            test for the function _private_get_features_contribution_str
        """

        self.features.features_name.boolean_features = ['test1']
        self.features.features_values['test1'] = [1]
        result = self.features._private_get_features_contribution_str('test1')
        self.assertEqual(result, 'Si')
        self.features.features_values['test1'] = [0]
        result = self.features._private_get_features_contribution_str('test1')
        self.assertEqual(result, None)

        self.features.features_name.binary_features = ['test2']
        self.features.features_values['test2'] = [1]
        result = self.features._private_get_features_contribution_str('test2')
        self.assertEqual(result, '1')

        self.features.features_name.numerical_features = ['test3']
        self.features.features_values['test3'] = [2.5]
        result = self.features._private_get_features_contribution_str('test3')
        self.assertEqual(result, '2')
        self.features.features_values['test3'] = [0]
        result = self.features._private_get_features_contribution_str('test3')
        self.assertEqual(result, None)

        self.features.features_name.currency_features = ['test4']
        self.features.features_values['test4'] = [3000]
        result = self.features._private_get_features_contribution_str('test4')
        self.assertEqual(result, '€3000.00')
        self.features.features_values['test4'] = [0]
        result = self.features._private_get_features_contribution_str('test4')
        self.assertEqual(result, None)

        self.features.features_name.risk_profile_name = 'test5'
        self.features.features_values['test5'] = [2]
        result = self.features._private_get_features_contribution_str('test5')
        self.assertEqual(result, 'Medio')

        with self.assertRaises(ValueError):
            self.features._private_get_features_contribution_str('test6')
