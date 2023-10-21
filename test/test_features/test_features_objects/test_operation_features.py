#!/usr/bin/env python3

"""
@Author: Miro
@Date: 12/07/2023
@Version: 1.0
@Objective: test TargetFeatures object
@TODO:
"""

import unittest
import pandas as pd
import pandas.testing as pd_testing
from pandas import Timestamp
from kassandra.config_module import feature_creation_config as cfg
from kassandra.features_creation.features_objects.operation_features import OperationsSubjectsFeatures, OperationsFeatures
from kassandra.features_creation.features_objects.target_features import TargetFeatures
from kassandra.features_creation.features_objects.categorization_data import ListValues, AnalyticalCausal
from test.test_features import operation_subject_path, operation_path, target_path, operations_result_path


class TestOperationsSubjectsFeatures(unittest.TestCase):
    def test_get_operations_subjects(self):
        """
            Test get_operations_subjects method from OperationsSubjectsFeatures object
        """

        df = pd.DataFrame({cfg.ndg_name: ['1', '2', '3', '4', '4'],
                           cfg.subject_type_column: ['S', 'E', 'E', 'T', 'A']})



        operations_subjects_features = OperationsSubjectsFeatures(df, [cfg.ndg_name, cfg.subject_type_column], 'string')
        result = operations_subjects_features.get_operations_subjects()

        expected_df = pd.DataFrame({cfg.ndg_name: ['2', '3', '4'],
                                    cfg.subject_type_column: ['E', 'E', 'T']})

        expected_df.reset_index(drop=True, inplace=True)
        result.reset_index(drop=True, inplace=True)

        self.assertEqual(expected_df.to_dict(), result.to_dict())


class TestOperationsFeatures(unittest.TestCase):
    def setUp(self):
        self.target_data = pd.read_csv(target_path, delimiter=',', dtype='string')
        self.operations_subjects_data = pd.read_csv(operation_subject_path, delimiter=',', dtype='string')
        self.operations_data = pd.read_csv(operation_path, delimiter=',', dtype='string')

        self.target = TargetFeatures(self.target_data)

        self.operations_subjects = OperationsSubjectsFeatures(self.operations_subjects_data,
                                                              cfg.operations_subject_columns,
                                                              cfg.operations_subject_dtype)

    def test_get_last_date(self):
        """
            Test get_last_date method from OperationsFeatures object
        """

        operations = OperationsFeatures(dataframe=self.operations_data, columns=cfg.operations_columns, dtype='string',
                                        analytical_causal=AnalyticalCausal(), country_risk=ListValues(),
                                        target_object=self.target)
        self.assertIsNone(operations.get_last_date())

        operations.get_operations_subjects_merged(self.operations_subjects.get_operations_subjects())
        result = operations.get_last_date()

        self.assertEqual(result[cfg.ndg_name].loc[0], '0000000000000002')
        self.assertEqual(result[cfg.end_date_operations].loc[0], Timestamp('2018-07-01 01:00:00'))
        self.assertEqual(result[cfg.start_date_operations].loc[0], Timestamp('2017-07-01 01:00:00'))

        self.assertEqual(result[cfg.ndg_name].loc[1], '0000000000000001')
        self.assertEqual(result[cfg.end_date_operations].loc[1], Timestamp('2018-07-01 01:00:00'))
        self.assertEqual(result[cfg.start_date_operations].loc[1], Timestamp('2017-07-01 01:00:00'))

    def test_operations_processed(self):
        """
            Test the call method from OperationsFeatures object
        """

        operations = OperationsFeatures(dataframe=self.operations_data, columns=cfg.operations_columns, dtype='string',
                                        analytical_causal=AnalyticalCausal(), country_risk=ListValues(),
                                        target_object=self.target)

        result = operations(self.operations_subjects.get_operations_subjects())
        expected_df = pd.read_csv(operations_result_path, delimiter=',', header=0, dtype={cfg.ndg_name: 'string'})

        pd_testing.assert_frame_equal(result, expected_df, check_dtype=False, check_like=True, check_exact=False)