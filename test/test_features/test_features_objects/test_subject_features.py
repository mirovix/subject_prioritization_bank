#!/usr/bin/env python3

"""
@Author: Miro
@Date: 11/07/2023
@Version: 1.0
@Objective: test SubjectFeatures object
@TODO:
"""

import unittest
import pandas as pd
from unittest.mock import MagicMock
from kassandra.config_module import feature_creation_config as cfg
from kassandra.features_creation.features_objects.subject_features import SubjectFeatures, ObjectFeatures


class TestSubjectFeatures(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tmp_subject_dtypes = cfg.subjects_dtypes
        cfg.subjects_dtypes = str

    def setUp(self):
        self.list_values = MagicMock()

    @classmethod
    def tearDownClass(cls):
        cfg.subjects_dtypes = cls.tmp_subject_dtypes

    def test_age_categorization(self):
        """
            test age categorization function with different configurations
            :return:
        """

        start_date = ['1938-02-03 00:00:00.000000', '1950-10-30 00:00:00.000000', '1925-01-16 00:00:00.000000',
                      '', '1825-01-16 00:00:00.000000', '2025-01-16 00:00:00.000000']
        end_date = ['2023-02-03 00:00:00.000000', '2022-10-30 00:00:00.000000', '2017-05-16 00:00:00.000000',
                    '2023-02-03 00:00:00.000000', '2020-05-01 00:00:00.000000', '2025-01-16 00:00:00.000000']
        ndg_list = ['NDG1', 'NDG2', 'NDG3', 'NDG4', 'NDG5', 'NDG6']

        last_date_df = pd.DataFrame({cfg.end_date_operations: end_date, cfg.ndg_name: ndg_list})
        operation_object = MagicMock()
        operation_object.get_last_date.return_value = last_date_df

        age_df = pd.DataFrame({cfg.birth_column: start_date, cfg.ndg_name: ndg_list})
        tmp_subjects_columns = cfg.subjects_columns
        cfg.subjects_columns = [cfg.birth_column, cfg.ndg_name]

        expected_df = ['80-89', '70-79', '90-99', 'NOT_FOUND', 'NOT_FOUND', 'NOT_FOUND']
        subject_features = SubjectFeatures(age_df, self.list_values)
        subject_features._private_categorize_age(operation_object)
        ages = subject_features._private_dataframe[cfg.age_column].tolist()
        self.assertEqual(expected_df, ages)

        cfg.interval_age = 20
        expected_df = ['80-99', '60-79', '80-99', 'NOT_FOUND', 'NOT_FOUND', 'NOT_FOUND']
        subject_features = SubjectFeatures(age_df, self.list_values)
        subject_features._private_categorize_age(operation_object)
        ages = subject_features._private_dataframe[cfg.age_column].tolist()
        self.assertEqual(expected_df, ages)

        cfg.max_age = 110
        cfg.interval_age = 10
        expected_df = ['80-89', '70-79', '90-99', 'NOT_FOUND', 'NOT_FOUND', 'NOT_FOUND']
        subject_features = SubjectFeatures(age_df, self.list_values)
        subject_features._private_categorize_age(operation_object)
        ages = subject_features._private_dataframe[cfg.age_column].tolist()
        self.assertEqual(expected_df, ages)

        cfg.subjects_columns = tmp_subjects_columns

    def test_categorization(self):
        df_categorization = pd.DataFrame(['TEST1', 'TEST2', None, 'NOT_FOUND'], columns=['TEST'])
        tmp_subject_columns = cfg.subjects_columns
        cfg.subjects_columns = ['TEST']
        self.list_values.get_list_values.return_value = pd.DataFrame({'TEST': ['TEST1', 'TEST2']})

        expected_df = ['TEST', 'TEST', 'TEST_NONE', 'TEST_OTHER']
        subject_features = SubjectFeatures(df_categorization, self.list_values)
        subject_features._private_categorize(row_name='TEST', prefix_name='TEST')
        category = subject_features._private_dataframe['TEST'].tolist()
        self.assertEqual(expected_df, category)
        cfg.subjects_columns = tmp_subject_columns

    def test_call(self):
        """
            test call function
            :return:
        """

        df = pd.DataFrame({'TEST': ['TEST1']})
        tmp_subject_columns = cfg.subjects_columns
        tmp_residence_province_column = cfg.residence_province_column
        cfg.subjects_columns = ['TEST']
        cfg.residence_province_column = 'TEST'
        expected = [['TEST1', 'TEST1']]

        subject_features = SubjectFeatures(df, self.list_values)
        subject_features._private_categorize = MagicMock()
        subject_features._private_categorize_age = MagicMock()

        operation_object = ObjectFeatures(df, [], None)

        result = subject_features(matching_column_name='MATCHING_COLUMN_NAME', operation_object=operation_object).values.tolist()
        self.assertEqual(expected, result)
        cfg.subjects_columns = tmp_subject_columns
        cfg.residence_province_column = tmp_residence_province_column
