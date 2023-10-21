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
from datetime import datetime
from unittest.mock import patch
from kassandra.config_module import feature_creation_config as cfg
from kassandra.features_creation.features_objects.target_features import TargetFeatures


class TestTargetFeatures(unittest.TestCase):
    def setUp(self):
        cfg.target_data_source, cfg.status_column_source = 'DATA', 'STATO'
        self.target_df = pd.DataFrame({cfg.ndg_name: ['01', '02', '03'],
                                       cfg.target_data_source: ['2017-03-01 01:00:00.000',
                                                                '2018-03-22 01:10:00.000',
                                                                '2019-09-01 01:00:00.000'],
                                       cfg.status_column_source: ['NOT_TO_ALERT',
                                                                  'TO_ALERT',
                                                                  'TO_ALERT']})
        cfg.target_columns = [cfg.ndg_name, cfg.target_data_source, cfg.status_column_source]
        cfg.target_dtype = 'string'

    def test_get_target(self):
        """
            Test the get_target method
        """

        target = TargetFeatures(self.target_df)
        target_df = target.get_target()

        self.assertEqual(target_df.shape[0], 3)
        self.assertEqual(target_df.shape[1], 3)

        given_date = '2018-01-01 01:00:00.000'

        target = TargetFeatures(self.target_df, given_date=given_date)
        target_df = target.get_target()

        self.assertEqual(target_df.shape[0], 1)
        self.assertEqual(target_df.shape[1], 3)

    @patch('kassandra.features_creation.features_objects.target_features.datetime')
    def test_get_target_to_alert(self, mock_datetime):
        """
            Test the get_target_to_alert method
        """

        target = TargetFeatures(self.target_df)
        target_df = target.get_to_alert()

        self.assertEqual(target_df.shape[0], 2)
        self.assertEqual(target_df.shape[1], 2)
        self.assertEqual(target_df.index.tolist(), ['02', '03'])

        mock_datetime.now.return_value = datetime(2023, 7, 1, 1, 0, 0, 0)

        lower_bound_month = 12
        target = TargetFeatures(self.target_df, lower_bound_month=lower_bound_month)
        self.assertEqual(target.get_to_alert().values.tolist(), [])

        mock_datetime.now.return_value = datetime(2020, 7, 1, 1, 0, 0, 0)
        target = TargetFeatures(self.target_df, lower_bound_month=lower_bound_month)
        target_df = target.get_to_alert()
        self.assertEqual(target_df.index.tolist(), ['03'])


    def test_get_not_to_alert_count(self):
        """
            Test the get_not_to_alert_count method
        """

        target = TargetFeatures(self.target_df)
        last_operation_df = pd.DataFrame({cfg.ndg_name: ['01', '02', '03'],
                                          cfg.end_date_operations: ['2017-04-01 01:00:00.000',
                                                                    '2018-05-22 01:10:00.000',
                                                                    '2019-09-01 01:00:00.000'],
                                          cfg.start_date_operations: ['2016-04-01 01:00:00.000',
                                                                      '2017-04-22 01:10:00.000',
                                                                      '2018-09-01 01:00:00.000']})


        result = target.get_not_to_alert_count(last_operation_df).values.tolist()
        self.assertEqual(result, [['01', 1]])


    def test_get_status(self):
        """
            Test the get_status method
        """

        target = TargetFeatures(self.target_df)
        target_df = target.get_status()

        self.assertEqual(target_df.index.tolist(), ['02', '03'])
        self.assertEqual(target_df.values.tolist(), ['1', '1'])
