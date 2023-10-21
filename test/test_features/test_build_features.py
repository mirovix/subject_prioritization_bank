#!/usr/bin/env python3

"""
@Author: Miro
@Date: 20/07/2023
@Version: 1.0
@Objective: test BuildFeatures object
@TODO:
"""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
import pandas.testing as pd_testing

from kassandra.config_module import app_config as apc
from kassandra.config_module import feature_creation_config as cfg
from kassandra.features_creation.build_features import BuildFeatures
from kassandra.features_creation.build_features_prod import BuildFeaturesProd
from kassandra.features_creation.build_features_train import BuildFeaturesTrain
from kassandra.features_creation.features_objects.target_features import TargetFeatures
from test.test_features import operation_subject_path, operation_path, target_path, \
    subject_path, build_features_result_path, transformed_train_path


class TestBuildFeatures(unittest.TestCase):
    def setUp(self) -> None:
        self.target_data = pd.read_csv(target_path, delimiter=',', dtype='string')
        self.operations_subjects_data = pd.read_csv(operation_subject_path, delimiter=',', dtype='string')
        self.operations_data = pd.read_csv(operation_path, delimiter=',', dtype='string')
        self.subject_data = pd.read_csv(subject_path, delimiter=',', dtype=cfg.subjects_dtypes, keep_default_na=False)

        self.target = TargetFeatures(self.target_data)

        month = 12
        self.build_features = BuildFeatures(self.subject_data, self.operations_data, self.operations_subjects_data, self.target_data, month)
        self.build_features_given_date = BuildFeatures(self.subject_data, self.operations_data, self.operations_subjects_data,
                                                       self.target_data, month, '2018-01-01', 24)

    def test_call_build_features(self):
        result = pd.read_csv(build_features_result_path, delimiter=',', index_col=0, dtype={cfg.ndg_name: 'string'})
        pd_testing.assert_frame_equal(result, self.build_features(), check_dtype=False, check_like=True, check_exact=False)
        self.assertTrue(self.build_features_given_date().empty)

    @patch('joblib.load', MagicMock())
    @patch('kassandra.features_creation.BuildFeaturesProd._check_encoders_folder', MagicMock())
    def test_build_features_prod(self):
        build_features_prod = BuildFeaturesProd(self.operations_data, self.operations_subjects_data, self.subject_data, self.target_data)

        result = build_features_prod.get_transformed_prod()
        self.assertTrue(result.empty)

    def test_build_features_train(self):
        tmp_save_model = apc.save_model
        apc.save_model = False
        build_features_train = BuildFeaturesTrain(self.operations_data, self.operations_subjects_data, self.subject_data,
                                                  self.target_data, '3018-01-01', 0.5, False, 0)

        x_train, _, y_train, _ = build_features_train.get_transformed_train_test()
        result = pd.read_csv(transformed_train_path, delimiter=',', dtype={cfg.ndg_name: 'string'})

        self.assertTrue(isinstance(y_train, pd.Series))
        self.assertTrue(isinstance(x_train, pd.DataFrame))

        pd_testing.assert_frame_equal(result, x_train, check_dtype=False, check_like=True, check_exact=False)
        apc.save_model = tmp_save_model
