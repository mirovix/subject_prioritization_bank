#!/usr/bin/env python3

"""
@Author: Miro
@Date: 11/07/2023
@Version: 1.0
@Objective: test class for the categorization of the data
@TODO:
"""

import unittest
import numpy as np
from kassandra.features_creation.features_objects.categorization_data import ProvinceCategorization, \
    ListValues, AnalyticalCausal
from kassandra.config_module import feature_creation_config as cfg


class TestCategorizationData(unittest.TestCase):
    def test_province_categorization(self):
        """
            test province categorization object
            :return: None
        """

        alto, medio, basso = 'ALTO', 'MEDIO', 'BASSO'
        category = cfg.province_categorization_dict[0][0]

        tmp_province_categorization_dict = cfg.province_categorization_dict
        tmp_province_features_to_not_process = cfg.province_features_to_not_process

        cfg.province_categorization_dict = [[category, {alto: np.inf, medio: np.inf, basso: 0.0}]]
        cfg.province_features_to_not_process = [cfg.province_features_to_not_process[0]]

        province = ProvinceCategorization()
        province_processed = province()

        # assert
        self.assertEqual(province_processed.shape[0], 107)
        self.assertEqual(province_processed.shape[1], 2)

        self.assertEqual(province_processed[category.upper() + ' RANGED'].unique().tolist(), [basso])

        with self.assertRaises(ValueError):
            cfg.province_categorization_dict = [['Wrong', {alto: 1.5, medio: 0.5, basso: 0.0}]]
            province = ProvinceCategorization()()
            _ = province()
            
        cfg.province_categorization_dict = tmp_province_categorization_dict
        cfg.province_features_to_not_process = tmp_province_features_to_not_process

    def test_list_values(self):
        """
            test list values object
            :return: None
        """

        list_values = ListValues()
        list_values_processed = list_values.get_list_values()
        list_values_risk = list_values.get_country_risk()

        # assert
        self.assertIsNotNone(list_values_processed)
        self.assertTrue(len(list_values_risk) > 0)

        for e in list_values_risk:
            self.assertTrue(isinstance(e, str))

    def test_analytical_causal(self):
        """
            test analytical causal object
            :return: None
        """

        analytical_causal = AnalyticalCausal()
        analytical_causal_processed = analytical_causal.get_analytical_causal()

        # assert
        self.assertIsNotNone(analytical_causal_processed)