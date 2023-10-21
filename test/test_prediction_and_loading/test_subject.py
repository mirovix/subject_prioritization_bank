#!/usr/bin/env python3

"""
@Author: Miro
@Date: 16/06/2022
@Version: 1.0
@Objective: test per della classe Subject
@TODO:
"""

import unittest
import pandas as pd
from kassandra.prediction_and_loading.subject import Subject
from kassandra.prediction_and_loading.features import Features, FeatureCategories


class TestSubject(unittest.TestCase):
    def setUp(self) -> None:
        features_categories = FeatureCategories(description_features={},
                                                boolean_features=[],
                                                binary_features=[],
                                                numerical_features=[],
                                                currency_features=[])

        features = Features(features_values=pd.DataFrame(),
                            features_contribution=pd.Series(),
                            features_name=features_categories,
                            max_features=0,
                            min_contribution=0.0)

        self.subject = Subject(ndg='123456789', name='test', fiscal_code='test', residence_country='86',
                               birthday='1929-02-17 00:00:00.000', juridical_nature='PG',
                               residence_city='test', sae='600', ateco='600',
                               office='test', score=0.5, features_values=features)

    def test_pre_process_input(self):
        """
            test per la funzione pre_process_input
        """

        result = self.subject.pre_process_input(input_to_process='123456789', input_length=9)
        self.assertEqual(result, '123456789')

        result = self.subject.pre_process_input(input_to_process='123456789', input_length=10)
        self.assertEqual(result, '0123456789')

        with self.assertRaises(ValueError):
            self.subject.pre_process_input(input_to_process='123456789', input_length=8)

        result = self.subject.pre_process_input(input_to_process='600.0')
        self.assertEqual(result, '600')

        result = self.subject.pre_process_input(input_to_process='600.0', input_length=5)
        self.assertEqual(result, '00600')

        result = self.subject.pre_process_input(input_to_process='', input_length=6)
        self.assertEqual(result, '')

        result = self.subject.pre_process_input(input_to_process='nan', input_length=6)
        self.assertEqual(result, '')

        result = self.subject.pre_process_input(input_to_process='None', input_length=6)
        self.assertEqual(result, '')

        self.assertEqual(self.subject.juridical_nature, 'PG')

    def test_birthday_date_convert(self):
        """
            test per la funzione birthday_date_convert
        """

        result = self.subject.birthday_date_convert(birthday='1929-02-17 00:00:00.000')
        self.assertEqual(result, "17/02/1929")

        result = self.subject.birthday_date_convert(birthday='')
        self.assertEqual(result, '')

        with self.assertRaises(ValueError):
            self.subject.birthday_date_convert(birthday='1929-0')


