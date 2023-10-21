#!/usr/bin/env python3

"""
@Author: Miro
@Date: 11/07/2023
@Version: 1.0
@Objective: test ObjectFeatures object
@TODO:
"""

import pandas as pd
from unittest import TestCase
from kassandra.features_creation.features_objects.object_features import ObjectFeatures


class TestObjectFeatures(TestCase):
    def test_preprocess(self):
        columns = ['A', 'B', 'C']
        dtype = {'A': int}
        dataframe = pd.DataFrame([[1, 'a', 1.0], [2, 'b', 2.0], [3, 'c', 3.0]], columns=columns)

        general_object = ObjectFeatures(dataframe, ['A'], dtype)

        self.assertEqual(general_object._private_dataframe.shape, (3, 1))
        self.assertEqual(general_object._private_dataframe.columns.tolist(), ['A'])
        self.assertEqual(general_object._private_dataframe.dtypes['A'], 'int64')

        string_dtype = 'str'
        general_object = ObjectFeatures(dataframe, ['A'], string_dtype)

        self.assertEqual(general_object._private_dataframe.dtypes['A'], object)

        with self.assertRaises(AttributeError):
            ObjectFeatures(dataframe, ['A'], 'dtype')