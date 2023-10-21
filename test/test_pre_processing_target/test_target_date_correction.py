#!/usr/bin/env python3

"""
@Author: Miro
@Date: 21/03/2022
@Version: 1.0
@Objective: test per correggere la data di creazione delle anomalie
@TODO:
"""

import unittest
from datetime import datetime
import pandas as pd
from kassandra.pre_processing_target.target_date_correction import parse_date, update_date, update_target


class TestTargetProcessing(unittest.TestCase):

    def test_parse_date(self):
        # test parsing of 'YYYYMM' format
        month, year = parse_date('201703')
        self.assertEqual(month, 3)
        self.assertEqual(year, 2017)

        # test parsing of 'MM/YYYY' format
        month, year = parse_date('10/2022')
        self.assertEqual(month, 10)
        self.assertEqual(year, 2022)

        # test parsing with invalid input
        with self.assertRaises(ValueError):
            parse_date('invalid')

        with self.assertRaises(ValueError):
            parse_date('13/2021')

        with self.assertRaises(ValueError):
            parse_date('202113')

    def test_update_date(self):
        # create a sample dataframe
        df = pd.DataFrame({
            'SOFTWARE': ['COMPORTAMENT', 'OTHER_SOFTWARE', 'COMPORTAMENT'],
            'XML': [
                '<main><attributes><simple><id>REFMONTH</id><value>202110</value></simple></attributes></main>',
                '<main><attributes><simple><id>OTHERID</id><value>2022/02</value></simple></attributes></main>',
                '<main><attributes><simple><id>REFMONTH</id><value>202201</value></simple></attributes></main>'
            ],
            'DATA': [None, None, None]
        })

        # call the update_date function
        update_date(df)

        # check if the date column has been updated correctly
        self.assertEqual(df.at[0, 'DATA'], datetime(2021, 10, 1, 1, 0, 0).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
        self.assertEqual(df.at[1, 'DATA'], None)
        self.assertEqual(df.at[2, 'DATA'], datetime(2022, 1, 1, 1, 0, 0).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

    def test_update_date_invalid_input(self):
        # create a sample dataframe with invalid input
        df = pd.DataFrame({
            'SOFTWARE': ['COMPORTAMENT'],
            'XML': [
                '<main><attributes><simple><id>REFMONTH</id><value>invalid_date</value></simple></attributes></main>'
            ],
            'DATA': [None]
        })

        # check if the function raises a ValueError for invalid input
        with self.assertRaises(ValueError):
            update_date(df)

    def test_update_target(self):
        eval_df = pd.DataFrame({
            'ID': [1, 2],
            'XML': ['<main><attributes><simple><id>REFMONTH</id><value>202211</value></simple></attributes></main>',
                    '<main><attributes><simple><id>REFMONTH</id><value>01/2022</value></simple></attributes></main>'],
            'SOFTWARE': ['COMPORTAMENT', 'COMPORTAMENT'],
            'DATA': ['2022-11-11 14:10:38.952', '2022-01-22 14:10:38.952']
        })
        updated_df = update_target(eval_df)
        self.assertIsInstance(updated_df, pd.DataFrame)
        self.assertEqual(updated_df.shape, (2, 3))
        self.assertEqual(updated_df['DATA'].tolist(), ['2022-01-01 01:00:00.000', '2022-11-01 01:00:00.000'])
        self.assertNotIn('XML', updated_df.columns)


if __name__ == '__main__':
    unittest.main()
