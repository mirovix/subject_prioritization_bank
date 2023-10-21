#!/usr/bin/env python3

"""
@Author: Miro
@Date: 08/06/2023
@Version: 1.0
@Objective: class for extraction of the data test
@TODO:
"""

import unittest
import pandas as pd
import sqlalchemy
from unittest.mock import patch
from sqlalchemy.exc import SQLAlchemyError
from kassandra.config_module.extraction_config import operations_subject_query_path, operations_query_path, \
    subjects_query_path, ndg_list_path, anomalies_other_systems_path, ndg_name, cols_names_evaluation_csv
from kassandra.extraction.extract_data import ExtractData
from kassandra.registry_management import Registry
from test.test_extraction import target


class TestExtractData(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = sqlalchemy.create_engine('sqlite:///:memory:')

    def test_read_query(self):
        """
            test read query
        """

        self.assertTrue(len(ExtractData._private_read_query(ndg_list_path)) > 0)
        self.assertTrue(len(ExtractData._private_read_query(anomalies_other_systems_path)) > 0)
        self.assertTrue(len(ExtractData._private_read_query(operations_subject_query_path)) > 0)
        self.assertTrue(len(ExtractData._private_read_query(operations_query_path)) > 0)
        self.assertTrue(len(ExtractData._private_read_query(subjects_query_path)) > 0)

        with self.assertRaises(FileNotFoundError):
            ExtractData._private_read_query('wrong_path')

    def test_ref_month_to_date(self):
        """
            test ref month to date
        """

        ref_month_start, ref_month_end = ExtractData._private_ref_month_to_date('062023')
        self.assertEqual(ref_month_start, '2023-06-01')
        self.assertEqual(ref_month_end, '2023-06-30')

        ref_month_start, ref_month_end = ExtractData._private_ref_month_to_date('012023')
        self.assertEqual(ref_month_start, '2023-01-01')
        self.assertEqual(ref_month_end, '2023-01-31')

        with self.assertRaises(ValueError):
            ExtractData._private_ref_month_to_date('202301')

    @patch('kassandra.extraction.extract_data.Registry.get_client_id_to_not_process')
    def test_get_ndg_from_registry(self, mock_get_client_id_to_not_process):
        """
            test get ndg from registry
        """

        registry = Registry(engine=None)
        expected_ndg_list = ['1', '2', '3']
        mock_get_client_id_to_not_process.return_value = expected_ndg_list

        extract_data_obj = ExtractData(engine_evaluation=self.engine, engine_dwa=self.engine,
                                       registry=registry, system_id='1', control_code='1',
                                       intermediary_code='1', ref_month='062023',
                                       registry_month_to_skip=1)

        self.assertEqual(extract_data_obj._private_get_ndg_from_registry(), expected_ndg_list)

        extract_data_obj.registry_month_to_skip = 0
        self.assertEqual(extract_data_obj._private_get_ndg_from_registry(), [])

        with self.assertRaises(ValueError):
            extract_data_obj.registry_month_to_skip = 'wrong_month'
            extract_data_obj._private_get_ndg_from_registry()

    def test_get_ndg_from_other_systems(self):
        """
            test get ndg from other systems
        """

        expected_ndg_list = ['100717708', '100480272', '100840389', '100649174', '100817840']
        expected_ndg_list_df = pd.read_csv(target, header=0, names=cols_names_evaluation_csv)
        systems = ['SYSTEM1', 'SYSTEM2', 'SYSTEM3']

        extract_data_obj = ExtractData(engine_evaluation=sqlalchemy, engine_dwa=sqlalchemy,
                                       registry=Registry(None), system_id='1', control_code='1',
                                       intermediary_code='1', ref_month='062023',
                                       registry_month_to_skip=1, reported_other_systems=systems)

        with patch('pandas.read_sql') as mock_get_ndg_from_registry:
            mock_get_ndg_from_registry.return_value = expected_ndg_list_df
            self.assertEqual(extract_data_obj._private_get_ndg_from_other_systems(), expected_ndg_list)

        with self.assertRaises(SQLAlchemyError):
            extract_data_obj.engine_evaluation = None
            extract_data_obj._private_get_ndg_from_other_systems()

        with self.assertRaises(ValueError):
            extract_data_obj.reported_other_systems = 'wrong_system'
            extract_data_obj._private_get_ndg_from_other_systems()

        with self.assertRaises(ValueError):
            extract_data_obj.reported_other_systems = [None, 1]
            extract_data_obj._private_get_ndg_from_other_systems()

    @patch('kassandra.extraction.extract_data.ExtractData._private_get_ndg_from_registry')
    @patch('kassandra.extraction.extract_data.ExtractData._private_get_ndg_from_other_systems')
    @patch('pandas.read_sql_query')
    def test_get_ndg_list(self, mock_read_sql_query, mock_get_ndg_from_other_systems,
                          mock_get_ndg_from_registry):
        """
            test get ndg list
        """
        ndg_list = ['1', '2', '3', '4', '5', '6', '7', '8']
        registry_list = ['1', '2', '6']
        other_system_ndg = ['1', '2', '3', '4', '5']

        mock_get_ndg_from_registry.return_value = registry_list
        mock_get_ndg_from_other_systems.return_value = other_system_ndg
        mock_read_sql_query.return_value = pd.DataFrame(ndg_list, columns=[ndg_name])

        extract_data_obj = ExtractData(engine_evaluation=self.engine, engine_dwa=self.engine, registry=Registry(None),
                                       system_id='1', control_code='1', intermediary_code='1', ref_month='062023',
                                       registry_month_to_skip=1, reported_other_systems=['SYSTEM1'])
        extract_data_obj()
        self.assertEqual(extract_data_obj.get_ndgs(), ndg_list)
        self.assertEqual(extract_data_obj.get_ndgs_from_registry(), registry_list)
        self.assertEqual(extract_data_obj.get_ndgs_from_other_systems(), other_system_ndg)

    @patch('kassandra.extraction.extract_data.ExtractData._private_get_ndg_from_registry')
    @patch('kassandra.extraction.extract_data.ExtractData._private_get_ndg_from_other_systems')
    def test_common_function_extraction(self, mock_get_ndg_from_other_systems, mock_get_ndg_from_registry):
        """
            test get operations subjects
            :return:
        """

        mock_get_ndg_from_registry.return_value = []
        mock_get_ndg_from_other_systems.return_value = []
        operations_subject_cols = ['CODE_OPERATION', 'NDG', 'SUBJECT_TYPE']
        expected_value = pd.DataFrame([['1A', '1', 'T'], ['2A', '2', 'T'], ['3A', '3', 'T']], columns=[operations_subject_cols])

        extract_data_obj = ExtractData(engine_evaluation=self.engine, engine_dwa=self.engine, registry=Registry(None),
                                       system_id='1', control_code='1', intermediary_code='060459', ref_month='062022',
                                       registry_month_to_skip=1, reported_other_systems=['SYSTEM1'])

        with patch('pandas.read_sql_query') as mock_read_sql_query:
            mock_read_sql_query.return_value = pd.DataFrame(['1', '2', '3'], columns=[ndg_name])
            extract_data_obj()

        with patch('pandas.read_sql_query') as mock_read_sql_query:
            mock_read_sql_query.return_value = expected_value
            query = extract_data_obj._private_read_query(operations_subject_query_path)
            self.assertTrue(expected_value.equals(extract_data_obj._private_common_function_extraction(query)))
