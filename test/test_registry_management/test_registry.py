#!/usr/bin/env python3

"""
@Author: Miro
@Date: 22/05/2023
@Version: 1.0
@Objective: class for the registry management test
@TODO:
"""

import unittest
import pandas as pd
from sqlalchemy.orm import declarative_base
from datetime import datetime
from kassandra.config_module import registry_config as rc
from kassandra.registry_management.registry import Registry
from test import create_in_memory_engine

Base = declarative_base()

idx = 0


class TestRegistry(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_in_memory_engine()

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_get_load_data_on_registry(self):
        """
            test get client id to not process in the registry table
            :return:
        """

        intermediary_code = "000000"
        control_code = "000000"
        system_id = "000000"

        # init registry
        registry = Registry(self.engine)

        # create registry table
        registry.create_registry_table_index()

        # add value to registry table
        registry.load_on_registry(system_id=system_id,
                                  control_code=control_code,
                                  intermediary_code=intermediary_code,
                                  client_id="0",
                                  current_date=datetime(year=2023, month=5, day=1),
                                  prediction=0.5,
                                  model_name_date="model_name_date")

        registry.load_on_registry(system_id=system_id,
                                  control_code=control_code,
                                  intermediary_code=intermediary_code,
                                  client_id="1",
                                  current_date=datetime(year=2023, month=2, day=2),
                                  prediction=0.5,
                                  model_name_date="model_name_date")

        # verify the function get_client_id_to_not_process

        expected_list = ['0']
        id_to_not_process = registry.get_client_id_to_not_process(system_id=system_id,
                                                                  control_code=control_code,
                                                                  intermediary_code=intermediary_code,
                                                                  start_date='2023-03-01',
                                                                  end_date='2023-05-02')
        self.assertEqual(id_to_not_process, expected_list)

        expected_list = ['0', '1']
        id_to_not_process = registry.get_client_id_to_not_process(system_id=system_id,
                                                                  control_code=control_code,
                                                                  intermediary_code=intermediary_code,
                                                                  start_date='2023-01-01',
                                                                  end_date='2023-05-02')
        self.assertEqual(id_to_not_process, expected_list)

        # verify the function get_last_prediction
        expected_last_prediction = pd.DataFrame({rc.ndg_name: ['0', '1'],
                                                 rc.prediction_name: [0.5, 0.7],
                                                 rc.model_name_date_name: ['model_name_date', 'model_name_date']})

        registry.load_on_registry(system_id=system_id,
                                  control_code=control_code,
                                  intermediary_code=intermediary_code,
                                  client_id="1",
                                  current_date=datetime(year=2023, month=3, day=2),
                                  prediction=0.7,
                                  model_name_date="model_name_date")

        last_prediction = registry.get_last_prediction(system_id=system_id,
                                                       control_code=control_code,
                                                       intermediary_code=intermediary_code)
        pd.testing.assert_frame_equal(last_prediction, expected_last_prediction)

    def test_create_table_index_registry(self):
        """
            test create table and index registry
            :return:
        """

        registry = Registry(self.engine)
        self.assertIsNone(registry.get_table())

        registry.create_registry_table_index()
        self.assertIsNotNone(registry.get_table())

    def test_execute_query(self):
        """
            test execute query
            :return:
        """

        registry = Registry(self.engine)
        registry.create_registry_table_index()

        query = "select * from " + registry.get_table_name()
        self.assertTrue(registry.execute_query(query))

    def test_delete_registry(self):
        """
            test delete registry table
            :return:
        """

        registry = Registry(self.engine)
        self.assertIsNone(registry.get_table())

        registry.create_registry_table_index()
        self.assertIsNotNone(registry.get_table())

        registry.delete_registry()
        self.assertIsNone(registry.get_table())

    def test_exceptions(self):
        """
            test exceptions to the registry class
            :return:
        """

        registry = Registry(None)

        self.assertEqual(registry.delete_registry(), False)
        self.assertEqual(registry.load_on_registry("", "", "", "", 0.0, ""), False)
        self.assertEqual(registry.execute_query(""), False)
        self.assertEqual(registry.create_registry_table_index(), False)
        self.assertEqual(registry.get_table(), None)
