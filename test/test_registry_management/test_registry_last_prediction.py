#!/usr/bin/env python3

"""
@Author: Miro
@Date: 02/10/2023
@Version: 1.0
@Objective: class for the registry management of the last prediction test
@TODO:
"""

import unittest
import pandas as pd
from sqlalchemy.orm import declarative_base
from datetime import datetime
from kassandra.config_module import registry_config as rc
from kassandra.registry_management.registry_last_prediction import RegistryLastPrediction
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
        registry = RegistryLastPrediction(self.engine)

        # create registry table
        registry.create_registry_table_index()

        # add value to registry table
        registry.load(system_id=system_id,
                      control_code=control_code,
                      intermediary_code=intermediary_code,
                      current_date=datetime(year=2022, month=7, day=11),
                      prediction=pd.DataFrame({rc.ndg_name.lower(): ['0', '1'],
                                               rc.prediction_name.lower(): [0.5, 0.6]}),
                      model_name_date="model_name_date")

        # verify the function get_client_id_to_not_process

        expected_result = pd.DataFrame({rc.ndg_name: ["0", "1"],
                                        rc.prediction_name: [0.50000, 0.60000],
                                        rc.model_name_date_name: ["model_name_date", "model_name_date"]})
        last_predictions_df = registry.get_last_prediction(system_id=system_id,
                                                           control_code=control_code,
                                                           intermediary_code=intermediary_code)

        pd.testing.assert_frame_equal(expected_result, last_predictions_df)

        registry.load(system_id=system_id,
                      control_code=control_code,
                      intermediary_code=intermediary_code,
                      prediction=pd.DataFrame({rc.ndg_name.lower(): ['0'],
                                               rc.prediction_name.lower(): [0.2]}),
                      model_name_date="model_name_date")

        expected_result = pd.DataFrame({rc.ndg_name: ["0", "1"],
                                        rc.prediction_name: [0.20000, 0.60000],
                                        rc.model_name_date_name: ["model_name_date", "model_name_date"]})
        last_predictions_df = registry.get_last_prediction(system_id=system_id,
                                                           control_code=control_code,
                                                           intermediary_code=intermediary_code)

        pd.testing.assert_frame_equal(expected_result, last_predictions_df)