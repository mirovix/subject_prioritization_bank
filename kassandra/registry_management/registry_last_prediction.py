#!/usr/bin/env python3

"""
@Author: Miro
@Date: 02/10/2023
@Version: 1.0
@Objective: class for the registry management of the last prediction
@TODO:
"""

import sqlalchemy
import logging as log
import pandas as pd
from datetime import datetime
from kassandra.registry_management.registry import Registry
from kassandra.config_module import prediction_and_loading_config as cfg
from kassandra.config_module import registry_config as rc
from sqlalchemy import Column, String, Date, Float


class RegistryLastPrediction(Registry):
    def __init__(self,  engine: sqlalchemy):
        """
        :param engine: db engine
        """

        super().__init__(engine=engine, table_name=rc.registry_last_prediction_name,
                         index_name=rc.registry_last_prediction_index_name)
        self._private_columns_registry = [Column(rc.id_system_name, String(15), primary_key=True),
                                          Column(rc.control_code_name, String(10), nullable=False, primary_key=True),
                                          Column(rc.intermediary_code_name, String(11), nullable=False, primary_key=True),
                                          Column(rc.client_id_name, String(16), nullable=False, primary_key=True),
                                          Column(rc.report_date_name, Date, nullable=False, primary_key=False),
                                          Column(rc.prediction_name, Float, nullable=False, primary_key=False),
                                          Column(rc.model_name_date_name, String(16), nullable=False, primary_key=True)]

    def load(self, system_id: str, control_code: str, intermediary_code: str,
             prediction: pd.DataFrame, model_name_date: str,
             current_date: datetime = None, print_error: bool = True) -> bool:
        """
            load data into the database. If data with the same primary key already exists, update it.
            :param system_id: system id
            :param control_code: control code
            :param intermediary_code: intermediary code
            :param prediction: prediction data frame
            :param model_name_date: model name date (model name + date)
            :param current_date: current date (today by default)
            :param print_error: true if the error must be printed, False otherwise
            :return: true if the data is loaded or updated successfully, False otherwise
        """

        table = self.get_table()
        if table is None:
            if print_error:
                log.error(f">> Table: {self._private_table_name} does not exist.")
            return False

        if current_date is None:
            current_date = datetime.today().date()

        # change 'prediction' col name to uppercase
        prediction.rename(columns={rc.prediction_name.lower(): rc.prediction_name.upper()}, inplace=True)

        prediction[rc.prediction_name] = prediction[rc.prediction_name].astype(float).round(cfg.round_prediction_score)
        prediction[rc.id_system_name] = system_id
        prediction[rc.control_code_name] = control_code
        prediction[rc.intermediary_code_name] = intermediary_code
        prediction[rc.report_date_name] = current_date
        prediction[rc.model_name_date_name] = model_name_date

        # remove index
        prediction.reset_index(inplace=True)
        prediction.rename(columns={prediction.columns[0]: rc.client_id_name}, inplace=True)

        primary_key_columns = [rc.id_system_name, rc.control_code_name, rc.intermediary_code_name,
                               rc.client_id_name, rc.model_name_date_name]

        # order columns according to the table
        prediction = prediction[table.columns.keys()]

        try:
            last_predictions = self._private_engine.execute(table.select())
            last_predictions = pd.DataFrame(last_predictions, columns=table.columns.keys())

            # vertically concatenate the two dataframes
            prediction = pd.concat([prediction, last_predictions], ignore_index=True, axis=0)
            prediction.sort_values(by=[rc.client_id_name, rc.report_date_name], inplace=True)

            prediction.drop_duplicates(subset=primary_key_columns, keep='last', inplace=True)
            prediction.reset_index(inplace=True, drop=True)

            # update the table
            prediction.to_sql(self._private_table_name, self._private_engine, if_exists='replace', index=False)

            log.debug(">> Data from DataFrame loaded successfully.")
            return True
        except Exception as e:
            if print_error:
                log.error(f">> Data not loaded ({str(e)})")
            return False