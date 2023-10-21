#!/usr/bin/env python3

"""
@Author: Miro
@Date: 22/05/2023
@Version: 1.0
@Objective: class for the registry management
@TODO:
"""

import pandas as pd
import sqlalchemy
import logging as log
from kassandra.config_module import registry_config as rc
from datetime import datetime
from sqlalchemy import Index, Table, MetaData, exc, insert, Column, String, Date, Float


class Registry:
    def __init__(self, engine: sqlalchemy, table_name: str = rc.registry_kassandra_name,
                 index_name: str = rc.registry_kassandra_index_name):
        """
            init class
            :param engine: db engine
            :param table_name: table name
            :param index_name: index name
        """

        self._private_engine = engine
        self._private_metadata = MetaData()
        self._private_table_name = table_name.lower()
        self._private_index_name = index_name.lower()
        self._private_columns_registry = [Column(rc.id_system_name, String(15), primary_key=True),
                                          Column(rc.control_code_name, String(10), nullable=False, primary_key=True),
                                          Column(rc.intermediary_code_name, String(11), nullable=False, primary_key=True),
                                          Column(rc.client_id_name, String(16), nullable=False, primary_key=True),
                                          Column(rc.report_date_name, Date, nullable=False, primary_key=True),
                                          Column(rc.prediction_name, Float, nullable=False, primary_key=False),
                                          Column(rc.model_name_date_name, String(16), nullable=False, primary_key=False)]

    def get_table_name(self) -> str:
        """
            get table name
            :return: table's name
        """

        return self._private_table_name

    def get_columns_registry(self) -> list:
        """
            get columns of the table
            :return: columns of the table
        """

        return self._private_columns_registry

    def get_client_id_to_not_process(self, system_id: str, control_code: str, intermediary_code: str,
                                     start_date: str, end_date: str) -> list:
        """
            get client id to not process in the registry table
            :param system_id: system id to search
            :param control_code: control code to search
            :param intermediary_code: intermediary code to search
            :param start_date: start date to search
            :param end_date: end date to search
            :return: list of client id to not process
        """

        table = self.get_table()

        log.debug(f'>> get_client_id_to_not_process: {system_id}, {control_code}, {intermediary_code}, {start_date}, {end_date}')

        try:
            query = table.select()\
                    .where(table.c[rc.report_date_name] >= start_date) \
                    .where(table.c[rc.report_date_name] <= end_date) \
                    .where(table.c[rc.id_system_name] == system_id) \
                    .where(table.c[rc.control_code_name] == control_code) \
                    .where(table.c[rc.intermediary_code_name] == intermediary_code)
            result = self._private_engine.execute(query)
        except Exception as e:
            raise sqlalchemy.exc.SQLAlchemyError(e)

        return [row[rc.client_id_name] for row in result]

    def get_last_prediction(self, system_id: str, control_code: str, intermediary_code: str) -> pd.DataFrame:
        """
            get last prediction
            :param system_id: system id to search
            :param control_code: control code to search
            :param intermediary_code: intermediary code to search
            :return: last prediction
        """

        table = self.get_table()

        log.debug(f'>> get_last_prediction: {system_id}, {control_code}, {intermediary_code}')

        try:
            query = table.select()\
                    .where(table.c[rc.id_system_name] == system_id) \
                    .where(table.c[rc.control_code_name] == control_code) \
                    .where(table.c[rc.intermediary_code_name] == intermediary_code)
            result = self._private_engine.execute(query)
        except Exception as e:
            raise sqlalchemy.exc.SQLAlchemyError(e)

        last_predictions_df = pd.DataFrame(result, columns=result.keys())
        last_predictions_df[rc.client_id_name] = last_predictions_df[rc.client_id_name].astype(str)
        last_predictions_df[rc.report_date_name] = pd.to_datetime(last_predictions_df[rc.report_date_name])
        last_predictions_df.sort_values(by=[rc.client_id_name, rc.report_date_name, rc.model_name_date_name], inplace=True)
        last_predictions_df.drop_duplicates(subset=[rc.client_id_name, rc.model_name_date_name], keep='last', inplace=True)
        last_predictions_df.reset_index(drop=True, inplace=True)
        last_predictions_df = last_predictions_df[[rc.client_id_name, rc.prediction_name, rc.model_name_date_name]]
        last_predictions_df = last_predictions_df.rename(columns={rc.client_id_name: rc.ndg_name})
        return last_predictions_df

    def delete_registry(self) -> bool:
        """
            delete registry table
            :return: True if the table is deleted, False otherwise
        """

        table = self.get_table()

        # verify if the table exists
        if table is not None:
            # drop table from the database
            if self._private_index_name is not None:
                index = Index(self._private_index_name, *table.c)
                index.drop(self._private_engine)
            table.drop(self._private_engine)
            log.info(f">> Table: {self._private_table_name} deleted successfully.")
            return True
        else:
            log.warning(f">> Table: {self._private_table_name} does not exists.")
            return False

    def create_registry_table_index(self) -> bool:
        """
            create registry table
            :return: True if the table is created, False otherwise
        """

        # create table
        registry_table = Table(self._private_table_name, self._private_metadata, *self.get_columns_registry())

        try:
            self._private_metadata.create_all(self._private_engine)
            log.info(f">> Table: {self._private_table_name} created successfully.")
        except Exception as e:
            log.error(f">> Table not created ({str(e)})")
            return False

        # create index
        registry_index = Index(self._private_index_name, *registry_table.c)

        try:
            registry_index.create(self._private_engine)
            log.info(f">> Index: {self._private_index_name} created successfully.")
        except Exception as e:
            log.warning(f">> Index not created ({str(e)})")
            self._private_index_name = None
        return True

    def execute_query(self, query: str) -> bool:
        """
            add new column to the table
            :param query: query to execute
            :return: True if the query is executed, False otherwise
        """

        table = self.get_table()

        # verify if the table exists
        if table is not None:
            try:
                self._private_engine.execute(query)
                log.info(f">> Query: {query} executed successfully.")
                return True
            except Exception as e:
                log.error(f">> Query not executed ({str(e)})")
        else:
            log.warning(f">> Table: {self._private_table_name} does not exists.")
        return False

    def load_on_registry(self, system_id: str, control_code: str, intermediary_code: str,
                         client_id: str, prediction: float, model_name_date: str,
                         current_date: datetime = None) -> bool:
        """
            load data on registry
            :param system_id: system id
            :param control_code: control code
            :param intermediary_code: intermediary code
            :param client_id: client id
            :param prediction: prediction value to load
            :param model_name_date: model name date to load on registry
            :param current_date: current date to load
            :return: True if the data is loaded, False otherwise
        """

        if current_date is None:
            current_date = datetime.today().date()

        data = {rc.id_system_name: system_id,
                rc.control_code_name: control_code,
                rc.intermediary_code_name: intermediary_code,
                rc.client_id_name: client_id,
                rc.report_date_name: current_date,
                rc.prediction_name: prediction,
                rc.model_name_date_name: model_name_date}

        return self.load_on_db(data)

    def load_on_db(self, data: dict, print_error: bool = True) -> bool:
        """
            load data on db
            :param data: dict with the data to load
            :param print_error: True if the error must be printed, False otherwise
            :return:
        """

        table = self.get_table()
        if table is None:
            if print_error:
                log.warning(f">> Table: {self._private_table_name} does not exists.")
            return False

        try:
            self._private_engine.execute(insert(table).values(data))
            log.debug(">> " + str(data) + " loaded successfully.")
            return True
        except Exception as e:
            if print_error:
                log.warning(f">> Data not loaded ({str(e)})")
            return False

    def get_table(self):
        """
            get table object
            :return: table's object or None
        """

        try:
            self._private_metadata.reflect(bind=self._private_engine, only=[self._private_table_name])
            table = Table(self._private_table_name, self._private_metadata, autoload=True)
        except exc.InvalidRequestError:
            return None

        return table
