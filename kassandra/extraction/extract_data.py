#!/usr/bin/env python3

"""
@Author: Miro
@Date: 07/06/2023
@Version: 1.0
@Objective: extraction package of the data
@TODO: Ottimizzazione di get_target (eliminare da self.target quei elementi presenti ndgs_from_other_systems)
"""

import pandas as pd
import sqlalchemy
import logging as log
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy.exc import SQLAlchemyError
from kassandra.config_module.db_config import oracle_name
from kassandra.config_module.extraction_config import operations_subject_query_path, operations_query_path, \
    subjects_query_path, ndg_list_path, ndg_name, date_target_col, status_target_col, format_date, \
    anomalies_other_systems_path, cols_names_evaluation_csv, dtypes_evaluation_csv, default_software
from kassandra.registry_management import Registry
from kassandra.pre_processing_target.target_date_correction import update_target


class ExtractData:
    def __init__(self, engine_evaluation: sqlalchemy, engine_dwa: sqlalchemy,
                 registry: Registry, system_id: str, control_code: str, intermediary_code: str,
                 ref_month: str, registry_month_to_skip: int, reported_other_systems: list = None,
                 chunk_size: int = 500):
        """
            init
            :param engine_evaluation: sqlalchemy engine object to connect to the database
            :param engine_dwa: sqlalchemy engine object to connect to the database
            :param registry: registry object to manage the registry table
            :param system_id: system id
            :param control_code: control code
            :param intermediary_code: intermediary code
            :param ref_month: reference month (e.g. 062023)
            :param registry_month_to_skip: number of months to skip in the registry to be reported again
            :param reported_other_systems: list of systems that have already reported the ndg
            :param chunk_size: chunk size to read the data from the database
        """

        self.engine_dwa = engine_dwa
        self.engine_evaluation = engine_evaluation

        self.system_id = str(system_id)
        self.control_code = str(control_code)
        self.intermediary_code = str(intermediary_code)
        self.ref_month_start, self.ref_month_end = self._private_ref_month_to_date(ref_month)
        self.registry_month_to_skip = registry_month_to_skip
        self.reported_other_systems = reported_other_systems

        self.ndgs_from_other_systems = []
        self.ndgs_from_registry = []

        self.ndg_list = None
        self.operations_subject_df = None
        self.operations_df = None
        self.subjects_df = None
        self.target_df = None
        self.anomaly_processed_df = None

        self.registry = registry

        self.ndg_list_query = self._private_read_query(ndg_list_path)
        self.anomalies_other_system_query = self._private_read_query(anomalies_other_systems_path)
        self.operations_subject_query = self._private_read_query(operations_subject_query_path)
        self.operations_query = self._private_read_query(operations_query_path)
        self.subjects_query = self._private_read_query(subjects_query_path)

        self.chunk_size = chunk_size

    def __call__(self):
        """
            call method to process the extraction
        """

        self._private_define_ndgs_to_process()

    @staticmethod
    def _private_read_query(query_path) -> str:
        """
            read query
            :param query_path:
            :return: query as string
        """

        try:
            with open(query_path, 'r') as file:
                query = file.read()
                file.close()
        except FileNotFoundError:
            raise FileNotFoundError(f'>> Query file not found: {query_path}')
        return query

    @staticmethod
    def _private_ref_month_to_date(ref_month: str) -> (str, str):
        """
            convert ref month to date
            :param ref_month: reference month (e.g. 062023)
            :return: first day and last day of the month
        """

        try:
            month = int(ref_month[:2])
            year = int(ref_month[2:])
        except ValueError:
            raise ValueError(f'>> Invalid ref month: {ref_month}')

        first_day = datetime(year, month, 1)
        next_month = first_day.replace(day=28) + timedelta(days=4)
        last_day = next_month - timedelta(days=next_month.day)
        return first_day.strftime(format_date), last_day.strftime(format_date)

    def _private_get_start_date(self) -> str:
        """
            get start date to extract data from the database
            :return: str start date
        """

        # convert the input date string to a datetime object and remove the number of months
        date = datetime.strptime(datetime.now().strftime('%Y-%m-%d'), format_date)
        start_date = date - relativedelta(months=self.registry_month_to_skip)
        return start_date.strftime(format_date)

    def _private_get_ndg_from_registry(self) -> list:
        """
            get ndg list from registry
            :return: list of ndg to not process
        """

        try:
            self.registry_month_to_skip = int(self.registry_month_to_skip)
        except ValueError:
            raise ValueError(f">> Invalid registry month to skip: {self.registry_month_to_skip}")

        if self.registry_month_to_skip == 0: return []

        ndgs = self.registry.get_client_id_to_not_process(system_id=self.system_id,
                                                          control_code=self.control_code,
                                                          intermediary_code=self.intermediary_code,
                                                          start_date=self._private_get_start_date(),
                                                          end_date=datetime.now().strftime(format_date))
        return ndgs

    def _private_get_ndg_from_other_systems(self) -> list:
        """
            get ndg list from other systems (e.g. Comportamenti/Day) to not process
            :return: list of ndgs to avoid
        """

        if self.reported_other_systems is None or len(self.reported_other_systems) == 0:
            self.target_df = self._private_target_query()
            return []

        if not isinstance(self.reported_other_systems, list):
            raise ValueError(f"Invalid reported other systems: {self.reported_other_systems}")

        for system in self.reported_other_systems:
            if not isinstance(system, str):
                raise ValueError(f">> Invalid reported other systems: {self.reported_other_systems}")

        try:
            self.target_df = self._private_target_query(list(set(default_software) - set(self.reported_other_systems)))
            ndgs = self._private_target_query(self.reported_other_systems)[ndg_name].tolist()
        except Exception as e:
            raise SQLAlchemyError(f">> Error in getting ndg from other systems: {e}")
        return ndgs

    def _private_target_query(self, software_systems: list = default_software):
        """
            completing target query
            :param software_systems: list of systems to process
            :return: target query
        """
        systems = "(" + ", ".join(f"'{item}'" for item in software_systems) + ")"
        if len(software_systems) == 0:
            systems = "('')"

        query = self.anomalies_other_system_query % (self.intermediary_code, systems)
        ndgs_df = pd.read_sql(query, self.engine_evaluation).astype(dtypes_evaluation_csv)
        ndgs_df.columns = cols_names_evaluation_csv
        target_df = update_target(eval_df=ndgs_df)
        target_df.dropna(subset=[ndg_name], inplace=True)
        return target_df

    def _private_define_ndgs_to_process(self):
        """
            get ndg list to process
            :return: list of ndg to process
        """

        # define the query (with or without date, for solving the problem of the different date format in Oracle)
        if self.engine_dwa.name == oracle_name:
            ndg_query = self.ndg_list_query % (self.intermediary_code, 'DATE', self.ref_month_start, 'DATE', self.ref_month_end)
        else:
            ndg_query = self.ndg_list_query % (self.intermediary_code, '', self.ref_month_start, '', self.ref_month_end)

        # get ndg list
        ndg_list_df = pd.read_sql_query(ndg_query, self.engine_dwa).astype(str)
        ndg_list_df.columns = [ndg_name]
        self.ndg_list = ndg_list_df[ndg_name].tolist()

        log.info(f">> Number of ndg to process: {len(self.ndg_list)}")

        if len(self.ndg_list) < 1: return

        # save ndg that are in the registry
        self.ndgs_from_registry = self._private_get_ndg_from_registry()
        log.info(f">> Number of ndg present in the registry: {len(self.ndgs_from_registry)}")

        # save ndg that are in other systems
        self.ndgs_from_other_systems = self._private_get_ndg_from_other_systems()
        log.info(f">> Number of ndg present in other systems: {len(self.ndgs_from_other_systems)}")

    def _private_common_function_extraction(self, query: str) -> pd.DataFrame:
        """
            common function to extract data
            :param query: query to execute
            :return: DataFrame
        """

        if self.ndg_list is None: self.__call__()

        if len(self.ndg_list) < 1: exit(0)

        ndg_chunk = [self.ndg_list[i:i + self.chunk_size] for i in range(0, len(self.ndg_list), self.chunk_size)]

        dfs = []
        for ndgs in ndg_chunk:
            ndgs_str = "(" + ", ".join(f"'{item}'" for item in ndgs) + ")"
            dfs.append(pd.read_sql_query(query % (self.intermediary_code, ndgs_str), self.engine_dwa))

        return pd.concat(dfs)

    def get_ndgs(self) -> list:
        """
            get ndg list
            :return: ndg to process
        """

        if self.ndg_list is None: self.__call__()
        return self.ndg_list

    def get_operations_subjects(self) -> pd.DataFrame:
        """
            get operations subjects
            :return: pandas dataframe
        """

        self.operations_subject_df = self._private_common_function_extraction(self.operations_subject_query)
        return self.operations_subject_df

    def get_subjects(self) -> pd.DataFrame:
        """
            get subjects data
            :return: pandas dataframe
        """

        self.subjects_df = self._private_common_function_extraction(self.subjects_query)
        return self.subjects_df

    def get_operations(self) -> pd.DataFrame:
        """
            get operations data
            :return: pandas dataframe
        """

        self.operations_df = self._private_common_function_extraction(self.operations_query)
        return self.operations_df

    def get_target(self) -> pd.DataFrame:
        """
            get target data
            :return: pandas dataframe
        """

        if self.target_df is None: self._private_get_ndg_from_other_systems()
        return self.target_df

    def get_anomaly_processed(self) -> pd.DataFrame:
        """
            get last anomaly processed data
            :return: pandas dataframe
        """

        if self.anomaly_processed_df is None:
            self.anomaly_processed_df = self._private_target_query([self.system_id])
            self.anomaly_processed_df.sort_values(by=[ndg_name, date_target_col], inplace=True)
            self.anomaly_processed_df.drop_duplicates(subset=[ndg_name], keep='last', inplace=True)
            self.anomaly_processed_df.reset_index(drop=True, inplace=True)
            self.anomaly_processed_df = self.anomaly_processed_df[[ndg_name, date_target_col, status_target_col]]
        return self.anomaly_processed_df

    def get_ndgs_from_registry(self) -> list:
        """
            get ndg from registry
            :return: list of ndg
        """

        return self.ndgs_from_registry

    def get_ndgs_from_other_systems(self) -> list:
        """
            get ndg from other systems
            :return: list of ndg
        """

        return self.ndgs_from_other_systems