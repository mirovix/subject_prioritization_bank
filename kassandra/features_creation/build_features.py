#!/usr/bin/env python3

"""
@Author: Miro
@Date: 26/06/2023
@Version: 1.0
@Objective: BuildFeatures object
@TODO:
"""

import os
import pandas as pd
import logging as log
from kassandra.config_module import app_config as apc
from kassandra.features_creation.features_objects.categorization_data import ProvinceCategorization, AnalyticalCausal
from kassandra.features_creation.features_objects.subject_features import SubjectFeatures
from kassandra.features_creation.features_objects.categorization_data import ListValues
from kassandra.features_creation.features_objects.operation_features import OperationsSubjectsFeatures, OperationsFeatures
from kassandra.features_creation.features_objects.target_features import TargetFeatures
from kassandra.config_module import feature_creation_config as cfg


class BuildFeatures:
    def __init__(self, subjects: pd.DataFrame, operations: pd.DataFrame,
                 operations_subjects: pd.DataFrame, target_information: pd.DataFrame,
                 months_operations: int, given_date: str = None, bank_months: int = None) -> None:
        """
            :param subjects: subject information (NDG, ...)
            :param operations: operations information (COD_OPERATION, ...)
            :param operations_subjects: operations subjects information (NDG, COD_OPERATION, ...)
            :param target_information: target information (STATUS, DATE_REPORT, ...)
            :param months_operations: number of months for operations features
            :param given_date: date of the report
            :param bank_months: number of months of the bank in the database
        """

        log.debug('>> Initializing BuildFeatures object')
        log.debug(f'>> Given date: {given_date}')
        log.debug(f'>> Bank months: {bank_months}')
        log.debug(f'>> Months operations: {months_operations}')

        list_values = ListValues()
        self._private_subjects = SubjectFeatures(subjects, list_values)

        n_months = None
        if bank_months is not None:
            n_months = bank_months - months_operations
        self._private_target = TargetFeatures(target_information, given_date, n_months)

        self._private_operations = OperationsFeatures(dataframe=operations, columns=cfg.operations_columns, dtype=cfg.operations_dtype,
                                                      analytical_causal=AnalyticalCausal(), country_risk=list_values,
                                                      months_operations=months_operations, target_object=self._private_target,
                                                      last_anomaly_date=given_date)

        self._private_operations_subjects = OperationsSubjectsFeatures(operations_subjects,
                                                                       cfg.operations_subject_columns,
                                                                       cfg.operations_subject_dtype)

        self._private_province_country_cat = ProvinceCategorization()

        self._private_column_trans_file = 'encoder.joblib'
        self._private_column_names_file = 'column_names.joblib'

    def __call__(self) -> pd.DataFrame:
        """
            extract subject and operations features and merge them
            :return: dataframe with all the features for the model
        """

        log.info('>> Building features...')

        # operation features
        log.info('>> Processing operations features...')
        operations_features = self._private_operations(self._private_operations_subjects.get_operations_subjects())
        operations_features.drop_duplicates(subset=[cfg.ndg_name], keep='first', inplace=True)

        matching_column_name_province = 'SIGLA'

        log.info(">> Processing province features...")
        provinces_processed = self._private_province_country_cat()

        log.info('>> Processing subject features...')
        subjects_processed = self._private_subjects(self._private_operations, matching_column_name_province)

        subjects_provinces = pd.merge(subjects_processed, provinces_processed, on=matching_column_name_province, how='left')

        # take only the columns that are necessary
        subjects_provinces = subjects_provinces[cfg.output_features_subject]

        inputs = pd.merge(operations_features, subjects_provinces, on=cfg.ndg_name, how="left")
        inputs.drop_duplicates(subset=[cfg.ndg_name], keep='first')

        inputs.set_index(cfg.ndg_name, inplace=True)

        # define the dtype of the columns
        for col in cfg.non_categorical_columns:
            inputs[col] = inputs[col].astype('float64')

        # fill the missing values with 0 where columns are not 'object' type and with 'other' where columns are 'object' type
        log.info('>> Filling missing values...')
        for col in inputs.columns:
            if inputs[col].dtype != 'object':
                inputs[col].fillna(0, inplace=True)
            else:
                inputs[col].fillna('other'.upper(), inplace=True)

        return inputs

    @staticmethod
    def _check_encoders_folder():
        if not os.path.exists(apc.encoders_directory):
            os.mkdir(apc.encoders_directory)