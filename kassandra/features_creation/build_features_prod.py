#!/usr/bin/env python3

"""
@Author: msalihi
@Date: 05/07/2023
@Version: 1.0
@Objective: BuildFeaturesProd object
@TODO:
"""

import logging as log
import pandas as pd
import joblib
import shap
import kassandra.config_module.feature_creation_config as cfg
from kassandra.features_creation.build_features import BuildFeatures
from kassandra.config_module import app_config as apc


class BuildFeaturesProd(BuildFeatures):
    def __init__(self, operations: pd.DataFrame, operation_subjects: pd.DataFrame, subjects: pd.DataFrame,
                 target_information: pd.DataFrame, bank_months: int = None) -> None:
        """
            :param operations: operations information (COD_OPERATION, ...)
            :param operation_subjects: operations subjects information (NDG, COD_OPERATION, ...)
            :param subjects: subject information (NDG, ...)
            :param target_information: target information (STATUS, DATE_REPORT, ...)
            :param bank_months: number of months (operations) saved in the bank's database
        """

        super().__init__(subjects=subjects, operations=operations, operations_subjects=operation_subjects,
                         target_information=target_information, months_operations=cfg.operations_months_features,
                         bank_months=bank_months)
        self._private_dataset = super().__call__()

    def get_transformed_prod(self) -> pd.DataFrame:
        """
            method for transforming the data for production
            :return: transformed data
        """

        try:
            log.debug('>> Loading encoding information')
            self._check_encoders_folder()
            column_trans = joblib.load(f'{apc.encoders_directory}/' + self._private_column_trans_file)
            column_names = joblib.load(f'{apc.encoders_directory}/' + self._private_column_names_file)
        except Exception as e:
            raise FileNotFoundError(f'>> Error while loading the column transformer: {e}')

        try:
            log.info('>> Transforming the data with encoder')
            dataset_trans = column_trans.transform(self._private_dataset)
            dataset_trans_df = pd.DataFrame(dataset_trans, columns=column_names).astype(float)

            if dataset_trans_df.empty:
                return pd.DataFrame([], columns=column_names)

            dataset_trans_df.set_index(self._private_dataset.index, inplace=True)
        except Exception as e:
            raise ValueError(f'>> Error while transforming the train data: {e}')

        return dataset_trans_df

    def get_dataset_prod(self) -> pd.DataFrame:
        """
            method for getting the dataset for production
            :return: dataset for production
        """

        return self._private_dataset

    @staticmethod
    def get_shap_explainer(model_object: any, explainer: shap = shap.Explainer) -> any:
        """
            method for creating the shap explainer
            :param model_object: model to explain its predictions (must have a predict method)
            :param explainer: shap object (default: shap.Explainer)
            :return: shap explainer object for the given model
        """

        try:
            shap_df = pd.read_csv(apc.data_directory + cfg.x_train_name, sep=',', dtype=float)
        except Exception as e:
            raise FileNotFoundError(f'>> Error while loading the test csv for the explainer: {e}')

        try:
            return explainer(model_object.predict, shap_df)
        except Exception as e:
            raise ValueError(f'>> Error while creating the explainer: {e}')
