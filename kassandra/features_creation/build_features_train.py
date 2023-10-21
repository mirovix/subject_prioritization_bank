#!/usr/bin/env python3

"""
@Author: msalihi
@Date: 05/07/2023
@Version: 1.0
@Objective: BuildFeaturesTrain object
@TODO:
"""

import os
import pandas as pd
import joblib
import kassandra.config_module.feature_creation_config as cfg
from kassandra.features_creation.build_features import BuildFeatures
from kassandra.config_module import app_config as apc
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import make_column_transformer


class BuildFeaturesTrain(BuildFeatures):
    def __init__(self, operations: pd.DataFrame, operation_subjects: pd.DataFrame, subjects: pd.DataFrame,
                 target_information: pd.DataFrame, given_date: str, test_size: float, shuffle: bool,
                 random_state: int, stratify: any = None) -> None:
        """
            :param operations: operations information (COD_OPERATION, ...)
            :param operation_subjects: operations subjects information (NDG, COD_OPERATION, ...)
            :param subjects: subject information (NDG, ...)
            :param target_information: target information (STATUS, DATE_REPORT, ...)
            :param given_date: last date of the report
            :param test_size: test size for the split
            :param shuffle: shuffle for the split
            :param random_state: random state for reproducibility
            :param stratify: stratify for the split
        """

        super().__init__(subjects=subjects, operations=operations, operations_subjects=operation_subjects,
                         target_information=target_information, months_operations=cfg.operations_months_features,
                         given_date=given_date)

        self._test_size = test_size
        self._shuffle = shuffle
        self._random_state = random_state
        self._stratify = stratify

        self._x_train = None
        self._x_test = None
        self._y_train = None
        self._y_test = None

    def _get_split_data_inputs(self) -> None:
        """
            get the split data inputs (x_train, x_test, y_train, y_test)
            :return: None
        """

        x = super().__call__()
        y = self._private_target.get_status()

        # for each ndg present in x and not in y create a new row with status 0
        x = pd.merge(x, y, how='left', left_index=True, right_index=True)
        x[cfg.status_column].fillna('0', inplace=True)

        # drop duplicates and keep the last one
        x.drop_duplicates(keep='last', inplace=True)

        y = x[cfg.status_column]
        x.drop(cfg.status_column, axis=1, inplace=True)

        print(f"y shape {y.value_counts()}")

        self._x_train, self._x_test, self._y_train, self._y_test = train_test_split(x, y,
            test_size=self._test_size, shuffle=self._shuffle, random_state=self._random_state,
            stratify=self._stratify)

    def _get_column_transformer(self) -> (any, list):
        """
            get the column transformer and the column names after transformation with one hot encoder
            :return: column transformer, column names
        """

        try:
            self._get_split_data_inputs()
        except Exception as e:
            raise ValueError(f'>> Error while splitting the data: {e}')

        object_columns = self._x_train.select_dtypes(include='object').columns

        column_trans = make_column_transformer((OneHotEncoder(handle_unknown='ignore'), object_columns),
                                               remainder='passthrough')

        column_trans.fit(self._x_train)

        # get the transformed column names
        transformed_columns = column_trans.get_feature_names_out()

        # get the encoder names
        encoder_names = [name for name in transformed_columns if name in transformed_columns]
        encoder_names = [name.replace('onehotencoder__', '') for name in encoder_names]

        # get the remainder names
        remainder_names = [name for name in encoder_names if name in encoder_names]

        # get the columns names
        column_names = [name.replace('remainder__', '').upper() for name in remainder_names]
        print(f"column names {column_names}")

        if apc.save_model:
            try:
                self._check_encoders_folder()
                joblib.dump(column_trans, f'{apc.encoders_directory}/' + self._private_column_trans_file)
                joblib.dump(column_names, f'{apc.encoders_directory}/' + self._private_column_names_file)
            except Exception as e:
                raise ValueError(f'>> Error while saving the encoders: {e}')

        return column_trans, column_names

    def get_transformed_train_test(self) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame):
        """
            transform the train and test data
            :return: x_train, x_test, y_train, y_test (dataframes)
        """

        # get the columns names
        try:
            column_trans, column_names = self._get_column_transformer()
        except Exception as e:
            raise ValueError(f'>> Error while getting the column transformer: {e}')

        try:
            # transform the train
            x_train_trans = column_trans.transform(self._x_train)
            x_train_trans_df = pd.DataFrame(x_train_trans, columns=column_names).astype(float)

            # transform the test
            x_test_trans = column_trans.transform(self._x_test)
            x_test_trans_df = pd.DataFrame(x_test_trans, columns=column_names).astype(float)
        except Exception as e:
            raise ValueError(f'>> Error while transforming the data: {e}')

        # save the transformed test for production use
        if apc.save_model:
            try:
                _check_ml_models_folder()
                x_train_trans_df.to_csv(apc.data_directory + cfg.x_train_name, index=False)
            except Exception as e:
                raise ValueError(f'>> Error while saving the transformed test: {e}')

        return x_train_trans_df, x_test_trans_df, self._y_train, self._y_test


def _check_ml_models_folder():
    if not os.path.exists(apc.data_directory):
        os.mkdir(apc.data_directory)
