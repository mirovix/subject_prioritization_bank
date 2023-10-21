#!/usr/bin/env python3

"""
@Author: Miro
@Date: 05/07/2023
@Version: 1.0
@Objective: OperationFeatures object
@TODO:
"""

import logging as log
import numpy as np
import pandas as pd
from kassandra.config_module import feature_creation_config as cfg
from kassandra.features_creation.features_objects.object_features import ObjectFeatures
from kassandra.features_creation.features_objects.categorization_data import ListValues, AnalyticalCausal
from kassandra.features_creation.features_objects.target_features import TargetFeatures


class OperationsSubjectsFeatures(ObjectFeatures):
    def __init__(self, dataframe: pd.DataFrame, columns: list, dtype: any):
        """
            :param dataframe: operations_subjects dataframe
            :param columns: columns to be used
            :param dtype: data type of the columns
        """

        super().__init__(dataframe, columns, dtype)
        self.processed_operations_subjects = None

    def _private_process_operations_subjects(self) -> None:
        """
            preprocess the operations_subjects dataframe
        """

        self.processed_operations_subjects = self._private_dataframe.copy()
        self.processed_operations_subjects = self.processed_operations_subjects.loc[
                                             (self.processed_operations_subjects[cfg.subject_type_column] == 'T') |
                                             (self.processed_operations_subjects[cfg.subject_type_column] == 'E'), :]
        self.processed_operations_subjects = self.processed_operations_subjects.drop_duplicates()

    def get_operations_subjects(self) -> pd.DataFrame:
        """
            return the processed operations_subjects dataframe
            :return: processed operations_subjects dataframe
        """

        if self.processed_operations_subjects is None:
            self._private_process_operations_subjects()
        return self.processed_operations_subjects


class OperationsFeatures(ObjectFeatures):
    def __init__(self, dataframe: pd.DataFrame, columns: list, dtype: any, analytical_causal: AnalyticalCausal,
                 target_object: TargetFeatures, country_risk: ListValues, months_operations: int = 12,
                 last_anomaly_date: str = None):
        """
            :param dataframe: Operations dataframe
            :param columns: columns to keep
            :param dtype: data type of the columns
            :param analytical_causal: analytical causal object
            :param target_object: target object
            :param country_risk: country risk object
            :param months_operations: number of months to consider for the operations (default: 12)
            :param last_anomaly_date: last date of the anomaly only for the training phase (default: None)
        """

        super().__init__(dataframe, columns, dtype)
        self._private_last_date = last_anomaly_date
        self._private_months_operations = months_operations

        self.analytics_causals = analytical_causal.get_analytical_causal()
        self.country_risk = country_risk.get_country_risk()
        self.target = target_object

        self._private_processed_operations = None
        self._private_operations_subjects_merged = None
        self._private_last_operation_df = None

    def _private_process_operations(self) -> None:
        """
            Process the operations dataframe in order to have the right format
        """

        self._private_processed_operations = self._private_dataframe.copy()

        # convert the date column to the datetime format
        self._private_processed_operations[cfg.date_operation_column] = pd.to_datetime(self._private_processed_operations[cfg.date_operation_column])

        # during the training phase, we have to remove recent operations
        if self._private_last_date is not None:
            condition = (self._private_processed_operations[cfg.date_operation_column] <= self._private_last_date)
            self._private_processed_operations = self._private_processed_operations[condition]

        # convert the amount column to the numeric format
        self._private_processed_operations.loc[:, cfg.amount_column] = pd.to_numeric(self._private_processed_operations[cfg.amount_column])

        self._private_processed_operations = self._private_processed_operations.drop_duplicates()

    def _private_merge_operations_subjects(self, subject_information: pd.DataFrame) -> None:
        """
            Merge the operations dataframe with the subject_information dataframe
            :param subject_information: df with the subject information
        """

        merged = pd.merge(self.get_operations(), subject_information, on=[cfg.merge_operation_subject_columns], how='left')
        self._private_operations_subjects_merged = merged

    def _private_compute_last_date(self) -> None:
        """
            Compute the last date of the operations for each subject
        """

        if self._private_operations_subjects_merged is None:
            return None

        # get the last date of the operations
        last_operation_df = self._private_operations_subjects_merged.copy()
        last_operation_df.sort_values(by=cfg.date_operation_column, inplace=True)
        last_operation_df.drop_duplicates(subset=cfg.ndg_name, keep='last', inplace=True)
        last_operation_df = last_operation_df[[cfg.ndg_name, cfg.date_operation_column]].set_index(cfg.ndg_name)

        # merge with the target to get the last date of the target
        last_date_df = last_operation_df.merge(self.target.get_to_alert(), left_index=True, right_index=True, how='left')

        # if the subject is not in the target, the last date is the last date of the operations
        last_date_df[cfg.end_date_operations] = last_date_df[cfg.target_data].fillna(last_date_df[cfg.date_operation_column])
        last_date_df = last_date_df.reset_index()[[cfg.ndg_name, cfg.end_date_operations]]

        # compute the start date of the operations (e.g. end_date - 12 months)
        offset = pd.DateOffset(months=self._private_months_operations)
        last_date_df[cfg.start_date_operations] = last_date_df[cfg.end_date_operations] - offset
        last_date_df = last_date_df.drop_duplicates()

        self._private_last_operation_df = last_date_df

    def _private_post_processing_merge(self, operations_subjects_merged: pd.DataFrame) -> pd.DataFrame:
        """
            drop the operations out of the time window defined and convert some columns to the right format
            :param operations_subjects_merged: merged dataframe
            :return: postprocessor dataframe
        """

        operations_processed = pd.merge(operations_subjects_merged, self.get_last_date(), on=[cfg.ndg_name], how='left')

        # compute the number of months between the last date of the operations and the date of the operation
        operations_processed = operations_processed.sort_values(by=[cfg.ndg_name, cfg.date_operation_column]).reset_index(drop=True)
        upper_condition = (operations_processed[cfg.date_operation_column] >= operations_processed[cfg.start_date_operations])
        lower_condition = (operations_processed[cfg.date_operation_column] <= operations_processed[cfg.end_date_operations])
        operations_processed = operations_processed[upper_condition & lower_condition]
        operations_processed.reset_index(drop=True, inplace=True)

        # convert some columns to the right format
        operations_processed[cfg.date_operation_column] = pd.to_datetime(operations_processed[cfg.date_operation_column])
        operations_processed[cfg.amount_column] = pd.to_numeric(operations_processed[cfg.amount_column])
        operations_processed[cfg.month_year_column] = pd.DatetimeIndex(operations_processed[cfg.date_operation_column]).to_period('M')

        return operations_processed

    @staticmethod
    def _private_compute_mean_features(operations_processed: pd.DataFrame) -> pd.DataFrame:
        """
            Computing the avg amount, avg frequency and avg months of the operations for each subject and sign
            :param operations_processed: input df processed from the operations
            :return: df with the mean features
        """

        tot_amount_name, tot_months_name, tot_freq_name = 'TOT_AMOUNT', 'TOT_MONTHS', 'TOT_FREQ'
        avg_amount_name, avg_freq_name = 'AVG_AMOUNT', 'AVG_FREQ'

        # compute the total amount, total months and total frequency of the operations for each subject and sign
        total_amounts = group_and_aggregate(operations_processed, [cfg.ndg_name, cfg.sign_column], cfg.amount_column, 'sum',
                                            {cfg.sign_options_in: tot_amount_name + '_' + cfg.sign_options_in,
                                             cfg.sign_options_out: tot_amount_name + '_' + cfg.sign_options_out})
        total_months = group_and_aggregate(operations_processed, [cfg.ndg_name, cfg.sign_column], cfg.month_year_column, 'nunique',
                                           {cfg.sign_options_in: tot_months_name + '_' + cfg.sign_options_in,
                                            cfg.sign_options_out: tot_months_name + '_' + cfg.sign_options_out})
        freq_amounts = group_and_aggregate(operations_processed, [cfg.ndg_name, cfg.sign_column], cfg.amount_column, 'count',
                                           {cfg.sign_options_in: tot_freq_name + '_' + cfg.sign_options_in,
                                            cfg.sign_options_out: tot_freq_name + '_' + cfg.sign_options_out})

        features_statistics = total_amounts.merge(total_months, on=cfg.ndg_name, how='left')
        features_statistics = features_statistics.merge(freq_amounts, on=cfg.ndg_name, how='left')

        # compute the mean features for each sign and subject
        operation = manage_zero_division(features_statistics[tot_amount_name + '_' + cfg.sign_options_in],
                                         features_statistics[tot_months_name + '_' + cfg.sign_options_in])
        features_statistics[avg_amount_name + '_' + cfg.sign_options_in] = operation

        operation = manage_zero_division(features_statistics[tot_amount_name + '_' + cfg.sign_options_out],
                                         features_statistics[tot_months_name + '_' + cfg.sign_options_out])
        features_statistics[avg_amount_name + '_' + cfg.sign_options_out] = operation

        operation = manage_zero_division(features_statistics[tot_freq_name + '_' + cfg.sign_options_in],
                                         features_statistics[tot_months_name + '_' + cfg.sign_options_in])
        features_statistics[avg_freq_name + '_' + cfg.sign_options_in] = operation

        operation = manage_zero_division(features_statistics[tot_freq_name + '_' + cfg.sign_options_out],
                                         features_statistics[tot_months_name + '_' + cfg.sign_options_out])
        features_statistics[avg_freq_name + '_' + cfg.sign_options_out] = operation

        return features_statistics

    def _private_compute_analytical_causal_features(self, operations_processed: pd.DataFrame, features_statistics: pd.DataFrame):
        """
            Computing the analytical causal features for each subject and sign
            :param operations_processed: input df processed from the operations
            :param features_statistics: df with the mean features
            :return: df with the analytical causal features
        """

        risk_country_name = 'RISCHIO_PAESE'
        risk_country_tot_name = 'RISCHIO_PAESE_TOT'

        operations_cod = operations_processed[operations_processed[cfg.counterpart_column].isin(self.country_risk)]
        cod = group_and_aggregate(operations_cod, [cfg.ndg_name, cfg.sign_column], cfg.amount_column, 'sum',
                                  {cfg.sign_options_in: risk_country_tot_name + '_' + cfg.sign_options_in,
                                   cfg.sign_options_out: risk_country_tot_name + '_' + cfg.sign_options_out})

        features_statistics = features_statistics.merge(cod, on=cfg.ndg_name, how='left')


        for causale in self.analytics_causals.columns:
            operations_cod = operations_processed[operations_processed[cfg.causal_column].isin(self.analytics_causals[causale])]
            cod = group_and_aggregate(operations_cod, [cfg.ndg_name, cfg.sign_column], cfg.amount_column, 'count',
                                      {cfg.sign_options_in: causale.upper() + '_FREQ_' + cfg.sign_options_in,
                                       cfg.sign_options_out: causale.upper() + '_FREQ_' + cfg.sign_options_out})
            features_statistics = features_statistics.merge(cod, on=cfg.ndg_name, how='left')

            for sign in [cfg.sign_options_in, cfg.sign_options_out]:
                operation = manage_zero_division(features_statistics[causale.upper() + '_FREQ_' + sign],
                                                 features_statistics['TOT_MONTHS' + '_' + sign])
                features_statistics['AVG_' + causale.upper() + '_FREQ_' + sign] = operation


        for sign in [cfg.sign_options_in, cfg.sign_options_out]:
            operation = manage_zero_division(features_statistics[risk_country_tot_name + '_' + sign],
                                             features_statistics['TOT_AMOUNT' + '_' + sign])
            features_statistics['AVG_' + risk_country_name + '_' + sign] = operation
        return features_statistics

    def _private_add_target_features(self, features_statistics) -> pd.DataFrame:
        """
            Add target features to the features_statistics dataframe (not to alert count)
            :param features_statistics: input df with the features statistics
            :return: df with the target features
        """

        not_to_alerts = self.target.get_not_to_alert_count(self.get_last_date())
        features_processed = pd.merge(features_statistics, not_to_alerts, on=cfg.ndg_name, how="left")

        return features_processed

    def get_operations(self) -> pd.DataFrame:
        """
            get operations dataframe after processing
            :return: processed dataframe
        """

        if self._private_processed_operations is None:
            self._private_process_operations()
        return self._private_processed_operations

    def get_operations_subjects_merged(self, subject_information: pd.DataFrame) -> pd.DataFrame:
        """
            get operations and subjects merged after processing
            :param subject_information: dataframe with subject information
            :return: merged dataframe
        """

        if self._private_operations_subjects_merged is None:
            self._private_merge_operations_subjects(subject_information)
        return self._private_operations_subjects_merged

    def get_last_date(self) -> pd.DataFrame:
        """
            get last date of operations dataframe
            :return: last date
        """

        if self._private_last_operation_df is None:
            self._private_compute_last_date()
        return self._private_last_operation_df

    def __call__(self, subject_information: pd.DataFrame) -> pd.DataFrame:
        """
            main function for creating features
            :param subject_information: dataframe with subject information
            :return: processed features dataframe
        """

        # get operations and subjects merged after processing
        log.debug(">> Getting operations and subjects merged after processing")
        operations_subjects_merged = self.get_operations_subjects_merged(subject_information)

        # postprocessing merge
        log.debug(">> Postprocessing merge of operations and subjects")
        operations_processed = self._private_post_processing_merge(operations_subjects_merged)

        # compute mean features
        log.debug(">> Computing mean features")
        mean_features = self._private_compute_mean_features(operations_processed)

        # compute analytical causal features
        log.debug(">> Computing analytical causal features")
        analytical_causal_features = self._private_compute_analytical_causal_features(operations_processed, mean_features)

        # add target features
        log.debug(">> Adding target features")
        features_processed = self._private_add_target_features(analytical_causal_features)

        # return processed features dataframe
        log.debug(">> Returning processed features dataframe")
        features_processed = features_processed[cfg.output_features_columns]

        for col in features_processed.columns:
            if col != cfg.ndg_name:
                features_processed[col] = features_processed[col].astype(float)

        return features_processed


def group_and_aggregate(df: pd.DataFrame, group_cols: list, count_col: str, operation_type: str,
                        rename_cols: dict = None) -> pd.DataFrame:
    """
        intermediary function for computing statistics
        :param df: input df to be processed
        :param group_cols: columns to group by
        :param count_col: column to be counted
        :param operation_type: type of operation to be performed
        :param rename_cols: columns to be renamed after the operation
        :return: df processed
    """

    if df is None or df.empty:
        return pd.DataFrame([], columns=[cfg.ndg_name, rename_cols[cfg.sign_options_in],
                                         rename_cols[cfg.sign_options_out]])

    df = df.copy().groupby(group_cols)[count_col]

    if operation_type == 'sum':
        result = df.sum()
    elif operation_type == 'count':
        result = df.count()
    elif operation_type == 'nunique':
        result = df.nunique()
    else:
        raise ValueError('Operation not supported')

    result = result.unstack().rename(columns=rename_cols).reset_index()

    if rename_cols[cfg.sign_options_out] not in result.columns:
        result[rename_cols[cfg.sign_options_out]] = np.nan
    elif rename_cols[cfg.sign_options_in] not in result.columns:
        result.insert(0, rename_cols[cfg.sign_options_in], np.nan)

    return result


def manage_zero_division(num: any, den: any) -> any:
    """
        intermediary function for computing statistics
        :param num: numerator
        :param den: denominator
        :return: operation result
    """

    try:
        return num / den
    except ZeroDivisionError:
        return 0
