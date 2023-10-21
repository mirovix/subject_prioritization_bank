#!/usr/bin/env python3

"""
@Author: Miro
@Date: 28/06/2023
@Version: 1.0
@Objective: TargetFeatures object
@TODO:
"""

from datetime import datetime
import pandas as pd
from kassandra.config_module import feature_creation_config as cfg
from kassandra.features_creation.features_objects.object_features import ObjectFeatures


class TargetFeatures(ObjectFeatures):
    def __init__(self, target: pd.DataFrame, given_date: str = None, lower_bound_month: int = None):
        """
            :param target: operations information (STATUS, ...)
            :param given_date: date of the target
            :param lower_bound_month: lower bound of the target (e.g. only the last 24 months)
        """

        super().__init__(target, cfg.target_columns, cfg.target_dtype)

        self._private_given_date = given_date
        self._private_lower_bound_month = lower_bound_month

        self._private_process_target = None
        self._private_target_to_alert = None

    def _private_preprocess_target(self) -> None:
        """
            Preprocess the target dataframe and apply the given_date filter
            :return: None
        """
        
        self._private_process_target = self._private_dataframe.copy()

        # rename the columns and convert the date
        self._private_process_target.rename(columns=cfg.rename_target_columns, inplace=True)
        self._private_process_target[cfg.target_data] = pd.to_datetime(self._private_process_target[cfg.target_data])

        # extract the target before the given_date (only in training)
        if self._private_given_date is not None:
            self._private_process_target = self._private_process_target[
                (self._private_process_target[cfg.target_data] <= self._private_given_date)]

    def _private_to_alert(self) -> None:
        """
            Extract the target to alert and apply the lower_bound_month filter (e.g. only if the target is in the last n months)
            :return:  None
        """
        
        if self._private_process_target is None:
            self._private_preprocess_target()

        self._private_target_to_alert = self._private_process_target.copy()

        # extract the target last positive report
        self._private_target_to_alert = self._private_target_to_alert[self._private_target_to_alert.STATUS.isin([cfg.to_alert_value])]
        self._private_target_to_alert.sort_values(by=[cfg.target_data], inplace=True)
        self._private_target_to_alert.drop_duplicates(subset=cfg.ndg_name, keep='last', inplace=True)

        # this cut is important for avoiding the missing operations in some banks. They have only the last 24 months for instance.
        # apply the cut only in production mode
        if self._private_lower_bound_month is not None:
            current_date = datetime.now()
            last_date = current_date - pd.DateOffset(months=self._private_lower_bound_month)
            condition = (self._private_target_to_alert[cfg.target_data] >= last_date)
            self._private_target_to_alert = self._private_target_to_alert[condition]

        self._private_target_to_alert.set_index(cfg.ndg_name, inplace=True)

    def get_target(self) -> pd.DataFrame:
        """
            Get target dataframe processed
            :return: target dataframe
        """

        if self._private_process_target is None:
            self._private_preprocess_target()

        return self._private_process_target

    def get_to_alert(self) -> pd.DataFrame:
        """
            Get target to alert dataframe processed
            :return: target to alert
        """

        if self._private_target_to_alert is None:
            self._private_to_alert()

        return self._private_target_to_alert

    def get_not_to_alert_count(self, last_operation_df: pd.DataFrame,
                               output_col_name: str = 'NOT_TO_ALERT_FREQ') -> pd.DataFrame:
        """
            Get the number of operations not to alert for each NDG in the last n months
            :param last_operation_df: last operation dataframe if the target is not to alert
            :param output_col_name: name of the output column in the dataframe
            :return: counting of operations not to alert
        """

        target_df = self.get_target().copy()

        # extract the target not reported
        target_df = target_df.loc[target_df.STATUS == cfg.not_to_alert_value]
        target_df = pd.merge(target_df, last_operation_df, on=cfg.ndg_name, how='left')
        target_df = target_df.sort_values(by=[cfg.ndg_name, cfg.target_data]).reset_index(drop=True)

        # extract the target not reported in the last n months
        target_df = target_df[(target_df[cfg.target_data] >= target_df[cfg.start_date_operations]) &
                              (target_df[cfg.target_data] <= target_df[cfg.end_date_operations])]
        target_df.reset_index(drop=True, inplace=True)

        return target_df.groupby(cfg.ndg_name).size().reset_index(name=output_col_name)

    def get_status(self) -> pd.DataFrame:
        """
            Get status dataframe for training and testing
            :return: status dataframe
        """

        return self.get_to_alert()[cfg.status_column].replace(cfg.binary_target_dict)
