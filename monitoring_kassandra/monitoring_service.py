#!/usr/bin/env python3

"""
@Author: Miro
@Date: 10/10/2023
@Version: 1.0
@Objective: kassandra monitoring starter
@TODO:
"""

import os
import sys
import time
import warnings
import pandas as pd
import monitoring_kassandra as ms
warnings.filterwarnings("ignore")
from monitoring import MonitoringKassandra
from kassandra.config_module import app_config as apc
from kassandra.extraction import ExtractData
from kassandra.config_module import prediction_and_loading_config as cfg
from kassandra.config_module import feature_creation_config as fcc
from kassandra.config_module import registry_config as rc
from kassandra.config_module import extraction_config as ec
from kassandra.starter.database_manager import DatabaseManager
from kassandra.starter.log import log, notify_with_email
from kassandra.config_module.extraction_config import default_software
from kassandra.starter.subject_prioritization_oneshot import setup_service_folder, get_model, \
    get_registries, get_parameters


def parse_args():
    arguments = sys.argv
    if len(arguments) != 2:
        raise ValueError("Submit intermediary code only")
    return arguments[1]


def main() -> None:
    """
    Main function to start the service for monitoring kassandra
    :return: None
    """

    try:
        intermediary_code = parse_args()
        setup_service_folder()
        log.info(f'>> Starting Monitoring Kassandra for {intermediary_code}')
        while True:
            dbm = DatabaseManager()
            _execute(dbm, intermediary_code)

            if 'TEST_ENV' not in os.environ:
                dbm.close_connection()

            if apc.time_to_sleep is None or apc.time_to_sleep < 1:
                break

            time.sleep(apc.time_to_sleep)
    except Exception as e:
        log.error(f'>> monitoring kassandra service error: {str(e)}', exc_info=True)
        notify_with_email(body=f'>> monitoring kassandra service error: {str(e)}',
                          subject='Monitoring Kassandra Service Error')


def _execute(dbm, intermediary_code):
    log.info('>> *** NEW MONITORING KASSANDRA EXECUTION ***')

    registry, registry_last_prediction = get_registries(dbm)
    months_to_skip, systems, threshold = get_parameters()

    # create a extraction
    log.info('>> Loading data from dwa and evaluation')
    extract_data = ExtractData(engine_evaluation=dbm.engine_evaluation,
                               engine_dwa=dbm.engine_dwa,
                               registry=registry,
                               system_id=apc.system_id_name,
                               control_code=apc.control_code,
                               intermediary_code=intermediary_code,
                               ref_month='012100',
                               registry_month_to_skip=months_to_skip,
                               reported_other_systems=systems)

    # get target from evaluation
    target = extract_data.get_target()
    log.info(f'>> Extracting target from evaluation completed, {target.shape[0]} target extracted')

    log.info('>> Monitoring the results')

    # get data from evaluation and registry
    last_prediction = registry_last_prediction.get_last_prediction(system_id=apc.system_id_name,
                                                                   control_code=apc.control_code,
                                                                   intermediary_code=intermediary_code)
    registry_prediction = registry.get_last_prediction(system_id=apc.system_id_name,
                                                       control_code=apc.control_code,
                                                       intermediary_code=intermediary_code)

    # remove registry prediction from last prediction to get only new predictions
    last_prediction = last_prediction[~last_prediction[cfg.ndg_name].isin(registry_prediction[cfg.ndg_name])]
    predictions = pd.concat([registry_prediction, last_prediction], axis=0, ignore_index=True)

    evaluation_prediction = extract_data.get_anomaly_processed()
    monitoring_input = pd.merge(predictions, evaluation_prediction, on=cfg.ndg_name, how='left')

    # rename columns
    monitoring_input = monitoring_input.rename(columns={rc.ndg_name: ms.index_column,
                                                        rc.prediction_name: ms.score_column,
                                                        rc.model_name_date_name: ms.model_column,
                                                        ec.status_target_col: ms.target_column})
    target.rename(columns={ec.ndg_name: ms.index_column,
                           ec.status_target_col: ms.target_column}, inplace=True)

    log.info(f'>> {monitoring_input.shape[0]} predictions to monitor')
    log.info(f'>> {monitoring_input[monitoring_input[ms.score_column] < threshold].shape[0]} elements under threshold')
    log.info(f'>> {monitoring_input[monitoring_input[ms.score_column] >= threshold].shape[0]} elements over threshold')
    log.info(f'>> {monitoring_input[monitoring_input[ms.target_column].notnull()].shape[0]} elements with status not none')
    log.info(f'>> {monitoring_input[monitoring_input[ms.target_column].isnull()].shape[0]} elements with status none')

    monitoring_object = MonitoringKassandra(target_df=target,
                                            predictions_df=monitoring_input,
                                            threshold=threshold,
                                            model_ids=get_model(),
                                            plot_directory=apc.plot_directory,
                                            show_plots=apc.show_plots,
                                            save_plots=apc.save_plots)
    # call the function to plot the results
    monitoring_object.plot_kassandra_statistics(software_list_names=default_software,
                                                kassandra_system=apc.system_id_name)
    monitoring_object.plot_correct_predictions()
    monitoring_object.plot_wrong_predictions()
    monitoring_object.plot_confusion_matrix(classes=[fcc.not_to_alert_value, fcc.to_alert_value])

    log.info('>> Monitoring completed')


if __name__ == '__main__':
    main()
    exit(0)
