#!/usr/bin/env python3

"""
@Author: apadoan
@Date: 15/06/2023
@Version: 1.0
@Objective: subject prioritization oneshot starter
@TODO:
"""

import os
import sys
import joblib
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime
from ml_anomaly_gate import AnomalyGate
from kassandra.config_module import app_config as apc
from kassandra.features_creation import BuildFeaturesProd
from kassandra.extraction import ExtractData
from kassandra.prediction_and_loading import SubjectManagement, FeatureCategories
from kassandra.config_module import prediction_and_loading_config as cfg
from kassandra.parametrization import ParameterManagement
from kassandra.registry_management import Registry, RegistryLastPrediction
from kassandra.self_census import SelfCensus
from kassandra.starter.database_manager import DatabaseManager
from kassandra.starter.log import log, notify_with_email


def parse_args():
    arguments = sys.argv
    if len(arguments) != 3:
        raise ValueError("Submit intermediary code and reference month")

    intermediary_code = arguments[1]
    ref_month = arguments[2]
    return intermediary_code, ref_month


def setup_service_folder():
    os.makedirs(apc.service_folder, exist_ok=True)


def get_model(endswith: str = ".joblib"):
    """
        Get the most recent model
        :param endswith: end of the filename to search
        :return: name of the most recent model
    """

    file_list = os.listdir(apc.models_directory)

    if not file_list:
        raise FileNotFoundError(">> No files found in the folder")

    # filter out only the ".joblib" files
    joblib_files = [filename for filename in file_list if filename.endswith(endswith)]

    file_dates = [datetime.strptime(filename.split(".")[0], "%Y%m%d") for filename in joblib_files]

    # find the index of the most recent date
    most_recent_index = file_dates.index(max(file_dates))

    # get the filename with the most recent date
    most_recent_file = joblib_files[most_recent_index].split(".")[0]

    return most_recent_file + endswith


def get_registries(dbm):
    # create a registry
    log.info(f'>> Loading Registry, system_id={apc.system_id_name}, control_code={apc.control_code}')
    registry = Registry(engine=dbm.engine_kassandra)
    if registry.get_table() is None:
        registry.create_registry_table_index()

    # create a registry for saving all results
    log.info(f'>> Loading Registry Last Prediction, system_id={apc.system_id_name}, control_code={apc.control_code}')
    registry_last_prediction = RegistryLastPrediction(engine=dbm.engine_kassandra)
    if registry_last_prediction.get_table() is None:
        registry_last_prediction.create_registry_table_index()

    return registry, registry_last_prediction


def get_parameters():
    # create a parameter management
    log.info(f'>> Requesting parameters from {apc.url_discovery_api}')
    parameter_management = ParameterManagement(system_id=apc.system_id_name,
                                               control_code=apc.control_code)

    months_to_skip = parameter_management.get_parameters_api(url_api=apc.url_discovery_api,
                                                             parameter_place_holder='months_to_skip').get_value()
    systems = parameter_management.get_parameters_api(url_api=apc.url_discovery_api,
                                                      parameter_place_holder='systems').get_value()
    threshold = parameter_management.get_parameters_api(url_api=apc.url_discovery_api,
                                                        parameter_place_holder='threshold').get_value() / 100
    log.info(f'>> Loaded params: months_to_skip={months_to_skip}; systems={systems}; threshold={threshold}')

    return months_to_skip, systems, threshold


def main() -> None:
    """
    Main function to start the subject prioritization oneshot
    :return: None
    """

    try:
        intermediary_code, ref_month = parse_args()
        setup_service_folder()
        dbm = DatabaseManager()
        _execute(dbm, intermediary_code, ref_month)

        if 'TEST_ENV' not in os.environ:
            dbm.close_connection()

    except Exception as e:
        log.error(f'>> subject_prioritization_oneshot error: {str(e)}', exc_info=True)
        notify_with_email(f'>> subject_prioritization_oneshot error: {str(e)}')


def _execute(dbm, intermediary_code, ref_month):
    log.info(f'>> Starting Kassandra for {intermediary_code} and {ref_month}')
    log.info(f'>> Starting Self Census for {apc.url_census}')

    SelfCensus(url_link=apc.url_census,
               system_id_name=apc.system_id_name,
               system_id_description=apc.system_id_description)()

    registry, registry_last_prediction = get_registries(dbm)
    months_to_skip, systems, threshold = get_parameters()

    model_name_date = get_model()
    log.info(f'>> Loading model from {apc.models_directory + "/" + model_name_date}')
    model = joblib.load(apc.models_directory + "/" + model_name_date)

    # create a extraction
    log.info('>> Loading data from dwa and evaluation')
    extract_data = ExtractData(engine_evaluation=dbm.engine_evaluation,
                               engine_dwa=dbm.engine_dwa,
                               registry=registry,
                               system_id=apc.system_id_name,
                               control_code=apc.control_code,
                               intermediary_code=intermediary_code,
                               ref_month=ref_month,
                               registry_month_to_skip=months_to_skip,
                               reported_other_systems=systems)

    # get operations from dwa
    operations = extract_data.get_operations()
    log.info(f'>> Extracting operations from dwa completed, {operations.shape[0]} operations extracted')

    # get operation subjects from dwa
    operation_subjects = extract_data.get_operations_subjects()
    log.info(f'>> Extracting operation subjects from dwa completed, {operation_subjects.shape[0]} operation subjects extracted')

    # get subjects information from dwa
    subjects_information = extract_data.get_subjects()
    log.info(f'>> Extracting subjects information from dwa completed, {subjects_information.shape[0]} subjects extracted')

    # get target from evaluation
    target = extract_data.get_target()
    log.info(f'>> Extracting target from evaluation completed, {target.shape[0]} target extracted')

    # ndg from registry and other systems
    ndg_registry = extract_data.get_ndgs_from_registry()
    ndg_other_systems = extract_data.get_ndgs_from_other_systems()

    # features creation and selection for production
    build_features_prod = BuildFeaturesProd(operations=operations,
                                            operation_subjects=operation_subjects,
                                            subjects=subjects_information,
                                            target_information=target,
                                            bank_months=apc.bank_months)
    log.info('>> Building features transformed for production')
    dataset_categorized = build_features_prod.get_transformed_prod()

    log.info('>> Creating shap explainer')
    shap_explainer = build_features_prod.get_shap_explainer(model_object=model)

    log.info('>> Creating feature categories')
    features_name = FeatureCategories(description_features=cfg.features_dict,
                                      boolean_features=cfg.boolean_features_name,
                                      numerical_features=cfg.numerical_features_name,
                                      currency_features=cfg.currency_features_name,
                                      binary_features=cfg.to_round_features)

    # create a new anomaly gate
    log.info(f'>> Creating anomaly gate object, system_id={apc.system_id_name}, id_transition={apc.id_transition}')
    gate = AnomalyGate(system=apc.system_id_name, id_transition=apc.id_transition)
    gate.define_id(code=apc.control_code, bank_code=intermediary_code, name=apc.system_id_name)
    gate.define_basic_information(code=apc.control_code, bank_code=intermediary_code,
                                  description=apc.system_id_description)

    # predict and load results in evaluation
    log.info('>> Predicting and loading results in evaluation')
    subject_management = SubjectManagement(model_object=model,
                                           model_date_name=model_name_date,
                                           shap_explainer=shap_explainer,
                                           registry_object=registry,
                                           registry_last_prediction_object=registry_last_prediction,
                                           evaluation_connection=dbm.engine_evaluation,
                                           anomaly_gate=gate,
                                           features_categories=features_name,
                                           subjects_info=subjects_information,
                                           ref_month_anomaly=ref_month,
                                           threshold=threshold)

    # predict the anomaly and postprocessing the results
    subject_management(input_to_predict=dataset_categorized,
                       ndgs_from_registry=ndg_registry,
                       ndgs_from_other_systems=ndg_other_systems)


if __name__ == '__main__':
    main()
    exit(0)
