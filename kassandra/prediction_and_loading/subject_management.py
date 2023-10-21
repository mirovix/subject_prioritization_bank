#!/usr/bin/env python3

"""
@Author: Miro
@Date: 14/06/2023
@Version: 1.0
@Objective: gestione dei soggetti (caricamento, salvataggio, ecc.)
@TODO:
"""

import shap
import pandas as pd
import sqlalchemy
import logging as log
from ml_anomaly_gate import AnomalyGate
from kassandra.prediction_and_loading.features import Features, FeatureCategories
from kassandra.config_module import prediction_and_loading_config as cfg
from kassandra.prediction_and_loading.subject import Subject
from kassandra.registry_management import Registry, RegistryLastPrediction


class SubjectManagement:
    def __init__(self, model_object: any, model_date_name: str, shap_explainer: shap.Explainer,
                 registry_object: Registry, registry_last_prediction_object: RegistryLastPrediction,
                 evaluation_connection: sqlalchemy, subjects_info: pd.DataFrame,
                 features_categories: FeatureCategories, anomaly_gate: AnomalyGate,
                 ref_month_anomaly: str, max_features: int = 10, threshold: float = 0.4):
        """
            :param model_object: model object (e.g. sklearn)
            :param model_date_name: name of the model (e.g. 20210101)
            :param shap_explainer: explainer object (shap)
            :param registry_object: registry object
            :param registry_last_prediction_object: registry last prediction object
            :param evaluation_connection: connection to the evaluation database
            :param subjects_info: subjects info (NDG, ATECO, SAE, etc.)
            :param features_categories: object that contains the names of the features
            :param anomaly_gate: object that contains the anomaly gate
            :param ref_month_anomaly: reference month
            :param max_features: max number of features to show
            :param threshold: threshold for the prediction
        """

        self.list_subjects = []

        self.engine = evaluation_connection
        self.subjects_info = subjects_info
        self.anomaly_gate = anomaly_gate
        self.registry = registry_object
        self.registry_last_prediction = registry_last_prediction_object

        self.ref_month = ref_month_anomaly
        self.explainer = shap_explainer
        self.model = model_object
        self.model_date_name = model_date_name
        self.threshold = float(threshold)
        self.max_features = max_features
        self.features_name = features_categories

        self.system_id = next(iter(self.anomaly_gate.basic_information.system_name.values()))
        self.control_code = next(iter(self.anomaly_gate.basic_information.code.values()))
        self.intermediary_code = next(iter(self.anomaly_gate.basic_information.bank_code.values()))

    def __call__(self, input_to_predict: pd.DataFrame, ndgs_from_registry: list = [],
                 ndgs_from_other_systems: list = []) -> None:
        """
            predict the probability of default for the input_to_predict
            :param input_to_predict: input to predict (pandas DataFrame)
            :return: predicted probability
        """

        if input_to_predict.empty:
            log.info(">> Empty input to predict")
            return

        try:
            if cfg.ndg_name in input_to_predict.columns:
                input_to_predict.set_index(cfg.ndg_name, inplace=True)
            self.subjects_info.set_index(cfg.ndg_name, inplace=True)

            df_predictions, shap_values_df = self._private_predict_and_process(input_to_predict=input_to_predict,
                                                                               ndgs_from_registry=ndgs_from_registry,
                                                                               ndgs_from_other_systems=ndgs_from_other_systems)

            if df_predictions.empty: return

            for ndg, row in shap_values_df.iterrows():
                score = round(df_predictions.loc[ndg].values[0], cfg.round_prediction_score)
                self._private_feature_subject_creation(ndg, row, score, input_to_predict)
        except Exception as e:
            raise ValueError(f">> Error in prediction/shap implementation. Error: {e}")

        self.create_xml_and_load()

    def _private_feature_subject_creation(self, ndg: str, row: pd.Series, score: float,
                                          input_to_predict: pd.DataFrame) -> None:
        """
            create a Subject object and append it to the list of subjects
            :param ndg: NDG of the subject
            :param score: score of the subject
            :param row: row of the shap values
            :param input_to_predict: input to predict (pandas DataFrame)
            :return: None
        """

        # define the features of the subject
        features = Features(features_values=input_to_predict.loc[ndg],
                            features_contribution=row,
                            features_name=self.features_name,
                            max_features=self.max_features)

        # process the features
        features()

        # create the subject and append it to the list of subjects
        self.list_subjects.append(Subject(ndg=str(ndg),
                                          name=self.subjects_info.loc[ndg, cfg.name_subject_col],
                                          fiscal_code=self.subjects_info.loc[ndg, cfg.fiscal_code_subject_col],
                                          residence_city=self.subjects_info.loc[ndg, cfg.residence_city_subject_col],
                                          juridical_nature=self.subjects_info.loc[ndg, cfg.juridical_nature_subject_col],
                                          birthday=self.subjects_info.loc[ndg, cfg.birth_date_subject_col],
                                          residence_country=self.subjects_info.loc[ndg, cfg.residence_country_subject_col],
                                          sae=self.subjects_info.loc[ndg, cfg.sae_subject_col],
                                          ateco=self.subjects_info.loc[ndg, cfg.ateco_subject_col],
                                          office=self.subjects_info.loc[ndg, cfg.office_subject_col],
                                          score=score,
                                          features_values=features))

    def _private_predict_and_process(self, input_to_predict: pd.DataFrame, ndgs_from_registry: list,
                                     ndgs_from_other_systems: list) -> (pd.DataFrame, pd.DataFrame):
        """
            predict and process the results
            :param input_to_predict: input to predict (pandas DataFrame)
            :return: predicted probability, shap values
        """

        predictions = self.model.predict_proba(input_to_predict)[:, 1]
        df_predictions = pd.DataFrame(predictions, columns=["prediction"], index=input_to_predict.index)

        log.info(f">> Number of subjects to predict: {df_predictions.shape[0]}")
        log.info(">> Loading predictions on the registry last prediction table...")

        # load the predictions to the registry last prediction table
        self.registry_last_prediction.load(system_id=self.system_id,
                                           control_code=self.control_code,
                                           intermediary_code=self.intermediary_code,
                                           prediction=df_predictions.copy(),
                                           model_name_date=self.model_date_name)

        # remove subjects with prediction below the threshold
        df_predictions = df_predictions[df_predictions["prediction"] >= self.threshold]

        log.info(f">> Number of subjects to predict after threshold: {df_predictions.shape[0]}")

        # remove ndg that are in the registry
        df_predictions = df_predictions[~df_predictions.index.isin(ndgs_from_registry)]
        log.info(f">> Number of subjects to predict after registry: {df_predictions.shape[0]}")

        # remove ndg that are in other systems
        df_predictions = df_predictions[~df_predictions.index.isin(ndgs_from_other_systems)]
        log.info(f">> Number of subjects to predict after other systems: {df_predictions.shape[0]}")

        if df_predictions.empty: return df_predictions, None

        input_to_predict = input_to_predict.loc[df_predictions.index]

        shap_values = self.explainer(input_to_predict)
        shap_values_df = pd.DataFrame(shap_values.values, columns=input_to_predict.columns, index=input_to_predict.index)

        log.info(">> Shap explanation completed")

        return df_predictions, shap_values_df

    def create_xml_and_load(self) -> None:
        """
            create the xml file and load the subjects in the evaluation database
        """

        log.info(">> Creating xml for each ndg and loading on db")

        for subject in self.list_subjects:
            try:
                attributes = self._private_define_attribute(subject)
                tables = self._private_define_table(subject.features_values)

                self.anomaly_gate.define_attribute_list(attribute_list=attributes, table_list=tables)
                self.anomaly_gate.create_xml()
                self.anomaly_gate.load_on_db(engine=self.engine)
            except Exception as e:
                raise ValueError(f">> Error in xml creation/loading. Error: {e}")

            try:
                self.registry.load_on_registry(system_id=self.system_id,
                                               control_code=self.control_code,
                                               intermediary_code=self.intermediary_code,
                                               client_id=subject.ndg,
                                               prediction=subject.score,
                                               model_name_date=self.model_date_name)
            except Exception as e:
                raise ValueError(f">> Error in loading on registry. Error: {e}")

        log.info(">> Registry loading completed")

    def _private_define_table(self, features: Features) -> list:
        """
            define the table of the anomaly
            :param features: features of the subject
            :return: list of the tables
        """

        header_description = self.anomaly_gate.create_header(cfg.table_header_description)
        header_value = self.anomaly_gate.create_header(cfg.table_header_value_name)
        header_contribution = self.anomaly_gate.create_header(cfg.table_header_contribution)
        header_list = [header_description, header_value, header_contribution]

        rows = []
        for feature in features.list_features:
            value_list = [feature.contribution_str, f"{feature.percentage_contribution}%"]
            row = self.anomaly_gate.create_row(description=self.features_name.description_features[feature.name],
                                               value_list=value_list)
            rows.append(row)

        table = self.anomaly_gate.create_table(description=cfg.table_name,
                                               header_list=header_list,
                                               row_list=rows)
        return [table]

    def _private_define_attribute(self, subject: Subject) -> list:
        """
            define the attribute of the anomaly
            :return: list of the attributes
        """

        # attributes of the anomaly
        ndg = self.anomaly_gate.create_attribute(etichetta=cfg.ndg_name, valore=subject.ndg)
        name = self.anomaly_gate.create_attribute(etichetta=cfg.name_attribute_xml, valore=subject.name)
        fiscal_code = self.anomaly_gate.create_attribute(etichetta=cfg.fiscal_code_attribute_xml, valore=subject.fiscal_code)
        juridical_nature = self.anomaly_gate.create_attribute(etichetta=cfg.juridical_nature_attribute_xml,
                                                              valore=subject.juridical_nature)
        birthday = self.anomaly_gate.create_attribute(etichetta=cfg.birth_date_attribute_xml, valore=subject.birth_day)
        residence_country = self.anomaly_gate.create_attribute(etichetta=cfg.residence_country_attribute_xml,
                                                               valore=subject.residence_country)
        residence_city = self.anomaly_gate.create_attribute(etichetta=cfg.residence_city_attribute_xml,
                                                            valore=subject.residence_city)
        sae = self.anomaly_gate.create_attribute(etichetta=cfg.sae_attribute_xml, valore=subject.sae)
        ateco = self.anomaly_gate.create_attribute(etichetta=cfg.ateco_attribute_xml, valore=subject.ateco)
        office = self.anomaly_gate.create_attribute(etichetta=cfg.office_attribute_xml, valore=subject.office)
        ref_month_anomaly = self.anomaly_gate.create_attribute(etichetta=cfg.ref_month_xml,
                                                               valore=date_transformation(self.ref_month))
        score = self.anomaly_gate.create_attribute(etichetta=cfg.prediction_score_xml, hilight='true',
                                                   valore="{:.2f}%".format(subject.score * 100))

        return [ndg, name, fiscal_code, juridical_nature, birthday, office, residence_country,
                residence_city, sae, ateco, ref_month_anomaly, score]


def date_transformation(data: str) -> str:
    """
        transform the date in the format YYYYMM
        :param data: date to transform
        :return: date transformed
    """

    if len(data) == 6:
        # extract month and year from the input string
        month = data[:2]
        year = data[2:]

        # reconstruct the date string in the desired format
        transformed_date = year + month

        return transformed_date
    else:
        raise ValueError(f"Date {data} not valid")
