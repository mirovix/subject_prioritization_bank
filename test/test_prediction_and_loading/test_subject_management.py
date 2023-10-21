#!/usr/bin/env python3

"""
@Author: Miro
@Date: 16/06/2022
@Version: 1.0
@Objective: test per della classe SubjectManagement
@TODO:
"""

import unittest
import numpy as np
import pandas as pd
from unittest.mock import patch, MagicMock
from test.test_prediction_and_loading import subjects_path, input_data_path, xml_to_load_path
from ml_anomaly_gate import AnomalyGate
from kassandra.prediction_and_loading import FeatureCategories, Subject, Features, Feature
from kassandra.config_module import prediction_and_loading_config as cfg
from kassandra.prediction_and_loading import SubjectManagement, date_transformation


class TestSubjectManagement(unittest.TestCase):
    def setUp(self) -> None:
        """
            setup for test
        """

        self.features_name = FeatureCategories(description_features={},
                                               boolean_features=[],
                                               numerical_features=[],
                                               currency_features=[],
                                               binary_features=[])

        ref_month = '062023'

        gate = AnomalyGate(system='KASSANDRA', id_transition="0001")
        gate.define_id(code='0001', bank_code="000000", name="default")
        gate.define_basic_information(code='0001', bank_code="000000", description="Kassandra01")

        self.subjects_df = pd.read_csv(subjects_path)
        self.data_df = pd.read_csv(input_data_path)

        self.subject_management = SubjectManagement(model_object=MagicMock(),
                                                    model_date_name=MagicMock(),
                                                    shap_explainer=MagicMock(),
                                                    registry_object=MagicMock(),
                                                    registry_last_prediction_object=MagicMock(),
                                                    evaluation_connection=None,
                                                    anomaly_gate=gate,
                                                    features_categories=self.features_name,
                                                    subjects_info=self.subjects_df,
                                                    ref_month_anomaly=ref_month)

    @patch('kassandra.prediction_and_loading.subject_management.SubjectManagement.create_xml_and_load')
    def test_processing_subjects(self, _):
        """
            test for processing_subjects function
        """

        expected_ndg = self.subjects_df[cfg.ndg_name].iloc[0]
        expected_name = self.subjects_df[cfg.name_subject_col].iloc[0]
        expected_fiscal_code = self.subjects_df[cfg.fiscal_code_subject_col].iloc[0]
        expected_residence_country = self.subjects_df[cfg.residence_country_subject_col].iloc[0]
        expected_sae = self.subjects_df[cfg.sae_subject_col].iloc[0]

        self.subject_management.features_name.numerical_features = self.data_df.columns.tolist()

        self.subject_management.explainer.return_value = pd.DataFrame([1.0] * (self.data_df.shape[1] - 1)).T

        with patch.object(self.subject_management.model, "predict_proba") as mock_predict_proba:
            mock_predict_proba.return_value = np.array([[0.1, 0.9]])
            self.subject_management(input_to_predict=self.data_df.copy())

            with self.assertRaises(ValueError):
                self.subject_management(input_to_predict=self.data_df.copy())

        subject_processed = self.subject_management.list_subjects[0]
        self.assertEqual(subject_processed.score, 0.9)
        self.assertEqual(subject_processed.ndg, expected_ndg)
        self.assertEqual(subject_processed.name, expected_name)
        self.assertEqual(subject_processed.fiscal_code, expected_fiscal_code)
        self.assertEqual(int(subject_processed.residence_country), expected_residence_country)
        self.assertEqual(int(subject_processed.sae), expected_sae)

        self.assertIsNone(self.subject_management(input_to_predict=pd.DataFrame()))

    def test_create_xml_and_load(self):
        """
            test for create_xml_and_load function
        """

        index = ['test' + str(i) for i in range(1, self.data_df.shape[1])]
        index.insert(0, cfg.ndg_name)

        self.features_name.binary_features = index
        self.features_name.description_features = {key: key for key in index}

        features_contribution = pd.Series([1.0] * self.data_df.shape[1], index=index)
        features_df = pd.DataFrame([1.0] * self.data_df.shape[1], index=index).astype(str)

        features = Features(features_values=features_df,
                            features_contribution=features_contribution,
                            features_name=self.features_name,
                            max_features=10)

        subject1 = Subject(ndg='1',
                           name='test',
                           fiscal_code='test',
                           residence_city='test',
                           juridical_nature='PG',
                           residence_country='086',
                           birthday='1929-02-17 00:00:00.000',
                           sae='600',
                           ateco='01',
                           office='test',
                           score=0.0,
                           features_values=features)

        subject2 = Subject(ndg='1',
                           name='test',
                           fiscal_code='test',
                           residence_city='test',
                           juridical_nature='PG',
                           birthday='1929-02-17 00:00:00.000',
                           residence_country='086',
                           sae='600',
                           ateco='01',
                           office='test',
                           score=0.7532,
                           features_values=features)

        self.subject_management.list_subjects = [subject1, subject2]

        feature = Feature("test1", 0.1, 50, "test")
        self.subject_management.list_subjects[1].features_values.list_features = [feature]

        with patch.object(self.subject_management.anomaly_gate, 'load_on_db') as mock_load_on_db:
            self.subject_management.create_xml_and_load()
            mock_load_on_db.assert_called()
            with open(xml_to_load_path, 'r', encoding="utf-8") as file:
                xml = file.read()
                file.close()

            gate = AnomalyGate(system='KASSANDRA', id_transition="0001")
            gate.read_xml(xml)
            self.assertTrue(gate.compare_xml(self.subject_management.anomaly_gate.content))

        with self.assertRaises(ValueError):
            self.subject_management.anomaly_gate = None
            self.subject_management.create_xml_and_load()

    def test_date_transformation(self):
        """
            testing date transformation function
        """

        date = "062023"
        wrong_date = "01111111"

        result = date_transformation(date)
        self.assertEqual(result, "202306")

        with self.assertRaises(ValueError):
            date_transformation(wrong_date)
