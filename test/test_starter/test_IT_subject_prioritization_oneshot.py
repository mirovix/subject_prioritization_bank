#!/usr/bin/env python3

"""
@Author: apadoan
@Date: 15/06/2023
@Version: 1.0
@Objective: subject prioritization oneshot starter
@TODO: completare test intero giro oneshot
"""
import logging
import os
os.environ['TEST_ENV'] = '1'
import json
import sys
import unittest
import runpy
import httpretty
from sqlalchemy import inspect

from kassandra.config_module import app_config as apc
from kassandra.config_module.self_census_config import path_system_id_request, path_icon_request
from kassandra.starter import subject_prioritization_oneshot
from kassandra.starter.database_manager import DatabaseManager

from monitoring_kassandra import monitoring_service

from test.abstract_IT import AbstractIT



class TestITSubjectPrioritizationOneshot(AbstractIT):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._setup_mock_server()
        runpy.run_module("train", run_name="__main__")

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        httpretty.disable()
        httpretty.reset()
        logging.shutdown()

    def setUp(self):
        super().setUp()
        self._insert_db()

    def test_givenOneShot_assertCorrectExecution(self):
        engine_kassandra = DatabaseManager().engine_kassandra
        engine_evaluation = DatabaseManager().engine_evaluation

        self.assertFalse(inspect(engine_kassandra).has_table("REGISTRY_KASSANDRA"))
        self.assertFalse(inspect(engine_evaluation).has_table("RX_TRASMISSIONE"))
        sys.argv = ["subject_prioritization_oneshot", "123456", "052023"]
        subject_prioritization_oneshot.main()

        mock_server_requests = httpretty.latest_requests()
        self.assertEqual(5, len(mock_server_requests))
        self.assertEqual(1, sum(r.path == "/Evaluation/systemadd" for r in mock_server_requests))
        self.assertEqual(1, sum(r.path == "/Evaluation/iconadd" for r in mock_server_requests))

        self.assertTrue(inspect(engine_kassandra).has_table("REGISTRY_KASSANDRA"))

        self.assertEqual(3, sum(
            r.path == "/DiscoveryApi/v1/systems/KASSANDRA/controls/KAS-00/parameters" for r in mock_server_requests))

        self.assertEqual(1, len(engine_kassandra.execute("SELECT * FROM REGISTRY_KASSANDRA").fetchall()))
        self.assertEqual(1, len(engine_evaluation.execute("SELECT * FROM RX_TRASMISSIONE").fetchall()))

        sys.argv = ["monitoring_service", "123456"]
        apc.time_to_sleep = 0
        monitoring_service.main()

    def _insert_db(self):
        dbm = DatabaseManager()
        current_path = os.path.dirname(os.path.abspath(__file__))
        path_insert_dwa = os.path.join(current_path,
                                       '../dbscripts/test_IT_subject_prioritization_oneshot/insert_dwa.sql')
        path_insert_kassandra = os.path.join(current_path,
                                             '../dbscripts/test_IT_subject_prioritization_oneshot/insert_kassandra.sql')
        path_insert_evaluation = os.path.join(current_path,
                                              '../dbscripts/test_IT_subject_prioritization_oneshot/insert_evaluation.sql')

        super()._read_sql(path_insert_dwa, dbm.engine_dwa)
        super()._read_sql(path_insert_kassandra, dbm.engine_kassandra)
        super()._read_sql(path_insert_evaluation, dbm.engine_evaluation)

    @staticmethod
    def _setup_mock_server():
        httpretty.enable()
        # Census /systemadd
        httpretty.register_uri(
            httpretty.POST,
            apc.url_census + path_system_id_request,
            status=200
        )
        # Census /iconadd
        httpretty.register_uri(
            httpretty.POST,
            apc.url_census + path_icon_request,
            status=200
        )

        # Discovery api
        httpretty.register_uri(
            httpretty.GET,
            apc.url_discovery_api + "/v1/systems/" + apc.system_id_name + "/controls/" + apc.control_code + "/parameters",
            body=json.dumps([
                {
                    "name": "threshold",
                    "description": "Soglia",
                    "valueType": "numeric",
                    "value": "0",
                    "systemId": "KASSANDRA",
                    "controlCode": "KAS-00"
                },
                {
                    "name": "months_to_skip",
                    "description": "Numero di mesi da saltare",
                    "valueType": "numeric",
                    "value": "12",
                    "systemId": "KASSANDRA",
                    "controlCode": "KAS-00"
                },
                {
                    "name": "systems",
                    "description": "Sistemi",
                    "valueType": "string_list",
                    "value": "[\"DISCOVERY\", \"COMPORTAMENT\"]",
                    "systemId": "KASSANDRA",
                    "controlCode": "KAS-00"
                }
            ]),
            status=200
        )


if __name__ == '__main__':
    unittest.main()
