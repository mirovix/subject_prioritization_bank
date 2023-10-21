#!/usr/bin/env python3

"""
@Author: Miro
@Date: 29/05/2023
@Version: 1.0
@Objective: class for the parameters management test
@TODO:
"""

import unittest
from unittest.mock import patch

from kassandra.parametrization.parameter import Parameter
from kassandra.parametrization.params_management import ParameterManagement


class TestParameterManagement(unittest.TestCase):
    def setUp(self):
        self._private_system_id = 'KASSANDRA'
        self._private_control_code = '01'
        self._private_req_get = 'requests.get'
        self._url_link_test = 'http://localhost:8080/Test'

        self._private_parameter_management = ParameterManagement(system_id=self._private_system_id,
                                                                 control_code=self._private_control_code)

        self._private_sigle_parameter = Parameter(place_holder='place_holder_1',
                                                  value='\"1\"',
                                                  description='description_1',
                                                  value_type='string')

        self._private_list_parameters = [Parameter(place_holder='place_holder_1',
                                                   value='\"1\"',
                                                   description='description_1',
                                                   value_type='string'),
                                         Parameter(place_holder='place_holder_2',
                                                   value='\"A1\"',
                                                   description='description_2',
                                                   value_type='string')]

    def test_get_parameters_api_list(self):
        with patch(self._private_req_get) as mock:
            self.mock_response(mock)
            result_list_param = self._private_parameter_management.get_parameters_api(self._url_link_test)
            self.assertEqual(len(self._private_list_parameters), len(result_list_param))
            for i in range(len(result_list_param)):
                self.assertTrue(self._private_list_parameters[i] == result_list_param[i])

    def test_get_parameters_api_single_param(self):
        with patch(self._private_req_get) as mock:
            self.mock_response(mock)
            result_list_param = self._private_parameter_management.get_parameters_api(self._url_link_test,
                                                                                      parameter_place_holder="place_holder_1")
            self.assertTrue(self._private_sigle_parameter == result_list_param)

    def test_get_parameters_api_given_error_status_code_raise_connection_error(self):
        with self.assertRaises(ConnectionError):
            with patch(self._private_req_get) as mock:
                mock.return_value.status_code = 404
                self._private_parameter_management.get_parameters_api('')

    def test_get_parameters_api_given_error_exception_on_get_raise_connection_error(self):
        with self.assertRaises(ConnectionError):
            with patch(self._private_req_get) as mock:
                mock.raiseError.side_effect = Exception('Sample exception')
                self._private_parameter_management.get_parameters_api('')

    @staticmethod
    def mock_response(mock):
        mock.return_value.status_code = 200
        mock.return_value.json.return_value = [
            {
                "name": "place_holder_1",
                "description": "description_1",
                "valueType": "string",
                "value": "\"1\"",
                "systemId": "KASSANDRA",
                "controlCode": "01"
            },
            {
                "name": "place_holder_2",
                "description": "description_2",
                "valueType": "string",
                "value": "\"A1\"",
                "systemId": "KASSANDRA",
                "controlCode": "01"
            }
        ]


if __name__ == '__main__':
    unittest.main()
