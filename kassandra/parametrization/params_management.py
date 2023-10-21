#!/usr/bin/env python3

"""
@Author: Miro
@Date: 25/05/2023
@Version: 1.0
@Objective: class for params definition
@TODO:
"""

import requests
import logging as log
from kassandra.config_module.parametrization_config import name_place_holder_api, name_value_api, name_description_api
from kassandra.config_module.parametrization_config import name_value_type_api
from kassandra.parametrization.parameter import Parameter


class ParameterManagement:
    def __init__(self, system_id: str, control_code: str):
        """
            init class
            :param system_id: system id (e.g. 'Kassandra')
            :param control_code: control code of the parameter
        """

        self._private_system_id = system_id
        self._private_control_code = control_code

    def get_parameters_api(self, url_api: str, parameter_place_holder: str = None) -> any:
        """
        get all parameters from api
        :param url_api: url of the api (e.g. http://localhost:8080/DiscoveryApi)
        :param parameter_place_holder: parameter name, if not specified return all parameters
        :return:
        """

        url = url_api + "/v1/systems/" + self._private_system_id + "/controls/" + self._private_control_code + "/parameters"

        try:
            response = requests.get(url)
        except Exception as e:
            raise ConnectionError(f">> Error during the get of the parameters from api: {e}")

        if response.status_code != 200:
            raise ConnectionError(f">> Error during the get of the parameters from api: {response.status_code}")

        parameter_list = []

        log.debug(f'>> Response code from {url}: {response.status_code}')
        log.debug(f'>> Response from {url}: {response.json()}')

        for parameter in response.json():
            place_holder = parameter[name_place_holder_api].strip()
            description = parameter[name_description_api].strip()
            value = parameter[name_value_api].strip()
            value_type = parameter[name_value_type_api].strip()
            parameter = Parameter(place_holder=place_holder, description=description, value=value,
                                  value_type=value_type)

            if parameter_place_holder is not None and place_holder == parameter_place_holder.strip():
                return parameter

            parameter_list.append(parameter)

        if parameter_place_holder is not None:
            return None

        return parameter_list
