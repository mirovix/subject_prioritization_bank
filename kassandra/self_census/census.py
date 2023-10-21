#!/usr/bin/env python3

"""
@Author: Miro
@Date: 24/05/2023
@Version: 1.0
@Objective: class for census the system
@TODO:
"""

import logging as log
import requests
from kassandra.config_module.self_census_config import path_icon_file, path_icon_request, \
    path_system_id_request, name_param_id, name_param_description


class SelfCensus:
    def __init__(self, url_link: str, system_id_name: str, system_id_description: str,
                 system_id_path: str = path_system_id_request, icon_path: str = path_icon_request,
                 icon_file_path: str = path_icon_file, param_id: str = name_param_id,
                 param_description: str = name_param_description):
        """
        init class
        :param url_link: url of the api
        :param system_id_name: name of the system
        :param system_id_description: description of the system
        :param system_id_path: path for the system id request
        :param icon_path: path for the icon request
        :param icon_file_path: path for the icon file
        :param param_id: name of the id parameter
        :param param_description: name of the description parameter
        """

        self._private_url = url_link
        self._private_system_id_name = system_id_name.strip().upper()
        self._private_system_id_description = system_id_description.strip()

        self._private_system_id_path = system_id_path
        self._private_icon_path = icon_path
        self._private_icon_file_path = icon_file_path
        self._private_param_id = param_id
        self._private_param_description = param_description

    def __call__(self):
        """
        call class
        :return:
        """

        response_add_system = self.add_system()
        response_add_icon = self.add_icon()
        if response_add_system and response_add_icon:
            log.info(f">> All requests for {self._private_system_id_name} completed successfully")
        else:
            log.info(f">> Some requests for {self._private_system_id_name} failed")

    def add_system(self) -> bool:
        """
        add system to the registry
        :return: True if the system is added, False otherwise
        """

        log.info(f">> Adding system {self._private_system_id_name} to the registry")
        payload_system_id = {self._private_param_id: self._private_system_id_name,
                             self._private_param_description: self._private_system_id_description}

        try:
            response_system_id = requests.post(self._private_url + self._private_system_id_path, data=payload_system_id)
            if response_system_id.status_code == 200:
                return True
        except Exception as e:
            log.error(f"System {self._private_system_id_name} request failed with error: {e}")
        return False

    def add_icon(self) -> bool:
        """
        add icon to the registry
        :return: True if the icon is added, False otherwise
        """

        log.info(f">> Adding icon {self._private_system_id_name} to the registry")
        payload_icon = {self._private_param_id: self._private_system_id_name}

        try:
            files = {'file': open(self._private_icon_file_path, 'rb')}
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Icon file not found: {self._private_icon_file_path}") from e

        try:
            response_icon = requests.post(self._private_url + self._private_icon_path, data=payload_icon, files=files)
            if response_icon.status_code == 200:
                files['file'].close()
                return True
        except Exception as e:
            log.error(f"Icon {self._private_system_id_name} request failed with error: {e}")

        files['file'].close()
        return False
