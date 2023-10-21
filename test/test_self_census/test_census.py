#!/usr/bin/env python3

"""
@Author: Miro
@Date: 24/05/2023
@Version: 1.0
@Objective: class for the self census test
@TODO:
"""

import unittest
from unittest.mock import patch

from parameterized import parameterized

from kassandra.self_census.census import SelfCensus
from test.test_self_census import url_link_test, system_id_name_test, system_id_description_test


class TestCensus(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._private_req_post = 'requests.post'

    @parameterized.expand([(200, True), (500, False)])
    def test_add_system(self, status_code, expected_return):
        with patch(self._private_req_post) as mock:
            mock.return_value.status_code = status_code
            self_census = SelfCensus(url_link=url_link_test,
                                     system_id_name=system_id_name_test,
                                     system_id_description=system_id_description_test)
            self.assertEqual(expected_return, self_census.add_system())

    @parameterized.expand([(200, True), (500, False)])
    def test_add_icon(self, status_code, expected_return):
        with patch(self._private_req_post) as mock:
            mock.return_value.status_code = status_code
            self_census = SelfCensus(url_link=url_link_test,
                                     system_id_name=system_id_name_test,
                                     system_id_description=system_id_description_test)
            self.assertEqual(expected_return, self_census.add_icon())

    def test_add_icon_wrong_file(self):
        with self.assertRaises(FileNotFoundError):
            self_census = SelfCensus(url_link=url_link_test,
                                     system_id_name=system_id_name_test,
                                     system_id_description=system_id_description_test,
                                     icon_file_path="wrong_file")
            self_census.add_icon()

    def test_call_class(self):
        with patch(self._private_req_post) as mock:
            SelfCensus(url_link=url_link_test,
                       system_id_name=system_id_name_test,
                       system_id_description=system_id_description_test)()

            self.assertEqual(mock.call_count, 2)
