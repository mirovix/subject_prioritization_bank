#!/usr/bin/env python3

"""
@Author: Miro
@Date: 29/05/2023
@Version: 1.0
@Objective: test parametrization class
@TODO:
"""

import unittest

from parameterized import parameterized

from kassandra.parametrization.parameter import Parameter


def get_valid_parameter_values():
    return [
        (1, "numeric", 1),
        ('1', "numeric", 1),
        ('\"1\"', "string", "1"),
        ('\"A1\"', "string", "A1"),
        ("[1, 2, 3]", "string", "[1, 2, 3]"),
        ("[4, 5, 6]", "string_list", [4, 5, 6]),
        ("[\"STR1\", \"STR2\"]", "string_list", ["STR1", "STR2"])
    ]


def get_wrong_parameter_values():
    return [
        ('\"A1\"', "numeric", TypeError),
        ('\"A1\"', "string_list", TypeError),
        ("1", "string_list", TypeError),
        ('\"A1\"', "numerico", TypeError),
    ]


class TestParameter(unittest.TestCase):
    def setUp(self):
        self.parameter = Parameter('place_holder', '\"value\"', 'description', 'string')

    def test_get_place_holder(self):
        self.assertEqual(self.parameter.get_place_holder(), 'place_holder')

    def test_get_value(self):
        self.assertEqual(self.parameter.get_value(), 'value')

    def test_get_description(self):
        self.assertEqual(self.parameter.get_description(), 'description')

    def test_get_type(self):
        self.assertEqual(self.parameter.get_type(), 'string')

    def test_str(self):
        self.assertEqual(str(self.parameter), 'Parameter: place_holder - value - string - description')

    @parameterized.expand(get_valid_parameter_values())
    def test_private_define_type_value(self, input_value, type_value, expected_value):
        value = self.parameter._private_define_type_value(input_value, type_value)
        self.assertEqual(value, expected_value)

    @parameterized.expand(get_wrong_parameter_values())
    def test_wrong_type_raise_error(self, input_value, type_value, expected_error):
        with self.assertRaises(expected_error):
            self.parameter._private_define_type_value(input_value, type_value)

    def test_eq(self):
        expected_value = '\"value\"'
        self.assertTrue(self.parameter == Parameter('place_holder', expected_value, 'description', 'string'))
        self.assertFalse(self.parameter == Parameter('place_holder', expected_value, 'description2', 'string'))
        self.assertFalse(self.parameter == Parameter('place_holder', '\"value2\"', 'description', 'string'))
        self.assertFalse(self.parameter == Parameter('place_holder2', expected_value, 'description', 'string'))
        self.assertFalse(self.parameter == Parameter('place_holder2', '\"value2\"', 'description2', 'string'))
