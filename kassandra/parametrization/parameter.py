#!/usr/bin/env python3

"""
@Author: Miro
@Date: 25/05/2023
@Version: 1.0
@Objective: parameter class
@TODO:
"""


class Parameter:
    def __init__(self, place_holder: str, value, description: str, value_type: str):
        """
            define a parameter object
            :param place_holder: placeholder of the parameter (i.e. the name of the parameter)
            :param value: value of the parameter
            :param description: description of the parameter
            :param value_type: type of the value
        """

        self._private_place_holder = place_holder
        self._private_description = description
        self._private_type = value_type
        self._private_value = self._private_define_type_value(value, self._private_type)

    @staticmethod
    def _private_define_type_value(value_input, type_value: str) -> any:
        """
            define the value of the parameter given its type
            :param value_input: value of the parameter
            :param type_value: type of the value passed as input
            :return: value of correct type
        """
        value = value_input

        try:
            if type_value.lower() == 'string':
                value = str(eval(value_input))
            elif type_value.lower() == 'numeric':
                value = int(value_input)
            elif type_value.lower() == 'string_list':
                value = eval(value_input)
                if not isinstance(value, list):
                    raise TypeError(f'>> Value {value} not of type list')
            else:
                raise TypeError(f'>> Wrong type value {type_value}')
        except (SyntaxError, NameError, ValueError, TypeError) as e:
            raise TypeError(f'>> Error: {e} - {value_input} - {type(value_input)}')
        return value

    def __eq__(self, other):
        """
            compare two parameters
            :param other: parameter to compare
            :return: boolean
        """

        if isinstance(other, Parameter):
            return self._private_place_holder == other._private_place_holder and \
                   self._private_value == other._private_value and \
                   self._private_type == other._private_type and \
                   self._private_description == other._private_description
        else:
            return False

    def __str__(self) -> str:
        """
            return the string representation of the parameter
            :return: string representation of the parameter
        """

        return f'Parameter: {self._private_place_holder} - {self._private_value} - ' \
               f'{self._private_type} - {self._private_description}'

    def get_place_holder(self) -> str:
        """
            return the placeholder of the parameter
            :return: placeholder
        """

        return self._private_place_holder

    def get_value(self):
        """
            return the value of the parameter
            :return: value
        """

        return self._private_value

    def get_description(self) -> str:
        """
            return the description of the parameter
            :return: description
        """

        return self._private_description

    def get_type(self) -> str:
        """
            return the type of the parameter
            :return: type
        """

        return self._private_type
