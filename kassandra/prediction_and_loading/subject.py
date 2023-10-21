#!/usr/bin/env python3

"""
@Author: Miro
@Date: 14/06/2023
@Version: 1.0
@Objective: oggetto che contiene le informazioni di un soggetto (NDG, status, features, ecc.)
@TODO:
"""

import numpy as np
import pandas as pd
from datetime import datetime
from kassandra.config_module.prediction_and_loading_config import ndg_length, residence_country_length
from kassandra.prediction_and_loading.features import Features


class Subject:
    def __init__(self, ndg: str, name: str, fiscal_code: str, residence_city: str,
                 birthday: str, juridical_nature: str, residence_country: str,
                 sae: str, ateco: str, office: str, score: float, features_values: Features):
        """
            :param ndg: subject ndg
            :param name: subject name
            :param fiscal_code: subject fiscal code
            :param residence_city: subject residence city
            :param birthday: subject birthdate (only for natural person)
            :param juridical_nature: subject juridical nature
            :param residence_country: subject residence country
            :param sae: subject sae
            :param ateco: subject ateco
            :param office: subject office
            :param score: subject score from prediction
            :param features_values: subject features
        """

        self.ndg = self.pre_process_input(str(ndg), ndg_length)
        self.name = name
        self.fiscal_code = fiscal_code
        self.juridical_nature = self.pre_process_input(juridical_nature)
        self.birth_day = self.birthday_date_convert(birthday)
        self.residence_city = self.pre_process_input(residence_city)
        self.residence_country = self.pre_process_input(str(residence_country), residence_country_length)
        self.sae = self.pre_process_input(str(sae))
        self.ateco = self.pre_process_input(str(ateco))
        self.office = self.pre_process_input(str(office))

        self.score = score
        self.features_values = features_values

    @staticmethod
    def pre_process_input(input_to_process: str, input_length: int = None) -> str:
        """
            preprocessing the input to be input_length characters long
            :param input_to_process: input to be preprocessed
            :param input_length: desired length of the input after preprocessing
            :return: ndg
        """

        # check if the input is empty
        empty_list = [None, 'nan', '', ' ', 'None', np.nan, '<NA>', 'NaT']
        if str(input_to_process) in empty_list:
            return ''

        # remove all the spaces from the input
        input_to_process = input_to_process.split(".")[0]

        if input_length is None: return input_to_process

        # check if the input is already input_length characters long
        original_length = len(input_to_process)
        if original_length <= input_length:
            padded_string = "0" * (input_length - original_length) + input_to_process
            return padded_string
        else:
            raise ValueError("NDG length is greater than desired length")

    def birthday_date_convert(self, birthday: any) -> str:
        """
            convert the birthday from YYYY-MM-DD 00:00:00.000 to DD/MM/YYYY
            :param birthday: birthday to be converted
            :return: birthday converted
        """

        if isinstance(birthday, pd.Timestamp):
            birthday = birthday.strftime('%Y-%m-%d %H:%M:%S')

        birthday = self.pre_process_input(birthday)
        if birthday == '': return ''

        try:
            parsed_date = datetime.strptime(birthday, "%Y-%m-%d %H:%M:%S")
            formatted_date = parsed_date.strftime("%d/%m/%Y")
        except Exception as e:
            raise ValueError("Error in converting birthday date: " + str(e))

        return formatted_date
