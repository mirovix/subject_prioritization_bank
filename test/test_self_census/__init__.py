#!/usr/bin/env python3

"""
@Author: Miro
@Date: 24/05/2023
@Version: 1.0
@Objective: configuration for self census management test
@TODO:
"""

from dotenv import dotenv_values

env = dotenv_values()
url_link_test = env['URL_CENSUS']
system_id_name_test = "Kassandra"
system_id_description_test = "Kassandra"
