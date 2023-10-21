#!/usr/bin/env python3

"""
@Author: Miro
@Date: 07/06/2023
@Version: 1.0
@Objective: configuration for extraction package of the data
@TODO:
"""

import os

anomaly_table_name = 'ANOMALY'
date_operation_name = 'DATE_OPERATION'
format_date = "%Y-%m-%d"

ndg_name = 'NDG'

default_software = ['DISCOVERY', 'COMPORTAMENT']

date_target_col = 'DATA'
status_target_col = 'STATO'
cols_names_evaluation_csv = ['SOFTWARE', date_target_col, status_target_col, ndg_name, 'XML']
dtypes_evaluation_csv = {ndg_name: str}

base_path = os.path.dirname(__file__)

queries_directory = os.path.join(base_path, '../extraction/queries')

ndg_list_path = os.path.join(queries_directory, 'ndg.sql')

if 'TEST_ENV' in os.environ:
    anomalies_other_systems_path = os.path.join(base_path, '../../test/dbscripts/test_IT_subject_prioritization_oneshot/anomalies_other_systems.sql')
else:
    anomalies_other_systems_path = os.path.join(queries_directory, 'anomalies_other_systems.sql')

operations_subject_query_path = os.path.join(queries_directory, 'operations_subject.sql')
operations_query_path = os.path.join(queries_directory, 'operations.sql')
subjects_query_path = os.path.join(queries_directory, 'subjects.sql')
