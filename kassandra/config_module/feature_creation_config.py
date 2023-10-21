#!/usr/bin/env python3

"""
@Author: Miro
@Date: 26/06/2023
@Version: 1.0
@Objective: Config file for features package
@TODO:
"""

import os

from test.temp_dir_manager import TempDirManager

root_path = os.path.dirname(__file__)
data_path = os.path.join(root_path, '../features_creation/data')

ndg_name = 'NDG'
operations_months_features = 12
x_train_name = '/x_train.csv'
merge_operation_subject_columns = 'CODE_OPERATION'
days_in_year = 365.25

na_values_list = ['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', 'N/A',
                  'NULL', 'NaN', 'n/a', 'nan', 'null', 'None', '<NA>']

# SUBJECT INFORMATION
non_categorical_columns = ['GROSS_INCOME', 'NCHECKREQUIRED', 'NCHECKDEBITED', 'NCHECKAVAILABLE', 'RISK_PROFILE']
output_features_subject = ['NDG', 'AGE', 'LEGAL_SPECIE', 'GROSS_INCOME', 'RESIDENCE_PROVINCE',
                           'SAE', 'ATECO', 'RISK_PROFILE', 'NCHECKREQUIRED', 'NCHECKDEBITED',
                           'NCHECKAVAILABLE', 'REGIONE', 'ZONA GEOGRAFICA',
                           'PROVINCIA DI CONFINE', 'PROVINCIA TURISTICA',
                           'OCCUPAZIONE TOTALE RANGED', '% GRADO DI EVASIONE RANGED',
                           'RAPPORTO IMPRESE PER 100 ABITANTI RANGED',
                           'RAPPORTO SPORTELLI BANCARI OGNI 10000 ABITANTI RANGED',
                           'DENUNCE RICICLAGGIO RANGED', 'ASSOCIAZIONE DI TIPO MAFIOSO RANGED']

# age information
age_column = 'AGE'
birth_column = 'BIRTH_DAY'
max_age = 101
min_age = 0
interval_age = 10

# gross income information
sae_column, ateco_column, residence_province_column = 'SAE', 'ATECO', 'RESIDENCE_PROVINCE'
subjects_columns = [ndg_name, birth_column, 'LEGAL_SPECIE', 'GROSS_INCOME', residence_province_column,
                    'RESIDENCE_CITY', 'RESIDENCE_COUNTRY', sae_column, ateco_column, 'RISK_PROFILE',
                    'NCHECKREQUIRED', 'NCHECKDEBITED', 'NCHECKAVAILABLE']

subjects_dtypes = {ndg_name: 'string', sae_column: 'string', ateco_column: 'string', 'RESIDENCE_COUNTRY': 'string', birth_column: 'string'}
subjects_columns_infix = {sae_column: 'SAE', ateco_column: 'ATECO', residence_province_column: 'PRV'}

# PROVINCE AND COUNTRY INFORMATION
country_province_path = os.path.join(data_path, 'country_province.xlsx')

province_sheet_name = 'Province'

province_columns = ['Sigla', 'Provincia', 'Regione', 'Zona geografica',
                    'Provincia di Confine',
                    'Rapporto Imprese per 100 abitanti',
                    'Provincia Turistica',
                    'Rapporto Sportelli bancari Ogni 10000 abitanti',
                    'Occupazione Totale',
                    '% Grado di Evasione', 'Presenza di distretti industriali',
                    'Poplazione Totale', 'Denunce Riciclaggio',
                    'Associazione di tipo mafioso']

# province categorization information [name of the column, dict with value and category]
alto, medio, basso = 'ALTO', 'MEDIO', 'BASSO'
province_categorization_dict = [['Occupazione Totale', {alto: 0.5, medio: 0.41, basso: 0.0}],
                                ['% Grado di Evasione', {alto: 0.22, medio: 0.125, basso: 0.0}],
                                ['Rapporto Imprese per 100 abitanti', {alto: 11.0, medio: 9.0, basso: 0.0}],
                                ['Rapporto Sportelli bancari Ogni 10000 abitanti', {alto: 6.0, medio: 4.0, basso: 0.0}],
                                ['Denunce Riciclaggio', {alto: 5.5, medio: 1.5, basso: 0.0}],
                                ['Associazione di tipo mafioso', {alto: 1.0, medio: 0.1, basso: 0.0}]]
province_features_to_not_process = ['Sigla', 'Provincia', 'Regione', 'Zona geografica', 'Provincia di Confine',
                                    'Provincia Turistica']

# list values information
list_values_path = os.path.join(data_path, 'list_values.csv')
list_values_delimiter = ';'
list_values_dtype = 'string'

# OPERATION INFORMATION
date_operation_column, amount_column, month_year_column, \
    causal_column, counterpart_column, sign_column = 'DATE_OPERATION', 'AMOUNT', 'MONTH_YEAR', 'CAUSAL', 'COUNTERPART_COUNTRY', 'SIGN'
operations_columns = ['ACCOUNT', sign_column, amount_column, date_operation_column, 'CODE_OPERATION', causal_column,
                      counterpart_column]
operations_dtype = 'string'
sign_options_in, sign_options_out = 'A', 'D'

# analysis information
analytical_causal_path = os.path.join(data_path, 'causale_analitica_13.csv')
analytical_causal_delimiter = ';'
analytical_causal_columns = ['COD_CONTANTE', 'COD_ASSEGNI', 'COD_FINANZIAMENTI', 'COD_BONIFICI_DOMESTICI',
                             'COD_BONIFICI_ESTERI',
                             'COD_TIT_CER_INV', 'COD_POS', 'COD_PAG_INC_DIVERSI', 'COD_EFF_DOC_RIBA', 'COD_DIVIDENDI',
                             'COD_REVERSALI']
analytical_causal_dtype = 'string'
output_features_columns = [ndg_name, 'AVG_FREQ_A', 'AVG_FREQ_D', 'AVG_AMOUNT_A', 'AVG_AMOUNT_D',
                           'RISCHIO_PAESE_TOT_A', 'RISCHIO_PAESE_TOT_D',
                           'AVG_COD_CONTANTE_FREQ_A', 'AVG_COD_CONTANTE_FREQ_D',
                           'AVG_COD_ASSEGNI_FREQ_A', 'AVG_COD_ASSEGNI_FREQ_D',
                           'AVG_COD_FINANZIAMENTI_FREQ_A', 'AVG_COD_FINANZIAMENTI_FREQ_D',
                           'AVG_COD_BONIFICI_DOMESTICI_FREQ_A', 'AVG_COD_BONIFICI_DOMESTICI_FREQ_D',
                           'AVG_COD_BONIFICI_ESTERI_FREQ_A', 'AVG_COD_BONIFICI_ESTERI_FREQ_D',
                           'AVG_COD_TIT_CER_INV_FREQ_A', 'AVG_COD_TIT_CER_INV_FREQ_D',
                           'AVG_COD_POS_FREQ_A', 'AVG_COD_POS_FREQ_D',
                           'AVG_COD_PAG_INC_DIVERSI_FREQ_A', 'AVG_COD_PAG_INC_DIVERSI_FREQ_D',
                           'AVG_COD_EFF_DOC_RIBA_FREQ_A', 'AVG_COD_EFF_DOC_RIBA_FREQ_D',
                           'AVG_COD_DIVIDENDI_FREQ_A', 'AVG_COD_DIVIDENDI_FREQ_D',
                           'AVG_COD_REVERSALI_FREQ_A', 'AVG_COD_REVERSALI_FREQ_D',
                           'AVG_RISCHIO_PAESE_A', 'AVG_RISCHIO_PAESE_D', 'NOT_TO_ALERT_FREQ']

# OPERATION SUBJECT INFORMATION
operations_subject_dtype = 'string'
subject_type_column = 'SUBJECT_TYPE'
operations_subject_columns = ['CODE_OPERATION', 'NDG', subject_type_column]

# TARGET INFORMATION
target_data_source, status_column_source = 'DATA', 'STATO'
target_data, status_column = 'DATE_OF_ANOMALY', 'STATUS'
start_date_operations, end_date_operations = 'START_DATE', 'DATE'
rename_target_columns = {target_data_source: target_data, status_column_source: status_column}
target_columns = [target_data_source, status_column_source, ndg_name]
target_dtype = 'string'
to_alert_value, not_to_alert_value = 'TO_ALERT', 'NOT_TO_ALERT'
binary_target_dict = {to_alert_value: '1', not_to_alert_value: '0'}
