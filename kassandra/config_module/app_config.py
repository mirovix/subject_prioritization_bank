# !/usr/bin/env python3

"""
@Author: apadoan
@Date: 15/06/2023
@Version: 1.0
@Objective: app configuration file
@TODO:
"""
import os

from dotenv import dotenv_values

from test.temp_dir_manager import TempDirManager

env = dotenv_values()

base_path = env['BASE_PATH']
verbose = int(env['VERBOSE'])

time_to_sleep = int(env['TIME_TO_SLEEP_MONITORING_KASSANDRA'])

if 'TEST_ENV' in os.environ:
    temp = TempDirManager()
    service_folder = temp.get_dir_name()
else:
    service_folder = env['SERVICE_KASSANDRA']

# email variables (in case of error)
mail_notification = env['KASSANDRA_MAIL_NOTIFICATION'].lower() in ['true', '1']
user_mail = env['KASSANDRA_FROM_ADDRESS']
pass_mail = env['SMTP_PASSWORD']
users_receiver_mail = env['KASSANDRA_MAIL_TO_ADDRESS'].split(',')
host_mail = env['SMTP_HOST']
port_mail = env['SMTP_PORT']

# logging
logging_base_path = service_folder + '/logs'
logging_file = logging_base_path + '/kassandra.log'
when_rotate_log = 'midnight'
log_backup_count = 30
logging_format = '%(asctime)-15s %(levelname)-4s %(message)s'
logging_date_format = '%Y-%m-%d %H:%M:%S'

# bank months present in the database
bank_months = 24

# census
url_census = env['URL_CENSUS']
system_id_name = "KASSANDRA"
id_transition = '0001'
system_id_description = "Kassandra"
system_id_description_long = "Clienti con posizione globale da approfondire"

# parameters
control_code = 'KAS-00'
url_discovery_api = env['URL_DISCOVERY_API']

# plots
show_plots = False
save_plots = True
plot_directory = f'{service_folder}/plots'

# results
save_results = True
result_directory = f'{service_folder}/results'

# ml_models
save_model = True
models_directory = f'{service_folder}/ml_models'

# x_train
data_directory = f'{service_folder}/data'

# encoder
save_encoder = True
encoders_directory = f'{service_folder}/encoders'
