#!/usr/bin/env python3

"""
@Author: apadoan
@Date: 15/06/2023
@Version: 1.0
@Objective: database configuration file
@TODO:
"""
from dotenv import dotenv_values

from kassandra.config_module import app_config as apc

env = dotenv_values()

# drivers
sql_server_driver = 'DRIVER=SQL Server Native Client 11.0'
sql_server_driver_name = 'mssql+pyodbc'
sql_server_query_driver = 'odbc_connect'

mysql_driver_name = 'mysql+pymysql'

oracle_driver_name = 'oracle+cx_oracle'
oracle_service = 'netech'
oracle_instant_client_path = apc.base_path + "/kassandra/drivers/instantclient_19_9"

# db types >> oracle, sqlserver, mysql
oracle_name = 'oracle'
sqlserver_name = 'sqlserver'
mysql_name = 'mysql'
inmermory_name = 'memory'

# db connections
dwa_ssh_host = env['DWA_SSH_HOST']
dwa_ssh_username = env['DWA_SSH_USERNAME']
dwa_ssh_key_file_path = env['DWA_SSH_KEY_FILE_PATH']
dwa_ssh_port = env['DWA_SSH_PORT']
dwa_ssh_hostname = env['DWA_SSH_HOSTNAME']
dwa_server_type = env['DWA_SERVER_TYPE']
dwa_server = env['DWA_SERVER']
dwa_database = env['DWA_DATABASE']
dwa_username = env['DWA_USERNAME']
dwa_password = env['DWA_PASSWORD']
dwa_port = env['DWA_PORT']

kassandra_ssh_host = env['KASSANDRA_SSH_HOST']
kassandra_ssh_username = env['KASSANDRA_SSH_USERNAME']
kassandra_ssh_key_file_path = env['KASSANDRA_SSH_KEY_FILE_PATH']
kassandra_ssh_port = env['KASSANDRA_SSH_PORT']
kassandra_ssh_hostname = env['KASSANDRA_SSH_HOSTNAME']
kassandra_server_type = env['KASSANDRA_SERVER_TYPE']
kassandra_server = env['KASSANDRA_SERVER']
kassandra_database = env['KASSANDRA_DATABASE']
kassandra_username = env['KASSANDRA_USERNAME']
kassandra_password = env['KASSANDRA_PASSWORD']
kassandra_port = env['KASSANDRA_PORT']

evaluation_ssh_host = env['EVALUATION_SSH_HOST']
evaluation_ssh_username = env['EVALUATION_SSH_USERNAME']
evaluation_ssh_key_file_path = env['EVALUATION_SSH_KEY_FILE_PATH']
evaluation_ssh_port = env['EVALUATION_SSH_PORT']
evaluation_ssh_hostname = env['EVALUATION_SSH_HOSTNAME']
evaluation_server_type = env['EVALUATION_SERVER_TYPE']
evaluation_server = env['EVALUATION_SERVER']
evaluation_database = env['EVALUATION_DATABASE']
evaluation_username = env['EVALUATION_USERNAME']
evaluation_password = env['EVALUATION_PASSWORD']
evaluation_port = env['EVALUATION_PORT']

domain_ssh_host = env['DOMAIN_SSH_HOST']
domain_ssh_username = env['DOMAIN_SSH_USERNAME']
domain_ssh_key_file_path = env['DOMAIN_SSH_KEY_FILE_PATH']
domain_ssh_port = env['DOMAIN_SSH_PORT']
domain_ssh_hostname = env['DOMAIN_SSH_HOSTNAME']
domain_server_type = env['DOMAIN_SERVER_TYPE']
domain_server = env['DOMAIN_SERVER']
domain_database = env['DOMAIN_DATABASE']
domain_username = env['DOMAIN_USERNAME']
domain_password = env['DOMAIN_PASSWORD']
domain_port = env['DOMAIN_PORT']
