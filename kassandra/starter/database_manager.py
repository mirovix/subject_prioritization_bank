#!/usr/bin/env python3

"""
@Author: apadoan
@Date: 15/06/2023
@Version: 1.0
@Objective: db connection setup
@TODO:
"""

import os
import cx_Oracle
import paramiko
from paramiko.ssh_exception import SSHException
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine, engine
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError
from kassandra.config_module import db_config as dbc
from kassandra.starter.log import log


class DatabaseManager:
    _instance = None
    _initialized = False

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self.ssh_tunnels = []
            log.debug(">> CONNECTION TO DWA")
            self.engine_dwa = self.get_connection_dwa()
            log.debug(">> CONNECTION TO KASSANDRA")
            self.engine_kassandra = self.get_connection_kassandra()
            log.debug(">> CONNECTION TO EVALUATION")
            self.engine_evaluation = self.get_connection_evaluation()
            log.debug(">> CONNECTION TO DOMAIN")
            self.engine_domain = self.get_connection_domain()
            self._initialized = True

    def get_connection_dwa(self) -> engine:
        """"
        Database connection for dwa
        :return engine
        """

        info_db = dbc.dwa_server, dbc.dwa_database, dbc.dwa_username, dbc.dwa_password, dbc.dwa_port
        info_ssh = dbc.dwa_ssh_host, dbc.dwa_ssh_username, dbc.dwa_ssh_key_file_path, dbc.dwa_ssh_port, dbc.dwa_ssh_hostname

        return self._db_connection(info_db, info_ssh, dbc.dwa_server_type)

    def get_connection_kassandra(self) -> engine:
        """"
        Database connection for kassandra
        :return engine
        """

        info_db = dbc.kassandra_server, dbc.kassandra_database, dbc.kassandra_username, dbc.kassandra_password, dbc.kassandra_port
        info_ssh = dbc.kassandra_ssh_host, dbc.kassandra_ssh_username, dbc.kassandra_ssh_key_file_path, dbc.kassandra_ssh_port, dbc.kassandra_ssh_hostname

        return self._db_connection(info_db, info_ssh, dbc.kassandra_server_type)

    def get_connection_evaluation(self) -> engine:
        """"
        Database connection for evaluation
        :return engine
        """

        info_db = dbc.evaluation_server, dbc.evaluation_database, dbc.evaluation_username, dbc.evaluation_password, dbc.evaluation_port
        info_ssh = dbc.evaluation_ssh_host, dbc.evaluation_ssh_username, dbc.evaluation_ssh_key_file_path, dbc.evaluation_ssh_port, dbc.evaluation_ssh_hostname

        return self._db_connection(info_db, info_ssh, dbc.evaluation_server_type)

    def get_connection_domain(self):
        """"
        Database connection for domain
        :return engine
        """

        info_db = dbc.domain_server, dbc.domain_database, dbc.domain_username, dbc.domain_password, dbc.domain_port
        info_ssh = dbc.domain_ssh_host, dbc.domain_ssh_username, dbc.domain_ssh_key_file_path, dbc.domain_ssh_port, dbc.domain_ssh_hostname

        return self._db_connection(info_db, info_ssh, dbc.domain_server_type)

    def close_connection(self) -> None:
        """
            close all connections
            :return None
        """

        for ssh_tunnel in self.ssh_tunnels:
            ssh_tunnel.close()

        self.engine_dwa.dispose()
        self.engine_kassandra.dispose()
        self.engine_evaluation.dispose()
        self.engine_domain.dispose()

        log.info(">> Connections closed")

    @staticmethod
    def _get_connection_string(server: str, database: str, user: str,
                               pw: str, port: str, server_type: str) -> any:
        """
            database connection string (oracle, mysql, sql server)
            :param server: server name or ip
            :param database: database name
            :param user: username db
            :param pw: password
            :param port: port
            :param server_type: server type (oracle, mysql, sql server)
            :return: connection string
        """

        if server_type == dbc.sqlserver_name:
            connection_string = dbc.sql_server_driver + \
                                ';SERVER=' + server + \
                                ';PORT=' + port + \
                                ';DATABASE=' + database + \
                                ';UID=' + user + \
                                ';PWD=' + pw + \
                                ';TDS_Version=8.0'
            connection_url = URL.create(dbc.sql_server_driver_name,
                                        query={dbc.sql_server_query_driver: connection_string})

        elif server_type == dbc.mysql_name:
            connection_url = dbc.mysql_driver_name + "://{0}:{1}@{2}:{3}/{4}".format(user, pw, server, port, database)

        elif server_type == dbc.oracle_name:
            try:
                cx_Oracle.init_oracle_client(lib_dir=dbc.oracle_instant_client_path)
            except Exception as e:
                log.warning(">> Oracle instant client not found " + str(e), exc_info=True)
            connection_url = dbc.oracle_driver_name + "://{0}:{1}@{2}:{3}/?service_name={4}".format(user, pw, server,
                                                                                                    port,
                                                                                                    dbc.oracle_service)
        elif server_type == dbc.inmermory_name:
            connection_url = 'sqlite:///:memory:'
        else:
            log.error(">> Server type not found", exc_info=True)
            raise NameError(">> Server type not found")

        log.info(">> Connection db" +
                " >> SERVER: " + server +
                " >> DATABASE: " + database +
                " >> PORT: " + port +
                " >> USER: " + user)

        return connection_url


    def _db_connection(self, info_db: tuple, info_ssh: tuple, server_type: str) -> engine:
        """
            database connection (oracle, mysql, sql server)
            :param info_db: database info (server, database, user, pw, driver, port)
            :param info_ssh: ssh info (host, user, key_file_path, port, hostname)
            :param server_type: server type (oracle, mysql, sql server)
            :return: engine
        """

        ssh_host, ssh_username, ssh_key_file_path, ssh_port, ssh_hostname = info_ssh
        server, database, user, pw, port = info_db

        if ssh_host:
            if not os.path.exists(ssh_key_file_path):
                log.warning(">> SSH key file not found " + ssh_key_file_path, exc_info=True)
            else:
                try:
                    log.info(">> SSH connection for " + database + " database ...")

                    key = paramiko.RSAKey.from_private_key_file(ssh_key_file_path)
                    ssh_tunnel = SSHTunnelForwarder((ssh_host, int(ssh_port)),
                                                    ssh_username=ssh_username,
                                                    ssh_pkey=key,
                                                    remote_bind_address=(ssh_hostname, int(port)))
                    ssh_tunnel.start()
                    self.ssh_tunnels.append(ssh_tunnel)
                    port = str(ssh_tunnel.local_bind_port)
                    log.info(">> SSH connection established for " + ssh_host + ":" + ssh_port)
                except Exception as e:
                    log.error(">> SSH connection could not be made due to the following error " + str(e), exc_info=True)
                    raise SSHException(">> SSH connection could not be made")

        try:
            connection_string = self._get_connection_string(server, database, user, pw, port, server_type)
            new_engine = create_engine(connection_string)
        except Exception as e:
            log.error(">> Connection could not be made due to the following error " + str(e), exc_info=True)
            raise SQLAlchemyError(">> Error in connection to database")

        return new_engine