#!/usr/bin/env python3

"""
@Author: apadoan
@Date: 15/06/2023
@Version: 1.0
@Objective: test db connection
@TODO:
"""

import os
import unittest
from unittest.mock import MagicMock

from paramiko.ssh_exception import SSHException
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from kassandra.config_module import db_config as dbc
from kassandra.starter.database_manager import DatabaseManager


class TestDatabaseManager(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # Set in memory db
        dbc.kassandra_server_type = dbc.inmermory_name
        dbc.evaluation_server_type = dbc.inmermory_name
        dbc.dwa_server_type = dbc.inmermory_name

    def test_database_engine_init(self):
        database_manager = DatabaseManager()
        self.assertIsInstance(database_manager.engine_evaluation, Engine)
        self.assertIsInstance(database_manager.engine_kassandra, Engine)
        self.assertIsInstance(database_manager.engine_evaluation, Engine)

    def test_get_connection_dwa(self):
        database_manager = DatabaseManager()
        engine = database_manager.get_connection_dwa()
        self.assertIsInstance(engine, Engine)

    def test_get_connection_kassandra(self):
        database_manager = DatabaseManager()
        engine = database_manager.get_connection_kassandra()
        self.assertIsInstance(engine, Engine)

    def test_get_connection_evaluation(self):
        database_manager = DatabaseManager()
        engine = database_manager.get_connection_evaluation()
        self.assertIsInstance(engine, Engine)

    def test_db_connection_with_wrong_server_type(self):
        info_db = ("server", "database", "user", "password", "port")
        info_ssh = ("ssh_host", "ssh_username", "ssh_key_file_path", "ssh_port", "ssh_hostname")
        server_type = "unknown server type"
        database_manager = DatabaseManager()
        self.assertRaises(SQLAlchemyError, lambda: database_manager._db_connection(info_db, info_ssh, server_type))

    def test_db_connection_with_wrong_ssh_port(self):
        info_db = ("server", "database", "user", "password", "port")
        info_ssh = ("ssh_host", "ssh_username", MagicMock(), "ssh_port", "ssh_hostname")
        server_type = "sql_server"

        database_manager = DatabaseManager()
        self.assertRaises(SSHException, lambda: database_manager._db_connection(info_db, info_ssh, server_type))

    def test_given_two_database_manager_objects_are_the_same(self):
        instance1 = DatabaseManager()
        instance2 = DatabaseManager()
        self.assertEqual(instance1.engine_dwa, instance2.engine_dwa)
        self.assertEqual(instance1.engine_kassandra, instance2.engine_kassandra)
        self.assertEqual(instance1.engine_evaluation, instance2.engine_evaluation)


if __name__ == '__main__':
    unittest.main()
