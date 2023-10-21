import os
import unittest

from sqlalchemy import text
from kassandra.config_module import extraction_config as ex_conf
from kassandra.config_module import db_config as dbc
from kassandra.starter.database_manager import DatabaseManager


class AbstractIT(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        current_path = os.path.dirname(os.path.abspath(__file__))
        ex_conf.anomalies_other_systems_path = os.path.join(current_path, 'dbscripts/test_IT_subject_prioritization_oneshot/anomalies_other_systems.sql')
        # Set in memory db
        dbc.kassandra_server_type = dbc.inmermory_name
        dbc.evaluation_server_type = dbc.inmermory_name
        dbc.dwa_server_type = dbc.inmermory_name

    def setUp(self):
        self._init_db()

    def tearDown(self):
        # Re-init connections with empty in-memory db
        dbm = DatabaseManager()
        dbm.engine_dwa = dbm.get_connection_dwa()
        dbm.engine_kassandra = dbm.get_connection_kassandra()
        dbm.engine_evaluation = dbm.get_connection_evaluation()

    def _init_db(self):
        current_path = os.path.dirname(os.path.abspath(__file__))
        path_init_kassandra_sql = os.path.join(current_path, 'dbscripts/init_kassandra.sql')
        path_init_evaluation_sql = os.path.join(current_path, 'dbscripts/init_evaluation.sql')
        path_init_dwa_sql = os.path.join(current_path, 'dbscripts/init_dwa.sql')

        dbm = DatabaseManager()
        engine_kassandra = dbm.engine_kassandra
        engine_evaluation = dbm.engine_evaluation
        engine_dwa = dbm.engine_dwa

        self._read_sql(path_init_kassandra_sql, engine_kassandra)
        self._read_sql(path_init_evaluation_sql, engine_evaluation)
        self._read_sql(path_init_dwa_sql, engine_dwa)

    @staticmethod
    def _read_sql(path_file, engine):
        with open(path_file, 'r') as file:
            sql = file.read()
        statements = sql.split(';')
        with engine.connect() as con:
            for s in statements:
                con.execute(text(s), multi=True)

