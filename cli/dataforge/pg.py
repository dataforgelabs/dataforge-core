import os
import sys
import psycopg2
from importlib_resources import files

from .util import confirm_action, save_os_variable


class Pg:
    def __init__(self, connection_string: str):
        try:
            self.connect(connection_string)

        except Exception as e:
            print(f"Error connecting to Postgres: {e}")
            sys.exit(1)

    def sql(self, query: str, params=None, fetch=True):
        # Execute a query
        cur = self.conn.cursor()
        cur.execute(query, params)
        # Retrieve query results
        res = cur.fetchone() if fetch else [None]
        cur.close()
        return res[0]

    def connect(self, connection_string: str):
        # Execute a query
        try:
            self.conn = psycopg2.connect(connection_string)
            self.conn.set_session(autocommit=True)
            self.sql("select 1")  # execute test query
            # Change connection
        except Exception as e:
            print(f"Error connecting to Postgres database or insufficient permissions. Details: {e}")
            sys.exit(1)

    def seed(self):
        schemas = self.sql(
            "select string_agg(schema_name,',') from information_schema.schemata where schema_name IN ('meta','log')")
        if schemas:
            if not confirm_action(
                    f"All objects in schema(s) {schemas} in postgres database will be deleted. Do you want to continue (y/n)? "):
                sys.exit(1)
        #  Drop schemas
        self.sql("DROP SCHEMA IF EXISTS meta CASCADE;"
                 "DROP SCHEMA IF EXISTS log CASCADE;", fetch=False)
        #  Deploy DB code
        print("Initializing database..")
        deploy_sql = files().joinpath('resources', 'pg_deploy.sql').read_text()
        self.sql(deploy_sql, fetch=False)
        print("Database initialized")


