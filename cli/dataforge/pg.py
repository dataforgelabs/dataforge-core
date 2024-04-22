import os
import sys
import psycopg2
from importlib_resources import files

from .util import confirm_action, save_os_variable


class Pg:
    def __init__(self, connection_string: str = None, initialize=False):
        try:
            if initialize:
                self.initialize(connection_string)
            else:
                conn_string = os.environ.get('DATAFORGE_PG_CONNECTION')
                print(f"Connecting to Postgres..")
                if conn_string is None:
                    print("Postgres connection is not initialized. Run dataforge --connect \"pg_connection\"")
                    sys.exit(1)
                self.conn = psycopg2.connect(conn_string)
                self.conn.set_session(autocommit=True)

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

    def initialize(self, connection_string: str):
        # Execute a query
        try:
            print("Platform :", sys.platform)
            self.conn = psycopg2.connect(connection_string)
            self.sql("select 1")  # execute test query
            save_os_variable('DATAFORGE_PG_CONNECTION', connection_string)
            # Change connection
        except Exception as e:
            print(f"Error initializing Postgres database or insufficient permissions. Details: {e}")
            sys.exit(1)

    def seed(self):
        schemas = self.sql(
            "select string_agg(schema_name,',') from information_schema.schemata where schema_name IN ('meta','log')")
        if schemas:
            if not confirm_action(
                    f"All objects in schema(s) {schemas} in postgres database will be deleted. Do you want to continue (y/n)?"):
                sys.exit(1)
        #  Drop schemas
        self.sql("DROP SCHEMA IF EXISTS meta CASCADE;"
                 "DROP SCHEMA IF EXISTS log CASCADE;", fetch=False)
        #  Deploy DB code
        print("Initializing database..")
        deploy_sql = files().joinpath('resources', 'pg_deploy.sql').read_text()
        self.sql(deploy_sql, fetch=False)
        print("Database initialized")


