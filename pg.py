import os
import re
import sys

import psycopg2


class Pg:
    def __init__(self, connection_string: str=None, initialize=False):
        try:
            if initialize:
                self.conn = psycopg2.connect(connection_string)
                self.initialize()
            else
                conn_string = os.environ.get('DFCORE_PG_CONNECTION')
                if conn_string is None:
                    print("Java is not installed or JAVA_HOME environment variable is not set")
                    sys.exit(1)
            self.conn = psycopg2.connect(connection_string)
            #  "host=development-pg14-cluster-wmp.cluster-crghn1wrpflt.us-west-2.rds.amazonaws.com dbname=core user=stageuser password=goWest123!")
            self.conn.set_session(autocommit=True)

        except Exception as e:
            print(f"Error connecting to Postgres: {e}")
            sys.exit(1)

    def sql(self, query: str, params=None):
        # Execute a query
        cur = self.conn.cursor()
        cur.execute(query, params)
        # Retrieve query results
        res = cur.fetchone()
        cur.close()
        return res[0]

    def initialize(self):
        # Execute a query
        try:
            schemas = self.sql("select string_agg(schema_name,',') from information_schema.schemata where schema_name IN ('meta','log')")
            # Change connection
            if schemas:
                if not confirm_action(f"All objects in schema(s) {schemas} in postgres database will be deleted. Do you want to continue?"):
                    sys.exit(1)
            #  Drop schemas
            #  Deploy DB code
        except Exception as e:
            print(f"Error initializing Postgres database. Make sure you have super-user permission. Details: {e}")
            sys.exit(1)


def confirm_action(message: str):
    while True:
        confirmation = input(message).strip().lower()
        return confirmation in ('yes', 'y')
