import os
import sys
import psycopg2


class Pg:
    def __init__(self, connection_string: str = None, initialize=False):
        try:
            if initialize:
                self.initialize(connection_string)
            else:
                conn_string = os.environ.get('DATAFORGE_PG_CONNECTION')
                print(f"Connecting to Postgres..")
                if conn_string is None:
                    print("Postgres connection is not initialized. Run with --mode init")
                    sys.exit(1)
                self.conn = psycopg2.connect(conn_string)
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

    def initialize(self, connection_string: str):
        # Execute a query
        try:
            self.conn = psycopg2.connect(connection_string)
            schemas = self.sql(
                "select string_agg(schema_name,',') from information_schema.schemata where schema_name IN ('meta','log')")
            os.system(f"SETX DATAFORGE_PG_CONNECTION \"{connection_string}\"")
            # Change connection
            if schemas:
                if not confirm_action(
                        f"All objects in schema(s) {schemas} in postgres database will be deleted. Do you want to continue?"):
                    sys.exit(1)
            #  Drop schemas
            #  Deploy DB code
            # Open the resource file in read mode
            # with open("resources/pg_deploy.sql", "r") as file:
                # Read the contents of the file
            #    file_contents = file.read()

            # Print or process the contents of the file
            # print(file_contents)
            print("Please restart your console")
        except Exception as e:
            print(f"Error initializing Postgres database or insufficient permissions. Details: {e}")
            sys.exit(1)


def confirm_action(message: str):
    while True:
        confirmation = input(message).strip().lower()
        return confirmation in ('yes', 'y')
