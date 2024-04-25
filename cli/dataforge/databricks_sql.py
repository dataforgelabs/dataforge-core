import sys
import os
from .util import  validate_value
from databricks import sql


class Databricks:
    def __init__(self, config, initialize=False):
        try:
            self.config = config
            if initialize:
                self.validate()
        except Exception as e:
            print(f"Error connecting to Databricks: {e}")
            sys.exit(1)

    def run(self, path: str):
        try:
            # load file
            with open(path, "r") as file:
                query = file.read()
                self.execute(query)

        except Exception as e:
            print(f"Run error {e}")
            sys.exit(1)

    def execute(self, query: str, mode='run'):
        try:
            print(f"Connecting to Databricks SQL Warehouse {self.config['hostname']}")
            connection = sql.connect(
                server_hostname=self.config['hostname'],
                http_path=self.config['http_path'],
                access_token=self.config['access_token'],
                catalog=self.config['catalog'],
                schema=self.config['schema']
            )

            cursor = connection.cursor()
            if mode == 'run':
                print(f"Executing query")
            for statement in query.split(';'):
                try:
                    cursor.execute(statement)
                except Exception as e:
                    print(f"Databricks error {e} while executing statement {statement}")
                    sys.exit(1)
            cursor.close()
            connection.close()
            if mode == 'run':
                print("Execution completed successfully")
            elif mode == 'test':
                print("Databricks connection validated successfully")

        except Exception as e:
            print(f"Databricks error {e}")
            sys.exit(1)

    def validate(self):
        validate_value(self.config,'hostname')
        validate_value(self.config,'http_path')
        validate_value(self.config,'access_token')
        validate_value(self.config,'catalog')
        validate_value(self.config, 'schema')
        self.execute("SELECT 1 as i;", mode='test')  # execute test query
