import sys
from argparse import Namespace
import os
from .util import save_os_variable, check_var
from databricks import sql


class Databricks:
    def __init__(self, args: Namespace = None, initialize=False):
        try:
            if initialize:
                self.initialize(args)
            else:
                self.host = check_var('DATAFORGE_DATABRICKS_HOST', f"Run dataforge --connect_databricks")
                self.http_path = check_var('DATAFORGE_DATABRICKS_HTTP_PATH', f"Run dataforge --connect_databricks")
                self.access_token = check_var('DATAFORGE_DATABRICKS_ACCESS_TOKEN',
                                              f"Run dataforge --connect_databricks")
        except Exception as e:
            print(f"Error connecting to Databricks: {e}")
            sys.exit(1)

    def run(self, path: str):
        try:
            # load file
            run_file_name = os.path.join(path, 'run.sql')
            with open(run_file_name, "w") as file:
                query = file.read()
                self.execute(query)

        except Exception as e:
            print(f"Run error {e}")
            sys.exit(1)

    def execute(self, query: str, mode='run'):
        try:
            print(f"Connecting to Databricks SQL Warehouse {self.host}")
            connection = sql.connect(
                server_hostname=self.host,
                http_path=self.http_path,
                access_token=self.access_token)

            cursor = connection.cursor()
            if mode == 'run':
                print(f"Executing query")
            cursor.execute(query)
            cursor.close()
            connection.close()
            if mode == 'run':
                print("Execution completed successfully")
            elif mode == 'test':
                print("Databricks connection validated successfully")

        except Exception as e:
            print(f"Run error {e}")
            sys.exit(1)

    def initialize(self, args: Namespace):
        if args.connect_databricks is None:
            print("--connect_databricks parameter is required")
            sys.exit(1)
        if args.http_path is None:
            print("--http_path parameter is required")
            sys.exit(1)
        if args.access_token is None:
            print("--access_token parameter is required")
            sys.exit(1)
        self.host = args.connect_databricks
        self.http_path = args.http_path
        self.access_token = args.access_token
        self.execute("SELECT 1", mode='test')  # execute test query
        save_os_variable('DATAFORGE_DATABRICKS_HOST', self.host)
        save_os_variable('DATAFORGE_DATABRICKS_HTTP_PATH', self.http_path)
        save_os_variable('DATAFORGE_DATABRICKS_ACCESS_TOKEN', self.access_token)
