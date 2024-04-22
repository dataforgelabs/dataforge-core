import sys
from argparse import Namespace
from databricks import sql
import os
from .util import save_os_variable


def run(query: str):
    try:
        connection = sql.connect(
            server_hostname=check_var('DATAFORGE_DATABRICKS_HOST'),
            http_path=check_var('DATAFORGE_DATABRICKS_HTTP_PATH'),
            access_token=check_var('DATAFORGE_DATABRICKS_ACCESS_TOKEN'))

        cursor = connection.cursor()
        cursor.execute(query)
        cursor.close()
        connection.close()

    except Exception as e:
        print(f"Run error {e}")
        sys.exit(1)


def check_var(name: str):
    value = os.environ.get('DATAFORGE_DATABRICKS_HOST')
    if value is None:
        print(f"Environment variable {name} is not initialized. Run dataforge --connect_databricks")
        sys.exit(1)
    return value


def init_databricks(args: Namespace):
    if args.http_path is None:
        print("--http_path parameter is required")
        sys.exit(1)
    if args.access_token is None:
        print("--access_token parameter is required")
        sys.exit(1)
    save_os_variable('DATAFORGE_DATABRICKS_HOST', args.connect_databricks)
    save_os_variable('DATAFORGE_DATABRICKS_HTTP_PATH', args.http_path)
    save_os_variable('DATAFORGE_DATABRICKS_ACCESS_TOKEN', args.access_token)
