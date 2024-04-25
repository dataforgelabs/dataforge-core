import argparse
import os
import shutil
import sys

from importlib_resources.abc import Traversable

from .databricks_sql import Databricks
from .pg import Pg
import importlib_resources


def get_folder_path(path: str):
    return os.getcwd() if path == '' else path


class MainConfig:
    def __init__(self):
        # check if Java is installed
        if os.environ.get('JAVA_HOME') is None:
            print("Java is not installed or JAVA_HOME environment variable is not set")
            sys.exit(1)
        # parse command line args
        _parser = argparse.ArgumentParser(
            prog='dataforge',
            description='Dataforge Core compiles project and generates SQL queries that create source and output tables defined in the project',
            epilog='Documentation and examples: https://github.com/dataforgelabs/dataforge-core')
        _parser.add_argument('--build', type=str, help='Project folder', metavar='<Project Path>', nargs='?', const='')
        _parser.add_argument('--init', '-i', type=str, help='Initialize project folder', nargs='?', const='')
        _parser.add_argument('--seed', action='store_true', help='Deploy and seed postgres database')
        _parser.add_argument('--connect', '-c', type=str, help='Connect to postgres database',
                             metavar='<Postgres connection string>')
        _parser.add_argument('--connect_databricks', '-d', type=str, help='Connect to databricks SQL warehouse',
                             metavar='<Databricks host URL>')
        _parser.add_argument('--http_path', type=str, help='Databricks SQL warehouse http path',
                             metavar='<Databricks SQL warehouse http path>')
        _parser.add_argument('--access_token', type=str, help='Databricks access token',
                             metavar='<Databricks SQL warehouse access token>')
        _parser.add_argument('--run', '-r', action='store_true',
                             help='Execute compiled project using configured Databricks SQL warehouse connection')

        args = _parser.parse_args()
        self.import_flag = False
        if len(sys.argv) < 2:
            _parser.print_help()
            sys.exit(0)
        if args.connect:
            self.pg = Pg(args.connect, initialize=True)
        else:
            self.pg = Pg()
        if args.seed or args.connect:
            self.pg.seed()
        if args.connect_databricks:
            self.databricks = Databricks(args, True)
        if args.init is not None:
            try:
                self.source_path = get_folder_path(args.init)
                if not os.path.exists(self.source_path):
                    os.makedirs(self.source_path)
                self.traverse_resource_dir(importlib_resources.files().joinpath('resources', 'project'))
                print(f"Initialized project in {self.source_path}")
            except Exception as e:
                print(f"Error initializing project in {self.source_path} : {e}")
            sys.exit(0)
        if args.build is not None:
            self.source_path = get_folder_path(args.build)
            self.output_path = os.path.join(self.source_path, 'target')
            self.log_path = os.path.join(self.output_path, 'log.txt')
            shutil.rmtree(self.output_path, ignore_errors=True)
            os.makedirs(self.output_path)
            self.output_source_path = os.path.join(self.output_path, 'sources')
            os.makedirs(self.output_source_path)
            self.output_output_path = os.path.join(self.output_path, 'outputs')
            os.makedirs(self.output_output_path)
            self.import_flag = True
        self.run_flag = args.run
        if self.run_flag:
            self.databricks = Databricks()

    def traverse_resource_dir(self, resource: Traversable, folder=''):
        for file in resource.iterdir():
            if file.is_dir():
                dir_path = os.path.join(self.source_path, folder, file.name)
                if not os.path.exists(dir_path):
                    os.makedirs(dir_path)
                self.traverse_resource_dir(resource.joinpath(file.name), file.name)
            if file.is_file():
                self.copy_resource_file(folder, file)

    def copy_resource_file(self, folder: str, resource: Traversable):
        file_path = os.path.join(self.source_path, folder, resource.name)
        if os.path.exists(file_path):
            raise Exception(f"File {file_path} already exists")
        with open(file_path, "w") as file:
            # Write the string to the file
            file.write(resource.read_text())
