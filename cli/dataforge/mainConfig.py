import argparse
import os
import shutil
import sys
import traceback

from .databricks_sql import init_databricks
from .util import confirm_action, save_os_variable

from importlib_resources.abc import Traversable
from .pg import Pg
import importlib_resources


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
            epilog='Try our cloud product')
        _parser.add_argument('source', type=str, help='Project folder', metavar='<Project Path>', nargs='?')
        _parser.add_argument('--init', '-i', action='store_true', help='Initialize project folder')
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
        if args.connect:
            self.pg = Pg(args.connect, initialize=True)
            self.pg.seed()
            sys.exit(0)
        else:
            self.pg = Pg()
        if args.seed:
            self.pg.seed()
        if args.connect_databricks:
            init_databricks(args)
        self.source_path = os.getcwd() if args.source is None else args.source
        if args.init:
            try:
                if confirm_action(f"All files and subfolders in {self.source_path} will be deleted. Continue (y/n)? "):
                    shutil.rmtree(self.source_path, ignore_errors=True)
                    os.makedirs(self.source_path)
                    self.traverse_resource_dir(importlib_resources.files().joinpath('resources', 'project'))
                print(f"Initialized project in {self.source_path}")
            except Exception as e:
                traceback.print_stack()
                print(f"Error initializing project in {self.source_path} : {e}")
            sys.exit(0)
        self.run_flag = args.run
        self.output_path = os.path.join(self.source_path, 'target')
        self.log_path = os.path.join(self.output_path, 'log.txt')
        shutil.rmtree(self.output_path, ignore_errors=True)
        os.makedirs(self.output_path)
        self.output_source_path = os.path.join(self.output_path, 'sources')
        os.makedirs(self.output_source_path)
        self.output_output_path = os.path.join(self.output_path, 'outputs')
        os.makedirs(self.output_output_path)

    def traverse_resource_dir(self, resource: Traversable, folder=''):
        for file in resource.iterdir():
            if file.is_dir():
                os.makedirs(os.path.join(self.source_path, folder, file.name))
                self.traverse_resource_dir(resource.joinpath(file.name), file.name)
            if file.is_file():
                self.copy_resource_file(folder, file)

    def copy_resource_file(self, folder: str, resource: Traversable):
        file_name = os.path.join(self.source_path, folder, resource.name)
        with open(file_name, "w") as file:
            # Write the string to the file
            file.write(resource.read_text())
