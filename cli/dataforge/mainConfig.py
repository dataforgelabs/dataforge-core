import argparse
import os
import shutil
import sys
import yaml
import importlib_resources
from importlib_resources.abc import Traversable
from .databricks_sql import Databricks
from .pg import Pg
from importlib.metadata import version
from .util import get_input, confirm_action


def get_folder_path(path: str):
    return os.getcwd() if path == '' else path


def profile_dir():
    home_path = os.environ['APPDATA'] if sys.platform == 'win32' else os.environ['HOME']
    return os.path.join(home_path, 'Dataforge')


def profile_path():
    return os.path.join(profile_dir(), 'profile.yaml')


class MainConfig:
    def __init__(self):
        # check if Java is installed
        self.run_path = None
        self.config = {}
        self.pg: Pg | None = None
        if os.environ.get('JAVA_HOME') is None:
            print("Java is not installed or JAVA_HOME environment variable is not set")
            sys.exit(1)
        # parse command line args
        _parser = argparse.ArgumentParser(
            prog='dataforge',
            description='Dataforge Core compiles project and generates SQL queries that create source and output tables defined in the project',
            epilog='Documentation and examples: https://github.com/dataforgelabs/dataforge-core')
        _parser.add_argument('--build', '-b', type=str, metavar='Project Path', nargs='?', const='',
                             help='Build project')
        _parser.add_argument('--init', '-i', type=str, help='Initialize project folder', metavar='Project Path', nargs='?', const='')
        _parser.add_argument('--seed', '-s', action='store_true', help='Deploy and seed postgres database')
        _parser.add_argument('--configure', '-c', action='store_true', help='Configure connection profile')
        _parser.add_argument('--version', '-v', action='store_true', help='Display version')

        _parser.add_argument('--profile', '-p', type=str, help='Load configuration from profile file specified by the path',
                             metavar='"Dataforge profile file path"')

        _parser.add_argument('--run', '-r', type=str, help='Execute compiled project using configured Databricks SQL warehouse connection',
                             metavar='Project Path', nargs='?', const='')

        args = _parser.parse_args()
        self.import_flag = False

        if len(sys.argv) < 2:
            _parser.print_help()
            sys.exit(0)
        if args.version:
            print('dataforge-core ' + version('dataforge-core'))
        if args.configure:
            self.load_config(args.profile, True)
            self.configure()
        else:
            self.load_config(args.profile)
        if args.seed or args.configure:
            self.pg.seed()
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
        if args.run is not None:
            self.source_path = get_folder_path(args.run)
            self.output_path = os.path.join(self.source_path, 'target')
            self.run_path = os.path.join(self.output_path, 'run.sql')
            self.databricks = Databricks(self.config['databricks'], path=self.output_path)

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

    def configure(self):
        connection = get_input("Enter Postgres connection string: ", current_value=self.config.get('pg_connection'))
        self.pg = Pg(connection)
        self.config['pg_connection'] = connection
        if confirm_action('Do you want to configure Databricks SQL Warehouse connection (y/n)? '):
            databricks_config = {'hostname': get_input("Enter Server hostname: "),
                                 'http_path': get_input("Enter HTTP path: "),
                                 'access_token': get_input("Enter access token: "),
                                 'catalog': get_input("Enter catalog name: ", 'hive_metastore'),
                                 'schema': get_input("Enter schema name: "),
                                 }
            self.databricks = Databricks(databricks_config, path='', initialize=True)
            self.config['databricks'] = databricks_config
        self.save_config()
        sys.exit(0)

    def save_config(self):
        # Everything validated - save into yaml
        try:
            if not os.path.exists(profile_dir()):
                os.makedirs(profile_dir())
            with open(profile_path(), 'w') as outfile:
                yaml.dump(self.config, outfile, default_flow_style=False)
            print(f"Profile saved in {profile_path()}")
        except Exception as e:
            print(f"Error saving profile: {e}")
            sys.exit(1)

    def load_config(self, path: str = None, ignore_if_not_exists=False):
        # Everything validated - save into yaml
        save_profile_path = path if path else profile_path()
        try:
            if not os.path.exists(save_profile_path):
                if ignore_if_not_exists:
                    return
                else:
                    print(f"Profile {save_profile_path} does not exist. Run dataforge --configure")
                    sys.exit(1)
            if path:
                print(f"Loading profile: {save_profile_path}")
            with open(save_profile_path, 'r') as file:
                self.config = yaml.safe_load(file)
            if not ignore_if_not_exists:
                self.pg = Pg(self.config['pg_connection'])
        except Exception as e:
            print(f"Error loading profile {save_profile_path}: {e}")
            sys.exit(1)
