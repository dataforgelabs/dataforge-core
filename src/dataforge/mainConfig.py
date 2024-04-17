import argparse
import os
import shutil
import sys
from .pg import Pg


class MainConfig:
    # check if Java is installed
    if os.environ.get('JAVA_HOME') is None:
        print("Java is not installed or JAVA_HOME environment variable is not set")
        sys.exit(1)
    # parse command line args
    _parser = argparse.ArgumentParser()
    _parser.add_argument('--mode', default='import', choices=['import', 'init', 'query'], type=str,
                         help='Operation mode: import, init, query')
    _parser.add_argument('--source', type=str, help='Source folder', metavar='<Source Path>')

    _parser.add_argument('--output', type=str, help='Output folder', metavar='<Output Path>')
    _parser.add_argument('--pg_conn', type=str, help='Postgres connection string',
                         metavar='<Postgres connection string>')

    args = _parser.parse_args()
    if args.mode == "init":
        if args.pg_conn is None:
            print("--pg_conn parameter is required for --mode init")
            sys.exit(1)
        pg = Pg(args.pg_conn, initialize=True)
    else:
        pg = Pg()
    if args.source is None:
        print("--source parameter is not provided")
        sys.exit(0)
    output_path = args.output if args.output else args.source + '_out'
    log_path = os.path.join(output_path, 'log.txt')
    shutil.rmtree(output_path, ignore_errors=True)
    os.makedirs(output_path)
    output_source_path = os.path.join(output_path, 'sources')
    os.makedirs(output_source_path)
    output_output_path = os.path.join(output_path, 'outputs')
    os.makedirs(output_output_path)
