import argparse
import os
import shutil

from pg import Pg


class MainConfig:
    # parse command line args
    _parser = argparse.ArgumentParser()
    _parser.add_argument('--mode', default='import', choices=['import', 'init', 'query'], type=str,
                        help='Operation mode: import, init, query')
    _parser.add_argument('--source', type=str, help='Source folder', required=True, metavar='<Source Path>')

    _parser.add_argument('--output', type=str, help='Output folder', metavar='<Output Path>')
    args = _parser.parse_args()
    print("Parameters ", args)
    output_path = args.output if args.output else args.source + '_out'
    log_path = os.path.join(output_path, 'log.txt')
    shutil.rmtree(output_path, ignore_errors=True)
    os.makedirs(output_path)
    output_source_path = os.path.join(output_path, 'sources')
    os.makedirs(output_source_path)
    output_output_path = os.path.join(output_path, 'outputs')
    os.makedirs(output_output_path)

    pg = Pg()  # connect to postgres
