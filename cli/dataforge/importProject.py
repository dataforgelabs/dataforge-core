import json
import os
import sys

import yaml
from .mainConfig import MainConfig
from .miniSparky import MiniSparky


class ImportProject:
    def __init__(self, config: MainConfig):
        self.ms = None
        self._config = config
        self.import_id = 0

    def start(self):
        _import_id = self._config.pg.sql("select meta.svc_import_start()")
        self.import_id = int(_import_id)
        print('Started import with id ', self.import_id)

    def validate(self):
        print(f"Validating project path {self._config.source_path}")
        meta_flag = False
        source_flag = False
        with os.scandir(self._config.source_path) as entries:
            for file in entries:
                if file.is_dir() and file.name == 'sources':
                    source_flag = True
                elif file.name == "meta.yaml":
                    meta_flag = True
        if not meta_flag:
            print(f"Missing meta.yaml in project path {self._config.source_path}")
            sys.exit(1)
        if not source_flag:
            print(f"Missing sources folder in project path {self._config.source_path}")
            sys.exit(1)

    def load(self):
        self.validate()
        self.start()
        with os.scandir(self._config.source_path) as entries:
            for file in entries:
                if file.is_dir() and file.name in ('sources', 'outputs'):
                    self.list_files(file.path, file.name)
                elif file.name.endswith(".yaml"):
                    self.load_file(file.path, file.name)
        self._config.pg.sql("SELECT meta.svc_import_complete(%s, 'I')", [self.import_id])
        self._config.pg.sql("SELECT meta.imp_parse_objects(%s)", [self.import_id])
        print("Files parsed")
        self._config.pg.sql("SELECT meta.svc_import_execute(%s)", [self.import_id])
        print("Objects loaded")
        self.test_expressions()
        print("Expressions validated")
        print("Import completed successfully")
        self.write_log()
        self.write_queries("source")
        self.write_queries("output")

    def list_files(self, path: str, folder_name: str):
        print("Importing files..")
        with os.scandir(path) as entries:
            for file in entries:
                if file.is_file() & file.name.endswith(".yaml"):
                    self.load_file(file.path, folder_name + '/' + file.name)

    def load_file(self, full_path: str, path: str):
        print(path)
        with open(full_path, 'r') as file:
            file_js = yaml.safe_load(file)
            self._config.pg.sql("SELECT meta.svc_import_load_object(%s, %s, %s)",
                                (self.import_id, path, json.dumps(file_js)))

    def test_expressions(self):
        exps = self._config.pg.sql("SELECT meta.impc_test_expressions(%s)", [self.import_id])
        self.ms = MiniSparky(self._config)
        self.test_expressions_recursive(exps)
        self.ms.stop()
        del self.ms

    def test_expressions_recursive(self, test_expressions, recursion_level=0):
        try:
            test_results = []
            for exp in test_expressions:
                query_result = self.ms.execute_query(exp['expression'])
                del exp['expression']
                exp['result'] = query_result
                test_results.append(exp)
            #  update test results
            test_results_str = json.dumps(test_results)
            res = self._config.pg.sql("SELECT meta.impc_update_test_results(%s, %s)",
                                      (self.import_id, test_results_str))
            if res.get('error'):
                self.fail_import('Invalid expression detected. See log file for details')
            if recursion_level > 20:
                self.fail_import('Maximum recursion exceeded while testing expressions. Check error logs and '
                                 'expression test tables for more details')
            if len(res['next']) > 0:
                self.test_expressions_recursive(res['next'], recursion_level + 1)
        except Exception as e:
            print(e)
            self.fail_import(str(e))

    def fail_import(self, message):
        print(f"Import failed: {message}")
        self._config.pg.sql("SELECT meta.svc_import_complete(%s, 'F', %s)", (self.import_id, message))
        self.write_log()
        sys.exit(1)

    def write_log(self):
        log_file = self._config.pg.sql("SELECT meta.svc_import_get_log(%s)", [self.import_id])
        with open(self._config.log_path, "w") as file:
            # Write the string to the file
            file.write(log_file)

    def write_queries(self, out_type: str):
        queries = self._config.pg.sql(f"select meta.svcc_get_{out_type}_queries(%s)", [self.import_id])
        if not queries:
            return
        if out_type == "source":
            out_path = self._config.output_source_path
        elif out_type == "output":
            out_path = self._config.output_output_path
        for query in queries:
            file_name = os.path.join(out_path, query['file_name'])
            with open(file_name, "w") as file:
                # Write the string to the file
                file.write(query['query'])
        print(f"Generated {len(queries)} {out_type} queries")
