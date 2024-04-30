from dataforge.util import stop_spark_and_exit
from .importProject import ImportProject
from .mainConfig import MainConfig


def main():
    conf = MainConfig()
    if conf.import_flag:
        imp = ImportProject(conf)
        imp.load()
    if conf.run_path:
        conf.databricks.run(conf.run_path)
    stop_spark_and_exit()



if __name__ == '__main__':
    main()
