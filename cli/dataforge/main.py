from .importProject import ImportProject
from .mainConfig import MainConfig


def main():
    conf = MainConfig()
    if conf.import_flag:
        imp = ImportProject(conf)
        imp.load()
    if conf.run_flag:
        conf.databricks.run(conf.output_path)


if __name__ == '__main__':
    main()
