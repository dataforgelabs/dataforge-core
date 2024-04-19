from .importProject import ImportProject
from .mainConfig import MainConfig


def main():
    conf = MainConfig()
    imp = ImportProject(conf)
    imp.load()


if __name__ == '__main__':
    main()
