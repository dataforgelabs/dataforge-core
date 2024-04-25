import os
import signal
import psutil

from .importProject import ImportProject
from .mainConfig import MainConfig


def main():
    conf = MainConfig()
    if conf.import_flag:
        imp = ImportProject(conf)
        imp.load()
    if conf.run_path:
        conf.databricks.run(conf.run_path)

    current_process = psutil.Process()
    children = current_process.children(recursive=True)

    #  terminate child processes
    if len(children) > 0:
        for p in children:
            try:
                p.send_signal(signal.SIGTERM)
            except psutil.NoSuchProcess:
                pass
        psutil.wait_procs(children, timeout=5)
    os._exit(os.EX_OK)


if __name__ == '__main__':
    main()
