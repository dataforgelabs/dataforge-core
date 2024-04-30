import os
import signal
import sys
import psutil


def confirm_action(message: str):
    confirmation = input(message).strip().lower()
    return confirmation in ('yes', 'y')


def get_input(message: str, current_value: str = None, required=True):
    while True:
        value = input(message + (f"[{current_value}]" if current_value else ""))
        if value != '' or not required:
            return value
        if value == '' and current_value is not None:
            return current_value


def save_os_variable(name: str, value: str):
    try:
        match sys.platform:
            case 'win32' | 'cygwin':
                os.system(f"SETX {name} \"{value}\"")
                os.system(f"set {name}=\"{value}\"")
                print(f"OS variable {name} updated.")
            case _:
                os.system(f"export {name}=\"{value}\"")  # TODO: may not work properly on linux/Mac
    except Exception as e:
        print(f"Error updating OS environment variable {name}. Details: {e}")
        sys.exit(1)


def check_var(name: str, error_text):
    value = os.environ.get(name)
    if value is None:
        print(f"Environment variable {name} is not initialized. {error_text}")
        sys.exit(1)
    return value


def validate_value(config, value):
    if config.get(value) is None:
        print(f"{value} is required")
        sys.exit(1)

def stop_spark_and_exit():
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
