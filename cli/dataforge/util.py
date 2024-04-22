import os
import sys


def confirm_action(message: str):
    confirmation = input(message).strip().lower()
    return confirmation in ('yes', 'y')


def save_os_variable(name: str, value: str):
    try:
        match sys.platform:
            case 'win32' | 'cygwin':
                os.system(f"SETX {name} \"{value}\"")
                print(f"OS variable {name} updated. Please reopen your terminal window !")
            case _:
                os.system(f"export {name}=\"{value}\"")  # TODO: may not work properly on linux/Mac
    except Exception as e:
        print(f"Error updating OS environment variable {name}. Details: {e}")
        sys.exit(1)
