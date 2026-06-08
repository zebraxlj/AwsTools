import os


def is_running_in_pycharm():
    return 'PYCHARM_HOSTED' in os.environ
