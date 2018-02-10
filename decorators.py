from datetime import datetime
import threading


def run_time_decorator(main_function):
    """
    this decorator will calculate the run time of the function
    """
    def wrapper(*args):
        start_time = datetime.now()
        response = main_function(*args)
        end_time = datetime.now()
        print "{} {}: '{}' function took {} secs to execute".format(str(datetime.now()).split('.')[0], threading.currentThread().getName(), main_function.__name__, str((end_time-start_time).total_seconds()))
        return response
    return wrapper

