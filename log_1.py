import logging
import traceback
import time
from logging.handlers import RotatingFileHandler
import inspect

def configure_logging(log_filename='automation_tests.log', max_bytes=5 * 1024 * 1024, backup_count=2):
    """
    Configures the logging system, setting up file rotation and stream handlers.

    :param log_filename: Name of the log file.
    :param max_bytes: Maximum size in bytes before rotating the log file.
    :param backup_count: Number of backup files to keep.
    """
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        handlers=[RotatingFileHandler(log_filename, maxBytes=max_bytes, backupCount=backup_count),
                                  logging.StreamHandler()])
    return logging.getLogger('AutomationTestLogger')

logger = configure_logging()

def log_errors(error, message='An unexpected error occurred', extra_data=None, detailed_traceback=False):
    """
    Logs detailed error information with customizable detail level and extra data.

    Args:
        error (Exception): The exception to log.
        message (str, optional): Custom message for the log. Defaults to 'An error occurred'.
        extra_data (dict, optional): Additional data for context. Defaults to None.
        detailed_traceback (bool, optional): Whether to log detailed traceback. Defaults to False.
    """
    caller_frame = inspect.currentframe().f_back
    function_name = caller_frame.f_code.co_name

    if detailed_traceback:
        # Get detailed traceback as a string
        tb_str = traceback.format_exception(etype=type(error), value=error, tb=error.__traceback__)
        tb_str = "".join(tb_str)
        log_message = f"{message} in {function_name}. Error: {error}. Traceback: {tb_str}"
    else:
        # Simple error message with type and message
        log_message = f"{message} in {function_name}. Error: {str(error)}"

    if extra_data:
        # Incorporating extra data into the log using the 'extra' parameter of logger
        logger.error(log_message, extra={'extra_data': extra_data})
    else:
        logger.error(log_message)

def log_test_execution(test_name, execution_function, *args, **kwargs):
    """
    Wraps test execution function calls with logging for start, success, and failure.

    :param test_name: Name of the test for logging.
    :param execution_function: Test function to execute.
    :param args: Positional arguments for the test function.
    :param kwargs: Keyword arguments for the test function.
    """
    logger.info(f"Starting test: {test_name}")
    start_time = time.time()
    try:
        execution_function(*args, **kwargs)
        end_time = time.time()
        logger.info(f"Test '{test_name}' passed in {inspect.currentframe().f_code.co_name}. Execution time: {end_time - start_time:.2f} seconds")
    except Exception as e:
        end_time = time.time()
        logger.error(f"Test '{test_name}' failed in {inspect.currentframe().f_code.co_name}. Execution time: {end_time - start_time:.2f} seconds")
        log_errors(e)

# Usage example
def example_test_function(input_value):
    if input_value < 0:
        raise ValueError("Input value must be non-negative")
    return input_value * 2

# Example of running a test with logging
test_input = 5
log_test_execution("example_test_function", example_test_function, test_input)
