def read_file_content(file_path):
    """
    Reads the content of a file and returns it as a string.
    
    Parameters:
    - file_path: The path to the file.
    
    Returns:
    - A string containing the content of the file.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    except FileNotFoundError:
        return "The file was not found."
    except Exception as e:
        return f"An error occurred: {e}"

# Example usage:
# file_content = read_file_content("path/to/your/file.txt")
# print(file_content)


import logging
import traceback

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

def log_error(error, message='An error occurred', extra_data=None):
    """
    Logs detailed error information including a custom message, the traceback, and any additional data provided.

    Args:
        error (Exception): The exception to log.
        message (str, optional): A custom message to include with the error log. Defaults to 'An error occurred'.
        extra_data (dict, optional): Additional data to log with the error for context. Defaults to None.
    """
    # Capturing the traceback
    tb_str = traceback.format_exception(etype=type(error), value=error, tb=error.__traceback__)
    tb_str = "".join(tb_str)
    
    # Preparing the log message
    log_message = f"{message}. Error: {error}. Traceback: {tb_str}"
    
    # If there's extra data, include it in the log message
    if extra_data:
        extra_data_str = ", ".join(f"{key}: {value}" for key, value in extra_data.items())
        log_message += f" Additional data: {extra_data_str}"

    logger.error(log_message)

# Example usage of the enhanced log_error function
try:
    # Simulate an error
    raise ValueError("Example error for demonstration.")
except Exception as e:
    log_error(e, "Unexpected error occurred", {"user": "exampleUser", "action": "exampleAction"})



import logging
import traceback
import sys

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

def log_error_func(error):
    """
    Logs an error with detailed information including the exception type, function name, file name,
    line number, and the error message.
    """
    exc_type, exc_obj, exc_tb = sys.exc_info()
    
    # Loop through the traceback to get the last (most relevant) call stack information
    while exc_tb.tb_next:
        exc_tb = exc_tb.tb_next
    file_name = exc_tb.tb_frame.f_code.co_filename
    function_name = exc_tb.tb_frame.f_code.co_name
    line_no = exc_tb.tb_lineno
    
    # Preparing and logging the error message with detailed traceback
    logger.error(f'An exception [{exc_type.__name__}] occurred in {function_name}() in {file_name} at line # {line_no} => {str(error)}\nTraceback: {" ".join(traceback.format_tb(exc_tb))}')

# Example usage of the improved log_error_func function
try:
    # Simulate an error
    1 / 0
except Exception as e:
    log_error_func(e)




import logging
import traceback
import sys

# Configure logging once for all uses
def configure_logging():
    logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

# Improved and unified log_error function
def log_error(error, message='An error occurred', extra_data=None, detailed_traceback=False):
    """
    Logs detailed error information with customizable detail level and extra data.

    Args:
        error (Exception): The exception to log.
        message (str, optional): Custom message for the log. Defaults to 'An error occurred'.
        extra_data (dict, optional): Additional data for context. Defaults to None.
        detailed_traceback (bool, optional): Whether to log detailed traceback. Defaults to False.
    """
    logger = logging.getLogger(__name__)
    
    if detailed_traceback:
        # Get detailed traceback as a string
        tb_str = traceback.format_exception(etype=type(error), value=error, tb=error.__traceback__)
        tb_str = "".join(tb_str)
        log_message = f"{message}. Error: {error}. Traceback: {tb_str}"
    else:
        # Simple error message with type and message
        log_message = f"{message}. Error: {str(error)}"

    if extra_data:
        # Incorporating extra data into the log using the 'extra' parameter of logger
        logger.error(log_message, extra={'extra_data': extra_data})
    else:
        logger.error(log_message)

# Configure logging
configure_logging()

# Commented out example usage to prevent execution in PCI
#try:
#    raise ValueError("Example error for demonstration.")
#except Exception as e:
#    log_error(e, "Unexpected error occurred", {"user": "exampleUser", "action": "exampleAction"}, detailed_traceback=True)
