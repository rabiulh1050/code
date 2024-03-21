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


WITH MD_DATA_ENUM_FILTERED AS (
    SELECT
        Data_enum_tp,
        md_data_enum_lkup_OBJ_ID,
        Data_Enum_ATTR_NM
    FROM
        MD_data_ENUM_LKUP_VW1
    WHERE
        Data_Enum_ATTR_NM IN ('ABC', 'xyz')
),
UWRG_PREP AS (
    SELECT
        LN_UWRG.FNM_LN_ID,
        1 AS LN_REVW_UWRG_SPST_OBJ_ID,
        AUTOD_UWRG_RCMN_TI.DATA_ENUM_TP AS LN_AUWRG_RCMN_TYP,
        AUTOD_UWRG_SYS_TI.DATA_ENUM_TP AS LN_AUTOD_UWRG_SYS_TYP,
        LN_UWRG.AUTOD_UWRG_CASE_ID,
        LN_UWRG.MD_SRC_SYS_LKUP_OBJ_ID
    FROM
        LN_UWRG
    LEFT JOIN MD_DATA_ENUM_FILTERED AS AUTOD_UWRG_RCMN_TI ON 
        AUTOD_UWRG_RCMN_TI.md_data_enum_lkup_OBJ_ID = LN_UWRG.AUTOD_UWRG_RCMN_TI
        AND AUTOD_UWRG_RCMN_TI.DATA_ENUM_ATTR_NM = 'ABC'
    LEFT JOIN MD_DATA_ENUM_FILTERED AS AUTOD_UWRG_SYS_TI ON 
        AUTOD_UWRG_SYS_TI.md_data_enum_lkup_OBJ_ID = LN_UWRG.AUTOD_UWRG_SYS_TI
        AND AUTOD_UWRG_SYS_TI.DATA_ENUM_ATTR_NM = 'xyz'
    WHERE 
        LN_UWRG.MD_SRC_SYS_LKUP_OBJ_ID IN (
            SELECT MD_SRC_SYS_LKUP_OBJ_ID 
            FROM MD_SRC_SYS_LKUP_VW1
            WHERE SRC_SYS_NM IN ('IDS', 'LQCS', 'QAS')
        )
)

SELECT
    LN_REVW_ST_SPST_1.LN_REVW_ST_SPST_OBJ_ID,
    LN_REVW_ST_SPST_1.FNM_LN_ID,
    LN_REVW_ST_SPST_1.LN_REVW_ID,
    NVL(UWRG.LN_AUTOD_UWRG_SYS_TYP, 'ENUMNOTAVAILABLE') AS LN_AUTOD_UWRG_SYS_TYP,
    'IDS' AS SRC_SYS_INFC_NME_TYP,
    'ASDELIVERED' AS LN_REVW_ST_TYPE,
    UWRG.AUTOD_UWRG_CASE_ID,
    UWRG.LN_AUWRG_RCMN_TYP,
    UWRG.LN_AUTOD_UWRG_SYS_TYP AS AUTOD_UWRG_RCMN_TI,
    UWRG.LN_AUTOD_UWRG_SYS_TYP AS AUTOD_UWRG_SYS_TI,
    CASE
        WHEN STG_1.SRC_SYS_OBJ_ID = 7026 THEN 'LQCS'
        WHEN STG_1.SRC_SYS_OBJ_ID = 7038 THEN 'QAS'
        ELSE 'UNKNOWN'
    END AS SRC_SYS_OBJ_ID_DESCRIPTION
FROM
    STG_1
JOIN LN_REVW_ST_SPST_1 ON
    STG_1.LN_REVW_ID = LN_REVW_ST_SPST_1.LN_REVW_ID
    AND STG_1.FNM_LN_ID = LN_REVW_ST_SPST_1.FNM_LN_ID
JOIN UWRG_PREP AS UWRG ON
    LN_REVW_ST_SPST_1.FNM_LN_ID = UWRG.FNM_LN_ID
    AND UWRG.MD_SRC_SYS_LKUP_OBJ_ID = CASE 
        WHEN LN_REVW_ST_SPST_1.SRC_SYS_INFC_NME_TYP = 'LQCS' THEN 7026
        WHEN LN_REVW_ST_SPST_1.SRC_SYS_INFC_NME_TYP = 'QAS' THEN 7038
        ELSE 9999
    END;

