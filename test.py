import re
import os

# Function to convert INSERT INTO to SELECT
def convert_insert_to_select(sql_content):
    # Regular expression to find the INSERT statement
    match = re.search(r'INSERT INTO\s+(\w+)\s+\((.*?)\)\s+VALUES\s+\((.*?)\);', sql_content, re.IGNORECASE)
    if match:
        # Extract table name and column values
        table_name, columns, values = match.groups()
        # Create the SELECT statement
        select_statement = f'SELECT {values} FROM {table_name} WHERE 1=0;'
        return select_statement
    else:
        raise ValueError("No INSERT statement found.")

# Function to read the SQL file, convert the INSERT to SELECT, and save it as a new file
def process_sql_file(file_path):
    # Read the SQL file
    with open(file_path, 'r') as file:
        sql_content = file.read()

    # Convert the INSERT statement to a SELECT statement
    select_statement = convert_insert_to_select(sql_content)

    # Create new file path with "test" appended before the file extension
    new_file_path = f"{os.path.splitext(file_path)[0]}test{os.path.splitext(file_path)[1]}"

    # Write the converted SELECT statement to the new file
    with open(new_file_path, 'w') as file:
        file.write(select_statement)

    print(f"The new file {new_file_path} has been created with the SELECT statement.")

# Example usage
if __name__ == "__main__":
    # Replace 'path_to_your_sql_file.sql' with the actual path to your SQL file
    sql_file_path = 'path_to_your_sql_file.sql'
    # Process the SQL file
    process_sql_file(sql_file_path)


from datetime import datetime

test_results = {
    "happyPath": {"Data Validation": "Not Required","Step Function Validation": "Succeeded", "DSet Validation": "Succeeded", "Notification Validation": "Succeeded", "DTF Validation": "Succeeded"},
    "mismatch": {"Data Validation": "Succeeded", "DSet Validation": "Failed", "Notification Validation": "Not Run", "DTF Validation": "Error"}, 
    "scanned": {"Data Validation": "Succeeded", "DSet Validation": "Succeeded", "Notification Validation": "Failed", "DTF Validation": "Not Required"},
    "missingfile": {"Data Validation": "Failed", "DSet Validation": "Not Run", "Notification Validation": "Not Run", "DTF Validation": "Not Run"},
    "missingcountfile": {"Data Validation": "Succeeded", "DSet Validation": "Failed", "Notification Validation": "Succeeded", "DTF Validation": "Not Run"},
    "multievent": {"Data Validation": "Succeeded", "DSet Validation": "Succeeded", "Email Notification Validation": "Succeeded", "DTF Validation": "Failed"}
}

status_classes = {
    "Succeeded": "succeeded",
    "Failed": "failed",
    "Not Run": "not-run",
    "Error": "error",
    "Not Required": "not-required" 
}

from datetime import datetime

# Assuming test_results and status_classes remain unchanged.

html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Test Automation Report</title>
    <style>
        :root {{
            --success-color: #4CAF50;
            --fail-color: #F44336;
            --not-run-color: #FF9800;
            --error-color: #F44336;
            --not-required-color: #607D8B; 
            --background-color: #f2f2f2;
        }}
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 20px;
        }}
        h1 {{
            text-align: center; 
        }}
        .report-container {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
            gap: 20px;
        }}
        .report-item {{
            border: 1px solid #ddd;
            padding: 20px;
            border-radius: 5px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            overflow: auto; 
        }}
        .report-item h2 {{
            font-size: 20px;
            margin-top: 0;
        }}
        .status {{
            padding: 5px 10px;
            color: white;
            border-radius: 4px;
            display: inline-block;
            margin-top: 5px;
            margin-right: 10px; 
            white-space: nowrap; 
        }}
        .succeeded {{ background-color: var(--success-color); }}
        .failed {{ background-color: var(--fail-color); }}
        .not-run {{ background-color: var(--not-run-color); }}
        .error {{ background-color: var(--error-color); }}
        .not-required {{ background-color: var(--not-required-color); }} 
        .metadata {{
            margin-top: 20px;
            background: var(--background-color);
            padding: 10px;
            border-radius: 4px;
        }}
        .explanation {{
            margin-top: 20px;
            font-style: italic;
        }}
    </style>
</head>
<body>
    <h1>{test_name}</h1>
    <div class="metadata">
        <p>Environment: {environment}</p>
        <p>Test Execution Timestamp: {timestamp}</p>
    </div>
    <div class="report-container">
        {rows}
    </div>
    {multievent_explanation}
</body>
</html>
"""


def generate_html_report(test_results):
    rows = ""
    multievent_explanation = ""
    for event, results in test_results.items():
        row = f'<div class="report-item">\n<h2>{event}</h2>\n'
        for step, status in results.items():
            css_class = status_classes.get(status, "not-run")
            row += f'<div class="status {css_class}">{step}: {status}</div>\n'
        row += "</div>\n"
        rows += row
        if event == "multievent":
            multievent_explanation = "<div class='explanation'>The 'multievent' encompasses various scenarios where multiple conditions are tested simultaneously.</div>"

    current_timestamp = datetime.now().strftime('%Y%m%d')
    return html_template.format(test_name = 'DR Test Automation Report',timestamp=current_timestamp, rows=rows, environment = 'test env', multievent_explanation=multievent_explanation)

# Generate HTML content
html_content = generate_html_report(test_results)

# Save this HTML content to a file
with open("test_report.html", "w") as report_file:
    report_file.write(html_content)

print("HTML report generated successfully.")






import logging
import traceback
import time
from logging.handlers import RotatingFileHandler

# Configure logging
def configure_logging():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        handlers=[RotatingFileHandler('automation_tests.log', maxBytes=5*1024*1024, backupCount=2),
                                  logging.StreamHandler()])
    return logging.getLogger('AutomationTestLogger')

logger = configure_logging()

# Enhanced function to log errors with traceback
def log_errors(error):
    tb = traceback.TracebackException.from_exception(error, capture_locals=True)
    logger.error("".join(tb.format()))

# Function to log test execution details
def log_test_execution(test_name, execution_function, *args, **kwargs):
    logger.info(f"Starting test: {test_name}")
    start_time = time.time()
    try:
        execution_function(*args, **kwargs)
        end_time = time.time()
        logger.info(f"Test '{test_name}' passed. Execution time: {end_time - start_time:.2f} seconds")
    except Exception as e:
        end_time = time.time()
        logger.error(f"Test '{test_name}' failed. Execution time: {end_time - start_time:.2f} seconds")
        log_errors(e)

# Example usage of the logging functions
def example_test_function(parameter):
    # Simulate test logic, which may raise an exception
    if parameter < 0:
        raise ValueError("Parameter must be non-negative!")
    else:
        # Simulate test operations
        logger.info("Example test function executed successfully.")

# Example test execution logging
log_test_execution("Example Test Function", example_test_function, -1)  # This will log an error
log_test_execution("Example Test Function", example_test_function, 1)   # This will log success


import logging
from logging.handlers import RotatingFileHandler

# Basic configuration for logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[RotatingFileHandler('application.log', maxBytes=5000000, backupCount=5),
                              logging.StreamHandler()])

logger = logging.getLogger(__name__)




import logging
import traceback
import time
from logging.handlers import RotatingFileHandler

def configure_logging(log_filename='automation_tests.log', max_bytes=5*1024*1024, backup_count=2):
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

def log_errors(error):
    """
    Logs detailed errors, including traceback.

    :param error: Exception object to log.
    """
    tb = traceback.TracebackException.from_exception(error)
    logger.error("Exception occurred: " + "".join(tb.format()))

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
        logger.info(f"Test '{test_name}' passed. Execution time: {end_time - start_time:.2f} seconds")
    except Exception as e:
        end_time = time.time()
        logger.error(f"Test '{test_name}' failed. Execution time: {end_time - start_time:.2f} seconds")
        log_errors(e)

# Example test function
def example_test_function(parameter):
    """
    Example test function that simulates test logic.

    :param parameter: A test parameter that must be non-negative.
    """
    if parameter < 0:
        raise ValueError("Parameter must be non-negative!")
    logger.info("Example test function executed successfully.")

# Running example tests
log_test_execution("Negative Parameter Test", example_test_function, -1)  # Expected to log an error
log_test_execution("Positive Parameter Test", example_test_function, 1)   # Expected to log success
