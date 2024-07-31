import subprocess
import logging
import os

def execute_script(script_path: str, test_case_name: str) -> dict:
    """
    Execute the provided script with the given test case name.

    :param script_path: The path to the script to execute.
    :param test_case_name: The name of the test case file to pass to the script.
    :return: Dictionary with the execution result.
    """
    result_dict = {
        "status": "NotRun",
        "details": {}
    }
    try:
        logging.info(f"Starting to execute script: {script_path}")

        # Validate script path
        if not os.path.exists(script_path):
            logging.error(f"Script not found: {script_path}")
            return result_dict

        if not os.access(script_path, os.X_OK):
            st_mode = os.stat(script_path).st_mode
            logging.error(f"Script is not executable: {script_path}")
            logging.error(f"Script permissions: {st_mode}")
            return result_dict

        # Execute the script securely with a timeout of 5 minutes (300 seconds)
        try:
            result = subprocess.run(['bash', script_path, f'{test_case_name}.yaml'], capture_output=True, text=True, timeout=300)
        except subprocess.TimeoutExpired:
            logging.error(f"Script execution timed out after 5 minutes.")
            result_dict['status'] = "Timeout"
            result_dict['details'] = "The script execution exceeded the time limit of 5 minutes."
            return result_dict

        if result.returncode != 0:
            logging.error("Script execution failed")
            logging.error(f"Error: {result.stderr}")
            result_dict['status'] = "Failed"
            result_dict['details'] = result.stderr
            return result_dict

        logging.info("Script executed successfully!")
        logging.info(f"Output: {result.stdout}")

        # Update result dictionary on success
        result_dict['status'] = "Success"
        result_dict['details'] = result.stdout

    except subprocess.CalledProcessError as e:
        logging.error(f"Script execution failed with error: {str(e)}")
        result_dict['status'] = "Failed"
        result_dict['details'] = str(e)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
        result_dict['status'] = "Error"
        result_dict['details'] = str(e)
    
    return result_dict
