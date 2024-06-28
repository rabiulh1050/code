import glob
import os
import re
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def get_latest_files(dr_file_dir):
    """
    Returns the latest files matching the required file pattern.

    :param dr_file_dir: Directory containing the files.
    :return: List of the latest files matching the pattern.
    """
    # Use glob to get all zip files in the directory
    file_list = glob.glob(os.path.join(dr_file_dir, '*.zip'))
    logger.info(f'Test data found: {file_list} in {dr_file_dir}')
    
    pattern = re.compile(r'(\d{8})-\d{4}_par0[1-3]\.zip')
    files_by_date = {}

    for file_path in file_list:
        filename = os.path.basename(file_path)
        match = pattern.match(filename)
        if match:
            date_str = match.group(1)
            date = datetime.strptime(date_str, '%Y%m%d')
            if date not in files_by_date:
                files_by_date[date] = []
            files_by_date[date].append(filename)

    sorted_dates = sorted(files_by_date.keys(), reverse=True)
    latest_files = []

    for date in sorted_dates:
        parts = sorted(files_by_date[date])
        latest_files.extend(parts)
        if len(latest_files) >= 3:
            break

    return latest_files[:3]

# Example usage
dr_file_dir = 'path/to/your/directory'
latest_files = get_latest_files(dr_file_dir)
print(latest_files)
