import os
import re
from datetime import datetime
import logging
from glob import glob

logger = logging.getLogger(__name__)

def get_pattern(extension):
    """
    Returns a compiled regular expression pattern for the given file extension.

    :param extension: File extension to match, such as 'zip'.
    :return: Compiled regular expression pattern.
    """
    return re.compile(fr'(\d{8})-\d{4}_par0[1-3]\.{extension}')

def get_latest_files(file_dir, file_type, file_count=3):
    """
    Returns the latest files matching the required file pattern.

    :param file_dir: Directory containing the files.
    :param file_type: File type such as zip, text, etc.
    :param file_count: Number of latest files to return. Default is 3.
    :return: List of the latest files matching the pattern.
    """
    # Use glob to get all files of the specified type in the directory
    file_list = glob(os.path.join(file_dir, f'*.{file_type}'))
    logger.info(f'Test data found: {file_list} in {file_dir}')

    pattern = get_pattern(file_type)
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
        if len(latest_files) >= file_count:
            break

    logger.info(f'Latest file data found: {latest_files}')
    
    if len(latest_files) != file_count:
        return [] 
    else: 
        latest_data = [os.path.join(file_dir, file) for file in latest_files]
        logger.info(f'Path to latest {file_type} files: {latest_data}')
        return latest_data

# Example usage
dr_file_dir = 'path/to/your/directory'
latest_files = get_latest_files(dr_file_dir, 'zip')
print(latest_files)




def unzip_file(input_zip_file: str, output_dir: str) -> bool:
    """Unzips the specified zip file to the given output directory, overwriting existing files."""
    try:
        with zipfile.ZipFile(input_zip_file, 'r') as zip_ref:
            for member in zip_ref.namelist():
                target_path = os.path.join(output_dir, member)
                # Create target directory if it doesn't exist
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                # Extract file (overwrites if it exists)
                with zip_ref.open(member) as source, open(target_path, "wb") as target:
                    target.write(source.read())
        logger.info(f"Extracted {input_zip_file} to {output_dir}")
        return True
    except (zipfile.BadZipFile, zipfile.LargeZipFile, OSError) as e:
        log_errors(e, detailed_traceback=True)
        return False
