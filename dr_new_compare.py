import os
import zipfile
import logging
import concurrent.futures
from glob import glob
from datetime import datetime
import boto3
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from logs import log_errors, configure_logging

logger = configure_logging()

def unzip_file(input_zip_file: str, output_dir: str) -> bool:
    """
    Unzips the specified zip file to the given output directory.
    :param input_zip_file: Path to the zip file.
    :param output_dir: Directory to extract the contents of the zip file.
    :return: True if successful, False otherwise.
    """
    try:
        with zipfile.ZipFile(input_zip_file, 'r') as zip_ref:
            zip_ref.extractall(output_dir)
        logger.info(f"Extracted {input_zip_file} to {output_dir}")
        return True
    except Exception as e:
        log_errors(e, detailed_traceback=True)
        return False

def read_parquet_file_metadata(parquet_file: str) -> dict:
    """
    Reads metadata from the specified parquet file.
    :param parquet_file: Path to the parquet file.
    :return: Dictionary containing column names and counts.
    """
    try:
        parquet_file_name = os.path.basename(parquet_file).split('.parquet')[0]
        logger.info(f"Reading Parquet File: {parquet_file_name}")
        pq_schema = pq.read_schema(parquet_file)
        columns = pq_schema.names
        metadata = {
            'File_Columns_Names': columns,
            'File_Columns_Counts': len(columns)
        } if columns else {
            'File_Columns_Names': [],
            'File_Columns_Counts': 0
        }
        logger.debug(f"{parquet_file_name}: {columns}")
        return parquet_file_name, metadata
    except Exception as e:
        log_errors(e, detailed_traceback=True)
        return os.path.basename(parquet_file).split('.parquet')[0], {
            'File_Columns_Names': [],
            'File_Columns_Counts': 0
        }

def process_zip_file(zip_file: str, output_dir: str) -> dict:
    """
    Processes a single zip file: unzips it and reads metadata from contained parquet files.
    :param zip_file: Path to the zip file.
    :param output_dir: Directory to extract and process the files.
    :return: Metadata dictionary for parquet files.
    """
    file_metadata_dict = {}
    if unzip_file(zip_file, output_dir):
        parquet_files = glob(f"{output_dir}/*.parquet")
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_parquet = {
                executor.submit(read_parquet_file_metadata, file): file
                for file in parquet_files
            }
            for future in concurrent.futures.as_completed(future_to_parquet):
                parquet_file_name, metadata = future.result()
                file_metadata_dict[parquet_file_name] = metadata
    return file_metadata_dict

def get_dr_file_header(input_zip_dir: str, output_dir: str) -> dict:
    """
    Retrieves metadata for parquet files within zip files in the specified directory.
    :param input_zip_dir: Directory containing the zip files.
    :param output_dir: Directory to extract the files.
    :return: Metadata dictionary for all parquet files.
    """
    try:
        zip_files = glob(f"{input_zip_dir}/*.zip")
        logger.info(f"Found {len(zip_files)} zip files in {input_zip_dir}")

        file_metadata_dict = {}
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_zip = {
                executor.submit(process_zip_file, zip_file, output_dir): zip_file
                for zip_file in zip_files
            }
            for future in concurrent.futures.as_completed(future_to_zip):
                try:
                    metadata = future.result()
                    file_metadata_dict.update(metadata)
                except Exception as e:
                    log_errors(e, detailed_traceback=True)
        
        return file_metadata_dict
    except Exception as e:
        log_errors(e, detailed_traceback=True)
        return {}

def get_glue_tbls_metadata(tables_list: list) -> dict:
    """
    Retrieves metadata for a list of Glue tables.
    :param tables_list: List of tables in the format "database_name,table_name".
    :return: Dictionary containing metadata for each table.
    """
    table_metadata_dict = {}
    client = boto3.client('glue')

    def fetch_table_metadata(table: str):
        db_name, table_name = map(str.lower, table.split(','))
        logger.info(f"Fetching metadata for Glue Table: {db_name}.{table_name}")
        try:
            response = client.get_table(DatabaseName=db_name, Name=table_name)
            if response:
                columns = [col['Name'] for col in response['Table']['StorageDescriptor']['Columns']]
                metadata = {
                    'Table_Columns_Names': columns,
                    'Table_Columns_Counts': len(columns)
                }
                logger.info(f"Metadata for {table_name} fetched successfully")
            else:
                metadata = {'Table_Columns_Names': [], 'Table_Columns_Counts': 0}
                logger.warning(f"No metadata found for {table_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                logger.error(f"Table does not exist: {db_name}.{table_name}")
            metadata = {'Table_Columns_Names': [], 'Table_Columns_Counts': 0}
        except Exception as e:
            log_errors(e, detailed_traceback=True)
            metadata = {'Table_Columns_Names': [], 'Table_Columns_Counts': 0}
        return table_name, metadata

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_table = {executor.submit(fetch_table_metadata, table): table for table in tables_list}
        for future in concurrent.futures.as_completed(future_to_table):
            table_name, metadata = future.result()
            table_metadata_dict[table_name] = metadata

    return table_metadata_dict

def compare_glue_tbl_structure(table_list: list, file_md_dict: dict, tbl_md_dict: dict) -> str:
    """
    Compares the structure of Glue tables with the metadata of corresponding parquet files.
    :param table_list: List of tables in the format "database_name,table_name".
    :param file_md_dict: Metadata dictionary of parquet files.
    :param tbl_md_dict: Metadata dictionary of Glue tables.
    :return: Validation result as 'SUCCEEDED' or 'FAILED'.
    """
    comparision_result = {}
    current_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    summary_report_path = f'./logs/DR_COMP_Summary_Report_{current_timestamp}.txt'
    details_report_path = f'./logs/DR_COMP_Details_Report_{current_timestamp}.txt'
    try:
        with open(summary_report_path, 'w') as sum_fl, open(details_report_path, 'w') as dtl_fl:
            for table in table_list:
                db_name, table_name = map(str.lower, table.split(','))
                logger.info(f"Comparing structure for {db_name}.{table_name}")
                dtl_fl.write(f"\n{'-' * 25}| {db_name} - {table_name} |{'-' * 25}\n")
                dtl_fl.write(f"\tTable Columns Names - {tbl_md_dict[table_name]['Table_Columns_Names']}\n")
                dtl_fl.write(f"\tTable Columns Counts - {len(tbl_md_dict[table_name]['Table_Columns_Names'])}\n")
                dtl_fl.write(f"\tFile Columns Names - {file_md_dict[table_name]['File_Columns_Names']}\n")
                dtl_fl.write(f"\tFile Columns Counts - {len(file_md_dict[table_name]['File_Columns_Names'])}\n")
                if file_md_dict[table_name]['File_Columns_Names'] == tbl_md_dict[table_name]['Table_Columns_Names']:
                    logger.info(f'Glue Table Structure validation for {table_name} - Passed')
                    comparision_result[table_name] = 'SUCCEEDED'
                else:
                    logger.info(f'Glue Table Structure validation for {table_name} - Failed')
                    comparision_result[table_name] = 'FAILED'
                dtl_fl.write(f"{'-' * 75}")
            failed_tables = [key for key, val in comparision_result.items() if val == 'FAILED']
            passed_tables = [key for key, val in comparision_result.items() if val == 'SUCCEEDED']
            validation = "FAILED" if failed_tables else "SUCCEEDED"
            if failed_tables:
                sum_fl.write(f"{'-' * 25}| Failed |{'-' * 25}\n")
                for idx, item in enumerate(failed_tables, start=1):
                    sum_fl.write(f"{idx}. {item} - FAILED\n")
                    sum_fl.write(f"\tDifference - {set(file_md_dict[item]['File_Columns_Names']) - set(tbl_md_dict[item]['Table_Columns_Names'])}\n")
            if passed_tables:
                sum_fl.write(f"{'-' * 25}| Passed |{'-' * 25}\n")
                for idx, item in enumerate(passed_tables, start=1):
                    sum_fl.write(f"{idx}. {item} - PASSED\n")

        return validation
    except Exception as e:
        log_errors(e, detailed_traceback=True)
        return "FAILED"

def dr_glue_main_process(dr_zip_file_dir: str, dr_input_file_dir: str) -> dict:
    """
    Main process to compare Glue table structures with DR Parquet file headers.
    :param dr_zip_file_dir: Directory containing zip files with parquet files.
    :param dr_input_file_dir: Directory to extract parquet files.
    :return: Result of Glue table validation.
    """
    try:
        with open('./config/dr_table_config.txt', 'r') as tbl_cfg_fl:
            table_list = tbl_cfg_fl.read().splitlines()
            if not table_list:
                logger.warning("Config Tables list is empty.")
                return {"Glue Table Validation": "FAILED"}

            logger.info("Fetching DR Parquet Files Headers.")
            file_md_dict = get_dr_file_header(dr_zip_file_dir, dr_input_file_dir)
            logger.info("Fetching Glue Tables Metadata.")
            tbl_md_dict = get_glue_tbls_metadata(table_list)
            logger.info("Comparing the Glue Tables Structure vs Parquet File Header.")
            val_res = compare_glue_tbl_structure(table_list, file_md_dict, tbl_md_dict)

            return {"Glue Table Validation": val_res}
    except Exception as e:
        log_errors(e, detailed_traceback=True)
        return {"Glue Table Validation": "FAILED"}

# Example usage:
# result = dr_glue_main_process('/path/to/zip/files', '/path/to/extracted/files')
# print(result)


















import os
import zipfile
import logging
import traceback
from glob import glob
from datetime import datetime
import boto3
import pyarrow.parquet as pq
import concurrent.futures
from botocore.exceptions import ClientError
from aws_utils_func import log_errors, configure_logging

# logger configuration
logger = configure_logging()

def unzip_file(input_zip_file: str, output_dir: str) -> bool:
    """Unzips the specified zip file to the given output directory."""
    try:
        with zipfile.ZipFile(input_zip_file, 'r') as zip_ref:
            zip_ref.extractall(output_dir)
        logger.info(f"Extracted {input_zip_file} to {output_dir}")
        return True
    except Exception as e:
        tb = traceback.TracebackException.from_exception(e, capture_locals=True)
        logger.error("".join(tb.format()))
        return False

def read_parquet_file_metadata(parquet_file: str) -> dict:
    """Reads metadata from the specified parquet file."""
    try:
        parquet_file_name = os.path.basename(parquet_file).split('.parquet')[0]
        pq_df = pq.read_schema(parquet_file)
        pq_cols_name_ls = pq_df.names
        metadata = {
            'File_Columns_Names': pq_cols_name_ls,
            'File_Columns_Counts': len(pq_cols_name_ls)
        } if pq_cols_name_ls else {
            'File_Columns_Names': [],
            'File_Columns_Counts': 0
        }
        logger.debug(f"{parquet_file_name}: {pq_cols_name_ls}")
        return parquet_file_name, metadata
    except Exception as e:
        log_errors(e, detailed_traceback=True)
        return os.path.basename(parquet_file).split('.parquet')[0], {
            'File_Columns_Names': [],
            'File_Columns_Counts': 0
        }

def process_zip_file(zip_file: str, output_dr_dir: str) -> dict:
    """Processes a single zip file: unzips it and reads metadata from contained parquet files."""
    file_metadata_dict = {}
    if unzip_file(zip_file, output_dr_dir):
        parquet_files = glob(f"{output_dr_dir}/*.parquet")
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_parquet = {
                executor.submit(read_parquet_file_metadata, file): file
                for file in parquet_files
            }
            for future in concurrent.futures.as_completed(future_to_parquet):
                file = future_to_parquet[future]
                parquet_file_name, metadata = future.result()
                file_metadata_dict[parquet_file_name] = metadata
    return file_metadata_dict

def get_dr_file_header(input_dr_zip_dir: str, output_dr_dir: str) -> dict:
    """Retrieves metadata for parquet files within zip files in the specified directory."""
    try:
        zip_files = glob(f"{input_dr_zip_dir}/*.zip")
        logger.info(f"Found {len(zip_files)} zip files in {input_dr_zip_dir}")

        file_metadata_dict = {}
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_zip = {
                executor.submit(process_zip_file, zip_file, output_dr_dir): zip_file
                for zip_file in zip_files
            }
            for future in concurrent.futures.as_completed(future_to_zip):
                zip_file = future_to_zip[future]
                try:
                    metadata = future.result()
                    file_metadata_dict.update(metadata)
                except Exception as e:
                    logger.error(f"Error processing zip file: {zip_file}")
                    log_errors(e, detailed_traceback=True)
        
        return file_metadata_dict
    except Exception as e:
        log_errors(e, detailed_traceback=True)
        return {}

def get_glue_table_metadata(client, db_name: str, table_name: str) -> dict:
    """Retrieves metadata for a specific Glue table."""
    table_metadata = {}
    try:
        response = client.get_table(DatabaseName=db_name, Name=table_name)
        if response:
            tbl_name = response['Table']['Name']
            if table_name == tbl_name:
                tbl_cols_names = [col['Name'] for col in response['Table']['StorageDescriptor']['Columns']]
                table_metadata['Table_Columns_Names'] = tbl_cols_names
                table_metadata['Table_Columns_Counts'] = len(tbl_cols_names)
            else:
                logger.warning(f"Retrieved incorrect metadata for table: {table_name}")
                table_metadata['Table_Columns_Names'] = []
                table_metadata['Table_Columns_Counts'] = 0
        else:
            logger.warning(f"No response received for Glue table: {db_name}.{table_name}")
            table_metadata['Table_Columns_Names'] = []
            table_metadata['Table_Columns_Counts'] = 0
    except ClientError as e:
        log_errors(e, detailed_traceback=True)
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            logger.error(f"Table does not exist: {db_name}.{table_name}")
        table_metadata['Table_Columns_Names'] = []
        table_metadata['Table_Columns_Counts'] = 0
    except Exception as e:
        log_errors(e, detailed_traceback=True)
        table_metadata['Table_Columns_Names'] = []
        table_metadata['Table_Columns_Counts'] = 0
    return table_metadata

def get_glue_tbls_metadata(tables_list: list) -> dict:
    """Retrieves metadata for a list of Glue tables."""
    client = boto3.client('glue')
    table_metadata_dict = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_table = {
            executor.submit(get_glue_table_metadata, client, table.split(',')[0].lower(), table.split(',')[1].lower()): table
            for table in tables_list
        }
        for future in concurrent.futures.as_completed(future_to_table):
            table = future_to_table[future]
            table_name = table.split(',')[1].lower()
            try:
                table_metadata = future.result()
                table_metadata_dict[table_name] = table_metadata
            except Exception as e:
                logger.error(f"Error retrieving metadata for table: {table}")
                log_errors(e, detailed_traceback=True)
                table_metadata_dict[table_name] = {'Table_Columns_Names': [], 'Table_Columns_Counts': 0}
    return table_metadata_dict

def compare_glue_tbl_structure(table_list: list, file_md_dict: dict, tbl_md_dict: dict) -> str:
    """Compares Glue table structures with parquet file headers."""
    comparison_result = {}
    try:
        current_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        summary_file_path = f'./logs/DR_COMP_Summary_Report_{current_timestamp}.txt'
        details_file_path = f'./logs/DR_COMP_Details_Report_{current_timestamp}.txt'
        
        with open(summary_file_path, 'w+') as sum_fl, open(details_file_path, 'w+') as dtl_fl:
            for table in table_list:
                db_name, table_name = table.split(',')[0].lower(), table.split(',')[1].lower()
                logger.info(f"Comparing {db_name} - {table_name}")
                
                dtl_fl.write(f"\n{'-' * 25}| {db_name} - {table_name} |{'-' * 25}\n")
                dtl_fl.write(f"\tTable Columns Names - {tbl_md_dict[table_name]['Table_Columns_Names']}\n")
                dtl_fl.write(f"\tTable Columns Counts - {len(tbl_md_dict[table_name]['Table_Columns_Names'])}\n")
                dtl_fl.write(f"\tFile Columns Names - {file_md_dict[table_name]['File_Columns_Names']}\n")
                dtl_fl.write(f"\tFile Columns Counts - {len(file_md_dict[table_name]['File_Columns_Names'])}\n")
                
                if file_md_dict[table_name]['File_Columns_Names'] == tbl_md_dict[table_name]['Table_Columns_Names']:
                    logger.info(f'Glue Table Structure validation for {table_name} - Passed')
                    comparison_result[table_name] = 'SUCCEEDED'
                else:
                    logger.info(f'Glue Table Structure validation for {table_name} - Failed')
                    comparison_result[table_name] = 'FAILED'
                
                dtl_fl.write(f"{'-' * 75}")
            
            failed_tables = [key for key, val in comparison_result.items() if 'FAILED' in val]
            passed_tables = [key for key, val in comparison_result.items() if 'SUCCEEDED' in val]
            validation = "FAILED" if failed_tables else "SUCCEEDED"
            
            if failed_tables:
                sum_fl.write(f"{'-' * 25}| Failed |{'-' * 25}\n")
                for idx, item in enumerate(failed_tables, start=1):
                    sum_fl.write(f"{idx}. {item} - FAILED\n")
                    sum_fl.write(f"\tDifference - {set(file_md_dict[item]['File_Columns_Names']) - set(tbl_md_dict[item]['Table_Columns_Names'])}\n")
            
            if passed_tables:
                sum_fl.write(f"{'-' * 25}| Passed |{'-' * 25}\n")
                for idx, item in enumerate(passed_tables, start=1):
                    sum_fl.write(f"{idx}. {item} - PASSED\n")
        
        return validation
    except Exception as e:
        tb = traceback.TracebackException.from_exception(e, capture_locals=True)
        logger.error("".join(tb.format()))
        return "FAILED"

def dr_glue_main_process(dr_zip_file_dir: str, dr_input_file_dir: str) -> dict:
    """Main process to fetch and compare Glue table structures and DR file headers."""
    try:
        with open(f'./config/dr_table_config.txt', 'r') as tbl_cfg_fl:
            table_list = tbl_cfg_fl.read().splitlines()
            if table_list:
                logger.info(f"Fetching DR Parquet Files Headers.")
                file_md_dict = get_dr_file_header(dr_zip_file_dir, dr_input_file_dir)
                logger.info(f"Fetching Glue Tables Metadata.")
                tbl_md_dict = get_glue_tbls_metadata(table_list)
                logger.info(f"Comparing the Glue Tables Structure vs Parquet File Header.")
                val_res = compare_glue_tbl_structure(table_list, file_md_dict, tbl_md_dict)
            else:
                logger.warning(f"Config Tables list is empty.")
                return {"Glue Table Validation": "FAILED"}
        return {"Glue Table Validation": val_res}
    except Exception as e:
        tb = traceback.TracebackException.from_exception(e, capture_locals=True)
        logger.error("".join(tb.format()))
        return {"Glue Table Validation": "FAILED"}

# Example usage:
# result = dr_glue_main_process('/path/to/dr_zip_file_dir', '/path/to/dr_input_file_dir')
# print(result)
