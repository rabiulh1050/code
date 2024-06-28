import os
import zipfile
import logging
import traceback
from glob import glob
from datetime import datetime
import boto3
import pyarrow.parquet as pq
import multiprocessing
from botocore.exceptions import ClientError
from aws_utils_func import log_errors, configure_logging

# logger configuration
logger = configure_logging()

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

def read_parquet_file_metadata(parquet_file: str) -> dict:
    """Reads metadata from the specified parquet file."""
    try:
        parquet_file_name = os.path.basename(parquet_file).split('.parquet')[0]
        pq_df = pq.read_schema(parquet_file)
        pq_cols_name_ls = pq_df.names
        metadata = {
            'File_Columns_Names': pq_cols_name_ls,
            'File_Columns_Counts': len(pq_cols_name_ls)
        }
        logger.debug(f"{parquet_file_name}: {pq_cols_name_ls}")
        return parquet_file_name, metadata
    except (pq.ArrowInvalid, OSError) as e:
        log_errors(e, detailed_traceback=True)
        return os.path.basename(parquet_file).split('.parquet')[0], {
            'File_Columns_Names': [],
            'File_Columns_Counts': 0
        }

def process_parquet_file(parquet_file: str) -> dict:
    """Reads metadata from a single parquet file."""
    return read_parquet_file_metadata(parquet_file)

def process_zip_file(zip_file: str, output_dr_dir: str) -> dict:
    """Processes a single zip file: unzips it and reads metadata from contained parquet files."""
    file_metadata_dict = {}
    if not unzip_file(zip_file, output_dr_dir):
        logger.error(f"Failed to extract zip file: {zip_file}")
        return None  # Indicate failure to process this zip file

    parquet_files = glob(f"{output_dr_dir}/*.parquet")
    with multiprocessing.Pool() as pool:
        results = pool.map(process_parquet_file, parquet_files)
        for parquet_file_name, metadata in results:
            if metadata['File_Columns_Counts'] == 0:
                logger.warning(f"Failed to extract column names for parquet file: {parquet_file_name}")
            file_metadata_dict[parquet_file_name] = metadata

    return file_metadata_dict

def get_dr_file_header(input_dr_zip_dir: str, output_dr_dir: str) -> dict:
    """Retrieves metadata for parquet files within zip files in the specified directory."""
    try:
        zip_files = glob(f"{input_dr_zip_dir}/*.zip")
        logger.info(f"Found {len(zip_files)} zip files in {input_dr_zip_dir}")

        file_metadata_dict = {}
        with multiprocessing.Pool() as pool:
            results = pool.starmap(process_zip_file, [(zip_file, output_dr_dir) for zip_file in zip_files])
            for result in results:
                if result is None:
                    logger.error("Stopping execution due to failed zip file extraction.")
                    return {}
                file_metadata_dict.update(result)

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

def get_glue_table_metadata_helper(args):
    client, db_name, table_name = args
    return get_glue_table_metadata(client, db_name, table_name)

def get_glue_tbls_metadata(tables_list: list) -> dict:
    """Retrieves metadata for a list of Glue tables."""
    client = boto3.client('glue')
    table_metadata_dict = {}
    with multiprocessing.Pool() as pool:
        results = pool.map(get_glue_table_metadata_helper, [(client, table.split(',')[0].lower(), table.split(',')[1].lower()) for table in tables_list])
        for table, table_metadata in zip(tables_list, results):
            table_name = table.split(',')[1].lower()
            table_metadata_dict[table_name] = table_metadata
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
                    sum_fl.write(
                        f"\tDifference - {set(file_md_dict[item]['File_Columns_Names']) - set(tbl_md_dict[item]['Table_Columns_Names'])}\n")

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
                if not file_md_dict:
                    return {"Glue Table Validation": "FAILED"}
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

def get_latest_files(file_list):
    """Returns the latest files matching the required file pattern."""
    pattern = re.compile(r'(\d{8})-\d{4}_par0[1-3]\.zip')
    files_by_date = {}

    for filename in file_list:
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
