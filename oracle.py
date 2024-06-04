import cx_Oracle
import psycopg2
from config import APP_CONFIG
from aws_utils_func import fetch_db_credential
from logs import log_errors, configure_logging

# Logger configuration
logger = configure_logging()

def output_type_handler(cursor, name, default_type, size, precision, scale):
    """Handles the output types for Oracle CLOB, BLOB, and NCLOB data types."""
    if default_type == cx_Oracle.CLOB:
        return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize)
    if default_type == cx_Oracle.BLOB:
        return cursor.var(cx_Oracle.LONG_BINARY, arraysize=cursor.arraysize)
    if default_type == cx_Oracle.NCLOB:
        return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize)

def get_db_credentials():
    """Fetch database credentials from AWS Secrets Manager or configuration."""
    if APP_CONFIG.get("RDS_DB_Secret"):
        db_secret = APP_CONFIG["RDS_DB_Secret"]
    else:
        app_short_cd = APP_CONFIG['App_Short_Name']
        rds_db_scrt_nm = APP_CONFIG["AWS_Secret_Name"]['RDS_Oracle_Secret_Name']
        db_secret = fetch_db_credential(app_short_cd, rds_db_scrt_nm)
    return db_secret

def construct_conn_str(db_secret):
    """Construct the Oracle connection string."""
    return f'(DESCRIPTION = (ADDRESS = (PROTOCOL = TCPS)(HOST = {db_secret["host"]})(PORT = {db_secret["port"]}))' \
           f'(CONNECT_DATA = (SID = {db_secret["dbname"]})))'

def db_connection():
    """Establish a database connection to Oracle DB."""
    db_secret = get_db_credentials()
    user = db_secret['username']
    pwd = db_secret['password']
    conn_str = construct_conn_str(db_secret)
    
    try:
        conn = cx_Oracle.connect(user, pwd, conn_str)
        conn.outputtypehandler = output_type_handler
        conn.autocommit = True
        logger.info('Connected to the Oracle database successfully.')
        return conn
    except cx_Oracle.DatabaseError as e:
        log_errors(e, detailed_traceback=True)
        return None

def fetch_one(query: str, **params):
    """Fetch a single record from the database."""
    try:
        db_conn = db_connection()
        if db_conn:
            with db_conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
            return result
    except Exception as e:
        log_errors(e)
    finally:
        if db_conn:
            db_conn.close()
    return None

def fetch_all(query: str, **params):
    """Fetch all records for a given query."""
    try:
        db_conn = db_connection()
        if db_conn:
            with db_conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchall()
            return result
    except Exception as e:
        log_errors(e)
    finally:
        if db_conn:
            db_conn.close()
    return None

def delete_record(query: str, **params):
    """Delete data for a given query and parameters."""
    try:
        db_conn = db_connection()
        if db_conn:
            with db_conn.cursor() as cur:
                cur.execute(query, params)
                logger.info(f"Delete statement executed, query: {query}")
    except cx_Oracle.Error as e:
        log_errors(e)
    finally:
        if db_conn:
            db_conn.close()
            logger.info("Connection is closed.")

def get_redshift_conn(conn_str):
    """Establish a connection to Redshift."""
    try:
        conn = psycopg2.connect(
            dbname=conn_str['dbname'],
            user=conn_str['username'],
            host=conn_str['host'],
            password=conn_str['password'],
            port=conn_str['port']
        )
        logger.info("Redshift connection established successfully.")
        return conn
    except Exception as e:
        log_errors(e, message='Exception occurred while trying to connect to Redshift DB', detailed_traceback=True)
        return None

def rs_fetchall(conn, query: str, **params):
    """Fetch all records from Redshift for a given query."""
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchall()
                logger.info(f'Successfully extracted data from Redshift for query: {query}')
                return result
        except Exception as e:
            log_errors(e, message='Unable to fetch data from Redshift, unexpected error occurred')
    return None

# Example usage
if __name__ == "__main__":
    # Example calls to the functions
    query = "SELECT * FROM some_table WHERE id = :id"
    params = {'id': 1}
    
    single_record = fetch_one(query, **params)
    logger.info(f"Fetched one record: {single_record}")

    all_records = fetch_all(query, **params)
    logger.info(f"Fetched all records: {all_records}")

    delete_query = "DELETE FROM some_table WHERE id = :id"
    delete_record(delete_query, **params)
    logger.info("Record deleted successfully.")

    redshift_conn_str = {
        'dbname': 'mydb',
        'username': 'myuser',
        'host': 'myhost',
        'password': 'mypassword',
        'port': '5439'
    }
    redshift_conn = get_redshift_conn(redshift_conn_str)
    redshift_query = "SELECT * FROM another_table WHERE id = %s"
    redshift_records = rs_fetchall(redshift_conn, redshift_query, id=1)
    logger.info(f"Fetched records from Redshift: {redshift_records}")

    if redshift_conn:
        redshift_conn.close()
        logger.info("Redshift connection closed.")
