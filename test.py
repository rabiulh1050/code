import re

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

# Function to read the SQL file, convert the INSERT to SELECT, and overwrite the file
def process_sql_file(file_path):
    # Read the SQL file
    with open(file_path, 'r') as file:
        sql_content = file.read()

    # Convert the INSERT statement to a SELECT statement
    select_statement = convert_insert_to_select(sql_content)

    # Write the converted SELECT statement back to the file
    with open(file_path, 'w') as file:
        file.write(select_statement)

    print(f"The file {file_path} has been updated with the SELECT statement.")

# Path to the SQL file
sql_file_path = 'path_to_your_sql_file.sql'

# Process the SQL file
process_sql_file(sql_file_path)
