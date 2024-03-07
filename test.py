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



I hope this email finds you in great spirits.

I wanted to touch base with you regarding my upcoming vacation, which is set from April 22nd to May 3rd. During this time, I'll be traveling outside the country. I've made sure to mark these dates clearly on my Outlook calendar and have also shared this information with the team during our recent meetings.

Thank you for your support and understanding. I look forward to your confirmation of my leave dates.
