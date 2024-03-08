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
    "happyPath": {"Data Validation": "Succeeded","Step Function Validation": "Succeeded", "DSet Validation": "Succeeded", "Notification Validation": "Succeeded", "DTF Validation": "Succeeded"},
    "mismatch": {"Data Validation": "Succeeded", "DSet Validation": "Failed", "Notification Validation": "Not Run", "DTF Validation": "Error"},  # Example with "Error" status
    "scanned": {"Data Validation": "Succeeded", "DSet Validation": "Succeeded", "Notification Validation": "Failed", "DTF Validation": "Not Run"},
    "missingfile": {"Data Validation": "Failed", "DSet Validation": "Not Run", "Notification Validation": "Not Run", "DTF Validation": "Not Run"},
    "missingcountfile": {"Data Validation": "Succeeded", "DSet Validation": "Failed", "Notification Validation": "Succeeded", "DTF Validation": "Not Run"},
    #"multievent": {"Data Validation": "Succeeded", "DSet Validation": "Succeeded", "Notification Validation": "Succeeded", "DTF Validation": "Failed"}
}

status_classes = {
    "Succeeded": "succeeded",
    "Failed": "failed",
    "Not Run": "not-run"
}
# Your existing test_results and status_classes remain unchanged.

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
            --not-run-color: #9E9E9E;
            --error-color: #FF9800; /* Color for Error status */
            --background-color: #f2f2f2;
        }}
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 20px;
        }}
        .report-container {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
            gap: 20px;
        }}
        .report-item {{
            border: 1px solid #ddd;
            padding: 20px;
            border-radius: 5px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
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
            margin-top: 5px; /* Added margin for visual spacing between status items */
        }}
        .succeeded {{ background-color: var(--success-color); }}
        .failed {{ background-color: var(--fail-color); }}
        .not-run {{ background-color: var(--not-run-color); }}
        .error {{ background-color: var(--error-color); }} /* Style for Error status */
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
    <h1>Test Automation Report</h1>
    <div class="metadata">
        <p>Environment: Production</p>
        <p>OS: Linux</p>
        <p>Python Version: 3.8</p>
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

    current_timestamp = datetime.now().strftime('%Y%m%d')  # Updated as per your previous request
    return html_template.format(timestamp=current_timestamp, rows=rows, multievent_explanation=multievent_explanation)

# Generate HTML content
html_content = generate_html_report(test_results)

# Save this HTML content to a file
with open("test_report.html", "w") as report_file:
    report_file.write(html_content)

print("HTML report generated successfully.")

