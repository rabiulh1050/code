from datetime import datetime

# Updated test_results to include an "Error" status example
test_results = {
    "happyPath": {"Data Validation": "Succeeded", "DSet Validation": "Succeeded", "Notification Validation": "Succeeded", "DTF Validation": "Succeeded"},
    "mismatch": {"Data Validation": "Succeeded", "DSet Validation": "Failed", "Notification Validation": "Not Run", "DTF Validation": "Error"},  # Example with "Error" status
    "scanned": {"Data Validation": "Succeeded", "DSet Validation": "Succeeded", "Notification Validation": "Failed", "DTF Validation": "Not Run"},
    "missingfile": {"Data Validation": "Failed", "DSet Validation": "Not Run", "Notification Validation": "Not Run", "DTF Validation": "Not Run"},
    "missingcountfile": {"Data Validation": "Succeeded", "DSet Validation": "Failed", "Notification Validation": "Succeeded", "DTF Validation": "Not Run"},
    "multievent": {"Data Validation": "Succeeded", "DSet Validation": "Succeeded", "Notification Validation": "Succeeded", "DTF Validation": "Failed"}
}

status_classes = {
    "Succeeded": "succeeded",
    "Failed": "failed",
    "Not Run": "not-run",
    "Error": "error"  # Added "Error" status class
}

html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>DR Automation Test Report</title>
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
    </style>
</head>
<body>
    <h1>DR Automation Test Report</h1>
    <div class="metadata">
        <p>Environment: {environment}</p>
        <p>Test Execution Timestamp: {timestamp}</p>
    </div>
    <div class="report-container">
        {rows}
    </div>
</body>
</html>
"""

def generate_html_report(test_results, current_timestamp, test_environment):
    rows = ""
    for event, results in test_results.items():
        row = f'<div class="report-item">\n<h2>{event}</h2>\n'
        for step, status in results.items():
            css_class = status_classes.get(status, "not-run")
            row += f'<div class="status {css_class}">{step}: {status}</div>\n'
        row += "</div>\n"
        rows += row
    return html_template.format(timestamp=current_timestamp, rows=rows, environment = test_environment)

# Generate HTML content
test_environment = "QA Test"
current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
html_content = generate_html_report(test_results, current_timestamp, test_environment)

# Save this HTML content to a file
with open("DR_Automation_test_report.html", "w") as report_file:
    report_file.write(html_content)

print("HTML report generated successfully.")
