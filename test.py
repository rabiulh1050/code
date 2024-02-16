from datetime import datetime

# Updated test_results to the new dictionary format
test_results = {
    "happyPath": {"Data Validation": "Succeeded", "DSet Validation": "Succeeded", "Notification Validation": "Succeeded", "DTF Validation": "Succeeded"},
    "mismatch": {"Data Validation": "Succeeded", "DSet Validation": "Failed", "Notification Validation": "Not Run", "DTF Validation": "Not Run"},
    "scanned": {"Data Validation": "Succeeded", "DSet Validation": "Succeeded", "Notification Validation": "Failed", "DTF Validation": "Not Run"},
    "missingfile": {"Data Validation": "Failed", "DSet Validation": "Not Run", "Notification Validation": "Not Run", "DTF Validation": "Not Run"},
    "missingcountfile": {"Data Validation": "Succeeded", "DSet Validation": "Failed", "Notification Validation": "Succeeded", "DTF Validation": "Not Run"},
    "multievent": {"Data Validation": "Succeeded", "DSet Validation": "Succeeded", "Notification Validation": "Succeeded", "DTF Validation": "Failed"}
}

status_classes = {
    "Succeeded": "succeeded",
    "Failed": "failed",
    "Not Run": "not-run"
}

html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Test Automation Report</title>
    <style>
        .succeeded {{ background-color: #4CAF50; color: white; }}
        .failed {{ background-color: #F44336; color: white; }}
        .not-run {{ background-color: #9E9E9E; color: white; }}
        table, th, td {{
            border: 1px solid black;
            border-collapse: collapse;
            padding: 5px;
            text-align: center;
        }}
        th {{ background-color: #f2f2f2; }}
    </style>
</head>
<body>
    <h1>Test Automation Report</h1>
    <p>Environment: Production</p>
    <p>OS: Linux</p>
    <p>Python Version: 3.8</p>
    <p>Test Execution Timestamp: {timestamp}</p>

    <table>
        <tr>
            <th>Event</th>
            <th>Data Validation</th>
            <th>DSet Validation</th>
            <th>Notification Validation</th>
            <th>DTF Validation</th>
        </tr>
        {rows}
    </table>
</body>
</html>
"""

def generate_html_report(events, test_results):
    rows = ""
    validation_steps = ["Data Validation", "DSet Validation", "Notification Validation", "DTF Validation"]
    for event in events:
        event_results = test_results.get(event, {step: "Not Run" for step in validation_steps})
        row = f"<tr>\n<td>{event}</td>\n"
        for step in validation_steps:
            status = event_results.get(step, "Not Run")
            css_class = status_classes.get(status, "not-run")
            row += f'<td class="{css_class}">{status}</td>\n'
        row += "</tr>\n"
        rows += row

    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return html_template.format(timestamp=current_timestamp, rows=rows)

# Assuming events list is still relevant for iterating through events
events = [
    "happyPath",
    "mismatch",
    "scanned",
    "missingfile",
    "missingcountfile",
    "multievent"
]

# Generate HTML content
html_content = generate_html_report(events, test_results)

# Save this HTML content to a file
with open("test_report.html", "w") as report_file:
    report_file.write(html_content)

print("HTML report generated successfully.")
