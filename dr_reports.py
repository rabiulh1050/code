html_template = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Test Automation Report</title>
    <style>
        :root {{
            --success-color: #4CAF50;
            --fail-color: #F44336;
            --not-run-color: #FF9800;
            --error-color: #F44336;
            --not-required-color: #607D8B;
            --background-color: #f2f2f2;
        }}
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 20px;
        }}
        h1 {{
            text-align: center;
        }}
        .report-container {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
            gap: 20px;
        }}
        .report-item {{
            border: 1px solid #ddd;
            padding: 20px;
            border-radius: 5px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            overflow: auto;
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
            margin-top: 5px;
            margin-right: 10px;
            white-space: nowrap;
        }}
        .succeeded {{ background-color: var(--success-color); }}
        .failed {{ background-color: var(--fail-color); }}
        .not-run {{ background-color: var(--not-run-color); }}
        .error {{ background-color: var(--error-color); }}
        .not-required {{ background-color: var(--not-required-color); }}
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
        .chart-container {{
            width: 320px;
            height: 320px;
            max-width: 4in;
            max-height: 4in;
            margin: 20px auto;
            text-align: center;
        }}
        .chart-container h2 {{
            text-align: center;
            font-size: 20px;
        }}
    </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
    <h1>{test_name}</h1>
    <div class="metadata">
        <p>Environment: {environment}</p>
        <p>Test Execution Timestamp: {timestamp}</p>
    </div>
    <div class="report-container">
        {rows}
    </div>
    {multievent_explanation}
    <script>
        {chart_script}
    </script>
</body>
</html>
"""

def generate_html_report(test_results, current_timestamp, test_environment, result_count: tuple = None):
    rows = ""
    multievent_explanation = ""
    chart_script = ""
    test_passed, test_failed = result_count if result_count else (0, 0)

    for event, results in test_results.items():
        row = f'<div class="report-item">\n<h2>{event}</h2>\n'
        for step, status in results.items():
            css_class = status_classes.get(status, "not-run")
            row += f'<div class="status {css_class}">{step}: {status}</div>\n'
        if event == 'gluecompare':
            row += f"""
            <div class="chart-container">
                <h2>Glue Compare Test Result</h2>
                <canvas id="testResultChart"></canvas>
            </div>
            """
            chart_script = f"""
            const passed = {test_passed};  // Input value for passed
            const failed = {test_failed};  // Input value for failed

            const ctx = document.getElementById('testResultChart').getContext('2d');
            const myPieChart = new Chart(ctx, {{
                type: 'pie',
                data: {{
                    labels: ['Passed', 'Failed'],
                    datasets: [{{
                        data: [passed, failed],
                        backgroundColor: ['#36a2eb', '#ff6384'],
                        hoverBackgroundColor: ['#36a2eb', '#ff6384']
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    let label = context.label || '';
                                    if (label) {{
                                        label += ': ';
                                    }}
                                    label += context.raw + ' (' + (context.raw / (passed + failed) * 100).toFixed(2) + '%)';
                                    return label;
                                }}
                            }}
                        }}
                    }}
                }}
            }});
            """
        row += "</div>\n"
        rows += row
        if event == "multievent":
            multievent_explanation = "<div class='explanation'>The 'multievent' encompasses scenarios: mismatch, " \
                                     "scanned, and missingfile.</div>"

    return html_template.format(test_name='DR Test Automation Report', timestamp=current_timestamp, rows=rows,
                                environment=test_environment, multievent_explanation=multievent_explanation,
                                chart_script=chart_script)

# Sample test results
test_results = {
    "event1": {
        "step1": "succeeded",
        "step2": "failed",
        "step3": "succeeded"
    },
    "gluecompare": {
        "step1": "succeeded",
        "step2": "failed",
        "step3": "succeeded"
    },
    "event2": {
        "step1": "not-run",
        "step2": "error",
        "step3": "not-required"
    }
}

# Sample result count for gluecompare
result_count = (7, 3)  # 7 passed, 3 failed

# Other details
current_timestamp = "2023-05-20 12:00:00"
test_environment = "Test Environment"

# Status classes mapping
status_classes = {
    "succeeded": "succeeded",
    "failed": "failed",
    "not-run": "not-run",
    "error": "error",
    "not-required": "not-required"
}

# Generate the HTML report
html_report = generate_html_report(test_results, current_timestamp, test_environment, result_count)

# Save the HTML report to a file
with open("test_report.html", "w") as report_file:
    report_file.write(html_report)

print("HTML report generated successfully.")
