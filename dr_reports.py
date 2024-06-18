<!DOCTYPE html>
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
            padding: 1em;
            margin-top: 10px;
            border-radius: 5px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .report-item h2 {{
            font-size: 20px;
            padding: 0.1em;
            text-align: center;
        }}
        .status {{
            padding: 5px 10px;
            color: white;
            border-radius: 4px;
            margin-top: 5px;
            margin-right: 10px;
            white-space: nowrap;
            text-align: center;
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
            width: 80%;
            height: 80%;
            max-width: 4in;
            max-height: 4in;
            margin: auto;
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
    <h1>{{test_name}}</h1>
    <div class="metadata">
        <p>Environment: {{environment}}</p>
        <p>Test Execution Timestamp: {{timestamp}}</p>
    </div>
    <div class="report-container">
        {{rows}}
    </div>
    {{multievent_explanation}}
    <script>
        {{chart_script}}
    </script>
</body>
</html>







import logging
from pathlib import Path
from typing import Dict, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define status classes for CSS
status_classes = {
    'succeeded': 'succeeded',
    'failed': 'failed',
    'not_run': 'not-run',
    'error': 'error',
    'not_required': 'not-required'
}

def read_html_file(html_file_path: Path) -> str:
    """Read the HTML template file and return its content as a string."""
    try:
        with html_file_path.open('r', encoding='utf-8') as file:
            return file.read()
    except FileNotFoundError:
        logging.error(f"HTML file not found: {html_file_path}")
        raise
    except Exception as e:
        logging.error(f"Error reading HTML file: {e}")
        raise

def create_chart_script(test_passed: int, test_failed: int) -> str:
    """Create JavaScript for rendering the chart in the HTML report."""
    return f"""
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

def generate_html_report(test_results: Dict[str, Dict[str, str]], 
                         current_timestamp: str, 
                         test_environment: str, 
                         result_count: Tuple[int, int] = None) -> str:
    """
    Generate an HTML report for the test automation results.

    :param test_results: Dictionary containing test events and their results
    :param current_timestamp: Timestamp of the test execution
    :param test_environment: Environment in which the tests were run
    :param result_count: Tuple containing the count of passed and failed tests
    :return: Generated HTML report as a string
    """
    try:
        html_file_path = Path("test_reports/dr_html_report.html").resolve()
        html_template = read_html_file(html_file_path)
        
        rows, multievent_explanation, chart_script = "", "", ""
        test_passed, test_failed = result_count if result_count else (0, 0)

        for event, results in test_results.items():
            row = create_report_item(event, results)
            if event == 'gluecompare':
                row += create_chart_container()
                chart_script = create_chart_script(test_passed, test_failed)
            rows += row
            if event == "multievent":
                multievent_explanation = ("<div class='explanation'>The 'multievent' encompasses scenarios: mismatch, "
                                          "scanned, and missingfile.</div>")

        # Ensure the HTML template placeholders are correctly formatted
        return html_template.format(
            test_name='DR Test Automation Report',
            timestamp=current_timestamp,
            rows=rows,
            environment=test_environment,
            multievent_explanation=multievent_explanation,
            chart_script=chart_script
        )
    except KeyError as e:
        logging.error(f"KeyError encountered: {e}")
        raise
    except Exception as e:
        logging.error(f"Error generating HTML report: {e}")
        raise

def create_report_item(event: str, results: Dict[str, str]) -> str:
    """Create an HTML div for a report item."""
    row = f'<div class="report-item">\n<h2>{event}</h2>\n'
    for step, status in results.items():
        css_class = status_classes.get(status, "not-run")
        row += f'<div class="status {css_class}">{step}: {status}</div>\n'
    row += "</div>\n"
    return row

def create_chart_container() -> str:
    """Create an HTML div for the chart container."""
    return """
    <div class="chart-container">
        <h2>Glue Compare Test Result</h2>
        <canvas id="testResultChart"></canvas>
    </div>
    """

# Example usage:
# test_results = {'event1': {'step1': 'succeeded', 'step2': 'failed'}, 'gluecompare': {'step1': 'succeeded'}}
# current_timestamp = '2023-06-18 12:00:00'
# test_environment = 'Production'
# result_count = (5, 2)
# report = generate_html_report(test_results, current_timestamp, test_environment, result_count)
# print(report)
