events = [
    "happyPath",
    "mismatch",
    "scanned",
    "missingfile",
    "missingcountfile",
    "multievent"
]

test_results = {
    "happyPath": ["Succeeded", "Succeeded", "Succeeded", "Succeeded"],
    "mismatch": ["Succeeded", "Failed", "Not Run", "Not Run"],
    "scanned": ["Succeeded", "Succeeded", "Failed", "Not Run"],
    "missingfile": ["Failed", "Not Run", "Not Run", "Not Run"],
    "missingcountfile": ["Succeeded", "Failed", "Succeeded", "Not Run"],
    "multievent": ["Succeeded", "Succeeded", "Succeeded", "Failed"]
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
    for event in events:
        results = test_results.get(event, ["Not Run"] * 4)
        row = f"<tr>\n<td>{event}</td>\n"
        for status in results:
            css_class = status_classes.get(status, "not-run")
            row += f'<td class="{css_class}">{status}</td>\n'
        row += "</tr>\n"
        rows += row

    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return html_template.format(timestamp=current_timestamp, rows=rows)

# Generate HTML content
html_content = generate_html_report(events, test_results)

# You can then save this HTML content to a file or use it as needed
with open("test_report.html", "w") as report_file:
    report_file.write(html_content)

print("HTML report generated successfully.")









# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
DTF_DIRECTORY = Path('./dtf').resolve()


# Refactored functions
def log_event_start(event):
    logging.info(f'Initiating event validation for event: {event}')


def validate_and_process_event(event, dtf=False):
    log_event_start(event)
    try:
        test_data = check_s3_objects_v2(s3_prefix)
        if not test_data:
            logging.error(f'Test data does not exist at {s3_prefix}')
            return

        date = test_data['Date']
        if not copy_s3_objects(source_prefix, destination_prefix):
            logging.error('Failed to copy data')
            return

        logging.info(f'Data was copied to {destination_prefix}')
        unzip_data = s3_check_dr_unzip_file(date_string)
        if not unzip_data:
            logging.error('Failed to unzip data')
            return

        count_file_prefix = unzip_data['count_file']
        if not execute_step_function_and_wait(state_machine_arn, input_data):
            logging.error('Step function execution failed')
            return

        logging.info('Step function was executed successfully')
        if not dr_sns_notification_validation(s3_prefix, event):
            logging.error('SNS notification failed')
            return

        logging.info('SNS notification validation successful')
        if dtf:
            dtf_status = execute_dtf_test(event, count_file_prefix)
            if not dtf_status:
                logging.error('DTF test failed')
            else:
                logging.info('DTF test executed successfully')
    except Exception as e:
        logging.error(f'Error during event validation for {event}: {e}')


def execute_dtf_test(event, count_file_prefix):
    test_case_file_name = DTF_DIRECTORY / 'DR_DTF.yaml'
    dtf_token = {'@s3@': count_file_prefix, 'event_name': event}
    return dtf_executoion(test_case_file_name, dtf_token)

def dr_main(events):
    events_list = [event.strip().lower() for event in events.split(',')]

    for event in events_list:
        if event == 'happypath':
            print('Event is happyPath')
            validate_and_process_event(event, dtf=True)
        elif event == 'mismatch':
            print('Event is mismatch')
            validate_and_process_event(event, dtf=False)
        elif event == 'scanned':
            print('Event is scanned')
            validate_and_process_event(event, dtf=False)
        elif event == 'missingfile':
            print('Event is missingfile')
            validate_and_process_event(event, dtf=False)
        elif event == 'missingcountfile':
            print('Event is missingcountfile')
            validate_and_process_event(event, dtf=False)
        elif event == 'multievent':
            print('Event is multievent')
            validate_and_process_event(event, dtf=False)
        else:
            print(f'Event "{event}" does not match any target events')
