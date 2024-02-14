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
