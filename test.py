import boto3
import time

def execute_step_function_and_wait(state_machine_arn, input_data, wait_time=5):
    """
    Executes an AWS Step Function state machine and waits for its completion to get the output.
    
    Parameters:
    - state_machine_arn: ARN of the Step Functions state machine to execute.
    - input_data: JSON input data for the state machine execution.
    - wait_time: Time in seconds to wait between status checks. Default is 5 seconds.
    
    Returns:
    - The output from the completed Step Functions execution.
    """
    # Initialize the boto3 client for Step Functions
    sf_client = boto3.client('stepfunctions')
    
    # Start execution of the state machine
    execution_response = sf_client.start_execution(
        stateMachineArn=state_machine_arn,
        input=input_data
    )
    
    execution_arn = execution_response['executionArn']
    
    # Wait for the execution to complete
    while True:
        # Check the execution status
        status_response = sf_client.describe_execution(executionArn=execution_arn)
        status = status_response['status']
        
        if status in ['SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED']:
            # Execution completed
            break
        else:
            # Wait before the next status check
            time.sleep(wait_time)
    
    # Return the output from the execution
    return status_response['output'] if status == 'SUCCEEDED' else None

# Example usage (commented out to prevent accidental execution)
# state_machine_arn = 'arn:aws:states:region:account-id:stateMachine:stateMachineName'
# input_data = '{"key": "value"}'
# output = execute_step_function_and_wait(state_machine_arn, input_data)
# print(output)
