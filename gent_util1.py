import json
import traceback
from collections import defaultdict
from lxml import etree
from logs import log_errors, configure_logging

# Logger configuration
logger = configure_logging()

def read_file(file_path: str) -> str:
    """Read the content of a file."""
    try:
        with open(file_path, 'r') as file:
            return file.read()
    except Exception as e:
        log_errors(e)
        return ""

def extract_tax_ids_from_xml_content(xml_content: str, xml_tag: str) -> list:
    """Extract tax IDs from XML content based on the provided XML tag."""
    try:
        root = etree.fromstring(bytes(xml_content, encoding='utf8'))
        tax_ids = [element.text for element in root.iter() if xml_tag == etree.QName(element).localname]
        return [tax_id for tax_id in tax_ids if tax_id is not None]
    except Exception as e:
        log_errors(e)
        return []

def extract_tax_ids_from_json_message(json_msg_file: str) -> list:
    """Extract tax IDs from a JSON message file."""
    logger.info(f"Extracting Borrower Tax IDs from {json_msg_file} message.")
    try:
        msg = json.loads(read_file(json_msg_file))
        xml_msg = msg.get('Message', '')
        tax_ids = extract_tax_ids_from_xml_content(xml_msg, 'BorrowerTaxpayerIdentifier')
        log_tax_id_extraction_results(json_msg_file, tax_ids)
        return tax_ids
    except Exception as e:
        log_errors(e)
        return []

def extract_tax_ids_from_xml_s3(xml_s3_path: str, xml_tag: str = 'BorrowerTaxpayerIdentifier') -> list:
    """Extract tax IDs from an XML file stored in S3."""
    logger.info(f"Extracting Borrower Tax IDs from {xml_s3_path} file.")
    try:
        xml_content = read_s3_file(xml_s3_path)
        tax_ids = extract_tax_ids_from_xml_content(xml_content, xml_tag)
        log_tax_id_extraction_results(xml_s3_path, tax_ids)
        return tax_ids
    except Exception as e:
        log_errors(e)
        return []

def extract_loan_data_from_xml_s3(xml_s3_path: str, xml_tag: str) -> dict:
    """Extract loan data from an XML file stored in S3."""
    logger.info(f'Starting to process XML for vend out decryption validation for {xml_s3_path} and {xml_tag}')
    try:
        xml_content = read_s3_file(xml_s3_path)
        root = etree.fromstring(bytes(xml_content, encoding='utf8'))
        data = extract_tag_path_and_values(root, xml_tag)
        log_loan_data_extraction_results(xml_s3_path, data)
        return dict(data)
    except Exception as e:
        log_errors(e)
        return {}

def extract_tag_path_and_values(root, xml_tag) -> defaultdict:
    """Extract tag paths and values from the XML root element."""
    tag_path_and_value = [(root.getroottree().getpath(element), element.text) for element in root.iter()
                          if xml_tag == etree.QName(element).localname]
    data = defaultdict(list)
    for tag_path, tag_value in tag_path_and_value:
        if 'Casefiles' in tag_path and tag_value not in data['casefiles']:
            data['casefiles'].append(tag_value)
        elif "Loans" in tag_path and tag_value not in data['loans']:
            data['loans'].append(tag_value)
    return data

def extract_loan_id(queue_message_body: str) -> tuple:
    """Extract loan ID from a queue message."""
    json_message = json.loads(queue_message_body)
    event_name = json_message.get('MessageAttributes').get("EVENT_TYPE").get("Value")
    try:
        logger.info('Extracting loan ID')
        loan_list = json.loads(json_message['MessageAttributes']["CLIENT_ATTRIBUTES"]["Value"])
        return event_name, loan_list[0].split('=')[1]
    except Exception as e:
        log_errors(e, message='Error occurred while extracting loan ID', detailed_traceback=True)
        return event_name, False

def extract_loan_data_from_queue_message(json_message_body: str) -> tuple:
    """Extract loan data from a JSON queue message."""
    logger.info(f"Extracting loan data from JSON queue message.")
    try:
        message_body = json.loads(json_message_body)
        xml_message = message_body['Message']
        event_name = message_body['MessageAttributes']["EVENT_TYPE"]["Value"]
        return event_name, xml_message
    except Exception as e:
        log_errors(e, message='Error occurred while extracting loan data from queue message')
        return None

def log_tax_id_extraction_results(file_path: str, tax_ids: list):
    """Log the results of tax ID extraction."""
    if tax_ids:
        logger.info(f"Borrower Tax ID(s) found in {file_path}.\n"
                    f"Number of Borrower Tax ID(s) from file: {len(tax_ids)}")
    else:
        logger.info(f"Borrower Tax ID(s) not found in {file_path}.")

def log_loan_data_extraction_results(xml_s3_path: str, data: dict):
    """Log the results of loan data extraction."""
    if data:
        logger.info(f"Borrower Tax ID(s) found in {xml_s3_path} file.\n"
                    f"Number of Borrower Tax ID(s) from XML file: {len(data)}")
    else:
        logger.info(f"Borrower Tax ID(s) not found in {xml_s3_path} file.")

# Example usage
if __name__ == "__main__":
    # Replace with actual parameters for testing
    json_msg_file = 'path/to/json_message.json'
    xml_s3_path = 'path/to/xml_s3_path'
    queue_message_body = '{"MessageAttributes": {"EVENT_TYPE": {"Value": "TEST_EVENT"}, "CLIENT_ATTRIBUTES": {"Value": "[\\"LoanID=12345\\"]"}}}'

    print(extract_tax_ids_from_json_message(json_msg_file))
    print(extract_tax_ids_from_xml_s3(xml_s3_path))
    print(extract_loan_id(queue_message_body))
    print(extract_loan_data_from_queue_message(queue_message_body))
