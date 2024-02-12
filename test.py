import re
import boto3

def check_s3_objects(bucket_name, parent_directory):
    """
    Checks if a specified parent directory in an AWS S3 bucket exists,
    contains exactly 3 objects with specific naming conventions.
    
    Parameters:
    - bucket_name: The name of the S3 bucket.
    - parent_directory: The parent directory path within the bucket.
    
    Returns:
    - A tuple containing the date extracted from the file names and a list of all 3 object prefixes,
      or a message indicating any issues encountered.
    """
    # Ensure parent_directory is formatted correctly
    if not parent_directory.endswith('/'):
        parent_directory += '/'
    
    # Initialize boto3 S3 client
    s3_client = boto3.client('s3')
    
    # Attempt to list objects within the parent directory
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=parent_directory, Delimiter='/')
        objects = response.get('Contents', [])
        
        # Check if exactly 3 objects exist
        if len(objects) != 3:
            return "Parent directory does not contain exactly 3 objects.", None
        
        # Pattern to match expected file format
        pattern = re.compile(r'^(\d{8})-\d{4}-par0[1-3]\.zip$')
        dates = []
        prefixes = []
        
        for obj in objects:
            filename = obj['Key'].split('/')[-1]  # Extract filename from object key
            match = pattern.match(filename)
            if not match:
                return "One or more files do not match the expected format.", None
            dates.append(match.group(1))
            prefixes.append(filename[:filename.find('-par')])
        
        # Verify all dates are the same
        if len(set(dates)) != 1:
            return "Files do not have the same date.", None
        
        return dates[0], prefixes
    
    except Exception as e:
        return f"An error occurred: {str(e)}", None

# Example function call (commented out to prevent execution)
# result = check_s3_objects("example-bucket", "path/to/parent-directory")
# print(result)
