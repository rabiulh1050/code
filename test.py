def check_s3_objects_v2(bucket_name, parent_directory):
    """
    Checks if a specified parent directory in an AWS S3 bucket contains exactly 3 objects
    with filenames ending with par01.zip, par02.zip, and par03.zip, and extracts the date part.
    
    Parameters:
    - bucket_name: The name of the S3 bucket.
    - parent_directory: The parent directory path within the bucket.
    
    Returns:
    - A list containing the date parts extracted from the filenames and a list of all 3 object prefixes,
      or a message indicating any issues encountered.
    """
    # Ensure parent_directory is formatted correctly
    if not parent_directory.endswith('/'):
        parent_directory += '/'
    
    # Initialize boto3 S3 client
    s3_client = boto3.client('s3')
    
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=parent_directory, Delimiter='/')
        objects = response.get('Contents', [])
        
        # Filter objects that end with the required suffixes
        filtered_objects = [obj for obj in objects if obj['Key'].endswith(('par01.zip', 'par02.zip', 'par03.zip'))]
        
        # Verify there are exactly 3 such objects
        if len(filtered_objects) != 3:
            return "Directory does not contain exactly 3 objects with the required endings.", None
        
        dates = []
        prefixes = []
        
        for obj in filtered_objects:
            filename = obj['Key'].split('/')[-1]
            # Extract the date part - assuming it's at the start of the filename, before a delimiter like "-"
            date_part = re.search(r'^(\d+)', filename)
            if date_part:
                dates.append(date_part.group(1))
                prefix = filename[:filename.find('-par')]
                prefixes.append(prefix)
            else:
                return "Failed to extract the date part from one or more filenames.", None
        
        return dates, prefixes
    
    except Exception as e:
        return f"An error occurred: {str(e)}", None

# Example function call (commented out to prevent execution)
# result = check_s3_objects_v2("example-bucket", "path/to/parent-directory")
# print(result)
