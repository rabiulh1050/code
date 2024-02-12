import boto3

def copy_s3_objects(source_bucket, source_prefix, destination_bucket, destination_prefix):
    """
    Copies a set of objects from a source location to a destination location within S3.
    
    Parameters:
    - source_bucket: The name of the source S3 bucket.
    - source_prefix: The key prefix for objects in the source bucket to be copied.
    - destination_bucket: The name of the destination S3 bucket.
    - destination_prefix: The key prefix for objects in the destination bucket.
    """
    s3_client = boto3.client('s3')
    # List objects in the source bucket with the specified prefix
    response = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)
    
    if 'Contents' in response:
        for obj in response['Contents']:
            source_key = obj['Key']
            destination_key = destination_prefix + source_key[len(source_prefix):]
            
            # Copy object from source to destination
            copy_source = {
                'Bucket': source_bucket,
                'Key': source_key
            }
            s3_client.copy(copy_source, destination_bucket, destination_key)
            print(f"Copied {source_key} to {destination_key} in bucket {destination_bucket}")
    else:
        print("No objects found with the specified prefix.")

# Example function call (commented out to prevent execution)
# copy_s3_objects('source-bucket', 'source-prefix/', 'destination-bucket', 'destination-prefix/')
