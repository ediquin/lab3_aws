import json
import os
import boto3
from urllib.parse import unquote_plus

# Initialize AWS clients
sqs = boto3.client('sqs')

# Get environment variables (set in template.yaml)
QUEUE_URL = os.environ['QUEUE_URL']

# Valid image extensions
VALID_EXTENSIONS = ['.jpg', '.jpeg', '.png']

def lambda_handler(event, context):
    """
    Lambda 1: Ingest Function
    Triggered when a file is uploaded to S3 incoming/ prefix
    Validates the file and sends message to SQS
    """
    
    print(f"Received event: {json.dumps(event)}")
    
    # Process each record (S3 can send multiple files at once)
    for record in event['Records']:
        # Extract S3 information
        bucket_name = record['s3']['bucket']['name']
        object_key = unquote_plus(record['s3']['object']['key'])  # Decode URL encoding
        etag = record['s3']['object']['eTag']
        
        print(f"Processing: bucket={bucket_name}, key={object_key}, etag={etag}")
        
        # Validate file extension
        if not is_valid_image(object_key):
            print(f"Skipping non-image file: {object_key}")
            continue
        
        # Send message to SQS
        message_body = {
            'bucket': bucket_name,
            'key': object_key,
            'etag': etag
        }
        
        try:
            response = sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=json.dumps(message_body)
            )
            print(f"Message sent to SQS: MessageId={response['MessageId']}")
        except Exception as e:
            print(f"Error sending message to SQS: {str(e)}")
            raise  # Re-raise to mark Lambda as failed
    
    return {
        'statusCode': 200,
        'body': json.dumps('Ingestion complete')
    }

def is_valid_image(filename):
    """
    Check if the file has a valid image extension
    """
    filename_lower = filename.lower()
    return any(filename_lower.endswith(ext) for ext in VALID_EXTENSIONS)