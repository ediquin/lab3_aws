import json
import os
import boto3
from PIL import Image
from io import BytesIO
from datetime import datetime

# Initialize AWS clients
s3 = boto3.client('s3')

# Get environment variables
BUCKET_NAME = os.environ['BUCKET_NAME']

def lambda_handler(event, context):
    """
    Lambda 2: Processor Function
    Triggered by SQS messages
    Downloads image, extracts metadata, saves to S3
    """
    
    print(f"Received event: {json.dumps(event)}")
    
    # Process each SQS record (usually 1 because BatchSize=1)
    for record in event['Records']:
        # Parse the message body
        message_body = json.loads(record['body'])
        
        bucket = message_body['bucket']
        key = message_body['key']
        etag = message_body['etag']
        
        print(f"Processing: bucket={bucket}, key={key}, etag={etag}")
        
        # Extract filename from key
        filename = key.split('/')[-1]  # "incoming/cat.jpg" → "cat.jpg"
        filename_without_ext = filename.rsplit('.', 1)[0]  # "cat.jpg" → "cat"
        
        # Generate metadata file path
        metadata_key = f"metadata/{filename_without_ext}.json"
        
        # Check if metadata already exists (IDEMPOTENCY)
        if metadata_exists(bucket, metadata_key):
            print(f"Metadata already exists: {metadata_key}. Skipping.")
            continue
        
        try:
            # Download image from S3
            print(f"Downloading image from S3...")
            response = s3.get_object(Bucket=bucket, Key=key)
            image_data = response['Body'].read()
            
            # Extract metadata
            print(f"Extracting metadata...")
            metadata = extract_metadata(image_data, filename, etag)
            
            # Save metadata to S3
            print(f"Saving metadata to S3...")
            s3.put_object(
                Bucket=bucket,
                Key=metadata_key,
                Body=json.dumps(metadata, indent=2),
                ContentType='application/json'
            )
            
            print(f"Successfully processed: {filename}")
            
        except Exception as e:
            print(f"Error processing {key}: {str(e)}")
            raise  # Re-raise to keep message in queue for retry
    
    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete')
    }

def metadata_exists(bucket, key):
    """
    Check if metadata file already exists in S3
    This ensures idempotency
    """
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise

def extract_metadata(image_data, filename, etag):
    """
    Extract metadata from image bytes
    Returns a dictionary with image information
    """
    # Open image using PIL (Pillow)
    image = Image.open(BytesIO(image_data))
    
    # Basic metadata
    metadata = {
        'filename': filename,
        'etag': etag,
        'format': image.format,  # JPEG, PNG, etc.
        'mode': image.mode,      # RGB, RGBA, L, etc.
        'width': image.width,
        'height': image.height,
        'size_bytes': len(image_data),
        'processed_at': datetime.utcnow().isoformat()
    }
    
    # Extract EXIF data if available (mainly for JPEG)
    try:
        exif_data = image._getexif()
        if exif_data:
            # Convert EXIF data to readable format
            exif = {}
            for tag_id, value in exif_data.items():
                # Get human-readable tag name
                tag_name = Image.ExifTags.TAGS.get(tag_id, tag_id)
                
                # Convert bytes to string if needed
                if isinstance(value, bytes):
                    try:
                        value = value.decode()
                    except:
                        value = str(value)
                
                exif[tag_name] = value
            
            metadata['exif'] = exif
    except:
        # No EXIF data or error reading it
        metadata['exif'] = None
    
    return metadata