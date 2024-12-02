import json
import os
import boto3
import logging
import subprocess
import concurrent.futures
import time
from pathlib import Path
from typing import Dict, Optional, List, Tuple
from datetime import datetime
from botocore.config import Config
from dataset_extraction_scripts.l1bconvertandupload import process_file
from utils.metadata_handler import MetadataHandler  
from dataset_extraction_scripts.l1bconvertandupload import INSAT3DProcessor  # Add this import

# Configure logging with more detail
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

def lambda_handler(event, context):
    try:
        # Get region from environment
        region = os.environ.get('AWS_DEFAULT_REGION', 'ap-south-1')
        
        # Initialize handlers with correct region
        metadata_handler = MetadataHandler(region=region)
        
        # Get S3 event details
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        file_path = f"/vsis3/{bucket}/{key}"
        
        logger.info(f"Processing file: {file_path}")
        
        # Process metadata
        metadata_success = metadata_handler.upload_metadata(file_path)
        
        # Process file with same region
        destination_bucket = os.environ.get('DESTINATION_BUCKET', 'final-cog')
        processor = INSAT3DProcessor(
            input_path=f"{bucket}/{key}",
            output_bucket=destination_bucket,
            max_workers=5
        )
        
        results = processor.process()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'metadata_processed': metadata_success,
                'files_processed': list(results.keys()) if results else []
            })
        }
            
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }