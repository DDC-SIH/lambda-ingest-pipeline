import subprocess
import json
import logging
from decimal import Decimal
import boto3
from datetime import datetime
from botocore.config import Config
from typing import Dict, Optional, Any
import os
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)

class MetadataHandler:
    """Handle metadata extraction and upload for INSAT-3D data"""
    
    # This matches the table name in template.yaml
    TABLE_NAME = "Files"  # Correct table name
    REQUIRED_FIELDS = ["Unique_Id", "Acquisition_Date", "Acquisition_Time_in_GMT"]
    VALID_REGIONS = ['ap-south-1', 'us-east-1', 'us-west-2']  # Add all needed regions
    
    def __init__(self, region: str = "ap-south-1"):
        """Initialize DynamoDB client"""
        if region not in self.VALID_REGIONS:
            raise ValueError(f"Invalid region. Must be one of {self.VALID_REGIONS}")
        self.region = region  # Store region
        self.dynamodb = boto3.client('dynamodb', region_name=self.region)  # Use stored region

    def _extract_metadata(self, h5_file_path: str) -> Dict[str, Any]:
        """Extract metadata using gdalinfo command"""
        try:
            cmd = ['gdalinfo', '-json', h5_file_path]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            if result.returncode != 0:
                raise RuntimeError(f"gdalinfo failed: {result.stderr}")
            
            metadata = json.loads(result.stdout)
            processed_metadata = {}
            
            # Extract metadata from nested structure
            if 'metadata' in metadata:
                for domain, values in metadata['metadata'].items():
                    if domain == '':
                        processed_metadata.update(values)
                    else:
                        processed_metadata[domain] = values

            return processed_metadata
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to run gdalinfo: {str(e)}")
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse gdalinfo JSON output: {str(e)}")
            return {}
        except Exception as e:
            logger.error(f"Failed to extract metadata: {str(e)}")
            return {}

    def _process_metadata(self, raw_metadata: Dict) -> Dict:
        """Process and validate metadata"""
        try:
            processed = {}
            
            # Handle required fields
            for field in self.REQUIRED_FIELDS:
                if field in raw_metadata:
                    processed[field] = str(raw_metadata[field])
            
            # Create acquisition timestamp
            date_str = raw_metadata.get("Acquisition_Date", "")
            time_str = raw_metadata.get("Acquisition_Time_in_GMT", "")
            if date_str and time_str:
                timestamp = f"{date_str}T{time_str}Z"
                processed["acquisition_timestamp"] = timestamp

            # Process remaining fields
            for key, value in raw_metadata.items():
                if value is not None:
                    if isinstance(value, (list, float)):
                        processed[key] = value[0] if isinstance(value, list) and len(value) == 1 else value
                    else:
                        processed[key] = str(value)

            return processed
        except Exception as e:
            logger.error(f"Failed to process metadata: {str(e)}")
            return {}

    def upload_metadata(self, h5_file_path: str) -> bool:
        """Extract and upload metadata to DynamoDB"""
        try:
            logger.info(f"Extracting metadata from {h5_file_path}")
            raw_metadata = self._extract_metadata(h5_file_path)
            if not raw_metadata:
                raise ValueError("No metadata extracted")

            processed_data = self._process_metadata(raw_metadata)
            if not processed_data:
                raise ValueError("Failed to process metadata")

            # Convert to DynamoDB format
            item = {k: {'S': str(v)} if isinstance(v, str) else {'N': str(v)}
                   for k, v in processed_data.items()}

            # Upload to DynamoDB
            self.dynamodb.put_item(
                TableName=self.TABLE_NAME,
                Item=item
            )
            
            logger.info(f"Successfully uploaded metadata for {h5_file_path}")
            return True
            
        except ClientError as e:
            logger.error(f"DynamoDB error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Failed to upload metadata: {str(e)}")
            return False