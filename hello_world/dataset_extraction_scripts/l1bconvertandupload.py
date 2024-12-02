from typing import Dict, List, Tuple, Optional
import os
import json
import boto3
import logging
import subprocess
import concurrent.futures
import time
from botocore.config import Config
from utils.metadata_handler import MetadataHandler

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class INSAT3DProcessor:
    """Handles INSAT-3D HDF5 data conversion using GDAL's virtual filesystem"""
    
    def __init__(self, input_path: str, output_bucket: str, max_workers: int = 5):
        self.input_path = f"/vsis3/{input_path}"
        self.output_bucket = output_bucket
        self.max_workers = max_workers
        self.start_time = time.time()
        
        # Extract filename without extension for destination folder
        self.filename = os.path.splitext(os.path.basename(input_path))[0]
        
        # Get region from environment variable or default to ap-south-1
        self.region = os.environ.get('AWS_DEFAULT_REGION', 'ap-south-1')
        
        # Configure S3 client with region
        self.s3_client = boto3.client('s3', 
            region_name=self.region,
            config=Config(
                max_pool_connections=50,
                retries={'max_attempts': 3},
                connect_timeout=60,
                read_timeout=60
            )
        )

        # Initialize metadata handler with correct region
        self.metadata_handler = MetadataHandler(region=self.region)

        # Set GDAL environment variables
        os.environ.update({
            'GDAL_DISABLE_READDIR_ON_OPEN': 'EMPTY_DIR',
            'VSI_CACHE': 'TRUE',
            'VSI_CACHE_SIZE': '5242880',
            'GDAL_HTTP_TIMEOUT': '3600',
            'CPL_VSIL_CURL_ALLOWED_EXTENSIONS': '.tif,.tiff,.h5',
            'GDAL_CACHEMAX': '256',
            'VSI_MALLOC_TRIM_THRESHOLD_BYTES': '5000000',
            'GDAL_MAX_DATASET_POOL_SIZE': '256',
            'CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE': 'YES'
        })

    def _log_performance(self, operation: str) -> None:
        """Log operation duration"""
        duration = time.time() - self.start_time
        logger.info(f"{operation} completed in {duration:.2f} seconds")

    def _run_command(self, command: List[str], timeout: int = 600) -> Tuple[bool, str]:
        """Execute command with increased timeout"""
        try:
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
                timeout=timeout  # Increased timeout
            )
            return True, result.stdout
        except subprocess.TimeoutExpired:
            logger.error(f"Command timed out after {timeout}s: {' '.join(command)}")
            return False, "Timeout"
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {e.stderr}")
            return False, e.stderr

    def _get_subdatasets(self) -> Dict[str, str]:
        """Get subdatasets from HDF5 file"""
        try:
            # Use full path to gdalinfo
            gdalinfo_path = "/usr/bin/gdalinfo"
            if not os.path.exists(gdalinfo_path):
                gdalinfo_path = f"{os.environ.get('LAMBDA_TASK_ROOT', '')}/miniconda3/bin/gdalinfo"

            cmd = [gdalinfo_path, self.input_path]
            success, output = self._run_command(cmd)
            
            if not success:
                return {}
            
            subdatasets = {}
            for line in output.splitlines():
                if 'SUBDATASET' in line and 'NAME' in line and '://IMG_' in line:
                    path = line.split('=')[1].strip()
                    band_name = path.split('://IMG_')[1].split('\"')[0]
                    subdatasets[band_name] = path.strip('\"')
            
            logger.info(f"Found {len(subdatasets)} subdatasets")
            return subdatasets
            
        except Exception as e:
            logger.error(f"Failed to get subdatasets: {str(e)}")
            return {}

    def process_band(self, band_info: Tuple[str, str]) -> Optional[str]:
        """Process single band with optimized settings"""
        band_name, subdataset_path = band_info
        temp_file = f"/tmp/{band_name}_cog.tiff"
        
        # Use filename as folder prefix
        s3_key = f"{self.filename}/{band_name}_cog.tiff"

        try:
            # Use full path to gdal_translate
            gdal_translate_path = "/usr/bin/gdal_translate"
            if not os.path.exists(gdal_translate_path):
                gdal_translate_path = f"{os.environ.get('LAMBDA_TASK_ROOT', '')}/miniconda3/bin/gdal_translate"

            translate_cmd = [
                gdal_translate_path,
                '-of', 'COG',
                '-co', 'TILED=YES',
                '-co', 'BLOCKXSIZE=256',
                '-co', 'BLOCKYSIZE=256',
                '-co', 'COMPRESS=DEFLATE',
                '-co', 'NUM_THREADS=ALL_CPUS',
                '--config', 'GDAL_CACHEMAX', '256',
                subdataset_path,
                temp_file
            ]

            success, _ = self._run_command(translate_cmd)
            if not success:
                return None

            # Upload to S3 with filename-based path
            self.s3_client.upload_file(
                temp_file,
                self.output_bucket,
                s3_key,
                ExtraArgs={'ACL': 'bucket-owner-full-control'}
            )

            logger.info(f"Successfully processed and uploaded {s3_key}")
            return f"s3://{self.output_bucket}/{s3_key}"

        except Exception as e:
            logger.error(f"Failed to process band {band_name}: {str(e)}")
            return None

        finally:
            # Cleanup
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def process(self) -> Dict[str, str]:
        """Process metadata and bands concurrently"""
        self.start_time = time.time()
        results = {}

        # Handle metadata first
        logger.info(f"Extracting metadata from {self.input_path}")
        self.metadata_handler.upload_metadata(self.input_path)

        # Continue with existing band processing...
        subdatasets = self._get_subdatasets()
        if not subdatasets:
            return results

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_band = {
                executor.submit(self.process_band, (band, path)): band 
                for band, path in subdatasets.items()
            }
            
            for future in concurrent.futures.as_completed(future_to_band):
                band = future_to_band[future]
                try:
                    if result := future.result():
                        results[band] = result
                except Exception as e:
                    logger.error(f"Band {band} failed: {str(e)}")

        self._log_performance("Total processing")
        return results

def process_file(event, context):
    """Lambda handler with performance monitoring"""
    start_time = time.time()
    try:
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        source_key = event['Records'][0]['s3']['object']['key']
        destination_bucket = os.environ['DESTINATION_BUCKET']

        input_path = f"{source_bucket}/{source_key}"
        logger.info(f"Processing file: {input_path}")

        processor = INSAT3DProcessor(
            input_path=input_path,
            output_bucket=destination_bucket,
            max_workers=10  # Should be less than or equal to max_pool_connections
        )
        
        results = processor.process()

        if not results:
            raise Exception("No output files generated")

        duration = time.time() - start_time
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processing completed successfully',
                'processed_bands': list(results.keys()),
                'duration_seconds': duration
            })
        }

    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
