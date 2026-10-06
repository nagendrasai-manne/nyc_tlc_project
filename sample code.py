#!/usr/bin/env python3

# Copyright © Sachin Chandrashekhar - Data Engineering Hub. All Rights Reserved.
#
# This code is provided as part of a paid training program.
# Unauthorized copying, distribution, or
# publication of this code—either in whole or in part—
# is strictly prohibited.
#
# This code may NOT be:
# - Uploaded to public repositories (GitHub, GitLab, etc.)
# - Shared with third parties
# - Used for commercial purposes
#
# Licensed for personal educational use only.
#
# If this code is found on a public repository or distributed without
# authorization, please notify: legal@dataengineeringhub.in

"""
NYC TLC Data Downloader for AWS Glue


Downloads taxi trip data from NYC TLC directly to S3 with partitioning
Looks for latest available month for the currnet year in current mode and 
downloads data 2 months behind due to NYC TLC's publication delay,
For historical mode, downloads all months except the latest available month.
This will help you run the job once in historical mode and then in current mode
to simulate incremental run.
Creates a .done file in S3 to trigger event-driven pipelines.

Run the glue crawler to create bronze tables in Glue Data Catalog after this job.

This job is to be scheduled so data is extracted incrementally every month.

ETA ~ 5 mins
"""

import os
import sys
import requests
from datetime import datetime
import logging
import boto3
from botocore.exceptions import ClientError
import tempfile
import json
from awsglue.utils import getResolvedOptions

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# S3 Configuration - Dynamic bucket name with account ID
AWS_REGION = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
ACCOUNT_ID = boto3.client('sts').get_caller_identity()['Account']
S3_BUCKET = f"mission-deh-hof-nyc-tlc-{ACCOUNT_ID}"
S3_PREFIX = "raw"

def get_s3_client():
    """Initialize S3 client"""
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        return s3_client
    except Exception as e:
        logger.error(f"Failed to create S3 client: {e}")
        raise

def download_and_upload_to_s3(url, s3_key, s3_client):
    """Download file and upload directly to S3"""
    try:
        logger.info(f"Downloading and uploading: {url} -> s3://{S3_BUCKET}/{s3_key}")
        
        # Check if file already exists in S3
        try:
            s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
            logger.info(f"File already exists in S3: {s3_key}")
            return True
        except ClientError as e:
            if int(e.response['Error']['Code']) != 404:
                raise
        
        # Download file
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        # Use temporary file for upload
        with tempfile.NamedTemporaryFile() as temp_file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    temp_file.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        progress = (downloaded / total_size) * 100
                        print(f"\rProgress: {progress:.1f}%", end='', flush=True)
            
            print()  # New line after progress
            temp_file.seek(0)
            
            # Upload to S3
            s3_client.upload_fileobj(temp_file, S3_BUCKET, s3_key)
            logger.info(f"Uploaded to S3: s3://{S3_BUCKET}/{s3_key} ({downloaded:,} bytes)")
            return True
        
    except Exception as e:
        logger.error(f"Failed to download/upload {url}: {e}")
        return False

def download_taxi_data_to_s3(data_type="fhvhv", start_year=2023, start_month=1, num_months=6, s3_client=None):
    """Download taxi data files directly to S3 with partitioning"""
    current_year = start_year
    current_month = start_month
    uploaded_files = []
    
    for i in range(num_months):
        month_str = f"{current_month:02d}"
        filename = f"{data_type}_tripdata_{current_year}-{month_str}.parquet"
        url = f"{BASE_URL}/{filename}"
        s3_key = f"{S3_PREFIX}/hvfhv/year={current_year}/month={month_str}/{filename}"
        
        if download_and_upload_to_s3(url, s3_key, s3_client):
            uploaded_files.append(s3_key)
        
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1
    
    return uploaded_files

def download_reference_data_to_s3(s3_client):
    """Download reference/lookup tables to S3"""
    zone_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
    s3_key = f"{S3_PREFIX}/zone/taxi_zone_lookup.csv"
    
    uploaded_files = []
    if download_and_upload_to_s3(zone_url, s3_key, s3_client):
        uploaded_files.append(s3_key)
    
    return uploaded_files

def find_latest_available_month():
    """Find the latest available month, checking current year first, then previous years"""
    current_year = datetime.now().year
    current_month = datetime.now().month
    
    # Check current year first
    logger.info(f"Checking for latest available data in {current_year}...")
    
    for month in range(current_month, 0, -1):
        month_str = f"{month:02d}"
        test_url = f"{BASE_URL}/fhvhv_tripdata_{current_year}-{month_str}.parquet"
        
        try:
            response = requests.head(test_url, timeout=10)
            if response.status_code == 200:
                logger.info(f"Latest available month found: {current_year}-{month_str}")
                return current_year, month
        except:
            continue
    
    # If no data found for current year, check previous year
    previous_year = current_year - 1
    logger.info(f"No data found for {current_year}, checking {previous_year}...")
    
    for month in range(12, 0, -1):
        month_str = f"{month:02d}"
        test_url = f"{BASE_URL}/fhvhv_tripdata_{previous_year}-{month_str}.parquet"
        
        try:
            response = requests.head(test_url, timeout=10)
            if response.status_code == 200:
                logger.info(f"Latest available month found: {previous_year}-{month_str}")
                return previous_year, month
        except:
            continue
    
    logger.warning(f"No data found for {current_year} or {previous_year}, defaulting to {previous_year}-01")
    return previous_year, 1

def get_s3_file_info(s3_keys, s3_client):
    """Display information about uploaded S3 files"""
    total_size = 0
    logger.info("\n=== Uploaded S3 Files ===")
    
    for s3_key in s3_keys:
        try:
            response = s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
            size = response['ContentLength']
            total_size += size
            size_mb = size / (1024 * 1024)
            logger.info(f"{os.path.basename(s3_key)}: {size_mb:.1f} MB")
        except Exception as e:
            logger.warning(f"Could not get info for {s3_key}: {e}")
    
    total_gb = total_size / (1024 * 1024 * 1024)
    logger.info(f"\nTotal uploaded: {total_gb:.2f} GB")
    logger.info(f"S3 Location: s3://{S3_BUCKET}/{S3_PREFIX}/")

def get_latest_available_data():
    """Get latest available month data configuration"""
    latest_year, latest_month = find_latest_available_month()
    
    logger.info(f"Configured for latest available month: {latest_year}-{latest_month:02d}")
    return latest_year, latest_month, 1  # year, start_month, num_months

def create_done_file(job_type, uploaded_files, s3_client):
    """Create .done file to trigger event-driven pipeline"""
    try:
        current_time = datetime.now()
        
        # Determine data period from uploaded files
        data_period = "unknown"
        if uploaded_files:
            # Extract year-month from first uploaded file path
            for file_path in uploaded_files:
                if "year=" in file_path and "month=" in file_path:
                    year_part = file_path.split("year=")[1].split("/")[0]
                    month_part = file_path.split("month=")[1].split("/")[0]
                    data_period = f"{year_part}-{month_part}"
                    break
        
        # Calculate total size
        total_size_bytes = 0
        for s3_key in uploaded_files:
            try:
                response = s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
                total_size_bytes += response['ContentLength']
            except:
                pass
        
        total_size_gb = round(total_size_bytes / (1024 * 1024 * 1024), 2)
        
        # Get job run ID from Glue context if available
        job_run_id = os.environ.get('AWS_GLUE_JOB_RUN_ID', 'unknown')
        
        # Create done file content
        done_content = {
            "ingestion_date": current_time.isoformat(),
            "job_type": job_type,
            "data_period": data_period,
            "taxi_types": ["fhvhv"],
            "total_files": len(uploaded_files),
            "total_size_gb": total_size_gb,
            "job_run_id": job_run_id,
            "latest_available_month": data_period,
            "publication_delay_months": 2,
            "duplicate_files_skipped": 0,
            "status": "SUCCESS"
        }
        
        # Create .done file with fixed name (gets overwritten each time)
        done_file_key = f"{S3_PREFIX}/ingestion.done"
        
        # Upload .done file to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=done_file_key,
            Body=json.dumps(done_content, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Created .done file: s3://{S3_BUCKET}/{done_file_key}")
        logger.info(f"Done file content: {json.dumps(done_content, indent=2)}")
        
    except Exception as e:
        logger.error(f"Failed to create .done file: {e}")
        # Don't fail the job if .done file creation fails
        pass

def main():
    """Main download function with jobtype parameter support"""
    # Get job parameters
    try:
        args = getResolvedOptions(sys.argv, ['jobtype'])
        job_type = args['jobtype'].lower()
    except:
        job_type = 'historical'  # default to historical
        logger.info("No jobtype parameter provided, defaulting to 'historical'")
    
    logger.info(f"Job type: {job_type}")
    
    s3_client = get_s3_client()
    
    if job_type == 'current':
        # Current mode - download only the latest available month
        latest_year, latest_month = find_latest_available_month()
        
        logger.info(f"Starting NYC TLC latest available data upload to S3 ({latest_year}-{latest_month:02d})")
        
        download_configs = [
            {"data_type": "fhvhv", "start_year": latest_year, "start_month": latest_month, "num_months": 1},
        ]
        
    else:
        # Historical mode - download all 2024 data + all available 2025 data except latest month
        current_year = datetime.now().year
        latest_year, latest_month = find_latest_available_month()
        
        download_configs = []
        
        # Download all 12 months of 2024
        logger.info("Adding 2024 full year data to download queue")
        download_configs.append({
            "data_type": "fhvhv", 
            "start_year": 2024, 
            "start_month": 1, 
            "num_months": 12
        })
        
        # Download available months of latest year except the latest month
        if latest_year > 2024 and latest_month > 1:
            months_to_download = latest_month - 1
            logger.info(f"Adding {latest_year} data (Jan to {months_to_download:02d}) to download queue")
            download_configs.append({
                "data_type": "fhvhv", 
                "start_year": latest_year, 
                "start_month": 1, 
                "num_months": months_to_download
            })
        
        logger.info(f"Starting NYC TLC historical data upload to S3 (2024 full year + {latest_year} partial)")
    
    logger.info(f"Target S3 bucket: s3://{S3_BUCKET}/{S3_PREFIX}/")
    
    all_s3_keys = []
    
    for config in download_configs:
        logger.info(f"Uploading {config['data_type']} taxi data to S3 (Year: {config['start_year']}, Months: {config['num_months']})...")
        s3_keys = download_taxi_data_to_s3(**config, s3_client=s3_client)
        all_s3_keys.extend(s3_keys)
    
    # Download reference data in both modes (it's small and doesn't change often)
    logger.info("Uploading reference data to S3...")
    ref_s3_keys = download_reference_data_to_s3(s3_client)
    all_s3_keys.extend(ref_s3_keys)
    
    get_s3_file_info(all_s3_keys, s3_client)
    
    # Create .done file for event-driven orchestration
    create_done_file(job_type, all_s3_keys, s3_client)
    
    logger.info(f"S3 upload complete! Job type: {job_type}")

if __name__ == "__main__":
    main()