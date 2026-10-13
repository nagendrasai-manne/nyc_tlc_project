#!/usr/bin/env python3
"""
NYC TLC Data Downloader for AWS Glue

Downloads taxi trip data from NYC TLC directly to S3 with partitioning.
Supports two run modes via the 'jobtype' Glue job parameter:

  historical  – Downloads full 2024 data + all available months of the current
                year except the latest (to leave it for the first incremental run).
  current     – Downloads only the single latest available month per dataset.

After all uploads, writes a JSON .done file to S3 to trigger event-driven
pipelines (e.g. EventBridge → Glue crawler).

Run the Glue crawler to create bronze tables in Glue Data Catalog after this job.
Schedule in 'current' mode monthly for incremental ingestion.

ETA ~5 mins per dataset month.
"""

import os
import sys
import json
import logging
import tempfile
import time
from datetime import datetime
from typing import Optional, List, Tuple

import boto3
import requests
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions

# ── Logging setup ─────────────────────────────────────────────────────────────
# Glue pre-configures the root logger; grab a named logger and attach a handler
# explicitly so output is flushed to stdout → CloudWatch Logs.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s  %(levelname)-8s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(_handler)
# ──────────────────────────────────────────────────────────────────────────────

# ── Configuration ─────────────────────────────────────────────────────────────
BASE_URL         = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL  = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
ZONE_LOOKUP_FILE = "taxi_zone_lookup.csv"

AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
S3_BUCKET  = f"nyc-tlc-{ACCOUNT_ID}"
S3_PREFIX  = "raw"

CHUNK_SIZE       = 8192   # bytes per chunk when streaming downloads
MAX_RETRIES      = 3
RETRY_BACKOFF    = 2      # seconds; doubles each attempt (2 → 4 → 8)
MAX_PROBE_MONTHS = 6      # how far back to probe when detecting latest month

# Each dataset: (label, data_type, s3_folder)
#   label      – human-readable name for logs
#   data_type  – TLC filename prefix, e.g. "fhvhv_tripdata"
#   s3_folder  – folder under S3_PREFIX, e.g. "hvfhv"
DATASETS = [
    ("HVFHV",       "fhvhv_tripdata",  "hvfhv"),
    # ("Yellow Taxi", "yellow_tripdata", "yellow"),
    # ("Green Taxi",  "green_tripdata",  "green"),
]
# ──────────────────────────────────────────────────────────────────────────────


# ── S3 client ─────────────────────────────────────────────────────────────────

def get_s3_client():
    """Initialise and return an S3 client."""
    try:
        return boto3.client("s3", region_name=AWS_REGION)
    except Exception as e:
        logger.error("Failed to create S3 client: %s", e)
        raise


# ── Latest-month detection ────────────────────────────────────────────────────

def _month_offset(year: int, month: int, offset: int) -> Tuple[int, int]:
    """Return (year, month) shifted back by *offset* months."""
    month -= offset
    while month < 1:
        month += 12
        year  -= 1
    return year, month


def find_latest_available_month(data_type: str = "fhvhv_tripdata") -> Tuple[int, int]:
    """
    Probe the TLC CDN for *data_type* starting from the current month and
    walking backwards (up to MAX_PROBE_MONTHS) until an HTTP 200 is found.
    Falls back to 2 months behind today if nothing is found.
    Returns (year, month).
    """
    today = datetime.now()
    logger.info("Probing TLC CDN for latest available month (%s) ...", data_type)

    for offset in range(0, MAX_PROBE_MONTHS + 1):
        year, month = _month_offset(today.year, today.month, offset)
        file_name   = f"{data_type}_{year}-{month:02d}.parquet"
        url         = f"{BASE_URL}/{file_name}"

        try:
            resp = requests.head(url, timeout=10)
            if resp.status_code == 200:
                logger.info("Latest available month: %d-%02d", year, month)
                return year, month
            elif resp.status_code == 404:
                logger.info("  %d-%02d not yet published (404)", year, month)
            else:
                logger.warning("  Unexpected HTTP %s for %s", resp.status_code, file_name)
        except requests.RequestException as e:
            logger.warning("  HEAD request failed for %s: %s", file_name, e)

    year, month = _month_offset(today.year, today.month, 2)
    logger.warning(
        "Could not detect latest month after %d probes – falling back to %d-%02d",
        MAX_PROBE_MONTHS, year, month,
    )
    return year, month


# ── Download helpers ──────────────────────────────────────────────────────────

def download_to_tempfile(url: str, label: str) -> Optional[tempfile.SpooledTemporaryFile]:
    """
    Stream *url* into a SpooledTemporaryFile (stays in memory up to 64 MB,
    spills to disk only if larger) with exponential-backoff retries.
    Returns the open temp file seeked to 0 on success, or None on failure.
    The caller must close the file.
    """
    logger.info("──── Starting download: %s ────", label)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, stream=True, timeout=180)

            if response.status_code == 404:
                logger.warning("Not found (404) – skipping %s", label)
                return None

            if response.status_code != 200:
                raise requests.HTTPError(f"HTTP {response.status_code} for {label}")

            tmp   = tempfile.SpooledTemporaryFile(max_size=64 * 1_048_576)
            total = 0

            total_size     = int(response.headers.get("content-length", 0))
            total_size_mb  = total_size / 1_048_576
            last_log_pct   = -1   # track last logged percentage milestone

            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    tmp.write(chunk)
                    total += len(chunk)
                    if total_size > 0:
                        pct = int(total / total_size * 100)
                        # Log every 10 % milestone to avoid flooding logs
                        if pct // 10 > last_log_pct // 10:
                            last_log_pct = pct
                            logger.info(
                                "  [%s]  %d%%  (%.1f / %.1f MB)",
                                label, pct, total / 1_048_576, total_size_mb,
                            )

            tmp.seek(0)
            logger.info(
                "Download complete  [%s]  %.1f MB total",
                label, total / 1_048_576,
            )
            return tmp

        except (requests.RequestException, requests.HTTPError) as e:
            if attempt == MAX_RETRIES:
                logger.error("All %d attempts failed for %s: %s", MAX_RETRIES, label, e)
                return None
            wait = RETRY_BACKOFF * (2 ** (attempt - 1))
            logger.warning(
                "Attempt %d/%d failed for %s (%s) – retrying in %ds",
                attempt, MAX_RETRIES, label, e, wait,
            )
            time.sleep(wait)

    return None


def download_file_with_retry(url: str, label: str) -> Optional[bytes]:
    """Download a small file (e.g. CSV) into memory with exponential-backoff retries."""
    logger.info("Downloading %s ...", label)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=180)

            if response.status_code == 404:
                logger.warning("Not found (404) – skipping %s", label)
                return None

            if response.status_code != 200:
                raise requests.HTTPError(f"HTTP {response.status_code} for {label}")

            logger.info("Downloaded %s  (%.1f MB)", label, len(response.content) / 1_048_576)
            return response.content

        except (requests.RequestException, requests.HTTPError) as e:
            if attempt == MAX_RETRIES:
                logger.error("All %d attempts failed for %s: %s", MAX_RETRIES, label, e)
                return None
            wait = RETRY_BACKOFF * (2 ** (attempt - 1))
            logger.warning(
                "Attempt %d/%d failed for %s (%s) – retrying in %ds",
                attempt, MAX_RETRIES, label, e, wait,
            )
            time.sleep(wait)

    return None


# ── S3 helpers ────────────────────────────────────────────────────────────────

def s3_key_exists(s3_client, s3_key: str) -> bool:
    """Return True if *s3_key* already exists in S3_BUCKET."""
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def upload_fileobj_to_s3(s3_client, s3_key: str, fileobj) -> None:
    """Upload an open file-like object to S3 using multipart streaming."""
    try:
        s3_client.upload_fileobj(fileobj, S3_BUCKET, s3_key)
        logger.info("Uploaded  →  s3://%s/%s", S3_BUCKET, s3_key)
    except ClientError as e:
        logger.error("Upload failed for %s: %s", s3_key, e)
        raise


def upload_bytes_to_s3(s3_client, s3_key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
    """Upload raw bytes to S3."""
    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=data, ContentType=content_type)
        logger.info("Uploaded  →  s3://%s/%s", S3_BUCKET, s3_key)
    except ClientError as e:
        logger.error("Upload failed for %s: %s", s3_key, e)
        raise


def get_s3_file_info(s3_keys: List[str], s3_client) -> float:
    """
    Log size info for each uploaded S3 key.
    Returns total size in bytes.
    """
    total_bytes = 0
    logger.info("=== Uploaded S3 Files ===")

    for s3_key in s3_keys:
        try:
            response  = s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
            size      = response["ContentLength"]
            total_bytes += size
            logger.info("  %s  (%.1f MB)", os.path.basename(s3_key), size / 1_048_576)
        except Exception as e:
            logger.warning("  Could not get info for %s: %s", s3_key, e)

    logger.info("Total uploaded: %.2f GB", total_bytes / 1_073_741_824)
    logger.info("S3 location: s3://%s/%s/", S3_BUCKET, S3_PREFIX)
    return total_bytes


# ── Upload logic ──────────────────────────────────────────────────────────────

def download_and_upload_to_s3(url: str, s3_key: str, s3_client, label: str) -> bool:
    """
    Download *url* and upload to *s3_key*.
    Skips if the key already exists.
    Returns True on success (including skip), False on failure.
    """
    if s3_key_exists(s3_client, s3_key):
        logger.info("Already exists – skipping %s", label)
        return True

    tmp = download_to_tempfile(url, label)
    if tmp is None:
        return False

    try:
        upload_fileobj_to_s3(s3_client, s3_key, tmp)
        return True
    except ClientError:
        return False
    finally:
        tmp.close()


def download_taxi_data_to_s3(
    data_type: str,
    s3_folder: str,
    start_year: int,
    start_month: int,
    num_months: int,
    s3_client,
) -> List[str]:
    """
    Download *num_months* months of *data_type* parquet files starting at
    *start_year*/*start_month* and upload them to S3 with Hive partitioning.
    Returns a list of successfully uploaded S3 keys.
    """
    uploaded: List[str] = []
    year, month = start_year, start_month

    for i in range(num_months):
        month_str = f"{month:02d}"
        file_name = f"{data_type}_{year}-{month_str}.parquet"
        url       = f"{BASE_URL}/{file_name}"
        s3_key    = f"{S3_PREFIX}/{s3_folder}/year={year}/month={month_str}/{file_name}"

        logger.info(
            "── Month %d/%d  [%s]  %d-%s ──",
            i + 1, num_months, data_type, year, month_str,
        )

        if download_and_upload_to_s3(url, s3_key, s3_client, file_name):
            uploaded.append(s3_key)

        month += 1
        if month > 12:
            month = 1
            year += 1

    return uploaded


def download_reference_data_to_s3(s3_client) -> List[str]:
    """
    Download the taxi zone lookup CSV and upload to S3.
    Skips if already present.
    Returns a list of successfully uploaded S3 keys.
    """
    logger.info("--- Taxi Zone Lookup ---")
    s3_key    = f"{S3_PREFIX}/zone_lookup/{ZONE_LOOKUP_FILE}"
    uploaded: List[str] = []

    if s3_key_exists(s3_client, s3_key):
        logger.info("Already exists – skipping %s", ZONE_LOOKUP_FILE)
        return uploaded

    data = download_file_with_retry(ZONE_LOOKUP_URL, ZONE_LOOKUP_FILE)
    if data is None:
        return uploaded

    try:
        upload_bytes_to_s3(s3_client, s3_key, data, content_type="text/csv")
        uploaded.append(s3_key)
    except ClientError:
        pass

    return uploaded


# ── Done file ─────────────────────────────────────────────────────────────────

def create_done_file(job_type: str, uploaded_files: List[str], total_bytes: float, s3_client) -> None:
    """Write a JSON .done file to S3 to signal job completion."""
    now = datetime.now()

    # Extract data period from first partitioned key
    data_period = "unknown"
    for path in uploaded_files:
        if "year=" in path and "month=" in path:
            year_part  = path.split("year=")[1].split("/")[0]
            month_part = path.split("month=")[1].split("/")[0]
            data_period = f"{year_part}-{month_part}"
            break

    done_content = {
        "ingestion_date":            now.isoformat(),
        "job_type":                  job_type,
        "data_period":               data_period,
        "datasets":                  [d[0] for d in DATASETS],
        "total_files":               len(uploaded_files),
        "total_size_gb":             round(total_bytes / 1_073_741_824, 2),
        "job_run_id":                os.environ.get("AWS_GLUE_JOB_RUN_ID", "unknown"),
        "publication_delay_months":  2,
        "status":                    "SUCCESS",
    }

    s3_key = f"{S3_PREFIX}/ingestion.done"
    try:
        upload_bytes_to_s3(
            s3_client, s3_key,
            json.dumps(done_content, indent=2).encode("utf-8"),
            content_type="application/json",
        )
        logger.info("Done file content: %s", json.dumps(done_content, indent=2))
    except Exception as e:
        logger.error("Failed to create .done file: %s", e)
        # Non-fatal — don't fail the job over a missing sentinel file


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    # ── Resolve job type parameter ────────────────────────────────────────────
    try:
        args     = getResolvedOptions(sys.argv, ["jobtype"])
        job_type = args["jobtype"].lower()
    except Exception:
        job_type = "historical"
        logger.info("No 'jobtype' parameter provided – defaulting to 'historical'")

    logger.info("Job type        : %s", job_type)
    logger.info("Target bucket   : s3://%s", S3_BUCKET)
    logger.info("Datasets        : %s", [d[0] for d in DATASETS])
    logger.info("MAX_RETRIES     : %d", MAX_RETRIES)
    logger.info("MAX_PROBE_MONTHS: %d", MAX_PROBE_MONTHS)

    s3_client = get_s3_client()

    # ── Build per-dataset download configs ───────────────────────────────────
    download_configs = []   # list of dicts passed to download_taxi_data_to_s3

    for label, data_type, s3_folder in DATASETS:
        latest_year, latest_month = find_latest_available_month(data_type)

        if job_type == "current":
            # Current mode: only the single latest available month
            logger.info("[%s] current mode → %d-%02d only", label, latest_year, latest_month)
            download_configs.append({
                "data_type":   data_type,
                "s3_folder":   s3_folder,
                "start_year":  latest_year,
                "start_month": latest_month,
                "num_months":  1,
            })

        else:
            # Historical mode: full 2025 + current year up to (but not including) latest month
            logger.info("[%s] historical mode → full 2025 + partial %d", label, latest_year)

            # Full 2024
            download_configs.append({
                "data_type":   data_type,
                "s3_folder":   s3_folder,
                "start_year":  2025,
                "start_month": 1,
                "num_months":  12,
            })

            # Current year up to latest_month - 1 (leave latest for first incremental run)
            if latest_year > 2025 and latest_month > 1:
                months_to_download = latest_month - 1
                logger.info(
                    "[%s] adding %d Jan–%02d to queue",
                    label, latest_year, months_to_download,
                )
                download_configs.append({
                    "data_type":   data_type,
                    "s3_folder":   s3_folder,
                    "start_year":  latest_year,
                    "start_month": 1,
                    "num_months":  months_to_download,
                })

    # ── Execute uploads ───────────────────────────────────────────────────────
    all_s3_keys: List[str] = []

    for config in download_configs:
        logger.info(
            "Uploading %s data (start %d-%02d, %d month(s)) ...",
            config["data_type"], config["start_year"], config["start_month"], config["num_months"],
        )
        keys = download_taxi_data_to_s3(**config, s3_client=s3_client)
        all_s3_keys.extend(keys)

    # Zone lookup reference (always, both modes)
    all_s3_keys.extend(download_reference_data_to_s3(s3_client))

    # ── Summary + done file ───────────────────────────────────────────────────
    total_bytes = get_s3_file_info(all_s3_keys, s3_client)
    create_done_file(job_type, all_s3_keys, total_bytes, s3_client)

    logger.info("S3 upload complete! Job type: %s", job_type)


if __name__ == "__main__":
    main()
