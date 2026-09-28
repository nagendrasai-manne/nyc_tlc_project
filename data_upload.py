import boto3
import requests
from botocore.exceptions import ClientError

# ── Configuration ─────────────────────────────────────────────────────────────
# To download a single month : set START and END to the same year/month
#   e.g. START_YEAR=2024, START_MONTH=6, END_YEAR=2024, END_MONTH=6
# To download a full year    : set START_MONTH=1, END_MONTH=12
# To download multiple years : adjust all four values accordingly
START_YEAR  = 2026
START_MONTH = 1     # 1–12
END_YEAR    = 2026
END_MONTH   = 1     # 1–12
# ──────────────────────────────────────────────────────────────────────────────

# HVFHV data is available from February 2019 onwards
HVFHV_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def get_bucket_name():
    """Derive the S3 bucket name using the AWS account number."""
    sts_client = boto3.client("sts")
    account_number = sts_client.get_caller_identity()["Account"]
    return f"nyc-tlc-{account_number}"


def upload_to_s3(s3_client, bucket_name, file_name, data):
    """Upload file bytes to S3."""
    s3_key = f"raw/hvfhv/{file_name}"
    try:
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=data)
        print(f"  ✔ Uploaded to s3://{bucket_name}/{s3_key}")
    except ClientError as e:
        print(f"  ✘ Upload failed for {file_name}: {e}")


def download_and_upload(year, month, bucket_name, s3_client):
    """Download one HVFHV parquet file and upload it to S3."""
    file_name = f"fhvhv_tripdata_{year}-{month:02d}.parquet"
    url = f"{HVFHV_BASE_URL}/{file_name}"

    print(f"Downloading {file_name} ...")
    try:
        response = requests.get(url, timeout=120)
        if response.status_code == 200:
            print(f"  ✔ Downloaded ({len(response.content) / 1_048_576:.1f} MB)")
            upload_to_s3(s3_client, bucket_name, file_name, response.content)
        elif response.status_code == 404:
            print(f"  ⚠ Not found (404) – skipping {file_name}")
        else:
            print(f"  ✘ HTTP {response.status_code} for {file_name}")
    except requests.RequestException as e:
        print(f"  ✘ Request error for {file_name}: {e}")


def main():
    bucket_name = get_bucket_name()
    s3_client = boto3.client("s3", region_name="us-east-1")

    print(f"Target bucket : s3://{bucket_name}")
    print(f"Range         : {START_YEAR}-{START_MONTH:02d} → {END_YEAR}-{END_MONTH:02d}\n")

    for year in range(START_YEAR, END_YEAR + 1):
        # Determine month boundaries for this year
        m_start = START_MONTH if year == START_YEAR else 1
        m_end   = END_MONTH   if year == END_YEAR   else 12

        # HVFHV data starts from February 2019
        if year == 2019 and m_start < 2:
            m_start = 2

        for month in range(m_start, m_end + 1):
            download_and_upload(year, month, bucket_name, s3_client)

    print("\nDone.")


if __name__ == "__main__":
    main()
