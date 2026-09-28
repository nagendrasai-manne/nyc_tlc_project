import boto3
from botocore.exceptions import ClientError

# ── Configuration ─────────────────────────────────────────────────────────────
DATABASE_NAME = "nyc_tlc_db"
DATABASE_DESCRIPTION = "NYC TLC trip record data database"
REGION = "us-east-1"
# ──────────────────────────────────────────────────────────────────────────────


def get_bucket_name():
    """Derive the S3 bucket name using the AWS account number."""
    sts_client = boto3.client("sts")
    account_number = sts_client.get_caller_identity()["Account"]
    return f"nyc-tlc-{account_number}"


def create_glue_database():
    glue_client = boto3.client("glue", region_name=REGION)
    bucket_name = get_bucket_name()
    location_uri = f"s3://{bucket_name}/raw/hvfhv"

    # Check if database already exists
    try:
        glue_client.get_database(Name=DATABASE_NAME)
        print(f"Glue database already exists: {DATABASE_NAME}")
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            print(f"Error checking database: {e}")
            return

    # Create the database
    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": DATABASE_NAME,
                "Description": DATABASE_DESCRIPTION,
                "LocationUri": location_uri,
            }
        )
        print(f"✔ Glue database created: {DATABASE_NAME}")
        print(f"  Location: {location_uri}")
    except ClientError as e:
        print(f"✘ Failed to create database: {e}")


if __name__ == "__main__":
    create_glue_database()
