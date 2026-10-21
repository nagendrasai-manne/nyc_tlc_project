import json
import os

import boto3
from botocore.exceptions import ClientError

# ── Lambda configuration ───────────────────────────────────────────────────────
DATABASE_NAME = os.environ.get("DATABASE_NAME", "nyc_tlc_db")
DATABASE_DESCRIPTION = os.environ.get(
    "DATABASE_DESCRIPTION", "NYC TLC trip record data database"
)
REGION = os.environ.get("AWS_REGION", "us-east-1")
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

    try:
        glue_client.get_database(Name=DATABASE_NAME)
        print(f"Glue database already exists: {DATABASE_NAME}")
        return location_uri, False
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            print(f"Error checking database: {e}")
            raise

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
        return location_uri, True
    except ClientError as e:
        print(f"✘ Failed to create database: {e}")
        raise


def lambda_handler(event, context):
    """AWS Lambda entry point that creates the Glue database if needed."""
    try:
        location_uri, created = create_glue_database()
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": (
                    "Glue database created" if created
                    else "Glue database already exists"
                ),
                "database_name": DATABASE_NAME,
                "location_uri": location_uri,
                "created": created,
            }),
        }
    except ClientError as exc:
        error = exc.response.get("Error", {})
        print(f"AWS Glue operation failed: {error}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Failed to create or validate Glue database",
                "database_name": DATABASE_NAME,
                "error_code": error.get("Code", "Unknown"),
                "error_message": error.get("Message", str(exc)),
            }),
        }
    except Exception as exc:
        print(f"Unhandled error while creating Glue database: {exc}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Unhandled error while creating Glue database",
                "error": str(exc),
            }),
        }


if __name__ == "__main__":
    print(lambda_handler({}, None))
