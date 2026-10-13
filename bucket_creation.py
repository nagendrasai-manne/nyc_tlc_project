import logging

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def create_bucket(region="us-east-1"):
    """Create the account's NYC TLC bucket if it does not already exist."""
    sts_client = boto3.client("sts", region_name=region)
    account_number = sts_client.get_caller_identity()["Account"]
    bucket_name = f"nyc-tlc-{account_number}"
    s3_client = boto3.client("s3", region_name=region)

    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info("S3 bucket already exists: %s", bucket_name)
        return {"bucket_name": bucket_name, "status": "already_exists"}
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")
        if error_code not in {"404", "NoSuchBucket", "NotFound"}:
            raise

    create_parameters = {"Bucket": bucket_name}
    if region != "us-east-1":
        create_parameters["CreateBucketConfiguration"] = {
            "LocationConstraint": region
        }

    try:
        s3_client.create_bucket(**create_parameters)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") != "BucketAlreadyOwnedByYou":
            raise
        logger.info(
            "S3 bucket was created by another concurrent invocation: %s",
            bucket_name,
        )
        return {"bucket_name": bucket_name, "status": "already_exists"}

    logger.info("S3 bucket created: %s", bucket_name)
    return {"bucket_name": bucket_name, "status": "created"}


def lambda_handler(event, _context):
    """AWS Lambda entry point; returns fields consumed by the Step Functions workflow."""
    if not isinstance(event, dict):
        raise ValueError("Lambda event must be a JSON object.")

    region = event.get("region", "us-east-1")
    if not isinstance(region, str) or not region.strip():
        raise ValueError("'region' must be a non-empty string.")

    return create_bucket(region=region)


if __name__ == "__main__":
    print(create_bucket())
