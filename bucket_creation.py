import boto3
from botocore.exceptions import ClientError


def create_bucket():
    # Get AWS account number
    sts_client = boto3.client("sts")
    account_number = sts_client.get_caller_identity()["Account"]

    bucket_name = f"nyc-tlc-{account_number}"
    region = "us-east-1"

    s3_client = boto3.client("s3", region_name=region)

    # Check if bucket already exists
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"S3 bucket already exists: {bucket_name}")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            # Bucket does not exist, create it
            try:
                # us-east-1 does not accept a LocationConstraint
                if region == "us-east-1":
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={"LocationConstraint": region},
                    )
                print(f"S3 bucket created: {bucket_name}")
            except ClientError as create_error:
                print(f"Failed to create bucket: {create_error}")
        else:
            print(f"Error checking bucket: {e}")


if __name__ == "__main__":
    create_bucket()
