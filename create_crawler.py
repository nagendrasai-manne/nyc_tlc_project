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
GLUE_IAM_ROLE = os.environ.get("GLUE_IAM_ROLE", "AWSGlueServiceRole-nyc-tlc")

# Define each crawler as (name, s3_prefix, description)
CRAWLERS = [
    (
        "nyc-tlc-crawler",
        "raw/hvfhv/",
        "Crawls NYC TLC HVFHV parquet files from S3",
    ),
    (
        "zone-crawler-for-nyc-tlc",
        "raw/zone_lookup/",
        "Crawls NYC TLC zone look up csv files from S3",
    ),
    (
        "nyc-tlc-curated-crawler",
        "curated/hvfhv/",
        "Crawls NYC TLC curated HVFHV data from S3",
    ),
    (
        "nyc-tlc-aggregated-crawler",
        "aggregated/",
        "Crawls NYC TLC aggregated data from S3",
    ),
]
# ──────────────────────────────────────────────────────────────────────────────

TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "glue.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }
    ],
}

MANAGED_POLICIES = [
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
]


def get_account_number():
    sts_client = boto3.client("sts")
    return sts_client.get_caller_identity()["Account"]


def get_bucket_name(account_number):
    return f"nyc-tlc-{account_number}"


def create_glue_database(bucket_name):
    """Create the Glue database if it does not already exist."""
    glue_client = boto3.client("glue", region_name=REGION)
    location_uri = f"s3://{bucket_name}/raw/hvfhv"

    try:
        glue_client.get_database(Name=DATABASE_NAME)
        print(f"Glue database already exists: {DATABASE_NAME}")
        return location_uri, False
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            raise

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


def ensure_iam_role(bucket_name, account_number):
    """Create the Glue IAM role if needed and ensure its policies are attached."""
    iam_client = boto3.client("iam")

    try:
        response = iam_client.get_role(RoleName=GLUE_IAM_ROLE)
        print(f"IAM role already exists: {GLUE_IAM_ROLE}")
        role_arn = response["Role"]["Arn"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise
        response = iam_client.create_role(
            RoleName=GLUE_IAM_ROLE,
            AssumeRolePolicyDocument=json.dumps(TRUST_POLICY),
            Description="Glue service role for NYC TLC project",
        )
        role_arn = response["Role"]["Arn"]
        print(f"✔ IAM role created: {GLUE_IAM_ROLE}")
        print(f"  ARN: {role_arn}")

    for policy_arn in MANAGED_POLICIES:
        iam_client.attach_role_policy(RoleName=GLUE_IAM_ROLE, PolicyArn=policy_arn)
        print(f"  ✔ Attached: {policy_arn.split('/')[-1]}")

    inline_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}",
                    f"arn:aws:s3:::{bucket_name}/*",
                ],
            }
        ],
    }
    iam_client.put_role_policy(
        RoleName=GLUE_IAM_ROLE,
        PolicyName="nyc-tlc-s3-access",
        PolicyDocument=json.dumps(inline_policy),
    )
    print(f"  ✔ Inline S3 policy attached for bucket: {bucket_name}")

    pass_role_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "iam:PassRole",
                "Resource": (
                    f"arn:aws:iam::{account_number}:role/{GLUE_IAM_ROLE}"
                ),
                "Condition": {
                    "StringLike": {
                        "iam:PassedToService": "glue.amazonaws.com",
                    },
                },
            }
        ],
    }
    iam_client.put_role_policy(
        RoleName=GLUE_IAM_ROLE,
        PolicyName="nyc-tlc-glue-pass-role",
        PolicyDocument=json.dumps(pass_role_policy),
    )
    print(f"  ✔ Inline Glue PassRole policy attached for role: {GLUE_IAM_ROLE}")

    return role_arn


def create_crawler(glue_client, bucket_name, crawler_name, s3_prefix, description):
    """Create a Glue crawler if needed; return whether it was created."""
    s3_target_path = f"s3://{bucket_name}/{s3_prefix}"

    try:
        glue_client.get_crawler(Name=crawler_name)
        print(f"Crawler already exists: {crawler_name}")
        return False
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            raise

    glue_client.create_crawler(
        Name=crawler_name,
        Role=GLUE_IAM_ROLE,
        DatabaseName=DATABASE_NAME,
        Description=description,
        Targets={"S3Targets": [{"Path": s3_target_path}]},
        SchemaChangePolicy={
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "LOG",
        },
        RecrawlPolicy={"RecrawlBehavior": "CRAWL_EVERYTHING"},
        Configuration='{"Version":1.0,"CrawlerOutput":{"Partitions":{"AddOrUpdateBehavior":"InheritFromTable"}}}',
    )
    print(f"✔ Crawler created: {crawler_name}")
    print(f"  Target  : {s3_target_path}")
    print(f"  Database: {DATABASE_NAME}")
    return True


def lambda_handler(event, context):
    """Ensure the Glue role and crawler definitions exist without starting them."""
    account_number = get_account_number()
    bucket_name = get_bucket_name(account_number)
    print(f"Bucket  : s3://{bucket_name}")
    print(f"Database: {DATABASE_NAME}\n")

    try:
        role_arn = ensure_iam_role(bucket_name, account_number)
        database_location, database_created = create_glue_database(bucket_name)
        glue_client = boto3.client("glue", region_name=REGION)
        results = []
        errors = []

        for crawler_name, s3_prefix, description in CRAWLERS:
            print(f"\n── {crawler_name} ──")
            try:
                created = create_crawler(
                    glue_client, bucket_name, crawler_name, s3_prefix, description
                )
                if not created:
                    results.append({
                        "crawler_name": crawler_name,
                        "created": False,
                        "status": "already_exists",
                    })
                    continue

                results.append({
                    "crawler_name": crawler_name,
                    "created": True,
                    "status": "created",
                })
            except ClientError as exc:
                error = exc.response.get("Error", {})
                print(f"Failed to configure crawler {crawler_name}: {error}")
                errors.append({
                    "crawler_name": crawler_name,
                    "error_code": error.get("Code", "Unknown"),
                    "error_message": error.get("Message", str(exc)),
                })

        status_code = 500 if errors else 200
        all_crawlers_exist = results and all(
            result["status"] == "already_exists" for result in results
        )
        return {
            "statusCode": status_code,
            "body": json.dumps({
                "message": (
                    "One or more crawlers failed"
                    if errors
                    else "Crawler already created"
                    if all_crawlers_exist
                    else "Glue crawlers are ready"
                ),
                "database_name": DATABASE_NAME,
                "database_created": database_created,
                "database_location": database_location,
                "bucket_name": bucket_name,
                "role_name": GLUE_IAM_ROLE,
                "role_arn": role_arn,
                "crawlers": results,
                "errors": errors,
            }),
        }
    except ClientError as exc:
        error = exc.response.get("Error", {})
        print(f"AWS operation failed: {error}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Failed to prepare Glue IAM role or crawlers",
                "error_code": error.get("Code", "Unknown"),
                "error_message": error.get("Message", str(exc)),
            }),
        }
    except Exception as exc:
        print(f"Unhandled error while preparing Glue crawlers: {exc}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Unhandled error while preparing Glue crawlers",
                "error": str(exc),
            }),
        }


if __name__ == "__main__":
    print(lambda_handler({}, None))
