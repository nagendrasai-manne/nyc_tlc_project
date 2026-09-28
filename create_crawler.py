import boto3
import json
import time
from botocore.exceptions import ClientError

# ── Configuration ─────────────────────────────────────────────────────────────
CRAWLER_NAME   = "nyc-tlc-hvfhv-crawler"
DATABASE_NAME  = "nyc_tlc_db"
S3_DATA_PREFIX = "hvfhv/"
REGION         = "us-east-1"
GLUE_IAM_ROLE  = "AWSGlueServiceRole-nyc-tlc"
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
    "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
]


def get_bucket_name():
    sts_client = boto3.client("sts")
    account_number = sts_client.get_caller_identity()["Account"]
    return f"nyc-tlc-{account_number}"


# ── Step 1: IAM Role ──────────────────────────────────────────────────────────

def ensure_iam_role(bucket_name):
    """Create the Glue IAM role if it doesn't already exist. Returns the role ARN."""
    iam_client = boto3.client("iam")

    # Check if role already exists
    try:
        response = iam_client.get_role(RoleName=GLUE_IAM_ROLE)
        print(f"IAM role already exists: {GLUE_IAM_ROLE}")
        return response["Role"].get("Arn") or response["Role"].get("RoleArn")
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            print(f"Error checking IAM role: {e}")
            return None

    # Create the role
    try:
        response = iam_client.create_role(
            RoleName=GLUE_IAM_ROLE,
            AssumeRolePolicyDocument=json.dumps(TRUST_POLICY),
            Description="Glue service role for NYC TLC project",
        )
        role_arn = response["Role"].get("Arn") or response["Role"].get("RoleArn")
        print(f"✔ IAM role created: {GLUE_IAM_ROLE}")
        print(f"  ARN: {role_arn}")
    except ClientError as e:
        print(f"✘ Failed to create IAM role: {e}")
        return None

    # Attach managed policies
    for policy_arn in MANAGED_POLICIES:
        try:
            iam_client.attach_role_policy(RoleName=GLUE_IAM_ROLE, PolicyArn=policy_arn)
            print(f"  ✔ Attached: {policy_arn.split('/')[-1]}")
        except ClientError as e:
            print(f"  ✘ Failed to attach {policy_arn}: {e}")

    # Inline policy scoped to the project bucket
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
    try:
        iam_client.put_role_policy(
            RoleName=GLUE_IAM_ROLE,
            PolicyName="nyc-tlc-s3-access",
            PolicyDocument=json.dumps(inline_policy),
        )
        print(f"  ✔ Inline S3 policy attached for bucket: {bucket_name}")
    except ClientError as e:
        print(f"  ✘ Failed to attach inline policy: {e}")

    return role_arn


# ── Step 2: Crawler ───────────────────────────────────────────────────────────

def create_crawler(glue_client, bucket_name):
    """Create the Glue crawler if it doesn't already exist."""
    s3_target_path = f"s3://{bucket_name}/{S3_DATA_PREFIX}"

    try:
        glue_client.get_crawler(Name=CRAWLER_NAME)
        print(f"Crawler already exists: {CRAWLER_NAME}")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            print(f"Error checking crawler: {e}")
            return False

    try:
        glue_client.create_crawler(
            Name=CRAWLER_NAME,
            Role=GLUE_IAM_ROLE,
            DatabaseName=DATABASE_NAME,
            Description="Crawls NYC TLC HVFHV parquet files from S3",
            Targets={"S3Targets": [{"Path": s3_target_path}]},
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "LOG",
            },
            RecrawlPolicy={"RecrawlBehavior": "CRAWL_EVERYTHING"},
            Configuration='{"Version":1.0,"CrawlerOutput":{"Partitions":{"AddOrUpdateBehavior":"InheritFromTable"}}}',
        )
        print(f"✔ Crawler created: {CRAWLER_NAME}")
        print(f"  Target  : {s3_target_path}")
        print(f"  Database: {DATABASE_NAME}")
        return True
    except ClientError as e:
        print(f"✘ Failed to create crawler: {e}")
        return False


def run_crawler(glue_client):
    """Start the crawler and wait for it to finish."""
    try:
        glue_client.start_crawler(Name=CRAWLER_NAME)
        print(f"\n▶ Crawler started: {CRAWLER_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "CrawlerRunningException":
            print("Crawler is already running.")
        else:
            print(f"✘ Failed to start crawler: {e}")
            return

    print("  Waiting for crawler to complete", end="", flush=True)
    while True:
        time.sleep(15)
        response = glue_client.get_crawler(Name=CRAWLER_NAME)
        state = response["Crawler"]["State"]
        print(".", end="", flush=True)
        if state == "READY":
            last_crawl = response["Crawler"].get("LastCrawl", {})
            status = last_crawl.get("Status", "UNKNOWN")
            print(f"\n✔ Crawler finished with status: {status}")
            if "ErrorMessage" in last_crawl:
                print(f"  Error: {last_crawl['ErrorMessage']}")
            break


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    bucket_name = get_bucket_name()
    print(f"Bucket  : s3://{bucket_name}")
    print(f"Database: {DATABASE_NAME}\n")

    # Step 1: ensure IAM role exists
    role_arn = ensure_iam_role(bucket_name)
    if not role_arn:
        print("Aborting: could not create IAM role.")
        return

    # Step 2: create and run crawler
    glue_client = boto3.client("glue", region_name=REGION)
    created = create_crawler(glue_client, bucket_name)
    if created:
        run_crawler(glue_client)
        print("\nDone. Check AWS Glue → Databases → nyc_tlc_db for the new tables.")


if __name__ == "__main__":
    main()
