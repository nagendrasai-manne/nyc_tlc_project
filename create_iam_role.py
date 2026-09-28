import boto3
import json
from botocore.exceptions import ClientError

# ── Configuration ─────────────────────────────────────────────────────────────
ROLE_NAME = "AWSGlueServiceRole-nyc-tlc-data-exploration"
# ──────────────────────────────────────────────────────────────────────────────

# Trust policy — allows Glue service to assume this role
TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "glue.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}

# Managed policies to attach
MANAGED_POLICIES = [
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",  # Glue permissions
    "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",           # S3 read access
]


def get_bucket_name():
    """Derive the S3 bucket name using the AWS account number."""
    sts_client = boto3.client("sts")
    account_number = sts_client.get_caller_identity()["Account"]
    return f"nyc-tlc-{account_number}"


def create_iam_role():
    iam_client = boto3.client("iam")

    # Check if role already exists
    try:
        response = iam_client.get_role(RoleName=ROLE_NAME)
        print(f"IAM role already exists: {ROLE_NAME}")
        print(f"  ARN: {response['Role']['RoleArn']}")
        return response["Role"]["RoleArn"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntityException":
            print(f"Error checking role: {e}")
            return None

    # Create the role
    try:
        response = iam_client.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(TRUST_POLICY),
            Description="IAM role for AWS Glue to access S3 and Glue services for NYC TLC project",
        )
        role_arn = response["Role"]["RoleArn"]
        print(f"✔ IAM role created: {ROLE_NAME}")
        print(f"  ARN: {role_arn}")
    except ClientError as e:
        print(f"✘ Failed to create role: {e}")
        return None

    # Attach managed policies
    for policy_arn in MANAGED_POLICIES:
        try:
            iam_client.attach_role_policy(
                RoleName=ROLE_NAME,
                PolicyArn=policy_arn,
            )
            print(f"  ✔ Attached policy: {policy_arn.split('/')[-1]}")
        except ClientError as e:
            print(f"  ✘ Failed to attach {policy_arn}: {e}")

    # Add inline policy to allow access specifically to the nyc-tlc bucket
    bucket_name = get_bucket_name()
    inline_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}",
                    f"arn:aws:s3:::{bucket_name}/*"
                ]
            }
        ]
    }

    try:
        iam_client.put_role_policy(
            RoleName=ROLE_NAME,
            PolicyName="nyc-tlc-s3-access",
            PolicyDocument=json.dumps(inline_policy),
        )
        print(f"  ✔ Attached inline S3 policy for bucket: {bucket_name}")
    except ClientError as e:
        print(f"  ✘ Failed to attach inline policy: {e}")

    print(f"\n✔ IAM role ready: {ROLE_NAME}")
    return role_arn


if __name__ == "__main__":
    create_iam_role()
