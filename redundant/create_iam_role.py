import json
import os

import boto3
from botocore.exceptions import ClientError

# ── Lambda configuration ───────────────────────────────────────────────────────
ROLE_NAME = os.environ.get("ROLE_NAME", "AWSGlueServiceRole-nyc-tlc")
# ──────────────────────────────────────────────────────────────────────────────

# Trust policy — allows Glue service to assume this role
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

# Managed policies to attach
MANAGED_POLICIES = [
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
]


def get_account_number():
    """Return the AWS account number for the current Lambda execution role."""
    sts_client = boto3.client("sts")
    return sts_client.get_caller_identity()["Account"]


def get_bucket_name():
    """Derive the S3 bucket name using the AWS account number."""
    return f"nyc-tlc-{get_account_number()}"


def create_iam_role():
    iam_client = boto3.client("iam")

    try:
        response = iam_client.get_role(RoleName=ROLE_NAME)
        print(f"IAM role already exists: {ROLE_NAME}")
        role_arn = response["Role"]["Arn"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntityException":
            print(f"Error checking role: {e}")
            raise
        try:
            response = iam_client.create_role(
                RoleName=ROLE_NAME,
                AssumeRolePolicyDocument=json.dumps(TRUST_POLICY),
                Description="IAM role for AWS Glue to access S3 and Glue services for NYC TLC project",
            )
            role_arn = response["Role"]["Arn"]
            print(f"✔ IAM role created: {ROLE_NAME}")
            print(f"  ARN: {role_arn}")
        except ClientError as e:
            print(f"✘ Failed to create role: {e}")
            raise

    for policy_arn in MANAGED_POLICIES:
        try:
            iam_client.attach_role_policy(
                RoleName=ROLE_NAME,
                PolicyArn=policy_arn,
            )
            print(f"  ✔ Attached policy: {policy_arn.split('/')[-1]}")
        except ClientError as e:
            print(f"  ✘ Failed to attach {policy_arn}: {e}")
            raise

    account_number = get_account_number()
    bucket_name = f"nyc-tlc-{account_number}"
    inline_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}",
                    f"arn:aws:s3:::{bucket_name}/*",
                ],
            }
        ],
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
        raise

    pass_role_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "iam:PassRole",
                "Resource": (
                    f"arn:aws:iam::{account_number}:"
                    "role/AWSGlueServiceRole-nyc-tlc"
                ),
                "Condition": {
                    "StringLike": {
                        "iam:PassedToService": "glue.amazonaws.com",
                    },
                },
            },
        ],
    }

    try:
        iam_client.put_role_policy(
            RoleName=ROLE_NAME,
            PolicyName="nyc-tlc-glue-pass-role",
            PolicyDocument=json.dumps(pass_role_policy),
        )
        print("  ✔ Attached inline Glue PassRole policy")
    except ClientError as e:
        print(f"  ✘ Failed to attach Glue PassRole policy: {e}")
        raise

    print(f"\n✔ IAM role ready: {ROLE_NAME}")
    return role_arn


def lambda_handler(event, context):
    """AWS Lambda entry point for creating or validating the Glue IAM role."""
    try:
        role_arn = create_iam_role()
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "IAM role ready",
                "role_name": ROLE_NAME,
                "role_arn": role_arn,
                "bucket_name": get_bucket_name(),
            }),
        }
    except ClientError as exc:
        error = exc.response.get("Error", {})
        print(f"AWS IAM operation failed: {error}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Failed to create or configure IAM role",
                "role_name": ROLE_NAME,
                "error_code": error.get("Code", "Unknown"),
                "error_message": error.get("Message", str(exc)),
            }),
        }
    except Exception as exc:
        print(f"Unhandled error while creating IAM role: {exc}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Unhandled error while creating IAM role",
                "error": str(exc),
            }),
        }


if __name__ == "__main__":
    print(lambda_handler({}, None))
