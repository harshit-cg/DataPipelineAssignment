import os
import argparse
import boto3
from botocore.exceptions import ClientError


def upload_file(local_path: str, bucket: str, key: str):
    s3 = boto3.client("s3")
    try:
        s3.upload_file(local_path, bucket, key)
        print(f"Uploaded {local_path} to s3://{bucket}/{key}")
        return True
    except ClientError as e:
        print(f"Failed to upload: {e}")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="Local file to upload")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--key", required=False, help="S3 object key (default: same basename)")
    args = parser.parse_args()

    key = args.key if args.key else os.path.basename(args.file)
    success = upload_file(args.file, args.bucket, key)
    if not success:
        raise SystemExit(1)
