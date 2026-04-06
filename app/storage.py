import hashlib
import logging
import os

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import HTTPException

logger = logging.getLogger(__name__)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")

MAX_UPLOAD_SIZE = int(os.getenv("MAX_UPLOAD_SIZE_BYTES", 52428800))  # 50 MB


def get_s3_client():
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        raise HTTPException(status_code=500, detail="Object storage credentials are not configured")
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


def upload_file_to_s3(file_bytes: bytes, storage_key: str, content_type: str) -> str:
    """Upload file bytes to S3 and return the storage key."""
    if not AWS_S3_BUCKET:
        raise HTTPException(status_code=500, detail="Object storage is not configured")

    client = get_s3_client()
    try:
        client.put_object(
            Bucket=AWS_S3_BUCKET,
            Key=storage_key,
            Body=file_bytes,
            ContentType=content_type,
        )
    except (BotoCoreError, ClientError) as exc:
        logger.error("S3 upload failed for key %s: %s", storage_key, exc)
        raise HTTPException(status_code=500, detail="Failed to upload file to storage") from exc

    return storage_key


def delete_file_from_s3(storage_key: str) -> None:
    """Delete an object from S3. Errors are logged but not raised."""
    if not AWS_S3_BUCKET:
        return
    try:
        client = get_s3_client()
        client.delete_object(Bucket=AWS_S3_BUCKET, Key=storage_key)
    except (BotoCoreError, ClientError) as exc:
        logger.error("S3 cleanup failed for key %s: %s", storage_key, exc)


def download_file_from_s3(storage_key: str) -> bytes:
    """Download an object from S3 and return its bytes."""
    if not AWS_S3_BUCKET:
        raise RuntimeError("Object storage is not configured")

    client = get_s3_client()
    try:
        response = client.get_object(Bucket=AWS_S3_BUCKET, Key=storage_key)
        return response["Body"].read()
    except (BotoCoreError, ClientError) as exc:
        logger.error("S3 download failed for key %s: %s", storage_key, exc)
        raise RuntimeError(f"Failed to download file from storage: {exc}") from exc


def compute_sha256(file_bytes: bytes) -> str:
    """Return the hex-encoded SHA-256 hash of the given bytes."""
    return hashlib.sha256(file_bytes).hexdigest()
