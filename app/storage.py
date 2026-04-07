import hashlib
import logging
import os

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import HTTPException

logger = logging.getLogger(__name__)

MAX_UPLOAD_SIZE = int(os.getenv("MAX_UPLOAD_SIZE_BYTES", 52428800))  # 50 MB


def is_r2_configured() -> bool:
    """Return True if all required R2 environment variables are present."""
    return all([
        os.getenv("R2_ENDPOINT_URL"),
        os.getenv("R2_ACCESS_KEY_ID"),
        os.getenv("R2_SECRET_ACCESS_KEY"),
        os.getenv("R2_BUCKET_NAME"),
    ])


def get_s3_client():
    access_key = os.getenv("R2_ACCESS_KEY_ID")
    secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
    endpoint_url = os.getenv("R2_ENDPOINT_URL")
    region = os.getenv("R2_REGION", "auto")
    if not access_key or not secret_key:
        raise HTTPException(status_code=500, detail="Object storage credentials are not configured")
    if not endpoint_url:
        raise HTTPException(status_code=500, detail="Object storage endpoint is not configured")
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def upload_file_to_s3(file_bytes: bytes, storage_key: str, content_type: str) -> str:
    """Upload file bytes to S3 and return the storage key."""
    if not os.getenv("R2_BUCKET_NAME"):
        raise HTTPException(status_code=500, detail="Object storage is not configured")

    client = get_s3_client()
    try:
        client.put_object(
            Bucket=os.getenv("R2_BUCKET_NAME"),
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
    bucket = os.getenv("R2_BUCKET_NAME")
    if not bucket:
        return
    try:
        client = get_s3_client()
        client.delete_object(Bucket=bucket, Key=storage_key)
    except (BotoCoreError, ClientError) as exc:
        logger.error("S3 cleanup failed for key %s: %s", storage_key, exc)


def download_file_from_s3(storage_key: str) -> bytes:
    """Download an object from S3 and return its bytes."""
    bucket = os.getenv("R2_BUCKET_NAME")
    if not bucket:
        raise RuntimeError("Object storage is not configured")

    client = get_s3_client()
    try:
        response = client.get_object(Bucket=bucket, Key=storage_key)
        return response["Body"].read()
    except (BotoCoreError, ClientError) as exc:
        logger.error("S3 download failed for key %s: %s", storage_key, exc)
        raise RuntimeError(f"Failed to download file from storage: {exc}") from exc


def compute_sha256(file_bytes: bytes) -> str:
    """Return the hex-encoded SHA-256 hash of the given bytes."""
    return hashlib.sha256(file_bytes).hexdigest()
