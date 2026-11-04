#!/usr/bin/env python3
"""One-shot MinIO bucket bootstrap for the Stage 2 stack.

Runs inside the custom Airflow image (boto3 is already installed via
apache-airflow-providers-amazon) after the minio service starts. Waits
a bounded number of attempts for MinIO to accept S3 calls, creates
MINIO_BUCKET when absent, and is a no-op when it already exists —
safe to rerun on every `docker compose up`. Never logs credentials.
"""

from __future__ import annotations

import os
import sys
import time

import boto3
from botocore.exceptions import BotoCoreError, ClientError

ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
BUCKET = os.environ["MINIO_BUCKET"]
ATTEMPTS = 10
DELAY_SECONDS = 3

NOT_FOUND_CODES = {"404", "NoSuchBucket", "NotFound"}
AUTH_CODES = {"403", "AccessDenied", "SignatureDoesNotMatch", "InvalidAccessKeyId"}


def _error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))


def main() -> int:
    client = boto3.client(
        "s3",
        endpoint_url=f"http://{ENDPOINT}",
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",  # MinIO ignores the region; boto3 needs one
    )

    # None = reachability never confirmed; True/False = answered.
    bucket_exists: bool | None = None
    last_error: Exception | None = None
    for attempt in range(1, ATTEMPTS + 1):
        try:
            client.head_bucket(Bucket=BUCKET)
            bucket_exists = True
            break
        except ClientError as exc:
            code = _error_code(exc)
            if code in NOT_FOUND_CODES:
                bucket_exists = False
                break
            last_error = exc
            if code in AUTH_CODES:
                print(
                    "ERROR: MinIO rejected the credentials from .env "
                    "(check MINIO_ROOT_USER/MINIO_ROOT_PASSWORD)",
                    file=sys.stderr,
                )
                return 1
        except BotoCoreError as exc:  # connection refused, timeouts, ...
            last_error = exc
        print(f"minio not ready (attempt {attempt}/{ATTEMPTS})", flush=True)
        time.sleep(DELAY_SECONDS)

    if bucket_exists is None:
        print(
            f"ERROR: MinIO at {ENDPOINT} did not answer after "
            f"{ATTEMPTS} attempts ({last_error})",
            file=sys.stderr,
        )
        return 1

    if bucket_exists:
        print(f'bucket "{BUCKET}" already exists — nothing to do', flush=True)
        return 0

    try:
        client.create_bucket(Bucket=BUCKET)
    except ClientError as exc:
        if _error_code(exc) == "BucketAlreadyOwnedByYou":
            print(f'bucket "{BUCKET}" already exists — nothing to do', flush=True)
            return 0
        print(
            f"ERROR: failed to create bucket {BUCKET!r}: {exc}",
            file=sys.stderr,
        )
        return 1

    print(f'created bucket "{BUCKET}"', flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
