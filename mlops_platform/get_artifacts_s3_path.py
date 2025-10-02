"""
This module scans an S3 bucket for the first object key containing a specific substring.
It lists experiment prefixes under a root prefix, then scans each prefix concurrently
for the substring, stopping as soon as a match is found.

This is required only if the mlflow webhook's payload does not contain the experiment ID.
This module loses its relevance if the payload always contains the experiment ID. Please see
https://github.com/mlflow/mlflow/issues/17919 for more context.
"""

import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

# Will cause an error if one of the following env variables does not exist
S3_REGION_NAME = os.environ["S3_REGION_NAME"]
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
S3_HOST = os.environ["S3_HOST"]
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]

ROOT_PREFIX = "self_service_mlops/"  # Example hard-coded root prefix where experiments are stored
MAX_WORKERS = 4

session = boto3.session.Session(
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name=S3_REGION_NAME,
)
s3 = session.client(
    "s3", endpoint_url=S3_HOST if S3_HOST.startswith("http") else f"http://{S3_HOST}"
)


def list_experiment_prefixes():
    """
    Returns a list like ['self_service_mlops/12/', 'self_service_mlops/69/', ...]
    Uses Delimiter='/' to avoid listing all objects.
    """
    prefixes = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=ROOT_PREFIX, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            p = cp.get("Prefix")
            if p:
                prefixes.append(p)
    return prefixes


def scan_prefix_for_substr(prefix: str, substr: str, stop_event: threading.Event) -> str | None:
    """
    Scan all objects under a specific experiment prefix for substr.
    Short-circuits if stop_event is set, or returns the first matching key.
    """
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix):
        if stop_event.is_set():
            return None
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if substr in key:
                return key
    return None


def find_first_match() -> str | None:
    exp_prefixes = list_experiment_prefixes()
    if not exp_prefixes:
        return None

    stop_event = threading.Event()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(scan_prefix_for_substr, p, stop_event): p for p in exp_prefixes}
        for fut in as_completed(futures):
            key = fut.result()
            if key:
                stop_event.set()
                # Optionally: cancel others (not strictly necessary; they'll see stop_event or finish soon)
                return f"s3://{S3_BUCKET_NAME}/{key}"
    return None


if __name__ == "__main__":
    match_uri = find_first_match()
    if match_uri:
        print(match_uri)
    else:
        print("No matching key found.")
