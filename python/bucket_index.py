import boto3
import gzip
import io
import json
from typing import Optional

import pandas as pd


def build_bucket_index(bucket: str, tenant: str, aws_profile: Optional[str] = None, aws_region: Optional[str] = None) -> pd.DataFrame:
    """Return a bucket index DataFrame from S3 without saving locally.

    Parameters
    ----------
    bucket : str
        S3 bucket name.
    tenant : str
        Tenant prefix inside the bucket.
    aws_profile : str, optional
        AWS credentials profile to use.
    aws_region : str, optional
        AWS region of the bucket.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns ``["block-id", "min-time", "max-time"]``.
    """

    session_kwargs = {}
    if aws_profile:
        session_kwargs["profile_name"] = aws_profile
    session = boto3.Session(**session_kwargs)

    client_kwargs = {}
    if aws_region:
        client_kwargs["region_name"] = aws_region
    s3 = session.client("s3", **client_kwargs)

    key = f"{tenant}/bucket-index.json.gz"
    resp = s3.get_object(Bucket=bucket, Key=key)

    body = resp["Body"].read()
    with gzip.GzipFile(fileobj=io.BytesIO(body)) as gz:
        data = json.load(gz)

    blocks = []
    if isinstance(data, dict):
        if "blocks" in data:
            blocks = data["blocks"]
        else:
            for block_id, meta in data.items():
                if isinstance(meta, dict) and "minTime" in meta and "maxTime" in meta:
                    blocks.append({"block-id": block_id, "minTime": meta["minTime"], "maxTime": meta["maxTime"]})
    elif isinstance(data, list):
        blocks = data

    rows = []
    for b in blocks:
        block_id = b.get("blockID") or b.get("id") or b.get("ulid") or b.get("block-id")
        min_time = b.get("minTime") or b.get("min-time")
        max_time = b.get("maxTime") or b.get("max-time")
        if block_id is None or min_time is None or max_time is None:
            continue
        rows.append({"block-id": block_id, "min-time": min_time, "max-time": max_time})

    return pd.DataFrame(rows, columns=["block-id", "min-time", "max-time"])
