import boto3
import gzip
import io
import json
import os
import subprocess
import tempfile
import uuid
from pathlib import Path
from typing import Optional, Dict, Iterable
from datetime import datetime

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


def _parse_block_path(block_path: str) -> tuple[str, str, str]:
    """Parse ``s3://bucket/tenant/block-id`` and return its components."""

    if not block_path.startswith("s3://"):
        raise ValueError("block path must start with s3://")

    path = block_path[len("s3://") :]
    parts = path.split("/", 2)
    if len(parts) < 3:
        raise ValueError(
            "block path must be in format s3://bucket/tenant/block-id"
        )
    return parts[0], parts[1], parts[2]


class s3MetricsDump:
    """Helper for dumping metrics from S3 blocks using ``dump_index``."""

    def __init__(self, bucket: str, tenant: str, dump_index_bin_path: str, aws_profile: str = "") -> None:
        self.bucket = bucket
        self.tenant = tenant
        self.aws_profile = aws_profile
        self.dump_index_bin_path = dump_index_bin_path

    def dump_metrics(
        self,
        bucket_index: pd.DataFrame,
        metric_name: str,
        lable_key: str,
        lable_value: list[str],
        output_path: str,
        min_time: datetime,
        max_time: datetime,
    ) -> pd.DataFrame:
        """Dump a metric across all blocks using the ``dump_index`` CLI.

        Parameters
        ----------
        bucket_index : pd.DataFrame
            DataFrame obtained from :func:`build_bucket_index`. It must contain a
            ``"block-id"`` column and optionally ``"block-path"`` providing the full
            S3 path for the block.
        metric_name : str
            Name of the metric to dump.
        lable_key : str
            Label key to filter on. If empty no filtering is applied.
        lable_value : list[str]
            One or more label values to match. If empty all values are returned.
        output_path : str
            Destination CSV file path for the combined output.
        min_time : datetime
            Minimum timestamp to include when dumping metrics.
        max_time : datetime
            Maximum timestamp to include when dumping metrics.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the combined metric data.
    """

        if "block-id" not in bucket_index.columns:
            raise ValueError("bucket_index must contain a 'block-id' column")

        label_key = lable_key
        label_values = lable_value or []
        if isinstance(label_values, str):
            label_values = [label_values]
        if not label_values:
            label_values = [None]

        results: list[pd.DataFrame] = []

        with tempfile.TemporaryDirectory() as workdir:
            for _, row in bucket_index.iterrows():
                block_id = row.get("block-id") or row.get("blockID") or row.get("id")
                if block_id is None:
                    continue

                row_bucket = self.bucket
                row_tenant = self.tenant

                if "bucket" in row:
                    row_bucket = row["bucket"]
                if "tenant" in row:
                    row_tenant = row["tenant"]

                if "block-path" in row:
                    row_bucket, row_tenant, _bid = _parse_block_path(str(row["block-path"]))
                    block_id = _bid

                block_path = f"s3://{row_bucket}/{row_tenant}/{block_id}"

                for value in label_values:
                    name = uuid.uuid4().hex
                    cmd = [
                        self.dump_index_bin_path,
                        "--block",
                        block_path,
                        "--metric-name",
                        metric_name,
                        "--output",
                        "csv",
                        "--working-dir",
                        workdir,
                        "--ouput-filename",
                        name,
                        "--start-time",
                        str(int(min_time.timestamp() * 1000)),
                        "--end-time",
                        str(int(max_time.timestamp() * 1000)),
                    ]

                    if self.aws_profile:
                        cmd.extend(["--aws-profile", self.aws_profile])

                    if label_key and value is not None:
                        cmd.extend(["--label-key", label_key, "--label-value", str(value)])

                    proc = subprocess.run(cmd, capture_output=True, text=True)
                    if proc.returncode != 0:
                        raise RuntimeError(proc.stderr.strip())

                    metric_dir = metric_name or "all-metrics"
                    tmp_file = Path(workdir) / row_bucket / row_tenant / block_id / metric_dir / f"{name}.csv"
                    if not tmp_file.exists():
                        # If no data is found, the CLI still creates a file with only the header.
                        continue
                    df = pd.read_csv(tmp_file)
                    results.append(df)

        if results:
            combined = pd.concat(results, ignore_index=True)
        else:
            combined = pd.DataFrame(columns=["series_labels", "timestamp", "value"])

        combined.to_csv(output_path, index=False)
        return combined
