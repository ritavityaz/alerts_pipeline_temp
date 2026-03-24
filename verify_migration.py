"""Verify that all alerts from alerts_all.json exist in the daily parquet files on S3."""
import json
import io
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from pipeline.config import s3_client, S3_BUCKET, S3_RAW_ALERTS_PREFIX
from pipeline.storage import s3_list_keys
import polars as pl

# Load legacy JSON from S3
print("Loading alerts_all.json from S3...")
resp = s3_client.get_object(Bucket=S3_BUCKET, Key="raw/alerts_all.json")
legacy_alerts = json.loads(resp["Body"].read())
legacy_rids = {a["rid"] for a in legacy_alerts}
print(f"  Legacy JSON: {len(legacy_alerts)} alerts, {len(legacy_rids)} unique rids")

# Load all daily parquet files from S3
print("Loading daily parquet files from S3...")
keys = s3_list_keys(S3_RAW_ALERTS_PREFIX)
print(f"  Found {len(keys)} daily files")

parquet_rids = set()
for key in keys:
    resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    df = pl.read_parquet(io.BytesIO(resp["Body"].read()))
    parquet_rids.update(df["rid"].to_list())

print(f"  Parquet files: {len(parquet_rids)} unique rids")

# Compare
missing = legacy_rids - parquet_rids
if missing:
    print(f"\nMISSING: {len(missing)} rids from legacy JSON not found in parquet files!")
    for rid in list(missing)[:10]:
        print(f"  {rid}")
else:
    print(f"\nAll {len(legacy_rids)} legacy rids are present in the parquet files.")
