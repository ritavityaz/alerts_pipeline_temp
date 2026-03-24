import json
import os

import polars as pl

from .config import s3_client, S3_BUCKET, S3_RAW_ALERTS_PREFIX, LOCAL_RAW_ALERTS_DIR


def s3_exists(key):
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except Exception:
        return False


def s3_read_json(key):
    resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    return json.loads(resp["Body"].read())


def s3_write_json(key, data):
    body = json.dumps(data, ensure_ascii=False)
    s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"),
                         ContentType="application/json")
    print(f"Uploaded {key} ({len(body) / 1024:.0f} KB)")


def s3_write_parquet(key, local_path):
    """Upload a local Parquet file to S3 using multipart upload."""
    s3_client.upload_file(local_path, S3_BUCKET, key,
                          ExtraArgs={"ContentType": "application/octet-stream"})
    size_kb = os.path.getsize(local_path) / 1024
    print(f"Uploaded {key} ({size_kb:.0f} KB)")


def s3_list_keys(prefix):
    """List all object keys under a prefix."""
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return sorted(keys)


def partition_by_day(alerts):
    """Group alerts into {date_str: [alerts]} by alertDate."""
    by_day = {}
    for a in alerts:
        date_str = a.get("alertDate", "")[:10]
        if date_str:
            by_day.setdefault(date_str, []).append(a)
    return by_day


def load_day_local(date_str):
    """Load a day's alerts from local cache. Returns [] if not found."""
    path = os.path.join(LOCAL_RAW_ALERTS_DIR, f"{date_str}.parquet")
    if os.path.exists(path):
        return pl.read_parquet(path).to_dicts()
    return []


def load_day_s3(date_str):
    """Load a day's alerts from S3. Returns [] if not found."""
    key = f"{S3_RAW_ALERTS_PREFIX}{date_str}.parquet"
    try:
        resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        return pl.read_parquet(resp["Body"].read()).to_dicts()
    except Exception:
        return []


def save_day(date_str, alerts):
    """Save a day's alerts locally and upload to S3."""
    os.makedirs(LOCAL_RAW_ALERTS_DIR, exist_ok=True)
    path = os.path.join(LOCAL_RAW_ALERTS_DIR, f"{date_str}.parquet")
    df = pl.DataFrame(alerts)
    df.write_parquet(path, compression="zstd")
    key = f"{S3_RAW_ALERTS_PREFIX}{date_str}.parquet"
    s3_write_parquet(key, path)


def save_day_local(date_str, alerts):
    """Save a day's alerts locally only (no S3 upload)."""
    os.makedirs(LOCAL_RAW_ALERTS_DIR, exist_ok=True)
    path = os.path.join(LOCAL_RAW_ALERTS_DIR, f"{date_str}.parquet")
    pl.DataFrame(alerts).write_parquet(path, compression="zstd")


def load_all_days():
    """Load all daily parquet files, preferring local cache, falling back to S3."""
    os.makedirs(LOCAL_RAW_ALERTS_DIR, exist_ok=True)
    # Get the union of local and S3 dates
    local_dates = set()
    for f in os.listdir(LOCAL_RAW_ALERTS_DIR):
        if f.endswith(".parquet"):
            local_dates.add(f.replace(".parquet", ""))
    s3_dates = set()
    for key in s3_list_keys(S3_RAW_ALERTS_PREFIX):
        fname = key.removeprefix(S3_RAW_ALERTS_PREFIX)
        if fname.endswith(".parquet"):
            s3_dates.add(fname.replace(".parquet", ""))
    all_dates = sorted(local_dates | s3_dates)
    all_alerts = []
    for date_str in all_dates:
        if date_str in local_dates:
            all_alerts.extend(load_day_local(date_str))
        else:
            day_alerts = load_day_s3(date_str)
            if day_alerts:
                # Cache locally for next time
                save_day_local(date_str, day_alerts)
                all_alerts.extend(day_alerts)
    print(f"Loaded {len(all_alerts)} alerts across {len(all_dates)} days")
    return all_alerts
