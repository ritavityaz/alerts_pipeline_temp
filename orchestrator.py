import asyncio
import json
import os
import tempfile
from datetime import datetime, timezone

import polars as pl

from .config import (
    S3_BUCKET,
    CLOUDFRONT_DISTRIBUTION_ID,
    S3_RAW_ALERTS_PREFIX,
    s3_client,
    cf_client,
)
from .fetch import fetch_all_cities, fetch_latest, load_cities, load_geo_maps
from .storage import (
    load_all_days,
    load_day_local,
    load_day_s3,
    partition_by_day,
    s3_exists,
    s3_list_keys,
    s3_write_json,
    s3_write_parquet,
    save_day,
)
from .transform import transform
from .generate import (
    generate_alerts_parquet,
    generate_events_parquet,
    generate_snapshot_json,
)


def run_pipeline():
    print(f"[{datetime.now()}] Starting pipeline...")

    # Merge any legacy monolithic files into daily partitions.
    # Legacy files are never deleted — only read and merged.
    legacy_alerts = []
    local_legacy_parquet = os.path.join(os.path.dirname(__file__), "alerts_all.parquet")
    local_legacy_json = os.path.join(os.path.dirname(__file__), "alerts_all.json")

    if os.path.exists(local_legacy_parquet):
        print("Found local legacy parquet, merging into daily files...")
        legacy_alerts = pl.read_parquet(local_legacy_parquet).to_dicts()
        print(f"  Loaded {len(legacy_alerts)} alerts from local parquet")
    elif os.path.exists(local_legacy_json):
        print("Found local legacy JSON, merging into daily files...")
        with open(local_legacy_json, "r", encoding="utf-8") as f:
            legacy_alerts = json.load(f)
        print(f"  Loaded {len(legacy_alerts)} alerts from local JSON")
    elif s3_exists("raw/alerts_all.parquet"):
        print("Found S3 legacy parquet, merging into daily files...")
        tmp_path = os.path.join(tempfile.gettempdir(), "legacy_all.parquet")
        s3_client.download_file(S3_BUCKET, "raw/alerts_all.parquet", tmp_path)
        legacy_alerts = pl.read_parquet(tmp_path).to_dicts()
        os.unlink(tmp_path)
        print(f"  Loaded {len(legacy_alerts)} alerts from S3 parquet")
    elif s3_exists("raw/alerts_all.json"):
        print("Found S3 legacy JSON, merging into daily files...")
        tmp_path = os.path.join(tempfile.gettempdir(), "legacy_all.json")
        s3_client.download_file(S3_BUCKET, "raw/alerts_all.json", tmp_path)
        with open(tmp_path, "r", encoding="utf-8") as f:
            legacy_alerts = json.load(f)
        os.unlink(tmp_path)
        print(f"  Loaded {len(legacy_alerts)} alerts from S3 JSON")

    if legacy_alerts:
        for date_str, day_alerts in partition_by_day(legacy_alerts).items():
            existing = load_day_local(date_str) or load_day_s3(date_str)
            by_rid = {a["rid"]: a for a in existing}
            new_count = 0
            for a in day_alerts:
                if a["rid"] not in by_rid:
                    by_rid[a["rid"]] = a
                    new_count += 1
            if new_count > 0:
                save_day(date_str, list(by_rid.values()))
                print(f"  {date_str}: added {new_count} alerts from legacy data")

    # Check if we have any data at all
    has_daily = bool(s3_list_keys(S3_RAW_ALERTS_PREFIX))

    if not has_daily:
        # First run — no legacy files and no daily files
        print("First run — fetching all cities...")
        cities = load_cities()
        raw_alerts = asyncio.run(fetch_all_cities(cities))
        for date_str, day_alerts in partition_by_day(raw_alerts).items():
            save_day(date_str, day_alerts)
    else:
        # Incremental: fetch last 24h and merge into affected days
        new_alerts = asyncio.run(fetch_latest())
        new_by_day = partition_by_day(new_alerts)
        for date_str, new_day in new_by_day.items():
            existing = load_day_local(date_str) or load_day_s3(date_str)
            by_rid = {a["rid"]: a for a in existing}
            for a in new_day:
                by_rid[a["rid"]] = a
            merged = list(by_rid.values())
            print(f"  {date_str}: {len(existing)} existing + {len(new_day)} fetched = {len(merged)} total")
            save_day(date_str, merged)

    # Load full dataset for transform
    raw_alerts = load_all_days()

    # Transform
    print("Transforming...")
    df_typed, alerts_matched = transform(raw_alerts)

    # Generate optimized parquet files
    print("Generating optimized files...")
    zone_map, name_en_map = load_geo_maps()
    print(f"  Loaded geo mappings for {len(zone_map)} cities")
    alerts_pq = generate_alerts_parquet(df_typed, zone_map)
    events_pq = generate_events_parquet(alerts_matched, zone_map, name_en_map)
    snapshot = generate_snapshot_json(alerts_pq)
    s3_write_json("optimized/snapshot.json", snapshot)

    # Write parquet to temp files and upload
    with tempfile.TemporaryDirectory() as tmp:
        alerts_path = os.path.join(tmp, "alerts.parquet")
        events_path = os.path.join(tmp, "events.parquet")
        alerts_pq.write_parquet(alerts_path, compression="zstd")
        events_pq.write_parquet(events_path, compression="zstd")

        for key, path in [("optimized/alerts.parquet", alerts_path),
                          ("optimized/events.parquet", events_path)]:
            s3_write_parquet(key, path)

    # Invalidate CloudFront cache
    cf_client.create_invalidation(
        DistributionId=CLOUDFRONT_DISTRIBUTION_ID,
        InvalidationBatch={
            "Paths": {
                "Quantity": 3,
                "Items": [
                    "/optimized/alerts.parquet",
                    "/optimized/events.parquet",
                    "/optimized/snapshot.json",
                ],
            },
            "CallerReference": str(datetime.now(timezone.utc).timestamp()),
        },
    )
    print("CloudFront invalidation created")
    print(f"[{datetime.now()}] Pipeline complete! Processed {len(raw_alerts)} alerts")


