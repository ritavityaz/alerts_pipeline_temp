import asyncio
import os
import signal
import tempfile
import time
from datetime import datetime, timezone

from .config import (
    S3_BUCKET,
    CLOUDFRONT_DISTRIBUTION_ID,
    PIPELINE_RUN_INTERVAL_SECONDS,
    S3_RAW_ALERTS_PREFIX,
    s3_client,
    cf_client,
    get_current_date,
)
from .fetch import fetch_all_cities, fetch_latest, load_cities, load_geo_maps
from .storage import (
    latest_s3_date,
    load_all_days,
    load_day_local,
    load_day_s3,
    merge_by_rid,
    partition_by_day,
    s3_list_keys,
    s3_write_json,
    s3_write_parquet,
    save_day,
)
from .transform import transform
from .incidents import build_incidents
from .generate import (
    generate_alerts_parquet,
    generate_events_parquet,
    generate_incident_events_parquet,
    generate_incidents_parquet,
    generate_snapshot_json,
)


def fetch_alerts():
    """Fetch alerts — full fetch on first run or data gap, incremental otherwise."""
    s3_keys = s3_list_keys(S3_RAW_ALERTS_PREFIX)

    if not s3_keys:
        print("First run — fetching all cities...")
        cities = load_cities()# get array of all city names
        raw_alerts = asyncio.run(fetch_all_cities(cities)) #fetch up to 3k events per city
        for date_str, day_alerts in partition_by_day(raw_alerts).items():
            save_day(date_str, day_alerts)# save locally and to S3
        return

    gap_days = (get_current_date() - latest_s3_date(s3_keys)).days
    if gap_days > 1:
        print(f"Data gap detected ({gap_days} days) — fetching all cities...")
        cities = load_cities() # get array of all city names
        raw_alerts = asyncio.run(fetch_all_cities(cities))
    else:
        print("Fetching fresh data...")
        raw_alerts = asyncio.run(fetch_latest()) # fetch past 24h data

    for date_str, new_day in partition_by_day(raw_alerts).items():
        existing = load_day_local(date_str) or load_day_s3(date_str) or [] # get day's file
        merged, new_count = merge_by_rid(existing, new_day) # merge day's file w/ new data
        print(f"  {date_str}: {len(existing)} existing + {len(new_day)} fetched = {len(merged)} total ({new_count} new)")
        save_day(date_str, merged) # save locally and to S3


def generate_and_upload(df_typed, alerts_matched, incident_events, incident_summary):
    """Generate optimized files, upload to S3, and invalidate CloudFront."""
    zone_map, name_en_map = load_geo_maps()
    print(f"  Loaded geo mappings for {len(zone_map)} cities")

    # Legacy tables (preserved for continuity)
    alerts_pq = generate_alerts_parquet(df_typed, zone_map)
    events_pq = generate_events_parquet(alerts_matched, zone_map, name_en_map)
    snapshot = generate_snapshot_json(alerts_pq)
    s3_write_json("optimized/snapshot.json", snapshot)

    # New incident-based tables
    incident_events_pq = generate_incident_events_parquet(incident_events)
    incidents_pq = generate_incidents_parquet(incident_summary, zone_map, name_en_map)

    with tempfile.TemporaryDirectory() as tmp:
        files = {
            "optimized/alerts.parquet": alerts_pq,
            "optimized/events.parquet": events_pq,
            "optimized/incident_events.parquet": incident_events_pq,
            "optimized/incidents.parquet": incidents_pq,
        }
        for key, df in files.items():
            path = os.path.join(tmp, os.path.basename(key))
            df.write_parquet(path, compression="zstd")
            s3_write_parquet(key, path)

    cf_client.create_invalidation(
        DistributionId=CLOUDFRONT_DISTRIBUTION_ID,
        InvalidationBatch={
            "Paths": {
                "Quantity": 5,
                "Items": [
                    "/optimized/alerts.parquet",
                    "/optimized/events.parquet",
                    "/optimized/incident_events.parquet",
                    "/optimized/incidents.parquet",
                    "/optimized/snapshot.json",
                ],
            },
            "CallerReference": str(datetime.now(timezone.utc).timestamp()),
        },
    )
    print("CloudFront invalidation created")


def run_pipeline():
    print(f"[{datetime.now()}] Starting pipeline...")
    fetch_alerts()
    raw_alerts = load_all_days()

    print("Transforming (legacy)...")
    df_typed, alerts_matched = transform(raw_alerts)

    print("Building incidents...")
    incident_events, incident_summary = build_incidents(raw_alerts)

    print("Generating optimized files...")
    generate_and_upload(df_typed, alerts_matched, incident_events, incident_summary)
    print(f"[{datetime.now()}] Pipeline complete! Processed {len(raw_alerts)} alerts")


def main():
    """Run pipeline on a loop with graceful shutdown."""
    stop = False

    def handle_signal(signum, frame):
        nonlocal stop
        print(f"\nReceived signal {signum}, shutting down...")
        stop = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    print(f"[{datetime.now()}] Pipeline launched")

    while not stop:
        try:
            run_pipeline()
        except Exception as e:
            print(f"[{datetime.now()}] Pipeline error: {e}")

        if stop:
            break

        print(f"Sleeping {PIPELINE_RUN_INTERVAL_SECONDS}s until next run...")
        for _ in range(PIPELINE_RUN_INTERVAL_SECONDS):
            if stop:
                break
            time.sleep(1)

    print("Shutdown complete.")
