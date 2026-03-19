import json
import os
import csv
import asyncio
import time
import signal
import tempfile
import aiohttp
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import polars as pl
import boto3

BUCKET = os.environ.get("S3_BUCKET", "alerts-dashboard-data")
CF_DISTRIBUTION = os.environ.get("CF_DISTRIBUTION", "E28MP73WOLCLYQ")
API_URL = "https://alerts-history.oref.org.il/Shared/Ajax/GetAlarmsHistory.aspx"
CUTOFF = datetime(2026, 2, 26, tzinfo=ZoneInfo("Asia/Jerusalem"))
CONCURRENCY = 10
INTERVAL = int(os.environ.get("INTERVAL_SECONDS", "3600"))  # default 1 hour

s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-2"))
cf = boto3.client("cloudfront", region_name=os.environ.get("AWS_REGION", "us-east-2"))


# ── Fetch ──────────────────────────────────────────────────────


API_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Referer": "https://www.oref.org.il/",
    "X-Requested-With": "XMLHttpRequest",
}


async def fetch_api(session, params):
    """Fetch alerts from the Pikud HaOref API."""
    async with session.get(API_URL, params=params, headers=API_HEADERS) as resp:
        if resp.status != 200:
            print(f"  API returned status {resp.status} for {params}")
            return []
        text = (await resp.text()).strip()
        if not text:
            return []
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            print(f"  Failed to parse JSON for {params}: {text[:200]}")
            return []


async def fetch_latest():
    """Fetch last 24 hours of alerts (single request)."""
    async with aiohttp.ClientSession() as session:
        data = await fetch_api(session, {"lang": "he", "mode": "1"})
    print(f"Fetched {len(data)} alerts (last 24h)")
    return data


async def fetch_all_cities(cities):
    """Fetch last month per city (initialization). Returns deduplicated alerts."""
    all_alerts = {}
    sem = asyncio.Semaphore(CONCURRENCY)

    async def fetch_city(session, city, idx):
        async with sem:
            data = await fetch_api(session, {
                "lang": "he",
                "mode": "3",
                "city_0": city,
            })
            for a in data:
                rid = a.get("rid")
                if rid:
                    all_alerts[rid] = a
            if idx % 100 == 0:
                print(f"  Fetched {idx}/{len(cities)} cities, {len(all_alerts)} unique alerts so far")

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_city(session, city, i) for i, city in enumerate(cities)]
        await asyncio.gather(*tasks)

    print(f"Initialization complete: {len(all_alerts)} unique alerts from {len(cities)} cities")
    return list(all_alerts.values())


def load_cities():
    """Load city list from bundled cities.csv."""
    cities = []
    csv_path = os.path.join(os.path.dirname(__file__), "cities.csv")
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            if row:
                cities.append(row[0])
    return cities


def load_geo_maps():
    """Load city→zone_en and city→name_en mappings from zones.geojson on S3."""
    resp = s3.get_object(Bucket=BUCKET, Key="optimized/zones.geojson")
    gj = json.loads(resp["Body"].read())
    zone_map = {}
    name_en_map = {}
    for feat in gj["features"]:
        p = feat["properties"]
        zone_map[p["name_he"]] = p.get("zone_en", "")
        name_en_map[p["name_he"]] = p.get("name_en", "")
    return zone_map, name_en_map


# ── S3 helpers ─────────────────────────────────────────────────


def s3_exists(key):
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:
        return False


def s3_read_json(key):
    resp = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(resp["Body"].read())


def s3_write_json(key, data):
    body = json.dumps(data, ensure_ascii=False)
    s3.put_object(Bucket=BUCKET, Key=key, Body=body.encode("utf-8"),
                  ContentType="application/json")
    print(f"Uploaded {key} ({len(body) / 1024:.0f} KB)")


RAW_PREFIX = "raw/alerts/"
LOCAL_RAW_DIR = os.path.join(os.path.dirname(__file__), "raw_alerts")


def s3_write_parquet(key, local_path):
    """Upload a local Parquet file to S3 using multipart upload."""
    s3.upload_file(local_path, BUCKET, key,
                   ExtraArgs={"ContentType": "application/octet-stream"})
    size_kb = os.path.getsize(local_path) / 1024
    print(f"Uploaded {key} ({size_kb:.0f} KB)")


def s3_list_keys(prefix):
    """List all object keys under a prefix."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
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
    path = os.path.join(LOCAL_RAW_DIR, f"{date_str}.parquet")
    if os.path.exists(path):
        return pl.read_parquet(path).to_dicts()
    return []


def load_day_s3(date_str):
    """Load a day's alerts from S3. Returns [] if not found."""
    key = f"{RAW_PREFIX}{date_str}.parquet"
    try:
        resp = s3.get_object(Bucket=BUCKET, Key=key)
        return pl.read_parquet(resp["Body"].read()).to_dicts()
    except Exception:
        return []


def save_day(date_str, alerts):
    """Save a day's alerts locally and upload to S3."""
    os.makedirs(LOCAL_RAW_DIR, exist_ok=True)
    path = os.path.join(LOCAL_RAW_DIR, f"{date_str}.parquet")
    df = pl.DataFrame(alerts)
    df.write_parquet(path, compression="zstd")
    key = f"{RAW_PREFIX}{date_str}.parquet"
    s3_write_parquet(key, path)


def load_all_days():
    """Load all daily parquet files, preferring local cache, falling back to S3."""
    os.makedirs(LOCAL_RAW_DIR, exist_ok=True)
    # Get the union of local and S3 dates
    local_dates = set()
    for f in os.listdir(LOCAL_RAW_DIR):
        if f.endswith(".parquet"):
            local_dates.add(f.replace(".parquet", ""))
    s3_dates = set()
    for key in s3_list_keys(RAW_PREFIX):
        fname = key.removeprefix(RAW_PREFIX)
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


def save_day_local(date_str, alerts):
    """Save a day's alerts locally only (no S3 upload)."""
    os.makedirs(LOCAL_RAW_DIR, exist_ok=True)
    path = os.path.join(LOCAL_RAW_DIR, f"{date_str}.parquet")
    pl.DataFrame(alerts).write_parquet(path, compression="zstd")


# ── Transform ──────────────────────────────────────────────────


def transform(raw_alerts):
    """
    1. Type threats
    2. Classify events
    3. Match resolutions (2-pass asof join)
    4. Match warnings (backward asof join)
    Returns (df_typed, alerts_matched)
    """
    if not raw_alerts:
        raise ValueError("No alerts fetched — check API or city list")

    df = pl.DataFrame(raw_alerts).unique()
    print(f"  DataFrame columns: {df.columns}, rows: {df.height}")

    # Ensure category is integer for comparison
    df = df.with_columns(pl.col("category").cast(pl.Int64))

    # Type threats + classify events
    df_typed = df.with_columns(
        pl.when(pl.col("category") == 1).then(pl.lit("missiles"))
        .when(pl.col("category") == 2).then(pl.lit("drones"))
        .when(pl.col("category") == 10).then(pl.lit("terrorists"))
        .when(pl.col("category_desc").str.contains("רקטות וטילים")).then(pl.lit("missiles"))
        .when(pl.col("category_desc").str.contains("כלי טיס עוין")).then(pl.lit("drones"))
        .when(pl.col("category_desc").str.contains("מחבלים")).then(pl.lit("terrorists"))
        .otherwise(pl.lit("not_specified"))
        .alias("threat_type"),
        pl.when(pl.col("category").is_in([1, 2, 10])).then(pl.lit("alert"))
        .when(pl.col("category") == 14).then(pl.lit("early_warning"))
        .when(pl.col("category") == 13).then(pl.lit("resolved"))
        .otherwise(pl.lit("other"))
        .alias("event_type"),
    )

    df_typed = df_typed.with_columns(
        pl.col("alertDate").str.to_datetime("%Y-%m-%dT%H:%M:%S", time_zone="Asia/Jerusalem").alias("ts")
    )
    alerts = df_typed.filter(pl.col("event_type") == "alert").sort("data", "ts")
    warnings = df_typed.filter(pl.col("event_type") == "early_warning").sort("data", "ts")
    resolutions = df_typed.filter(pl.col("event_type") == "resolved").sort("data", "ts")

    print(f"  Alerts: {alerts.height}, Warnings: {warnings.height}, Resolutions: {resolutions.height}")

    # Pass 1: match resolutions by location + threat type
    res_typed = resolutions.filter(pl.col("threat_type") != "not_specified")
    pass1 = alerts.join_asof(
        res_typed.select("data", "threat_type",
                         pl.col("rid").alias("resolved_rid"),
                         pl.col("ts").alias("resolved_ts")),
        left_on="ts", right_on="resolved_ts",
        by=["data", "threat_type"],
        strategy="forward",
        tolerance=timedelta(hours=3),
    )

    # Pass 2: unmatched alerts try generic resolutions
    matched = pass1.filter(pl.col("resolved_ts").is_not_null())
    unmatched = pass1.filter(pl.col("resolved_ts").is_null()).drop("resolved_rid", "resolved_ts")

    res_generic = resolutions.filter(pl.col("threat_type") == "not_specified")
    pass2 = unmatched.join_asof(
        res_generic.select("data",
                           pl.col("rid").alias("resolved_rid"),
                           pl.col("ts").alias("resolved_ts")),
        left_on="ts", right_on="resolved_ts",
        by="data",
        strategy="forward",
        tolerance=timedelta(hours=3),
    )

    with_res = pl.concat([matched, pass2])

    # Match warnings (backward)
    alerts_matched = with_res.join_asof(
        warnings.select("data",
                        pl.col("rid").alias("warning_rid"),
                        pl.col("ts").alias("warning_ts")),
        left_on="ts", right_on="warning_ts",
        by="data",
        strategy="backward",
        tolerance=timedelta(hours=0.5),
    )

    res_count = alerts_matched.filter(pl.col("resolved_ts").is_not_null()).height
    warn_count = alerts_matched.filter(pl.col("warning_ts").is_not_null()).height
    print(f"  Matched: {res_count} resolved, {warn_count} warnings out of {alerts_matched.height}")

    return df_typed, alerts_matched


# ── Generate optimized files ───────────────────────────────────


def generate_alerts_parquet(df_typed, zone_map):
    """Generate alerts.parquet — one row per city/hour/category with count."""
    alerts = df_typed.filter(
        pl.col("event_type") == "alert",
        pl.col("ts") >= pl.lit(CUTOFF),
    )

    grouped = (
        alerts
        .with_columns(
            # Truncate to hour, store as UTC epoch ms
            pl.col("ts").dt.truncate("1h").dt.epoch("ms").alias("ts"),
            pl.col("category").cast(pl.Utf8).alias("category"),
        )
        .filter(pl.col("category").is_in(["1", "2", "10"]))
        .group_by("data", "ts", "category")
        .agg(pl.len().cast(pl.Int32).alias("count"))
        .with_columns(
            pl.col("data").replace(zone_map, default="").alias("zone_en"),
        )
        .sort("data", "ts", "category")
    )

    total = grouped["count"].sum()
    cities = grouped["data"].n_unique()
    print(f"  Alerts parquet: {grouped.height} rows, {cities} cities, {total} total alerts")
    return grouped


def generate_events_parquet(alerts_matched, zone_map, name_en_map):
    """Generate events.parquet — one row per alert event with start/end ms."""
    valid_threats = {"missiles", "drones", "terrorists"}

    filtered = alerts_matched.filter(
        pl.col("threat_type").is_in(list(valid_threats)),
    )

    # Compute start = min(warning_ts, ts), end = resolved_ts
    # Store as Israel wall-clock epoch ms (strip tz before epoch)
    # so dashboard can use new Date(ms) directly without toLocaleString per row
    events = (
        filtered
        .with_columns(
            pl.when(
                pl.col("warning_ts").is_not_null() & (pl.col("warning_ts") < pl.col("ts"))
            ).then(pl.col("warning_ts")).otherwise(pl.col("ts")).alias("start_ts"),
        )
        .filter(pl.col("start_ts") >= pl.lit(CUTOFF))
        .with_columns(
            pl.col("start_ts").dt.replace_time_zone(None).dt.epoch("ms").alias("start_ms"),
            pl.col("resolved_ts").dt.replace_time_zone(None).dt.epoch("ms").alias("end_ms"),
            pl.col("data").replace(zone_map, default="").alias("zone_en"),
            pl.col("data").replace(name_en_map, default="").alias("name_en"),
        )
        .select("data", "threat_type", "start_ms", "end_ms", "zone_en", "name_en")
    )

    print(f"  Events parquet: {events.height} events, {events['data'].n_unique()} cities")
    return events


def generate_snapshot_json(alerts_pq):
    """Generate snapshot.json — precomputed initial dashboard state for instant render."""
    city_agg = alerts_pq.group_by("data").agg(pl.col("count").sum().alias("cnt"))
    count_by_city = dict(zip(city_agg["data"].to_list(), city_agg["cnt"].to_list()))

    total_alerts = int(alerts_pq["count"].sum())
    n_cities = alerts_pq["data"].n_unique()

    daily = (
        alerts_pq
        .with_columns((pl.col("ts") // 86400000).alias("day_key"))
        .group_by("day_key").agg(pl.col("count").sum().alias("cnt"))
        .sort("cnt", descending=True)
    )
    peak_day_ms = int(daily[0, "day_key"] * 86400000)
    peak_count = int(daily[0, "cnt"])

    cat_agg = alerts_pq.group_by("category").agg(pl.col("count").sum().alias("cnt"))
    by_cat = dict(zip(cat_agg["category"].to_list(), cat_agg["cnt"].to_list()))

    hourly = (
        alerts_pq.group_by("ts").agg(pl.col("count").sum().alias("cnt")).sort("ts")
    )
    sparkline = list(zip(
        [int(x) for x in hourly["ts"].to_list()],
        [int(x) for x in hourly["cnt"].to_list()],
    ))

    snapshot = {
        "countByCity": count_by_city,
        "totalAlerts": total_alerts,
        "cities": n_cities,
        "peakDayMs": peak_day_ms,
        "peakCount": peak_count,
        "missiles": int(by_cat.get("1", 0)),
        "drones": int(by_cat.get("2", 0)),
        "infiltration": int(by_cat.get("10", 0)),
        "minTs": int(alerts_pq["ts"].min()),
        "maxTs": int(alerts_pq["ts"].max()),
        "sparkline": sparkline,
    }
    print(f"  Snapshot: {total_alerts} alerts, {n_cities} cities, {len(sparkline)} hourly points")
    return snapshot


# ── Main ───────────────────────────────────────────────────────


def run_pipeline():
    print(f"[{datetime.now()}] Starting pipeline...")

    # Check if daily partitioned data exists
    has_daily = bool(s3_list_keys(RAW_PREFIX))
    legacy_key = "raw/alerts_all.json"
    local_legacy = os.path.join(os.path.dirname(__file__), "alerts_all.json")

    if not has_daily and os.path.exists(local_legacy):
        # Migrate from local JSON cache
        print("Migrating local JSON cache to daily parquet files...")
        with open(local_legacy, "r", encoding="utf-8") as f:
            raw_alerts = json.load(f)
        print(f"Loaded {len(raw_alerts)} alerts, partitioning by day...")
        for date_str, day_alerts in partition_by_day(raw_alerts).items():
            save_day(date_str, day_alerts)
    elif not has_daily and s3_exists(legacy_key):
        # Migrate from S3 JSON
        print("Migrating S3 JSON to daily parquet files...")
        raw_alerts = s3_read_json(legacy_key)
        print(f"Loaded {len(raw_alerts)} alerts, partitioning by day...")
        for date_str, day_alerts in partition_by_day(raw_alerts).items():
            save_day(date_str, day_alerts)
    elif not has_daily:
        # First run
        print("First run \u2014 fetching all cities...")
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
    cf.create_invalidation(
        DistributionId=CF_DISTRIBUTION,
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


def main():
    """Run pipeline on a loop with graceful shutdown."""
    stop = False

    def handle_signal(signum, frame):
        nonlocal stop
        print(f"\nReceived signal {signum}, shutting down...")
        stop = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    # Run immediately on startup
    while not stop:
        try:
            run_pipeline()
        except Exception as e:
            print(f"[{datetime.now()}] Pipeline error: {e}")

        if stop:
            break

        print(f"Sleeping {INTERVAL}s until next run...")
        # Sleep in small increments to allow graceful shutdown
        for _ in range(INTERVAL):
            if stop:
                break
            time.sleep(1)

    print("Shutdown complete.")


if __name__ == "__main__":
    main()
