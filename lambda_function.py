import json
import io
import csv
import asyncio
import aiohttp
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import polars as pl
import boto3

BUCKET = "alerts-dashboard-data"
CF_DISTRIBUTION = "E28MP73WOLCLYQ"
API_URL = "https://alerts-history.oref.org.il/Shared/Ajax/GetAlarmsHistory.aspx"
CUTOFF = datetime(2026, 2, 26)  # Israel local time (naive, matches API data)
CONCURRENCY = 10

s3 = boto3.client("s3")
cf = boto3.client("cloudfront")


# ── Fetch ──────────────────────────────────────────────────────


async def fetch_api(session, params):
    """Fetch alerts from the Pikud HaOref API."""
    async with session.get(API_URL, params=params) as resp:
        if resp.status != 200:
            return []
        text = await resp.text()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
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
                f"city_0": city,
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
    with open("cities.csv", "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            if row:
                cities.append(row[0])
    return cities


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


# ── Transform ──────────────────────────────────────────────────


def transform(raw_alerts):
    """
    Replicates fetch_analysis.py:
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

    ts_col = pl.col("alertDate").str.to_datetime().alias("ts")
    alerts = df_typed.filter(pl.col("event_type") == "alert").with_columns(ts_col).sort("data", "ts")
    warnings = df_typed.filter(pl.col("event_type") == "early_warning").with_columns(ts_col).sort("data", "ts")
    resolutions = df_typed.filter(pl.col("event_type") == "resolved").with_columns(ts_col).sort("data", "ts")

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


def generate_cube(df_typed):
    """Generate alerts_cube.json from typed alerts."""
    # Filter to actual alerts after cutoff
    alerts = df_typed.filter(
        pl.col("event_type") == "alert",
        pl.col("alertDate").str.to_datetime() >= pl.lit(CUTOFF),
    ).with_columns(
        pl.col("alertDate").str.to_datetime().alias("ts"),
    )

    cat_map = {"1": 0, "2": 1, "10": 2}

    # Group by (city, hour, category)
    grouped = (
        alerts
        .with_columns(pl.col("ts").dt.strftime("%Y-%m-%dT%H").alias("hour"))
        .group_by("data", "hour", "category")
        .agg(pl.len().alias("count"))
        .sort("data", "hour", "category")
    )

    cities = sorted(grouped["data"].unique().to_list())
    city_idx = {c: i for i, c in enumerate(cities)}
    hours = sorted(grouped["hour"].unique().to_list())
    hour_idx = {h: i for i, h in enumerate(hours)}

    c, h, t, n = [], [], [], []
    for row in grouped.iter_rows(named=True):
        cat_str = str(row["category"])
        if cat_str not in cat_map:
            continue
        c.append(city_idx[row["data"]])
        h.append(hour_idx[row["hour"]])
        t.append(cat_map[cat_str])
        n.append(row["count"])

    cube = {"cities": cities, "hours": hours, "c": c, "h": h, "t": t, "n": n}
    total = sum(n)
    print(f"  Cube: {len(c)} tuples, {len(cities)} cities, {len(hours)} hours, {total} total alerts")
    return cube


def generate_timeline(alerts_matched):
    """Generate timeline_events.json from matched alerts."""
    threat_map = {"missiles": 0, "drones": 1, "terrorists": 2}
    valid_threats = {"missiles", "drones", "terrorists"}

    base = CUTOFF

    def to_minutes(dt):
        if dt is None:
            return None
        return round((dt - base).total_seconds() / 60)

    # Filter to valid threats after cutoff
    filtered = alerts_matched.filter(
        pl.col("threat_type").is_in(list(valid_threats)),
    ).with_columns(
        pl.col("ts").alias("_ts"),
    )

    # Compute start time (warning if earlier than alert, else alert)
    events = []
    for row in filtered.iter_rows(named=True):
        ts = row["_ts"]
        warning_ts = row.get("warning_ts")
        resolved_ts = row.get("resolved_ts")
        start = warning_ts if warning_ts and warning_ts < ts else ts
        if start < CUTOFF:
            continue
        events.append({
            "data": row["data"],
            "threat_type": row["threat_type"],
            "start": start,
            "resolved": resolved_ts,
            "warning": warning_ts,
        })

    cities = sorted(set(e["data"] for e in events))
    city_idx = {c: i for i, c in enumerate(cities)}

    c, t, s, r, w = [], [], [], [], []
    for e in events:
        c.append(city_idx[e["data"]])
        t.append(threat_map[e["threat_type"]])
        s.append(to_minutes(e["start"]))
        r.append(to_minutes(e["resolved"]))
        w.append(to_minutes(e["warning"]))

    result = {
        "cities": cities,
        "base": base.isoformat(),
        "c": c, "t": t, "s": s, "r": r, "w": w,
    }
    print(f"  Timeline: {len(c)} events, {len(cities)} cities")
    return result


# ── Main handler ───────────────────────────────────────────────


def lambda_handler(event, context):
    print("Starting pipeline...")

    # Step 1: Check if raw data exists (initialization vs incremental)
    raw_key = "raw/alerts_all.json"
    is_init = not s3_exists(raw_key)

    if is_init:
        print("First run — fetching all cities...")
        cities = load_cities()
        raw_alerts = asyncio.get_event_loop().run_until_complete(fetch_all_cities(cities))
    else:
        # Incremental: fetch last 24h and merge
        existing = s3_read_json(raw_key)
        new_alerts = asyncio.get_event_loop().run_until_complete(fetch_latest())

        # Merge & deduplicate by rid
        by_rid = {a["rid"]: a for a in existing}
        for a in new_alerts:
            by_rid[a["rid"]] = a
        raw_alerts = list(by_rid.values())
        print(f"Merged: {len(existing)} existing + {len(new_alerts)} new = {len(raw_alerts)} total")

    # Save raw data
    s3_write_json(raw_key, raw_alerts)

    # Step 2: Transform
    print("Transforming...")
    df_typed, alerts_matched = transform(raw_alerts)

    # Step 3: Generate optimized files
    print("Generating optimized files...")
    cube = generate_cube(df_typed)
    timeline = generate_timeline(alerts_matched)

    # Step 4: Upload
    s3_write_json("optimized/alerts_cube.json", cube)
    s3_write_json("optimized/timeline_events.json", timeline)

    # Step 5: Invalidate CloudFront cache
    cf.create_invalidation(
        DistributionId=CF_DISTRIBUTION,
        InvalidationBatch={
            "Paths": {
                "Quantity": 2,
                "Items": [
                    "/optimized/alerts_cube.json",
                    "/optimized/timeline_events.json",
                ],
            },
            "CallerReference": str(datetime.now(timezone.utc).timestamp()),
        },
    )
    print("CloudFront invalidation created")

    print("Pipeline complete!")
    return {"statusCode": 200, "body": f"Processed {len(raw_alerts)} alerts"}


# Allow local testing
if __name__ == "__main__":
    lambda_handler({}, None)
