import polars as pl

from .config import ALERTS_START_DATE


def generate_alerts_parquet(df_typed, zone_map):
    """Generate alerts.parquet — one row per city/hour/category with count."""
    alerts = df_typed.filter(
        pl.col("event_type") == "alert",
        pl.col("ts") >= pl.lit(ALERTS_START_DATE),
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
        .filter(pl.col("start_ts") >= pl.lit(ALERTS_START_DATE))
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


def generate_incident_events_parquet(incident_events):
    """Generate incident_events.parquet — every raw event with incident metadata.

    Stores proper UTC epoch ms (no fake-local hack).
    """
    result = (
        incident_events
        .filter(pl.col("ts") >= pl.lit(ALERTS_START_DATE))
        .with_columns(
            pl.col("ts").dt.epoch("ms").alias("ts"),
        )
        .select(
            "data", "ts", "category", "category_desc",
            "rid", "event_type", "threat_type", "group_id", "pattern",
        )
    )
    print(f"  Incident events parquet: {result.height} rows, {result['data'].n_unique()} cities")
    return result


def generate_incidents_parquet(incident_summary, zone_map, name_en_map):
    """Generate incidents.parquet — one row per incident × threat_type.

    Stores proper UTC epoch ms (no fake-local hack).
    end_ms is always populated (incidents are always finished).
    Explodes threat_types list so each threat_type gets its own row.
    """
    filtered = incident_summary.filter(
        pl.col("start") >= pl.lit(ALERTS_START_DATE),
    )

    events = (
        filtered
        .with_columns(
            pl.when(pl.col("threat_types").list.len() == 0)
            .then(pl.lit(["false_alarm"]))
            .otherwise(pl.col("threat_types"))
            .alias("threat_types"),
        )
        .explode("threat_types")
        .rename({"threat_types": "threat_type"})
        # Re-attach the full list for each exploded row
        .join(
            filtered.select("data", "group_id", "threat_types"),
            on=["data", "group_id"],
            how="left",
        )
        .with_columns(
            pl.col("start").dt.epoch("ms").alias("start_ms"),
            pl.col("end").dt.epoch("ms").alias("end_ms"),
            pl.col("data").replace(zone_map, default="").alias("zone_en"),
            pl.col("data").replace(name_en_map, default="").alias("name_en"),
        )
        .select(
            "data", "threat_type", "threat_types",
            "start_ms", "end_ms", "duration_min", "n_events",
            "pattern", "group_id", "zone_en", "name_en",
        )
    )

    print(f"  Incidents parquet: {events.height} rows ({filtered.height} incidents), {events['data'].n_unique()} cities")
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
