import polars as pl


# ── Constants ──────────────────────────────────────────────────

STAY_NEARBY = "ניתן לצאת מהמרחב המוגן אך יש להישאר בקרבתו"
THRESHOLD_MINUTES = 30
RESOLVED_TYPES = ["resolved"]
ATTACH_TYPES = ["resolved", "weak_resolved"]  # always join preceding group
ATTACH_THRESHOLD_MINUTES = 180  # max gap for ATTACH_TYPES to join preceding group
WARNING_TYPES = ["early_warning", "weak_resolved"]
EVENT_TYPES = ["alert"]


def build_incidents(raw_alerts):
    """
    Group raw alert events into incidents using gap-based grouping.

    Returns (incident_events, incident_summary):
      - incident_events: every raw row with group_id + pattern attached
      - incident_summary: one row per incident group with start/end/duration/etc.
    """
    print(f"  [incidents.py] polars version: {pl.__version__}")
    if not raw_alerts:
        raise ValueError("No alerts to process")

    df = pl.DataFrame(raw_alerts).unique()
    df = df.with_columns(pl.col("category").cast(pl.Int64))
    print("  [1/7] DataFrame created")

    # ── Row-level labeling ─────────────────────────────────────

    df = df.with_columns(
        pl.col("alertDate").str.to_datetime(
            "%Y-%m-%dT%H:%M:%S", time_zone="Asia/Jerusalem"
        ).alias("ts"),
        # event_type: distinguish weak_resolved from resolved
        pl.when(pl.col("category").is_in([1, 2, 10]))
        .then(pl.lit("alert"))
        .when(
            (pl.col("category") == 13)
            & (pl.col("category_desc") == STAY_NEARBY)
        )
        .then(pl.lit("weak_resolved"))
        .when(pl.col("category") == 13)
        .then(pl.lit("resolved"))
        .when(pl.col("category") == 14)
        .then(pl.lit("early_warning"))
        .otherwise(pl.lit("other"))
        .alias("event_type"),
        # threat_type
        pl.when(pl.col("category") == 1).then(pl.lit("missiles"))
        .when(pl.col("category") == 2).then(pl.lit("drones"))
        .when(pl.col("category") == 10).then(pl.lit("infiltration"))
        .when(pl.col("category_desc").str.contains("רקטות וטילים")).then(pl.lit("missiles"))
        .when(pl.col("category_desc").str.contains("כלי טיס עוין")).then(pl.lit("drones"))
        .when(pl.col("category_desc").str.contains("מחבלים")).then(pl.lit("infiltration"))
        .otherwise(pl.lit(None))
        .alias("threat_type"),
    ).sort("data", "ts")
    print("  [2/7] Row-level labeling done")

    # ── Incident grouping ──────────────────────────────────────
    # New group when gap > threshold or previous was resolved,
    # but ATTACH_TYPES always join preceding group.

    incidents = df.with_columns(
        pl.col("ts").diff().over("data").dt.total_minutes().alias("gap_minutes")
    )
    print("  [3/7] Gap minutes computed")

    incidents = incidents.with_columns(
            (
                (pl.col("gap_minutes") > ATTACH_THRESHOLD_MINUTES)
                | (
                    (
                        pl.col("event_type").shift(1).over("data").is_in(RESOLVED_TYPES)
                        | (pl.col("gap_minutes") > THRESHOLD_MINUTES)
                        | pl.col("gap_minutes").is_null()
                    )
                    & ~pl.col("event_type").is_in(ATTACH_TYPES)
                )
            ).cast(pl.Int32).alias("_new_group")
    )
    print("  [4a/7] New-group flag computed")

    incidents = incidents.with_columns(
        pl.col("_new_group").cum_sum().over("data").alias("group_id")
    ).drop("_new_group")
    print("  [4b/7] Group IDs assigned")

    # ── Pattern classification ─────────────────────────────
    incidents = incidents.with_columns(
            pl.col("event_type").is_in(WARNING_TYPES).any().over("data", "group_id").alias("has_warning"),
            pl.col("event_type").is_in(EVENT_TYPES).any().over("data", "group_id").alias("has_alert"),
            pl.col("event_type").is_in(RESOLVED_TYPES).any().over("data", "group_id").alias("has_resolution"),
            (pl.col("event_type").last().over("data", "group_id") == "weak_resolved").alias("ends_weakResolution"),
    )
    print("  [5/7] Pattern flags computed")

    incidents = incidents.with_columns(
            pl.when(pl.col("has_warning") & pl.col("has_alert") & pl.col("has_resolution"))
                .then(pl.lit("warning_alert_resolution"))
            .when(pl.col("has_alert") & pl.col("has_resolution"))
                .then(pl.lit("X_alert_resolution"))
            .when(pl.col("has_warning") & pl.col("has_resolution"))
                .then(pl.lit("warning_X_resolution"))
            .when(pl.col("has_warning") & pl.col("has_alert") & pl.col("ends_weakResolution"))
                .then(pl.lit("warning_alert_weakResolution"))
            .when(pl.col("has_warning") & pl.col("has_alert"))
                .then(pl.lit("warning_alert_X"))
            .when(pl.col("has_alert") & pl.col("ends_weakResolution"))
                .then(pl.lit("X_alert_weakResolution"))
            .when(pl.col("has_alert"))
                .then(pl.lit("X_alert_X"))
            .when(pl.col("ends_weakResolution"))
                .then(pl.lit("warning_X_weakResolution"))
            .when(pl.col("has_warning"))
                .then(pl.lit("warning_X_X"))
            .when(pl.col("has_resolution"))
                .then(pl.lit("X_X_resolution"))
            .otherwise(pl.lit("unclassified"))
            .alias("pattern")
    )
    incidents = incidents.drop("gap_minutes", "has_warning", "has_alert", "has_resolution", "ends_weakResolution")
    print("  [6/7] Pattern classification done")

    # ── Output: incident_events ────────────────────────────────
    incident_events = incidents.select(
        "data", "ts", "category", "category_desc",
        "rid", "event_type", "threat_type", "group_id", "pattern",
    )

    # ── Output: incident_summary ───────────────────────────────
    incident_summary = (
        incidents
        .group_by("data", "group_id")
        .agg(
            pl.col("ts").min().alias("start"),
            pl.col("ts").max().alias("end"),
            (pl.col("ts").max() - pl.col("ts").min())
                .dt.total_minutes().alias("duration_min"),
            pl.len().alias("n_events"),
            pl.col("threat_type").drop_nulls().unique().alias("threat_types"),
            pl.col("pattern").first(),
        )
        .sort("data", "group_id")
        .with_columns(pl.col("threat_types").list.sort())
    )
    print("  [7/7] Incident summary built")

    alerts_count = incident_events.filter(pl.col("event_type") == "alert").height
    print(f"  Incidents: {incident_summary.height} groups from {incident_events.height} events ({alerts_count} alerts)")

    return incident_events, incident_summary
