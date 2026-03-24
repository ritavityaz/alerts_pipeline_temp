from datetime import timedelta

import polars as pl


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
