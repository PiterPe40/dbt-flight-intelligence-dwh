"""
dbt Python Model: Military Activity Anomaly Detector.

Algorithm: Z-score anomaly detection on daily military flight counts,
stratified by country and day-of-week to account for weekly patterns
(e.g., lower activity on weekends).

Methodology:
    1. Group flights by country + day_of_week
    2. Calculate historical mean and std for each group
    3. Compute Z-score: (observed - mean) / std
    4. Classify anomaly levels: NORMAL / ELEVATED / HIGH / CRITICAL

Input:  int_daily_military_counts
Output: mart_military_anomaly_scores (table in marts schema)

Requires: dbt 1.3+ with Python model support, pandas, scipy
"""

import pandas as pd
import numpy as np


def model(dbt, session):
    # Configure this model
    dbt.config(
        materialized="table",
        tags=["python", "marts", "anomaly"],
    )

    # Load upstream data
    # .df() works with dbt-duckdb >= 1.7; .to_pandas() was used in older versions
    ref = dbt.ref("int_daily_military_counts")
    df = ref.df() if hasattr(ref, 'df') else ref.to_pandas()

    if df.empty:
        # Return empty DataFrame with expected schema
        return pd.DataFrame(columns=[
            "date_id", "country_code", "day_of_week",
            "military_flight_count", "baseline_mean", "baseline_std",
            "z_score", "anomaly_level", "percentile_rank",
        ])

    # ── Step 1: Calculate baseline per country + day_of_week ──
    # This accounts for weekly patterns (e.g., less military activity on Sundays)
    baseline = df.groupby(["country_code", "day_of_week"]).agg(
        baseline_mean=("military_flight_count", "mean"),
        baseline_std=("military_flight_count", "std"),
        baseline_median=("military_flight_count", "median"),
        sample_count=("military_flight_count", "count"),
    ).reset_index()

    # Merge baseline back to daily data
    df = df.merge(baseline, on=["country_code", "day_of_week"], how="left")

    # ── Step 2: Calculate Z-score ────────────────────────────
    # Z = (observed - mean) / std
    # Handle edge case: std = 0 (constant values)
    df["z_score"] = np.where(
        df["baseline_std"] > 0,
        (df["military_flight_count"] - df["baseline_mean"]) / df["baseline_std"],
        0.0,
    )

    # ── Step 3: Classify anomaly levels ──────────────────────
    df["anomaly_level"] = pd.cut(
        df["z_score"],
        bins=[-float("inf"), 1.5, 2.5, 3.5, float("inf")],
        labels=["NORMAL", "ELEVATED", "HIGH", "CRITICAL"],
    )

    # ── Step 4: Percentile rank within country ───────────────
    df["percentile_rank"] = df.groupby("country_code")[
        "military_flight_count"
    ].rank(pct=True)

    # ── Step 5: Select output columns ────────────────────────
    result = df[[
        "date_id",
        "country_code",
        "day_of_week",
        "military_flight_count",
        "unique_aircraft_count",
        "baseline_mean",
        "baseline_std",
        "baseline_median",
        "sample_count",
        "z_score",
        "anomaly_level",
        "percentile_rank",
    ]].copy()

    # Round numeric columns for readability
    result["baseline_mean"] = result["baseline_mean"].round(2)
    result["baseline_std"] = result["baseline_std"].round(2)
    result["z_score"] = result["z_score"].round(4)
    result["percentile_rank"] = result["percentile_rank"].round(4)

    return result.sort_values(["date_id", "z_score"], ascending=[True, False])
