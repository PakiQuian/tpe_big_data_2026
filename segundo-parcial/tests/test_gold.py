"""Unit tests for the Gold pure transforms (issue #6)."""

import datetime as dt

import cpa
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Minimal Silver-like schema feeding build_gold_daily.
SILVER_SCHEMA = StructType(
    [
        StructField("org_id", StringType()),
        StructField("service", StringType()),
        StructField("event_date", DateType()),
        StructField("cost_usd", DoubleType()),
        StructField("requests", DoubleType()),
        StructField("genai_tokens", LongType()),
        StructField("carbon_kg", DoubleType()),
        StructField("cost_anomaly_flag", BooleanType()),
        StructField("org_name", StringType()),
        StructField("plan_tier", StringType()),
        StructField("industry", StringType()),
        StructField("hq_region", StringType()),
    ]
)


def test_gold_daily_aggregates_across_regions(spark):
    d = dt.date(2025, 8, 1)
    rows = [
        # same org/service/day, two regions -> collapse to one row
        (
            "o1",
            "compute",
            d,
            1.0,
            None,
            0,
            0.1,
            False,
            "Org One",
            "std",
            "Edu",
            "us-east",
        ),
        (
            "o1",
            "compute",
            d,
            2.0,
            5.0,
            0,
            0.2,
            True,
            "Org One",
            "std",
            "Edu",
            "eu-west",
        ),
        # different day
        (
            "o1",
            "compute",
            dt.date(2025, 8, 2),
            4.0,
            1.0,
            7,
            0.3,
            False,
            "Org One",
            "std",
            "Edu",
            "us-east",
        ),
    ]
    df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
    out = {
        (r["service"], r["event_date"]): r for r in cpa.build_gold_daily(df).collect()
    }

    day1 = out[("compute", d)]
    assert day1["cost_usd"] == 3.0  # 1 + 2
    assert day1["requests"] == 5.0  # null ignored
    assert day1["event_count"] == 2
    assert day1["has_anomaly"] is True  # any flagged
    assert day1["org_name"] == "Org One"  # dims carried

    day2 = out[("compute", dt.date(2025, 8, 2))]
    assert day2["has_anomaly"] is False
    assert day2["genai_tokens"] == 7


GOLD_SCHEMA = StructType(
    [
        StructField("org_id", StringType()),
        StructField("service", StringType()),
        StructField("event_date", DateType()),
        StructField("cost_usd", DoubleType()),
        StructField("org_name", StringType()),
    ]
)


def test_cost_14d_sums_only_trailing_window(spark):
    as_of = dt.date(2025, 8, 31)
    rows = [
        ("o1", "compute", as_of, 10.0, "Org One"),  # in window (day 0)
        (
            "o1",
            "compute",
            as_of - dt.timedelta(days=13),
            20.0,
            "Org One",
        ),  # in window (day 13)
        (
            "o1",
            "compute",
            as_of - dt.timedelta(days=14),
            100.0,
            "Org One",
        ),  # OUT of window
        ("o1", "storage", as_of, 5.0, "Org One"),  # different service
    ]
    df = spark.createDataFrame(rows, schema=GOLD_SCHEMA)
    out = {
        (r["org_id"], r["service"]): r for r in cpa.build_cost_14d(df, as_of).collect()
    }

    assert out[("o1", "compute")]["total_cost_usd"] == 30.0  # 10 + 20, NOT 100
    assert out[("o1", "storage")]["total_cost_usd"] == 5.0
