"""Unit tests for the Silver pure transforms (issue #5).

These build tiny in-memory DataFrames and assert on external behavior of
conform_and_enrich / apply_dq_rules / flag_cost_anomalies.
"""

import datetime as dt

import cpa
from pyspark.sql import Row
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


def _events(spark, rows):
    """Build an events DataFrame using the production EVENTS_SCHEMA."""
    return spark.createDataFrame(rows, schema=cpa.EVENTS_SCHEMA)


def _ts(day):
    return dt.datetime(2025, 8, day, 12, 0, 0)


ORGS_SCHEMA = StructType(
    [
        StructField("org_id", StringType()),
        StructField("org_name", StringType()),
        StructField("plan_tier", StringType()),
        StructField("industry", StringType()),
        StructField("hq_region", StringType()),
    ]
)


def _orgs(spark):
    return spark.createDataFrame(
        [
            ("o1", "Org One", "standard", "Education", "us-east"),
            ("o2", "Org Two", "enterprise", "Finance", "eu-west"),
        ],
        schema=ORGS_SCHEMA,
    )


# --------------------------------------------------------------------------- #
# conform_and_enrich
# --------------------------------------------------------------------------- #


def test_conform_v1_coalesces_genai_keeps_carbon_null(spark):
    # v1 event: carbon_kg + genai_tokens null in source.
    v1 = Row(
        event_id="e1",
        timestamp=_ts(1),
        org_id="o1",
        resource_id="r1",
        service="compute",
        region="us-east",
        metric="cpu_hours",
        value=2.0,
        unit="hours",
        cost_usd_increment=1.0,
        schema_version=1,
        carbon_kg=None,
        genai_tokens=None,
    )
    out = cpa.conform_and_enrich(_events(spark, [v1]), _orgs(spark)).collect()[0]
    assert out["genai_tokens"] == 0  # coalesced to additive identity
    assert out["carbon_kg"] is None  # not fabricated for v1
    assert out["event_date"] == dt.date(2025, 8, 1)
    assert out["org_name"] == "Org One"  # enriched


def test_conform_requests_feature_only_for_requests_metric(spark):
    rows = [
        Row(
            event_id="e1",
            timestamp=_ts(1),
            org_id="o1",
            resource_id="r1",
            service="networking",
            region="us-east",
            metric="requests",
            value=42.0,
            unit="count",
            cost_usd_increment=1.0,
            schema_version=2,
            carbon_kg=0.1,
            genai_tokens=None,
        ),
        Row(
            event_id="e2",
            timestamp=_ts(1),
            org_id="o1",
            resource_id="r1",
            service="compute",
            region="us-east",
            metric="cpu_hours",
            value=9.0,
            unit="hours",
            cost_usd_increment=1.0,
            schema_version=2,
            carbon_kg=0.1,
            genai_tokens=None,
        ),
    ]
    out = {
        r["event_id"]: r
        for r in cpa.conform_and_enrich(_events(spark, rows), _orgs(spark)).collect()
    }
    assert out["e1"]["requests"] == 42.0
    assert out["e2"]["requests"] is None


def test_conform_unknown_org_survives_with_null_attrs(spark):
    row = Row(
        event_id="e1",
        timestamp=_ts(1),
        org_id="ghost",
        resource_id="r1",
        service="compute",
        region="us-east",
        metric="cpu_hours",
        value=2.0,
        unit="hours",
        cost_usd_increment=1.0,
        schema_version=2,
        carbon_kg=0.1,
        genai_tokens=None,
    )
    out = cpa.conform_and_enrich(_events(spark, [row]), _orgs(spark)).collect()
    assert len(out) == 1  # left join: not dropped
    assert out[0]["org_name"] is None


# --------------------------------------------------------------------------- #
# apply_dq_rules
# --------------------------------------------------------------------------- #


def test_dq_routes_and_keeps_negative(spark):
    rows = [
        # clean
        Row(
            event_id="ok",
            timestamp=_ts(1),
            org_id="o1",
            resource_id="r",
            service="compute",
            region="us-east",
            metric="cpu_hours",
            value=2.0,
            unit="hours",
            cost_usd_increment=1.0,
            schema_version=2,
            carbon_kg=0.1,
            genai_tokens=None,
        ),
        # R1: null event_id -> quarantine
        Row(
            event_id=None,
            timestamp=_ts(1),
            org_id="o1",
            resource_id="r",
            service="compute",
            region="us-east",
            metric="cpu_hours",
            value=2.0,
            unit="hours",
            cost_usd_increment=1.0,
            schema_version=2,
            carbon_kg=0.1,
            genai_tokens=None,
        ),
        # R3: value present, unit null -> quarantine
        Row(
            event_id="u",
            timestamp=_ts(1),
            org_id="o1",
            resource_id="r",
            service="compute",
            region="us-east",
            metric="cpu_hours",
            value=2.0,
            unit=None,
            cost_usd_increment=1.0,
            schema_version=2,
            carbon_kg=0.1,
            genai_tokens=None,
        ),
        # negative cost -> stays CLEAN (R2 flags, not quarantines)
        Row(
            event_id="neg",
            timestamp=_ts(1),
            org_id="o1",
            resource_id="r",
            service="compute",
            region="us-east",
            metric="cpu_hours",
            value=2.0,
            unit="hours",
            cost_usd_increment=-5.0,
            schema_version=2,
            carbon_kg=0.1,
            genai_tokens=None,
        ),
    ]
    clean, quarantine = cpa.apply_dq_rules(_events(spark, rows))
    clean_ids = {r["event_id"] for r in clean.collect()}
    q = {r["event_id"]: r["dq_rule"] for r in quarantine.collect()}

    assert clean_ids == {"ok", "neg"}  # negative kept
    assert q[None] == "event_id_null"
    assert q["u"] == "unit_missing_with_value"
    assert "dq_rule" not in clean.columns  # reason dropped from clean output


# --------------------------------------------------------------------------- #
# flag_cost_anomalies
# --------------------------------------------------------------------------- #

ANOMALY_SCHEMA = StructType(
    [
        StructField("service", StringType()),
        StructField("cost_usd", DoubleType()),
        StructField("cost_usd_increment", DoubleType()),
    ]
)


def test_flag_outlier_and_negative_not_normal(spark):
    # A realistic per-service sample: many normal points so p99 sits among them,
    # then a clear high outlier and a negative-cost row.
    rows = [("compute", 1.0, 1.0) for _ in range(100)]
    rows.append(("compute", 1000.0, 1000.0))  # statistical outlier (> p99)
    rows.append(("compute", -5.0, -5.0))  # negative cost (R2)
    df = spark.createDataFrame(rows, schema=ANOMALY_SCHEMA)

    flagged = cpa.flag_cost_anomalies(df).collect()
    by_cost = {r["cost_usd"]: r["cost_anomaly_flag"] for r in flagged}

    assert by_cost[1000.0] is True  # p99 outlier
    assert by_cost[-5.0] is True  # negative threshold
    assert by_cost[1.0] is False  # normal
