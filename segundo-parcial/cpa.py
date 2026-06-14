"""Cloud Provider Analytics — pure, importable definitions.

This module holds only side-effect-free building blocks: the schema registry,
dedupe specs, and the pure Silver/Gold DataFrame transforms. Keeping them free of
I/O lets them be unit tested in isolation, without running the pipeline. The
driver lives in ``pipeline.py``, which imports from here.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# --------------------------------------------------------------------------- #
# Master (batch) schemas — explicit typing, no inference.
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class MasterSpec:
    """One batch master: its CSV file stem, explicit schema, and dedupe key."""

    name: str
    filename: str
    schema: StructType
    dedupe_keys: list[str]


CUSTOMERS_ORGS_SCHEMA = StructType(
    [
        StructField("org_id", StringType()),
        StructField("org_name", StringType()),
        StructField("industry", StringType()),
        StructField("hq_region", StringType()),
        StructField("plan_tier", StringType()),
        StructField("is_enterprise", BooleanType()),
        StructField("signup_date", DateType()),
        StructField("sales_rep", StringType()),
        StructField("lifecycle_stage", StringType()),
        StructField("marketing_source", StringType()),
        StructField("nps_score", DoubleType()),
    ]
)

USERS_SCHEMA = StructType(
    [
        StructField("user_id", StringType()),
        StructField("org_id", StringType()),
        StructField("email", StringType()),
        StructField("role", StringType()),
        StructField("active", BooleanType()),
        StructField("created_at", DateType()),
        StructField("last_login", DateType()),
    ]
)

RESOURCES_SCHEMA = StructType(
    [
        StructField("resource_id", StringType()),
        StructField("org_id", StringType()),
        StructField("service", StringType()),
        StructField("region", StringType()),
        StructField("created_at", DateType()),
        StructField("state", StringType()),
        StructField("tags_json", StringType()),
    ]
)

SUPPORT_TICKETS_SCHEMA = StructType(
    [
        StructField("ticket_id", StringType()),
        StructField("org_id", StringType()),
        StructField("category", StringType()),
        StructField("severity", StringType()),
        StructField("created_at", DateType()),
        StructField("resolved_at", DateType()),
        StructField("csat", DoubleType()),
        StructField("sla_breached", BooleanType()),
    ]
)

MARKETING_TOUCHES_SCHEMA = StructType(
    [
        StructField("touch_id", StringType()),
        StructField("org_id", StringType()),
        StructField("campaign", StringType()),
        StructField("channel", StringType()),
        StructField("timestamp", DateType()),
        StructField("clicked", BooleanType()),
        StructField("converted", BooleanType()),
    ]
)

NPS_SURVEYS_SCHEMA = StructType(
    [
        StructField("org_id", StringType()),
        StructField("survey_date", DateType()),
        StructField("nps_score", DoubleType()),
        StructField("comment", StringType()),
    ]
)

BILLING_MONTHLY_SCHEMA = StructType(
    [
        StructField("invoice_id", StringType()),
        StructField("org_id", StringType()),
        StructField("month", DateType()),
        StructField("subtotal", DoubleType()),
        StructField("credits", DoubleType()),
        StructField("taxes", DoubleType()),
        StructField("currency", StringType()),
        StructField("exchange_rate_to_usd", DoubleType()),
    ]
)

# Registry of all 7 masters, keyed by logical entity name.
MASTERS: dict[str, MasterSpec] = {
    spec.name: spec
    for spec in [
        MasterSpec(
            "customers_orgs", "customers_orgs.csv", CUSTOMERS_ORGS_SCHEMA, ["org_id"]
        ),
        MasterSpec("users", "users.csv", USERS_SCHEMA, ["user_id"]),
        MasterSpec("resources", "resources.csv", RESOURCES_SCHEMA, ["resource_id"]),
        MasterSpec(
            "support_tickets",
            "support_tickets.csv",
            SUPPORT_TICKETS_SCHEMA,
            ["ticket_id"],
        ),
        MasterSpec(
            "marketing_touches",
            "marketing_touches.csv",
            MARKETING_TOUCHES_SCHEMA,
            ["touch_id"],
        ),
        MasterSpec(
            "nps_surveys",
            "nps_surveys.csv",
            NPS_SURVEYS_SCHEMA,
            ["org_id", "survey_date"],
        ),
        MasterSpec(
            "billing_monthly",
            "billing_monthly.csv",
            BILLING_MONTHLY_SCHEMA,
            ["invoice_id"],
        ),
    ]
}

# --------------------------------------------------------------------------- #
# Streaming (usage events) superset schema — v1 ∪ v2.
# v1 rows simply get null carbon_kg / genai_tokens; `value` is typed double so
# Spark does not infer it as string.
# --------------------------------------------------------------------------- #

EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("org_id", StringType()),
        StructField("resource_id", StringType()),
        StructField("service", StringType()),
        StructField("region", StringType()),
        StructField("metric", StringType()),
        StructField("value", DoubleType()),
        StructField("unit", StringType()),
        StructField("cost_usd_increment", DoubleType()),
        StructField("schema_version", IntegerType()),
        StructField("carbon_kg", DoubleType()),
        StructField("genai_tokens", LongType()),
    ]
)


# --------------------------------------------------------------------------- #
# Silver pure transforms — DataFrame -> DataFrame, no I/O, so they are
# unit-testable with small in-memory DataFrames.
# --------------------------------------------------------------------------- #

# Org master attributes attached to each event during enrichment.
ORG_ENRICH_COLS = ["org_name", "plan_tier", "industry", "hq_region"]

# Default negative-cost threshold for the DQ flag (rule R2).
NEG_COST_THRESHOLD = -0.01


def conform_and_enrich(events: DataFrame, orgs: DataFrame) -> DataFrame:
    """Conform v1/v2 events, derive features, and enrich with org attributes.

    - ``event_date`` derived from the event timestamp.
    - ``genai_tokens`` coalesced to 0 (null = not reported / non-genai; 0 is the
      additive identity for summing).
    - ``carbon_kg`` kept as-is (null for v1 — not fabricated).
    - Features: ``cost_usd``, ``genai_tokens``, ``carbon_kg``, ``requests``.
    - Broadcast LEFT join to the org master so events with an unknown ``org_id``
      survive with null org attributes.
    """
    enriched = (
        events.withColumn("event_date", F.to_date("timestamp"))
        .withColumn("genai_tokens", F.coalesce(F.col("genai_tokens"), F.lit(0)))
        .withColumn("cost_usd", F.col("cost_usd_increment"))
        .withColumn("requests", F.when(F.col("metric") == "requests", F.col("value")))
    )
    orgs_dim = orgs.select("org_id", *ORG_ENRICH_COLS)
    return enriched.join(F.broadcast(orgs_dim), on="org_id", how="left")


def apply_dq_rules(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split rows into (clean, quarantine) by the 3 active data-quality rules.

    - R1: ``event_id`` null -> quarantine (structurally broken).
    - R3: ``value`` present but ``unit`` null -> quarantine (unit-inconsistent).
    - R2 (negative cost) is NOT a quarantine rule: those rows are kept and
      flagged downstream by :func:`flag_cost_anomalies`.

    Quarantined rows carry a ``dq_rule`` reason column for inspectable samples.
    """
    reason = F.when(F.col("event_id").isNull(), F.lit("event_id_null")).when(
        F.col("value").isNotNull() & F.col("unit").isNull(),
        F.lit("unit_missing_with_value"),
    )
    tagged = df.withColumn("dq_rule", reason)
    quarantine = tagged.filter(F.col("dq_rule").isNotNull())
    clean = tagged.filter(F.col("dq_rule").isNull()).drop("dq_rule")
    return clean, quarantine


def flag_cost_anomalies(
    df: DataFrame, neg_threshold: float = NEG_COST_THRESHOLD
) -> DataFrame:
    """Add ``cost_anomaly_flag``: negative cost (R2) OR per-service p01/p99 outlier."""
    stats = df.groupBy("service").agg(
        F.percentile_approx("cost_usd", 0.01).alias("_p01"),
        F.percentile_approx("cost_usd", 0.99).alias("_p99"),
    )
    return (
        df.join(F.broadcast(stats), on="service", how="left")
        .withColumn(
            "cost_anomaly_flag",
            (F.col("cost_usd_increment") < F.lit(neg_threshold))
            | (F.col("cost_usd") < F.col("_p01"))
            | (F.col("cost_usd") > F.col("_p99")),
        )
        .drop("_p01", "_p99")
    )


# --------------------------------------------------------------------------- #
# Gold pure transforms — FinOps mart + 14-day rollup.
# --------------------------------------------------------------------------- #

# Trailing window (days) for the top-N-services-by-cost serving query.
COST_ROLLUP_DAYS = 14


def build_gold_daily(silver: DataFrame) -> DataFrame:
    """FinOps mart `org_daily_usage_by_service`: grain org x service x day.

    Measures summed across regions; `has_anomaly` is true if any event in the
    group was flagged. Org dimensions are carried for serving.
    """
    return silver.groupBy("org_id", "service", "event_date").agg(
        F.sum("cost_usd").alias("cost_usd"),
        F.sum("requests").alias("requests"),
        F.sum("genai_tokens").alias("genai_tokens"),
        F.sum("carbon_kg").alias("carbon_kg"),
        F.count(F.lit(1)).alias("event_count"),
        F.max("cost_anomaly_flag").alias("has_anomaly"),
        F.first("org_name", ignorenulls=True).alias("org_name"),
        F.first("plan_tier", ignorenulls=True).alias("plan_tier"),
        F.first("industry", ignorenulls=True).alias("industry"),
        F.first("hq_region", ignorenulls=True).alias("hq_region"),
    )


def build_cost_14d(
    gold_daily: DataFrame, as_of_date, window_days: int = COST_ROLLUP_DAYS
) -> DataFrame:
    """Per org x service cost summed over the trailing `window_days` ending at
    `as_of_date` (a datetime.date). Pre-aggregation for the top-N serving query.
    """
    import datetime as _dt

    start = as_of_date - _dt.timedelta(days=window_days - 1)
    windowed = gold_daily.filter(
        (F.col("event_date") >= F.lit(start))
        & (F.col("event_date") <= F.lit(as_of_date))
    )
    return windowed.groupBy("org_id", "service").agg(
        F.sum("cost_usd").alias("total_cost_usd"),
        F.first("org_name", ignorenulls=True).alias("org_name"),
    )
