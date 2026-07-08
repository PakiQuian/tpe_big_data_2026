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

# Anomaly-detection thresholds, one per method.
ZSCORE_THRESHOLD = 3.0  # classic |z| > 3 (assumes roughly-normal spread)
MAD_THRESHOLD = 3.5     # modified z-score, robust to outliers (Iglewicz & Hoaglin)

# Human-readable names of the anomaly methods, used as keys in the Cassandra
# ``methods set<text>`` / ``scores map<text,double>`` collections.
ANOMALY_METHODS = ["zscore", "mad", "ptiles", "negative"]


def conform(events: DataFrame) -> DataFrame:
    """Conform v1/v2 events and derive features — no dimensional join.

    - ``event_date`` derived from the event timestamp.
    - ``genai_tokens`` coalesced to 0 (null = not reported / non-genai; 0 is the
      additive identity for summing).
    - ``carbon_kg`` kept as-is (null for v1 — not fabricated).
    - Features: ``cost_usd``, ``genai_tokens``, ``carbon_kg``, ``requests``.

    The data-quality split (:func:`apply_dq_rules`) runs on this conformed output,
    so quarantined rows are raw Bronze events — not yet enriched.
    """
    return (
        events.withColumn("event_date", F.to_date("timestamp"))
        .withColumn("genai_tokens", F.coalesce(F.col("genai_tokens"), F.lit(0)))
        .withColumn("cost_usd", F.col("cost_usd_increment"))
        .withColumn("requests", F.when(F.col("metric") == "requests", F.col("value")))
    )


def enrich(events: DataFrame, orgs: DataFrame) -> DataFrame:
    """Broadcast LEFT join to the org master.

    LEFT so events with an unknown ``org_id`` survive with null org attributes.
    """
    orgs_dim = orgs.select("org_id", *ORG_ENRICH_COLS)
    return events.join(F.broadcast(orgs_dim), on="org_id", how="left")


def conform_and_enrich(events: DataFrame, orgs: DataFrame) -> DataFrame:
    """Conform v1/v2 events and enrich with org attributes (conform + enrich)."""
    return enrich(conform(events), orgs)


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
    """Score each event for cost anomalies with THREE methods, per service.

    The consigna offers three methods "a elección" (z-score, MAD, p-tiles); we
    implement all three so the anomaly mart can report *which* methods agreed and
    *how strong* each signal was — richer than a single flag. A fourth business
    rule (negative cost, R2) is folded in as its own method.

    Per-service statistics (computed in two aggregation passes so the MAD can use
    the service median):

    - **zscore**  — ``|x - mean| / std``; flagged above :data:`ZSCORE_THRESHOLD`.
      Sensitive but distorted by the very outliers it hunts.
    - **mad**     — modified z ``0.6745 * |x - median| / MAD``; flagged above
      :data:`MAD_THRESHOLD`. Robust to outliers. Null when ``MAD == 0`` (a
      degenerate service where the median absolute deviation collapses).
    - **ptiles**  — outside the per-service ``[p01, p99]`` band. Distribution-free.
    - **negative**— ``cost_usd_increment < neg_threshold`` (business rule R2).

    Columns added: per-method score (``score_zscore`` / ``score_mad`` /
    ``score_ptiles`` / ``score_negative``, doubles, null where not applicable),
    per-method boolean flag (``flag_*``), and the overall ``cost_anomaly_flag``
    (OR of the four). Scores feed the ``scores`` map collection in Gold.
    """
    stats1 = df.groupBy("service").agg(
        F.mean("cost_usd").alias("_mean"),
        F.stddev("cost_usd").alias("_std"),
        F.expr("percentile_approx(cost_usd, 0.5)").alias("_median"),
        F.expr("percentile_approx(cost_usd, 0.01)").alias("_p01"),
        F.expr("percentile_approx(cost_usd, 0.99)").alias("_p99"),
    )
    d = df.join(F.broadcast(stats1), on="service", how="left").withColumn(
        "_abs_dev", F.abs(F.col("cost_usd") - F.col("_median"))
    )
    # Second pass: MAD needs the median of the absolute deviations per service.
    stats2 = d.groupBy("service").agg(
        F.expr("percentile_approx(_abs_dev, 0.5)").alias("_mad")
    )
    d = d.join(F.broadcast(stats2), on="service", how="left")

    # Per-method scores (null when the method does not apply for that row/service).
    score_zscore = F.when(
        F.col("_std") > 0,
        F.abs(F.col("cost_usd") - F.col("_mean")) / F.col("_std"),
    )
    score_mad = F.when(
        F.col("_mad") > 0,
        F.lit(0.6745) * F.abs(F.col("cost_usd") - F.col("_median")) / F.col("_mad"),
    )
    # p-tiles score = signed distance outside the band (0 when inside).
    score_ptiles = F.greatest(
        F.col("cost_usd") - F.col("_p99"),
        F.col("_p01") - F.col("cost_usd"),
        F.lit(0.0),
    )
    score_negative = F.when(
        F.col("cost_usd_increment") < F.lit(neg_threshold),
        F.abs(F.col("cost_usd_increment")),
    )

    out = (
        d.withColumn("score_zscore", score_zscore)
        .withColumn("score_mad", score_mad)
        .withColumn("score_ptiles", score_ptiles)
        .withColumn("score_negative", score_negative)
        .withColumn("flag_zscore", F.col("score_zscore") > F.lit(ZSCORE_THRESHOLD))
        .withColumn("flag_mad", F.col("score_mad") > F.lit(MAD_THRESHOLD))
        .withColumn(
            "flag_ptiles",
            (F.col("cost_usd") < F.col("_p01")) | (F.col("cost_usd") > F.col("_p99")),
        )
        .withColumn(
            "flag_negative", F.col("cost_usd_increment") < F.lit(neg_threshold)
        )
    )
    # Nulls (degenerate service, non-applicable method) count as "not flagged".
    flag_cols = ["flag_zscore", "flag_mad", "flag_ptiles", "flag_negative"]
    for c in flag_cols:
        out = out.withColumn(c, F.coalesce(F.col(c), F.lit(False)))

    # CONSENSUS rule: cost distributions per service are heavily right-skewed, so
    # any single statistical method (MAD especially) flags the whole upper tail.
    # We only call an event an anomaly when at least two of the three statistical
    # methods agree, OR the negative-cost business rule (R2) fires on its own —
    # a negative charge is a hard signal, not a statistical guess. This is what
    # makes running three methods worthwhile: agreement, not union.
    stat_votes = (
        F.col("flag_zscore").cast("int")
        + F.col("flag_mad").cast("int")
        + F.col("flag_ptiles").cast("int")
    )
    out = out.withColumn(
        "cost_anomaly_flag",
        (stat_votes >= F.lit(2)) | F.col("flag_negative"),
    )
    return out.drop("_mean", "_std", "_median", "_p01", "_p99", "_abs_dev", "_mad")


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


# --------------------------------------------------------------------------- #
# Gold pure transforms — Soporte, FinOps revenue, Producto/GenAI.
# --------------------------------------------------------------------------- #


def build_gold_tickets(tickets: DataFrame) -> DataFrame:
    """Support mart `tickets_by_org_date`: grain org × severity × day.

    `event_date` is the ticket creation date. SLA breach rate = breached / total.
    `sla_breached` is boolean → cast to int for safe summing (nulls → 0).
    """
    return (
        tickets.withColumn("event_date", F.col("created_at"))
        .withColumn("sla_int", F.coalesce(F.col("sla_breached").cast("int"), F.lit(0)))
        .groupBy("org_id", "event_date", "severity")
        .agg(
            F.count(F.lit(1)).alias("ticket_count"),
            F.sum("sla_int").alias("sla_breach_count"),
            (F.sum("sla_int") / F.count(F.lit(1))).alias("sla_breach_rate"),
            F.avg("csat").alias("avg_csat"),
        )
    )


def build_gold_revenue(billing: DataFrame) -> DataFrame:
    """FinOps mart `revenue_by_org_month`: grain org × month.

    revenue_usd = (subtotal − credits + taxes) × exchange_rate_to_usd.
    credits can be null (no credits applied) → coalesced to 0.
    """
    return billing.select(
        "org_id",
        "month",
        (
            (
                F.col("subtotal")
                - F.coalesce(F.col("credits"), F.lit(0.0))
                + F.coalesce(F.col("taxes"), F.lit(0.0))
            )
            * F.col("exchange_rate_to_usd")
        ).alias("revenue_usd"),
        "subtotal",
        F.coalesce(F.col("credits"), F.lit(0.0)).alias("credits"),
        F.coalesce(F.col("taxes"), F.lit(0.0)).alias("taxes"),
        "currency",
    )


def build_gold_cost_anomaly(silver: DataFrame) -> DataFrame:
    """FinOps mart `cost_anomaly_by_org_date`: grain org × service × day.

    Keeps only the flagged events (the mart *is* the anomalies) and rolls them up
    per org/service/day, emitting two Cassandra **collections** plus a headline
    score:

    - ``scores`` (``map<string,double>`` → Cassandra ``map<text,double>``): the
      strongest score seen per method that fired. Modelling this as a map avoids
      one sparse ``score_<method>`` column per method (mostly nulls), and stays
      extensible if a fourth detector is added.
    - ``methods`` (``array<string>`` → Cassandra ``set<text>``): the detectors
      that fired — derived from the map keys, so set and map never disagree.
    - ``anomaly_score``: the max score across methods — a single sortable number
      that orders the serving table (worst first).

    Only methods that actually fired appear in ``scores`` / ``methods``.
    """
    flagged = silver.filter(F.col("cost_anomaly_flag"))
    agg = flagged.groupBy("org_id", "service", "event_date").agg(
        *[
            F.max(F.when(F.col(f"flag_{m}"), F.col(f"score_{m}"))).alias(f"m_{m}")
            for m in ANOMALY_METHODS
        ],
        F.count(F.lit(1)).alias("event_count"),
        F.first("org_name", ignorenulls=True).alias("org_name"),
    )
    # Build a full {method: max_score} map (fixed keys, some null values), then
    # drop the null-valued entries so only methods that fired remain. `methods`
    # comes from the surviving keys, keeping the set and map consistent.
    full_map = F.create_map(
        *[x for m in ANOMALY_METHODS for x in (F.lit(m), F.col(f"m_{m}"))]
    )
    scores = F.map_filter(full_map, lambda _k, v: v.isNotNull())
    return (
        agg.withColumn("scores", scores)
        .withColumn("methods", F.map_keys(scores))
        .withColumn(
            "anomaly_score",
            F.greatest(
                *[F.coalesce(F.col(f"m_{m}"), F.lit(0.0)) for m in ANOMALY_METHODS]
            ),
        )
        .select(
            "org_id",
            "event_date",
            "service",
            "anomaly_score",
            "methods",
            "scores",
            "event_count",
            "org_name",
        )
    )


def build_gold_genai(silver: DataFrame) -> DataFrame:
    """Producto mart `genai_tokens_by_org_date`: grain org × day, genai only.

    Aggregates token consumption and cost for GenAI service events.
    """
    return (
        silver.filter(F.col("service") == "genai")
        .groupBy("org_id", "event_date")
        .agg(
            F.sum("genai_tokens").alias("total_tokens"),
            F.sum("cost_usd").alias("cost_usd"),
            F.count(F.lit(1)).alias("event_count"),
        )
    )
