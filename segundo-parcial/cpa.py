"""Cloud Provider Analytics — pure, importable definitions.

This module holds only side-effect-free building blocks (schema registry, dedupe
specs, and — in later slices — pure DataFrame transforms) so they can be unit
tested in isolation without running the pipeline. The driver lives in
``pipeline.py``, which imports from here.
"""

from __future__ import annotations

from dataclasses import dataclass

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
