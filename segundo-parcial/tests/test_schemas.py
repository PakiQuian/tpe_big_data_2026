"""Unit tests for the cpa schema registry (issue #3).

Pure tests: they inspect StructType definitions only, so no SparkSession is
needed. They assert external behavior (field names, types, dedupe keys) rather
than implementation details.
"""

import cpa
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    TimestampType,
)

EXPECTED_FIELDS = {
    "customers_orgs": {
        "org_id",
        "org_name",
        "industry",
        "hq_region",
        "plan_tier",
        "is_enterprise",
        "signup_date",
        "sales_rep",
        "lifecycle_stage",
        "marketing_source",
        "nps_score",
    },
    "users": {
        "user_id",
        "org_id",
        "email",
        "role",
        "active",
        "created_at",
        "last_login",
    },
    "resources": {
        "resource_id",
        "org_id",
        "service",
        "region",
        "created_at",
        "state",
        "tags_json",
    },
    "support_tickets": {
        "ticket_id",
        "org_id",
        "category",
        "severity",
        "created_at",
        "resolved_at",
        "csat",
        "sla_breached",
    },
    "marketing_touches": {
        "touch_id",
        "org_id",
        "campaign",
        "channel",
        "timestamp",
        "clicked",
        "converted",
    },
    "nps_surveys": {"org_id", "survey_date", "nps_score", "comment"},
    "billing_monthly": {
        "invoice_id",
        "org_id",
        "month",
        "subtotal",
        "credits",
        "taxes",
        "currency",
        "exchange_rate_to_usd",
    },
}


def test_all_seven_masters_registered():
    assert set(cpa.MASTERS) == set(EXPECTED_FIELDS)
    assert len(cpa.MASTERS) == 7


def test_master_field_names_match_expected():
    for name, expected in EXPECTED_FIELDS.items():
        actual = {f.name for f in cpa.MASTERS[name].schema.fields}
        assert actual == expected, f"{name}: {actual ^ expected}"


def test_dedupe_keys_are_real_fields():
    for name, spec in cpa.MASTERS.items():
        fields = {f.name for f in spec.schema.fields}
        assert set(spec.dedupe_keys) <= fields, f"{name} dedupe keys not in schema"
        assert spec.dedupe_keys, f"{name} has no dedupe key"


def test_nps_surveys_has_composite_dedupe_key():
    assert cpa.MASTERS["nps_surveys"].dedupe_keys == ["org_id", "survey_date"]


def test_explicit_typing_no_inference():
    """Dates/booleans/doubles are explicitly typed, not left as strings."""
    types = {
        ("customers_orgs", "signup_date"): DateType,
        ("customers_orgs", "is_enterprise"): BooleanType,
        ("customers_orgs", "nps_score"): DoubleType,
        ("users", "active"): BooleanType,
        ("users", "last_login"): DateType,
        ("support_tickets", "sla_breached"): BooleanType,
        ("support_tickets", "csat"): DoubleType,
        ("marketing_touches", "timestamp"): DateType,
        ("billing_monthly", "month"): DateType,
        ("billing_monthly", "exchange_rate_to_usd"): DoubleType,
    }
    for (master, field), expected_type in types.items():
        actual = cpa.MASTERS[master].schema[field].dataType
        assert isinstance(actual, expected_type), f"{master}.{field} is {actual}"


def test_filenames_end_with_csv():
    for spec in cpa.MASTERS.values():
        assert spec.filename == f"{spec.name}.csv"


# --------------------------------------------------------------------------- #
# Events (streaming) superset schema
# --------------------------------------------------------------------------- #


def test_events_value_is_double_not_string():
    """The whole point of the explicit schema: Spark infers `value` as string."""
    assert isinstance(EVENTS_FIELD("value"), DoubleType)


def test_events_has_all_v1_and_v2_fields():
    expected = {
        "event_id",
        "timestamp",
        "org_id",
        "resource_id",
        "service",
        "region",
        "metric",
        "value",
        "unit",
        "cost_usd_increment",
        "schema_version",
        "carbon_kg",
        "genai_tokens",
    }
    actual = {f.name for f in cpa.EVENTS_SCHEMA.fields}
    assert actual == expected


def test_events_v2_only_fields_are_nullable():
    for field in cpa.EVENTS_SCHEMA.fields:
        if field.name in {"carbon_kg", "genai_tokens"}:
            assert field.nullable, f"{field.name} must be nullable for v1 events"


def test_events_key_types():
    assert isinstance(EVENTS_FIELD("timestamp"), TimestampType)
    assert isinstance(EVENTS_FIELD("schema_version"), IntegerType)
    assert isinstance(EVENTS_FIELD("carbon_kg"), DoubleType)
    assert isinstance(EVENTS_FIELD("genai_tokens"), LongType)


def EVENTS_FIELD(name):
    return cpa.EVENTS_SCHEMA[name].dataType
