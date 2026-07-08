"""Cassandra serving layer.

Connection, DDL, upserts, and the two business queries. This module talks to
Cassandra (so it is side-effecting), but it is importable and Spark-free so the
round-trip can be integration-tested against a local Docker Cassandra.

Rows passed to the upsert helpers are any mapping supporting ``row["col"]`` —
Spark ``Row`` objects and plain dicts both work.
"""

from __future__ import annotations

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

KEYSPACE = "cloud_analytics"
DAILY_TABLE = "org_daily_usage_by_service"
COST_14D_TABLE = "org_service_cost_14d"
TICKETS_TABLE = "tickets_by_org_date"
REVENUE_TABLE = "revenue_by_org_month"
GENAI_TABLE = "genai_tokens_by_org_date"

# Table 1 — query-first for business query #1 (daily cost+requests by org+service
# over a date range): partition by org, cluster by (event_date, service).
CREATE_DAILY = f"""
CREATE TABLE IF NOT EXISTS {DAILY_TABLE} (
    org_id text,
    event_date date,
    service text,
    cost_usd double,
    requests double,
    genai_tokens bigint,
    carbon_kg double,
    event_count bigint,
    has_anomaly boolean,
    org_name text,
    plan_tier text,
    industry text,
    hq_region text,
    PRIMARY KEY ((org_id), event_date, service)
)
"""

# Table 2 — query-first for business query #2 (top-N services by 14-day cost):
# cluster by total_cost_usd DESC so `WHERE org_id=? LIMIT n` returns top-N.
CREATE_COST_14D = f"""
CREATE TABLE IF NOT EXISTS {COST_14D_TABLE} (
    org_id text,
    total_cost_usd double,
    service text,
    org_name text,
    PRIMARY KEY ((org_id), total_cost_usd, service)
) WITH CLUSTERING ORDER BY (total_cost_usd DESC, service ASC)
"""


# Table 3 — query-first for business query #3 (ticket evolution + SLA breach rate
# by day, last 30 days): partition by org, cluster by (event_date DESC, severity).
CREATE_TICKETS = f"""
CREATE TABLE IF NOT EXISTS {TICKETS_TABLE} (
    org_id text,
    event_date date,
    severity text,
    ticket_count bigint,
    sla_breach_count bigint,
    sla_breach_rate double,
    avg_csat double,
    PRIMARY KEY ((org_id), event_date, severity)
) WITH CLUSTERING ORDER BY (event_date DESC, severity ASC)
"""

# Table 4 — query-first for business query #4 (monthly revenue with credits/taxes
# normalised to USD): partition by org, cluster by month DESC.
CREATE_REVENUE = f"""
CREATE TABLE IF NOT EXISTS {REVENUE_TABLE} (
    org_id text,
    month date,
    revenue_usd double,
    subtotal double,
    credits double,
    taxes double,
    currency text,
    PRIMARY KEY ((org_id), month)
) WITH CLUSTERING ORDER BY (month DESC)
"""

# Table 5 — query-first for business query #5 (GenAI tokens + estimated cost per
# day): partition by org, cluster by event_date DESC.
CREATE_GENAI = f"""
CREATE TABLE IF NOT EXISTS {GENAI_TABLE} (
    org_id text,
    event_date date,
    total_tokens bigint,
    cost_usd double,
    event_count bigint,
    PRIMARY KEY ((org_id), event_date)
) WITH CLUSTERING ORDER BY (event_date DESC)
"""


def connect(
    target,
    *,
    contact_points=None,
    port=9042,
    keyspace=KEYSPACE,
    astra_bundle=None,
    astra_token=None,
    create_keyspace=True,
):
    """Return (session, cluster). `target` is "docker" or "astra"."""
    if target == "docker":
        cluster = Cluster(contact_points or ["127.0.0.1"], port=port)
        session = cluster.connect()
        if create_keyspace:
            session.execute(
                f"CREATE KEYSPACE IF NOT EXISTS {keyspace} "
                "WITH replication = {'class':'SimpleStrategy','replication_factor':1}"
            )
    elif target == "astra":
        # AstraDB free tier: the keyspace is created in the UI, not via CQL.
        cluster = Cluster(
            cloud={"secure_connect_bundle": astra_bundle},
            auth_provider=PlainTextAuthProvider("token", astra_token),
        )
        session = cluster.connect()
    else:
        raise ValueError(f"unknown serving target: {target}")
    session.set_keyspace(keyspace)
    return session, cluster


def create_tables(session):
    session.execute(CREATE_DAILY)
    session.execute(CREATE_COST_14D)
    session.execute(CREATE_TICKETS)
    session.execute(CREATE_REVENUE)
    session.execute(CREATE_GENAI)


def _f(x):
    """Cassandra accepts None for null; floats otherwise."""
    return float(x) if x is not None else None


def upsert_daily(session, rows):
    """Upsert mart rows into the daily table. Idempotent by primary key."""
    stmt = session.prepare(
        f"INSERT INTO {DAILY_TABLE} "
        "(org_id, event_date, service, cost_usd, requests, genai_tokens, carbon_kg, "
        "event_count, has_anomaly, org_name, plan_tier, industry, hq_region) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
    )
    params = [
        (
            r["org_id"],
            r["event_date"],
            r["service"],
            _f(r["cost_usd"]),
            _f(r["requests"]),
            r["genai_tokens"],
            _f(r["carbon_kg"]),
            r["event_count"],
            r["has_anomaly"],
            r["org_name"],
            r["plan_tier"],
            r["industry"],
            r["hq_region"],
        )
        for r in rows
    ]
    execute_concurrent_with_args(session, stmt, params, concurrency=32)
    return len(params)


def upsert_cost_14d(session, rows, truncate=True):
    """(Re)load the 14-day cost rollup table.

    This table's primary key includes ``total_cost_usd`` (needed so clustering
    order serves top-N directly). That means a changed cost on re-run would
    insert a NEW row instead of overwriting the old one, leaving stale rows. The
    table is a full snapshot, so we TRUNCATE before loading to stay idempotent.
    """
    if truncate:
        session.execute(f"TRUNCATE {COST_14D_TABLE}")
    stmt = session.prepare(
        f"INSERT INTO {COST_14D_TABLE} (org_id, total_cost_usd, service, org_name) "
        "VALUES (?,?,?,?)"
    )
    params = [
        (r["org_id"], _f(r["total_cost_usd"]), r["service"], r["org_name"])
        for r in rows
    ]
    execute_concurrent_with_args(session, stmt, params, concurrency=32)
    return len(params)


def upsert_tickets(session, rows):
    """Upsert support mart rows. Idempotent by primary key (org, date, severity)."""
    stmt = session.prepare(
        f"INSERT INTO {TICKETS_TABLE} "
        "(org_id, event_date, severity, ticket_count, sla_breach_count, sla_breach_rate, avg_csat) "
        "VALUES (?,?,?,?,?,?,?)"
    )
    params = [
        (
            r["org_id"],
            r["event_date"],
            r["severity"],
            r["ticket_count"],
            r["sla_breach_count"],
            _f(r["sla_breach_rate"]),
            _f(r["avg_csat"]),
        )
        for r in rows
    ]
    execute_concurrent_with_args(session, stmt, params, concurrency=32)
    return len(params)


def upsert_revenue(session, rows):
    """Upsert revenue mart rows. Idempotent by primary key (org, month)."""
    stmt = session.prepare(
        f"INSERT INTO {REVENUE_TABLE} "
        "(org_id, month, revenue_usd, subtotal, credits, taxes, currency) "
        "VALUES (?,?,?,?,?,?,?)"
    )
    params = [
        (
            r["org_id"],
            r["month"],
            _f(r["revenue_usd"]),
            _f(r["subtotal"]),
            _f(r["credits"]),
            _f(r["taxes"]),
            r["currency"],
        )
        for r in rows
    ]
    execute_concurrent_with_args(session, stmt, params, concurrency=32)
    return len(params)


def upsert_genai(session, rows):
    """Upsert GenAI mart rows. Idempotent by primary key (org, event_date)."""
    stmt = session.prepare(
        f"INSERT INTO {GENAI_TABLE} "
        "(org_id, event_date, total_tokens, cost_usd, event_count) "
        "VALUES (?,?,?,?,?)"
    )
    params = [
        (
            r["org_id"],
            r["event_date"],
            r["total_tokens"],
            _f(r["cost_usd"]),
            r["event_count"],
        )
        for r in rows
    ]
    execute_concurrent_with_args(session, stmt, params, concurrency=32)
    return len(params)


def query_daily_by_org(session, org_id, start_date, end_date):
    """Business query #1: daily cost + requests by org + service over a date range."""
    stmt = session.prepare(
        f"SELECT org_id, event_date, service, cost_usd, requests FROM {DAILY_TABLE} "
        "WHERE org_id=? AND event_date>=? AND event_date<=?"
    )
    return list(session.execute(stmt, (org_id, start_date, end_date)))


def query_top_services_14d(session, org_id, n):
    """Business query #2: top-N services by 14-day accumulated cost for an org."""
    stmt = session.prepare(
        f"SELECT org_id, service, total_cost_usd FROM {COST_14D_TABLE} "
        "WHERE org_id=? LIMIT ?"
    )
    return list(session.execute(stmt, (org_id, n)))


def query_tickets_by_org(session, org_id, start_date):
    """Business query #3: ticket evolution + SLA breach rate by day from start_date."""
    stmt = session.prepare(
        f"SELECT org_id, event_date, severity, ticket_count, sla_breach_count, "
        f"sla_breach_rate, avg_csat FROM {TICKETS_TABLE} "
        "WHERE org_id=? AND event_date>=?"
    )
    return list(session.execute(stmt, (org_id, start_date)))


def query_revenue_by_org(session, org_id):
    """Business query #4: monthly revenue with credits/taxes in USD for an org."""
    stmt = session.prepare(
        f"SELECT org_id, month, revenue_usd, subtotal, credits, taxes, currency "
        f"FROM {REVENUE_TABLE} WHERE org_id=?"
    )
    return list(session.execute(stmt, (org_id,)))


def query_genai_by_org(session, org_id, start_date):
    """Business query #5: GenAI tokens + cost per day from start_date."""
    stmt = session.prepare(
        f"SELECT org_id, event_date, total_tokens, cost_usd, event_count "
        f"FROM {GENAI_TABLE} WHERE org_id=? AND event_date>=?"
    )
    return list(session.execute(stmt, (org_id, start_date)))
