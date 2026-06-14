"""Integration tests for the Cassandra serving layer (issue #7).

Run against a local Docker Cassandra (`docker run -p 9042:9042 cassandra:5`).
Skipped automatically if no Cassandra is reachable on localhost:9042.
"""

import datetime as dt

import pytest

import serving

TEST_KEYSPACE = "cloud_analytics_test"


@pytest.fixture(scope="module")
def cass():
    try:
        session, cluster = serving.connect("docker", keyspace=TEST_KEYSPACE)
    except Exception as exc:  # NoHostAvailable etc.
        pytest.skip(f"Docker Cassandra not reachable on localhost:9042 ({exc})")
    serving.create_tables(session)
    session.execute(f"TRUNCATE {serving.DAILY_TABLE}")
    session.execute(f"TRUNCATE {serving.COST_14D_TABLE}")
    yield session
    session.execute(f"DROP KEYSPACE IF EXISTS {TEST_KEYSPACE}")
    cluster.shutdown()


def _daily(org, date, service, cost, requests=None):
    return {
        "org_id": org,
        "event_date": date,
        "service": service,
        "cost_usd": cost,
        "requests": requests,
        "genai_tokens": 0,
        "carbon_kg": None,
        "event_count": 1,
        "has_anomaly": False,
        "org_name": "N",
        "plan_tier": "std",
        "industry": "Edu",
        "hq_region": "us-east",
    }


def test_query1_returns_only_rows_in_date_range(cass):
    rows = [
        _daily("o1", dt.date(2025, 8, 1), "compute", 10.0, 5.0),
        _daily("o1", dt.date(2025, 8, 15), "compute", 20.0),
        _daily("o1", dt.date(2025, 7, 1), "compute", 99.0),  # out of range
    ]
    serving.upsert_daily(cass, rows)
    res = serving.query_daily_by_org(
        cass, "o1", dt.date(2025, 8, 1), dt.date(2025, 8, 31)
    )
    assert sorted(str(r.event_date) for r in res) == ["2025-08-01", "2025-08-15"]


def test_query2_top_services_desc_with_limit(cass):
    rows = [
        {"org_id": "o2", "total_cost_usd": c, "service": s, "org_name": "N"}
        for s, c in [("a", 100.0), ("b", 300.0), ("c", 50.0), ("d", 200.0)]
    ]
    serving.upsert_cost_14d(cass, rows)
    top = serving.query_top_services_14d(cass, "o2", 2)
    # clustering DESC on total_cost_usd + LIMIT 2 -> b(300), d(200)
    assert [r.service for r in top] == ["b", "d"]
    assert [r.total_cost_usd for r in top] == [300.0, 200.0]


def test_upsert_is_idempotent_by_primary_key(cass):
    row = [_daily("o3", dt.date(2025, 8, 1), "compute", 10.0)]
    serving.upsert_daily(cass, row)
    serving.upsert_daily(cass, row)  # second load must not duplicate
    res = serving.query_daily_by_org(
        cass, "o3", dt.date(2025, 8, 1), dt.date(2025, 8, 1)
    )
    assert len(res) == 1
