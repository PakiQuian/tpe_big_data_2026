-- Cloud Provider Analytics — serving schema (query-first)
-- Keyspace: created in the AstraDB UI (free tier) or via this statement on a
-- local Docker Cassandra.

CREATE KEYSPACE IF NOT EXISTS cloud_analytics
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE cloud_analytics;

-- Table 1: serves business query #1 — daily cost + requests by org + service
-- over a date range. Partition by org_id, cluster by (event_date, service).
CREATE TABLE IF NOT EXISTS org_daily_usage_by_service (
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
);

-- Table 2: serves business query #2 — top-N services by 14-day accumulated cost
-- for an org. Cluster by total_cost_usd DESC so WHERE org_id = ? LIMIT n returns
-- the top-N directly, no client-side aggregation.
CREATE TABLE IF NOT EXISTS org_service_cost_14d (
    org_id text,
    total_cost_usd double,
    service text,
    org_name text,
    PRIMARY KEY ((org_id), total_cost_usd, service)
) WITH CLUSTERING ORDER BY (total_cost_usd DESC, service ASC);
