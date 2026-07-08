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

-- Table 3: serves business query #3 — ticket evolution + SLA breach rate by day
-- (last 30 days). Cluster by event_date DESC so recent days come first.
CREATE TABLE IF NOT EXISTS tickets_by_org_date (
    org_id text,
    event_date date,
    severity text,
    ticket_count bigint,
    sla_breach_count bigint,
    sla_breach_rate double,
    avg_csat double,
    PRIMARY KEY ((org_id), event_date, severity)
) WITH CLUSTERING ORDER BY (event_date DESC, severity ASC);

-- Table 4: serves business query #4 — monthly revenue with credits/taxes
-- normalised to USD. Cluster by month DESC so most recent month comes first.
CREATE TABLE IF NOT EXISTS revenue_by_org_month (
    org_id text,
    month date,
    revenue_usd double,
    subtotal double,
    credits double,
    taxes double,
    currency text,
    PRIMARY KEY ((org_id), month)
) WITH CLUSTERING ORDER BY (month DESC);

-- Table 5: serves business query #5 — GenAI token consumption and estimated cost
-- per day. Cluster by event_date DESC.
CREATE TABLE IF NOT EXISTS genai_tokens_by_org_date (
    org_id text,
    event_date date,
    total_tokens bigint,
    cost_usd double,
    event_count bigint,
    PRIMARY KEY ((org_id), event_date)
) WITH CLUSTERING ORDER BY (event_date DESC);
