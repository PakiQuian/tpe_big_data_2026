-- Cloud Provider Analytics — the five mandatory business queries.
-- Replace the bind values (org id, dates, limit) as needed.

USE cloud_analytics;

-- Query #1: daily cost and requests by organization and service over a date
-- range. Served by org_daily_usage_by_service (partition org_id, range on the
-- event_date clustering column).
SELECT org_id, event_date, service, cost_usd, requests
FROM org_daily_usage_by_service
WHERE org_id = 'org_0lvsnujz'
  AND event_date >= '2025-07-01'
  AND event_date <= '2025-09-01';

-- Query #2: top-N services by accumulated cost over the last 14 days for a
-- specific organization. Served by org_service_cost_14d, whose clustering order
-- (total_cost_usd DESC) makes LIMIT return the top-N directly.
SELECT org_id, service, total_cost_usd
FROM org_service_cost_14d
WHERE org_id = 'org_0lvsnujz'
LIMIT 5;

-- Query #3: evolution of critical tickets and SLA breach rate by day (last 30
-- days). event_date DESC clustering delivers recent days first without ALLOW
-- FILTERING; the client can further filter by severity if needed.
SELECT org_id, event_date, severity, ticket_count, sla_breach_count, sla_breach_rate, avg_csat
FROM tickets_by_org_date
WHERE org_id = 'org_0lvsnujz'
  AND event_date >= '2025-08-02';

-- Query #4: monthly revenue with credits and taxes applied, normalised to USD.
-- month DESC clustering returns the most recent invoice first.
SELECT org_id, month, revenue_usd, subtotal, credits, taxes, currency
FROM revenue_by_org_month
WHERE org_id = 'org_0lvsnujz';

-- Query #5: GenAI token consumption and estimated cost per day.
-- event_date DESC delivers most recent days first.
SELECT org_id, event_date, total_tokens, cost_usd, event_count
FROM genai_tokens_by_org_date
WHERE org_id = 'org_0lvsnujz'
  AND event_date >= '2025-07-01';
