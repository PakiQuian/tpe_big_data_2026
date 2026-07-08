-- Cloud Provider Analytics — the two mandatory business queries.
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
