# Idempotency & partition evidence

Captured by running `pipeline.py` twice back-to-back (no cleanup between runs).
The pipeline prints this table at the end of every run.

## Row counts per zone — identical across re-runs

| Zone | Run 1 | Run 2 |
|---|---|---|
| bronze/masters (latest snapshots) | 4112 | 4112 |
| bronze/usage_events | 43200 | 43200 |
| silver/usage_events_enriched | 41222 | 41222 |
| quarantine/usage_events | 1978 | 1978 |
| gold/org_daily_usage_by_service | 11050 | 11050 |
| gold/org_service_cost_14d | 262 | 262 |
| cassandra org_daily_usage_by_service | 11050 | 11050 |
| cassandra org_service_cost_14d | 262 | 262 |

Reprocessing does not duplicate: Bronze events resume from the streaming
checkpoint, masters/Silver/Gold overwrite, the Cassandra daily table upserts by
primary key, and the 14d table truncates-then-reloads. Also note conservation:
Silver 41222 + quarantine 1978 = 43200 Bronze events (no rows lost).

## Partition evidence (particionado sensato)

```
bronze/usage_events/  (60 partitions, 193.9MB total)
  date=2025-07-03        2.6MB
  date=2025-07-04        2.6MB
  date=2025-08-31        2.7MB

silver/usage_events_enriched/  (60 partitions, 44.4MB total)
  event_date=2025-07-03  697.6KB
  event_date=2025-07-04  724.0KB
  event_date=2025-08-31  808.6KB

gold/org_daily_usage_by_service/  (60 partitions, 1.6MB total)
  event_date=2025-07-03  26.5KB
  event_date=2025-07-04  26.0KB
  event_date=2025-08-31  27.9KB
```

One `date=` partition per event day (2025-07-03 … 2025-08-31). Bronze is the
largest zone (raw events; streaming micro-batches produce several small files per
partition); Silver is leaner after typing/projection; Gold is tiny after daily
aggregation by org × service.
