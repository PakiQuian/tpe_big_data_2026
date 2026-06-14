# Decision log — Cloud Provider Analytics MVP

Short rationale for the design choices in this pipeline. Builds on the
preliminary design in `../primer-parcial/primer-parcial.md`.

## Architecture pattern: Lambda, scoped for the MVP

We keep the **Lambda** pattern from parcial 1 (batch layer for masters, speed
layer for usage events), but **scope the speed layer to landing → Bronze only**.
Silver, Gold and the Cassandra load run as **batch** over the Bronze Parquet.

- Why: the rubric only requires Structured Streaming for the landing→Bronze hop.
  Running the rest as batch makes idempotency trivial to demonstrate (overwrite /
  upsert) and avoids streaming-state and stream-static-join pitfalls.
- The masters batch job runs **before** the event stream so the Silver
  enrichment join always has its dimension data.
- A full-streaming Silver→Gold→serving path remains the production target; this
  is a deliberate MVP narrowing, not an architecture reversal.

## Partitioning

- **Usage events** (Bronze/Silver/Gold): partitioned by `date=` (event date).
  Every business query and the mart are daily-grained, so date partitions prune
  well.
- **Masters** (Bronze): partitioned by `ingest_date=` (audit history — one
  snapshot per ingest). Consumers read the **latest** snapshot (see Idempotency).
- **No `service=` sub-partition.** With only 43,200 events, adding a 6-way
  service split would create many tiny files for no query benefit; `service`
  stays a regular column and Gold re-aggregates by it.

## Streaming watermark: 60 days

`withWatermark("timestamp", "60 days")` on the event stream.

- The 120 JSONL files arrive in **arbitrary timestamp order**. A watermark
  advances to `max(event_time) − threshold`; once one micro-batch contains a
  late-August event, a *tight* watermark would mark every later July file as
  "late" and silently drop it (including via `dropDuplicatesWithinWatermark`).
- The data spans ~59 days (2025-07-03 … 2025-08-31), so a 60-day watermark keeps
  every event while still bounding dedup state (43k event_ids is trivial).
- In production with real-time arrival this would be minutes; 60 days is correct
  **for historical replay**.

## Data-quality rules and thresholds

Three active rules on the Bronze→Silver transition:

- **R1** `event_id IS NULL` → **quarantine** (structurally broken). Active but 0
  hits in this dataset (no null event_ids) — wired and unit-tested regardless.
- **R2** `cost_usd_increment < -0.01` → **kept + `cost_anomaly_flag`** (a
  business anomaly, not corruption). 226 negative-cost events in the data.
- **R3** `value IS NOT NULL AND unit IS NULL` → **quarantine** (unit-inconsistent).
  1,978 rows quarantined, all tagged `dq_rule = unit_missing_with_value`.

**Statistical anomaly**: `cost_usd` outside per-service **p01/p99** percentiles is
also flagged. Combined with R2, 624 of 41,222 Silver rows carry
`cost_anomaly_flag`. Threshold `-0.01` (not `0`) tolerates floating-point noise.

## Cassandra (AstraDB) — query-first keys

Two tables, one per mandatory query:

- **`org_daily_usage_by_service`** — `PRIMARY KEY ((org_id), event_date, service)`.
  Serves query #1 (`WHERE org_id=? AND event_date>=? AND event_date<=?`): partition
  by org, range-slice on the `event_date` clustering column.
- **`org_service_cost_14d`** — `PRIMARY KEY ((org_id), total_cost_usd, service)
  WITH CLUSTERING ORDER BY (total_cost_usd DESC)`. Serves query #2 (top-N services
  by 14-day cost): `WHERE org_id=? LIMIT n` returns the top-N directly, no
  client-side sort.

Load via the Python `cassandra-driver` (not the Spark connector, whose Spark 4.0
/ Scala 2.13 support lags). Develop against Docker Cassandra; final evidence
captured against AstraDB via the `SERVING_TARGET` switch.

## Idempotency

- **Bronze events**: streaming checkpoint — re-running resumes from committed
  offsets, so no event is reprocessed.
- **Masters / Silver / Gold**: `mode("overwrite")`.
- **Cassandra daily table**: upsert by primary key (overwrites in place).
- **Cassandra 14d table**: its primary key includes `total_cost_usd`, so a
  changed value on re-run would insert a *new* row and orphan the old one. It is
  a full snapshot, so we **truncate-then-load** to stay idempotent.

**Lesson learned (bug fixed):** masters are written with `ingest_date=current_date()`
under `partitionOverwriteMode=dynamic`. Running on two different calendar days
left **two** `ingest_date` partitions, so reading the whole master directory
duplicated every dimension row and **doubled** the Silver enrichment join (e.g.
one org/day/service cost read `17.75` instead of `8.87`). Fix: `read_latest_master`
reads only the newest `ingest_date` snapshot. Same-day re-runs were always
idempotent; this made cross-day runs correct too.
