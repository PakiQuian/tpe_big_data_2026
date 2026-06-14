# %% [markdown]
# # Cloud Provider Analytics — End-to-End MVP (Segundo Parcial)
#
# End-to-end pipeline: landing → Bronze → Silver → Gold → Serving (Cassandra).
#
# Built incrementally as vertical slices:
# - #2 tracer bullet — the full spine, minimal at every layer.
# - #3 Bronze masters — all 7 CSV masters via the `cpa.MASTERS` registry.
# - #4 Bronze streaming — usage events via Structured Streaming (watermark,
#   dedup, checkpoint).
# - #5 Silver — conformance, enrichment, 3 DQ rules + quarantine, anomaly flags.
# - #6 Gold — FinOps mart `org_daily_usage_by_service` + 14-day cost rollup.
# - #7 Serving — two query-first Cassandra tables, business queries #1 and #2.
#
# Remaining: idempotency evidence + artifacts (DECISIONS.md, diagram, README,
# ipynb) in #8.

# %% [markdown]
# ## Configuration
#
# A single place that switches local vs Colab and Docker vs AstraDB. The same
# `pipeline.py` runs in both environments by changing only these values.

# %%
import os
from pathlib import Path

# Local vs Colab is auto-detected; override by setting LOCAL manually if needed.
try:
    import google.colab  # noqa: F401

    LOCAL = False
except ImportError:
    LOCAL = True

SERVING_TARGET = os.environ.get("SERVING_TARGET", "docker")  # "docker" | "astra"

if LOCAL:
    # System Java is 25 (too new for Spark); point at the project's Temurin-21.
    os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-21-temurin-jdk")
    BASE = Path("datalake")
else:
    BASE = Path("/content/datalake")

LANDING = BASE / "landing"
BRONZE = BASE / "bronze"
SILVER = BASE / "silver"
GOLD = BASE / "gold"
CHECKPOINTS = BASE / "checkpoints"
QUARANTINE = BASE / "quarantine"

# Cassandra / serving config
CASSANDRA_KEYSPACE = "cloud_analytics"
DOCKER_CONTACT_POINTS = ["127.0.0.1"]
DOCKER_PORT = 9042
ASTRA_BUNDLE = os.environ.get("ASTRA_BUNDLE", "")  # path to secure-connect-bundle.zip
ASTRA_TOKEN = os.environ.get("ASTRA_TOKEN", "")  # AstraDB application token

print(f"LOCAL={LOCAL}  SERVING_TARGET={SERVING_TARGET}  BASE={BASE}")

# %% [markdown]
# ## SparkSession bootstrap

# %%
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("cloud-provider-analytics-tracer")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("Spark", spark.version)

# %%
import cpa
from pyspark.sql import functions as F

# %% [markdown]
# ## Bronze — ingest all 7 masters (parameterized loop)
#
# Each master is read with an explicit schema from the `cpa.MASTERS` registry (no
# inference), deduped on its natural key, stamped with technical columns
# (`ingest_ts`, `source_file`), and written partitioned by `ingest_date` with
# overwrite so a same-day re-run is idempotent. `escape='"'` handles the embedded
# JSON in `resources.tags_json`.


# %%
def ingest_master(spec: "cpa.MasterSpec"):
    """Read one master CSV → typed, deduped Bronze Parquet partitioned by ingest_date."""
    df = (
        spark.read.option("header", True)
        .option("escape", '"')
        .schema(spec.schema)
        .csv(str(LANDING / spec.filename))
        .dropDuplicates(spec.dedupe_keys)
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingest_date", F.current_date())
    )
    (
        df.write.mode("overwrite")
        .partitionBy("ingest_date")
        .parquet(str(BRONZE / "masters" / spec.name))
    )
    return df.count()


for name, spec in cpa.MASTERS.items():
    n = ingest_master(spec)
    print(f"bronze master {name:<18} rows: {n}")

# %% [markdown]
# ## Bronze — usage events via Structured Streaming
#
# Reads `usage_events_stream/*.jsonl` as a stream with the explicit superset
# schema (`cpa.EVENTS_SCHEMA`, v1 ∪ v2). Key behaviors:
#
# - **Watermark = 60 days** on event time. The files arrive in arbitrary
#   timestamp order, so a tight watermark would mark whole out-of-order files as
#   "late" and silently drop them. Sizing it to the historical replay span keeps
#   every event while still bounding dedup state. (Production with real-time
#   arrival would use minutes — see DECISIONS.md.)
# - **Dedup by `event_id`** via `dropDuplicatesWithinWatermark`, defending
#   against re-delivery / reprocessing.
# - **`maxFilesPerTrigger=4` + `availableNow` trigger**: chunks the 120 files
#   into many micro-batches and then stops on its own, so the notebook runs
#   top-to-bottom and terminates.
# - **Checkpointing**: offsets + dedup state under `checkpoints/`. Re-running
#   resumes from committed offsets (no reprocessing) — the idempotency story.
#
# Bronze stays permissive; data-quality rules + quarantine live in Silver (#5).

# %%
events_stream = (
    spark.readStream.schema(cpa.EVENTS_SCHEMA)
    .option("maxFilesPerTrigger", 4)
    .json(str(LANDING / "usage_events_stream"))
    .withColumn("date", F.to_date("timestamp"))
    .withWatermark("timestamp", "60 days")
    .dropDuplicatesWithinWatermark(["event_id"])
)

events_query = (
    events_stream.writeStream.format("parquet")
    .option("path", str(BRONZE / "usage_events"))
    .option("checkpointLocation", str(CHECKPOINTS / "usage_events_bronze"))
    .partitionBy("date")
    .outputMode("append")
    .trigger(availableNow=True)
    .start()
)
events_query.awaitTermination()

bronze_events_count = spark.read.parquet(str(BRONZE / "usage_events")).count()
print("bronze events rows:", bronze_events_count)

# %% [markdown]
# ## Silver — conformance, enrichment, data quality, anomaly flags
#
# Pure transforms from `cpa`:
# 1. `conform_and_enrich` — v1/v2 conformance, features, broadcast LEFT join to
#    the org master.
# 2. `apply_dq_rules` — split clean vs quarantine (R1 event_id null, R3 value
#    without unit); negative cost (R2) is kept and flagged, not quarantined.
# 3. `flag_cost_anomalies` — `cost_anomaly_flag` from negative cost OR per-service
#    p01/p99 outliers.
#
# Quarantined rows (with a `dq_rule` reason) go to a separate zone for samples.


# %%
def read_latest_master(name: str):
    """Read only the most recent `ingest_date` snapshot of a master.

    Masters keep one snapshot per ingest_date in Bronze (audit history). A
    consumer must read the latest snapshot only — reading all partitions would
    duplicate every dimension row and double the enrichment join.
    """
    df = spark.read.parquet(str(BRONZE / "masters" / name))
    latest = df.agg(F.max("ingest_date")).first()[0]
    return df.filter(F.col("ingest_date") == F.lit(latest))


events_b = spark.read.parquet(str(BRONZE / "usage_events"))
orgs_b = read_latest_master("customers_orgs")

enriched = cpa.conform_and_enrich(events_b, orgs_b)
clean, quarantine = cpa.apply_dq_rules(enriched)
silver = cpa.flag_cost_anomalies(clean)

(
    silver.write.mode("overwrite")
    .partitionBy("event_date")
    .parquet(str(SILVER / "usage_events_enriched"))
)
(
    quarantine.write.mode("overwrite")
    .partitionBy("event_date")
    .parquet(str(QUARANTINE / "usage_events"))
)

silver_count = spark.read.parquet(str(SILVER / "usage_events_enriched")).count()
q_df = spark.read.parquet(str(QUARANTINE / "usage_events"))
quarantine_count = q_df.count()
anomaly_count = (
    spark.read.parquet(str(SILVER / "usage_events_enriched"))
    .filter(F.col("cost_anomaly_flag"))
    .count()
)
print("silver rows:", silver_count)
print("quarantine rows:", quarantine_count)
if quarantine_count:
    print("  quarantine reasons:")
    q_df.groupBy("dq_rule").count().show(truncate=False)
print("cost anomaly flagged rows:", anomaly_count)

# %% [markdown]
# ## Gold — FinOps mart + 14-day cost rollup
#
# `build_gold_daily` → `org_daily_usage_by_service` (grain org × service × day,
# all measures + `has_anomaly`). `build_cost_14d` → `org_service_cost_14d`, the
# trailing-14-day per-org/service cost rollup that powers the top-N serving query
# (#7). The window ends at the latest event date in the mart.

# %%
silver_g = spark.read.parquet(str(SILVER / "usage_events_enriched"))

gold_daily = cpa.build_gold_daily(silver_g)
(
    gold_daily.write.mode("overwrite")
    .partitionBy("event_date")
    .parquet(str(GOLD / "org_daily_usage_by_service"))
)
gold_daily = spark.read.parquet(str(GOLD / "org_daily_usage_by_service"))

as_of_date = gold_daily.agg(F.max("event_date")).collect()[0][0]
cost_14d = cpa.build_cost_14d(gold_daily, as_of_date)
(cost_14d.write.mode("overwrite").parquet(str(GOLD / "org_service_cost_14d")))

gold_rows = gold_daily.collect()
print("gold rows:", len(gold_rows))
print(
    f"14d rollup ({as_of_date}): {cost_14d.count()} org/service rows",
)

# %% [markdown]
# ## Serving — Cassandra (Docker for dev, AstraDB for final evidence)
#
# `serving.connect` is the single abstraction that `SERVING_TARGET` switches
# (Docker contact points vs AstraDB secure-connect bundle). Two query-first
# tables are loaded with prepared-statement upserts (idempotent by primary key):
# - `org_daily_usage_by_service` — serves query #1 (date-range slice).
# - `org_service_cost_14d` — serves query #2 (top-N via clustering DESC + LIMIT).

# %%
import serving

session, cluster = serving.connect(
    SERVING_TARGET,
    contact_points=DOCKER_CONTACT_POINTS,
    port=DOCKER_PORT,
    keyspace=CASSANDRA_KEYSPACE,
    astra_bundle=ASTRA_BUNDLE,
    astra_token=ASTRA_TOKEN,
)
serving.create_tables(session)

n_daily = serving.upsert_daily(session, gold_rows)
n_14d = serving.upsert_cost_14d(session, cost_14d.collect())
print("cassandra rows upserted:", n_daily, "daily,", n_14d, "rollup")

# %% [markdown]
# ## Business query #1 — daily cost + requests by org + service over a date range

# %%
import datetime

sample_org = gold_rows[0]["org_id"]
q1_rows = serving.query_daily_by_org(
    session, sample_org, datetime.date(2025, 7, 1), datetime.date(2025, 9, 1)
)
print(f"\nQuery #1 — org={sample_org}, 2025-07-01..2025-09-01  ({len(q1_rows)} rows):")
for row in q1_rows[:10]:
    req = row.requests if row.requests is not None else 0.0
    print(
        f"  {row.event_date}  {row.service:<12} cost_usd={row.cost_usd:8.4f}  requests={req:.0f}"
    )

# %% [markdown]
# ## Business query #2 — top-N services by 14-day accumulated cost for an org

# %%
top_n = serving.query_top_services_14d(session, sample_org, 5)
print(f"\nQuery #2 — top {len(top_n)} services (14d) for org={sample_org}:")
for row in top_n:
    print(f"  {row.service:<12} total_cost_usd={row.total_cost_usd:.2f}")

# %%
cluster.shutdown()
spark.stop()
print(
    "\nPipeline run complete: landing -> Bronze -> Silver -> Gold -> Cassandra -> queries #1/#2 OK"
)
