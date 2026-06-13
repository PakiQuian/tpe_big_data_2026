# %% [markdown]
# # Cloud Provider Analytics — End-to-End MVP (Segundo Parcial)
#
# **Tracer bullet (issue #2):** thinnest end-to-end path that proves the whole
# spine — landing → Bronze → Silver → Gold → Serving (Cassandra) — and de-risks
# the Spark 4.0 + `cassandra-driver` + Docker integration.
#
# Scope is intentionally minimal: one master, a *batch* event read (real
# Structured Streaming arrives in #4), a single-measure Gold mart, one Cassandra
# table, and business query #1. Feature depth comes in the later slices.

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
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# %% [markdown]
# ## Bronze — ingest the `customers_orgs` master
#
# Explicit schema (no inference), dedupe on the natural key, technical columns
# (`ingest_ts`, `source_file`), partitioned by `ingest_date` with overwrite so a
# same-day re-run is idempotent. The full 7-master loop is issue #3.

# %%
customers_orgs_schema = StructType(
    [
        StructField("org_id", StringType()),
        StructField("org_name", StringType()),
        StructField("industry", StringType()),
        StructField("hq_region", StringType()),
        StructField("plan_tier", StringType()),
        StructField("is_enterprise", BooleanType()),
        StructField("signup_date", DateType()),
        StructField("sales_rep", StringType()),
        StructField("lifecycle_stage", StringType()),
        StructField("marketing_source", StringType()),
        StructField("nps_score", DoubleType()),
    ]
)

orgs_bronze = (
    spark.read.option("header", True)
    .schema(customers_orgs_schema)
    .csv(str(LANDING / "customers_orgs.csv"))
    .dropDuplicates(["org_id"])
    .withColumn("ingest_ts", F.current_timestamp())
    .withColumn("source_file", F.input_file_name())
    .withColumn("ingest_date", F.current_date())
)
(
    orgs_bronze.write.mode("overwrite")
    .partitionBy("ingest_date")
    .parquet(str(BRONZE / "masters" / "customers_orgs"))
)
print("bronze customers_orgs rows:", orgs_bronze.count())

# %% [markdown]
# ## Bronze — usage events via a PLAIN BATCH read
#
# Explicit superset schema (v1 ∪ v2) so v1 rows get null `carbon_kg`/`genai_tokens`
# and `value` is typed as double (Spark otherwise infers it as string). Dedupe by
# `event_id`, partition by event `date`. Structured Streaming replaces this read
# in issue #4.

# %%
events_schema = StructType(
    [
        StructField("event_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("org_id", StringType()),
        StructField("resource_id", StringType()),
        StructField("service", StringType()),
        StructField("region", StringType()),
        StructField("metric", StringType()),
        StructField("value", DoubleType()),
        StructField("unit", StringType()),
        StructField("cost_usd_increment", DoubleType()),
        StructField("schema_version", IntegerType()),
        StructField("carbon_kg", DoubleType()),
        StructField("genai_tokens", LongType()),
    ]
)

events_bronze = (
    spark.read.schema(events_schema)
    .json(str(LANDING / "usage_events_stream"))
    .dropDuplicates(["event_id"])
    .withColumn("date", F.to_date("timestamp"))
)
(
    events_bronze.write.mode("overwrite")
    .partitionBy("date")
    .parquet(str(BRONZE / "usage_events"))
)
print("bronze events rows:", events_bronze.count())

# %% [markdown]
# ## Silver — minimal enrichment join
#
# Broadcast LEFT join events to the org master (events with an unknown org
# survive with null attributes). Full conformance / DQ / quarantine / anomaly is
# issue #5.

# %%
events_b = spark.read.parquet(str(BRONZE / "usage_events"))
orgs_b = spark.read.parquet(str(BRONZE / "masters" / "customers_orgs"))

silver = (
    events_b.join(
        F.broadcast(
            orgs_b.select("org_id", "org_name", "plan_tier", "industry", "hq_region")
        ),
        on="org_id",
        how="left",
    )
    .withColumnRenamed("date", "event_date")
    .withColumn("cost_usd", F.col("cost_usd_increment"))
)
(
    silver.write.mode("overwrite")
    .partitionBy("event_date")
    .parquet(str(SILVER / "usage_events_enriched"))
)
print("silver rows:", silver.count())

# %% [markdown]
# ## Gold — minimal FinOps mart (cost only)
#
# Grain org × service × day. Full measure set (requests, genai_tokens, carbon_kg,
# has_anomaly) + the 14-day rollup is issue #6.

# %%
silver_g = spark.read.parquet(str(SILVER / "usage_events_enriched"))
gold = silver_g.groupBy("org_id", "service", "event_date").agg(
    F.sum("cost_usd").alias("cost_usd"),
    F.first("org_name", ignorenulls=True).alias("org_name"),
    F.first("plan_tier", ignorenulls=True).alias("plan_tier"),
)
(
    gold.write.mode("overwrite")
    .partitionBy("event_date")
    .parquet(str(GOLD / "org_daily_usage_by_service"))
)
gold_rows = gold.collect()
print("gold rows:", len(gold_rows))

# %% [markdown]
# ## Serving — Cassandra (Docker for dev, AstraDB for final evidence)
#
# `get_session` is the single abstraction that `SERVING_TARGET` switches. Writes
# are upserts by primary key, so re-loading is idempotent. Query-first table #2
# and business query #2 arrive in issue #7.

# %%
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args


def get_session(target):
    if target == "docker":
        cluster = Cluster(DOCKER_CONTACT_POINTS, port=DOCKER_PORT)
        session = cluster.connect()
        session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE} "
            "WITH replication = {'class':'SimpleStrategy','replication_factor':1}"
        )
    elif target == "astra":
        cloud = {"secure_connect_bundle": ASTRA_BUNDLE}
        auth = PlainTextAuthProvider("token", ASTRA_TOKEN)
        cluster = Cluster(cloud=cloud, auth_provider=auth)
        session = cluster.connect()
    else:
        raise ValueError(f"unknown SERVING_TARGET: {target}")
    session.set_keyspace(CASSANDRA_KEYSPACE)
    return session, cluster


session, cluster = get_session(SERVING_TARGET)

session.execute(
    """
    CREATE TABLE IF NOT EXISTS org_daily_usage_by_service (
        org_id text,
        event_date date,
        service text,
        cost_usd double,
        org_name text,
        plan_tier text,
        PRIMARY KEY ((org_id), event_date, service)
    )
    """
)

insert_stmt = session.prepare(
    """
    INSERT INTO org_daily_usage_by_service
        (org_id, event_date, service, cost_usd, org_name, plan_tier)
    VALUES (?, ?, ?, ?, ?, ?)
    """
)
params = [
    (
        r["org_id"],
        r["event_date"],
        r["service"],
        float(r["cost_usd"]) if r["cost_usd"] is not None else 0.0,
        r["org_name"],
        r["plan_tier"],
    )
    for r in gold_rows
]
execute_concurrent_with_args(session, insert_stmt, params, concurrency=32)
print("cassandra rows upserted:", len(params))

# %% [markdown]
# ## Business query #1 — daily cost by org + service over a date range

# %%
import datetime

sample_org = gold_rows[0]["org_id"]
q1 = session.prepare(
    """
    SELECT org_id, event_date, service, cost_usd
    FROM org_daily_usage_by_service
    WHERE org_id = ? AND event_date >= ? AND event_date <= ?
    """
)
q1_rows = list(
    session.execute(
        q1, (sample_org, datetime.date(2025, 7, 1), datetime.date(2025, 9, 1))
    )
)
print(f"\nQuery #1 — org={sample_org}, 2025-07-01..2025-09-01  ({len(q1_rows)} rows):")
for row in q1_rows[:15]:
    print(f"  {row.event_date}  {row.service:<12} cost_usd={row.cost_usd:.4f}")

# %%
cluster.shutdown()
spark.stop()
print(
    "\nTracer bullet complete: landing -> Bronze -> Silver -> Gold -> Cassandra -> query #1 OK"
)
