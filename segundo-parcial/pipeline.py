# %% [markdown]
# # Cloud Provider Analytics — MVP End-to-End (Segundo Parcial)
#
# Pipeline end-to-end sobre los datos de un proveedor de nube: ingestamos lo crudo
# de `landing/`, lo estandarizamos y enriquecemos por capas, y publicamos marts de
# analítica en Cassandra para consumo de BI. El recorrido sigue el modelo de zonas
# del data lake:
#
# 1. **Bronze** — ingesta cruda y tipada: los 7 maestros (batch) y los usage events
#    (Structured Streaming), con columnas técnicas de auditoría.
# 2. **Silver** — conformance v1/v2, enriquecimiento con dimensiones, features de
#    costo/uso, reglas de calidad con quarantine y flags de anomalía.
# 3. **Gold** — el mart FinOps `org_daily_usage_by_service` (grano diario por org y
#    servicio) más un rollup de costo a 14 días.
# 4. **Serving** — dos tablas Cassandra modeladas *query-first* que responden las
#    consultas de negocio #1 y #2.
#
# El patrón es Lambda acotado al MVP: el streaming cubre landing→Bronze y el resto
# corre como batch sobre el Parquet de Bronze. El detalle de cada decisión está en
# `DECISIONS.md`.

# %% [markdown]
# ## Bootstrap de Colab
#
# Solo se ejecuta en Colab: clona el repo, instala las dependencias (incluido un
# JDK 17 que Spark 4 necesita) y se posiciona en `segundo-parcial/`, de modo que
# `cpa.py`, `serving.py` y `datalake/landing` queden en rutas relativas —
# exactamente como en un checkout local. Fuera de Colab no hace nada.

# %%
try:
    import google.colab  # noqa: F401

    IN_COLAB = True
except ImportError:
    IN_COLAB = False

if IN_COLAB:
    import os
    import subprocess

    REPO = "https://github.com/PakiQuian/tpe_big_data_2026.git"
    if not os.path.isdir("tpe_big_data_2026"):
        subprocess.run(["git", "clone", "--depth", "1", REPO], check=True)
    os.chdir("tpe_big_data_2026/segundo-parcial")

    subprocess.run(["apt-get", "-qq", "install", "-y", "openjdk-17-jdk-headless"], check=True)
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
    subprocess.run(["pip", "install", "-q", "pyspark==4.0.*", "cassandra-driver"], check=True)
    print("Bootstrap de Colab listo; cwd:", os.getcwd())

# %% [markdown]
# ## AstraDB en Colab
#
# Colab no tiene Cassandra local, así que el serving va contra **AstraDB**. Esta
# celda (solo en Colab) configura la conexión ANTES de la celda de Configuración:
#
# - El **Application Token** se toma de los **Secrets** de Colab (panel 🔑 a la
#   izquierda): creá un secret llamado `ASTRA_TOKEN` con valor `AstraCS:...` y
#   habilitá su acceso para este notebook. Así el token no queda escrito en el
#   `.ipynb`.
# - El **secure-connect-bundle** (`.zip`) se sube una vez con el file picker (queda
#   en el cwd; si ya está, no lo vuelve a pedir).
#
# Requiere que el keyspace `cloud_analytics` ya exista en la base de AstraDB.

# %%
if IN_COLAB:
    from google.colab import files, userdata

    os.environ["SERVING_TARGET"] = "astra"

    try:
        token = userdata.get("ASTRA_TOKEN").strip()
    except Exception as e:
        raise RuntimeError(
            "Falta el secret ASTRA_TOKEN. Agregalo en el panel 🔑 de Colab "
            "(nombre exacto: ASTRA_TOKEN, valor AstraCS:...) y habilitá su acceso "
            "para este notebook."
        ) from e
    if not token.startswith("AstraCS:"):
        raise RuntimeError(
            "El ASTRA_TOKEN no parece un Application Token válido (debe empezar con "
            "'AstraCS:'). Generá uno en AstraDB → Settings → Tokens con un rol que "
            "pueda escribir (p. ej. Database Administrator) y pegá el valor COMPLETO "
            f"en el secret. Recibido: prefijo={token[:8]!r}, largo={len(token)}."
        )
    os.environ["ASTRA_TOKEN"] = token
    print(f"ASTRA_TOKEN OK (prefijo={token[:8]!r}, largo={len(token)})")

    # Subir el secure-connect-bundle una sola vez (queda en el cwd = segundo-parcial/).
    bundle = next(
        (f for f in os.listdir(".") if f.startswith("secure-connect") and f.endswith(".zip")),
        None,
    )
    if bundle is None:
        print("Subí tu secure-connect-...zip de AstraDB:")
        bundle = next(iter(files.upload()))
    os.environ["ASTRA_BUNDLE"] = os.path.abspath(bundle)
    print("AstraDB configurado; bundle:", os.environ["ASTRA_BUNDLE"])

# %% [markdown]
# ## Configuración
#
# Un único lugar que conmuta local vs Colab y Docker vs AstraDB. El mismo
# `pipeline.py` corre en ambos entornos cambiando solo estos valores.

# %%
import os
from pathlib import Path

# Local vs Colab se autodetecta; forzar LOCAL manualmente si hiciera falta.
try:
    import google.colab  # noqa: F401

    LOCAL = False
except ImportError:
    LOCAL = True

SERVING_TARGET = os.environ.get("SERVING_TARGET", "docker")  # "docker" | "astra"

if LOCAL:
    # El Java del sistema es 25 (demasiado nuevo para Spark); apuntamos al Temurin-21.
    os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-21-temurin-jdk")

# En Colab el bootstrap ya hizo chdir a segundo-parcial/, así que la ruta relativa
# `datalake` sirve igual que en local.
BASE = Path("datalake")

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
    SparkSession.builder.appName("cloud-provider-analytics")
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
# ## Bronze — ingesta de los 7 maestros (loop parametrizado)
#
# Cada maestro se lee con un esquema explícito del registro `cpa.MASTERS` (sin
# inferencia), se deduplica por su clave natural, se sella con columnas técnicas
# (`ingest_ts`, `source_file`) y se escribe particionado por `ingest_date` con
# overwrite, de modo que una re-ejecución el mismo día es idempotente. `escape='"'`
# maneja el JSON embebido en `resources.tags_json`.


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
# ## Bronze — usage events vía Structured Streaming
#
# Lee `usage_events_stream/*.jsonl` como stream con el esquema superset explícito
# (`cpa.EVENTS_SCHEMA`, v1 ∪ v2). Comportamientos clave:
#
# - **Watermark = 60 días** sobre el tiempo de evento. Los archivos llegan en
#   orden de timestamp arbitrario, así que un watermark ajustado marcaría archivos
#   enteros fuera de orden como "tardíos" y los descartaría en silencio.
#   Dimensionarlo al span del replay histórico conserva todos los eventos a la vez
#   que acota el estado de dedup. (En producción con arribo en tiempo real serían
#   minutos — ver DECISIONS.md.)
# - **Dedup por `event_id`** vía `dropDuplicatesWithinWatermark`, defendiendo
#   contra re-entrega / reprocesamiento.
# - **`maxFilesPerTrigger=4` + trigger `availableNow`**: parte los 120 archivos en
#   muchos micro-batches y luego se detiene solo, de modo que el notebook corre de
#   arriba a abajo y termina.
# - **Checkpointing**: offsets + estado de dedup bajo `checkpoints/`. Re-ejecutar
#   reanuda desde los offsets confirmados (sin reprocesar) — la base de la
#   idempotencia.
#
# Bronze se mantiene permisivo; las reglas de calidad + quarantine viven en Silver.

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
# ## Silver — conformance, enriquecimiento, calidad de datos, flags de anomalía
#
# Transformaciones puras de `cpa`:
# 1. `conform_and_enrich` — conformance v1/v2, features, broadcast LEFT join al
#    maestro de organizaciones.
# 2. `apply_dq_rules` — separa limpios vs quarantine (R1 event_id nulo, R3 value
#    sin unit); el costo negativo (R2) se conserva y se marca, no se pone en
#    quarantine.
# 3. `flag_cost_anomalies` — `cost_anomaly_flag` por costo negativo O outliers
#    p01/p99 por servicio.
#
# Las filas en quarantine (con un motivo `dq_rule`) van a una zona separada como
# muestras.


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
# ## Gold — mart FinOps + rollup de costo a 14 días
#
# `build_gold_daily` → `org_daily_usage_by_service` (grano org × servicio × día,
# todas las medidas + `has_anomaly`). `build_cost_14d` → `org_service_cost_14d`, el
# rollup de costo por org/servicio de los últimos 14 días que alimenta la consulta
# de serving top-N. La ventana termina en la fecha de evento más reciente del mart.

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
# ## Serving — Cassandra (Docker para dev, AstraDB para evidencia final)
#
# `serving.connect` es la única abstracción que `SERVING_TARGET` conmuta (contact
# points de Docker vs secure-connect bundle de AstraDB). Dos tablas query-first se
# cargan con upserts vía prepared statements (idempotentes por primary key):
# - `org_daily_usage_by_service` — sirve la consulta #1 (rango de fechas).
# - `org_service_cost_14d` — sirve la consulta #2 (top-N vía clustering DESC + LIMIT).

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
# ## Consulta de negocio #1 — costo diario + requests por org + servicio en un rango de fechas

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
# ## Consulta de negocio #2 — top-N servicios por costo acumulado a 14 días para una org

# %%
top_n = serving.query_top_services_14d(session, sample_org, 5)
print(f"\nQuery #2 — top {len(top_n)} services (14d) for org={sample_org}:")
for row in top_n:
    print(f"  {row.service:<12} total_cost_usd={row.total_cost_usd:.2f}")

# %% [markdown]
# ## Evidencia de idempotencia y particionado
#
# Re-ejecutar todo el pipeline produce conteos de filas idénticos en cada zona: los
# eventos Bronze reanudan desde el checkpoint de streaming (sin reprocesar),
# maestros / Silver / Gold usan overwrite, y las escrituras a Cassandra son upserts
# (la tabla de 14 días hace truncate-y-recarga). Corré este notebook dos veces y
# compará la tabla de abajo — los conteos no cambian. El listado de particiones
# evidencia el "particionado sensato" con rutas y tamaños reales.


# %%
def _parquet_count(path):
    return spark.read.parquet(str(path)).count()


def _cass_count(table):
    return session.execute(f"SELECT COUNT(*) AS c FROM {table}").one().c


def _human(nbytes):
    for unit in ("B", "KB", "MB", "GB"):
        if nbytes < 1024 or unit == "GB":
            return f"{nbytes:.1f}{unit}"
        nbytes /= 1024


masters_latest = sum(read_latest_master(n).count() for n in cpa.MASTERS)

counts = [
    ("bronze/masters (latest snapshots)", masters_latest),
    ("bronze/usage_events", _parquet_count(BRONZE / "usage_events")),
    ("silver/usage_events_enriched", _parquet_count(SILVER / "usage_events_enriched")),
    ("quarantine/usage_events", _parquet_count(QUARANTINE / "usage_events")),
    (
        "gold/org_daily_usage_by_service",
        _parquet_count(GOLD / "org_daily_usage_by_service"),
    ),
    ("gold/org_service_cost_14d", _parquet_count(GOLD / "org_service_cost_14d")),
    ("cassandra org_daily_usage_by_service", _cass_count(serving.DAILY_TABLE)),
    ("cassandra org_service_cost_14d", _cass_count(serving.COST_14D_TABLE)),
]
print("\n=== Idempotency evidence — row counts per zone (identical across re-runs) ===")
for name, n in counts:
    print(f"  {name:<42} {n:>8}")

# %%
print("\n=== Partition evidence (particionado sensato) ===")
for zone in (
    "bronze/usage_events",
    "silver/usage_events_enriched",
    "gold/org_daily_usage_by_service",
):
    zpath = BASE / zone
    parts = sorted(d for d in zpath.iterdir() if d.is_dir() and "=" in d.name)
    total = sum(f.stat().st_size for f in zpath.rglob("*") if f.is_file())
    print(f"\n  {zone}/  ({len(parts)} partitions, {_human(total)} total)")
    for d in parts[:2] + parts[-1:]:
        psize = sum(f.stat().st_size for f in d.rglob("*") if f.is_file())
        print(f"    {d.name:<22} {_human(psize)}")

# %%
cluster.shutdown()
spark.stop()
print(
    "\nPipeline run complete: landing -> Bronze -> Silver -> Gold -> Cassandra -> queries #1/#2 OK"
)
