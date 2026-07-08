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
# 3. **Gold** — seis marts: `org_daily_usage_by_service` (FinOps diario), rollup de
#    costo a 14 días, `tickets_by_org_date` (Soporte), `revenue_by_org_month`
#    (FinOps revenue), `genai_tokens_by_org_date` (Producto/GenAI) y
#    `cost_anomaly_by_org_date` (FinOps anomalías, multi-método).
# 4. **Serving** — seis tablas Cassandra modeladas *query-first* que responden las
#    cinco consultas de negocio obligatorias (#1–#5) más una consulta extra de
#    anomalías de costo.
#
# El patrón es Lambda acotado al MVP: el streaming cubre landing→Bronze y el resto
# corre como batch sobre el Parquet de Bronze. El detalle de cada decisión está en
# el README (sección "Decisiones técnicas").

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
#   minutos — ver el README.)
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
# Transformaciones puras de `cpa`, en este orden:
# 1. `conform` — conformance v1/v2 + features, sin join dimensional.
# 2. `apply_dq_rules` — separa limpios vs quarantine (R1 event_id nulo, R3 value
#    sin unit) sobre los eventos crudos conformados; el costo negativo (R2) se
#    conserva y se marca, no se pone en quarantine.
# 3. `enrich` — broadcast LEFT join al maestro de organizaciones, solo sobre las
#    filas limpias.
# 4. `flag_cost_anomalies` — `cost_anomaly_flag` por costo negativo O outliers
#    p01/p99 por servicio.
#
# La compuerta de calidad vive en la transición Bronze→Silver: las filas en
# quarantine (con un motivo `dq_rule`) son eventos crudos de Bronze y van a una
# zona separada como muestras.


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

# La compuerta de calidad corre sobre los eventos crudos conformados (antes del
# join): las filas en quarantine son eventos de Bronze, sin enriquecer. Solo las
# limpias se enriquecen y siguen a Silver.
conformed = cpa.conform(events_b)
clean, quarantine = cpa.apply_dq_rules(conformed)
silver = cpa.flag_cost_anomalies(cpa.enrich(clean, orgs_b))

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
    print("  quarantine examples (columnas que gatillaron la regla):")
    q_df.select(
        "event_id", "service", "metric", "value", "unit", "dq_rule"
    ).show(5, truncate=False)
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
# ## Gold — mart de Soporte (tickets por org × severidad × día)
#
# Fuente: maestro `support_tickets` ya en Bronze. El grano es org × severidad × día
# de creación del ticket. Las medidas son: conteo de tickets, cantidad y tasa de SLA
# breach, y CSAT promedio (nulo cuando todos los tickets del grupo están sin CSAT).
# La tabla Cassandra está ordenada por `event_date DESC` para que las consultas de
# "últimos N días" sean eficientes sin full-scan.

# %%
tickets_b = read_latest_master("support_tickets")
gold_tickets = cpa.build_gold_tickets(tickets_b)
gold_tickets.write.mode("overwrite").parquet(str(GOLD / "tickets_by_org_date"))
gold_tickets = spark.read.parquet(str(GOLD / "tickets_by_org_date"))
print("gold tickets rows:", gold_tickets.count())

# %% [markdown]
# ## Gold — mart de Revenue FinOps (facturación mensual por org)
#
# Fuente: maestro `billing_monthly` ya en Bronze. Una fila por invoice (org × mes).
# `revenue_usd = (subtotal − credits + taxes) × exchange_rate_to_usd` normaliza
# cualquier moneda a USD. `credits` nulo se interpreta como sin crédito (→ 0).

# %%
billing_b = read_latest_master("billing_monthly")
gold_revenue = cpa.build_gold_revenue(billing_b)
gold_revenue.write.mode("overwrite").parquet(str(GOLD / "revenue_by_org_month"))
gold_revenue = spark.read.parquet(str(GOLD / "revenue_by_org_month"))
print("gold revenue rows:", gold_revenue.count())

# %% [markdown]
# ## Gold — mart de GenAI (tokens y costo estimado por org × día)
#
# Fuente: Silver `usage_events_enriched`, filtrado por `service='genai'`. El grano
# es org × día. Agrega tokens consumidos, costo en USD y cantidad de eventos. Los
# eventos v1 (sin `genai_tokens`) reportan 0 (identidad aditiva, coalesced en Silver).

# %%
gold_genai = cpa.build_gold_genai(silver_g)
gold_genai.write.mode("overwrite").parquet(str(GOLD / "genai_tokens_by_org_date"))
gold_genai = spark.read.parquet(str(GOLD / "genai_tokens_by_org_date"))
print("gold genai rows:", gold_genai.count())

# %% [markdown]
# ## Gold — mart de anomalías de costo (FinOps, multi-método)
#
# Fuente: Silver, ya con los scores/flags de los **tres métodos** (z-score, MAD,
# p-tiles) más la regla de negocio de costo negativo. El grano es org × servicio ×
# día y solo entran los grupos con al menos un evento marcado. Las dos columnas de
# colección resumen la evidencia: `methods` (set) lista qué detectores dispararon y
# `scores` (map) guarda el score más fuerte por método. `anomaly_score` es el máximo
# entre métodos, y ordena la tabla de serving para "peores anomalías primero".

# %%
gold_anomaly = cpa.build_gold_cost_anomaly(silver_g)
gold_anomaly.write.mode("overwrite").parquet(str(GOLD / "cost_anomaly_by_org_date"))
gold_anomaly = spark.read.parquet(str(GOLD / "cost_anomaly_by_org_date"))
print("gold cost-anomaly rows:", gold_anomaly.count())

# %% [markdown]
# ## Serving — Cassandra (Docker para dev, AstraDB para evidencia final)
#
# `serving.connect` es la única abstracción que `SERVING_TARGET` conmuta (contact
# points de Docker vs secure-connect bundle de AstraDB). Seis tablas query-first se
# cargan con upserts vía prepared statements (idempotentes por primary key; las
# tablas cuya PK incluye un score/costo hacen truncate-y-recarga):
# - `org_daily_usage_by_service` — consulta #1 (rango de fechas).
# - `org_service_cost_14d` — consulta #2 (top-N vía clustering DESC + LIMIT).
# - `tickets_by_org_date` — consulta #3 (tickets + SLA por día).
# - `revenue_by_org_month` — consulta #4 (revenue mensual en USD).
# - `genai_tokens_by_org_date` — consulta #5 (tokens GenAI por día).
# - `cost_anomaly_by_org_date` — consulta extra de anomalías (score por método, multi-método).

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
n_tickets = serving.upsert_tickets(session, gold_tickets.collect())
n_revenue = serving.upsert_revenue(session, gold_revenue.collect())
n_genai = serving.upsert_genai(session, gold_genai.collect())
n_anomaly = serving.upsert_cost_anomaly(session, gold_anomaly.collect())
print(
    f"cassandra rows upserted: {n_daily} daily, {n_14d} rollup-14d, "
    f"{n_tickets} tickets, {n_revenue} revenue, {n_genai} genai, "
    f"{n_anomaly} cost-anomaly"
)

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
# ## Consulta de negocio #3 — evolución de tickets críticos + tasa de SLA breach por día (últimos 30 días)

# %%
# Elegimos, para la demo, el org con más tickets dentro de la ventana de 30 días,
# así la consulta devuelve datos representativos (no un org sin tickets recientes).
start_30d = as_of_date - datetime.timedelta(days=29)
tickets_org = (
    gold_tickets.filter(F.col("event_date") >= F.lit(start_30d))
    .groupBy("org_id")
    .agg(F.sum("ticket_count").alias("n"))
    .orderBy(F.desc("n"))
    .first()[0]
)
q3_rows = serving.query_tickets_by_org(session, tickets_org, start_30d)
print(f"\nQuery #3 — org={tickets_org}, últimos 30 días ({start_30d}..{as_of_date})  ({len(q3_rows)} rows):")
for row in q3_rows[:10]:
    csat = f"{row.avg_csat:.2f}" if row.avg_csat is not None else "n/a"
    print(
        f"  {row.event_date}  {row.severity:<8} "
        f"tickets={row.ticket_count}  sla_breach={row.sla_breach_count}  "
        f"breach_rate={row.sla_breach_rate:.1%}  avg_csat={csat}"
    )

# %% [markdown]
# ## Consulta de negocio #4 — revenue mensual con créditos/impuestos normalizado a USD

# %%
revenue_org = gold_revenue.agg(F.first("org_id")).collect()[0][0]
q4_rows = serving.query_revenue_by_org(session, revenue_org)
print(f"\nQuery #4 — org={revenue_org}  ({len(q4_rows)} months):")
for row in q4_rows:
    print(
        f"  {row.month}  revenue_usd={row.revenue_usd:10.2f}  "
        f"subtotal={row.subtotal:.2f}  credits={row.credits:.2f}  "
        f"taxes={row.taxes:.2f}  currency={row.currency}"
    )

# %% [markdown]
# ## Consulta de negocio #5 — tokens GenAI y costo estimado por día

# %%
genai_org = gold_genai.agg(F.first("org_id")).collect()[0][0]
q5_rows = serving.query_genai_by_org(session, genai_org, datetime.date(2025, 7, 1))
print(f"\nQuery #5 — org={genai_org}, genai tokens desde 2025-07-01  ({len(q5_rows)} rows):")
for row in q5_rows[:10]:
    print(
        f"  {row.event_date}  tokens={row.total_tokens:>8}  "
        f"cost_usd={row.cost_usd:8.4f}  events={row.event_count}"
    )

# %% [markdown]
# ## Consulta extra — top anomalías de costo por org (multi-método)
#
# Sobre `cost_anomaly_by_org_date`. `methods` dice qué detectores coincidieron
# (z-score / MAD / p-tiles / negative) y cada `score_*` cuán fuerte fue ese método.
# El clustering por `anomaly_score DESC` devuelve las peores anomalías con un LIMIT.

# %%
anomaly_org = gold_anomaly.agg(F.first("org_id")).collect()[0][0]
qa_rows = serving.query_top_anomalies(session, anomaly_org, 10)
print(f"\nQuery anomalías — top {len(qa_rows)} para org={anomaly_org}:")
for row in qa_rows:
    print(
        f"  {row.event_date}  {row.service:<12} score={row.anomaly_score:7.2f}  "
        f"methods=[{row.methods}]  z={row.score_zscore}  mad={row.score_mad}  "
        f"ptiles={row.score_ptiles}"
    )

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
    ("gold/org_daily_usage_by_service", _parquet_count(GOLD / "org_daily_usage_by_service")),
    ("gold/org_service_cost_14d", _parquet_count(GOLD / "org_service_cost_14d")),
    ("gold/tickets_by_org_date", _parquet_count(GOLD / "tickets_by_org_date")),
    ("gold/revenue_by_org_month", _parquet_count(GOLD / "revenue_by_org_month")),
    ("gold/genai_tokens_by_org_date", _parquet_count(GOLD / "genai_tokens_by_org_date")),
    ("gold/cost_anomaly_by_org_date", _parquet_count(GOLD / "cost_anomaly_by_org_date")),
    ("cassandra org_daily_usage_by_service", _cass_count(serving.DAILY_TABLE)),
    ("cassandra org_service_cost_14d", _cass_count(serving.COST_14D_TABLE)),
    ("cassandra tickets_by_org_date", _cass_count(serving.TICKETS_TABLE)),
    ("cassandra revenue_by_org_month", _cass_count(serving.REVENUE_TABLE)),
    ("cassandra genai_tokens_by_org_date", _cass_count(serving.GENAI_TABLE)),
    ("cassandra cost_anomaly_by_org_date", _cass_count(serving.ANOMALY_TABLE)),
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
    "\nPipeline run complete: landing -> Bronze -> Silver -> Gold -> Cassandra -> queries #1–#5 + anomalías OK"
)
