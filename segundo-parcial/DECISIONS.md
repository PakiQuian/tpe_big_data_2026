# Log de decisiones — Cloud Provider Analytics MVP

Rationale breve de las decisiones de diseño de este pipeline. Construye sobre el
diseño preliminar en `../primer-parcial/primer-parcial.md`.

## Patrón de arquitectura: Lambda, acotado para el MVP

Mantenemos el patrón **Lambda** del parcial 1 (capa batch para los maestros, capa
de velocidad para los usage events), pero **acotamos la capa de velocidad a
landing → Bronze solamente**. Silver, Gold y la carga a Cassandra corren como
**batch** sobre el Parquet de Bronze.

- Por qué: la consigna solo exige Structured Streaming para el salto
  landing→Bronze. Correr el resto como batch hace la idempotencia trivial de
  demostrar (overwrite / upsert) y evita los problemas de estado de streaming y de
  los stream-static joins.
- El job batch de maestros corre **antes** del stream de eventos, así el join de
  enriquecimiento de Silver siempre tiene sus datos dimensionales.
- Un camino full-streaming Silver→Gold→serving sigue siendo el objetivo de
  producción; este es un angostamiento deliberado del MVP, no una reversión de
  arquitectura.

## Particionado

- **Usage events** (Bronze/Silver/Gold): particionados por `date=` (fecha del
  evento). Toda consulta de negocio y el mart tienen grano diario, así que las
  particiones por fecha podan bien.
- **Maestros** (Bronze): particionados por `ingest_date=` (historia de auditoría —
  un snapshot por ingesta). Los consumidores leen el snapshot **más reciente** (ver
  Idempotencia).
- **Sin sub-partición `service=`.** Con solo 43.200 eventos, agregar un split por
  servicio de 6 vías crearía muchos archivos diminutos sin beneficio de consulta;
  `service` queda como columna normal y Gold re-agrega por ella.

## Watermark de streaming: 60 días

`withWatermark("timestamp", "60 days")` sobre el stream de eventos.

- Los 120 archivos JSONL llegan en **orden de timestamp arbitrario**. Un watermark
  avanza a `max(event_time) − umbral`; una vez que un micro-batch contiene un
  evento de fines de agosto, un watermark *ajustado* marcaría todo archivo de julio
  posterior como "tardío" y lo descartaría en silencio (incluso vía
  `dropDuplicatesWithinWatermark`).
- Los datos abarcan ~59 días (2025-07-03 … 2025-08-31), así que un watermark de 60
  días conserva todos los eventos a la vez que acota el estado de dedup (43k
  event_ids es trivial).
- En producción con arribo en tiempo real esto serían minutos; 60 días es lo
  correcto **para el replay histórico**.

## Reglas de calidad de datos y umbrales

Tres reglas activas en la transición Bronze→Silver:

- **R1** `event_id IS NULL` → **quarantine** (estructuralmente roto). Activa pero 0
  hits en este dataset (no hay event_ids nulos) — cableada y testeada igual.
- **R2** `cost_usd_increment < -0.01` → **se conserva + `cost_anomaly_flag`** (una
  anomalía de negocio, no corrupción). 226 eventos con costo negativo en los datos.
- **R3** `value IS NOT NULL AND unit IS NULL` → **quarantine** (unit inconsistente).
  1.978 filas en quarantine, todas etiquetadas `dq_rule = unit_missing_with_value`.

**Anomalía estadística**: `cost_usd` fuera de los percentiles **p01/p99** por
servicio también se marca. Combinada con R2, 624 de 41.222 filas Silver llevan
`cost_anomaly_flag`. Umbral `-0.01` (no `0`) tolera el ruido de punto flotante.

## Cassandra (AstraDB) — claves query-first

Dos tablas, una por consulta obligatoria:

- **`org_daily_usage_by_service`** — `PRIMARY KEY ((org_id), event_date, service)`.
  Sirve la consulta #1 (`WHERE org_id=? AND event_date>=? AND event_date<=?`):
  partición por org, range-slice sobre la columna de clustering `event_date`.
- **`org_service_cost_14d`** — `PRIMARY KEY ((org_id), total_cost_usd, service)
  WITH CLUSTERING ORDER BY (total_cost_usd DESC)`. Sirve la consulta #2 (top-N
  servicios por costo a 14 días): `WHERE org_id=? LIMIT n` devuelve el top-N
  directo, sin sort del lado del cliente.

Carga vía el `cassandra-driver` de Python (no el conector de Spark, cuyo soporte
para Spark 4.0 / Scala 2.13 va atrasado). Desarrollo contra Cassandra en Docker;
evidencia final capturada contra AstraDB vía el switch `SERVING_TARGET`.

## Idempotencia

- **Eventos Bronze**: checkpoint de streaming — re-ejecutar reanuda desde los
  offsets confirmados, así ningún evento se reprocesa.
- **Maestros / Silver / Gold**: `mode("overwrite")`.
- **Tabla daily de Cassandra**: upsert por primary key (sobrescribe in place).
- **Tabla 14d de Cassandra**: su primary key incluye `total_cost_usd`, así que un
  valor cambiado en una re-ejecución insertaría una fila *nueva* y dejaría
  huérfana la vieja. Como es un snapshot completo, hacemos **truncate-y-recarga**
  para mantenerla idempotente.

**Lección aprendida (bug corregido):** los maestros se escriben con
`ingest_date=current_date()` bajo `partitionOverwriteMode=dynamic`. Correr en dos
días calendario distintos dejó **dos** particiones `ingest_date`, así que leer todo
el directorio del maestro duplicaba cada fila dimensional y **duplicaba** el join
de enriquecimiento de Silver (p. ej. un costo por org/día/servicio leía `17.75` en
vez de `8.87`). Fix: `read_latest_master` lee solo el snapshot `ingest_date` más
nuevo. Las re-ejecuciones del mismo día siempre fueron idempotentes; esto hizo
correctas también las corridas cruzando días.
