---
author-meta: Mateo Perez de Gracia, Francisco Quian Blanco y Theo Stanfield
keywords: ITBA
output: pdf_document
mainfont: "Calibri"
geometry: margin=3cm
lang: es-AR
indent: true
numbersections: true
header-includes: |
  \providecommand{\xmpquote}[1]{#1}
  \usepackage{graphicx}
  \usepackage{fancyhdr}
  \usepackage{tocloft}
  \usepackage{float}
  \usepackage{indentfirst}
  \usepackage{booktabs}
  \usepackage[table]{xcolor}
  \usepackage{array}
  \usepackage{longtable}
  \definecolor{tblalt}{HTML}{E3EEF9}
  \setlength{\tabcolsep}{8pt}
  \arrayrulecolor{black!35}
  \rowcolors{2}{tblalt}{white}
  \pagestyle{fancy}
  \setlength{\parskip}{1em}
  \lhead{72.80 - Big Data}
  \rhead{Primer Parcial}
  \cfoot{\thepage}
  \renewcommand{\headrulewidth}{0.4pt}
  \renewcommand{\footrulewidth}{0.4pt}
  \renewcommand{\arraystretch}{1.5}
  \setlength{\cftsecnumwidth}{2em}
  \setlength{\cftsubsecnumwidth}{3em}
  \setlength{\cftsubsubsecnumwidth}{4em}
---

```{=latex}
\begin{titlepage}
  \centering
  \includegraphics[width=0.5\textwidth]{itba_logo}\par\vspace{1cm}
  {\textsc{Instituto Tecnológico de Buenos Aires} \par}
  \vspace{1cm}
  {\Large \textsc{Primer Parcial}\par}
  \vspace{1.5cm}
  {\huge\bfseries Cloud Provider Analytics \par}
  {\huge\bfseries (ETL + Streaming + Serving en Cassandra)\par}
  \vspace{2cm}
  {\Large\itshape Perez de Gracia, Mateo (63401)\\
  Quian Blanco, Francisco (63006)\\
  Stanfield, Theo (63403)\par}
  \vfill
  {\large Mosquera, Diego\par}
  \vspace{0.5cm}
  {\large Big Data - 72.80}
  \vfill
  {\large Primer cuatrimestre 2026\par}
\end{titlepage}

\renewcommand{\contentsname}{Tabla de contenidos}
\tableofcontents
```

\newpage

# Introducción

Este documento presenta el diseño arquitectónico preliminar para el proyecto _Cloud Provider Analytics_. El objetivo principal de esta entrega es permitir una **validación temprana** y evaluar la viabilidad de la solución técnica, priorizando un enfoque conciso y accionable que evite la sobre-ingeniería en esta fase inicial.

La propuesta plantea la construcción de un pipeline de datos _end-to-end_ desarrollado con **PySpark** en Google Colab, utilizando **Parquet** como formato de almacenamiento intermedio estructurado en zonas lógicas (Data Lake), y **AstraDB (Cassandra)** como capa de servicio (Serving Layer).

Para satisfacer las distintas exigencias de latencia del negocio, el sistema implementa un patrón **Lambda**. Esta arquitectura permite procesar eficientemente tanto las cargas de trabajo por lotes (procesamiento _batch_ para maestros y facturación) como el flujo continuo de eventos operativos (procesamiento _streaming near real-time_ para eventos de uso), dejando los datos listos para ser consumidos por los equipos de FinOps, Soporte y Producto.

\newpage

# Diagrama de arquitectura de alto nivel

```{=latex}
\begin{figure}[H]
    \centering
    \includegraphics[width=0.8\textwidth]{diagrama.pdf}
    \caption{Diagrama de arquitectura de alto nivel}
    \label{fig:Diagrama de arquitectura de alto nivel}
\end{figure}
```

## Patrón Arquitectónico: Lambda

Se elige el patrón **Lambda** para resolver la doble necesidad del negocio: procesamiento histórico complejo y baja latencia operativa.

- **Batch Layer:** Procesa datos pesados e históricos, como la facturación mensual (`billing_monthly.csv`), los maestros estáticos y las encuestas.
- **Speed Layer (Streaming):** Utiliza PySpark _Structured Streaming_ para ingestar eventos de uso (`usage_events_stream` en `.jsonl`) en micro-lotes (_near real-time_). Gestiona la llegada de datos tardíos con _watermarks_ y evita duplicados filtrando por `event_id`.

## Zonas del Data Lake

El almacenamiento se estructura bajo el modelo Bronze-Silver-Gold, utilizando **Parquet particionado** gestionado por Spark.

- **Landing (Raw Inmutable):** Punto de entrada. Almacena los archivos originales (`.csv` estáticos y `.jsonl` de eventos) sin ninguna modificación.
- **Bronze (Raw Estándar):** Conserva la granularidad original. Aplica tipificación explícita de datos, deduplicación inicial por `event_id` y añade metadatos de auditoría (`ingest_ts`, `source_file`).
  - _Quarantine (Cuarentena):_ Aísla en un directorio Parquet separado los registros con errores técnicos o que no cumplen reglas estrictas de calidad (ej. `event_id` nulo o tipos de datos malformados) detectados en la transición de Bronze a Silver.
- **Silver (Conformado):** Capa de limpieza y normalización. Realiza cruces (_joins_) con tablas de dimensiones, gestiona nulos y maneja la evolución del esquema (compatibilizando las versiones 1 y 2 ante la aparición de `carbon_kg` y `genai_tokens`).
  - _Detección de Anomalías:_ A diferencia de los datos corruptos que van a cuarentena, las excepciones lógicas de negocio (ej. un pico inusual de consumo o costo) se calculan en esta capa utilizando percentiles como métodos estadísticos. Estos registros se conservan y se les agrega un _flag_ de anomalía para que sigan su curso.
- **Gold (Marts de Negocio):** Datos agregados bajo un enfoque _query-first_. Contiene los marts de **FinOps** (que recibe los _flags_ en su `cost_anomaly_mart`), **Soporte** y **Producto/Usage** optimizados y listos para ser exportados a la Serving Layer (AstraDB/Cassandra).

\newpage

# Mapeo de Requisitos a Componentes

## Requisitos Analíticos (Consultas de Negocio)

Para asegurar que el diseño de la Serving Layer sea verdaderamente accionable y cumpla con el modelo *query-first* requerido en AstraDB, la arquitectura garantiza los *Marts* necesarios en la capa Gold para responder eficientemente las siguientes consultas críticas de los dashboards:

1.  Costos y solicitudes (*requests*) diarios por organización y por servicio en un rango de fechas determinado.
2.  Top-N de servicios según su costo acumulado en los últimos 14 días para una organización específica.
3.  Evolución diaria de tickets críticos y la tasa de incumplimiento de SLA (*SLA breach*) durante los últimos 30 días.
4.  Ingresos (*revenue*) mensuales totales, contemplando créditos e impuestos aplicados, normalizados a USD.
5.  Cantidad de tokens de GenAI consumidos por día y su respectivo costo estimado.

## Requisitos de Negocio y Técnicos → Diseño

| Requisito Clave                                            | Componente / Decisión de Diseño                                                                                                                                                 |
| :--------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Near real-time** para métricas operativas (uso/consumos) | **PySpark Structured Streaming** leyendo el directorio `usage_events_stream/*.jsonl`, manejando datos tardíos (_late data_) con `withWatermark` y deduplicación por `event_id`. |
| **Batch diario/mensual** para maestros y facturación       | **PySpark Batch** leyendo archivos `.csv` inmutables desde Landing para procesarlos hacia la capa Bronze.                                                                       |
| Almacenamiento intermedio y particionado                   | Almacenamiento intermedio en formato **Parquet particionado** (por ejemplo, por `date=` y/o `service=`) gestionado íntegramente por Spark.                                      |
| Datos crudos con inconsistencias y nulos                   | Implementación de reglas de validación estrictas entre Bronze y Silver; los registros que fallan se desvían a un directorio **Quarantine** en Parquet aparte.                   |
| **Evolución de esquema** a mitad del histórico             | Compatibilización de versiones (`schema_version` v1 y v2) en la capa Silver para integrar armónicamente los nuevos campos `carbon_kg` y `genai_tokens`.                         |
| Detección de anomalías en costos                           | Transformaciones en Silver utilizando métodos estadísticos (Z-score, MAD o percentiles) para generar un _flag_ de anomalía en costos atípicos.                                  |
| Idempotencia (reprocesar sin duplicar)                     | Uso de _checkpointing_, claves naturales y _upserts_ en los flujos de escritura para garantizar la consistencia ante reprocesos.                                                |
| Serving y consultas para Dashboards (BI)                   | **AstraDB (Cassandra)** con tablas modeladas bajo un enfoque _query-first_, cargadas usando el conector de Spark para Cassandra o `foreachBatch` + driver Python.               |

## Las 5Vs del Big Data y cómo las cubre la solución

| V             | Contexto en el Proyecto                                                                                                            | Enfoque en la Arquitectura                                                                                                                                           |
| :------------------ | :---------------------------------------------------- | :------------------------------------------------------- |
| **Volumen**   | Archivos CSV históricos (ej. facturación mensual) y eventos JSONL intencionalmente fragmentados para simular micro-lotes.          | Uso de **Parquet particionado**, particionado sensato y operaciones de `coalesce` o `repartition` para optimizar el rendimiento del motor Spark.                     |
| **Velocidad** | Coexistencia de métricas operativas _near real-time_ y procesos de maestros _batch_ diarios/mensuales.                             | Implementación del patrón **Lambda**, combinando _Structured Streaming_ (con ventanas y _watermarks_) y _Jobs Batch_ independientes.                                 |
| **Variedad**  | Múltiples formatos originales (CSV, JSONL) y mutación de la estructura de datos (evolución de esquema a partir de ~45 días atrás). | Data Lake con capas definidas: tipificación explícita en **Bronze** y normalización/compatibilización de esquemas en **Silver**.                                     |
| **Veracidad** | Presencia de datos ruidosos, nulos, tipos ambiguos y valores atípicos (ej. incrementos de costo negativos ocasionales).            | Filtros de calidad hacia **Quarantine** para aislar errores técnicos, y cálculo de _flags_ para derivar excepciones de negocio a la capa Gold (`cost_anomaly_mart`). |
| **Valor**     | Necesidad de responder preguntas analíticas específicas de FinOps, Soporte y Producto (GenAI) mediante dashboards.                 | Capa **Gold** con _Marts_ de negocio altamente agregados, exportados a **Cassandra** y listos para responder velozmente mediante sentencias CQL.                     |

\newpage

# Flujos de Datos (Data Pipelines)

A continuación se presenta el flujo de datos, dividido lógicamente en las dos ramas de la arquitectura Lambda (Batch y Streaming). Ambas rutas procesan la información a través de las capas del Data Lake (en formato Parquet) y convergen finalmente en la capa de servicio.

## Pipeline Batch (Maestros y Facturación)

| Tramo               | Herramienta y Función                                 | Notas y Justificación                                                                                |
| ------------------------------- | ----------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| **CSV → Bronze**    | **PySpark** (`read.csv` → cast → `write.parquet`)     | Lectura de maestros y facturación estáticos; particionado por `ingest_date` o entidad.      |
| **Bronze → Silver** | **PySpark Batch** (Filtros y Joins)                   | Desvío de filas inválidas a **Quarantine**; unificación de campos para esquema v1/v2.       |
| **Silver → Gold**   | **PySpark Batch** (Agregaciones)                      | Cálculo de métricas mensuales y pre-cálculo de *flags* de anomalías; escritura en Parquet.  |
| **Gold → AstraDB**  | **Spark Cassandra Connector**                         | Carga masiva de los *marts* finalizados hacia las tablas diseñadas *query-first* en Cassandra.|

## Pipeline Streaming (Eventos de Uso)

| Tramo               | Herramienta y Función                                 | Notas y Justificación                                                                                |
| ------------------------------- | ----------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| **JSONL → Bronze**  | **Structured Streaming** (`readStream` + esquema explícito + `withWatermark`)| Ingesta continua de `usage_events_stream/*.jsonl`; manejo de *late data* y dedup por `event_id`.|
| **Bronze → Silver** | **Structured Streaming** + micro-batches              | Desvío a **Quarantine**; merges idempotentes para enriquecer eventos en tiempo real.        |
| **Silver → Gold**   | **Structured Streaming** (Agregaciones por ventana)   | Generación de métricas *near real-time* (ej. consumo GenAI y costos incrementales).         |
| **Gold → AstraDB**  | **Spark** (`foreachBatch` + driver Python)            | Escritura incremental (*upserts* o re-escrituras idempotentes) para actualizar dashboards al instante.|

\newpage

# Supuestos y Riesgos Iniciales

Para delimitar el alcance del diseño preliminar y evitar la sobre-ingeniería, se establecen los siguientes supuestos operativos y se identifican los riesgos técnicos más críticos junto con sus respectivas estrategias de mitigación.

## Supuestos

*   **Volumen de datos manejable en desarrollo:** Se asume que, para la fase de construcción y demostración en Google Colab, el volumen total de datos crudos no excederá los límites de memoria/almacenamiento de un único nodo de procesamiento (ej. < 10 GB en total). Para un entorno productivo real, esto escalaría de forma transparente gracias a Spark.
*   **Evolución de esquema aditiva:** Se asume que el cambio de esquema (`schema_version` v1 a v2, con la aparición de `carbon_kg` y `genai_tokens`) es estrictamente aditivo y no altera de forma destructiva los tipos de datos de las columnas preexistentes.
*   **Conectividad con la Serving Layer:** Se da por sentado que el entorno de ejecución (Colab) tendrá salida a internet estable para establecer la conexión mediante el driver de Python/conector de Spark hacia los servidores de AstraDB (Cassandra).

## Riesgos y Mitigaciones

*   **Riesgo 1: Cuellos de botella de memoria (OOM) en cruces de datos (Joins).** Al unir la tabla de eventos de uso (alta cardinalidad) con maestros como organizaciones o usuarios en la capa Silver, Spark podría quedarse sin memoria.
    *   *Mitigación:* Forzar el uso de *Broadcast Joins* para las tablas de dimensiones pequeñas (maestros) y asegurar un particionado adecuado (por fecha o servicio) al leer desde Bronze.
*   **Riesgo 2: Latencia en el procesamiento de Streaming sin un broker dedicado.** Al no contar con herramientas como Kafka o Pub/Sub y depender de leer archivos `.jsonl` de un directorio para simular el stream, se puede generar latencia o bloqueos en la lectura.
    *   *Mitigación:* Ajustar el *trigger* del micro-batch a intervalos controlados (ej. 1 a 5 minutos) y confiar en el manejo robusto de *watermarks* para acotar el tiempo que Spark espera por datos tardíos (*late data*).
*   **Riesgo 3: Duplicación de datos ante fallos del pipeline.** Si una celda de Colab falla a la mitad de una escritura hacia Gold o AstraDB, al re-ejecutar se podrían duplicar registros de costos o uso.
    *   *Mitigación:* Implementar estrictas medidas de **idempotencia** exigidas por el negocio: configurar directorios de *checkpointing* persistentes, usar `event_id` como clave natural y ejecutar *upserts* en lugar de simples *appends* al cargar en Cassandra.

\newpage

# Estimación de esfuerzo y recursos

| Frente                         | Alcance incluido                                                                      | Horas estimadas       |
| ------------------------------ | ------------------------------------------------------------------------------------- | ---------------------------------- |
| Arquitectura + notebooks Spark | Lake por zonas, batch + Structured Streaming, quarantine, reproducibilidad en Colab   | **25–45**             |
| Gold + calidad de datos        | Marts FinOps/Soporte/GenAI, reglas DQ, anomalías (percentiles robustos), idempotencia | **15–30**             |
| Cassandra / AstraDB            | Diseño tablas por consulta, carga desde Spark, 5 consultas demo con capturas          | **12–22**             |
| Demo + comunicación            | Presentación, video, ajustes de credenciales y paths                                  | **10–18**             |
| Buffer integración             | Colab + I/O + re-ejecuciones por esquema/credenciales                                 | **10–20**             |
| **Total orientativo**          |                                                                                       | **≈ 70–135 h** equipo |

**Roles sugeridos para el equipo:**

- **Data Engineer (Ingesta y Pipelines):** Encargado de la lectura de fuentes, el particionado, y la implementación técnica de las ramas batch (maestros y facturación) y *Structured Streaming* (eventos de uso) hacia Bronze.
- **Data Engineer (Calidad y Transformación):** Responsable de la lógica de negocio en la capa Silver, la gestión de la evolución de esquema (v1 a v2), las reglas hacia *Quarantine* y el cálculo estadístico de anomalías.
- **Data Architect / Integración End-to-End:** Encargado del modelado NoSQL *query-first* en Cassandra, la estrategia de carga (upserts/idempotencia), la ejecución de las consultas CQL de prueba y la orquestación general del notebook para asegurar que el pipeline funcione como un producto cohesivo.

\newpage

# Conclusión

El diseño preliminar detallado en este documento establece una base sólida, visual y accionable para la implementación del proyecto *Cloud Provider Analytics*. La elección de una **Arquitectura Lambda** resulta ser el enfoque más pragmático para satisfacer el doble requerimiento de latencia operativa (*near real-time*) y procesamiento histórico pesado, evitando introducir complejidades innecesarias en esta fase temprana de validación.

Al estructurar el almacenamiento intermedio en Parquet bajo el modelo de zonas (Bronze, Silver, Gold) y delegar la capa de servicio a una base de datos de alta velocidad como AstraDB, se garantiza la separación de responsabilidades. El diseño propuesto asegura la resiliencia del pipeline ante datos inconsistentes, soporta la evolución dinámica de los esquemas, e implementa la idempotencia necesaria para reprocesar información sin comprometer la integridad. 

Con los flujos de datos delineados y los riesgos iniciales mitigados, el equipo cuenta con una hoja de ruta clara para iniciar el desarrollo del código en PySpark, con la certeza de que la infraestructura resultante será capaz de alimentar de manera confiable los tableros analíticos de FinOps, Soporte y Producto.