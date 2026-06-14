# Evidencia de idempotencia y particionado

Capturado corriendo `pipeline.py` dos veces seguidas (sin limpiar entre corridas).
El pipeline imprime esta tabla al final de cada corrida.

## Conteo de filas por zona — idéntico entre re-ejecuciones

| Zona | Corrida 1 | Corrida 2 |
|---|---|---|
| bronze/masters (últimos snapshots) | 4112 | 4112 |
| bronze/usage_events | 43200 | 43200 |
| silver/usage_events_enriched | 41222 | 41222 |
| quarantine/usage_events | 1978 | 1978 |
| gold/org_daily_usage_by_service | 11050 | 11050 |
| gold/org_service_cost_14d | 262 | 262 |
| cassandra org_daily_usage_by_service | 11050 | 11050 |
| cassandra org_service_cost_14d | 262 | 262 |

Reprocesar no duplica: los eventos Bronze reanudan desde el checkpoint de
streaming, maestros/Silver/Gold hacen overwrite, la tabla daily de Cassandra hace
upsert por primary key, y la tabla 14d hace truncate-y-recarga. Notar también la
conservación: Silver 41222 + quarantine 1978 = 43200 eventos Bronze (no se pierde
ninguna fila).

## Evidencia de particionado (particionado sensato)

```
bronze/usage_events/  (60 particiones, 193.9MB total)
  date=2025-07-03        2.6MB
  date=2025-07-04        2.6MB
  date=2025-08-31        2.7MB

silver/usage_events_enriched/  (60 particiones, 44.4MB total)
  event_date=2025-07-03  697.6KB
  event_date=2025-07-04  724.0KB
  event_date=2025-08-31  808.6KB

gold/org_daily_usage_by_service/  (60 particiones, 1.6MB total)
  event_date=2025-07-03  26.5KB
  event_date=2025-07-04  26.0KB
  event_date=2025-08-31  27.9KB
```

Una partición `date=` por día de evento (2025-07-03 … 2025-08-31). Bronze es la
zona más grande (eventos crudos; los micro-batches de streaming producen varios
archivos chicos por partición); Silver es más liviana tras el tipado/proyección;
Gold es diminuta tras la agregación diaria por org × servicio.
