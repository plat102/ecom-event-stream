# ecom-event-stream

Real-time e-commerce event streaming pipeline: Kafka → Spark Structured Streaming →
PostgreSQL star schema, with Airflow monitoring the whole path.

| App | What it does | Doc |
|---|---|---|
| `apps/ingestion` | source Kafka → validate → local Kafka → MongoDB | [docs/ingestion-pipeline.md](docs/ingestion-pipeline.md) |
| `apps/processing` | Kafka → Spark → `fact_event` + dims → views → dashboard | [docs/data-processing.md](docs/data-processing.md) |
| `apps/orchestration` | Airflow DAGs watching Kafka, YARN/Spark and the warehouse | [docs/health-monitoring.md](docs/health-monitoring.md) |
| `apps/dashboard` | Streamlit reports over the warehouse | [docs/data-processing.md](docs/data-processing.md) |

## Orchestration

Two monitoring DAGs — `kafka_health_monitor` and `spark_health_monitor` — plus the shared
operators, hooks and callbacks in the `dec` package under `apps/orchestration/`.

```bash
make airflow-build && make airflow-db && make airflow-init   # once
make airflow-up          # webserver on http://localhost:18080 + scheduler
make airflow-seed        # import connections + variables from apps/orchestration/config
make airflow-test        # the orchestration tests, inside the Airflow image
```

`make test` runs the ingestion and processing tests only — the orchestration tests import
`airflow`, so they skip outside the image. Restart Airflow after editing `dec/` — libraries and
plugins load once per process, while DAG files are re-parsed continuously.
