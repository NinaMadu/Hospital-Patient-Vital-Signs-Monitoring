# Learning labs (Days 1–3)

Everyone completes all five labs. The lead prepares the lab first, then walks the other two through it.
Code here is throwaway practice, but keep it: it is useful viva preparation.

| Folder | Lab | Lead | Goal |
|---|---|---|---|
| [kafka/](kafka/) | K · Kafka | A | 3-partition topic via CLI, keyed producer in Python, 2 consumers in one group, `kafka-consumer-groups --describe` to see lag |
| [spark_streaming/](spark_streaming/) | S1 · Structured Streaming | A | Kafka source, `from_json`, window + watermark, console sink, restart from checkpoint, Spark UI |
| [spark_batch/](spark_batch/) | S2 · Spark batch | B | CSV + Parquet read, join, groupBy, partitioned Parquet write, JDBC write, `explain()` |
| [airflow/](airflow/) | AF · Airflow | B | FileSensor → PythonOperator → BashOperator (spark-submit), retries, params, clear and rerun |
| [observability/](observability/) | O · Observability | C | JSON logs, prometheus_client metrics, Prometheus scrape, Grafana panel, alert rule |

Useful commands against the running stack:

```bash
# Kafka CLI
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic patient-vitals --from-beginning --property print.key=true --property print.partition=true
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups

# Submit a PySpark lab script to the cluster (labs/ is mounted at /opt/project/labs)
docker compose exec spark-master spark-submit /opt/project/labs/spark_streaming/<script>.py
```

From the host, Python clients connect to Kafka at `localhost:9094`.
