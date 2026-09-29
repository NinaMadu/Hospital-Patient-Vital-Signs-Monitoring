# Runbook: start the pipeline and see its outputs

How to start the whole stack, check that it is healthy, and look at the output of every stage,
from the simulated bedside monitor to the daily risk report. For the demo video script, see
[DEMO.md](DEMO.md).

All commands are for **PowerShell**, run in the project folder. Git Bash rewrites container
paths such as `/data/lake`; if you must use it, run `export MSYS_NO_PATHCONV=1` first.

## Contents

1. [The pipeline at a glance](#1-the-pipeline-at-a-glance)
2. [One-time setup](#2-one-time-setup)
3. [Start the pipeline](#3-start-the-pipeline)
4. [Check it is healthy](#4-check-it-is-healthy)
5. [See the outputs, stage by stage](#5-see-the-outputs-stage-by-stage)
6. [Trigger a demo scenario](#6-trigger-a-demo-scenario)
7. [Stop, restart, reset](#7-stop-restart-reset)
8. [Troubleshooting](#8-troubleshooting)

---

## 1. The pipeline at a glance

| # | Stage | Output | Where to look |
|---|---|---|---|
| 1 | Vital simulator → Kafka | JSON readings in `patient-vitals` (3 partitions, key = patient_id) | Kafka UI, console consumer |
| 2 | Spark Q1: validate, windows, trend, risk | `patient_current_status`, `patient_vital_windows`; bad events → `vitals-dlq` | Spark UI, Postgres, API |
| 3 | Spark Q2: archive | Parquet lake `/data/lake/vitals/sim_day=N/` | `spark-sql` |
| 4 | Spark Q3: alerts → Kafka → alert consumer | `patient-alerts` topic → `vital_alerts` table | Kafka UI, Postgres, API |
| 5 | Lab simulator (once per sim day) | `data/landing/labs/labs_day=N.csv` + `_SUCCESS` marker | File Explorer |
| 6 | Airflow `daily_lab_consolidation` | `lab_results`, `vital_daily_summary`, `daily_patient_risk` | Airflow UI, Postgres |
| 7 | Report generator (last DAG task) | `data/reports/risk_report_day=N.html` / `.csv` | Browser |
| 8 | FastAPI serving layer | Merged speed + batch view | http://localhost:8000/docs |
| 9 | Observability | Metrics, dashboard, alert rules | Grafana, Prometheus |

**Simulated clock:** 1 simulated day = **5 real minutes** (`SIM_DAY_SECONDS=300` in `.env`).
A new lab file and an Airflow run therefore arrive every 5 minutes, while vitals stream in
real time.

---

## 2. One-time setup

1. **Docker Desktop**, with at least 8 GB of RAM for Docker (10 GB recommended). On Windows,
   create `%UserProfile%\.wslconfig`:
   ```ini
   [wsl2]
   memory=10GB
   processors=6
   ```
   Then run `wsl --shutdown` and restart Docker Desktop.
2. **Configuration:**
   ```powershell
   Copy-Item .env.example .env
   ```
   If a port is already in use on your PC, see [Troubleshooting](#8-troubleshooting).
3. **Build the images.** The first build takes several minutes:
   ```powershell
   docker compose --profile monitoring build
   ```

---

## 3. Start the pipeline

```powershell
docker compose --profile monitoring up -d
```

This starts everything:
- the core services: Kafka, Spark, the streaming app, Airflow, Postgres, the API, the two
  simulators and the alert consumer;
- the monitoring profile: Prometheus, Pushgateway, kafka-exporter and Grafana.

To start only the core, run `docker compose up -d`.

**Allow 2–3 minutes** for the services to become healthy. **Allow about 15 minutes** (3 simulated
days) before expecting full outputs:
- trends need 3 consecutive windows;
- the first Airflow run needs a complete day of lab file and lake data;
- the Grafana graphs need some history.

---

## 4. Check it is healthy

```powershell
docker compose --profile monitoring ps -a
```

Every service should be `Up` or `Up (healthy)`. `init-dirs` and `kafka-init` should be
`Exited (0)`, because they are one-off setup jobs.

Quick checks:

```powershell
curl.exe -s localhost:8000/health        # {"status":"ok","postgres":"ok"}
curl.exe -s localhost:8000/api/metrics   # current_sim_day, data freshness, risk categories
```

In the `/api/metrics` output, look for:
- `status_age_seconds` and `last_reading_age_seconds` of a few seconds (the speed layer is live);
- `latest_lab_sim_day` equal to `current_sim_day - 1`, and `latest_risk_sim_day` equal to
  `current_sim_day - 1` or `- 2` (the batch layer is keeping up);
- `recent_failures` equal to 0.

### Web UIs

| UI | URL | Login |
|---|---|---|
| Kafka UI | http://localhost:8085 | — |
| Spark master | http://localhost:8090 | — |
| Spark streaming app (Structured Streaming tab) | http://localhost:4040/StreamingQuery/ | — |
| Airflow | http://localhost:8080 | `admin` / `admin` |
| API (Swagger, "Try it out") | http://localhost:8000/docs | — |
| Grafana dashboard | http://localhost:3000/d/ward-vitals-overview/ward-vitals-pipeline | `admin` / `admin` |
| Prometheus alerts | http://localhost:9090/alerts | — |

---

## 5. See the outputs, stage by stage

### 5.1 Vital simulator → Kafka

Producer logs (one JSON log line per event batch; press Ctrl+C to stop following):

```powershell
docker compose logs -f --tail 5 vital-producer
```

Topic layout: 3 partitions, as the output shows.

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic patient-vitals
```

A few live messages with their key and partition:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 `
    --topic patient-vitals --max-messages 5 --property print.key=true --property print.partition=true
```

The same patient always lands on the same partition, which keeps each patient's readings in
order. In the Kafka UI, go to **Topics → patient-vitals → Messages**.

Invalid events go to the dead-letter topic, each with a `reason`. The normal producer sends
none, so start it with `--malformed-rate 0.02` to see some. Then:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 `
    --topic vitals-dlq --from-beginning --max-messages 5 --timeout-ms 10000
```

### 5.2 Spark Structured Streaming (speed layer)

On http://localhost:4040/StreamingQuery/ you should see these queries: `q1_windows`, `q1_dlq`,
`q2_archive`, `q3_threshold_alerts` and `q3_sustained_alerts`. Click one to see its:
- Input Rate and Process Rate;
- Batch Duration, which should stay well under the 10 s trigger;
- Global Watermark Gap.

**Q1 output:** each patient's current 2-minute window, trend and risk.

```powershell
docker compose exec postgres psql -U ward -d ward -c "SELECT patient_id, round(avg_heart_rate::numeric) hr, min_spo2, hr_trend, spo2_trend, vital_risk_score, vital_risk_category FROM patient_current_status ORDER BY vital_risk_score DESC LIMIT 8;"
```

Window history, used for trends:

```powershell
docker compose exec postgres psql -U ward -d ward -c "SELECT * FROM patient_vital_windows WHERE patient_id = 'P007' ORDER BY window_start DESC LIMIT 5;"
```

### 5.3 Parquet lake (Q2, the batch layer's source of truth)

The following command counts readings per simulated day. Run it with `-w /tmp`, because the
project folder is mounted read-only inside the container.

```powershell
docker compose exec -T -w /tmp spark-worker /opt/spark/bin/spark-sql --master 'local[1]' -e 'SELECT sim_day, count(*) AS readings, count(DISTINCT patient_id) AS patients FROM parquet.`/data/lake/vitals` GROUP BY sim_day ORDER BY sim_day DESC LIMIT 5' 2>$null
```

Expected: about 15 patients per day, and roughly 1,000–1,500 readings per complete day.

### 5.4 Alerts (Q3 → Kafka → alert consumer → Postgres)

Newest alerts, through the API:

```powershell
curl.exe -s "localhost:8000/api/alerts?limit=5"
curl.exe -s "localhost:8000/api/alerts?severity=CRITICAL&limit=5"
```

Alerts per severity and type, straight from Postgres:

```powershell
docker compose exec postgres psql -U ward -d ward -c "SELECT severity, alert_type, count(*) FROM vital_alerts GROUP BY 1, 2 ORDER BY 1, 2;"
```

There are two kinds of alert:
- **WARNING** (`THRESHOLD`): one reading outside a limit.
- **CRITICAL** (`SUSTAINED`): the same limit broken in 3 or more readings within 1 minute.

The normal ward mostly produces WARNINGs; section 6 shows how to force CRITICAL ones. The raw
alert messages are also visible in the Kafka UI under the `patient-alerts` topic.

### 5.5 Daily lab file (batch source)

```powershell
Get-ChildItem data\landing\labs | Sort-Object LastWriteTime | Select-Object -Last 4 Name
Get-Content data\landing\labs\labs_day=N.csv -TotalCount 5      # replace N with a day from the list
```

A new `labs_day=N.csv` and its `labs_day=N.csv._SUCCESS` marker appear every 5 minutes. Each
row carries its own reference range.

### 5.6 Airflow daily batch

Open http://localhost:8080 → `daily_lab_consolidation` → **Grid** or **Graph**. It runs once per
simulated day, with these tasks:

```text
resolve_sim_day ─┬─> wait_for_lab_file -> validate_lab_file -> load_lab_results ─┐
                 └─> wait_for_lake_settle -> vital_daily_summary (Spark) ────────┴─> risk_consolidation (Spark) -> generate_report
```

Click any task square → **Logs** to see its JSON log lines. From the command line:

```powershell
docker compose exec airflow airflow dags list-runs -d daily_lab_consolidation -o table
```

Re-run a specific day, where N is a day that already has a lab file. Loading replaces that
day's rows, so re-running never creates duplicates.

```powershell
docker compose exec airflow airflow dags trigger daily_lab_consolidation -c '{\"sim_day\": N}'
```

In the UI, the ▶ **Trigger DAG w/ config** button does the same. A run takes about 1–2 minutes.

**Batch outputs in Postgres:**

```powershell
# Labs loaded per day
docker compose exec postgres psql -U ward -d ward -c "SELECT sim_day, count(*) rows, count(DISTINCT patient_id) patients FROM lab_results GROUP BY 1 ORDER BY 1 DESC LIMIT 3;"

# Daily vital summary (computed by Spark from the lake)
docker compose exec postgres psql -U ward -d ward -c "SELECT * FROM vital_daily_summary ORDER BY sim_day DESC, patient_id LIMIT 5;"

# Combined risk: vitals + labs
docker compose exec postgres psql -U ward -d ward -c "SELECT sim_day, patient_id, vital_risk_score, lab_risk_score, total_risk_score, risk_category, lab_status, abnormal_labs FROM daily_patient_risk WHERE sim_day = (SELECT max(sim_day) FROM daily_patient_risk) ORDER BY total_risk_score DESC LIMIT 8;"

# Pipeline health log (every DAG run writes OK/FAIL here)
docker compose exec postgres psql -U ward -d ward -c "SELECT * FROM pipeline_health ORDER BY 1 DESC LIMIT 5;"
```

### 5.7 Daily risk report (the final deliverable)

```powershell
Get-ChildItem data\reports | Sort-Object LastWriteTime | Select-Object -Last 2 Name
Invoke-Item (Get-ChildItem data\reports\*.html | Sort-Object LastWriteTime | Select-Object -Last 1).FullName
```

The second command opens the newest HTML report in your browser. For each patient, the report
shows the category from vitals alone next to the combined vitals + labs category, so you can
see where yesterday's labs changed the risk picture. The `.csv` file has the same rows for
spreadsheets. A sample is in [reports/sample/](../reports/sample/).

### 5.8 API serving layer (speed + batch merged)

Open http://localhost:8000/docs and use **Try it out**, or call the endpoints directly:

```powershell
curl.exe -s "localhost:8000/api/patients?sort=risk"                 # live view, highest risk first
curl.exe -s "localhost:8000/api/patients?category=CONCERNING"       # filter by category
curl.exe -s localhost:8000/api/patients/P007                        # merged view for one patient
curl.exe -s localhost:8000/api/metrics                              # freshness, categories, alert counts
curl.exe -s localhost:8000/metrics                                  # Prometheus format
```

`/api/patients/P007` shows the whole Lambda merge in one response:
- `speed_view`: the live 2-minute window, trend and vital risk;
- `batch_view`: the latest labs and the daily risk row;
- `combined`: vital score + lab score = total score and category.

Tip: open the URLs in the browser; Chrome and Edge have a "pretty print" option for JSON.

### 5.9 Observability

- **Grafana** shows, among other things: vitals per second, patients CONCERNING, critical
  alerts, speed-view age, Spark batch duration, Kafka lag, patients by risk category, the latest
  alerts and firing Prometheus alerts. Open
  http://localhost:3000/d/ward-vitals-overview/ward-vitals-pipeline.
- **Prometheus rules** are at http://localhost:9090/alerts. Normally nothing is firing. The rules
  include `NoVitalsReceived`, `SpeedLayerStalled`, `SpeedViewStale`, `AlertConsumerLag`,
  `LabFileLate`, `LabFileInvalid`, `PipelineFailureRecorded`, `PostgresDown` and
  `PatientCriticalAlert`. They are defined in
  [observability/prometheus/alerts.yml](../observability/prometheus/alerts.yml).
- **Structured logs** are JSON lines from every stage:
  ```powershell
  docker compose logs --tail 20 spark-streaming
  docker compose logs --tail 20 alert-consumer
  docker compose logs --tail 20 lab-simulator
  ```

---

## 6. Trigger a demo scenario

The normal producer runs a quiet ward. To force alerts, replace it with a seeded scenario:

```powershell
docker compose stop vital-producer
docker compose run --rm --use-aliases vital-producer python -m simulators.vital_producer.main --scenario spike --patient P007 --scenario-start 10
```

`--use-aliases` gives the scenario container the service name, so Prometheus keeps scraping the
producer's metrics. Without it, `ScrapeTargetDown` fires for `vital-producer`.

| Scenario | What happens | What to look at |
|---|---|---|
| `spike` | P007: heart rate 135–160 and SpO₂ 84–89 at once | WARNINGs within about 10 s, CRITICALs within about 1 min (section 5.4); P007 at the top of `/api/patients?sort=risk` |
| `hr_spike` | P007 heart rate only | `HEART_RATE_HIGH` alerts |
| `spo2_drop` | P007 SpO₂ falls steadily while heart rate climbs | `spo2_trend` = `FALLING` after about 75 s (section 5.2) |
| `outage` | The whole feed goes silent (add `--scenario-duration 90`) | `NoVitalsReceived` fires in Prometheus after about 50 s; vitals per second in Grafana drops to 0 |

Press Ctrl+C to stop the scenario, then bring the normal ward back:

```powershell
docker compose start vital-producer
```

---

## 7. Stop, restart, reset

```powershell
docker compose --profile monitoring stop        # pause everything, keep containers and data
docker compose --profile monitoring start       # resume
docker compose --profile monitoring down        # remove containers, KEEP data (volumes)
docker compose --profile monitoring down -v     # remove containers AND ALL DATA (Kafka, Postgres, lake, checkpoints)
```

After `down -v`, also clear the host-side files so the lab and report numbering starts fresh:

```powershell
Remove-Item data\landing\labs\labs_day=*, data\reports\risk_report_day=*
```

---

## 8. Troubleshooting

| Symptom | Fix |
|---|---|
| `Ports are not available ... bind: Only one usage of each socket address` | Another program uses that port. For Postgres, set `POSTGRES_HOST_PORT` in `.env`. For any other service, create a `docker-compose.override.yml` next to `docker-compose.yml` (Compose loads it automatically) and keep it out of git by adding it to `.git/info/exclude`. For example, for Prometheus: `services: {prometheus: {ports: !override ["9095:9090"]}}`. Then run `up -d` again and use the new port in the URL |
| Containers exit with code 137 or keep restarting | Docker is out of memory. Raise the WSL memory (section 2), or start without `--profile monitoring` |
| `/api/patients` is empty or `stale: true` | The producer or `spark-streaming` is not running. Check `docker compose ps` and `docker compose logs --tail 50 spark-streaming` |
| No report or `daily_patient_risk` rows yet | Wait for the first full simulated day (up to 10 minutes after start), then check the Airflow grid for failed tasks |
| DAG failed at `wait_for_lab_file` | The lab file for that day is missing. Write it with `docker compose run --rm lab-simulator python -m simulators.lab_batch.main --day N`, then re-trigger the DAG for day N (section 5.6) |
| A UI does not load after Docker Desktop restarted | Run `docker compose --profile monitoring down`, then `up -d`. This keeps the data |
| `spark-sql` fails with `metastore_db cannot be created` | Add `-w /tmp` to the `docker compose exec` command (section 5.3) |

More fixes are in the [README troubleshooting section](../README.md#troubleshooting) and in
[DEMO.md §7](DEMO.md#7-troubleshooting-during-the-demo).
