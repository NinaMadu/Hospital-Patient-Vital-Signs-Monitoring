# Hospital Patient Vital Signs Monitoring

Lambda-architecture big data pipeline for hospital patient vital-signs monitoring. Simulated bedside monitors stream vitals through Kafka into Spark Structured Streaming for real-time risk scoring and alerts, while Airflow orchestrates daily Spark batch jobs joining pathology lab results. PostgreSQL, a Parquet lake, FastAPI and Prometheus/Grafana provide serving and observability.

EC8203 Applied Big Data Engineering mini-project, use case 2.

> **Business question:** Which patients show concerning vital-sign trends right now, and how do yesterday's lab results change the risk picture for those patients going forward?

> Risk thresholds in this project are simulated academic rules for a data-engineering exercise, **not clinical guidance**.

- Team plan, work split and schedule: [docs/PROJECT_PLAN.md](docs/PROJECT_PLAN.md)
- Step-by-step work plan for each member: [docs/WORK_PLAN.md](docs/WORK_PLAN.md)
- Assignment brief and implementation guide: [docs/brief/](docs/brief/)
- Architecture decisions: [docs/decisions/](docs/decisions/)

**Status:** environment setup (Day 1–2). Pipeline components are stubs owned by team members; see the plan.

---

## Architecture (target)

```text
Vital simulator ──► Kafka (patient-vitals, 3 partitions) ──► Spark Structured Streaming ─┬─► PostgreSQL ──► FastAPI / Grafana
                                                                                        ├─► Kafka (patient-alerts)
                                                                                        └─► Parquet lake (raw vitals)
Lab simulator ──► landing/labs_day=N.csv ──► Airflow DAG ──► Spark batch (lake + labs) ──► PostgreSQL ──► daily risk report
```

Full diagram and ownership: [docs/PROJECT_PLAN.md §2](docs/PROJECT_PLAN.md#2-target-architecture).

## Technology stack

| Layer | Tool | Version (pinned) |
|---|---|---|
| Ingestion | Apache Kafka (KRaft, no ZooKeeper) | 3.9.0 |
| Stream + batch processing | Apache Spark standalone (Structured Streaming + batch) | 3.5.5 (Scala 2.12, Java 17, Python 3.10) |
| Orchestration | Apache Airflow (LocalExecutor) | 2.10.5 |
| Storage | PostgreSQL (serving) + Parquet lake (raw history) | 16.4 |
| Serving | FastAPI | 0.115 |
| Observability | Prometheus, Pushgateway, kafka-exporter, Grafana | 2.55 / 1.10 / 1.8 / 11.4 |
| Packaging | Docker Compose | v2 |

## Prerequisites

- **Docker Desktop** (Windows/macOS) or Docker Engine + Compose v2 (Linux).
- **At least 8 GB of RAM given to Docker**; 10 GB is recommended. On Windows (WSL2) create `%UserProfile%\.wslconfig`:
  ```ini
  [wsl2]
  memory=10GB
  processors=6
  ```
  then run `wsl --shutdown` and restart Docker Desktop.
- **Python 3.12** for local development and tests (optional for running the stack).
- About 10 GB of free disk space for images.

## Quick start

```bash
# 1. Configuration
cp .env.example .env

# 2. Build the custom images (Spark, Airflow, Python app) — first time takes several minutes
docker compose build

# 3. Start the core stack
docker compose up -d

# 3b. (optional) with monitoring: Prometheus, Pushgateway, kafka-exporter, Grafana
docker compose --profile monitoring up -d

# 4. Check status: every service should be "running"/"healthy"; init-dirs and kafka-init "exited (0)"
docker compose ps -a
```

### Service URLs

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| Kafka UI | http://localhost:8085 | — |
| Spark master UI | http://localhost:8090 | — |
| Spark worker UI | http://localhost:8081 | — |
| API | http://localhost:8000/docs (endpoints below) | — |
| Alert consumer metrics | http://localhost:8001/metrics | — |
| PostgreSQL | `localhost:5432`, db `ward` | `ward` / `ward_dev_pw` |
| Kafka (from host) | `localhost:9094` | — |
| Prometheus (alerts: /alerts) | http://localhost:9090 | monitoring profile |
| Pushgateway | http://localhost:9091 | monitoring profile |
| Grafana (dashboard "Ward Vitals Pipeline") | http://localhost:3000 | `admin` / `admin`, monitoring profile |

Inside the Docker network use `kafka:9092`, `postgres:5432` and `spark://spark-master:7077`.

### API endpoints

| Endpoint | Returns |
|---|---|
| `GET /health` | `200` if Postgres answers, `503` if not |
| `GET /api/patients?sort=risk&category=WATCH` | Speed view: each patient's current 2-min window and vital risk |
| `GET /api/patients/{id}` | Merged view: vital risk (speed) + lab risk (batch) → combined category |
| `GET /api/alerts?patient_id=P007&severity=CRITICAL` | Alerts from Spark Q3 (newest first) |
| `GET /api/metrics` | JSON summary: data freshness, risk categories, alert counts, API traffic |
| `GET /metrics` | Prometheus format |

Demo alert for P007 (details in [docs/PART_C_README.md](docs/PART_C_README.md)):
```bash
docker compose stop vital-producer
docker compose run --rm vital-producer python -m simulators.vital_producer.main --scenario spike --patient P007 --scenario-start 10
```

## Verify the environment

1. **Kafka topics:** the `kafka-init` log lists `patient-vitals` (3 partitions), `vitals-dlq` (1) and `patient-alerts` (3):
   ```bash
   docker compose logs kafka-init
   ```
2. **Spark cluster:** http://localhost:8090 shows one ALIVE worker with 4 cores.
3. **Airflow → Spark → lake → Postgres:** in Airflow, trigger the `env_check` DAG. Both tasks should turn green. You can also run it from the command line:
   ```bash
   docker compose exec airflow airflow dags test env_check
   ```
4. **Postgres:** the env check writes a row into `pipeline_health`:
   ```bash
   docker compose exec postgres psql -U ward -d ward -c "select * from pipeline_health;"
   ```

## Local development

```bash
py -3.12 -m venv .venv                 # Windows  (macOS/Linux: python3.12 -m venv .venv)
.venv\Scripts\activate                 # Windows  (macOS/Linux: source .venv/bin/activate)
pip install -r requirements-dev.txt
pytest
```

Or run every test inside the Spark image (no local Python or Java needed):

```bash
docker compose run --rm --no-deps -T --entrypoint sh spark-worker scripts/test_in_docker.sh
```

Local PySpark is for unit tests on small in-memory DataFrames. On Windows, writing files from local Spark needs Hadoop `winutils`, so run file-writing jobs in the containers instead.

## Repository layout

```text
.
├── docker-compose.yml   .env.example   requirements-dev.txt   pyproject.toml
├── config/            app.yaml (settings), thresholds.yaml (risk rules)
├── common/            shared code: sim_clock, config, logger, metrics, risk_rules
├── simulators/        vital_producer/ (stream source), lab_batch/ (daily file source)
├── kafka/init/        topic creation script
├── spark/
│   ├── conf/          spark-defaults.conf (shared by every Spark app)
│   ├── streaming/     speed layer: Q1 windows, Q2 archive, Q3 alerts, listener
│   └── batch/         batch layer: vital summary, risk consolidation, reconciliation
├── airflow/dags/      daily_lab_consolidation, replay_sim_day, pipeline_health, env_check
├── database/init/     Postgres init: airflow DB + ward serving schema
├── api/               FastAPI serving layer + alert consumer
├── observability/     prometheus/ (scrape + alert rules), grafana/ (provisioning, dashboards)
├── reports/           daily report generator, sample output
├── scripts/           environment checks
├── tests/             unit tests (one folder per slice)
├── labs/              learning-lab code (Days 1–3)
├── docker/            Dockerfiles: spark, airflow, python
├── data/              runtime bind mounts: landing/labs (lab files), reports (git-ignored)
└── docs/              brief/, PROJECT_PLAN.md, decisions/, architecture/, screenshots/, report/
```

## Data locations

| What | Where | Visible from host |
|---|---|---|
| Daily lab files | `/data/landing/labs` ↔ `./data/landing/labs` | yes |
| Generated reports | `/data/reports` ↔ `./data/reports` | yes |
| Raw vitals Parquet lake | `/data/lake` (Docker volume `lake`) | via containers |
| Streaming checkpoints | `/data/checkpoints` (Docker volume `checkpoints`) | via containers |

## Common operations

```bash
docker compose logs -f <service>        # follow logs
docker compose restart <service>
docker compose down                     # stop, keep data
docker compose down -v                  # stop and DELETE all data (Kafka, Postgres, lake, checkpoints)
```

`database/init/` scripts run only on an empty Postgres volume. After changing the schema, run `docker compose down -v` or apply the SQL manually.

## Simulated clock

1 simulated day = **5 real minutes** (`SIM_DAY_SECONDS=300`), and `sim_day = floor((ts − SIM_EPOCH) / 300)`. See `.env`.

## Troubleshooting

- **Containers exit with code 137 or keep restarting:** Docker is out of memory. Raise the WSL memory (see Prerequisites) or stop the monitoring profile.
- **`/bin/bash^M: bad interpreter`:** a script was checked out with CRLF line endings. `.gitattributes` forces LF; re-checkout with `git rm --cached -r . && git reset --hard`.
- **Git Bash turns container paths into Windows paths** (`/data/lake` becomes `C:/Program Files/Git/data/lake`): run `export MSYS_NO_PATHCONV=1` first, or use PowerShell for `docker compose exec` commands.
- **Port already in use:** change the host-side port in `docker-compose.yml`, or `POSTGRES_HOST_PORT` in `.env`.

## Team

| Member | Slice |
|---|---|
| A | Real-time vitals: simulator, Kafka, streaming windows |
| B | Labs and history: lab simulator, batch layer, Airflow |
| C | Alerts, serving, observability |

Individual contribution statement: to be added before submission.
