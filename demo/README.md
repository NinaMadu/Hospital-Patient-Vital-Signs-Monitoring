# Demo control centre

One web page, **http://localhost:8050**, to drive and explain the pipeline in the demo video.
It shows the live ward, the alerts, the daily risk from the batch layer and the Airflow runs,
and has buttons that change what goes into the pipeline. Everything on it comes from the real
system: the ward board is read through the serving API, the daily risk from the batch tables,
the DAG runs from Airflow.

> Requires `main` merged into your branch: the scenarios *Heart-rate spike*, *Gradual oxygen
> drop* and *Sensor feed outage* come from Member A's A8 simulator. Before that merge only
> *Sudden deterioration* is enabled; the others are greyed out.

## Start and stop

```powershell
.\demo\start-demo.ps1               # core stack + control centre, opens the page
.\demo\start-demo.ps1 -Monitoring   # also Prometheus and Grafana (needs more memory)
.\demo\stop-demo.ps1                # back to the normal stack, all data kept
```

The script is a wrapper around:

```powershell
docker compose stop vital-producer
docker compose -f docker-compose.yml -f demo/docker-compose.demo.yml up -d
```

Start it **15 minutes before recording**. The ward board needs about 30 s for the first
windows. The daily risk table needs one full simulated day (5 min), plus about 2 min for
the daily run.

## How it works

```
 browser :8050 ──> demo-control (FastAPI, demo/control/server.py)
                     │  ├─ owns the vital feed: Member A's VitalSimulator ──> Kafka patient-vitals
                     │  ├─ reads GET /api/patients, /api/alerts, /api/patients/{id} ──> api:8000
                     │  ├─ reads daily_patient_risk, lab_results, pipeline_health ──> Postgres
                     │  ├─ triggers / reads daily_lab_consolidation ──> Airflow REST API
                     │  └─ bad-lab-file drill writes data/landing/labs/labs_day=N.csv
                     └─ serves data/reports/*.html (the daily report)
```

- **Demo mode swaps one producer for another.** The normal `vital-producer` container
  always streams the same way. In demo mode it is left off, and `demo-control` runs the
  same simulator with the same seed and Kafka settings, but can switch scenario at any time.
  Only one producer runs, so readings are never doubled.
- **Prometheus still works.** The container answers under the network name
  `vital-producer` and serves the same `ward_producer_*` metrics on port 8001. The scrape
  target and the NoVitalsReceived alert therefore keep working.
- **Scenarios are repeatable.** A scenario button restarts the simulator with the scenario
  starting 3 s later. The seed is fixed, so the same scenario gives the same values and
  alerts on every take. That matters when you record a scene twice.
- **Reaction times are measured live.** When a scenario runs, the page measures, from the
  moment the deterioration starts:
  - how long until the first alert is stored;
  - how long until the patient's tile turns CONCERNING;
  - for the outage: how long until the ward board shows the data as stale.

  These are real measurements of the pipeline, so you can quote them in the video.
- **Airflow change.** `docker-compose.demo.yml` turns on basic auth for Airflow's REST
  API, so the page can trigger DAG runs. `stop-demo.ps1` puts Airflow back to its normal
  settings.

## The page

| Area | What it shows | Data from |
|---|---|---|
| Top bar | Simulated day and progress through it; health of Kafka, API, Postgres, Airflow | clock, `/health`, producer acks |
| **Story / Technical** (top right, or press **T**) | Story: plain-language captions for a non-technical audience. Technical: endpoints, tables, Spark queries, raw Kafka messages with partition numbers | — |
| 1 · Live scenarios | Buttons that change one patient's vitals (pick the patient and duration first) and a live timer of the pipeline's reaction | the feed |
| 2 · Data quality | Switch on 5 % broken readings; they go to the dead-letter queue, never to the ward board. You can also pause and resume the whole feed | the feed |
| 3 · Daily batch | Run the daily DAG for any day; the failure drill for a bad lab file | Airflow REST API |
| Ward board | 15 patient tiles coloured NORMAL / WATCH / CONCERNING, with trend arrows ↑↓ and alert counts. **Click a tile** for the patient's full picture: live vitals + labs = combined score, lab table, alerts | `GET /api/patients` |
| Daily risk | The last 3 DAG runs task by task; the day's risk table with each patient's category *from vitals only* next to it *with labs*. Rows are highlighted where the labs changed the category. Links to the HTML/CSV report | `daily_patient_risk`, Airflow |
| Alerts | Live alerts, newest first, flashing when new | `GET /api/alerts` |
| What we did | A timeline of your button presses, so viewers can follow along | — |

URL options for a bookmarked start view: `http://localhost:8050/?mode=tech` and
`http://localhost:8050/?patient=P007` (opens that patient).

## Suggested demo flow (about 6 minutes)

| Time | Do | Say (story) | Add in technical mode |
|---|---|---|---|
| 0:00 | Page open in **Story** mode, ward all green | "15 patients, each monitor sends a reading every few seconds. Green means fine right now." | "Q1 computes 2-minute windows sliding every 30 s." |
| 0:40 | **Sudden deterioration**, P007, 3 min | "P007's heart races and their oxygen drops." Wait: tile turns red, alerts appear. Read out the timer: "first alert after ~10 s". | Switch to **Technical**: the Kafka feed shows P007 always on the same partition; alert rules in the alert feed. |
| 1:40 | Click the **P007** tile | "The live score plus yesterday's lab results give one combined score." | "That's the serving layer merging speed view and batch view with the same `combine()` function." |
| 2:20 | **Gradual oxygen drop**, P003 | "Not a sudden event: the oxygen slides slowly. The system spots the *trend* (↓) before it is dangerous." | "Trend: three consecutive windows falling by at least 1 point." |
| 3:00 | **Break 5 % of readings** on, watch the counter, then off | "Broken sensor messages are set aside and never reach the doctors' screen." | "Q1 routes them to `vitals-dlq` with a reason; the lake keeps them for diagnosis." |
| 3:30 | Scroll to **Daily risk** | "Once a day the lab results arrive. These highlighted patients looked fine from vitals alone but their labs moved them up." | "Airflow: sensor on the `_SUCCESS` marker → validate → load → Spark risk join. Left join, so a patient without labs is `LAB_UNAVAILABLE`, not 0." |
| 4:30 | **Failure drill**: *Corrupt lab file* → *Run daily consolidation* → watch *validate* turn red and a FAIL appear → *Restore file* → run again | "A bad file is rejected as a whole; nothing wrong is loaded, and the team is alerted." | "`validate_lab_file` fails without retries; the failure callback writes to `pipeline_health`; LabFileInvalid fires in Prometheus." |
| 5:20 | **Sensor feed outage** (optional, needs monitoring profile for the alert) | "If the monitors go silent, the board shows it within half a minute." | "NoVitalsReceived after 30 s; Grafana link." |
| 5:50 | **Normal ward** | Wrap up | — |

Tips:
- Use a **different patient** for each scenario so the tiles tell separate stories.
- Scenarios last 1.5, 3 or 5 minutes. Press **Normal ward** to end one early.
- For the drill, use a day that has already been loaded (the day box defaults to the last
  complete day). Always press **Restore file** afterwards and run that day again.
- Browser zoom at 90 % fits the whole page on a 1080p recording.

## Troubleshooting

| Symptom | Fix |
|---|---|
| "Serving API not reachable" | `docker compose ps api`, then `docker compose up -d api` |
| Ward board empty | The first windows take ~30 s after the feed starts; check `docker compose logs --tail 20 spark-streaming` |
| Airflow chip red / "Airflow API not reachable" | Airflow takes ~2 min to start. Start through `start-demo.ps1` so basic auth is on. |
| Scenario buttons greyed out | Merge `main` (Member A's A8 scenarios) |
| Readings doubled (every patient twice as often) | The normal producer is running too: `docker compose stop vital-producer` |
| Page loads but nothing moves | `docker compose logs --tail 30 demo-control` |

## Files

| File | Purpose |
|---|---|
| `docker-compose.demo.yml` | Adds `demo-control` (port 8050), leaves `vital-producer` off, enables Airflow API basic auth |
| `start-demo.ps1`, `stop-demo.ps1` | Enter and leave demo mode |
| `control/server.py` | HTTP endpoints for the page, reaction-time measurement |
| `control/producer.py` | The switchable vital feed (wraps `simulators/vital_producer`) |
| `control/batch.py` | Airflow client, batch-table queries, bad-lab-file writer |
| `control/static/` | The page: `index.html`, `style.css`, `app.js` (no build step, works offline) |
| `../tests/demo/test_demo_control.py` | 21 tests: feed switching, malformed detection, lab drill, endpoints |
