# Demo guide: video or live

How to show the pipeline running end to end with its observability results, in 5–10 minutes,
either as a recorded video (the deliverable) or live in front of the examiners. The same script
works for both. The assumptions to state are in the README:
[Assumptions, simplifications and simulated time](../README.md#assumptions-simplifications-and-simulated-time).

## Contents

1. [What the demo must show](#1-what-the-demo-must-show)
2. [Video or live?](#2-video-or-live)
3. [Preparation](#3-preparation)
4. [The script (10 minutes)](#4-the-script)
5. [Recording the video](#5-recording-the-video)
6. [Live demo: backup plan](#6-live-demo-backup-plan)
7. [Troubleshooting during the demo](#7-troubleshooting-during-the-demo)
8. [Afterwards and deliverables checklist](#8-afterwards-and-deliverables-checklist)

---

## 1. What the demo must show

The brief asks for "a short (5–10 minute) demo video OR be prepared to demo live, showing the
pipeline running end-to-end and the observability results", and to "clearly state any
assumptions, simplifications, or simulated-time compression used". Every item below appears in
the script:

| Requirement | Where in the script |
|---|---|
| Streaming source → Kafka (keys, partitions) | 1:00–2:40 Ninada |
| Spark Structured Streaming (windows, watermark, trend) | 2:40–3:30 Ninada |
| Real-time serving and patient alerts | 3:30–4:30 Kasun |
| Daily batch source → Airflow → Spark batch → risk join → report | 5:30–8:30 Imalsha |
| Serving-layer merge of the speed and batch views | 7:45 Imalsha, 8:30 Kasun |
| Observability: logs, metrics, dashboard, a health alert firing | 4:30–5:30 Kasun |
| Assumptions and simulated-time compression stated | 0:20 Ninada, 9:30 Kasun |

---

## 2. Video or live?

**Record the video** (the brief accepts it, and it can't fail on the day), and still bring the
running laptop to the viva in case the examiners ask to see something live.

- **Video:** record each member's part as a separate clip and join them. A mistake then costs
  one clip, and the timing of the P007 alerts is easy to control.
- **Live:** one continuous run of the same script; section 4 marks the extra steps live mode needs.

---

## 3. Preparation

### The day before

- [ ] Choose the laptop with the most RAM. Docker needs **10 GB**: `%UserProfile%\.wslconfig` with
      `[wsl2]` `memory=10GB` `processors=6`, then `wsl --shutdown` and restart Docker Desktop.
- [ ] Pull the latest `main` and build: `git pull`, then `docker compose --profile monitoring build`.
- [ ] Rehearse the whole script once with a timer. Each presenter says their lines out loud.
- [ ] Prepare the architecture diagram (report Figure 4.1) and one closing slide with the
      assumptions (text in [section 4](#4-the-script), part 8:30–10:00).

### 30 minutes before

Use **PowerShell** in the project folder (Git Bash rewrites container paths). Start
everything, including the monitoring profile:

```powershell
docker compose --profile monitoring up -d
docker compose --profile monitoring ps          # every service Up / healthy; init-dirs and kafka-init Exited (0)
```

**Let it run for at least 15 minutes before recording.** That is 3 simulated days
(1 day = 5 minutes), so that:
- Airflow has processed at least two days: lab file → summary → risk join → report
- Q1 has enough windows for trends (3 consecutive windows)
- Grafana graphs have history

Optional clean start, for small, tidy numbers. This **deletes all data**, so allow the full 15 minutes afterwards:

```powershell
docker compose --profile monitoring down -v
Remove-Item data\landing\labs\labs_day=*, data\reports\risk_report_day=*
docker compose --profile monitoring up -d
```

### 5 minutes before: pre-flight checks

```powershell
docker compose --profile monitoring ps
curl.exe -s localhost:8000/health                                   # {"status":"ok","postgres":"ok"}
curl.exe -s localhost:8000/api/metrics                              # current_sim_day, latest_lab_sim_day = current - 1 or - 2
docker compose exec airflow airflow dags list-runs -d daily_lab_consolidation -o table   # latest runs: success
Get-ChildItem data\reports | Select-Object -Last 2                  # risk_report_day=N.html / .csv
```

- http://localhost:9090/alerts: nothing firing. `PipelineFailureRecorded` stays on for 30 minutes
  after any failed DAG run; if it's firing, wait or explain it.
- The Grafana dashboard has data in every panel.

### Browser tabs, in the order they're used

| # | Tab | URL |
|---|---|---|
| 1 | Architecture diagram | report Figure 4.1 |
| 2 | Kafka UI: `patient-vitals` overview | http://localhost:8085/ui/clusters/ward-vitals/all-topics/patient-vitals |
| 3 | Kafka UI: messages | same topic, **Messages** tab |
| 4 | Spark UI: Structured Streaming | http://localhost:4040/StreamingQuery/ |
| 5 | API: patients by risk | http://localhost:8000/api/patients?sort=risk |
| 6 | API: P007 merged view | http://localhost:8000/api/patients/P007 |
| 7 | API: critical alerts | http://localhost:8000/api/alerts?severity=CRITICAL&limit=5 |
| 8 | Grafana dashboard (admin/admin) | http://localhost:3000/d/ward-vitals-overview/ward-vitals-pipeline |
| 9 | Prometheus alerts | http://localhost:9090/alerts |
| 10 | Airflow: DAG grid (admin/admin) | http://localhost:8080/dags/daily_lab_consolidation/grid |
| 11 | The newest daily report | `data\reports\risk_report_day=N.html` (open from File Explorer) |

Turn on the browser's JSON "pretty print", and zoom pages to 125% so text is readable in the video.

---

## 4. The script

Aim for about 9 minutes; the brief allows 10 at most. Each member presents their own slice,
the timings follow [PROJECT_PLAN.md §9](PROJECT_PLAN.md#9-report-and-demo-ownership), and each
presenter can adjust their wording.

| Time | Presenter | Part |
|---|---|---|
| 0:00–3:30 | Ninada (A) | Use case, architecture, Kafka, Spark streaming |
| 3:30–5:30 | Kasun (C) | API, patient alerts, Grafana, a health alert firing |
| 5:30–8:30 | Imalsha (B) | Lab file → Airflow → Spark batch → risk report, merged view |
| 8:30–10:00 | Kasun (C) | Trade-offs, assumptions, limitations |

### 0:00–3:30 Ninada: use case, architecture, speed layer

Follow [docs/report/member_a_demo_script.md](report/member_a_demo_script.md). Add one sentence
to the introduction (0:20) so the assumptions are stated at the start:

> "Everything is simulated: 15 patients and a daily lab file from Python simulators with fixed
> seeds. Time is compressed: one simulated day is five real minutes, so a lab file and an
> Airflow run arrive every five minutes, while the vital signs stream in real time. The risk
> thresholds are project rules, not clinical guidance."

**Live mode only:** Kasun starts the P007 scenario at **0:00** (step 1 below), so its alerts
are ready at 3:30. At 1:30 Ninada shows `docker logs -f --tail 5 demo-producer` instead of
`docker compose logs -f vital-producer` (the scenario producer replaces the normal one and
sends the whole ward, so Kafka and Spark look the same).

### 3:30–5:30 Kasun: serving layer, alerts, observability

**Step 1: start the P007 scenario 3.5 minutes before this part** (for a separate video clip:
3.5 minutes before pressing record). Use a second PowerShell window:

```powershell
docker compose stop vital-producer
docker compose run --rm --use-aliases --name demo-producer vital-producer `
    python -m simulators.vital_producer.main --scenario spike --patient P007 --scenario-start 120 --scenario-duration 480
```

P007 deteriorates 2 minutes after the command (heart rate 135–160, SpO₂ 84–89) and stays that
way for 8 minutes. This timing was measured:
- WARNING alerts appear within about 10 s.
- CRITICAL (sustained) alerts appear about 55 s after each full minute of deterioration.
- By 3:30 there are warnings. The first CRITICAL alerts arrive between about 3:10 and 4:10, depending on where the minute boundary falls.
- `--use-aliases` keeps Prometheus scraping the producer, so no "target down" alert fires.

| Time | Show | Say |
|---|---|---|
| 3:30 | Tab 5 `/api/patients?sort=risk` | "The serving layer. This is the first half of the business question: which patients are at risk right now. P007 is at the top: its current 2-minute window has low SpO₂ and a high heart rate, and the trend is rising. The score comes from the same shared rule code that Spark uses." Point at `vital_risk`, `reasons`, `trends`, `alerts_last_10m`. |
| 3:55 | Tab 7 `/api/alerts?severity=CRITICAL` (refresh until CRITICAL rows appear) | "Spark query Q3 raises two kinds of alert: a WARNING for one abnormal reading, and a CRITICAL when the same limit is broken in three or more readings within one minute, so a single spike doesn't page anyone. Alerts go to a Kafka topic, and a consumer stores them in Postgres. The alert ID is deterministic, so a re-delivered alert is stored only once." |
| 4:15 | Tab 8 Grafana | "The dashboard: vitals per second, speed-view freshness, the Spark batch duration (well under the 10-second trigger), Kafka lag, and the latest alerts straight from Postgres. P007's critical alerts are here." |
| 4:30 | Second PowerShell window: **Ctrl+C** the demo producer | "Now a failure: the bedside feed stops." |
| 4:35 | Tab 8 Grafana, then tab 9 Prometheus alerts | "Vitals per second drops to zero. The `NoVitalsReceived` rule watches the Kafka offsets; after 30 seconds without new data it goes pending, then firing." Refresh until **firing** (about 50 s after Ctrl+C, so about 5:20). |
| 5:20 | Tab 9, firing | "That's the health alert. The API also marks every patient as stale." Then start the normal ward again: `docker compose start vital-producer` (the alert resolves in about 45 s). |

If `NoVitalsReceived` isn't firing yet at 5:20, keep talking about the other rules on tab 9
(`LabFileLate`, `AlertConsumerLag`, `SpeedLayerStalled`) and refresh once more.

### 5:30–8:30 Imalsha: batch layer (adjust the wording to your part)

Find the last completed day: `current_sim_day − 1` in `curl.exe -s localhost:8000/api/metrics`,
or the newest green run in the Airflow grid. Call it **N**.

| Time | Show | Say / do |
|---|---|---|
| 5:30 | `Get-ChildItem data\landing\labs \| Select-Object -Last 4`, then `Get-Content data\landing\labs\labs_day=N.csv -TotalCount 5` | "The second source: one lab file per simulated day, written atomically with a `_SUCCESS` marker, so Airflow never reads half a file. Each row carries its reference range." |
| 6:00 | Tab 10 Airflow grid, then the Graph view | "One DAG run per simulated day. The file sensor waits for the marker, then validate and load; in parallel, after the lake settles, the Spark daily vital summary; then the risk join and the report." |
| 6:30 | Trigger button (▶) → fill in `sim_day` = N → Trigger | "Any day can be re-run. Loading replaces that day's rows in one transaction, so a re-run never duplicates anything." The run takes about 1 minute. Meanwhile, open the `validate_lab_file` task log (JSON log lines, rows checked). |
| 7:30 | `docker compose exec postgres psql -U ward -d ward -c "SELECT patient_id, vital_risk_score, lab_risk_score, total_risk_score, risk_category, lab_status, abnormal_labs FROM daily_patient_risk WHERE sim_day = N ORDER BY total_risk_score DESC LIMIT 6;"` | "The risk join: the day's vital summary from the Parquet lake plus the latest labs, scored with the shared rules. Patients without labs are `LAB_UNAVAILABLE`, never zero." |
| 7:45 | Tab 11 report HTML (refresh), then tab 6 `/api/patients/P007` | "The daily report shows the category from vitals alone next to the combined one, so you see where the labs changed the picture. That's the second half of the question. The API merges the live vital score with this lab score." Point at `combined`: vital + lab = total → CONCERNING. |

### 8:30–10:00 Kasun: trade-offs, assumptions, limitations

Show Grafana (tab 8) or the closing slide.

- **Lambda trade-off:** "The speed layer answers in seconds from 2-minute windows; the batch
  layer is complete and replayable from the Parquet lake, but arrives once a day. The two views
  deliberately differ, so the API labels where each number comes from, and one shared rules
  module stops their logic drifting apart."
- **Evidence from the failure drills:**
  - "We stopped the producer: `NoVitalsReceived` fired in 52 s."
  - "We stopped Postgres: `/health` answered 503 and `PostgresDown` fired. Spark re-sent 4
    alerts, but the table still had exactly one row per alert (212 distinct alerts, 212 rows)."
  - "We removed a lab file: `LabFileLate` fired in 80 s, and the DAG failure was recorded and
    alerted."
- **Assumptions** (closing slide):
  > One simulated day = 5 real minutes, a 288× compression of the daily cadence only; the
  > vital stream runs in real time. All data comes from seeded simulators: 15 patients, 6 lab
  > tests. Thresholds are project rules, not clinical guidance. Single-laptop setup: one Kafka
  > broker without replication, one Spark worker, Airflow standalone, one Postgres, no
  > authentication.
- **Production:**
  - 3 Kafka brokers with replication factor 3
  - a cluster manager for Spark
  - object storage for the lake
  - Alertmanager paging for CRITICAL alerts, with repeat suppression (threshold alerts are noisy)
  - authentication and TLS

---

## 5. Recording the video

- **Tool:** OBS Studio (free), or the Xbox Game Bar (`Win + Alt + R`). Record at 1920×1080 and 30 fps, with a headset microphone.
- Turn on Windows **Focus assist / Do not disturb**, close chat apps, and hide bookmarks and passwords.
- Record **one clip per segment** (4 clips), then join them in Clipchamp (built into Windows)
  or any editor. Cut waiting time: the ~50 s before an alert fires, and the ~1 min DAG run.
  Say "(cut: one minute later)" or add a caption.
- Check the final length is **≤ 10:00**, and that every member presents their own slice.
- Export MP4 (H.264). Upload as an **unlisted** YouTube video, or a Google Drive / OneDrive file
  shared with "anyone with the link". Test the link in a private browser window. Put the link
  in the README and the report.

---

## 6. Live demo: backup plan

- Start the stack **30 minutes** before, and run the pre-flight checks (section 3).
- Keep the recorded video on the laptop's desktop. If anything breaks and isn't fixed within
  30 seconds, say so and play that segment of the video.
- Keep `docs/screenshots/` open in a File Explorer window as a second fallback.
- Don't run `docker compose down -v` on the day: the stack needs 15 minutes to build up history again.
- Have the commands of section 4 in a text file, ready to copy and paste.

---

## 7. Troubleshooting during the demo

| Symptom | Fix |
|---|---|
| A UI or the API gives "empty reply" or won't load after Docker Desktop restarted | `docker compose --profile monitoring down` then `docker compose --profile monitoring up -d` (keeps all data) |
| Docker Desktop crashed or containers exit with 137 | Not enough memory: check `.wslconfig` (10 GB), close other apps, restart Docker Desktop |
| `spark-streaming` keeps restarting and its log shows `No content to map due to end-of-input` | A checkpoint was corrupted by a crash. Stop `spark-streaming`, delete only the named query's folder: `docker compose run --rm --no-deps --entrypoint sh spark-worker -c "rm -rf /data/checkpoints/q1"`, then start it again |
| No CRITICAL alert yet | They arrive about 55 s after each full minute of deterioration; WARNINGs are immediate. Check that the demo producer is running: `docker ps` |
| `NoVitalsReceived` doesn't fire | Check that nothing is producing: `docker ps` must show neither `demo-producer` nor `vital-producer` |
| `ScrapeTargetDown` for `vital-producer` during the demo | The scenario was started without `--use-aliases`. Harmless: say that Prometheus noticed the normal producer was stopped |
| DAG run failed at `wait_for_lab_file` | The lab file is missing (e.g. the simulator was stopped). Write it: `docker compose run --rm lab-simulator python -m simulators.lab_batch.main --day N`, then trigger the DAG with `{"sim_day": N}` |
| `PipelineFailureRecorded` is firing | A DAG task failed in the last 30 minutes (e.g. during a rehearsal). It clears by itself |

---

## 8. Afterwards and deliverables checklist

Bring the normal ward back after the demo or a rehearsal:

```powershell
docker stop demo-producer          # if it's still running
docker compose start vital-producer
```

- [ ] Video, 5–10 minutes, every member presents their slice; link in the README and the report
      (or the laptop ready for a live demo, with the video as backup)
- [ ] Assumptions, simplifications and simulated time: [README section](../README.md#assumptions-simplifications-and-simulated-time),
      stated in the video (0:20 and 9:30), and repeated in the report
- [ ] Screenshots in `docs/screenshots/` (Kafka partitions, Spark streaming, Airflow graph, report,
      API output, Grafana, an alert firing)
- [ ] Report PDF (8–15 pages) with the contributions statement
- [ ] Repository tagged `v1.0`
