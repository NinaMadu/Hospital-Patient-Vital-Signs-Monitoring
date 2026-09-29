# Demo video: plan and script

A 9½-minute video that shows the pipeline running end to end, driven by the demo UI's
animations, with short cuts to the real tools (Kafka UI, Spark UI, Airflow, Grafana,
Prometheus) as evidence. Each scene below says **what is on screen**, **what to click** and
**what to say**.

> This replaces the script part of [DEMO.md](DEMO.md), which was written before the demo UI
> existed. DEMO.md is still useful for the live-demo backup plan and troubleshooting.

## Contents

1. [What the video must show](#1-what-the-video-must-show)
2. [The storyline](#2-the-storyline)
3. [Scene overview](#3-scene-overview)
4. [Preparation](#4-preparation)
5. [The script](#5-the-script)
6. [Recording](#6-recording)
7. [Editing and delivery](#7-editing-and-delivery)
8. [If something goes wrong while recording](#8-if-something-goes-wrong-while-recording)

---

## 1. What the video must show

From the brief (Submission guidelines) and the project guide (§20):

| Requirement | Where |
|---|---|
| 5–10 minutes | whole video, target **9:20** |
| Pipeline running **end to end** | scenes 3 → 7: one patient's readings travel from the monitor to the ward board, the daily lab file travels to the risk report |
| **Observability results** | scene 6 (logs, metrics, Grafana, a health alert firing) and scene 8 (a batch failure detected) |
| Assumptions, simplifications, **simulated-time compression** stated | scene 1 (start) and scene 9 (end) |
| Streaming source, Kafka topic / key / **partitions** | scene 3 |
| Spark Structured Streaming with **meaningful** transformations (cleaning, windows, trend) | scene 4 |
| Threshold alerts per patient, real-time API / dashboard | scene 5 |
| Daily batch source → **Airflow** → Spark batch → **join of labs with vitals** → report | scene 7 |
| Lambda vs Kappa decision (20 of the 100 marks) | scene 2 (decision) and scene 9 (trade-offs) |

The guide's success criterion is the checklist for the whole video: *"a vital event entering
Kafka, Spark turning it into a meaningful patient-level result, the API exposing current
status, the daily lab file arriving, Airflow running the consolidation, the database/report
containing the joined result, and the observability layer showing that the pipeline is
healthy."*

---

## 2. The storyline

**Follow one patient through the pipeline.** At the start of scene 3, patient **P007**
suddenly deteriorates (the *Sudden deterioration* scenario). P007's readings are the
**yellow dots** in the Live pipeline tab, so the viewer can follow them: into P007's Kafka
partition (scene 3), through Spark (scene 4), out as orange alert dots into PostgreSQL, and
onto the ward board as a red CONCERNING tile (scene 5). Then P007's lab results (always high
CRP and lactate in the simulator) show how yesterday's labs change the risk picture (scene 7).
Both halves of the business question are answered with the same patient.

The **Live pipeline tab is the backbone** of the video: every moving dot is a real event, and
clicking a layer or a box zooms onto it full screen. The real tools appear for 10–20 s each
as evidence that the animation reflects the actual system.

---

## 3. Scene overview

| # | Time | Presenter | Scene | Main screen |
|---|---|---|---|---|
| 1 | 0:00–0:35 | Ninada (A) | Introduction, business question, assumptions | Live pipeline, full view |
| 2 | 0:35–1:25 | Ninada (A) | Architecture: Lambda, and why not Kappa | Live pipeline, zoom each layer |
| 3 | 1:25–2:35 | Ninada (A) | Ingestion: simulator → Kafka, keys and partitions | Zoom Monitors, Kafka → Kafka UI |
| 4 | 2:35–3:40 | Ninada (A) | Stream processing: Spark, windows, trend, DLQ | Zoom Spark → Spark UI |
| 5 | 3:40–4:40 | Kasun (C) | Alerts and serving: ward board, merged risk | Zoom serving layer → Control centre |
| 6 | 4:40–5:50 | Kasun (C) | Observability: logs, Grafana, a health alert firing | Terminal, Grafana, Prometheus |
| 7 | 5:50–7:30 | Imalsha (B) | Batch layer: lab file → Airflow → Spark batch → risk report | Zoom batch layer → Airflow → Control centre |
| 8 | 7:30–8:15 | Imalsha (B) | Batch failure drill: a bad lab file is rejected | Control centre, zoom Airflow |
| 9 | 8:15–9:20 | Kasun (C) | Summary, trade-offs, production, assumptions | Live pipeline, full view |

Each presenter explains their own slice, which also prepares them for the viva. If one person
records the whole video, keep the order and drop the name changes.

**Recorded as 4 clips**, because scenes 3 → 5 must run in real time without a break (the P007
scenario is live):

| Clip | Scenes | Real duration | Why one take |
|---|---|---|---|
| A | 1–2 | ~1.5 min | — |
| B | 3–5 | ~3.5 min | P007's scenario runs through all three scenes |
| C | 6 | ~2 min (1:10 after cutting the wait) | the outage runs in real time |
| D | 7–9 | ~5 min (3:30 after cutting the waits) | the DAG runs in real time |

---

## 4. Preparation

### The day before

- [ ] **Free port 9090 for Prometheus.** A Java program on the recording laptop was holding
      it; close that program, otherwise `NoVitalsReceived` (scene 6) and `LabFileInvalid`
      (scene 8) cannot fire. Check: http://localhost:9090/alerts opens.
- [ ] Docker memory: `%UserProfile%\.wslconfig` with `[wsl2]` `memory=10GB` `processors=6`,
      then `wsl --shutdown` and restart Docker Desktop.
- [ ] Install **OBS Studio** (free). Set it up as in [section 6](#6-recording).
- [ ] Rehearse the full script once with a timer, each presenter reading their lines aloud.
- [ ] Make the two cards for the editor (a slide or an image, 1920×1080):
      - **Title card:** "Ward Vitals Pipeline · EC8203 Applied Big Data Engineering ·
        Ninada, Imalsha, Kasun".
      - **Assumptions card** (shown in scene 9): "Simulated data: 15 patients, 6 lab tests,
        seeded Python simulators · 1 simulated day = 5 real minutes (vitals stream in real
        time) · Single-laptop setup: 1 Kafka broker, 1 Spark worker, Airflow standalone ·
        Risk thresholds are project rules, not clinical guidance".

### 20 minutes before recording

In **PowerShell**, in the project folder:

```powershell
.\demo\start-demo.ps1 -Monitoring
```

Let it run **at least 15 minutes** (3 simulated days) before clip D, so the daily risk table,
the reports and the Grafana graphs have history.

### Browser setup (Chrome)

- Window **1920×1080**, page zoom **100 %**, bookmarks bar hidden. Record in full screen
  (**F11**) so no browser chrome is visible.
- Log in to Airflow (admin / admin) and Grafana (admin / admin) beforehand.
- Open these tabs, in this order (**Ctrl+1 … Ctrl+6** switches to them while recording):

| Tab | What | URL |
|---|---|---|
| 1 | Demo UI | http://localhost:8050/?view=pipeline&mode=story |
| 2 | Kafka UI, topic `patient-vitals` | http://localhost:8085/ui/clusters/ward-vitals/all-topics/patient-vitals |
| 3 | Spark UI, Structured Streaming | http://localhost:4040/StreamingQuery/ |
| 4 | Grafana dashboard (kiosk mode, no Grafana menus) | http://localhost:3000/d/ward-vitals-overview/ward-vitals-pipeline?orgId=1&refresh=5s&kiosk |
| 5 | Prometheus alerts | http://localhost:9090/alerts |
| 6 | Airflow DAG grid | http://localhost:8080/dags/daily_lab_consolidation/grid |

- A **Windows Terminal** window (font size 16+), in the project folder, for scene 6.

### In the demo UI, before the first clip

- **Control centre** tab: Patient **P007**, Duration **5 min** (the Live pipeline's
  toolbar uses the same settings). *Break 5 %* off.
- Check that the day box under *Daily batch* shows the last complete day.
- Switch to **Live pipeline** (or press **P**), **Story** mode (press **T** to toggle).

### 2-minute pre-flight check

- [ ] All four chips in the header are green (Kafka, API, Postgres, Airflow).
- [ ] Live pipeline: dots moving, *Monitoring* box shows *Alerts firing: none ✓* (not
      *offline*).
- [ ] Control centre: the Daily risk table has rows and a **Report ↗** link.
- [ ] Airflow: the last runs are green, and the DAG is not paused.
- [ ] Prometheus: nothing firing except, possibly, rehearsal leftovers (they clear by themselves).
- [ ] Windows *Do not disturb* on, chat apps closed.

---

## 5. The script

Speaking pace: about 140 words per minute. Each **Say** block is sized to its time slot.
Words in *[brackets]* are cues for the presenter, not spoken.

---

### Scene 1 · 0:00–0:35 · Introduction (Ninada)

**Screen:** Title card for 3 s (added in editing), then tab 1, **Live pipeline, full view**,
Story mode. Don't click anything; let the dots move.

**Say:**

> Hi, we're Ninada, Imalsha and Kasun, and this is our Ward Vitals Pipeline.
> A hospital ward wants to know two things: which patients show concerning vital-sign trends
> right now, and how yesterday's lab results change that picture.
> What you see is our whole system, running live: every moving dot is a real event in the
> pipeline.
> Everything is simulated: fifteen patients and a daily lab file, from Python scripts. And
> time is compressed: one simulated day is five real minutes. The risk thresholds are project
> rules, not clinical advice.

---

### Scene 2 · 0:35–1:25 · Architecture (Ninada)

**Screen and clicks:**

1. Click the blue **SPEED LAYER** label → the speed layer fills the screen.
2. **Esc**, then click the purple **BATCH LAYER** label.
3. **Esc**, then click the green **SERVING LAYER** label (upper half).
4. **Esc** → full view.

**Say:**

> We chose a Lambda architecture, because this use case has two very different paths.
> *[speed layer]* The speed layer: vital signs stream through Kafka into Spark Structured
> Streaming, and we get an answer within seconds.
> *[batch layer]* The batch layer: once a simulated day, the lab file arrives, and Airflow
> runs a Spark batch job that joins the labs with the whole day of vitals from our Parquet
> data lake.
> *[serving layer]* The serving layer: PostgreSQL holds both views, and one API merges them
> into a single risk score per patient.
> *[full view]* We rejected Kappa. The lab feed is a daily file, not an event stream, and a
> scheduled, re-runnable batch job is the natural fit for it.

---

### Scene 3 · 1:25–2:35 · Ingestion: simulator → Kafka (Ninada)

**Screen and clicks:**

1. Press **T** → Technical mode (topic and table names appear in the boxes).
2. In the toolbar, click **Sudden deterioration** (patient P007). The yellow pill
   *Deterioration · P007* appears under Monitors.
3. Click the **Monitors** box → zoom.
4. Click the **Kafka** box → zoom. Watch the **yellow P007 dots**: they always go into the
   same partition lane. Wait until 2–3 yellow dots have gone in.
5. **Ctrl+2** → Kafka UI: show the 3 partitions on the *Overview* tab, then the **Messages**
   tab: point at the *Key* column (patient IDs) and one JSON value.
6. **Ctrl+1**, **Esc** → full view.

**Say:**

> Ingestion. *[Monitors]* Our bedside simulator sends one JSON reading per patient every two
> to five seconds: heart rate, oxygen, blood pressure and temperature. About four readings a
> second for the ward. I've just started a scenario: patient P007 suddenly deteriorates.
> P007's readings are the yellow dots.
> *[Kafka]* The producer writes to the Kafka topic *patient-vitals*, which has three
> partitions. The message key is the patient ID, so one patient always lands on the same
> partition: watch the yellow dots always go into the same lane. That keeps each patient's
> readings in order, which our trend calculation needs. Three partitions give us parallelism
> without extra overhead for fifteen patients.
> *[Kafka UI]* Here is the same topic in Kafka UI: three partitions, and every message keyed
> by its patient ID.

---

### Scene 4 · 2:35–3:40 · Stream processing: Spark (Ninada)

**Screen and clicks:**

1. In the full view, click **Break 5%** in the toolbar (it must be clicked before zooming:
   the toolbar is hidden while zoomed).
2. Click the **Spark Streaming** box → zoom. Point at a burst of dots arriving (one
   micro-batch) and at the three query rows.
3. When a **red dot** goes to *Dead letters*, say the DLQ line.
4. **Ctrl+3** → Spark UI, Structured Streaming: point at the queries and the input rate vs
   process rate.
5. **Ctrl+1**, **Esc**, click **Stop breaking** in the toolbar.

**Say:**

> Stream processing. Spark Structured Streaming reads Kafka in micro-batches every ten
> seconds: each burst of dots is one batch. One Spark session runs three queries.
> Query one cleans the data: it parses and validates every reading, drops duplicates, and
> uses a one-minute watermark for readings that arrive late. It then computes two-minute
> windows that slide every thirty seconds: average heart rate, lowest oxygen, highest
> temperature. It compares three windows in a row to detect a rising or falling trend, and
> turns all of that into risk points.
> *[red dot]* I'm now breaking five percent of the readings on purpose. The red dots are
> invalid readings: query one sends them to a dead-letter topic with the reason, so they
> never reach the ward board.
> Query three checks the alert rules, and query two archives every raw reading to the Parquet
> lake, for the batch layer.
> *[Spark UI]* In the Spark UI, every query keeps up: input rate and processing rate match.

---

### Scene 5 · 3:40–4:40 · Alerts and serving (Kasun)

**Screen and clicks:**

1. Click the **upper half of the SERVING LAYER** (next to its label) → zoom. Wait for the
   **orange P007 dots** to reach the *Alerts* table in PostgreSQL (it flashes orange).
2. **Esc**, then the **Control centre** tab in the header. The P007 tile is red
   (CONCERNING) and outlined; the dark box under *Scenarios* shows the reaction times.
3. Point at the **Alerts** list on the right.
4. Click the **P007 tile** → the patient panel: point at *Vitals (live) + Labs = total*.
5. Close the panel (**Esc**).

> If P007 is not CONCERNING yet (it usually takes 1–1.5 min), talk through the alerts first;
> the tile turns red while you speak. The reaction-time box shows the exact moment.

**Say:**

> Kasun here: alerts and serving. Query three raises a WARNING for one abnormal reading, and
> a CRITICAL when the same limit is broken three times within a minute, so a single spike
> doesn't page anyone.
> *[orange dots]* The orange dots are P007's alerts. They travel through their own Kafka
> topic, and an alert consumer stores each one exactly once in PostgreSQL.
> *[Control centre]* This is what the ward sees. P007's tile has turned red: CONCERNING. The
> system measured its own reaction time: the first alert was stored about ten seconds after
> P007's vitals changed.
> *[P007 panel]* And this is the Lambda merge in the serving layer: P007's live vital score
> from the speed layer, plus the lab score from the batch layer, gives one combined risk.

---

### Scene 6 · 4:40–5:50 · Observability (Kasun)

**Before this clip:** P007's scenario has ended or you press **Normal ward** first.

**Screen and clicks:**

1. Windows Terminal:
   ```powershell
   docker compose logs --tail 4 --no-log-prefix spark-streaming
   ```
   Point at one JSON line (query name, batch id, row counts).
2. **Ctrl+4** → Grafana: slowly show the top stats row, then the Kafka and Spark graphs and
   the *Latest patient alerts* table.
3. **Ctrl+1** → **Control centre**, click **Sensor feed outage**.
4. Header tab **Live pipeline**: no dots move, Monitors shows 0.
5. **Ctrl+5** → Prometheus alerts: refresh (F5) until `NoVitalsReceived` shows **PENDING**,
   then **FIRING** (about 40–50 s: **cut the wait** in editing).
6. **Ctrl+1** → Control centre, click **Normal ward**. The dots come back.

**Say:**

> Our pipeline is also observable. Every component writes structured JSON logs: here, Spark
> logs each micro-batch with the query, the batch number and the row counts.
> *[Grafana]* Every stage also exports Prometheus metrics, and this Grafana dashboard shows
> the health of the whole pipeline: vitals per second, Spark batch duration, Kafka lag,
> patients by risk category, and the latest alerts.
> *[outage]* Now a failure: all the bedside monitors go silent. The flow stops.
> *[Prometheus]* Our health rule, NoVitalsReceived, watches the Kafka offsets. After thirty
> seconds without a new reading it goes pending *[cut]* and then fires: this is where the
> team would be notified.
> *[Normal ward]* I switch the monitors back on, and the pipeline recovers by itself.

---

### Scene 7 · 5:50–7:30 · Batch layer: labs → Airflow → risk report (Imalsha)

**Screen and clicks:**

1. **Live pipeline**, full view. In the toolbar click **Run daily job** (it runs the last
   complete day).
2. Click the **BATCH LAYER** label → zoom. Point at the lab-file countdown, the landing file
   (*✓ complete*) and the **Airflow tasks** turning green one by one; violet squares move from
   the lake into Spark batch and on to the daily tables. **Cut** the waiting (~1 min) in
   editing.
3. **Ctrl+6** → Airflow grid: click the latest run, show the green tasks, open the
   **Graph** view for 3 s, then the log of `validate_lab_file` for 3 s.
4. **Ctrl+1**, **Esc**, **Control centre**: scroll to **Daily risk: vitals + labs**. Point
   at a **highlighted row** (e.g. P007: *vitals only* → *with labs*).
5. Click **Report ↗** → the HTML report opens in a new tab; scroll once; close it.

**Say:**

> Imalsha here: the batch layer. The second source is the hospital lab. At the end of every
> simulated day, a Python script writes one CSV file with six lab tests per patient, plus a
> success marker, so a half-written file is never read. Here's the countdown to the next file.
> *[Run daily job, zoom]* I've triggered the daily job for yesterday. Airflow runs eight
> tasks. A sensor waits for the file. Validate checks every row. Load replaces that day's lab
> results in one transaction, so running a day again never duplicates data. Then a Spark
> batch job summarises the whole day of vitals from the Parquet lake, and joins that summary
> with the newest lab results.
> *[Airflow]* Here's the same run in Airflow, with the logs of every task.
> *[Daily risk]* And the result: every patient's category from vitals alone, next to the
> category with labs. Highlighted rows are patients whose labs changed the picture. P007's
> high CRP and lactate push it up to CONCERNING. A patient without labs is marked *lab
> unavailable*, never scored as zero.
> *[Report]* Airflow also writes this daily report. That answers the second half of our
> business question.

---

### Scene 8 · 7:30–8:15 · Batch failure drill (Imalsha)

**Screen and clicks:**

1. **Control centre** → *Daily batch* → open **Bad lab file drill** → **Corrupt file**.
2. Click **Run daily job**.
3. Header tab **Live pipeline**, click the **Airflow** box → zoom. Wait until
   **3 validate** turns **red** and the box flashes red. **Cut** the wait.
4. (Optional, 5 s) **Ctrl+5** → Prometheus: `LabFileInvalid` firing.
5. **Esc**, **Control centre** → **Restore file** → **Run daily job** (no need to show the
   re-run; it goes green again).

**Say:**

> What if the lab sends a broken file? I corrupt yesterday's file: one value isn't a number,
> and one reference range is upside down.
> *[red]* Validation rejects the whole file, so nothing wrong reaches the database. The task
> turns red, the failure is recorded in our health table, and Prometheus raises
> LabFileInvalid. Once the lab sends a correct file again, the same run simply succeeds.

---

### Scene 9 · 8:15–9:20 · Summary, trade-offs, assumptions (Kasun)

**Screen:** **Live pipeline**, full view (**Esc**), Story mode (**T**). For the last
15 s, the **Assumptions card** (added in editing).

**Say:**

> To sum up: our system answers the business question. The speed layer shows, within
> seconds, which patients are concerning right now, and every simulated day the batch layer
> adds yesterday's labs to that picture.
> Lambda has a cost: two code paths that could drift apart. So both paths use one shared
> rules module, and the API labels where every number comes from.
> For production, we would run three Kafka brokers with replication, a real Spark cluster,
> object storage for the lake, authentication and encryption for patient data, and paging
> for critical alerts.
> *[Assumptions card]* Our assumptions once more: all data is simulated, one simulated day is
> five real minutes, everything runs on a single laptop, and the thresholds are project
> rules, not clinical guidance.
> Thank you for watching.

---

## 6. Recording

**OBS Studio settings**

- *Settings → Video:* base and output resolution **1920×1080**, **30 fps**.
- *Settings → Output:* recording format **MKV** (safe if OBS crashes), then *File → Remux
  Recordings* to MP4.
- *Sources:* **Display Capture** (not Window Capture: scenes switch between Chrome and
  Terminal), plus the microphone.
- On the microphone: *Filters → Noise Suppression* (RNNoise) and *Gain* if needed. A headset
  mic beats the laptop mic.
- Test: record 20 s, play it back, check the sound level and that the text is sharp.

**How to record**

- Record **live narration while you click**: the animations are live, so speaking as they
  happen keeps sound and picture in sync. Keep the script on a phone or a second screen.
- One clip per group in [section 3](#3-scene-overview): A (scenes 1–2), B (3–5), C (6),
  D (7–9). A mistake costs one clip, not the whole video.
- **Before clip B**, check that no scenario is running (*Normal ward*), so P007 starts
  from a normal state.
- **Before clip D**, wait until the Live pipeline shows no Airflow run in progress, so your
  *Run daily job* is the one on screen.
- Move the mouse slowly, and rest it on the thing you're talking about. Pause 1 s after each
  click, so the zoom animation completes before you speak about it.
- Leave 2 s of silence at the start and end of every clip for clean cuts.

---

## 7. Editing and delivery

Use **Clipchamp** (built into Windows 11) or any editor.

1. Add the clips in order: title card (3 s), A, B, C, D, assumptions card is inside scene 9.
2. **Cut the waits** and add a small caption where time was cut:
   - scene 6, waiting for `NoVitalsReceived` to fire → caption *"40 seconds later"*;
   - scene 7, the DAG run → *"1 minute later"*;
   - scene 8, the validation run → *"30 seconds later"*.
3. Optional: a one-line caption at each scene start (*"Speed layer · Ingestion"*,
   *"Observability"*…), bottom-left, small.
4. Check the total length: **between 5:00 and 10:00** (target 9:20).
5. Export **1080p MP4**.
6. Upload as an **unlisted YouTube video** or a Google Drive / OneDrive file shared with
   *anyone with the link*. Open the link in a private browser window to test it.
7. Put the link in the README and in the report.

**Final check, while watching the export:**

- [ ] Simulated-time compression and assumptions are said at the start and shown at the end.
- [ ] Kafka partitions and keys are visible (scene 3).
- [ ] A Spark transformation is explained, not just shown (scene 4).
- [ ] An alert appears and the ward board changes (scene 5).
- [ ] A health alert **fires** (scene 6).
- [ ] Airflow runs, and the joined vitals + labs result is shown (scene 7).
- [ ] Every member speaks about their own slice.
- [ ] No passwords, personal tabs or notifications are visible.

---

## 8. If something goes wrong while recording

| Problem | Fix |
|---|---|
| *Monitoring* box says *offline*, Prometheus tab won't open | Port 9090 is taken. Close the program holding it, then `.\demo\start-demo.ps1 -Monitoring` |
| P007 isn't CONCERNING by the end of scene 5 | Keep talking about the alerts; the tile turns red within about 1.5 min of the scenario start. Or re-record clip B with Duration 5 min |
| No yellow dots | The scenario isn't running: the toolbar's patient must be P007. Click *Sudden deterioration* again |
| `NoVitalsReceived` stays pending | Keep waiting; it fires 40–50 s after the outage starts. If it never goes pending, check Prometheus is running |
| The Airflow tasks don't move after *Run daily job* | A scheduled run is still busy; wait for it to finish (every 5 minutes a new day starts), then click again |
| Dots stop everywhere, header chips red | Check `docker compose ps`; restart with `.\demo\start-demo.ps1 -Monitoring` (all data is kept) |
| Zoomed in and lost | Press **Esc** |

More fixes: [DEMO.md section 7](DEMO.md#7-troubleshooting-during-the-demo) and
[demo/README.md](../demo/README.md#troubleshooting).
