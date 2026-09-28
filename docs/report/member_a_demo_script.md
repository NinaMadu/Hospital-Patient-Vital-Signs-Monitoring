# Demo video: Ninada's part (0:00–3:30)

Presenter: Ninada (A). Kasun takes over at 3:30 (API, Grafana, alerts).

## Before recording

Run these on the demo laptop about 15 minutes before recording, so the windows, trends and lake
have data.

```powershell
docker compose up -d
docker compose --profile monitoring up -d
docker compose ps                      # everything running or healthy
```

Open these browser tabs, in this order:
1. The architecture diagram (report Figure 4.1).
2. Kafka UI, topic `patient-vitals`, **Overview** tab: http://localhost:8085/ui/clusters/ward-vitals/all-topics/patient-vitals
3. Kafka UI, topic `patient-vitals`, **Messages** tab.
4. Spark UI, **Structured Streaming** tab: http://localhost:4040/StreamingQuery/
5. A terminal in the project folder.

## Script

| Time | Show | Say (key points) |
|---|---|---|
| 0:00–0:20 | Title slide or README | "We built a ward vital-signs monitoring pipeline for EC8203: 15 patients, bedside readings every few seconds, and a lab file once a day." |
| 0:20–0:45 | The business question (report §2) | "Two questions: which patients show concerning trends *right now*, and how do yesterday's labs change the risk picture. One needs seconds, the other a complete daily answer." |
| 0:45–1:00 | Architecture diagram | "That is why we chose Lambda. The speed layer, Spark Structured Streaming, answers the live question. The batch layer, Airflow and Spark batch, recomputes a daily view from a Parquet lake and joins the labs. The serving layer merges both. Kappa was rejected because the lab feed is a daily file and we need replay beyond Kafka's retention." |
| 1:00–1:30 | Terminal: `docker compose ps` | "Everything runs in one Docker Compose file: Kafka, a Spark master and worker, the streaming app, Airflow, Postgres, the API, and a monitoring profile with Prometheus and Grafana." |
| 1:30–2:10 | Terminal: `docker compose logs -f --tail 5 vital-producer` (stop with Ctrl+C), then Kafka UI Messages tab | "The simulator sends one JSON reading per patient every 2 to 5 seconds, keyed by patient ID. Here you can see the key and partition of each message." Point at P007 always on the same partition. |
| 2:10–2:40 | Kafka UI Overview tab | "`patient-vitals` has 3 partitions. The key keeps each patient on one partition, so the readings stay in order. The producer uses `acks=all` and idempotence, so a retry never writes a reading twice. Bad readings go to a dead-letter topic with a reason." |
| 2:40–3:15 | Spark UI: the list of queries, then click `q1_windows` | "One Spark app runs five queries. Q1 validates, drops duplicates within a one-minute watermark, and computes 2-minute windows sliding every 30 seconds per patient. Each batch finishes in about 4 seconds, well inside the 10-second trigger, and the input rate stays steady." Point at Input Rate, Batch Duration and Global Watermark Gap. |
| 3:15–3:30 | Terminal: the command below | "Q1 writes each patient's current window, trend and risk score to Postgres, about 6 seconds after the reading. Kasun will now show the API and the alerts." |

The command for 3:15:

```powershell
docker compose exec postgres psql -U ward -d ward -c "SELECT patient_id, round(avg_heart_rate::numeric) hr, min_spo2, hr_trend, spo2_trend, vital_risk_score, vital_risk_category FROM patient_current_status ORDER BY vital_risk_score DESC LIMIT 6;"
```

## For Kasun's part (3:30–5:30)

These scenarios are seeded, so the same alerts fire every time:

```powershell
docker compose stop vital-producer
docker compose run --rm vital-producer python -m simulators.vital_producer.main --scenario spike --patient P007 --scenario-start 10
# the no-data alert instead: --scenario outage --scenario-start 10 --scenario-duration 90
docker compose start vital-producer    # afterwards
```

- **Trend demo.** `--scenario spo2_drop` shows the `FALLING` trend: SpO₂ is marked `FALLING`
  about 75 s after the start.
- **Alert timing.** `spike` produces alerts within about 10 s.
