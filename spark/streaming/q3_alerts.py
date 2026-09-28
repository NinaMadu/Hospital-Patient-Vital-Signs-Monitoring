"""Q3: threshold and sustained-abnormal alert rules -> patient-alerts topic.

Owner: Member C.

Two streaming queries share the validated stream from Q1's parse() (invalid events are
Q1's job: they go to vitals-dlq and never reach the rules):

  q3_threshold_alerts  every valid reading, stateless
                       one alert per broken limit (SPO2_LOW, HEART_RATE_HIGH, ...)  WARNING
  q3_sustained_alerts  watermark -> drop duplicate event_ids -> 1-minute tumbling window
                       per patient; the same limit broken in >= 3 readings of the window
                       -> SUSTAINED_<rule>                                           CRITICAL

Both write JSON alerts (key = patient_id) to Kafka `patient-alerts`. The alert consumer
(api/alert_consumer.py) stores them in the vital_alerts table.

Delivery: the Kafka sink is at-least-once (a micro-batch can be re-sent after a crash), so
every alert has a deterministic alert_id: <event_id>:<rule> for a threshold alert and
<patient_id>:<rule>:<window start epoch> for a sustained one. The consumer inserts with
ON CONFLICT (alert_id) DO NOTHING, so the table ends up with each alert exactly once.

Limits come from config/thresholds.yaml through common/risk_rules.py, the same rules the
risk scores use, so an alert and a risk point can never disagree about what "low SpO2" is.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from common.config import load_thresholds
from common.risk_rules import RiskRules, default_rules
from common.sim_clock import sim_day_column
from spark.streaming import q1_windows as q1

ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "patient-alerts")
_SETTINGS = load_thresholds()["alerts"]
SUSTAINED_WINDOW = _SETTINGS["sustained_window"]
SUSTAINED_MIN_READINGS = int(_SETTINGS["sustained_min_readings"])
WATERMARK = _SETTINGS["watermark"]

# Every alert, from either query, has exactly these fields (the patient-alerts contract).
ALERT_COLUMNS = [
    "alert_id", "patient_id", "bed_id", "alert_type", "rule", "severity",
    "metric", "metric_value", "threshold", "event_time", "window_start", "window_end",
    "abnormal_count", "reading_count", "sim_day", "message",
]


@dataclass(frozen=True)
class Rule:
    """One limit on one vital, e.g. Rule("SPO2_LOW", "spo2", "below", 92)."""
    name: str
    vital: str
    direction: str          # "below" or "above"
    limit: float

    def breached(self, value: float | None) -> bool:
        """Plain-Python check (tests, docs). NULL is never a breach."""
        if value is None:
            return False
        return value < self.limit if self.direction == "below" else value > self.limit

    def breached_col(self) -> Column:
        """The same check as a Spark expression."""
        col = F.col(self.vital)
        return col < self.limit if self.direction == "below" else col > self.limit


def alert_rules(rules: RiskRules | None = None) -> list[Rule]:
    """The per-reading limits, taken from the shared risk rules (config/thresholds.yaml)."""
    r = rules or default_rules()
    return [
        Rule("SPO2_LOW", "spo2", "below", r.spo2_below),
        Rule("HEART_RATE_HIGH", "heart_rate", "above", r.hr_above),
        Rule("HEART_RATE_LOW", "heart_rate", "below", r.hr_below),
        Rule("TEMPERATURE_HIGH", "temperature", "above", r.temp_above),
        Rule("SYSTOLIC_BP_HIGH", "systolic_bp", "above", r.sbp_above),
        Rule("SYSTOLIC_BP_LOW", "systolic_bp", "below", r.sbp_below),
    ]


def _explode_hits(df: DataFrame, candidates: list[Column]) -> DataFrame:
    """Keep the non-NULL structs of `candidates` (one per fired rule), one row each."""
    hits = F.filter(F.array(*candidates), lambda c: c.isNotNull())
    return df.withColumn("a", F.explode(hits))


# -------------------------------------------------------- threshold alerts --

def threshold_alerts(valid: DataFrame, rules: list[Rule] | None = None) -> DataFrame:
    """One WARNING alert per reading and broken limit. Stateless: no window, no memory."""
    rules = rules or alert_rules()
    candidates = [
        F.when(r.breached_col(), F.struct(
            F.lit(r.name).alias("rule"),
            F.lit(r.vital).alias("metric"),
            F.col(r.vital).cast("double").alias("metric_value"),
            F.lit(float(r.limit)).alias("threshold"),
            F.lit(r.direction).alias("direction"),
        ))
        for r in rules
    ]
    return _explode_hits(valid, candidates).select(
        F.concat_ws(":", "event_id", "a.rule").alias("alert_id"),
        "patient_id",
        "bed_id",
        F.lit("THRESHOLD").alias("alert_type"),
        "a.rule",
        F.lit("WARNING").alias("severity"),
        "a.metric",
        "a.metric_value",
        "a.threshold",
        "event_time",
        F.lit(None).cast("timestamp").alias("window_start"),
        F.lit(None).cast("timestamp").alias("window_end"),
        F.lit(1).alias("abnormal_count"),
        F.lit(1).alias("reading_count"),
        sim_day_column(F.col("event_time")).alias("sim_day"),
        F.format_string("%s %.1f %s limit %.1f", "a.metric", "a.metric_value",
                        "a.direction", "a.threshold").alias("message"),
    )


# -------------------------------------------------------- sustained alerts --

def sustained_alerts(events: DataFrame, rules: list[Rule] | None = None,
                     window: str = SUSTAINED_WINDOW,
                     min_readings: int = SUSTAINED_MIN_READINGS) -> DataFrame:
    """CRITICAL alert when one limit is broken in >= min_readings readings of a window.

    Per patient and tumbling window, count the readings that break each limit and keep the
    worst value. A single spike (one bad reading) never raises this alert; a patient who
    stays abnormal does. In a stream the caller adds the watermark first (see start()).
    """
    rules = rules or alert_rules()
    aggs = [
        F.count(F.lit(1)).cast("int").alias("reading_count"),
        F.max("event_time").alias("last_event_time"),
        F.first("bed_id", ignorenulls=True).alias("bed_id"),
    ]
    for r in rules:
        worst = F.min if r.direction == "below" else F.max
        aggs += [
            F.sum(r.breached_col().cast("int")).cast("int").alias(f"n_{r.name}"),
            worst(F.when(r.breached_col(), F.col(r.vital))).cast("double").alias(f"worst_{r.name}"),
        ]
    windows = events.groupBy(F.window("event_time", window), "patient_id").agg(*aggs)

    candidates = [
        F.when(F.col(f"n_{r.name}") >= min_readings, F.struct(
            F.lit(f"SUSTAINED_{r.name}").alias("rule"),
            F.lit(r.vital).alias("metric"),
            F.col(f"worst_{r.name}").alias("metric_value"),
            F.lit(float(r.limit)).alias("threshold"),
            F.lit(r.direction).alias("direction"),
            F.col(f"n_{r.name}").alias("abnormal_count"),
        ))
        for r in rules
    ]
    return _explode_hits(windows, candidates).select(
        F.concat_ws(":", "patient_id", "a.rule",
                    F.unix_timestamp("window.start").cast("string")).alias("alert_id"),
        "patient_id",
        "bed_id",
        F.lit("SUSTAINED").alias("alert_type"),
        "a.rule",
        F.lit("CRITICAL").alias("severity"),
        "a.metric",
        "a.metric_value",
        "a.threshold",
        F.col("last_event_time").alias("event_time"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "a.abnormal_count",
        "reading_count",
        sim_day_column(F.col("last_event_time")).alias("sim_day"),
        F.format_string("%s %s limit %.1f in %d of %d readings (worst %.1f)",
                        "a.metric", "a.direction", "a.threshold", "a.abnormal_count",
                        "reading_count", "a.metric_value").alias("message"),
    )


# ------------------------------------------------------------------ output --

def to_kafka(alerts: DataFrame) -> DataFrame:
    """Alert rows -> Kafka (key, value) rows: key = patient_id, value = JSON alert."""
    return alerts.select(
        F.col("patient_id").alias("key"),
        F.to_json(F.struct(*ALERT_COLUMNS, F.current_timestamp().alias("detected_at")))
         .alias("value"),
    )


def valid_readings(raw: DataFrame) -> DataFrame:
    """Kafka rows -> valid readings, using Q1's parser and validation rules."""
    return (q1.parse(raw).filter(F.col("reason").isNull())
            .select("event_id", "patient_id", "bed_id", *q1.VITALS, "event_time"))


def start(raw: DataFrame, kafka_bootstrap: str) -> list:
    """Start Q3's two streaming queries on the raw Kafka stream."""
    valid = valid_readings(raw)

    def kafka_sink(df: DataFrame, name: str):
        return (to_kafka(df).writeStream
                .queryName(name)
                .outputMode("append")
                .format("kafka")
                .option("kafka.bootstrap.servers", kafka_bootstrap)
                .option("topic", ALERTS_TOPIC)
                .option("checkpointLocation", f"{q1.CHECKPOINTS}/{name}")
                .trigger(processingTime=q1.TRIGGER)
                .start())

    threshold = kafka_sink(threshold_alerts(valid), "q3_threshold_alerts")

    # Append mode: a window's alert is emitted once, when the watermark passes the window
    # end (window 1 min + watermark 30 s + trigger 10 s => at most ~100 s after it starts).
    deduped = (valid.withWatermark("event_time", WATERMARK)
               .dropDuplicatesWithinWatermark(["event_id"]))
    sustained = kafka_sink(sustained_alerts(deduped), "q3_sustained_alerts")
    return [threshold, sustained]
