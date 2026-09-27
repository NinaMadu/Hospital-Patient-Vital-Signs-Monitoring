"""Q1: parse, validate, de-duplicate, watermark, sliding windows -> patient_current_status.

Owner: Member A. Invalid records go to the vitals-dlq topic.

Two streaming queries share one parsed stream:
  q1_dlq      invalid events  -> Kafka topic vitals-dlq (with a `reason`)
  q1_windows  valid events    -> watermark -> drop duplicate event_ids
                              -> 2-minute windows sliding every 30 s, per patient
                              -> foreachBatch upsert into patient_current_status
"""
from __future__ import annotations

from datetime import timezone

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.config import get_settings, load_thresholds
from common.sim_clock import sim_day_column

SETTINGS = get_settings()
WINDOW = SETTINGS.streaming.window_duration
SLIDE = SETTINGS.streaming.slide_duration
WATERMARK = SETTINGS.streaming.watermark
TRIGGER = SETTINGS.streaming.trigger_interval
DLQ_TOPIC = SETTINGS.kafka.topics.dlq
CHECKPOINTS = SETTINGS.paths.checkpoints

# Physically plausible limits. A value outside them is a sensor or transmission error,
# not a sick patient, so the event is rejected. Clinical thresholds are a separate thing
# (config/thresholds.yaml) and only mark a valid reading as abnormal.
VALID_RANGES = {
    "heart_rate": (20, 250),
    "spo2": (50, 100),
    "systolic_bp": (50, 260),
    "diastolic_bp": (20, 160),
    "temperature": (30, 45),
}
VITALS = list(VALID_RANGES)
REQUIRED = ["event_id", "patient_id", "bed_id", *VITALS, "timestamp"]
CORRUPT = "_corrupt_record"

EVENT_SCHEMA = T.StructType(
    [T.StructField(c, T.StringType()) for c in ("event_id", "patient_id", "bed_id")]
    + [T.StructField(v, T.DoubleType()) for v in VITALS]
    + [T.StructField("timestamp", T.StringType()), T.StructField(CORRUPT, T.StringType())]
)

THRESHOLDS = load_thresholds().vitals


# ---------------------------------------------------------------- parsing --

def parse(raw: DataFrame) -> DataFrame:
    """Kafka rows (value bytes) -> one column per field, plus event_time and `reason`.

    `reason` is NULL for a valid event, otherwise why it was rejected. The first matching
    check wins, so a record gets exactly one reason.
    """
    value = F.col("value").cast("string")
    parsed = F.from_json(value, EVENT_SCHEMA, {"columnNameOfCorruptRecord": CORRUPT})
    df = (raw.select(value.alias("raw"), "partition", "offset", parsed.alias("e"))
             .select("raw", "partition", "offset", "e.*")
             .withColumn("event_time", F.col("timestamp").cast("timestamp")))

    # concat_ws skips NULLs, so these become e.g. "heart_rate,spo2", or "" when all is well.
    missing = F.concat_ws(",", *[F.when(F.col(c).isNull(), F.lit(c)) for c in REQUIRED])
    out_of_range = F.concat_ws(",", *[
        F.when((F.col(v) < lo) | (F.col(v) > hi), F.lit(v)) for v, (lo, hi) in VALID_RANGES.items()
    ])
    reason = (
        F.when(F.col(CORRUPT).isNotNull() & F.col("event_id").isNull(), F.lit("malformed_json"))
        .when(F.col(CORRUPT).isNotNull(), F.lit("wrong_type"))
        .when(missing != "", F.concat(F.lit("missing_or_null:"), missing))
        .when(F.col("event_time").isNull(), F.lit("bad_timestamp"))
        .when(out_of_range != "", F.concat(F.lit("out_of_range:"), out_of_range))
    )
    return df.withColumn("reason", reason).drop(CORRUPT)


def dlq_records(parsed: DataFrame) -> DataFrame:
    """Invalid events as Kafka (key, value) rows for vitals-dlq; the raw payload is kept."""
    return parsed.filter(F.col("reason").isNotNull()).select(
        F.col("patient_id").alias("key"),
        F.to_json(F.struct(
            "reason",
            "raw",
            F.col("partition").alias("source_partition"),
            F.col("offset").alias("source_offset"),
            F.current_timestamp().alias("detected_at"),
        )).alias("value"),
    )


# ---------------------------------------------------------------- windows --

def is_abnormal() -> F.Column:
    """A single reading outside the project thresholds (config/thresholds.yaml)."""
    t = THRESHOLDS
    hr, bp = t["heart_rate_avg"], t["systolic_bp"]
    return (
        (F.col("spo2") < t["spo2_min"]["below"])
        | (F.col("heart_rate") < hr["below"]) | (F.col("heart_rate") > hr["above"])
        | (F.col("temperature") > t["temperature_max"]["above"])
        | (F.col("systolic_bp") < bp["below"]) | (F.col("systolic_bp") > bp["above"])
    )


def deduplicate(valid: DataFrame) -> DataFrame:
    """Watermark on event time, then drop repeated event_ids (streaming only).

    The watermark tells Spark how late an event may arrive (1 minute behind the newest
    event seen). Older events are dropped, and state for windows and event_ids older
    than the watermark is discarded, so memory stays bounded.
    """
    return (valid.withWatermark("event_time", WATERMARK)
            .dropDuplicatesWithinWatermark(["event_id"]))


def window_aggregates(events: DataFrame) -> DataFrame:
    """Per patient, per 2-minute window sliding every 30 s: avg/min/max of every vital."""
    aggs = []
    for v in VITALS:
        aggs += [F.avg(v).alias(f"avg_{v}"), F.min(v).alias(f"min_{v}"), F.max(v).alias(f"max_{v}")]
    return (
        events.groupBy(F.window("event_time", WINDOW, SLIDE), "patient_id")
        .agg(*aggs,
             F.count(F.lit(1)).cast("int").alias("reading_count"),
             F.sum(is_abnormal().cast("int")).cast("int").alias("abnormal_count"),
             F.max("event_time").alias("last_event_time"))
        .select(F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "*")
        .drop("window")
    )


def current_status(windows: DataFrame) -> DataFrame:
    """One row per patient: the window that ends with the patient's latest reading.

    A reading sits in 4 overlapping windows (2 min / 30 s). Among the windows holding the
    latest reading, the earliest-starting one covers the fullest 2 minutes of history.
    """
    pick = Window.partitionBy("patient_id").orderBy(
        F.col("last_event_time").desc(), F.col("window_start").asc())
    return (windows.withColumn("_rank", F.row_number().over(pick))
            .filter("_rank = 1").drop("_rank")
            .withColumn("sim_day", sim_day_column(F.col("last_event_time"))))


# ----------------------------------------------------------------- output --

STATUS_COLUMNS = [
    "patient_id", "window_start", "window_end", "last_event_time", "sim_day",
    *[f"{agg}_{v}" for v in VITALS for agg in ("avg", "min", "max")],
    "reading_count", "abnormal_count",
]

UPSERT_SQL = f"""
INSERT INTO patient_current_status ({", ".join(STATUS_COLUMNS)}) VALUES %s
ON CONFLICT (patient_id) DO UPDATE SET
    {", ".join(f"{c} = EXCLUDED.{c}" for c in STATUS_COLUMNS[1:])},
    updated_at = now()
WHERE patient_current_status.last_event_time <= EXCLUDED.last_event_time
"""


def pg_connect():
    import psycopg2

    return psycopg2.connect(SETTINGS.postgres.dsn)


def upsert_batch(batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch sink: runs on the driver once per micro-batch.

    Idempotent: if Spark re-runs a batch after a crash, the same rows are upserted again
    and the table ends up the same. The WHERE clause stops a late batch from replacing a
    newer window with an older one.
    """
    rows = current_status(batch_df).select(*STATUS_COLUMNS).collect()  # <= 15 rows
    if not rows:
        return
    utc = [c for c in STATUS_COLUMNS if c.endswith(("_start", "_end", "_time"))]
    values = [
        tuple(r[c].replace(tzinfo=timezone.utc) if c in utc else r[c] for c in STATUS_COLUMNS)
        for r in rows
    ]
    from psycopg2.extras import execute_values

    with pg_connect() as conn, conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, values)
    conn.close()
    print(f'{{"component": "q1_windows", "event": "upsert", "batch_id": {batch_id}, '
          f'"rows": {len(values)}}}', flush=True)


def start(raw: DataFrame, kafka_bootstrap: str) -> list:
    """Start Q1's two streaming queries on the raw Kafka stream."""
    parsed = parse(raw)

    dlq = (dlq_records(parsed).writeStream
           .queryName("q1_dlq")
           .format("kafka")
           .option("kafka.bootstrap.servers", kafka_bootstrap)
           .option("topic", DLQ_TOPIC)
           .option("checkpointLocation", f"{CHECKPOINTS}/q1_dlq")
           .trigger(processingTime=TRIGGER)
           .start())

    valid = parsed.filter(F.col("reason").isNull()).select("event_id", "patient_id", *VITALS, "event_time")
    windows = (window_aggregates(deduplicate(valid)).writeStream
               .queryName("q1_windows")
               .outputMode("update")          # emit every window that changed in this batch
               .foreachBatch(upsert_batch)
               .option("checkpointLocation", f"{CHECKPOINTS}/q1")
               .trigger(processingTime=TRIGGER)
               .start())
    return [dlq, windows]
