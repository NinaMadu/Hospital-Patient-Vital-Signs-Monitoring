"""Q2: archive raw vital events to the Parquet lake, partitioned by sim_day (Lambda master dataset).

Owner: Member B.

    patient-vitals (Kafka, from earliest) -> parse (same parser as Q1) -> + sim_day, Kafka lineage
        -> Parquet file sink, /data/lake/vitals/sim_day=N/part-*.parquet

The lake is the batch layer's source of truth: vital_daily_summary (A4) and any recompute
read it, never Kafka. Every event is kept, including invalid ones (reason IS NOT NULL), so a
day can be reprocessed later if the validation rules change. Nothing is ever updated in place.

Exactly-once: the checkpoint stores the Kafka offsets of each micro-batch, and the file sink
records the files of each committed batch in /data/lake/vitals/_spark_metadata. After a crash,
Spark re-runs the unfinished batch; files from the failed attempt are not in the log, so
readers that go through the log never see them twice. That is why readers must use
read_lake() (the lake root), not a sim_day=N folder path directly.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.config import get_settings
from common.sim_clock import sim_day_column
from spark.streaming import q1_windows

ARCHIVE_TRIGGER = "30 seconds"  # longer than Q1's 10 s: fewer, larger Parquet files

VITAL_COLUMNS = ["heart_rate", "spo2", "systolic_bp", "diastolic_bp", "temperature"]
ARCHIVE_COLUMNS = [
    "event_id", "patient_id", "bed_id", *VITAL_COLUMNS, "event_time",
    "reason",                                   # NULL = valid; else why Q1 rejected it
    "raw",                                      # the original Kafka value, untouched
    "kafka_partition", "kafka_offset", "ingested_at",
    "sim_day",
]


def lake_path() -> str:
    return get_settings().paths.lake_vitals


def archive_records(raw: DataFrame) -> DataFrame:
    """Kafka rows -> lake rows. Works on a stream or on a static DataFrame (tests).

    Uses Q1's parser so the speed and batch layers agree on what "valid" means.
    sim_day comes from event time; an event with no usable timestamp (malformed JSON)
    falls back to the time it was archived, so it still lands in a partition.
    """
    ingested_at = F.current_timestamp()
    return (
        q1_windows.parse(raw)
        .withColumnRenamed("partition", "kafka_partition")
        .withColumnRenamed("offset", "kafka_offset")
        .withColumn("ingested_at", ingested_at)
        .withColumn("sim_day", F.coalesce(sim_day_column(F.col("event_time")),
                                          sim_day_column(F.col("ingested_at"))))
        .select(*ARCHIVE_COLUMNS)
    )


def start(raw: DataFrame, path: str | None = None, checkpoint: str | None = None,
          trigger: dict | None = None):
    """Start Q2 on the raw Kafka stream. Returns the StreamingQuery."""
    s = get_settings()
    return (
        archive_records(raw).writeStream
        .queryName("q2_archive")
        .format("parquet")
        .outputMode("append")                    # the file sink only appends
        .partitionBy("sim_day")
        .option("path", path or s.paths.lake_vitals)
        .option("checkpointLocation", checkpoint or f"{s.paths.checkpoints}/q2")
        .trigger(**(trigger or {"processingTime": ARCHIVE_TRIGGER}))
        .start()
    )


def read_lake(spark: SparkSession, sim_day: int | None = None, valid_only: bool = False,
              path: str | None = None) -> DataFrame:
    """Batch read of the lake for A4 / B8 / replays.

    Reads the lake root so Spark follows _spark_metadata (committed files only), then
    filters on the partition column, which Spark turns into partition pruning: only the
    sim_day=N folder is scanned.
    """
    df = spark.read.parquet(path or lake_path())
    if sim_day is not None:
        df = df.where(F.col("sim_day") == sim_day)
    if valid_only:
        df = df.where(F.col("reason").isNull())
    return df
