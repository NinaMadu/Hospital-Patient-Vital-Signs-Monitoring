"""Speed-layer entry point: one SparkSession running queries Q1, Q2 and Q3 over patient-vitals.

Shared by all members. Each owner adds one start() call below.

    docker compose up -d spark-streaming           # runs this file with spark-submit
    Spark UI (Structured Streaming tab): http://localhost:4040
"""
import os

from pyspark.sql import DataFrame, SparkSession

from spark.streaming import q1_windows, q2_archive

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_INTERNAL", "kafka:9092")
VITALS_TOPIC = os.getenv("VITALS_TOPIC", "patient-vitals")


def read_vitals(spark: SparkSession, starting_offsets: str = "latest") -> DataFrame:
    """The raw patient-vitals stream: key, value (bytes), partition, offset, timestamp.

    starting_offsets only applies on a query's very first start; after that, the query
    resumes from the offsets saved in its checkpoint.
    """
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", VITALS_TOPIC)
            .option("startingOffsets", starting_offsets)
            .option("maxOffsetsPerTrigger", 5000)
            .load())


def main() -> None:
    spark = SparkSession.builder.appName("ward-speed-layer").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    q1_windows.start(read_vitals(spark), KAFKA_BOOTSTRAP)
    # Q2 starts from the earliest retained offset so the lake holds all history (first start only).
    q2_archive.start(read_vitals(spark, "earliest"))   # Member B (B4)
    # q3_alerts.start(read_vitals(spark), KAFKA_BOOTSTRAP)  # Member C (C3)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
