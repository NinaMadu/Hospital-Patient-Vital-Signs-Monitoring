"""Environment smoke test for the Spark cluster.

Checks that a job can: run on the standalone cluster, write and read Parquet in the shared
lake volume, load the Kafka connector and the PostgreSQL JDBC driver, and write to Postgres.
Run it from Airflow (DAG env_check) or directly:

    docker compose exec spark-master spark-submit /opt/project/scripts/spark_env_check.py
"""
import json
import os
import sys

from pyspark.sql import SparkSession


def log(event: str, **fields) -> None:
    print(json.dumps({"component": "env-check", "event": event, **fields}), flush=True)


def main() -> int:
    spark = SparkSession.builder.appName("env-check").getOrCreate()
    sc = spark.sparkContext
    log("spark_session", master=sc.master, version=spark.version,
        python=sys.version.split()[0])

    # 1. Executors run Python on the worker (catches driver/worker Python mismatches).
    worker_python = sc.parallelize([0], 1).map(lambda _: sys.version.split()[0]).collect()[0]
    log("executor_python", python=worker_python)

    # 2. Shared Parquet lake is writable from executors and readable from the driver.
    path = "/data/lake/_env_check"
    spark.range(100).withColumnRenamed("id", "n").write.mode("overwrite").parquet(path)
    count = spark.read.parquet(path).count()
    log("lake_roundtrip", path=path, rows=count)
    assert count == 100

    # 3. Extra jars are on the classpath.
    for cls in ("org.apache.spark.sql.kafka010.KafkaSourceProvider", "org.postgresql.Driver"):
        sc._jvm.java.lang.Class.forName(cls)
        log("class_loaded", cls=cls)

    # 4. JDBC write to the ward database.
    url = f"jdbc:postgresql://postgres:5432/{os.environ['POSTGRES_DB']}"
    (spark.createDataFrame([("env-check", "spark-jdbc", "OK")],
                           ["component", "check_name", "status"])
        .write.mode("append").format("jdbc")
        .option("url", url).option("dbtable", "pipeline_health")
        .option("user", os.environ["POSTGRES_USER"])
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .save())
    log("jdbc_write", table="pipeline_health")

    log("env_check_passed")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
