"""Learning lab: score one day's lab file with Spark and the shared risk rules.

Throwaway preview of B8 (risk join) using only the lab side:
  labs_day=N.csv  -> lab_points_col / lab_flag_col per result
                  -> sum per patient, left-join the full patient list
                  -> patients with no rows = LAB_UNAVAILABLE (never lab score 0)

    docker compose exec spark-master /opt/spark/bin/spark-submit /opt/project/labs/spark_batch/lab_risk_preview.py --day 1
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.config import get_settings
from common.risk_rules import LAB_AVAILABLE, LAB_UNAVAILABLE, category_col, lab_flag_col, lab_points_col

p = argparse.ArgumentParser()
p.add_argument("--day", type=int, required=True)
args = p.parse_args()

s = get_settings()
path = f"{s.paths.landing_labs}/labs_day={args.day}.csv"
spark = SparkSession.builder.appName(f"lab-risk-preview-day{args.day}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

labs = spark.read.csv(path, header=True, inferSchema=True)
print(f"\n== 1. Raw file {path}: {labs.count()} rows, schema:")
labs.printSchema()

scored = (labs.withColumn("points", lab_points_col())
              .withColumn("flag", lab_flag_col()))
print("== 2. Out-of-range results (points from common/risk_rules.py):")
scored.filter("points > 0").select("patient_id", "test_type", "result_value",
                                   "reference_low", "reference_high", "flag", "points").show(50, False)

per_patient = scored.groupBy("patient_id").agg(
    F.sum("points").cast("int").alias("lab_risk_score"),
    F.concat_ws(",", F.sort_array(F.collect_list(
        F.when(F.col("flag").isNotNull(), F.concat_ws(":", "test_type", "flag"))))).alias("abnormal_labs"),
)

patients = spark.createDataFrame(
    [(f"{s.patients.id_prefix}{i:03d}",) for i in range(1, s.patients.count + 1)], "patient_id string")
result = (patients.join(per_patient, "patient_id", "left")
          .withColumn("lab_status", F.when(F.col("lab_risk_score").isNull(), F.lit(LAB_UNAVAILABLE))
                      .otherwise(F.lit(LAB_AVAILABLE)))
          .withColumn("lab_category", category_col("lab_risk_score"))
          .orderBy("patient_id"))

print("== 3. Per patient (left join: a patient with no labs is LAB_UNAVAILABLE, not 0):")
result.show(20, False)
spark.stop()
