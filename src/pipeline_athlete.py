from __future__ import annotations

import glob
import os
import shutil
from datetime import datetime
from typing import Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, trim

from config import Paths


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("pipeline-athlete")
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )


def latest_file(pattern: str) -> str:
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No files found for pattern: {pattern}")
    return max(files, key=os.path.getmtime)


def write_silver_athlete(spark: SparkSession, paths: Paths, athlete_csv_path: str) -> None:
    df_raw: DataFrame = spark.read.option("header", True).csv(athlete_csv_path)

    df_silver: DataFrame = (
        df_raw.select(
            col("athlete_id").cast("int").alias("athlete_id"),
            trim(col("full_name")).alias("full_name"),
            trim(col("sex")).alias("sex"),
            col("birth_year").cast("int").alias("birth_year"),
            col("height").cast("double").alias("height"),
            col("weight").cast("double").alias("weight"),
        )
        .dropDuplicates(["athlete_id"])
    )

    df_silver.write.format("delta").mode("overwrite").save(paths.silver_athlete_delta)
    spark.read.format("delta").load(paths.silver_athlete_delta).createOrReplaceTempView("src_athlete")


def ensure_gold_dim_athlete(spark: SparkSession, paths: Paths) -> None:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS delta.`{paths.gold_dim_athlete}` (
      athlete_sk      BIGINT,
      athlete_id      INT,
      full_name       STRING,
      sex             STRING,
      birth_year      INT,
      height          DOUBLE,
      weight          DOUBLE,
      effective_from  STRING,
      effective_to    STRING,
      is_current      BOOLEAN
    )
    USING DELTA
    """)


def scd2_update_dim_athlete(spark: SparkSession, paths: Paths, run_ts: str) -> None:
    # 1) Expire changed current rows
    spark.sql(f"""
    MERGE INTO delta.`{paths.gold_dim_athlete}` t
    USING src_athlete s
    ON t.athlete_id = s.athlete_id AND t.is_current = true
    WHEN MATCHED AND (
         COALESCE(t.height, -1.0) <> COALESCE(s.height, -1.0)
      OR COALESCE(t.weight, -1.0) <> COALESCE(s.weight, -1.0)
    )
    THEN UPDATE SET
      t.effective_to = '{run_ts}',
      t.is_current   = false
    """)

    # 2) Insert new + changed (now missing current)
    # Deterministic SK per version: hash(athlete_id, effective_from)
    spark.sql(f"""
    INSERT INTO delta.`{paths.gold_dim_athlete}`
    SELECT
      xxhash64(CAST(s.athlete_id AS STRING), '{run_ts}') AS athlete_sk,
      s.athlete_id,
      s.full_name,
      s.sex,
      s.birth_year,
      s.height,
      s.weight,
      '{run_ts}' AS effective_from,
      NULL        AS effective_to,
      true        AS is_current
    FROM src_athlete s
    LEFT JOIN delta.`{paths.gold_dim_athlete}` t
      ON s.athlete_id = t.athlete_id AND t.is_current = true
    WHERE t.athlete_id IS NULL
    """)


def move_to_processed(paths: Paths, file_path: str) -> str:
    os.makedirs(paths.processed_dir, exist_ok=True)
    dest = f"{paths.processed_dir}/{os.path.basename(file_path)}"
    shutil.move(file_path, dest)
    return dest


def main(base_path: str) -> None:
    paths = Paths(base_path=base_path)
    os.makedirs(paths.raw_dir, exist_ok=True)

    spark = build_spark()

    latest_athletes = latest_file(f"{paths.raw_dir}/athletes_*.csv")
    print("Latest athlete raw selected:", latest_athletes)

    write_silver_athlete(spark, paths, latest_athletes)
    ensure_gold_dim_athlete(spark, paths)

    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    scd2_update_dim_athlete(spark, paths, run_ts)
    print("SCD2 applied at", run_ts)

    moved = move_to_processed(paths, latest_athletes)
    print("Moved athlete raw to processed:", moved)

    # Checks
    spark.sql(f"""
      SELECT athlete_id, SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_records
      FROM delta.`{paths.gold_dim_athlete}`
      GROUP BY athlete_id
      HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1
    """).show(truncate=False)

    spark.sql(f"""
      SELECT athlete_sk, COUNT(*) AS cnt
      FROM delta.`{paths.gold_dim_athlete}`
      GROUP BY athlete_sk
      HAVING COUNT(*) > 1
    """).show(truncate=False)


if __name__ == "__main__":
    # Drive simulation (replace with s3://bucket/reto_tecnico in AWS)
    main("/content/drive/MyDrive/reto_tecnico")