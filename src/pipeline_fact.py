from __future__ import annotations

import glob
import os
import shutil
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, trim

from config import Paths


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("pipeline-fact")
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


def write_silver_results(spark: SparkSession, paths: Paths, results_csv_path: str) -> None:
    df_raw: DataFrame = spark.read.option("header", True).csv(results_csv_path)

    df_silver: DataFrame = (
        df_raw.select(
            col("athlete_id").cast("int").alias("athlete_id"),
            col("year").cast("int").alias("year"),
            trim(col("season")).alias("season"),
            trim(col("city")).alias("city"),
            trim(col("sport")).alias("sport"),
            trim(col("event")).alias("event"),
            trim(col("medal")).alias("medal"),
        )
    )

    df_silver.write.format("delta").mode("overwrite").save(paths.silver_results_delta)
    spark.read.format("delta").load(paths.silver_results_delta).createOrReplaceTempView("src_results")


def rebuild_fact(spark: SparkSession, paths: Paths, load_ts: str) -> None:
    # Drop + create fresh (challenge-friendly)
    spark.sql(f"DROP TABLE IF EXISTS delta.`{paths.gold_fact_results}`")

    spark.sql(f"""
    CREATE TABLE delta.`{paths.gold_fact_results}` (
      athlete_sk BIGINT,
      games_sk   BIGINT,
      sport      STRING,
      event      STRING,
      medal      STRING,
      load_ts    STRING
    )
    USING DELTA
    """)

    # Views for joins (only current athlete)
    spark.read.format("delta").load(paths.gold_dim_athlete).createOrReplaceTempView("dim_athlete")
    spark.read.format("delta").load(paths.gold_dim_games).createOrReplaceTempView("dim_games")

    spark.sql(f"""
    INSERT INTO delta.`{paths.gold_fact_results}`
    SELECT
      a.athlete_sk,
      g.games_sk,
      r.sport,
      r.event,
      r.medal,
      '{load_ts}' AS load_ts
    FROM src_results r
    JOIN dim_athlete a
      ON r.athlete_id = a.athlete_id AND a.is_current = true
    JOIN dim_games g
      ON r.year = g.year AND r.season = g.season AND r.city = g.city
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

    latest_results = latest_file(f"{paths.raw_dir}/results_*.csv")
    print("Latest results raw selected:", latest_results)

    write_silver_results(spark, paths, latest_results)

    load_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rebuild_fact(spark, paths, load_ts)
    print("Fact rebuilt at", load_ts)

    moved = move_to_processed(paths, latest_results)
    print("Moved results raw to processed:", moved)

    # Integrity check: fact_rows == join_rows
    spark.sql(f"SELECT COUNT(*) AS fact_rows FROM delta.`{paths.gold_fact_results}`").show()
    spark.sql(f"""
      SELECT COUNT(*) AS join_rows
      FROM delta.`{paths.gold_fact_results}` f
      JOIN delta.`{paths.gold_dim_athlete}` a ON f.athlete_sk = a.athlete_sk
      JOIN delta.`{paths.gold_dim_games}` g ON f.games_sk = g.games_sk
    """).show()


if __name__ == "__main__":
    # Drive simulation (replace with s3://bucket/reto_tecnico in AWS)
    main("/content/drive/MyDrive/reto_tecnico")