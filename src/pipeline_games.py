from __future__ import annotations

import glob
import os
import shutil
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, trim

from config import Paths


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("pipeline-games")
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


def write_silver_games(spark: SparkSession, paths: Paths, games_csv_path: str) -> None:
    df_raw: DataFrame = spark.read.option("header", True).csv(games_csv_path)

    df_silver: DataFrame = (
        df_raw.select(
            col("year").cast("int").alias("year"),
            trim(col("season")).alias("season"),
            trim(col("city")).alias("city"),
        )
        .dropDuplicates(["year", "season", "city"])
    )

    df_silver.write.format("delta").mode("overwrite").save(paths.silver_games_delta)
    spark.read.format("delta").load(paths.silver_games_delta).createOrReplaceTempView("src_games")


def ensure_gold_dim_games(spark: SparkSession, paths: Paths) -> None:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS delta.`{paths.gold_dim_games}` (
      games_sk BIGINT,
      year     INT,
      season   STRING,
      city     STRING
    )
    USING DELTA
    """)


def upsert_dim_games(spark: SparkSession, paths: Paths) -> None:
    # Deterministic SK: avoids duplicates across runs
    spark.sql(f"""
    INSERT INTO delta.`{paths.gold_dim_games}`
    SELECT
      xxhash64(CAST(s.year AS STRING), s.season, s.city) AS games_sk,
      s.year,
      s.season,
      s.city
    FROM (
      SELECT DISTINCT year, season, city
      FROM src_games
      WHERE year IS NOT NULL AND season IS NOT NULL AND city IS NOT NULL
    ) s
    LEFT JOIN delta.`{paths.gold_dim_games}` d
      ON d.year = s.year AND d.season = s.season AND d.city = s.city
    WHERE d.year IS NULL
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

    latest_games = latest_file(f"{paths.raw_dir}/games_*.csv")
    print("Latest games raw selected:", latest_games)

    write_silver_games(spark, paths, latest_games)
    ensure_gold_dim_games(spark, paths)
    upsert_dim_games(spark, paths)

    moved = move_to_processed(paths, latest_games)
    print("Moved games raw to processed:", moved)

    # Basic checks
    spark.sql(f"SELECT COUNT(*) AS rows FROM delta.`{paths.gold_dim_games}`").show()
    spark.sql(f"""
      SELECT games_sk, COUNT(*) AS cnt
      FROM delta.`{paths.gold_dim_games}`
      GROUP BY games_sk
      HAVING COUNT(*) > 1
    """).show(truncate=False)


if __name__ == "__main__":
    # Drive simulation (replace with s3://bucket/reto_tecnico in AWS)
    main("/content/drive/MyDrive/reto_tecnico")