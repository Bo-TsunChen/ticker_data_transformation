"""Command-line pipeline that converts ticker index data to daily cadence.

Given an input CSV in the same layout used throughout the notebook, the script
identifies the lowest-duration series per (ticker, index), expands those
periods into daily rows, and writes the result to a single output location.
"""

from __future__ import annotations

import argparse
import glob
import os
import shutil
from typing import List

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as f
from pyspark.sql.types import DateType

DURATION_ORDER = [
    ("Week", 0),
    ("Month", 1),
    ("Quarter", 2),
    ("Year", 3),
    ("Mid-month", 4),
    ("Custom Quarter", 5),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert index data to daily rows")
    parser.add_argument("--input", required=True, help="Path to the source CSV file")
    parser.add_argument("--output", required=True, help="Destination for the daily CSV data")
    parser.add_argument(
        "--app-name",
        default="DailyTransformation",
        help="Optional Spark application name",
    )
    return parser.parse_args()


def build_spark(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def read_source(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", True).option("inferSchema", True).csv(path)


def lowest_duration_per_index(df: DataFrame, spark: SparkSession) -> DataFrame:
    duration_lookup = spark.createDataFrame(DURATION_ORDER, ["DURATION", "duration_rank"])

    ranked = (
        df.select("TICKER", "INDEXNAME", "DURATION")
        .dropDuplicates()
        .join(duration_lookup, "DURATION", "left")
        .withColumn("duration_rank", f.coalesce(f.col("duration_rank"), f.lit(999)))
    )

    window = Window.partitionBy("TICKER", "INDEXNAME").orderBy("duration_rank", "DURATION")

    return (
        ranked.withColumn("rn", f.row_number().over(window))
        .filter(f.col("rn") == 1)
        .select("TICKER", "INDEXNAME", "DURATION")
    )


def build_daily_rows(raw: DataFrame, spark: SparkSession) -> DataFrame:
    lowest_idx = lowest_duration_per_index(raw, spark)

    filtered = (
        raw.join(lowest_idx, ["TICKER", "INDEXNAME", "DURATION"], "inner")
        .withColumn("PERIODEND_DATE", f.to_date("PERIODEND"))
        .withColumn("VALUE", f.col("VALUE").cast("double"))
        .withColumn("CUMULATIVEVALUE", f.col("CUMULATIVEVALUE").cast("double"))
    )

    window = Window.partitionBy("TICKER", "INDEXNAME").orderBy("PERIODEND_DATE")

    enriched = (
        filtered.withColumn("prev_period_end", f.lag("PERIODEND_DATE").over(window))
        .withColumn("prev_cum", f.lag("CUMULATIVEVALUE").over(window))
        .filter(f.col("prev_period_end").isNotNull())
        .withColumn("span_start", f.col("prev_period_end"))
        .withColumn("span_end", f.date_sub("PERIODEND_DATE", 1))
        .withColumn(
            "days_in_period",
            f.greatest(f.datediff("span_end", "span_start") + f.lit(1), f.lit(1)),
        )
        .withColumn("daily_value", f.col("VALUE") / f.col("days_in_period"))
        .withColumn("prev_cum", f.coalesce(f.col("prev_cum"), f.lit(0.0)))
        .withColumn("date_seq", f.sequence("span_start", "span_end"))
    )

    exploded = (
        enriched.withColumn("DAY_DATE", f.explode("date_seq"))
        .withColumn("day_offset", f.datediff("DAY_DATE", f.col("span_start")))
        .withColumn("VALUE", f.col("daily_value"))
        .withColumn(
            "CUMULATIVEVALUE",
            f.col("prev_cum") + (f.col("day_offset") + f.lit(1)) * f.col("daily_value"),
        )
        .select("TICKER", "INDEXNAME", "DAY_DATE", "VALUE", "CUMULATIVEVALUE")
    )

    return exploded


def align_to_raw_schema(daily: DataFrame, raw_schema) -> DataFrame:
    columns: List = []

    for field in raw_schema:
        name = field.name
        if name == "PERIODEND":
            if isinstance(field.dataType, DateType):
                col = f.col("DAY_DATE").cast(DateType())
            else:
                col = f.date_format("DAY_DATE", "yyyy-MM-dd")
        elif name == "DURATION":
            col = f.lit("Day")
        elif name in {"VALUE", "CUMULATIVEVALUE"}:
            col = f.col(name)
        elif name in daily.columns:
            col = f.col(name)
        else:
            col = f.lit(None).cast(field.dataType)
        columns.append(col.alias(name))

    return daily.select(*columns)


def write_dataframe(df: DataFrame, path: str) -> None:
    if path.lower().endswith(".csv"):
        tmp_path = f"{path}.tmp"
        tmp_parent = os.path.dirname(tmp_path) or "."
        os.makedirs(tmp_parent, exist_ok=True)
        (df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_path))
        part_files = glob.glob(os.path.join(tmp_path, "part-*.csv"))
        if not part_files:
            raise FileNotFoundError(f"No part file generated at {tmp_path}")
        target_dir = os.path.dirname(path) or "."
        os.makedirs(target_dir, exist_ok=True)
        shutil.move(part_files[0], path)
        shutil.rmtree(tmp_path)
    else:
        parent = os.path.dirname(path) or "."
        os.makedirs(parent, exist_ok=True)
        (df.write.mode("overwrite").option("header", True).csv(path))


def main() -> None:
    args = parse_args()
    spark = build_spark(args.app_name)

    raw = read_source(spark, args.input)

    daily_rows = build_daily_rows(raw, spark)
    aligned_daily = align_to_raw_schema(daily_rows, raw.schema)

    write_dataframe(aligned_daily, args.output)

    spark.stop()


if __name__ == "__main__":
    main()
