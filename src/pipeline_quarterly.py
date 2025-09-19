"""Quarterly forecast pipeline driven by the day-level feed."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as f

if __package__ in {None, ""}:
    import os
    import sys

    sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.utils import build_spark, read_source, write_dataframe

DEFAULT_WEIGHT = 0.7  # weight the simple run-rate more heavily than seasonality by default


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the quarterly forecast generator."""

    parser = argparse.ArgumentParser(
        description="Generate mid-quarter forecasts from the daily feed"
    )
    parser.add_argument(
        "--input", required=True, help="Path to the day-level CSV (e.g. output/dailyindex.csv)"
    )
    parser.add_argument("--output", required=True, help="Destination for the midpoint forecast CSV")
    parser.add_argument(
        "--full-output", help="Optional destination for the full daily forecast timeline"
    )
    parser.add_argument(
        "--weight",
        type=float,
        default=DEFAULT_WEIGHT,
        help="Blend weight for the simple run-rate forecast (default: %(default)s)",
    )
    parser.add_argument(
        "--app-name",
        default="QuarterlyForecast",
        help="Optional Spark application name",
    )
    return parser.parse_args()


def load_daily_frame(spark, path: str) -> DataFrame:
    """Load the day-level feed produced by the daily pipeline."""

    daily = read_source(spark, path)
    return (
        daily.filter(f.col("DURATION") == "Day")
        .withColumn("DAY_DATE", f.to_date("PERIODEND"))
        .withColumn("VALUE", f.col("VALUE").cast("double"))
        .withColumn("CUMULATIVEVALUE", f.col("CUMULATIVEVALUE").cast("double"))
    )


def compute_forecasts(daily: DataFrame, weight: float) -> DataFrame:
    """Build quarter-to-date metrics and hybrid forecasts for every day."""

    quarterized = (
        daily.withColumn("quarter_start", f.date_trunc("quarter", f.col("DAY_DATE")))
        .withColumn("quarter_end", f.date_sub(f.add_months("quarter_start", 3), 1))
        .withColumn("days_in_quarter", f.datediff("quarter_end", "quarter_start") + f.lit(1))
    )

    order_w = Window.partitionBy("TICKER", "INDEXNAME", "quarter_start").orderBy("DAY_DATE")
    accum_w = order_w.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    quarterized = quarterized.withColumn("day_of_quarter", f.row_number().over(order_w)).withColumn(
        "quarter_to_date_value", f.sum("VALUE").over(accum_w)
    )

    quarter_totals = quarterized.groupBy(
        "TICKER", "INDEXNAME", "quarter_start", "quarter_end", "days_in_quarter"
    ).agg(f.sum("VALUE").alias("quarter_total"))

    quarterized = quarterized.join(
        quarter_totals,
        ["TICKER", "INDEXNAME", "quarter_start", "quarter_end", "days_in_quarter"],
        "left",
    )

    quarterized = quarterized.withColumn(
        "quarter_proportion",
        f.when(
            f.col("quarter_total") != 0,
            f.col("quarter_to_date_value") / f.col("quarter_total"),
        ),
    )

    seasonal_curve = (
        quarterized.select(
            "TICKER", "INDEXNAME", "quarter_start", "day_of_quarter", "quarter_proportion"
        )
        .groupBy("TICKER", "INDEXNAME", "day_of_quarter")
        .agg(
            f.avg("quarter_proportion").alias("avg_cum_prop"),
            f.countDistinct("quarter_start").alias("num_quarters"),
        )
    )

    # Treat "mid-quarter" as the point where two full calendar months have elapsed.
    two_months_in = f.add_months(f.col("quarter_start"), 2)

    forecast_daily = (
        quarterized.join(seasonal_curve, ["TICKER", "INDEXNAME", "day_of_quarter"], "left")
        .withColumn(
            "simple_forecast",
            f.col("quarter_to_date_value") / f.col("day_of_quarter") * f.col("days_in_quarter"),
        )
        .withColumn(
            "seasonal_forecast",
            f.when(
                f.col("avg_cum_prop").isNotNull() & (f.col("avg_cum_prop") > 0),
                f.col("quarter_to_date_value") / f.col("avg_cum_prop"),
            ),
        )
        .withColumn(
            "hybrid_forecast",
            f.when(
                f.col("seasonal_forecast").isNotNull(),
                weight * f.col("simple_forecast") + (1 - weight) * f.col("seasonal_forecast"),
            ).otherwise(f.col("simple_forecast")),
        )
        .withColumn(
            "forecast_cumulativevalue",
            f.col("CUMULATIVEVALUE") - f.col("quarter_to_date_value") + f.col("hybrid_forecast"),
        )
        .withColumn("AS_OF_DATE", f.col("DAY_DATE"))
        .withColumn("is_mid_quarter", f.col("DAY_DATE") >= two_months_in)
    )

    # Track relative deviation from the realised quarter total so we can benchmark the estimator.
    forecast_daily = forecast_daily.withColumn(
        "forecast_error_pct",
        f.when(
            f.col("quarter_total") != 0,
            (f.col("hybrid_forecast") - f.col("quarter_total")) / f.col("quarter_total"),
        ),
    )

    return forecast_daily


def pick_midpoints(forecast_daily: DataFrame) -> DataFrame:
    """Select the first day that satisfies the mid-quarter rule for each series."""

    midpoint_rank = Window.partitionBy("TICKER", "INDEXNAME", "quarter_start").orderBy("AS_OF_DATE")

    return (
        forecast_daily.filter(f.col("is_mid_quarter"))
        .withColumn("mid_rank", f.row_number().over(midpoint_rank))
        .filter(f.col("mid_rank") == 1)
        .drop("mid_rank")
    )


def ensure_column(df: DataFrame, name: str, data_type: str) -> DataFrame:
    """Ensure optional metadata columns exist so the output schema stays stable."""

    # Ensure optional metadata columns always exist to keep the export schema stable.
    if name in df.columns:
        return df
    return df.withColumn(name, f.lit(None).cast(data_type))


def format_midpoints(midpoints: DataFrame) -> DataFrame:
    """Project midpoint rows into the deliverable schema with forecast/actual columns."""

    # Reformat midpoint outputs into the deliverable schema combining forecasts and actuals.
    enriched = (
        midpoints.withColumn("PERIODEND", f.col("quarter_end").cast("string"))
        .withColumn("DURATION", f.lit("Est. Quarter"))
        .withColumn("FORECAST_VALUE", f.col("hybrid_forecast"))
        .withColumn("ORIGINAL_VALUE", f.col("quarter_total"))
        .withColumn("FORECAST_CUMULATIVEVALUE", f.col("forecast_cumulativevalue"))
        .withColumn("ORIGINAL_CUMULATIVEVALUE", f.col("CUMULATIVEVALUE"))
        .withColumn("COMMENT", f.lit("QTD Forecast"))
        .withColumn("FORECAST_ERROR_PERCENTAGE", f.round(f.col("forecast_error_pct"), 4))
    )

    enriched = ensure_column(enriched, "RELEASEDDATE", "timestamp")
    enriched = ensure_column(enriched, "YOYCHANGE", "double")
    enriched = ensure_column(enriched, "INDEXHEALTH", "string")

    return enriched.select(
        "TICKER",
        "DURATION",
        "PERIODEND",
        "INDEXNAME",
        "FORECAST_VALUE",
        "ORIGINAL_VALUE",
        "FORECAST_CUMULATIVEVALUE",
        "ORIGINAL_CUMULATIVEVALUE",
        "COMMENT",
        "FORECAST_ERROR_PERCENTAGE",
        "RELEASEDDATE",
        "YOYCHANGE",
        "INDEXHEALTH",
    )


def main() -> None:
    """Entry point for the quarterly forecast CLI."""

    args = parse_args()
    spark = build_spark(args.app_name)
    spark.sparkContext.setLogLevel("ERROR")

    daily = load_daily_frame(spark, args.input)

    forecast_daily = compute_forecasts(daily, args.weight)
    midpoints = pick_midpoints(forecast_daily)
    formatted_midpoints = format_midpoints(midpoints)

    write_dataframe(formatted_midpoints, args.output)

    if args.full_output:
        write_dataframe(forecast_daily, args.full_output)

    spark.stop()


if __name__ == "__main__":
    main()
