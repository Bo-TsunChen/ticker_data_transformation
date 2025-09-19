"""Daily transformation pipeline for ticker index data."""

from __future__ import annotations

import argparse

if __package__ in {None, ""}:
    import os
    import sys

    sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from pyspark.sql import functions as f

from src.utils import (
    align_to_raw_schema,
    build_daily_rows,
    build_spark,
    read_source,
    write_dataframe,
)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the daily transformation pipeline."""

    parser = argparse.ArgumentParser(description="Convert index data to daily rows")
    parser.add_argument("--input", required=True, help="Path to the source CSV file")
    parser.add_argument("--output", required=True, help="Destination for the daily CSV data")
    parser.add_argument(
        "--app-name",
        default="DailyTransformation",
        help="Optional Spark application name",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Display a daily cumulative plot once processing completes",
    )
    return parser.parse_args()


def load_raw(args: argparse.Namespace, spark):
    """Load the raw index feed into a Spark DataFrame."""

    return read_source(spark, args.input)


def show_cumulative_plot(df):
    """Render an interactive line chart of cumulative values by ticker/index."""

    try:
        import pandas as pd
        import plotly.express as px
    except ImportError as exc:  # pragma: no cover - convenience for CLI usage
        raise RuntimeError(
            "Plotting requires pandas and plotly; install them or drop the --plot flag."
        ) from exc

    pdf = (
        df.filter(f.col("DURATION") == "Day")
        .withColumn("ticker_index", f.concat_ws("-", f.col("TICKER"), f.col("INDEXNAME")))
        .select("PERIODEND", "CUMULATIVEVALUE", "ticker_index")
        .toPandas()
    )

    if pdf.empty:
        print("No day-level rows available to plot.")
        return

    pdf["PERIODEND"] = pd.to_datetime(pdf["PERIODEND"])

    fig = px.line(
        pdf,
        x="PERIODEND",
        y="CUMULATIVEVALUE",
        color="ticker_index",
        title="Daily cumulative value by index",
        markers=True,
        labels={
            "PERIODEND": "Date",
            "CUMULATIVEVALUE": "Cumulative Value",
            "ticker_index": "Series",
        },
    )
    fig.show()


def main() -> None:
    """Entry point for the daily pipeline CLI."""

    args = parse_args()
    spark = build_spark(args.app_name)
    spark.sparkContext.setLogLevel("ERROR")

    print("[daily] reading source CSV...")
    raw = load_raw(args, spark)

    # Expand each ticker/index pair using its lowest cadence so downstream work sees day-level observations.
    print("[daily] expanding to day-level rows...")
    daily_rows = build_daily_rows(raw, spark)
    # Project the daily rows back into the original schema so we can union/write alongside the source feed.
    aligned_daily = align_to_raw_schema(daily_rows, raw.schema)

    print(f"[daily] writing output to {args.output}...")
    write_dataframe(aligned_daily, args.output)

    if args.plot:
        result_with_daily = raw.unionByName(aligned_daily, allowMissingColumns=True)
        print("[daily] rendering cumulative plot...")
        show_cumulative_plot(result_with_daily)

    print("[daily] completed.")
    spark.stop()


if __name__ == "__main__":
    main()
