#!/usr/bin/env python3
"""Summarize customer counts by Australian state and customer segment."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def summarize_customers(customers: DataFrame) -> DataFrame:
    """Build the state/segment summary DataFrame."""
    return (
        customers.groupBy("state", "segment")
        .agg(
            F.count("customer_id").alias("customer_count"),
            F.min("signup_date").alias("first_signup_date"),
            F.max("signup_date").alias("last_signup_date"),
        )
        .orderBy("state", "segment")
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize customer CSV data from HDFS")
    parser.add_argument("--input", required=True, help="input CSV file or directory")
    parser.add_argument("--output", required=True, help="output directory")
    parser.add_argument(
        "--format",
        choices=("csv", "parquet"),
        default="csv",
        help="output format",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("customer-state-summary").getOrCreate()

    try:
        customers = spark.read.csv(args.input, header=True, inferSchema=True)
        summary = summarize_customers(customers)

        writer = summary.coalesce(1).write.mode("overwrite")
        if args.format == "csv":
            writer.option("header", True).csv(args.output)
        else:
            writer.parquet(args.output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
