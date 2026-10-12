#!/usr/bin/env python3
"""
Basic CDE PySpark job.

This script is intentionally self-contained for a simple Cloudera CDE UI demo:
it generates sample data in memory, transforms it with Spark, and prints the
results to the driver logs. It does not read from or write to external storage.
"""

import logging

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


LOGGER = logging.getLogger("basic_cde_spark_job")


ORDER_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.StringType(), False),
        T.StructField("customer_id", T.StringType(), True),
        T.StructField("region", T.StringType(), False),
        T.StructField("product_category", T.StringType(), False),
        T.StructField("quantity", T.IntegerType(), True),
        T.StructField("unit_price", T.DoubleType(), True),
        T.StructField("discount_pct", T.DoubleType(), True),
        T.StructField("order_status", T.StringType(), False),
    ]
)


def build_sample_orders(spark):
    """Create a small in-memory dataset with valid and invalid records."""
    rows = [
        Row("ORD-1001", "CUST-001", "EMEA", "Hardware", 2, 899.99, 0.05, "COMPLETE"),
        Row("ORD-1002", "CUST-002", "AMER", "Software", 5, 129.99, 0.10, "COMPLETE"),
        Row("ORD-1003", "CUST-003", "APAC", "Services", 1, 499.00, 0.00, "PENDING"),
        Row("ORD-1004", "CUST-004", "EMEA", "Hardware", 3, 249.50, 0.15, "COMPLETE"),
        Row("ORD-1005", "CUST-005", "AMER", "Software", 0, 79.99, 0.00, "COMPLETE"),
        Row("ORD-1006", None, "APAC", "Hardware", 1, 349.99, 0.00, "COMPLETE"),
        Row("ORD-1007", "CUST-007", "EMEA", "Services", 4, -10.00, 0.00, "COMPLETE"),
        Row("ORD-1008", "CUST-008", "AMER", "Hardware", 2, 199.99, 0.20, "CANCELLED"),
    ]

    return spark.createDataFrame(rows, schema=ORDER_SCHEMA)


def apply_quality_rules(orders):
    """Add rejection reasons for common data quality failures."""
    return orders.withColumn(
        "rejection_reason",
        F.concat_ws(
            "; ",
            F.when(F.col("customer_id").isNull(), F.lit("missing customer_id")),
            F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("quantity must be positive")),
            F.when(F.col("unit_price").isNull() | (F.col("unit_price") <= 0), F.lit("unit_price must be positive")),
            F.when(~F.col("order_status").isin("COMPLETE", "PENDING", "CANCELLED"), F.lit("unsupported status")),
        ),
    )


def enrich_valid_orders(valid_orders):
    return (
        valid_orders.withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("discount_amount", F.round(F.col("gross_amount") * F.col("discount_pct"), 2))
        .withColumn("net_amount", F.round(F.col("gross_amount") - F.col("discount_amount"), 2))
    )


def print_section(title, dataframe, rows=20):
    print(f"\n=== {title} ===")
    dataframe.show(rows, truncate=False)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    spark = SparkSession.builder.appName("basic-cde-spark-job").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        LOGGER.info("Generating sample order data in memory")
        raw_orders = build_sample_orders(spark)

        quality_checked = apply_quality_rules(raw_orders)
        valid_orders = quality_checked.filter(F.col("rejection_reason") == "")
        rejected_orders = quality_checked.filter(F.col("rejection_reason") != "")
        enriched_orders = enrich_valid_orders(valid_orders)

        revenue_by_region = (
            enriched_orders.groupBy("region")
            .agg(
                F.count("*").alias("total_orders"),
                F.round(F.sum("gross_amount"), 2).alias("gross_revenue"),
                F.round(F.sum("discount_amount"), 2).alias("total_discount"),
                F.round(F.sum("net_amount"), 2).alias("net_revenue"),
            )
            .orderBy("region")
        )

        revenue_by_category = (
            enriched_orders.groupBy("product_category")
            .agg(
                F.count("*").alias("total_orders"),
                F.round(F.sum("net_amount"), 2).alias("net_revenue"),
            )
            .orderBy(F.desc("net_revenue"))
        )

        print_section("Raw Generated Orders", raw_orders)
        print_section(
            "Valid Orders With Revenue",
            enriched_orders.select(
                "order_id",
                "customer_id",
                "region",
                "product_category",
                "quantity",
                "unit_price",
                "gross_amount",
                "discount_amount",
                "net_amount",
                "order_status",
            ),
        )
        print_section(
            "Rejected Orders",
            rejected_orders.select("order_id", "customer_id", "quantity", "unit_price", "rejection_reason"),
        )
        print_section("Revenue By Region", revenue_by_region)
        print_section("Revenue By Product Category", revenue_by_category)

        generated_count = raw_orders.count()
        valid_count = valid_orders.count()
        rejected_count = rejected_orders.count()

        print("\n=== Job Summary ===")
        print(f"Generated records: {generated_count}")
        print(f"Valid records: {valid_count}")
        print(f"Rejected records: {rejected_count}")
        print("Output location: CDE job logs only. No data was written to storage.")

        LOGGER.info("Basic CDE Spark job completed successfully")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
