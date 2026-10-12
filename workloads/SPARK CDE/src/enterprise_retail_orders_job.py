#!/usr/bin/env python3
"""
Enterprise-style CDE PySpark example.

This job demonstrates a common pattern for on-prem Cloudera Data Engineering:
read raw operational data, enforce data quality rules, publish curated datasets,
and emit an audit record for observability.
"""

import argparse
import json
import logging
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


LOGGER = logging.getLogger("enterprise_retail_orders_job")


ORDER_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.StringType(), True),
        T.StructField("customer_id", T.StringType(), True),
        T.StructField("order_ts", T.StringType(), True),
        T.StructField("region", T.StringType(), True),
        T.StructField("product_category", T.StringType(), True),
        T.StructField("quantity", T.IntegerType(), True),
        T.StructField("unit_price", T.DoubleType(), True),
        T.StructField("discount_pct", T.DoubleType(), True),
        T.StructField("order_status", T.StringType(), True),
    ]
)


def parse_args():
    parser = argparse.ArgumentParser(description="CDE enterprise retail orders Spark job")
    parser.add_argument(
        "--config",
        default="/app/mount/conf/retail_orders_config.json",
        help="Path to the JSON job configuration. In CDE this is usually under /app/mount.",
    )
    parser.add_argument(
        "--run-date",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Business date for this run, formatted as YYYY-MM-DD.",
    )
    parser.add_argument(
        "--generate-sample-data",
        action="store_true",
        help="Generate demo raw data before processing.",
    )
    return parser.parse_args()


def load_config(path):
    with open(path, "r", encoding="utf-8") as config_file:
        return json.load(config_file)


def path_exists(spark, path):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    file_system = fs_path.getFileSystem(hadoop_conf)
    return file_system.exists(fs_path)


def generate_sample_data(spark, raw_input_path):
    rows = [
        Row("ORD-1001", "CUST-001", "2026-06-17 08:15:00", "EMEA", "Hardware", 2, 899.99, 0.05, "COMPLETE"),
        Row("ORD-1002", "CUST-002", "2026-06-17 09:20:00", "AMER", "Software", 5, 129.99, 0.10, "COMPLETE"),
        Row("ORD-1003", "CUST-003", "2026-06-17 10:35:00", "APAC", "Services", 1, 499.00, 0.00, "PENDING"),
        Row("ORD-1004", "CUST-004", "2026-06-17 11:05:00", "EMEA", "Hardware", 3, 249.50, 0.15, "COMPLETE"),
        Row("ORD-1005", "CUST-005", "2026-06-17 12:10:00", "AMER", "Software", 0, 79.99, 0.00, "COMPLETE"),
        Row("ORD-1006", None, "2026-06-17 13:45:00", "APAC", "Hardware", 1, 349.99, 0.00, "COMPLETE"),
        Row("ORD-1007", "CUST-007", "2026-06-17 14:25:00", "EMEA", "Services", 4, -10.00, 0.00, "COMPLETE"),
        Row("ORD-1002", "CUST-002", "2026-06-17 09:20:00", "AMER", "Software", 5, 129.99, 0.10, "COMPLETE"),
    ]

    spark.createDataFrame(rows, schema=ORDER_SCHEMA).write.mode("overwrite").option("header", True).csv(raw_input_path)
    LOGGER.info("Generated sample raw orders at %s", raw_input_path)


def read_orders(spark, raw_input_path):
    return spark.read.schema(ORDER_SCHEMA).option("header", True).csv(raw_input_path)


def apply_quality_rules(orders, accepted_statuses):
    duplicate_window = Window.partitionBy("order_id")

    prepared = (
        orders.withColumn("order_ts_parsed", F.to_timestamp("order_ts"))
        .withColumn("duplicate_count", F.count("*").over(duplicate_window))
        .withColumn(
            "rejection_reason",
            F.concat_ws(
                "; ",
                F.when(F.col("order_id").isNull(), F.lit("missing order_id")),
                F.when(F.col("customer_id").isNull(), F.lit("missing customer_id")),
                F.when(F.col("order_ts_parsed").isNull(), F.lit("invalid order_ts")),
                F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("quantity must be positive")),
                F.when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), F.lit("unit_price must be non-negative")),
                F.when(~F.col("order_status").isin(accepted_statuses), F.lit("unsupported order_status")),
                F.when(F.col("duplicate_count") > 1, F.lit("duplicate order_id")),
            ),
        )
    )

    valid_orders = prepared.filter(F.col("rejection_reason") == "").drop("duplicate_count", "rejection_reason")
    rejected_orders = prepared.filter(F.col("rejection_reason") != "")

    return valid_orders, rejected_orders


def enrich_orders(valid_orders, run_id, run_date):
    return (
        valid_orders.withColumn("order_date", F.to_date("order_ts_parsed"))
        .withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("discount_amount", F.round(F.col("gross_amount") * F.col("discount_pct"), 2))
        .withColumn("net_amount", F.round(F.col("gross_amount") - F.col("discount_amount"), 2))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("run_date", F.lit(run_date))
    )


def build_daily_kpis(curated_orders):
    return (
        curated_orders.groupBy("order_date", "region", "product_category")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.countDistinct("customer_id").alias("customers"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("gross_amount"), 2).alias("gross_revenue"),
            F.round(F.sum("discount_amount"), 2).alias("discounts"),
            F.round(F.sum("net_amount"), 2).alias("net_revenue"),
        )
        .orderBy("order_date", "region", "product_category")
    )


def write_outputs(curated_orders, rejected_orders, daily_kpis, config):
    paths = config["paths"]

    curated_orders.write.mode(config["write_mode"]).partitionBy("order_date", "region").parquet(paths["silver_orders"])
    rejected_orders.write.mode(config["write_mode"]).parquet(paths["rejected_orders"])
    daily_kpis.write.mode(config["write_mode"]).partitionBy("order_date").parquet(paths["gold_daily_kpis"])


def write_audit(spark, config, run_id, run_date, input_count, valid_count, rejected_count):
    audit_row = [
        Row(
            run_id=run_id,
            run_date=run_date,
            application="enterprise_retail_orders_job",
            input_count=input_count,
            valid_count=valid_count,
            rejected_count=rejected_count,
            completed_at_utc=datetime.now(timezone.utc).isoformat(),
        )
    ]

    spark.createDataFrame(audit_row).write.mode("append").parquet(config["paths"]["audit"])


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    args = parse_args()
    config = load_config(args.config)
    run_id = str(uuid.uuid4())

    spark = (
        SparkSession.builder.appName(config.get("spark_app_name", "enterprise-retail-orders-cde"))
        .enableHiveSupport()
        .getOrCreate()
    )

    try:
        spark.conf.set("spark.sql.session.timeZone", config.get("timezone", "UTC"))
        raw_input_path = config["paths"]["raw_orders"]

        if args.generate_sample_data or not path_exists(spark, raw_input_path):
            generate_sample_data(spark, raw_input_path)

        orders = read_orders(spark, raw_input_path)
        input_count = orders.count()
        LOGGER.info("Read %s raw orders from %s", input_count, raw_input_path)

        valid_orders, rejected_orders = apply_quality_rules(orders, config["data_quality"]["accepted_statuses"])
        curated_orders = enrich_orders(valid_orders, run_id, args.run_date)
        daily_kpis = build_daily_kpis(curated_orders)

        valid_count = curated_orders.count()
        rejected_count = rejected_orders.count()

        if config["data_quality"].get("fail_on_rejected_records", False) and rejected_count > 0:
            raise RuntimeError(f"Rejected {rejected_count} records and fail_on_rejected_records is enabled")

        write_outputs(curated_orders, rejected_orders, daily_kpis, config)
        write_audit(spark, config, run_id, args.run_date, input_count, valid_count, rejected_count)

        LOGGER.info(
            "Completed run_id=%s input=%s valid=%s rejected=%s",
            run_id,
            input_count,
            valid_count,
            rejected_count,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        LOGGER.exception("CDE Spark job failed")
        sys.exit(1)
