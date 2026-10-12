# Basic CDE Spark Job

This is a simple Cloudera Data Engineering (CDE) PySpark job for a UI-based demo. It generates sample retail order data inside the Spark application, runs transformations and aggregations, and prints the output to the CDE job logs.

It does not read from or write to HDFS, Ozone, Hive, Iceberg, or any external storage.

## Project Layout

```text
.
└── src/
    └── basic_cde_spark_job.py
```

## What The Job Does

The script creates sample order records in memory and uses Spark DataFrames to:

- Print the raw generated orders.
- Validate the records.
- Split valid and rejected records.
- Calculate gross revenue, discounts, and net revenue.
- Aggregate revenue by region.
- Aggregate revenue by product category.
- Print a final job summary.

## Deploy In CDE UI

Create a Spark job in the Cloudera CDE UI with these values:

```text
Application file:
src/basic_cde_spark_job.py
```

No application arguments are required.

Optional Spark configs:

```text
spark.sql.adaptive.enabled=true
spark.sql.shuffle.partitions=4
```

## Expected Output

All output appears in the CDE job run logs. You should see sections like:

```text
=== Raw Generated Orders ===
=== Valid Orders With Revenue ===
=== Rejected Orders ===
=== Revenue By Region ===
=== Revenue By Product Category ===
=== Job Summary ===
```

The final summary shows generated, valid, and rejected record counts.
