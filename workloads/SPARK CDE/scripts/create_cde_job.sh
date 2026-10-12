#!/usr/bin/env bash
set -euo pipefail

RESOURCE_NAME="${RESOURCE_NAME:-retail-orders-demo-files}"
JOB_NAME="${JOB_NAME:-enterprise-retail-orders-demo}"
RUN_DATE="${RUN_DATE:-$(date -u +%F)}"

APP_FILE="src/enterprise_retail_orders_job.py"
CONFIG_FILE="conf/retail_orders_config.json"

echo "Creating CDE files resource: ${RESOURCE_NAME}"
cde resource create --name "${RESOURCE_NAME}" --type files || true

echo "Uploading application and config"
cde resource upload --name "${RESOURCE_NAME}" --local-path "${APP_FILE}" --resource-path "${APP_FILE}"
cde resource upload --name "${RESOURCE_NAME}" --local-path "${CONFIG_FILE}" --resource-path "${CONFIG_FILE}"

echo "Creating CDE Spark job: ${JOB_NAME}"
cde job create \
  --name "${JOB_NAME}" \
  --type spark \
  --mount-1-resource "${RESOURCE_NAME}" \
  --application-file "${APP_FILE}" \
  --spark-name "enterprise-retail-orders-cde" \
  --driver-memory "2g" \
  --executor-memory "4g" \
  --executor-cores 2 \
  --min-executors 1 \
  --max-executors 4 \
  --conf "spark.sql.shuffle.partitions=24" \
  --conf "spark.sql.adaptive.enabled=true" \
  --default-variable "run_date=${RUN_DATE}" \
  --arg "--config" \
  --arg "/app/mount/${CONFIG_FILE}" \
  --arg "--run-date" \
  --arg "{{{run_date}}}"

echo "Run the job with:"
echo "cde job run --name ${JOB_NAME} --variable run_date=${RUN_DATE}"
