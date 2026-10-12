export PROJECT_DIR?=$(abspath ..)
include $(PROJECT_DIR)/env.mk

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(PROJECT_DIR)/workloads/Makefile | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: check-format
check-format: ## Check code format
	uv run ruff format --check

.PHONY: format
format: ## Format code
	uv run ruff format

.PHONY: check-lint
check-lint: ## Check lint code
	uv run ruff check

.PHONY: lint
lint: ## Lint code
	uv run ruff check --fix

.PHONY: check
check: check-format check-lint ## Run all the checks

.PHONY: caf-perf-help
caf-perf-help: ## How to run the Airflow 3.2.x API perf driver
	@echo "Example (AWC Console OAuth; export these before caf-perf):"
	@echo "  DOMAIN          - Cloudera / AWC domain (e.g. mycompany.com)"
	@echo "  CWO_URL         - Console base URL; also used as Airflow API host", copy URL form AWC Console UI
	@echo "  CLIENT_ID       - OAuth client id (client_credentials)", details: https://github.infra.cloudera.com/AWC/awc-core/blob/main/docs/programmatic-api-access-guide.md
	@echo "  CLIENT_SECRET   - OAuth client secret"
	@echo "Then:"
	@echo "  uv sync && uv run caf-perf --config perf_configs/example.cloudera-oauth.json --report /tmp/caf_perf_report.json"