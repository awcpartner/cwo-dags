"""Command-line entry for ``caf-perf`` (Airflow 3.2.x REST /api/v2)."""

from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import asdict
from pathlib import Path

from caf_perf.config import load_scenario
from caf_perf.git_publish import prepare_git_publish
from caf_perf.runner import run_scenario


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Drive Airflow 3.2.x via stable REST (/api/v2): DAG sync visibility, "
            "import errors, triggers, and run duration / concurrency sampling. "
            "Auth: empty object for no auth, or awc_console_oauth "
            "(DOMAIN, CWO_URL, CLIENT_ID, CLIENT_SECRET) for AWC Console OAuth."
        )
    )
    parser.add_argument(
        "--config",
        required=True,
        help="JSON scenario file (see perf_configs/).",
    )
    parser.add_argument(
        "--report",
        default="caf_perf_report.json",
        help="Write JSON report to this path (default: ./caf_perf_report.json).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Logging level for runtime progress output (default: INFO).",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    if args.log_level != "DEBUG":
        # Keep caf-perf progress logs at INFO while silencing per-request httpx/httpcore logs.
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
    cfg = load_scenario(args.config)
    report_path = Path(args.report)
    gp_state = prepare_git_publish(cfg)
    report_extras: dict | None = None
    if gp_state is not None:
        report_extras = {"sync": {"git_publish": asdict(gp_state)}}
    return asyncio.run(run_scenario(cfg, report_path, report_extras=report_extras))
