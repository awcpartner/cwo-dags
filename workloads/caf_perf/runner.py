"""Orchestrate sync, import, trigger, and wait phases; emit a JSON report."""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from caf_perf.client import AirflowRestClient, DagRunRef
from caf_perf.config import ScenarioConfig

LOGGER = logging.getLogger(__name__)


@asynccontextmanager
async def _optional_semaphore(sem: asyncio.Semaphore | None):
    if sem is None:
        yield
    else:
        async with sem:
            yield


def _utc_logical_date_unique(seq: int) -> str:
    """Distinct instants for parallel triggers; UUID mixes sub-second uniqueness."""
    now = datetime.now(timezone.utc)
    mix = (uuid.uuid4().int ^ seq) % 1_000_000
    return now.replace(microsecond=mix).isoformat().replace("+00:00", "Z")


def _expected_test_dag_ids(prefix: str, count: int) -> set[str]:
    return {f"{prefix}{i}" for i in range(count)}


def _parse_iso_datetime(raw: str) -> datetime | None:
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_run_id_timestamp(run_id: str) -> datetime | None:
    parts = run_id.split("__", 1)
    if len(parts) != 2:
        return None
    return _parse_iso_datetime(parts[1])


def _dedupe_refs(refs: list[DagRunRef]) -> list[DagRunRef]:
    out: list[DagRunRef] = []
    seen: set[str] = set()
    for ref in refs:
        if ref.key in seen:
            continue
        seen.add(ref.key)
        out.append(ref)
    return out


def _sum_counts(counts: dict[str, int]) -> int:
    return sum(v for v in counts.values() if isinstance(v, int))


def _summary_sec(samples: list[float]) -> dict[str, Any]:
    if not samples:
        return {}
    return {
        "count": len(samples),
        "min": round(min(samples), 6),
        "avg": round(sum(samples) / len(samples), 6),
        "max": round(max(samples), 6),
    }


async def _discover_triggered_children_refs(
    *,
    client: AirflowRestClient,
    child_expected_counts: dict[str, int],
    not_before: datetime | None,
    page_limit: int,
) -> list[DagRunRef]:
    out: list[DagRunRef] = []
    for dag_id, expected_count in sorted(child_expected_counts.items()):
        if expected_count < 1:
            continue
        limit = max(page_limit, expected_count * 5)
        data = await client.get_json(
            f"/dags/{dag_id}/dagRuns",
            params={"limit": limit, "offset": 0},
        )
        if not isinstance(data, dict):
            continue
        dag_runs = data.get("dag_runs")
        if not isinstance(dag_runs, list):
            continue
        candidates: list[tuple[datetime, str]] = []
        for dr in dag_runs:
            if not isinstance(dr, dict):
                continue
            rid = dr.get("dag_run_id")
            if not isinstance(rid, str) or not rid:
                continue
            dt_raw = dr.get("logical_date") or dr.get("start_date")
            if not isinstance(dt_raw, str):
                continue
            dt = _parse_iso_datetime(dt_raw)
            if dt is None:
                continue
            if not_before is not None and dt < not_before:
                continue
            candidates.append((dt, rid))
        candidates.sort(key=lambda x: x[0])
        for _, rid in candidates[:expected_count]:
            out.append(DagRunRef(dag_id=dag_id, dag_run_id=rid))
    return out


async def _collect_task_instance_totals(
    client: AirflowRestClient, refs: list[DagRunRef]
) -> tuple[dict[str, dict[str, Any]], dict[str, int]]:
    per_run: dict[str, dict[str, Any]] = {}
    overall: dict[str, int] = {}
    for ref in refs:
        try:
            counts = await client.count_task_instances_by_state(ref)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                continue
            raise
        by_state = dict(sorted(counts.items(), key=lambda x: x[0]))
        per_run[ref.key] = {
            "total": _sum_counts(counts),
            "by_state": by_state,
        }
        for state, n in counts.items():
            overall[state] = overall.get(state, 0) + n
    return per_run, dict(sorted(overall.items(), key=lambda x: x[0]))


async def _collect_run_duration_summary_sec(
    client: AirflowRestClient, refs: list[DagRunRef]
) -> dict[str, Any]:
    samples: list[float] = []
    for ref in refs:
        try:
            dr = await client.get_dag_run(ref)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                continue
            raise
        start_raw = dr.get("start_date")
        end_raw = dr.get("end_date")
        if isinstance(start_raw, str) and isinstance(end_raw, str):
            start = _parse_iso_datetime(start_raw)
            end = _parse_iso_datetime(end_raw)
            if start is not None and end is not None and end >= start:
                samples.append((end - start).total_seconds())
                continue
        duration = dr.get("duration")
        if isinstance(duration, int | float) and not isinstance(duration, bool):
            samples.append(float(duration))
    return _summary_sec(samples)


def _import_error_relevant(err: dict[str, Any], ignore_substrings: list[str]) -> bool:
    blob = " ".join(str(v) for v in err.values() if v is not None).lower()
    low = [s.lower() for s in ignore_substrings]
    return not any(s in blob for s in low)


@dataclass
class PhaseResult:
    name: str
    ok: bool
    duration_sec: float
    detail: dict[str, Any] = field(default_factory=dict)


def _poll_delay_sec(interval: float, jitter_fraction: float) -> float:
    if jitter_fraction <= 0.0:
        return interval
    lo = max(0.0, interval * (1.0 - jitter_fraction))
    hi = interval * (1.0 + jitter_fraction)
    return random.uniform(lo, hi)


async def _wait_until(
    timeout_sec: float,
    poll_interval_sec: float,
    poll_jitter_fraction: float,
    predicate,
) -> tuple[bool, float, Any]:
    start = time.monotonic()
    deadline = start + timeout_sec
    last: Any = None
    while time.monotonic() < deadline:
        last = await predicate()
        if last is True or (isinstance(last, tuple) and last[0] is True):
            return True, time.monotonic() - start, last
        await asyncio.sleep(_poll_delay_sec(poll_interval_sec, poll_jitter_fraction))
    return False, time.monotonic() - start, last


async def _dag_run_states_snapshot(
    client: AirflowRestClient,
    refs: list[DagRunRef],
    *,
    batch: bool,
    page_limit: int,
) -> dict[str, str]:
    """Map ``dag_id/dag_run_id`` -> state string."""
    if not batch:
        states: dict[str, str] = {}
        for ref in refs:
            try:
                dr = await client.get_dag_run(ref)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    continue
                raise
            st = dr.get("state")
            if isinstance(st, str):
                states[ref.key] = st
        return states

    out: dict[str, str] = {}
    by_dag: dict[str, list[DagRunRef]] = {}
    for r in refs:
        by_dag.setdefault(r.dag_id, []).append(r)
    for dag_id, group in by_dag.items():
        wanted = {x.dag_run_id for x in group}
        m = await client.map_dag_run_states(dag_id, wanted, page_limit)
        for rid, st in m.items():
            out[f"{dag_id}/{rid}"] = st
    return out


async def run_scenario(
    cfg: ScenarioConfig,
    report_path: Path | None,
    *,
    report_extras: dict[str, Any] | None = None,
) -> int:
    phases: list[PhaseResult] = []
    target: dict[str, Any] = {
        "base_url": cfg.airflow.base_url,
        "api_prefix": cfg.airflow.api_prefix,
        "note": "Airflow 3.2.x stable REST /api/v2",
        "http_retries": {
            "max_retries": cfg.airflow.http_max_retries,
            "retry_base_sec": cfg.airflow.http_retry_base_sec,
            "retry_max_sec": cfg.airflow.http_retry_max_sec,
        },
        "http_pool": {
            "max_connections": cfg.airflow.http_max_connections,
            "max_keepalive_connections": cfg.airflow.http_max_keepalive_connections,
        },
    }
    if cfg.airflow.awc_console_oauth is not None:
        target["auth"] = "awc_console_oauth"

    report: dict[str, Any] = {
        "airflow_target": target,
        "phases": [],
        "dag_runs": [],
        "peak_task_running": 0,
        "peak_dag_runs_running": 0,
    }
    if report_extras:
        report.update(report_extras)

    client = AirflowRestClient(cfg.airflow)
    try:
        LOGGER.info(
            "Starting scenario against %s%s",
            cfg.airflow.base_url,
            cfg.airflow.api_prefix,
        )
        expected_ids = _expected_test_dag_ids(
            cfg.dag_expectations.prefix, cfg.dag_expectations.count
        ) | {cfg.dag_expectations.trigger_dag_id}

        if cfg.sync.enabled:
            rev_marker = (cfg.sync.revision_marker or "").strip()
            LOGGER.info(
                "Sync phase: waiting for %d expected DAGs (timeout=%ss, poll=%ss)%s",
                len(expected_ids),
                cfg.sync.timeout_sec,
                cfg.sync.poll_interval_sec,
                (
                    f"; revision_marker on {cfg.sync.revision_marker_dag_id!r}"
                    if rev_marker
                    else ""
                ),
            )
            sync_polls = 0

            async def _sync_ok() -> tuple[bool, dict[str, Any]]:
                ids = await client.list_all_dag_ids()
                missing = sorted(expected_ids - ids)
                detail: dict[str, Any] = {
                    "missing_dag_ids": missing,
                    "present_count": len(expected_ids & ids),
                    "expected_count": len(expected_ids),
                }
                if rev_marker:
                    detail["revision_marker"] = rev_marker
                    detail["revision_marker_dag_id"] = cfg.sync.revision_marker_dag_id
                    if cfg.sync.push_time_iso:
                        detail["push_time_iso"] = cfg.sync.push_time_iso
                if missing:
                    return False, detail
                if rev_marker:
                    try:
                        dag = await client.get_dag(cfg.sync.revision_marker_dag_id)
                    except httpx.HTTPStatusError as exc:
                        if exc.response.status_code == 404:
                            detail["revision_marker_matched"] = False
                            detail["revision_marker_error"] = "dag_not_found"
                            return False, detail
                        raise
                    desc = dag.get("description")
                    desc_str = desc if isinstance(desc, str) else ""
                    preview = (
                        desc_str if len(desc_str) <= 240 else f"{desc_str[:240]}..."
                    )
                    detail["revision_marker_description_preview"] = preview
                    if rev_marker not in desc_str:
                        detail["revision_marker_matched"] = False
                        return False, detail
                    detail["revision_marker_matched"] = True
                if cfg.sync.require_import_errors_empty:
                    raw = await client.list_import_errors_raw()
                    relevant = [
                        e
                        for e in raw
                        if _import_error_relevant(
                            e, cfg.sync.ignore_import_error_substrings
                        )
                    ]
                    detail["import_errors_total"] = len(raw)
                    detail["import_errors_relevant"] = len(relevant)
                    if relevant:
                        detail["sample_relevant"] = relevant[:3]
                        return False, detail
                return True, detail

            async def pred():
                nonlocal sync_polls
                sync_polls += 1
                ok, d = await _sync_ok()
                missing = d.get("missing_dag_ids", [])
                if ok:
                    LOGGER.info(
                        "Sync poll %d: all expected DAGs visible (%d/%d)%s",
                        sync_polls,
                        d.get("present_count", 0),
                        d.get("expected_count", len(expected_ids)),
                        (
                            " and revision marker matched"
                            if d.get("revision_marker_matched") is True
                            else ""
                        ),
                    )
                elif rev_marker and not missing:
                    LOGGER.info(
                        "Sync poll %d: DAGs visible but waiting for revision marker on %s (%d/%d present)",
                        sync_polls,
                        cfg.sync.revision_marker_dag_id,
                        d.get("present_count", 0),
                        d.get("expected_count", len(expected_ids)),
                    )
                else:
                    LOGGER.info(
                        "Sync poll %d: waiting for DAG visibility (%d missing, %d/%d present)",
                        sync_polls,
                        len(missing),
                        d.get("present_count", 0),
                        d.get("expected_count", len(expected_ids)),
                    )
                return ok, d

            success, elapsed, last = await _wait_until(
                cfg.sync.timeout_sec,
                cfg.sync.poll_interval_sec,
                cfg.sync.poll_jitter_fraction,
                pred,
            )
            detail = last[1] if isinstance(last, tuple) else {}
            if success and cfg.sync.push_time_iso:
                push_dt = _parse_iso_datetime(cfg.sync.push_time_iso)
                if push_dt is None:
                    detail["sync_lag_since_push_sec_error"] = "invalid push_time_iso"
                else:
                    now = datetime.now(timezone.utc)
                    detail["sync_lag_since_push_sec"] = round(
                        (now - push_dt).total_seconds(), 3
                    )
                    detail["marker_observed_wall_time_iso"] = now.isoformat().replace(
                        "+00:00", "Z"
                    )
            phases.append(
                PhaseResult(
                    name="sync_and_import",
                    ok=success,
                    duration_sec=min(elapsed, cfg.sync.timeout_sec),
                    detail=detail,
                )
            )
            LOGGER.info(
                "Sync phase %s in %.2fs",
                "completed" if success else "failed",
                min(elapsed, cfg.sync.timeout_sec),
            )
            if not success:
                report["phases"] = [asdict(p) for p in phases]
                report["http_metrics"] = client.http_metrics_report()
                if report_path:
                    report_path.write_text(
                        json.dumps(report, indent=2), encoding="utf-8"
                    )
                    LOGGER.info("Wrote report to %s", report_path)
                return 1

        triggered_refs: list[DagRunRef] = []
        child_expected_counts: dict[str, int] = {}
        child_not_before_candidates: list[datetime] = []

        if cfg.execution.enabled:
            LOGGER.info("Execution phase: starting")
            for dag_id in cfg.execution.unpause_dag_ids:
                LOGGER.info("Unpausing DAG: %s", dag_id)
                await client.set_dag_paused(dag_id, is_paused=False)

            sem = (
                asyncio.Semaphore(cfg.execution.max_trigger_in_flight)
                if cfg.execution.max_trigger_in_flight is not None
                else None
            )

            seq = 0
            for spec in cfg.execution.triggers:
                spec_runs = spec.parallelism * spec.repetitions
                LOGGER.info(
                    "Triggering DAG %s (%d runs: repetitions=%d x parallelism=%d)",
                    spec.dag_id,
                    spec_runs,
                    spec.repetitions,
                    spec.parallelism,
                )

                async def _one_trigger(
                    dag_id_t: str, conf: dict[str, Any], n: int
                ) -> DagRunRef:
                    async with _optional_semaphore(sem):
                        return await client.trigger_dag_run(
                            dag_id_t,
                            conf=conf,
                            logical_date_iso=_utc_logical_date_unique(n),
                        )

                tasks = []
                for _ in range(spec.repetitions):
                    for _p in range(spec.parallelism):
                        seq += 1
                        n = seq
                        tasks.append(_one_trigger(spec.dag_id, spec.conf, n))
                batch = await asyncio.gather(*tasks)
                triggered_refs.extend(batch)
                LOGGER.info("Triggered %d DAG runs for %s", len(batch), spec.dag_id)
                if (
                    cfg.execution.wait.include_triggered_children
                    and spec.dag_id == cfg.dag_expectations.trigger_dag_id
                ):
                    dag_num_raw = spec.conf.get("dag_num")
                    if isinstance(dag_num_raw, bool) or not isinstance(
                        dag_num_raw, int
                    ):
                        LOGGER.warning(
                            "Execution wait: include_triggered_children is enabled, but no child runs "
                            "could be derived (expected integer conf.dag_num in trigger spec)"
                        )
                    else:
                        dag_num = max(0, min(cfg.dag_expectations.count, dag_num_raw))
                        if dag_num == 0:
                            LOGGER.warning(
                                "Execution wait: include_triggered_children enabled but conf.dag_num=0"
                            )
                        else:
                            for i in range(dag_num):
                                child_dag_id = f"{cfg.dag_expectations.prefix}{i}"
                                child_expected_counts[child_dag_id] = (
                                    child_expected_counts.get(child_dag_id, 0)
                                    + len(batch)
                                )
                            for parent in batch:
                                ts = _parse_run_id_timestamp(parent.dag_run_id)
                                if ts is not None:
                                    child_not_before_candidates.append(ts)
                            LOGGER.info(
                                "Execution wait: will discover up to %d child DAG runs across %d DAGs",
                                sum(child_expected_counts.values()),
                                len(child_expected_counts),
                            )

            report["dag_runs"] = [
                {"dag_id": r.dag_id, "dag_run_id": r.dag_run_id} for r in triggered_refs
            ]
            child_not_before = (
                min(child_not_before_candidates)
                if child_not_before_candidates
                else None
            )
            w = cfg.execution.wait
            observed_child_refs: list[DagRunRef] = []
            observed_refs: list[DagRunRef] = []

            async def _sync_observed_refs() -> None:
                nonlocal observed_child_refs, observed_refs
                if w.include_triggered_children and child_expected_counts:
                    observed_child_refs = await _discover_triggered_children_refs(
                        client=client,
                        child_expected_counts=child_expected_counts,
                        not_before=child_not_before,
                        page_limit=w.list_dag_runs_page_limit,
                    )
                else:
                    observed_child_refs = []
                observed_refs = _dedupe_refs(triggered_refs + observed_child_refs)

            await _sync_observed_refs()
            report["observed_dag_runs"] = [
                {"dag_id": r.dag_id, "dag_run_id": r.dag_run_id} for r in observed_refs
            ]
            planned_child_runs = sum(child_expected_counts.values())
            discovered_child_runs = len(observed_child_refs)
            LOGGER.info(
                "Execution wait: tracking %d DAG runs (%d directly triggered + %d discovered children; planned children=%d)",
                len(observed_refs),
                len(triggered_refs),
                discovered_child_runs,
                planned_child_runs,
            )

            terminal = {"success", "failed"}
            t_wait0 = time.monotonic()
            deadline = t_wait0 + cfg.execution.wait.timeout_sec
            peak_run = 0
            peak_ti_running = 0
            wait_polls = 0

            exec_opts = {
                "max_trigger_in_flight": cfg.execution.max_trigger_in_flight,
                "batch_poll_dag_runs": w.batch_poll_dag_runs,
                "list_dag_runs_page_limit": w.list_dag_runs_page_limit,
                "observe_task_concurrency": w.observe_task_concurrency,
                "include_triggered_children": w.include_triggered_children,
                "triggered_dag_runs_count": len(triggered_refs),
                "child_dag_ids_expected": sorted(child_expected_counts.keys()),
            }

            while time.monotonic() < deadline:
                wait_polls += 1
                await _sync_observed_refs()
                discovered_child_runs = len(observed_child_refs)
                planned_child_runs = sum(child_expected_counts.values())
                observe = list(dict.fromkeys(r.dag_id for r in observed_refs))
                states_snapshot = await _dag_run_states_snapshot(
                    client,
                    observed_refs,
                    batch=w.batch_poll_dag_runs,
                    page_limit=w.list_dag_runs_page_limit,
                )

                running_runs = 0
                for r in observed_refs:
                    st = states_snapshot.get(r.key)
                    if st == "running":
                        running_runs += 1

                running_tasks = 0
                if w.observe_task_concurrency:
                    for dag_id in observe:
                        for ref in observed_refs:
                            if ref.dag_id != dag_id:
                                continue
                            try:
                                tc = await client.count_task_instances_by_state(ref)
                            except httpx.HTTPStatusError:
                                continue
                            running_tasks += tc.get("running", 0) + tc.get(
                                "deferred", 0
                            )

                peak_run = max(peak_run, running_runs)
                peak_ti_running = max(peak_ti_running, running_tasks)
                terminal_done = sum(
                    1 for r in observed_refs if states_snapshot.get(r.key) in terminal
                )
                LOGGER.info(
                    "Execution poll %d: finished_dags=%d/%d, running_dag_runs=%d, running_tasks=%d, discovered_children=%d/%d",
                    wait_polls,
                    terminal_done,
                    len(observed_refs),
                    running_runs,
                    running_tasks,
                    discovered_child_runs,
                    planned_child_runs,
                )

                if all(states_snapshot.get(r.key) in terminal for r in observed_refs):
                    break
                await asyncio.sleep(
                    _poll_delay_sec(
                        w.poll_interval_sec,
                        w.poll_jitter_fraction,
                    )
                )

            elapsed_wait = time.monotonic() - t_wait0
            await _sync_observed_refs()
            final_snapshot = await _dag_run_states_snapshot(
                client,
                observed_refs,
                batch=w.batch_poll_dag_runs,
                page_limit=w.list_dag_runs_page_limit,
            )
            report["observed_dag_runs"] = [
                {"dag_id": r.dag_id, "dag_run_id": r.dag_run_id} for r in observed_refs
            ]
            ti_per_run, ti_overall = await _collect_task_instance_totals(
                client, observed_refs
            )
            child_refs = [r for r in observed_refs if r.dag_id in child_expected_counts]
            child_duration_summary = await _collect_run_duration_summary_sec(
                client, child_refs
            )
            final_states = {}
            failures = []
            for ref in observed_refs:
                st = final_snapshot.get(ref.key)
                if isinstance(st, str):
                    final_states[ref.key] = st
                    if st == "failed":
                        failures.append(ref.key)

            timed_out = elapsed_wait >= cfg.execution.wait.timeout_sec
            missing_terminal = any(
                final_states.get(r.key) not in terminal for r in observed_refs
            )
            ok_exec = not timed_out and not missing_terminal and len(failures) == 0
            ti_total = _sum_counts(ti_overall)
            ti_skipped = ti_overall.get("skipped", 0)
            ti_non_skipped = ti_total - ti_skipped

            phases.append(
                PhaseResult(
                    name="execution",
                    ok=ok_exec,
                    duration_sec=elapsed_wait,
                    detail={
                        "final_states": final_states,
                        "failures": failures,
                        "timed_out": timed_out,
                        "missing_terminal": missing_terminal,
                        "observed_child_dag_runs_count": len(observed_child_refs),
                        "observed_dag_runs_count": len(observed_refs),
                        "task_instances_total": ti_total,
                        "task_instances_non_skipped_total": ti_non_skipped,
                        "task_instances_skipped_total": ti_skipped,
                        "task_instances_by_state": ti_overall,
                        "task_instances_by_dag_run": ti_per_run,
                        "child_dag_execution_time_sec": child_duration_summary,
                        **exec_opts,
                    },
                )
            )
            report["peak_dag_runs_running"] = peak_run
            report["peak_task_running"] = peak_ti_running
            LOGGER.info(
                "Execution phase %s in %.2fs (peak_dag_runs_running=%d, peak_task_running=%d)",
                "completed" if ok_exec else "failed",
                elapsed_wait,
                peak_run,
                peak_ti_running,
            )

        report["phases"] = [asdict(p) for p in phases]
        report["http_metrics"] = client.http_metrics_report()
        exit_code = 0 if all(p.ok for p in phases) else 1
        if report_path:
            report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
            LOGGER.info("Wrote report to %s", report_path)
        LOGGER.info("Scenario finished with exit code %d", exit_code)
        return exit_code
    finally:
        await client.aclose()
