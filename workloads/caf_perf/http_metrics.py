"""Aggregate HTTP latencies, status counts, and retries for perf reports."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class OpStats:
    """Stats for one logical operation bucket (e.g. ``post_dag_runs``)."""

    latency_sec: list[float] = field(default_factory=list)
    final_status: dict[str, int] = field(default_factory=dict)

    def record(self, *, status_code: int, elapsed_sec: float) -> None:
        key = str(status_code)
        self.final_status[key] = self.final_status.get(key, 0) + 1
        if 200 <= status_code < 300:
            self.latency_sec.append(elapsed_sec)

    def to_report(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "final_status": dict(
                sorted(self.final_status.items(), key=lambda x: int(x[0]))
            ),
        }
        lat = _latency_summary_sec(self.latency_sec)
        if lat:
            out["latency_sec"] = lat
        return out


def _latency_summary_sec(samples: list[float]) -> dict[str, Any]:
    if not samples:
        return {}
    s = sorted(samples)
    n = len(s)

    def pct(p: float) -> float:
        if n == 1:
            return s[0]
        rank = (n - 1) * (p / 100.0)
        lo = int(rank)
        hi = min(lo + 1, n - 1)
        if lo == hi:
            return s[lo]
        frac = rank - lo
        return s[lo] + (s[hi] - s[lo]) * frac

    total = sum(s)
    return {
        "count": n,
        "min": round(s[0], 6),
        "mean": round(total / n, 6),
        "p50": round(pct(50), 6),
        "p95": round(pct(95), 6),
        "max": round(s[-1], 6),
    }


def metrics_label_for_request(method: str, path: str) -> str:
    """Map HTTP method + path (relative to API base) to a stable metric bucket."""
    p = path.split("?", 1)[0]
    m = method.upper()
    if m == "POST" and p.endswith("/dagRuns"):
        return "post_dag_runs"
    if m == "GET" and "/taskInstances" in p:
        return "get_task_instances"
    if m == "GET" and "/dagRuns/" in p:
        return "get_dag_run"
    if m == "GET" and p.rstrip("/").endswith("/dagRuns"):
        return "list_dag_runs"
    if m == "GET" and (p == "/dags" or p.rstrip("/") == "/dags"):
        return "get_dags"
    qpath = p.rstrip("/")
    if (
        m == "GET"
        and qpath.startswith("/dags/")
        and qpath.count("/") == 2
        and "/dagRuns" not in qpath
        and "/importErrors" not in qpath
    ):
        return "get_dag_detail"
    if m == "GET" and "/importErrors" in p:
        return "get_import_errors"
    if m == "PATCH" and p.startswith("/dags/") and "/dagRuns" not in p:
        return "patch_dag"
    return f"{m.lower()}_{_safe_key(p)}"


def _safe_key(path: str) -> str:
    x = path.strip("/").replace("/", "_")
    return x[:80] if len(x) > 80 else x or "root"


@dataclass
class HttpMetricsCollector:
    by_operation: dict[str, OpStats] = field(default_factory=dict)
    retries_total: int = 0
    retry_events_by_status: dict[str, int] = field(default_factory=dict)

    def _bucket(self, op: str) -> OpStats:
        if op not in self.by_operation:
            self.by_operation[op] = OpStats()
        return self.by_operation[op]

    def record_outcome(self, op: str, status_code: int, elapsed_sec: float) -> None:
        self._bucket(op).record(status_code=status_code, elapsed_sec=elapsed_sec)

    def record_retry(self, status_code: int | None) -> None:
        self.retries_total += 1
        key = str(status_code) if status_code is not None else "transport"
        self.retry_events_by_status[key] = self.retry_events_by_status.get(key, 0) + 1

    def to_report(self) -> dict[str, Any]:
        return {
            "by_operation": {
                k: v.to_report() for k, v in sorted(self.by_operation.items())
            },
            "retries_total": self.retries_total,
            "retry_events_by_status": dict(
                sorted(self.retry_events_by_status.items(), key=lambda x: x[0])
            ),
        }
