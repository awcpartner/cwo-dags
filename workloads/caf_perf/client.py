"""Minimal async client for Airflow 3.2.x ``/api/v2`` REST endpoints."""

from __future__ import annotations

import asyncio
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Mapping

import httpx

from caf_perf.config import AirflowConnConfig
from caf_perf.http_metrics import HttpMetricsCollector, metrics_label_for_request

_OAUTH_REFRESH_SKEW_SEC = 60.0


def _retryable_status(code: int) -> bool:
    return code in (429, 500, 502, 503, 504)


@dataclass(frozen=True)
class DagRunRef:
    dag_id: str
    dag_run_id: str

    @property
    def key(self) -> str:
        return f"{self.dag_id}/{self.dag_run_id}"


class AirflowRestClient:
    """
    Thin wrapper around Airflow stable REST API (tested against 3.2.1).

    Paths are relative to ``api_prefix`` (default ``/api/v2``).
    """

    def __init__(self, cfg: AirflowConnConfig) -> None:
        self._cfg = cfg
        base = cfg.base_url.rstrip("/") + cfg.api_prefix.rstrip("/")
        headers: dict[str, str] = {"Accept": "application/json"}
        self._oauth_lock = asyncio.Lock()
        self._oauth_access_token: str | None = None
        self._oauth_refresh_after_monotonic: float = 0.0
        self._metrics = HttpMetricsCollector()

        limits = httpx.Limits(
            max_connections=cfg.http_max_connections,
            max_keepalive_connections=cfg.http_max_keepalive_connections,
        )
        self._client = httpx.AsyncClient(
            base_url=base,
            headers=headers,
            verify=cfg.verify_tls,
            timeout=httpx.Timeout(120.0, connect=30.0),
            limits=limits,
        )

    def http_metrics_report(self) -> dict[str, Any]:
        return self._metrics.to_report()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _paginate_dict_list(
        self, path: str, *, list_key: str, page_size: int
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        offset = 0
        while True:
            data = await self.get_json(
                path, params={"limit": page_size, "offset": offset}
            )
            if not isinstance(data, dict):
                break
            chunk = data.get(list_key)
            if not isinstance(chunk, list):
                break
            for item in chunk:
                if isinstance(item, dict):
                    out.append(item)
            total = data.get("total_entries")
            if not isinstance(total, int):
                total = offset + len(chunk)
            offset += len(chunk)
            if offset >= total or not chunk:
                break
        return out

    async def _backoff_sleep(self, attempt: int) -> None:
        cap = min(
            self._cfg.http_retry_max_sec,
            self._cfg.http_retry_base_sec * (2**attempt),
        )
        delay = random.uniform(0.0, cap)
        await asyncio.sleep(delay)

    async def _refresh_awc_token(self) -> None:
        o = self._cfg.awc_console_oauth
        if o is None:
            return
        domain = os.environ[o.domain_env].strip()
        client_id = os.environ[o.client_id_env].strip()
        client_secret = os.environ[o.client_secret_env].strip()
        url = f"https://console.{domain}/api/v0/auth/access-keys/token"
        last_response: httpx.Response | None = None
        for attempt in range(self._cfg.http_max_retries + 1):
            try:
                async with httpx.AsyncClient(
                    verify=self._cfg.verify_tls,
                    timeout=httpx.Timeout(120.0, connect=30.0),
                    limits=httpx.Limits(
                        max_connections=max(10, self._cfg.http_max_connections),
                        max_keepalive_connections=max(
                            5, self._cfg.http_max_keepalive_connections
                        ),
                    ),
                ) as token_client:
                    last_response = await token_client.post(
                        url,
                        auth=(client_id, client_secret),
                        data={"grant_type": "client_credentials"},
                        headers={"Accept": "application/json"},
                    )
            except httpx.TransportError:
                if attempt < self._cfg.http_max_retries:
                    await self._backoff_sleep(attempt)
                    continue
                raise

            if last_response.is_success:
                break
            if (
                _retryable_status(last_response.status_code)
                and attempt < self._cfg.http_max_retries
            ):
                await self._backoff_sleep(attempt)
                continue
            last_response.raise_for_status()

        if last_response is None:
            msg = "Token request produced no response"
            raise RuntimeError(msg)
        data = last_response.json()
        if not isinstance(data, dict):
            msg = f"Unexpected token response (not an object): {data!r}"
            raise RuntimeError(msg)
        token = data.get("access_token")
        if not isinstance(token, str) or not token.strip():
            msg = f"Missing access_token in token response: {data!r}"
            raise RuntimeError(msg)
        expires_in = data.get("expires_in", 3600)
        if isinstance(expires_in, bool) or not isinstance(expires_in, int | float):
            expires_in = 3600
        ttl = max(0.0, float(expires_in) - _OAUTH_REFRESH_SKEW_SEC)
        self._oauth_access_token = token.strip()
        self._oauth_refresh_after_monotonic = time.monotonic() + ttl

    async def _ensure_awc_token(self) -> None:
        if self._cfg.awc_console_oauth is None:
            return
        async with self._oauth_lock:
            if (
                self._oauth_access_token is not None
                and time.monotonic() < self._oauth_refresh_after_monotonic
            ):
                return
            await self._refresh_awc_token()
            self._client.headers["Authorization"] = f"Bearer {self._oauth_access_token}"

    async def _invalidate_oauth_token(self) -> None:
        async with self._oauth_lock:
            self._oauth_access_token = None
            self._oauth_refresh_after_monotonic = 0.0

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        await self._ensure_awc_token()
        op = metrics_label_for_request(method, path)
        oauth_refresh_used = False

        for attempt in range(self._cfg.http_max_retries + 1):
            t0 = time.monotonic()
            try:
                response = await self._client.request(method, path, **kwargs)
            except httpx.TransportError:
                if attempt < self._cfg.http_max_retries:
                    self._metrics.record_retry(None)
                    await self._backoff_sleep(attempt)
                    continue
                raise

            elapsed = time.monotonic() - t0

            if 200 <= response.status_code < 300:
                self._metrics.record_outcome(op, response.status_code, elapsed)
                return response

            if (
                response.status_code == 401
                and self._cfg.awc_console_oauth is not None
                and not oauth_refresh_used
            ):
                oauth_refresh_used = True
                await self._invalidate_oauth_token()
                await self._ensure_awc_token()
                continue

            if (
                _retryable_status(response.status_code)
                and attempt < self._cfg.http_max_retries
            ):
                self._metrics.record_retry(response.status_code)
                await self._backoff_sleep(attempt)
                continue

            self._metrics.record_outcome(op, response.status_code, elapsed)
            return response

    async def get_json(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        r = await self._request("GET", path, params=params)
        r.raise_for_status()
        return r.json()

    async def patch_json(self, path: str, body: Mapping[str, Any]) -> Any:
        r = await self._request("PATCH", path, json=body)
        r.raise_for_status()
        if not r.content:
            return {}
        return r.json()

    async def post_json(self, path: str, body: Mapping[str, Any]) -> Any:
        r = await self._request("POST", path, json=body)
        r.raise_for_status()
        return r.json()

    async def list_all_dag_ids(self) -> set[str]:
        rows = await self._paginate_dict_list("/dags", list_key="dags", page_size=100)
        return {str(d["dag_id"]) for d in rows if "dag_id" in d}

    async def list_import_errors_raw(self) -> list[dict[str, Any]]:
        return await self._paginate_dict_list(
            "/importErrors", list_key="import_errors", page_size=100
        )

    async def get_dag(self, dag_id: str) -> dict[str, Any]:
        data = await self.get_json(f"/dags/{dag_id}")
        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected get dag response: {data!r}")
        return data

    async def set_dag_paused(self, dag_id: str, *, is_paused: bool) -> None:
        await self.patch_json(f"/dags/{dag_id}", {"is_paused": is_paused})

    async def trigger_dag_run(
        self,
        dag_id: str,
        *,
        conf: Mapping[str, Any] | None = None,
        logical_date_iso: str,
    ) -> DagRunRef:
        body: dict[str, Any] = {"logical_date": logical_date_iso}
        if conf:
            body["conf"] = dict(conf)
        data = await self.post_json(f"/dags/{dag_id}/dagRuns", body)
        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected dag run response: {data!r}")
        drid = data.get("dag_run_id")
        if not isinstance(drid, str) or not drid:
            raise RuntimeError(f"Missing dag_run_id in response: {data!r}")
        return DagRunRef(dag_id=dag_id, dag_run_id=drid)

    async def map_dag_run_states(
        self,
        dag_id: str,
        wanted_run_ids: set[str],
        page_limit: int,
    ) -> dict[str, str]:
        """
        Resolve ``dag_run_id -> state`` for ids in ``wanted_run_ids``.

        Lists ``GET /dags/{dag_id}/dagRuns`` (paginated), then falls back to
        per-run GET for any id not seen in list pages (rare).
        """
        found: dict[str, str] = {}
        offset = 0
        limit = max(1, page_limit)
        remaining = set(wanted_run_ids)

        while remaining:
            data = await self.get_json(
                f"/dags/{dag_id}/dagRuns",
                params={"limit": limit, "offset": offset},
            )
            if not isinstance(data, dict):
                break
            dag_runs = data.get("dag_runs")
            if not isinstance(dag_runs, list):
                break
            for dr in dag_runs:
                if not isinstance(dr, dict):
                    continue
                rid = dr.get("dag_run_id")
                st = dr.get("state")
                if rid in remaining and isinstance(rid, str) and isinstance(st, str):
                    found[rid] = st
                    remaining.discard(rid)
            total = data.get("total_entries")
            if not isinstance(total, int):
                total = offset + len(dag_runs)
            offset += len(dag_runs)
            if not dag_runs or offset >= total:
                break

        for rid in list(remaining):
            ref = DagRunRef(dag_id=dag_id, dag_run_id=rid)
            try:
                dr = await self.get_dag_run(ref)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    continue
                raise
            st = dr.get("state")
            if isinstance(st, str):
                found[rid] = st
                remaining.discard(rid)

        return found

    async def get_dag_run(self, ref: DagRunRef) -> dict[str, Any]:
        data = await self.get_json(f"/dags/{ref.dag_id}/dagRuns/{ref.dag_run_id}")
        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected get dag run response: {data!r}")
        return data

    async def count_task_instances_by_state(self, ref: DagRunRef) -> dict[str, int]:
        """Aggregate task instance states for one DAG run (paginated)."""
        path = f"/dags/{ref.dag_id}/dagRuns/{ref.dag_run_id}/taskInstances"
        rows = await self._paginate_dict_list(
            path, list_key="task_instances", page_size=200
        )
        counts: dict[str, int] = {}
        for ti in rows:
            st = ti.get("state")
            if isinstance(st, str):
                counts[st] = counts.get(st, 0) + 1
        return counts
