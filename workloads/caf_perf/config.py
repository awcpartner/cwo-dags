"""Load and validate JSON scenario files for ``caf-perf``."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class AWCConsoleOAuthConfig:
    """Client-credentials token from ``https://console.${DOMAIN}/api/v0/auth/access-keys/token``."""

    domain_env: str = "DOMAIN"
    cwo_url_env: str = "CWO_URL"
    client_id_env: str = "CLIENT_ID"
    client_secret_env: str = "CLIENT_SECRET"


@dataclass
class AirflowConnConfig:
    base_url: str
    api_prefix: str = "/api/v2"
    verify_tls: bool = True
    awc_console_oauth: AWCConsoleOAuthConfig | None = None
    http_max_retries: int = 4
    http_retry_base_sec: float = 0.5
    http_retry_max_sec: float = 30.0
    http_max_connections: int = 100
    http_max_keepalive_connections: int = 20


@dataclass
class DagExpectations:
    prefix: str = "test_dag_"
    count: int = 100
    trigger_dag_id: str = "trigger_test_dags"


@dataclass
class GitPublishConfig:
    """
    Under ``sync`` in JSON. Checkout ``branch`` in ``repo_path``, rewrite the marker block in
    ``marker_file_relative``, commit, and push. Fills ``sync.revision_marker`` and
    ``sync.push_time_iso`` for the sync wait and ``sync_lag_since_push_sec`` in the report.
    """

    enabled: bool = False
    #: Git checkout root. Absolute path, path relative to the scenario JSON file's directory,
    #: or ``__cwd__`` for the git worktree root that contains the process working directory.
    repo_path: str = ""
    remote: str = "origin"
    #: Branch that receives the marker commit (must already exist on ``remote``).
    branch: str = ""
    #: Repo-relative Python file containing the ``# --- caf-perf sync marker block ---`` section.
    marker_file_relative: str = "workloads/dags/test_dags.py"
    #: Stash tracked + untracked (``git stash push -u``) before checkout; ``stash pop`` after.
    stash_local_changes: bool = True


@dataclass
class SyncPhaseConfig:
    enabled: bool = True
    timeout_sec: float = 1200.0
    poll_interval_sec: float = 5.0
    poll_jitter_fraction: float = 0.0
    require_import_errors_empty: bool = False
    ignore_import_error_substrings: list[str] = field(
        default_factory=lambda: ["import_error.py", "invalid_dag.py"]
    )
    #: Substring that must appear in ``GET /dags/{revision_marker_dag_id}`` ``description``
    #: after deploy (``_CAF_PERF_SYNC_MARKER_VALUE`` in ``test_dags.py``, or env
    #: ``CAF_PERF_SYNC_MARKER`` at Airflow parse time). Empty = skip unless ``git_publish`` fills it.
    revision_marker: str | None = None
    #: DAG whose ``description`` must contain ``revision_marker``. Omitted in JSON →
    #: ``dag_expectations.trigger_dag_id``.
    revision_marker_dag_id: str = "trigger_test_dags"
    #: Wall time of the git push (ISO 8601). Not loaded from JSON; set by ``sync.git_publish``
    #: when enabled, for ``sync_lag_since_push_sec`` in the report.
    push_time_iso: str | None = None
    git_publish: GitPublishConfig = field(default_factory=GitPublishConfig)


@dataclass
class TriggerSpec:
    #: Omitted in JSON → ``dag_expectations.trigger_dag_id``.
    dag_id: str
    parallelism: int = 1
    repetitions: int = 1
    conf: dict[str, Any] = field(default_factory=dict)


@dataclass
class WaitConfig:
    timeout_sec: float = 7200.0
    poll_interval_sec: float = 5.0
    poll_jitter_fraction: float = 0.0
    batch_poll_dag_runs: bool = True
    list_dag_runs_page_limit: int = 100
    observe_task_concurrency: bool = True
    include_triggered_children: bool = False


@dataclass
class ExecutionPhaseConfig:
    enabled: bool = True
    #: Omitted in JSON → distinct ``dag_id`` values from ``triggers`` (so orchestrator DAGs unpause).
    unpause_dag_ids: list[str] = field(default_factory=list)
    triggers: list[TriggerSpec] = field(default_factory=list)
    wait: WaitConfig = field(default_factory=WaitConfig)
    max_trigger_in_flight: int | None = None


@dataclass
class ScenarioConfig:
    airflow: AirflowConnConfig
    dag_expectations: DagExpectations = field(default_factory=DagExpectations)
    sync: SyncPhaseConfig = field(default_factory=SyncPhaseConfig)
    execution: ExecutionPhaseConfig = field(default_factory=ExecutionPhaseConfig)


def _req_str(d: Mapping[str, Any], key: str, *, default: str | None = None) -> str:
    if key not in d:
        if default is not None:
            return default
        raise ConfigError(f"Missing required string field: {key!r}")
    v = d[key]
    if not isinstance(v, str) or not v.strip():
        raise ConfigError(f"Field {key!r} must be a non-empty string")
    return v.strip().rstrip("/")


def _opt_str(d: Mapping[str, Any], key: str) -> str | None:
    v = d.get(key)
    if v is None:
        return None
    if not isinstance(v, str):
        raise ConfigError(f"Field {key!r} must be a string or null")
    s = v.strip()
    return s or None


def _req_bool(d: Mapping[str, Any], key: str, default: bool) -> bool:
    v = d.get(key, default)
    if not isinstance(v, bool):
        raise ConfigError(f"Field {key!r} must be a boolean")
    return v


def _req_int(d: Mapping[str, Any], key: str, default: int) -> int:
    v = d.get(key, default)
    if isinstance(v, bool) or not isinstance(v, int):
        raise ConfigError(f"Field {key!r} must be an integer")
    return v


def _req_float(d: Mapping[str, Any], key: str, default: float) -> float:
    v = d.get(key, default)
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        raise ConfigError(f"Field {key!r} must be a number")
    return float(v)


def _require_non_empty_env(name: str) -> str:
    raw = os.environ.get(name, "").strip()
    if not raw:
        raise ConfigError(
            f"Environment variable {name!r} must be set and non-empty for this auth mode"
        )
    return raw


def _http_client_settings(af: Mapping[str, Any]) -> dict[str, Any]:
    h = af.get("http")
    if h is None:
        return {}
    if not isinstance(h, dict):
        raise ConfigError('"airflow.http" must be an object')
    mc = max(1, _req_int(h, "max_connections", 100))
    mk = max(1, _req_int(h, "max_keepalive_connections", 20))
    if mk > mc:
        mk = mc
    return {
        "http_max_retries": max(0, _req_int(h, "max_retries", 4)),
        "http_retry_base_sec": max(0.01, _req_float(h, "retry_base_sec", 0.5)),
        "http_retry_max_sec": max(0.01, _req_float(h, "retry_max_sec", 30.0)),
        "http_max_connections": mc,
        "http_max_keepalive_connections": mk,
    }


def _poll_jitter_fraction(d: Mapping[str, Any], key: str, default: float) -> float:
    v = _req_float(d, key, default)
    if v < 0.0 or v > 1.0:
        raise ConfigError(f"Field {key!r} must be between 0 and 1")
    return v


def _git_worktree_root(start: Path) -> Path:
    """Directory at or above ``start`` that contains a ``.git`` entry, else ``start`` resolved."""
    cur = start.resolve()
    for d in (cur, *cur.parents):
        git_entry = d / ".git"
        if git_entry.is_file() or git_entry.is_dir():
            return d
    return cur


def _resolve_git_repo_path(raw: str, *, scenario_dir: Path) -> str:
    """
    ``sync.git_publish.repo_path``: absolute paths are expanded as-is; relative paths are
    resolved against the scenario JSON's directory; the literal ``__cwd__`` resolves to the
    git worktree root containing the current working directory (walks up for ``.git``), or
    ``Path.cwd()`` if none is found.
    """
    s = raw.strip()
    if not s:
        return ""
    if s == "__cwd__":
        return str(_git_worktree_root(Path.cwd()))
    p = Path(s).expanduser()
    if p.is_absolute():
        return str(p.resolve())
    return str((scenario_dir / p).resolve())


def load_scenario(path: str | Path) -> ScenarioConfig:
    cfg_path = Path(path).expanduser()
    scenario_dir = cfg_path.resolve().parent
    raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ConfigError("Root of config must be a JSON object")

    af = raw.get("airflow")
    if not isinstance(af, dict):
        raise ConfigError('Missing object "airflow"')

    auth = af.get("auth") or {}
    if not isinstance(auth, dict):
        raise ConfigError('"airflow.auth" must be an object')

    auth_type = _opt_str(auth, "type")
    awc_oauth: AWCConsoleOAuthConfig | None = None
    base_url: str

    if auth_type == "awc_console_oauth":
        awc_oauth = AWCConsoleOAuthConfig(
            domain_env=_opt_str(auth, "domain_env") or "DOMAIN",
            cwo_url_env=_opt_str(auth, "cwo_url_env") or "CWO_URL",
            client_id_env=_opt_str(auth, "client_id_env") or "CLIENT_ID",
            client_secret_env=_opt_str(auth, "client_secret_env") or "CLIENT_SECRET",
        )
        for vname in (
            awc_oauth.domain_env,
            awc_oauth.cwo_url_env,
            awc_oauth.client_id_env,
            awc_oauth.client_secret_env,
        ):
            _require_non_empty_env(vname)
        base_url = _require_non_empty_env(awc_oauth.cwo_url_env).rstrip("/")
        if _opt_str(auth, "bearer_token_env") or auth.get("basic"):
            raise ConfigError(
                '"airflow.auth": awc_console_oauth must not be combined '
                "with bearer_token_env or basic"
            )
    elif auth_type is not None and auth_type != "":
        raise ConfigError(
            f"Unsupported airflow.auth.type: {auth_type!r} "
            '(use "awc_console_oauth" or omit type for no auth)'
        )
    else:
        if _opt_str(auth, "bearer_token_env") or auth.get("basic"):
            raise ConfigError(
                '"airflow.auth": bearer/basic auth options were removed; use '
                '"type": "awc_console_oauth" or empty auth object'
            )
        base_url = _req_str(af, "base_url")

    airflow = AirflowConnConfig(
        base_url=base_url,
        api_prefix=_req_str(af, "api_prefix", default="/api/v2"),
        verify_tls=_req_bool(af, "verify_tls", True),
        awc_console_oauth=awc_oauth,
        **_http_client_settings(af),
    )

    de_raw = raw.get("dag_expectations", {})
    if not isinstance(de_raw, dict):
        raise ConfigError('"dag_expectations" must be an object')
    dag_expectations = DagExpectations(
        prefix=_req_str(de_raw, "prefix", default="test_dag_"),
        count=_req_int(de_raw, "count", 100),
        trigger_dag_id=_req_str(de_raw, "trigger_dag_id", default="trigger_test_dags"),
    )

    sy_raw = raw.get("sync", {})
    if not isinstance(sy_raw, dict):
        raise ConfigError('"sync" must be an object')
    if raw.get("git_publish") is not None:
        raise ConfigError(
            '"git_publish" must be nested under "sync", not at the scenario root '
            '(use "sync": { ..., "git_publish": { ... } })'
        )
    ign = sy_raw.get(
        "ignore_import_error_substrings", ["import_error.py", "invalid_dag.py"]
    )
    if ign is None:
        ignore_list: list[str] = []
    elif not isinstance(ign, list):
        raise ConfigError(
            '"sync.ignore_import_error_substrings" must be a list of strings'
        )
    else:
        ignore_list = [str(x) for x in ign]

    rev_marker = _opt_str(sy_raw, "revision_marker")
    rev_dag_raw = _opt_str(sy_raw, "revision_marker_dag_id")
    rev_dag_id = rev_dag_raw or dag_expectations.trigger_dag_id

    gp_raw = sy_raw.get("git_publish")
    if gp_raw is None:
        git_publish = GitPublishConfig()
    elif not isinstance(gp_raw, dict):
        raise ConfigError('"sync.git_publish" must be an object or absent')
    else:
        gp_enabled = _req_bool(gp_raw, "enabled", False)
        gp_repo_raw = (_opt_str(gp_raw, "repo_path") or "").strip()
        gp_repo = _resolve_git_repo_path(gp_repo_raw, scenario_dir=scenario_dir)
        if gp_enabled and not gp_repo.strip():
            raise ConfigError(
                "sync.git_publish.repo_path is required when sync.git_publish.enabled is true"
            )
        gp_branch = _opt_str(gp_raw, "branch") or ""
        if gp_enabled and not gp_branch.strip():
            raise ConfigError(
                "sync.git_publish.branch is required when sync.git_publish.enabled is true"
            )
        git_publish = GitPublishConfig(
            enabled=gp_enabled,
            repo_path=gp_repo,
            remote=_req_str(gp_raw, "remote", default="origin"),
            branch=gp_branch.strip(),
            marker_file_relative=_req_str(
                gp_raw,
                "marker_file_relative",
                default="workloads/dags/test_dags.py",
            ),
            stash_local_changes=_req_bool(gp_raw, "stash_local_changes", True),
        )
        if gp_enabled and not _req_bool(sy_raw, "enabled", True):
            raise ConfigError(
                "sync.enabled must be true when sync.git_publish.enabled is true"
            )

    sync = SyncPhaseConfig(
        enabled=_req_bool(sy_raw, "enabled", True),
        timeout_sec=_req_float(sy_raw, "timeout_sec", 1200.0),
        poll_interval_sec=_req_float(sy_raw, "poll_interval_sec", 5.0),
        poll_jitter_fraction=_poll_jitter_fraction(sy_raw, "poll_jitter_fraction", 0.0),
        require_import_errors_empty=_req_bool(
            sy_raw, "require_import_errors_empty", False
        ),
        ignore_import_error_substrings=ignore_list,
        revision_marker=rev_marker,
        revision_marker_dag_id=rev_dag_id,
        git_publish=git_publish,
    )

    ex_raw = raw.get("execution", {})
    if not isinstance(ex_raw, dict):
        raise ConfigError('"execution" must be an object')
    triggers_in = ex_raw.get("triggers", [])
    if not isinstance(triggers_in, list):
        raise ConfigError('"execution.triggers" must be a list')
    triggers: list[TriggerSpec] = []
    for i, t in enumerate(triggers_in):
        if not isinstance(t, dict):
            raise ConfigError(f'"execution.triggers[{i}]" must be an object')
        conf = t.get("conf", {})
        if conf is not None and not isinstance(conf, dict):
            raise ConfigError(f'"execution.triggers[{i}].conf" must be an object')
        dag_id_raw = _opt_str(t, "dag_id")
        triggers.append(
            TriggerSpec(
                dag_id=dag_id_raw or dag_expectations.trigger_dag_id,
                parallelism=max(1, _req_int(t, "parallelism", 1)),
                repetitions=max(1, _req_int(t, "repetitions", 1)),
                conf=dict(conf) if isinstance(conf, dict) else {},
            )
        )

    if "unpause_dag_ids" not in ex_raw:
        unpause_dag_ids = sorted({tr.dag_id for tr in triggers})
    else:
        unpause = ex_raw.get("unpause_dag_ids")
        if not isinstance(unpause, list):
            raise ConfigError('"execution.unpause_dag_ids" must be a list')
        unpause_dag_ids = [str(x) for x in unpause]
    w_raw = ex_raw.get("wait", {})
    if not isinstance(w_raw, dict):
        raise ConfigError('"execution.wait" must be an object')
    page_lim = max(1, min(500, _req_int(w_raw, "list_dag_runs_page_limit", 100)))
    wait = WaitConfig(
        timeout_sec=_req_float(w_raw, "timeout_sec", 7200.0),
        poll_interval_sec=_req_float(w_raw, "poll_interval_sec", 5.0),
        poll_jitter_fraction=_poll_jitter_fraction(w_raw, "poll_jitter_fraction", 0.0),
        batch_poll_dag_runs=_req_bool(w_raw, "batch_poll_dag_runs", True),
        list_dag_runs_page_limit=page_lim,
        observe_task_concurrency=_req_bool(w_raw, "observe_task_concurrency", True),
        include_triggered_children=_req_bool(
            w_raw, "include_triggered_children", False
        ),
    )
    if "observe_dag_ids" in ex_raw:
        raise ConfigError(
            '"execution.observe_dag_ids" was removed; task concurrency now always follows observed DAG runs'
        )

    inflight = ex_raw.get("max_trigger_in_flight")
    max_in_flight: int | None = None
    if inflight is not None:
        if isinstance(inflight, bool) or not isinstance(inflight, int):
            raise ConfigError(
                '"execution.max_trigger_in_flight" must be an integer or null'
            )
        if inflight < 1:
            raise ConfigError('"execution.max_trigger_in_flight" must be >= 1 when set')
        max_in_flight = inflight

    execution = ExecutionPhaseConfig(
        enabled=_req_bool(ex_raw, "enabled", True),
        unpause_dag_ids=unpause_dag_ids,
        triggers=triggers,
        wait=wait,
        max_trigger_in_flight=max_in_flight,
    )

    return ScenarioConfig(
        airflow=airflow,
        dag_expectations=dag_expectations,
        sync=sync,
        execution=execution,
    )
