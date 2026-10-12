# caf-perf

JSON-driven harness against Airflow **3.2.x** stable REST (`/api/v2`): DAG visibility, triggers, wait for completion, and client-side HTTP metrics.

## Run

```bash
caf-perf --config path/to/scenario.json --report report.json
```

## Auth (`airflow.auth`)

| Mode | Config | Notes |
|------|--------|--------|
| None | `{}` | Only for environments that allow anonymous API access (e.g. local dev). |
| AWC Console OAuth | `"type": "awc_console_oauth"` | Requires `DOMAIN`, `CWO_URL`, `CLIENT_ID`, `CLIENT_SECRET`. **`airflow.base_url` is not used**; the API host comes from `CWO_URL`. |

`verify_tls: false` matches `curl -k` when testing with internal CAs.

## HTTP client (`airflow.http`)

- **`max_retries`**: Extra attempts after the first try for **429** and **5xx** (plus transport errors). Default `4`.
- **`retry_base_sec`**, **`retry_max_sec`**: Full jitter backoff between attempts.
- **`max_connections`**, **`max_keepalive_connections`**: `httpx` pool size (keepalive is capped by max connections).

## Sync (`sync`)

- **`poll_jitter_fraction`**: `0`–`1`; randomizes wait between polls (± fraction of `poll_interval_sec`).
- **`git_publish`**: When `enabled`, rewrites the marker block in `marker_file_relative`, commits, and pushes; sets `revision_marker` and push time for sync / `sync_lag_since_push_sec`. If you deploy markers manually instead, set `sync.revision_marker` (and optionally env `CAF_PERF_SYNC_MARKER` in Airflow). 
- **`repo_path`**: absolute path, path relative to the scenario JSON's directory, or **`__cwd__`** for the git worktree root that contains the current working directory (walks up for `.git`; use `"../.."` from `workloads/perf_configs/` if you prefer a path fixed to the scenario file). 
- **`stash_local_changes`**: default `true` uses `git stash push -u` around the publish; use `false` in CI for a strict clean tree.
- **`revision_marker`**: If set (or filled by `git_publish`), sync waits until `GET /dags/{revision_marker_dag_id}` returns a `description` containing this substring.
- **`revision_marker_dag_id`**: DAG id whose `description` is checked. Omit to use `dag_expectations.trigger_dag_id` (default orchestrator is `trigger_test_dags`).

## Execution (`execution`)

- **`triggers[].dag_id`**: Omit to use `dag_expectations.trigger_dag_id` (same orchestrator DAG for every spec that omits it).
- **`unpause_dag_ids`**: Omit to unpause every distinct `dag_id` appearing in `triggers`. Set to `[]` to skip unpausing. Otherwise list DAG ids to unpause before triggers.
- Per trigger spec, total DAG runs triggered via API are `parallelism * repetitions`. Each triggered DAG can start more DAG runs, e.g for `trigger_test_dags` it's configured as **`conf.dag_num`**, so expect about **`parallelism * repetitions * dag_num`** **child**(`test_dag_*`) DAG runs. Total runs are those API triggered runs plus children runs.
- **`max_trigger_in_flight`**: Optional cap on concurrent `POST .../dagRuns` (integer ≥ 1). Omit for unlimited concurrency (all triggers in a spec still use one `gather`, but each spec respects the semaphore).
- **`wait.batch_poll_dag_runs`**: Default `true` — uses `GET /dags/{id}/dagRuns` (paginated) per DAG instead of one GET per run while polling.
- **`wait.list_dag_runs_page_limit`**: Page size for that list (1–500, default 100).
- **`wait.observe_task_concurrency`**: Default `true` — polls task instance states to populate `peak_task_running`. Set `false` to reduce API load (peaks stay at 0).
- **`wait.include_triggered_children`**: Default `false` — when triggering `trigger_test_dags`, also observe derived child runs (`test_dag_*`) so pass/fail and concurrency include both orchestrator and children.
- **`wait.poll_jitter_fraction`**: Same as sync, for the execution wait loop.

## Report

- **`http_metrics`**: Per-operation latency (p50/p95 on successes), status histograms, retry counts.
- **`phases`**: `sync_and_import` detail may include `revision_marker_matched`, `revision_marker_description_preview`, `sync_lag_since_push_sec` (after `git_publish`, when push time was recorded), and `marker_observed_wall_time_iso`. The `execution` phase `detail` includes `task_instances_total`, `task_instances_non_skipped_total`, `task_instances_skipped_total`, `task_instances_by_state`, `task_instances_by_dag_run`, and `child_dag_execution_time_sec` (final counts for observed DAG runs).
- **`observed_dag_runs`**: DAG runs tracked in execution wait (direct triggers plus optional derived children).
- **`peak_dag_runs_running` / `peak_task_running`**: Peaks sampled during the execution wait loop.
- **`airflow_target.http_pool` / `http_retries`**: Effective client settings.

## Examples

See `perf_configs/example.cloudera-oauth.json` example for AWC Console.
