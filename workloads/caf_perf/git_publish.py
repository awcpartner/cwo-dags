"""Rewrite the caf-perf marker block in DAG source, commit, and push to the configured branch."""

from __future__ import annotations

import json
import logging
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from secrets import token_hex

from caf_perf.config import ScenarioConfig

LOGGER = logging.getLogger(__name__)

_CAF_PERF_BLOCK_START = "# --- caf-perf sync marker block ---"
_CAF_PERF_BLOCK_END = "# --- end caf-perf sync marker block ---"


def _inject_caf_perf_marker_block(source: str, marker: str, push_iso: str) -> str:
    """Replace the delimited constant block in a DAG source file."""
    lines = source.splitlines(keepends=True)
    out: list[str] = []
    i = 0
    replaced = False
    while i < len(lines):
        if lines[i].strip() == _CAF_PERF_BLOCK_START:
            replaced = True
            out.append(f"{_CAF_PERF_BLOCK_START}\n")
            out.append(f"_CAF_PERF_SYNC_MARKER_VALUE = {json.dumps(marker)}\n")
            out.append(f"_CAF_PERF_PUSH_TIME_ISO = {json.dumps(push_iso)}\n")
            out.append(f"{_CAF_PERF_BLOCK_END}\n")
            i += 1
            while i < len(lines) and lines[i].strip() != _CAF_PERF_BLOCK_END:
                i += 1
            if i >= len(lines):
                raise RuntimeError(
                    f"git_publish: {_CAF_PERF_BLOCK_START!r} without matching "
                    f"{_CAF_PERF_BLOCK_END!r} in sync.git_publish.marker_file_relative"
                )
            i += 1
        else:
            out.append(lines[i])
            i += 1
    if not replaced:
        raise RuntimeError(
            "git_publish: sync.git_publish.marker_file_relative must point to a Python "
            f"file that contains the block {_CAF_PERF_BLOCK_START!r} … "
            f"{_CAF_PERF_BLOCK_END!r} (see workloads/dags/test_dags.py)"
        )
    return "".join(out)


@dataclass
class GitPublishState:
    repo_path: str
    remote: str
    branch: str
    marker: str
    push_time_iso: str
    marker_file_relative: str
    autostash: bool


def _git(
    repo: Path,
    *args: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        check=check,
        capture_output=True,
        text=True,
    )


def _maybe_autostash(repo: Path, *, stash_local_changes: bool) -> bool:
    """Return True if a stash was created (``git stash push -u``); else False if already clean."""
    r = _git(repo, "status", "--porcelain")
    dirty = r.stdout
    if not dirty.strip():
        return False
    if not stash_local_changes:
        raise RuntimeError(
            "git_publish requires a clean working tree in "
            f'{repo} (commit or stash, or set "sync.git_publish.stash_local_changes": true). '
            "Current status:\n" + dirty[:4000]
        )
    LOGGER.info(
        "git_publish: working tree not clean; stashing tracked and untracked changes"
    )
    _git(
        repo,
        "stash",
        "push",
        "-u",
        "-m",
        "caf-perf autostash (sync.git_publish)",
    )
    r2 = _git(repo, "status", "--porcelain")
    if r2.stdout.strip():
        rr = _git(repo, "stash", "pop", check=False)
        if rr.returncode != 0:
            LOGGER.warning(
                "git_publish: stash pop after failed clean returned rc=%s: %s",
                rr.returncode,
                (rr.stderr or rr.stdout or "").strip(),
            )
        raise RuntimeError(
            "git_publish: working tree is still not clean after stashing "
            f"(e.g. skip-worktree or paths git refuses to stash). Fix in {repo}:\n"
            + r2.stdout[:4000]
        )
    return True


def prepare_git_publish(cfg: ScenarioConfig) -> GitPublishState | None:
    gp = cfg.sync.git_publish
    if not gp.enabled:
        return None

    repo = Path(gp.repo_path).expanduser().resolve()
    if not repo.is_dir():
        raise RuntimeError(f"git_publish.repo_path is not a directory: {repo}")

    _git(repo, "rev-parse", "--is-inside-work-tree")

    marker_rel = gp.marker_file_relative.strip().lstrip("/")
    dag_path = repo / marker_rel
    if not dag_path.is_file():
        raise RuntimeError(
            f"git_publish: sync.git_publish.marker_file_relative must be an existing file: {dag_path}"
        )

    stashed = _maybe_autostash(repo, stash_local_changes=gp.stash_local_changes)

    remote = gp.remote
    branch = gp.branch
    marker = token_hex(16)

    push_time = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    LOGGER.info(
        "git_publish: fetch %s, checkout %s, inject marker into %s",
        remote,
        branch,
        marker_rel,
    )

    head_before: str | None = None

    def _abort() -> None:
        if head_before is None:
            return
        try:
            _git(repo, "reset", "--hard", head_before)
        except subprocess.CalledProcessError:
            LOGGER.warning("git_publish: could not reset to pre-marker commit")

    try:
        _git(repo, "fetch", remote)
        _git(repo, "checkout", branch)
        _git(repo, "pull", "--ff-only", remote, branch)
        head_before = _git(repo, "rev-parse", "HEAD").stdout.strip()
        text = dag_path.read_text(encoding="utf-8")
        dag_path.write_text(
            _inject_caf_perf_marker_block(text, marker, push_time),
            encoding="utf-8",
        )
        _git(repo, "add", "--", marker_rel)
        _git(repo, "commit", "-m", f"caf-perf sync marker {marker[:12]}...")
        _git(repo, "push", remote, branch)
    except subprocess.CalledProcessError as exc:
        err = (exc.stderr or exc.stdout or "").strip()
        LOGGER.error("git_publish failed: %s", err or exc)
        _abort()
        raise RuntimeError(f"git_publish failed: {err or exc}") from exc
    except RuntimeError as exc:
        LOGGER.error("git_publish failed: %s", exc)
        _abort()
        raise
    else:
        if (cfg.sync.revision_marker or "").strip():
            LOGGER.warning(
                "git_publish: overwriting existing sync.revision_marker with generated marker"
            )
        cfg.sync.revision_marker = marker
        cfg.sync.push_time_iso = push_time

        LOGGER.info(
            "git_publish: pushed marker commit to %s/%s",
            remote,
            branch,
        )

        return GitPublishState(
            repo_path=str(repo),
            remote=remote,
            branch=branch,
            marker=marker,
            push_time_iso=push_time,
            marker_file_relative=marker_rel,
            autostash=stashed,
        )
    finally:
        if stashed:
            pr = _git(repo, "stash", "pop", check=False)
            if pr.returncode != 0:
                LOGGER.warning(
                    "git_publish: git stash pop failed (rc=%s). Your earlier changes may "
                    "still be on the stash stack; inspect with git stash list. stderr: %s",
                    pr.returncode,
                    (pr.stderr or pr.stdout or "").strip(),
                )
            else:
                LOGGER.info(
                    "git_publish: restored pre-run working tree changes (stash pop)"
                )
