# flake8: noqa: T201
"""Correctness checks for the incremental extraction pipeline.

Covers: page-boundary duplicates, sparse late corrections, same-timestamp
records, mid-run restart (hard kill at three different points), checkpoint
param mismatch, single-executor enforcement and cross-pipeline isolation.

Run:  python -m tools.incremental_extract.verify
Exit code is non-zero if any check fails.
"""

import os
import subprocess
import sys
import tempfile
from typing import Any, Callable, Dict, List, Tuple

from tools.incremental_extract.pipeline import RunConfig, run_pipeline
from tools.incremental_extract.server import PaginatedService, serve_in_thread
from tools.incremental_extract.store import PipelineLock, ResultStore

CheckResult = Tuple[str, bool, str]

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def _key(records: List[Dict[str, Any]]) -> List[Tuple[Any, Any, Any]]:
    return sorted((r["id"], r["version"], r["value"]) for r in records)


def check_full_vs_incremental(tmp: str) -> CheckResult:
    """An incremental run that receives corrections mid-flight produces the
    same result as a complete (folded) full read and the source final state."""
    name = "full_vs_incremental"
    service = PaginatedService(n_records=2000, overlap=5, correction_every_pages=3, plan_limit=200)
    server, url = serve_in_thread(service)
    try:
        incr = run_pipeline(
            RunConfig("incr", url, os.path.join(tmp, "a"), limit=200, reset_checkpoint=True)
        )
        full = run_pipeline(
            RunConfig(
                "full",
                url,
                os.path.join(tmp, "a"),
                limit=200,
                reset_checkpoint=True,
                fold_corrections=True,
            )
        )
        if incr.status != "completed" or full.status != "completed":
            return name, False, "runs failed: %s / %s" % (incr.error, full.error)
        if full.late_received != 0:
            return name, False, "folded full read unexpectedly received late records"
        store = ResultStore(os.path.join(tmp, "a", "results.sqlite3"))
        try:
            incr_rows = store.read_results("incr")
            full_rows = store.read_results("full")
        finally:
            store.close()
        expected = _key(service.final_state())
        if _key(incr_rows) != expected:
            return name, False, "incremental result differs from source final state"
        if _key(full_rows) != expected:
            return name, False, "full refresh differs from source final state"
        if incr.late_applied <= 0:
            return name, False, "no late corrections were applied during incremental run"
        if incr.duplicates_dropped <= 0:
            return name, False, "no page-boundary duplicates were observed/dropped"
        return (
            name,
            True,
            "incremental == full == source; late=%d dups=%d"
            % (
                incr.late_applied,
                incr.duplicates_dropped,
            ),
        )
    finally:
        server.shutdown()


def check_same_timestamp_records(tmp: str) -> CheckResult:
    """Records sharing one updated_at must all survive (no last-one-wins)."""
    name = "same_timestamp_records_kept"
    group = 4
    service = PaginatedService(n_records=1000, same_ts_group=group, correction_every_pages=0)
    server, url = serve_in_thread(service)
    try:
        report = run_pipeline(
            RunConfig("ts", url, os.path.join(tmp, "b"), limit=250, reset_checkpoint=True)
        )
        store = ResultStore(os.path.join(tmp, "b", "results.sqlite3"))
        try:
            rows = store.read_results("ts")
        finally:
            store.close()
        if report.status != "completed":
            return name, False, "run failed: %s" % report.error
        if len(rows) != 1000:
            return name, False, "expected 1000 rows, got %d" % len(rows)
        by_ts: Dict[int, int] = {}
        for row in rows:
            by_ts[row["updated_at"]] = by_ts.get(row["updated_at"], 0) + 1
        bad = {ts: n for ts, n in by_ts.items() if n != group}
        if bad:
            return name, False, "timestamp groups collapsed: %s" % list(bad.items())[:3]
        return name, True, "all %d same-timestamp groups intact" % len(by_ts)
    finally:
        server.shutdown()


def check_mid_run_restart(tmp: str) -> CheckResult:
    """Hard kill after page 3 confirm; checkpoint must name page/cursor/params;
    resume must finish with no duplicates and the same final state."""
    name = "mid_run_restart"
    service = PaginatedService(n_records=2000, overlap=5, correction_every_pages=4, plan_limit=200)
    server, url = serve_in_thread(service)
    work = os.path.join(tmp, "c")
    try:
        env = dict(os.environ)
        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "tools.incremental_extract.pipeline",
                "--pipeline-id",
                "restart",
                "--base-url",
                url,
                "--working-dir",
                work,
                "--limit",
                "200",
                "--crash-at",
                "after_confirm:3",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            env=env,
        )
        if proc.returncode == 0:
            return name, False, "crashed run unexpectedly exited 0"
        store = ResultStore(os.path.join(work, "results.sqlite3"))
        try:
            cp = store.get_checkpoint("restart")
        finally:
            store.close()
        if cp is None:
            return name, False, "no checkpoint after crash"
        if cp["last_confirmed_page"] != 3:
            return name, False, "checkpoint page %s != 3" % cp["last_confirmed_page"]
        if not cp["cursor_ts"] or not cp["cursor_id"] or not cp["params_digest"]:
            return name, False, "checkpoint missing cursor or params digest: %s" % cp
        # resume in-process
        resumed = run_pipeline(RunConfig("restart", url, work, limit=200))
        if resumed.status != "completed":
            return name, False, "resume failed: %s" % resumed.error
        if resumed.resumed_from_page != 4:
            return name, False, "resumed from page %d, expected 4" % resumed.resumed_from_page
        store = ResultStore(os.path.join(work, "results.sqlite3"))
        try:
            rows = store.read_results("restart")
        finally:
            store.close()
        if _key(rows) != _key(service.final_state()):
            return name, False, "resumed result differs from source final state"
        ids = [r["id"] for r in rows]
        if len(ids) != len(set(ids)):
            return name, False, "duplicate rows after restart"
        return name, True, "killed after page 3, resumed from page 4, %d rows" % len(rows)
    finally:
        server.shutdown()


def check_crash_windows(tmp: str) -> CheckResult:
    """Kills in the other two windows (after read, during write) also resume
    to the exact final state."""
    name = "crash_windows_resume"
    for kind in ("after_read", "during_write"):
        service = PaginatedService(
            n_records=1200, overlap=3, correction_every_pages=2, plan_limit=200
        )
        server, url = serve_in_thread(service)
        work = os.path.join(tmp, "cw-%s" % kind)
        try:
            proc = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tools.incremental_extract.pipeline",
                    "--pipeline-id",
                    "cw",
                    "--base-url",
                    url,
                    "--working-dir",
                    work,
                    "--limit",
                    "200",
                    "--crash-at",
                    "%s:2" % kind,
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
            )
            if proc.returncode == 0:
                return name, False, "%s: crashed run exited 0" % kind
            resumed = run_pipeline(RunConfig("cw", url, work, limit=200))
            if resumed.status != "completed":
                return name, False, "%s: resume failed: %s" % (kind, resumed.error)
            store = ResultStore(os.path.join(work, "results.sqlite3"))
            try:
                rows = store.read_results("cw")
            finally:
                store.close()
            if _key(rows) != _key(service.final_state()):
                return name, False, "%s: result differs after resume" % kind
        finally:
            server.shutdown()
    return name, True, "after_read and during_write kills both resumed cleanly"


def check_checkpoint_mismatch(tmp: str) -> CheckResult:
    """Changed filter params must be refused unless reset=True rebuilds."""
    name = "checkpoint_param_mismatch"
    service = PaginatedService(n_records=1000, correction_every_pages=0)
    server, url = serve_in_thread(service)
    work = os.path.join(tmp, "d")
    try:
        first = run_pipeline(RunConfig("p", url, work, limit=200, reset_checkpoint=True))
        if first.status != "completed":
            return name, False, "initial run failed: %s" % first.error
        refused = run_pipeline(RunConfig("p", url, work, limit=200, category="even"))
        if refused.status != "failed" or "CheckpointMismatchError" not in (refused.error or ""):
            return name, False, "mismatched params were not refused: %s" % refused.error
        rebuilt = run_pipeline(
            RunConfig("p", url, work, limit=200, category="even", reset_checkpoint=True)
        )
        if rebuilt.status != "completed":
            return name, False, "rebuild failed: %s" % rebuilt.error
        if rebuilt.rows_final != 500:
            return name, False, "rebuilt rows %d != 500 (even category)" % rebuilt.rows_final
        return name, True, "mismatch refused; reset rebuilt 500 filtered rows"
    finally:
        server.shutdown()


def check_single_executor(tmp: str) -> CheckResult:
    """A second executor for the same checkpoint must be rejected."""
    name = "single_executor"
    service = PaginatedService(n_records=100, correction_every_pages=0)
    server, url = serve_in_thread(service)
    work = os.path.join(tmp, "e")
    try:
        os.makedirs(work, exist_ok=True)
        with PipelineLock(work, "locked"):
            report = run_pipeline(RunConfig("locked", url, work, limit=50))
        if report.status != "failed" or "ExecutorConflictError" not in (report.error or ""):
            return name, False, "concurrent executor not rejected: %s" % report.error
        return name, True, "second executor rejected while lock held"
    finally:
        server.shutdown()


def check_cross_pipeline_isolation(tmp: str) -> CheckResult:
    """Dedup state of one pipeline must not filter another pipeline's rows."""
    name = "cross_pipeline_isolation"
    service = PaginatedService(n_records=800, correction_every_pages=0)
    server, url = serve_in_thread(service)
    work = os.path.join(tmp, "f")
    try:
        a = run_pipeline(RunConfig("pipe-a", url, work, limit=200, reset_checkpoint=True))
        b = run_pipeline(RunConfig("pipe-b", url, work, limit=200, reset_checkpoint=True))
        if a.status != "completed" or b.status != "completed":
            return name, False, "runs failed: %s / %s" % (a.error, b.error)
        store = ResultStore(os.path.join(work, "results.sqlite3"))
        try:
            na = store.count_results("pipe-a")
            nb = store.count_results("pipe-b")
        finally:
            store.close()
        if na != 800 or nb != 800:
            return name, False, "row counts a=%d b=%d, expected 800 each" % (na, nb)
        return name, True, "both pipelines hold their own 800 rows"
    finally:
        server.shutdown()


def check_unconfirmed_pages_identified(tmp: str) -> CheckResult:
    """After a kill, pages read but not confirmed are identifiable."""
    name = "unconfirmed_pages_identified"
    service = PaginatedService(n_records=2000, correction_every_pages=0)
    server, url = serve_in_thread(service)
    work = os.path.join(tmp, "g")
    try:
        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "tools.incremental_extract.pipeline",
                "--pipeline-id",
                "uc",
                "--base-url",
                url,
                "--working-dir",
                work,
                "--limit",
                "100",
                "--buffer-size",
                "4",
                "--crash-at",
                "after_read:6",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
        )
        if proc.returncode == 0:
            return name, False, "crashed run exited 0"
        store = ResultStore(os.path.join(work, "results.sqlite3"))
        try:
            cp = store.get_checkpoint("uc")
        finally:
            store.close()
        if cp is None:
            return name, False, "no checkpoint row after crash"
        confirmed = int(cp["last_confirmed_page"])
        read = int(cp["last_read_page"])
        # the killed run crashed after reading page 6: the gap (confirmed, read]
        # must identify pages that were read but never confirmed
        if read < 6:
            return name, False, "last_read_page %d < 6 after after_read:6 kill" % read
        if confirmed >= read:
            return name, False, "no read-but-unconfirmed gap: %s" % cp
        gap = list(range(confirmed + 1, read + 1))
        if any(p <= confirmed for p in gap):
            return name, False, "gap overlaps confirmed pages: %s" % cp
        return (
            name,
            True,
            "confirmed up to page %d, read-but-unconfirmed=%s"
            % (
                confirmed,
                gap,
            ),
        )
    finally:
        server.shutdown()


ALL_CHECKS: List[Callable[[str], CheckResult]] = [
    check_full_vs_incremental,
    check_same_timestamp_records,
    check_mid_run_restart,
    check_crash_windows,
    check_checkpoint_mismatch,
    check_single_executor,
    check_cross_pipeline_isolation,
    check_unconfirmed_pages_identified,
]


def main() -> int:
    failures = 0
    with tempfile.TemporaryDirectory(prefix="incr-verify-") as tmp:
        for check in ALL_CHECKS:
            name, ok, detail = check(tmp)
            print("%s %-32s %s" % ("PASS" if ok else "FAIL", name, detail))
            if not ok:
                failures += 1
    print("verify: %d/%d checks passed" % (len(ALL_CHECKS) - failures, len(ALL_CHECKS)))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
