# flake8: noqa: I251, T201
"""Benchmark: baseline (unbounded buffer) vs optimized (bounded buffer).

Every scenario runs BOTH modes against a freshly seeded but identical local
service with identical concurrency settings (one reader thread, one writer
thread, same page size, same writer delay). Reports peak memory (tracemalloc),
buffer occupancy, wait times and actual completed counts — not just runtime.

Run:  python -m tools.incremental_extract.benchmark [--out PATH]
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from typing import Any, Dict, List, Optional

from tools.incremental_extract.pipeline import RunConfig, run_pipeline
from tools.incremental_extract.server import PaginatedService, serve_in_thread
from tools.incremental_extract.store import ResultStore

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

SCENARIOS: List[Dict[str, Any]] = [
    {
        "name": "large-pages",
        "n_records": 20_000,
        "limit": 2_000,
        "overlap": 50,
        "writer_delay": 0.002,
        "correction_every_pages": 0,
    },
    {
        "name": "sparse-corrections",
        "n_records": 10_000,
        "limit": 500,
        "overlap": 5,
        "writer_delay": 0.005,
        "correction_every_pages": 7,
    },
    {
        "name": "slow-writer",
        "n_records": 10_000,
        "limit": 200,
        "overlap": 5,
        "writer_delay": 0.05,
        "correction_every_pages": 0,
    },
    {
        "name": "mid-run-restart",
        "n_records": 10_000,
        "limit": 500,
        "overlap": 5,
        "writer_delay": 0.005,
        "correction_every_pages": 7,
        "crash_after_confirm_page": 5,
    },
]


def _run_mode(scenario: Dict[str, Any], mode: str, work_root: str) -> Dict[str, Any]:
    """Run one mode of a scenario against a fresh, identically seeded service."""
    service = PaginatedService(
        n_records=int(scenario["n_records"]),
        overlap=int(scenario["overlap"]),
        correction_every_pages=int(scenario["correction_every_pages"]),
    )
    server, url = serve_in_thread(service)
    work = os.path.join(work_root, "%s-%s" % (scenario["name"], mode))
    try:
        crash_page = scenario.get("crash_after_confirm_page")
        legs: List[Dict[str, object]] = []
        if crash_page:
            # leg 1: hard-killed subprocess after confirming page N
            started = time.monotonic()
            proc = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tools.incremental_extract.pipeline",
                    "--pipeline-id",
                    "bench",
                    "--base-url",
                    url,
                    "--working-dir",
                    work,
                    "--limit",
                    str(scenario["limit"]),
                    "--mode",
                    mode,
                    "--writer-delay",
                    str(scenario["writer_delay"]),
                    "--crash-at",
                    "after_confirm:%d" % crash_page,
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
            )
            legs.append(
                {
                    "leg": "killed",
                    "exit_code": proc.returncode,
                    "wall_seconds": time.monotonic() - started,
                }
            )
            # leg 2: resume to completion
            report = run_pipeline(
                RunConfig(
                    pipeline_id="bench",
                    base_url=url,
                    working_dir=work,
                    limit=int(scenario["limit"]),
                    mode=mode,
                    writer_delay=float(scenario["writer_delay"]),
                )
            )
            legs.append({"leg": "resumed", **report.to_dict()})
            final = report
        else:
            final = run_pipeline(
                RunConfig(
                    pipeline_id="bench",
                    base_url=url,
                    working_dir=work,
                    limit=int(scenario["limit"]),
                    mode=mode,
                    writer_delay=float(scenario["writer_delay"]),
                    reset_checkpoint=True,
                )
            )
        store = ResultStore(os.path.join(work, "results.sqlite3"))
        try:
            rows = store.read_results("bench")
        finally:
            store.close()
        consistent = sorted((r["id"], r["version"], r["value"]) for r in rows) == sorted(
            (r["id"], r["version"], r["value"]) for r in service.final_state()
        )
        return {
            "mode": mode,
            "report": final.to_dict(),
            "legs": legs,
            "matches_source": consistent,
        }
    finally:
        server.shutdown()


def _row(scenario: str, result: Dict[str, Any]) -> str:
    r = result["report"]
    return "%-18s %-10s %8.3f %10.2f %9d %11d %9d %9d %9d %9d %10.3f %10.3f  %s" % (
        scenario,
        result["mode"],
        r["wall_seconds"],
        r["peak_memory_bytes"] / 1e6,
        r["buffer_peak_pages"],
        r["buffer_peak_records"],
        r["records_read"],
        r["duplicates_dropped"],
        r["late_applied"],
        r["rows_final"],
        r["reader_blocked_seconds"],
        r["writer_wait_seconds"],
        "OK" if result["matches_source"] else "MISMATCH",
    )


HEADER = "%-18s %-10s %8s %10s %9s %11s %9s %9s %9s %9s %10s %10s  %s" % (
    "scenario",
    "mode",
    "wall_s",
    "peak_mem_MB",
    "peak_pages",
    "peak_records",
    "recs_read",
    "dups",
    "late",
    "rows",
    "rd_block_s",
    "wr_wait_s",
    "source=",
)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="baseline vs optimized extraction benchmark")
    parser.add_argument(
        "--out",
        default=os.path.join("_storage", "incremental_extract", "benchmark.json"),
        help="where to write the JSON report",
    )
    parser.add_argument(
        "--scenario",
        default=None,
        help="run only the named scenario",
    )
    args = parser.parse_args(argv)

    scenarios = [s for s in SCENARIOS if args.scenario in (None, s["name"])]
    if not scenarios:
        print("no matching scenario")
        return 1

    all_results: List[Dict[str, Any]] = []
    print(HEADER)
    ok = True
    with tempfile.TemporaryDirectory(prefix="incr-bench-") as tmp:
        for scenario in scenarios:
            for mode in ("baseline", "optimized"):
                result = _run_mode(scenario, mode, tmp)
                result["scenario"] = scenario["name"]
                all_results.append(result)
                print(_row(scenario["name"], result))
                report = result["report"]
                if report["status"] != "completed" or not result["matches_source"]:
                    ok = False
            # paired comparison for the scenario
            base, opt = all_results[-2], all_results[-1]
            b, o = base["report"], opt["report"]
            print(
                "%-18s %-10s wall %+6.1f%%  peak_mem %+6.1f%%  rows %d -> %d"
                % (
                    scenario["name"],
                    "delta",
                    100.0 * (o["wall_seconds"] - b["wall_seconds"]) / max(b["wall_seconds"], 1e-9),
                    100.0
                    * (o["peak_memory_bytes"] - b["peak_memory_bytes"])
                    / max(b["peak_memory_bytes"], 1.0),
                    b["rows_final"],
                    o["rows_final"],
                )
            )

    payload = {
        "scenarios": scenarios,
        "results": all_results,
        "note": "same data, same concurrency; only the buffer bound differs",
    }
    out_dir = os.path.dirname(os.path.abspath(args.out))
    os.makedirs(out_dir, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    print("report written to %s" % args.out)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
