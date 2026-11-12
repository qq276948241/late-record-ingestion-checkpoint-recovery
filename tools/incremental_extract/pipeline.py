# flake8: noqa: I251, T201
"""Incremental extractor with baseline and optimized buffering modes.

Both modes run one reader thread (sequential cursor pagination) and one writer
thread with identical concurrency settings. The ONLY difference is the buffer
between them:

* ``baseline``  — unbounded queue: a slow writer lets pages pile up in memory;
* ``optimized`` — bounded queue: once full, the reader blocks, propagating
  backpressure to the fetch loop instead of accumulating memory.

Cancellation/write failure sets a stop event that the reader checks before
every page fetch, so pagination stops promptly; pages already read but not yet
confirmed remain identifiable via the in-flight registry and are persisted
into the checkpoint row.
"""

import json
import os
import queue
import threading
import time
import tracemalloc
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from tools.incremental_extract.store import (
    PipelineLock,
    ResultStore,
    note_page_read,
    params_digest,
)

Sentinel = None


@dataclass
class RunConfig:
    pipeline_id: str
    base_url: str
    working_dir: str
    limit: int = 500
    category: Optional[str] = None
    fold_corrections: bool = False  # ask the source for final versions (full read)
    mode: str = "optimized"  # "baseline" | "optimized"
    buffer_size: int = 8  # max pages buffered in optimized mode
    writer_delay: float = 0.0  # seconds per page, simulates a slow writer
    reset_checkpoint: bool = False
    # crash simulation (used by the restart scenario, via the CLI):
    # ("after_read" | "during_write" | "after_confirm", page_number)
    crash_at: Optional[Tuple[str, int]] = None
    measure_memory: bool = True


@dataclass
class RunReport:
    pipeline_id: str
    mode: str
    status: str = "completed"  # completed | failed
    error: Optional[str] = None
    pages_read: int = 0
    pages_confirmed: int = 0
    records_read: int = 0
    duplicates_dropped: int = 0
    late_received: int = 0
    late_applied: int = 0
    rows_inserted: int = 0
    rows_final: int = 0
    buffer_peak_pages: int = 0
    buffer_peak_records: int = 0
    reader_blocked_seconds: float = 0.0  # time the reader spent on a full buffer
    writer_wait_seconds: float = 0.0  # time the writer spent waiting for pages
    wall_seconds: float = 0.0
    peak_memory_bytes: int = 0  # tracemalloc peak (Python allocations)
    unconfirmed_pages: List[int] = field(default_factory=list)
    resumed_from_page: int = 0
    checkpoint: Optional[Dict[str, object]] = None

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


def _fetch_page(
    base_url: str,
    cursor_ts: Optional[int],
    cursor_id: Optional[str],
    limit: int,
    category: Optional[str],
    fold_corrections: bool = False,
) -> Dict[str, Any]:
    query = "limit=%d" % limit
    if cursor_ts is not None and cursor_id is not None:
        query += "&cursor_ts=%d&cursor_id=%s" % (cursor_ts, urllib.parse.quote(cursor_id))
    if category:
        query += "&category=%s" % urllib.parse.quote(category)
    if fold_corrections:
        query += "&fold=1"
    with urllib.request.urlopen("%s/records?%s" % (base_url, query), timeout=30) as resp:
        data: Dict[str, Any] = json.loads(resp.read().decode("utf-8"))
    return data


class _InFlight:
    """Registry of pages that were read but not yet confirmed."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pages: Dict[int, int] = {}  # page_no -> record count
        self.peak_pages = 0
        self.peak_records = 0

    def add(self, page_no: int, n_records: int) -> None:
        with self._lock:
            self._pages[page_no] = n_records
            self.peak_pages = max(self.peak_pages, len(self._pages))
            self.peak_records = max(self.peak_records, sum(self._pages.values()))

    def confirm(self, page_no: int) -> None:
        with self._lock:
            self._pages.pop(page_no, None)

    def pending(self) -> List[int]:
        with self._lock:
            return sorted(self._pages)


def run_pipeline(config: RunConfig) -> RunReport:
    report = RunReport(pipeline_id=config.pipeline_id, mode=config.mode)
    started = time.monotonic()
    if config.measure_memory:
        tracemalloc.start()
    store: Optional[ResultStore] = None
    try:
        with PipelineLock(config.working_dir, config.pipeline_id):
            store = ResultStore(os.path.join(config.working_dir, "results.sqlite3"))
            filters: Dict[str, Any] = {}
            if config.category:
                filters["category"] = config.category
            digest = params_digest(config.base_url, config.limit, filters)
            checkpoint = store.check_or_create_checkpoint(
                config.pipeline_id, digest, reset=config.reset_checkpoint
            )
            cursor_ts = checkpoint["cursor_ts"]
            cursor_id = checkpoint["cursor_id"]
            next_page = int(checkpoint["last_confirmed_page"]) + 1
            report.resumed_from_page = next_page

            buffer: "queue.Queue[Any]" = (
                queue.Queue()
                if config.mode == "baseline"
                else queue.Queue(maxsize=config.buffer_size)
            )
            stop_event = threading.Event()
            in_flight = _InFlight()
            reader_error: List[BaseException] = []

            def reader() -> None:
                page_no = next_page
                ts, cid = cursor_ts, cursor_id
                try:
                    while not stop_event.is_set():
                        page = _fetch_page(
                            config.base_url,
                            ts,
                            cid,
                            config.limit,
                            config.category,
                            config.fold_corrections,
                        )
                        if stop_event.is_set():
                            break
                        n_records = len(page["records"]) + len(page["late"])
                        in_flight.add(page_no, n_records)
                        note_page_read(store.db_path, config.pipeline_id, page_no)
                        if (
                            config.crash_at
                            and config.crash_at[0] == "after_read"
                            and config.crash_at[1] == page_no
                        ):
                            os._exit(1)  # hard kill: page read, nothing written
                        put_start = time.monotonic()
                        buffer.put((page_no, page))
                        report.reader_blocked_seconds += time.monotonic() - put_start
                        if not page["has_more"] or page["next_cursor"] is None:
                            break
                        nxt = page["next_cursor"]
                        ts, cid = nxt["ts"], nxt["id"]
                        page_no += 1
                except BaseException as exc:  # noqa: BLE001, B036
                    if not stop_event.is_set():
                        reader_error.append(exc)
                finally:
                    buffer.put(Sentinel)

            reader_thread = threading.Thread(target=reader, name="page-reader", daemon=True)
            reader_thread.start()

            try:
                while True:
                    get_start = time.monotonic()
                    item = buffer.get()
                    report.writer_wait_seconds += time.monotonic() - get_start
                    if item is Sentinel:
                        break
                    page_no, page = item
                    records = list(page["records"]) + list(page["late"])
                    late_ids = {r["id"] for r in page["late"]}
                    report.pages_read += 1
                    report.records_read += len(records)
                    report.late_received += len(late_ids)
                    if (
                        config.crash_at
                        and config.crash_at[0] == "during_write"
                        and config.crash_at[1] == page_no
                    ):
                        os._exit(1)  # hard kill inside the write window
                    if config.writer_delay:
                        time.sleep(config.writer_delay)
                    nxt = page["next_cursor"]
                    counts = store.confirm_page(
                        config.pipeline_id,
                        page_no,
                        records,
                        cursor_ts=nxt["ts"] if nxt else None,
                        cursor_id=nxt["id"] if nxt else None,
                        late_ids=late_ids,
                    )
                    if (
                        config.crash_at
                        and config.crash_at[0] == "after_confirm"
                        and config.crash_at[1] == page_no
                    ):
                        os._exit(1)  # hard kill right after the confirming commit
                    report.pages_confirmed += 1
                    report.rows_inserted += counts["inserted"]
                    report.late_applied += counts["late_applied"]
                    report.duplicates_dropped += counts["duplicates"]
                    in_flight.confirm(page_no)
            except BaseException as exc:  # noqa: BLE001, B036
                stop_event.set()
                report.status = "failed"
                report.error = "%s: %s" % (type(exc).__name__, exc)
            finally:
                stop_event.set()
                reader_thread.join(timeout=30)
                report.unconfirmed_pages = in_flight.pending()
                if report.unconfirmed_pages or report.status != "completed":
                    store.mark_unconfirmed(
                        config.pipeline_id,
                        report.unconfirmed_pages,
                        "completed" if report.status == "completed" else "interrupted",
                    )
                if reader_error and report.status == "completed":
                    report.status = "failed"
                    report.error = "reader: %s: %s" % (
                        type(reader_error[0]).__name__,
                        reader_error[0],
                    )
            report.buffer_peak_pages = in_flight.peak_pages
            report.buffer_peak_records = in_flight.peak_records
            report.rows_final = store.count_results(config.pipeline_id)
            report.checkpoint = store.get_checkpoint(config.pipeline_id)
    except BaseException as exc:  # noqa: BLE001, B036
        report.status = "failed"
        report.error = "%s: %s" % (type(exc).__name__, exc)
    finally:
        if store is not None:
            store.close()
        report.wall_seconds = time.monotonic() - started
        if config.measure_memory:
            _, peak = tracemalloc.get_traced_memory()
            report.peak_memory_bytes = peak
            tracemalloc.stop()
    return report


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry: run a single extraction and print the report as JSON.

    Used directly and by the benchmark's restart scenario (a subprocess that
    is hard-killed mid-run and then resumed).
    """
    import argparse

    parser = argparse.ArgumentParser(description="run one incremental extraction")
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--working-dir", required=True)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--category", default=None)
    parser.add_argument("--mode", choices=["baseline", "optimized"], default="optimized")
    parser.add_argument("--buffer-size", type=int, default=8)
    parser.add_argument("--writer-delay", type=float, default=0.0)
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--crash-at", default=None, help="KIND:PAGE, e.g. after_confirm:5")
    args = parser.parse_args(argv)

    crash_at = None
    if args.crash_at:
        kind, _, page = args.crash_at.partition(":")
        crash_at = (kind, int(page))
    config = RunConfig(
        pipeline_id=args.pipeline_id,
        base_url=args.base_url,
        working_dir=args.working_dir,
        limit=args.limit,
        category=args.category,
        mode=args.mode,
        buffer_size=args.buffer_size,
        writer_delay=args.writer_delay,
        reset_checkpoint=args.reset,
        crash_at=crash_at,
    )
    report = run_pipeline(config)
    print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    return 0 if report.status == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
