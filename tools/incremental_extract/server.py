# flake8: noqa: I251
"""Local paginated HTTP service used as the extraction source.

The service deliberately exhibits two behaviors that the extractor must
handle without giving up correctness for throughput:

* page-boundary duplicates: each page is preceded by the last ``overlap``
  records of the previous page (same id, same version);
* late corrections: records whose cursor lies BEHIND the current cursor are
  re-sent with a bumped version and changed payload (sparse corrections).

Correction emission is a pure function of the requested page range, so
re-reading a page after a crash yields the same late records again — the
extractor's idempotent upsert, not the server, is responsible for dedup.

Records sharing the same ``updated_at`` are common; the composite cursor
``(updated_at, id)`` keeps them all addressable.
"""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple, Type
from urllib.parse import parse_qs, urlparse

from tools.incremental_extract.dataset import Record, correction_plan, generate_records


class PaginatedService:
    """Serves a deterministic dataset with duplicates and late corrections."""

    def __init__(
        self,
        n_records: int = 10_000,
        same_ts_group: int = 4,
        overlap: int = 3,
        correction_every_pages: int = 7,
        correction_lag_pages: int = 3,
        plan_limit: int = 500,
        fetch_delay: float = 0.0,
    ) -> None:
        self.records: List[Record] = generate_records(n_records, same_ts_group)
        self.overlap = overlap
        self.fetch_delay = fetch_delay
        self.plan_limit = plan_limit
        self.correction_every_pages = correction_every_pages
        self.correction_lag_pages = correction_lag_pages
        self.served_pages = 0

    def _filtered(self, category: Optional[str]) -> List[Record]:
        if not category:
            return self.records
        return [r for r in self.records if r["category"] == category]

    def _plan(self, n_items: int) -> List[Tuple[int, int]]:
        """(trigger_position, record_index) pairs for the current filter size."""
        return correction_plan(
            n_items, self.plan_limit, self.correction_every_pages, self.correction_lag_pages
        )

    @staticmethod
    def _version_at(record_index: int, end_position: int, plan: List[Tuple[int, int]]) -> int:
        """Version of a record as visible from a read reaching ``end_position``."""
        return 1 + sum(
            1 for trigger, idx in plan if idx == record_index and trigger <= end_position
        )

    @staticmethod
    def _materialize(record: Record, version: int) -> Record:
        out = dict(record)
        out["version"] = version
        if version > 1:
            # a correction changed the payload deterministically
            out["value"] = record["value"] + 1_000_000 * version
        return out

    def get_page(
        self,
        cursor_ts: Optional[int],
        cursor_id: Optional[str],
        limit: int,
        category: Optional[str] = None,
        fold_corrections: bool = False,
    ) -> Dict[str, Any]:
        if self.fetch_delay:
            time.sleep(self.fetch_delay)
        self.served_pages += 1
        items = self._filtered(category)
        start = 0
        if cursor_ts is not None and cursor_id is not None:
            # first position strictly after the cursor in (updated_at, id) order
            start = len(items)
            for pos, rec in enumerate(items):
                if (rec["updated_at"], rec["id"]) > (cursor_ts, cursor_id):
                    start = pos
                    break
        page = items[start : start + limit]
        end = start + len(page)
        has_more = end < len(items)
        plan = self._plan(len(items))
        # when corrections are folded in, every record is served at its final
        # version and no late records are emitted (a "complete" read)
        version_horizon = len(items) if fold_corrections else end

        # page-boundary duplicates: repeat the tail of the previous page
        dup_start = max(0, start - self.overlap)
        out_records = [
            self._materialize(items[i], self._version_at(i, version_horizon, plan))
            for i in range(dup_start, end)
        ]
        # late corrections triggered by this page range: records behind the
        # cursor, re-sent with their bumped version
        late = (
            []
            if fold_corrections
            else [
                self._materialize(items[idx], self._version_at(idx, end, plan))
                for trigger, idx in plan
                if start < trigger <= end
            ]
        )

        next_cursor = None
        if page:
            last = page[-1]
            next_cursor = {"ts": last["updated_at"], "id": last["id"]}
        return {
            "records": out_records,
            "n_duplicates": start - dup_start,
            "late": late,
            "next_cursor": next_cursor,
            "has_more": has_more,
            "page_end": end,
        }

    def final_state(self, category: Optional[str] = None) -> List[Record]:
        """All records with every planned correction applied (diff baseline)."""
        items = self._filtered(category)
        plan = self._plan(len(items))
        return [
            self._materialize(rec, self._version_at(idx, len(items), plan))
            for idx, rec in enumerate(items)
        ]


def _make_handler(service: PaginatedService) -> Type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            qs = parse_qs(parsed.query)

            def one(name: str) -> Optional[str]:
                values = qs.get(name)
                return values[0] if values else None

            if parsed.path == "/records":
                cursor_ts = one("cursor_ts")
                body = service.get_page(
                    cursor_ts=int(cursor_ts) if cursor_ts is not None else None,
                    cursor_id=one("cursor_id"),
                    limit=int(one("limit") or "500"),
                    category=one("category") or None,
                    fold_corrections=one("fold") == "1",
                )
                self._respond(200, body)
            elif parsed.path == "/state":
                self._respond(200, {"records": service.final_state(one("category") or None)})
            elif parsed.path == "/health":
                self._respond(200, {"ok": True})
            else:
                self._respond(404, {"error": "not found"})

        def _respond(self, status: int, body: Dict[str, Any]) -> None:
            payload = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            pass

    return Handler


def serve_in_thread(service: PaginatedService) -> Tuple[ThreadingHTTPServer, str]:
    """Start the service on an ephemeral port. Returns (server, base_url)."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(service))
    thread = threading.Thread(target=server.serve_forever, name="paginated-service", daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    return server, "http://%s:%d" % (str(host), port)
