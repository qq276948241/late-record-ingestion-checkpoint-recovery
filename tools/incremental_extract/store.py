# flake8: noqa: I251
"""SQLite-backed result store, checkpoints and single-executor locking.

Design rules implemented here:

* a page's records and the checkpoint that confirms it are written in ONE
  transaction, so the checkpoint never advances past the final write;
* dedup state (the result rows themselves, keyed by ``(pipeline_id, id)``)
  is scoped per pipeline — reruns of the same pipeline are idempotent and
  no state leaks into another pipeline;
* a checkpoint records the params digest, the last confirmed page and the
  cursor, so a resume can verify it is continuing the same extraction;
* an OS-level file lock guarantees a single executor per checkpoint.
"""

import fcntl
import hashlib
import json
import os
import sqlite3
import time
from typing import AbstractSet, Any, Dict, List, Optional, Tuple

from tools.incremental_extract.dataset import Record

SCHEMA = """
CREATE TABLE IF NOT EXISTS results (
    pipeline_id TEXT NOT NULL,
    id TEXT NOT NULL,
    version INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (pipeline_id, id)
);
CREATE TABLE IF NOT EXISTS checkpoints (
    pipeline_id TEXT PRIMARY KEY,
    params_digest TEXT NOT NULL,
    last_confirmed_page INTEGER NOT NULL,
    last_read_page INTEGER NOT NULL DEFAULT 0,
    cursor_ts INTEGER,
    cursor_id TEXT,
    confirmed_records INTEGER NOT NULL,
    status TEXT NOT NULL,
    unconfirmed_pages TEXT NOT NULL DEFAULT '[]',
    updated_at REAL NOT NULL
);
"""


class CheckpointMismatchError(RuntimeError):
    """Raised when a checkpoint's params digest does not match the request."""


class ExecutorConflictError(RuntimeError):
    """Raised when another executor already holds the pipeline lock."""


def params_digest(source_url: str, limit: int, filters: Dict[str, object]) -> str:
    """Digest of the extraction parameters that define the result set.

    Page size is included: page numbering is part of the checkpoint, so a
    different page size is treated as a different extraction and refused.
    """
    canonical = json.dumps(
        {"source": source_url, "limit": limit, "filters": filters}, sort_keys=True
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class PipelineLock:
    """Non-blocking exclusive flock; only one executor may advance a checkpoint."""

    def __init__(self, working_dir: str, pipeline_id: str) -> None:
        self._path = os.path.join(working_dir, "%s.lock" % pipeline_id)
        self._fd: Optional[int] = None

    def __enter__(self) -> "PipelineLock":
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        self._fd = os.open(self._path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(self._fd)
            self._fd = None
            raise ExecutorConflictError("another executor holds the lock for %s" % self._path)
        return self

    def __exit__(self, *exc: object) -> None:
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None


def note_page_read(db_path: str, pipeline_id: str, page_no: int) -> None:
    """Record that ``page_no`` was fetched (best-effort, own connection).

    Called from the reader thread so that even after a hard kill the gap
    ``(last_confirmed_page, last_read_page]`` identifies pages that were read
    but never confirmed. Observational only — it never advances the cursor.
    """
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        with conn:
            conn.execute(
                "UPDATE checkpoints SET last_read_page = MAX(last_read_page, ?)"
                " WHERE pipeline_id = ?",
                (page_no, pipeline_id),
            )
    finally:
        conn.close()


class ResultStore:
    def __init__(self, db_path: str) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path, timeout=30)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # -- checkpoint handling -------------------------------------------------

    def get_checkpoint(self, pipeline_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT pipeline_id, params_digest, last_confirmed_page, last_read_page,"
            " cursor_ts, cursor_id, confirmed_records, status, unconfirmed_pages"
            " FROM checkpoints WHERE pipeline_id = ?",
            (pipeline_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "pipeline_id": row[0],
            "params_digest": row[1],
            "last_confirmed_page": row[2],
            "last_read_page": row[3],
            "cursor_ts": row[4],
            "cursor_id": row[5],
            "confirmed_records": row[6],
            "status": row[7],
            "unconfirmed_pages": json.loads(row[8]),
        }

    def check_or_create_checkpoint(
        self, pipeline_id: str, digest: str, reset: bool = False
    ) -> Dict[str, Any]:
        """Verify an existing checkpoint against ``digest`` or create a new one.

        Raises CheckpointMismatchError if the stored digest belongs to a
        different parameter set and ``reset`` was not requested.
        """
        existing = self.get_checkpoint(pipeline_id)
        if existing is not None and existing["params_digest"] != digest:
            if not reset:
                raise CheckpointMismatchError(
                    "checkpoint for pipeline %r was created with different parameters "
                    "(stored digest %s, requested %s); pass reset=True to rebuild it"
                    % (pipeline_id, existing["params_digest"], digest)
                )
            self.clear_pipeline(pipeline_id)
            existing = None
        if existing is None:
            with self._conn:
                self._conn.execute(
                    "INSERT OR REPLACE INTO checkpoints"
                    " (pipeline_id, params_digest, last_confirmed_page, cursor_ts, cursor_id,"
                    "  confirmed_records, status, unconfirmed_pages, updated_at)"
                    " VALUES (?, ?, 0, NULL, NULL, 0, 'running', '[]', ?)",
                    (pipeline_id, digest, time.time()),
                )
            return self.get_checkpoint(pipeline_id)  # type: ignore[return-value, unused-ignore]
        with self._conn:
            self._conn.execute(
                "UPDATE checkpoints SET status = 'running', updated_at = ? WHERE pipeline_id = ?",
                (time.time(), pipeline_id),
            )
        return existing

    def clear_pipeline(self, pipeline_id: str) -> None:
        with self._conn:
            self._conn.execute("DELETE FROM results WHERE pipeline_id = ?", (pipeline_id,))
            self._conn.execute("DELETE FROM checkpoints WHERE pipeline_id = ?", (pipeline_id,))

    # -- atomic page write + checkpoint confirm -------------------------------

    def confirm_page(
        self,
        pipeline_id: str,
        page_no: int,
        records: List[Record],
        cursor_ts: Optional[int],
        cursor_id: Optional[str],
        status: str = "running",
        unconfirmed_pages: Optional[List[int]] = None,
        late_ids: Optional[AbstractSet[str]] = None,
    ) -> Dict[str, int]:
        """Upsert one page's records and advance the checkpoint atomically.

        Returns counts: new rows, applied updates (late corrections),
        duplicates dropped (same id, same or older version re-sent) and how
        many of the late records were actually applied.
        """
        inserted = updated = duplicates = late_applied = 0
        late_ids = late_ids or set()
        with self._conn:
            for rec in records:
                row = self._conn.execute(
                    "SELECT version FROM results WHERE pipeline_id = ? AND id = ?",
                    (pipeline_id, rec["id"]),
                ).fetchone()
                if row is None:
                    inserted += 1
                elif rec["version"] > row[0]:
                    updated += 1
                else:
                    duplicates += 1
                    continue
                if rec["id"] in late_ids:
                    late_applied += 1
                self._conn.execute(
                    "INSERT INTO results (pipeline_id, id, version, updated_at, payload)"
                    " VALUES (?, ?, ?, ?, ?)"
                    " ON CONFLICT(pipeline_id, id) DO UPDATE SET"
                    "   version = excluded.version,"
                    "   updated_at = excluded.updated_at,"
                    "   payload = excluded.payload",
                    (
                        pipeline_id,
                        rec["id"],
                        rec["version"],
                        rec["updated_at"],
                        json.dumps(rec, sort_keys=True),
                    ),
                )
            self._conn.execute(
                "UPDATE checkpoints SET last_confirmed_page = ?, cursor_ts = ?, cursor_id = ?,"
                " confirmed_records = confirmed_records + ?, status = ?,"
                " unconfirmed_pages = ?, updated_at = ? WHERE pipeline_id = ?",
                (
                    page_no,
                    cursor_ts,
                    cursor_id,
                    inserted + updated,
                    status,
                    json.dumps(unconfirmed_pages or []),
                    time.time(),
                    pipeline_id,
                ),
            )
        return {
            "inserted": inserted,
            "updated": updated,
            "duplicates": duplicates,
            "late_applied": late_applied,
        }

    def mark_unconfirmed(self, pipeline_id: str, pages: List[int], status: str) -> None:
        """Best-effort record of pages that were read but never confirmed.

        With no explicit list (e.g. after a hard kill), derives the gap from
        ``last_read_page`` vs ``last_confirmed_page``.
        """
        if not pages:
            cp = self.get_checkpoint(pipeline_id)
            if cp is not None:
                pages = list(
                    range(
                        int(cp["last_confirmed_page"]) + 1,
                        int(cp["last_read_page"]) + 1,
                    )
                )
        with self._conn:
            self._conn.execute(
                "UPDATE checkpoints SET unconfirmed_pages = ?, status = ?, updated_at = ?"
                " WHERE pipeline_id = ?",
                (json.dumps(sorted(pages)), status, time.time(), pipeline_id),
            )

    # -- reads -----------------------------------------------------------------

    def read_results(self, pipeline_id: str) -> List[Record]:
        rows = self._conn.execute(
            "SELECT payload FROM results WHERE pipeline_id = ? ORDER BY updated_at, id",
            (pipeline_id,),
        ).fetchall()
        return [json.loads(row[0]) for row in rows]

    def count_results(self, pipeline_id: str) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) FROM results WHERE pipeline_id = ?", (pipeline_id,)
        ).fetchone()
        return int(row[0])
