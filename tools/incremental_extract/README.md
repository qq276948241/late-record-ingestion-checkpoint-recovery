# Incremental extraction: bounded-buffer optimization path

A self-contained, stdlib-only incremental extraction pipeline with a
repeatable, measurable optimization path. Data comes from a local paginated
HTTP service; results land in a local SQLite database you can read directly.
No external services, no containers.

## Layout

| file | role |
| --- | --- |
| `dataset.py` | deterministic dataset + sparse-correction plan |
| `server.py` | local paginated service (page-boundary duplicates, late corrections) |
| `store.py` | SQLite results + checkpoints, params digest, single-executor flock |
| `pipeline.py` | extractor: `baseline` (unbounded buffer) vs `optimized` (bounded) |
| `benchmark.py` | paired benchmark: same data, same concurrency, both modes |
| `verify.py` | correctness checks (also wrapped by `tools/tests/test_incremental_extract.py`) |

## Source behaviors and how they are received

* **Page-boundary duplicates** — every page repeats the tail of the previous
  page. The extractor upserts by `(pipeline_id, id)`; a re-sent record with
  the same version is counted as a dropped duplicate, never written twice.
* **Late corrections** — the service re-sends records *behind* the current
  cursor with a bumped version. The upsert applies them when the incoming
  version is newer, so corrections are not dropped for being "late".
* **Same-timestamp records** — the cursor is the composite
  `(updated_at, id)`, and dedup keys on `id`, never on the timestamp, so
  records sharing one `updated_at` all survive.

## The optimization

Both modes run one reader thread and one writer thread with identical
settings. The only difference is the buffer between them:

* `baseline`: unbounded queue — a slow writer lets pages accumulate in memory;
* `optimized`: `Queue(maxsize=buffer_size)` — a full buffer blocks the reader,
  propagating backpressure to the fetch loop.

Cancellation or write failure sets a stop event that the reader checks before
every fetch, so pagination stops promptly. Pages read but not yet confirmed
stay identifiable two ways: an in-memory in-flight registry (persisted as
`unconfirmed_pages` on graceful stops) and, even after a hard kill, the
`last_read_page` vs `last_confirmed_page` gap in the checkpoint row — the
reader notes each fetched page in the checkpoint table (observational only,
it never advances the cursor).

## Checkpoints and resume

* A page's records and its checkpoint advance in **one SQLite transaction** —
  the checkpoint never points past the final write.
* The checkpoint records `params_digest`, `last_confirmed_page`,
  `last_read_page`, the cursor `(cursor_ts, cursor_id)`, and status. On start
  the digest is verified: changed filter parameters are **refused**
  (`CheckpointMismatchError`) unless `reset=True` rebuilds the pipeline.
  Page size is part of the digest because page numbering is checkpointed.
* Reruns may re-read pages (idempotent upserts — no duplicate results), and
  dedup state is scoped per `pipeline_id`, so nothing leaks across pipelines.
* A non-blocking `flock` per pipeline guarantees a single executor advances a
  checkpoint; a concurrent executor gets `ExecutorConflictError`.

## Running

```sh
# correctness checks (duplicates, corrections, restart, mismatch, locking)
python -m tools.incremental_extract.verify        # or: make test-incremental-extract (pytest)

# paired benchmark: large pages, sparse corrections, slow writer, mid-run restart
python -m tools.incremental_extract.benchmark     # or: make benchmark-incremental-extract

# a single extraction run (also used by the restart scenario as a subprocess)
python -m tools.incremental_extract run --help

# pytest
python -m pytest tools/tests/test_incremental_extract.py
```

The benchmark prints, per scenario and mode: wall time, **peak memory**
(tracemalloc), buffer peak (pages/records), records read, duplicates dropped,
late corrections applied, final row count, reader-blocked and writer-wait
times — and a paired delta. JSON goes to
`_storage/incremental_extract/benchmark.json` (gitignored).

## Measured results (this machine, Python 3.13)

```
scenario           mode         wall_s peak_mem_MB peak_pages peak_records recs_read      dups      late      rows rd_block_s  wr_wait_s
large-pages        baseline      0.856       2.41         3        6150     20450       450         0     20000      0.000      0.129
large-pages        optimized     0.777       2.40         3        6150     20450       450         0     20000      0.000      0.087
sparse-corrections baseline      0.625       0.47         2        1011     10097        95         2     10000      0.000      0.186
sparse-corrections optimized     0.479       0.45         2        1011     10097        95         2     10000      0.000      0.085
slow-writer        baseline      3.495       3.39        45        9225     10245       245         0     10000      0.000      0.012
slow-writer        optimized     3.330       0.84        10        2050     10245       245         0     10000      2.301      0.013
mid-run-restart    baseline      0.368       0.45         2        1011      7577        75         2     10000      0.000      0.071
mid-run-restart    optimized     0.391       0.44         2        1011      7577        75         2     10000      0.000      0.091
```

Every row's final result was diffed against the source's final state
(`source=OK`); row counts are identical across modes in every scenario.
For `mid-run-restart`, `recs_read` covers the resume leg only (the killed
leg's reads are reported in the JSON `legs` array); the resume re-reads the
overlap window and the dedup upsert keeps the final row count exact.

## What the optimization buys, and its limits

* **slow-writer** is where the bound pays off: baseline buffered up to 45
  pages / 9225 records (peak Python heap 3.39 MB) while optimized capped at
  10 pages / 2050 records (0.84 MB, **−75%**) — the 2.3 s of reader-blocked
  time is the backpressure transferred to the fetch loop instead of memory.
  Wall time is unchanged (the writer is the bottleneck either way).
* **large-pages / sparse-corrections**: the writer keeps up, so both modes
  behave the same — the bound costs nothing when there is no backlog.
* Limits: pagination itself stays sequential (cursor dependency), so the
  optimization bounds memory and stops waste on failure — it does not
  parallelize fetching. Peak-memory numbers are Python-heap (tracemalloc)
  measurements of one process; absolute values vary with page size and record
  payload, so compare modes within a scenario, not across scenarios. The
  checkpoint/lock files are local (single machine); the flock does not
  coordinate across hosts.

