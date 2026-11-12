"""Incremental extraction pipeline with a measurable, bounded-buffer optimization path.

Self-contained (stdlib only): a local paginated HTTP service, an extractor with
baseline (unbounded buffer) and optimized (bounded buffer + backpressure) modes,
SQLite-backed results and checkpoints, a benchmark runner and correctness checks.
"""
