"""Deterministic dataset and correction plan for the local paginated service.

The dataset is fully determined by its seed/size so that benchmark and
correctness runs compare like for like. Records are ordered by the composite
cursor ``(updated_at, id)``; groups of ``same_ts_group`` records share the same
``updated_at`` so that cursor handling must not collapse same-timestamp rows.
"""

from typing import Any, Dict, List, Tuple

Record = Dict[str, Any]

BASE_TS = 1_700_000_000


def generate_records(n_records: int, same_ts_group: int = 4) -> List[Record]:
    """Generate ``n_records`` records sorted by (updated_at, id).

    Every ``same_ts_group`` consecutive records share one ``updated_at``
    timestamp, so any dedup keyed on the timestamp alone would wrongly keep
    only one record per group.
    """
    records: List[Record] = []
    for i in range(n_records):
        ts = BASE_TS + (i // same_ts_group) * 60
        records.append(
            {
                "id": "rec-%06d" % i,
                "updated_at": ts,
                "category": "even" if i % 2 == 0 else "odd",
                "value": i * 10,
                "version": 1,
            }
        )
    return records


def correction_plan(
    n_records: int, limit: int, every_pages: int = 7, lag_pages: int = 3
) -> List[Tuple[int, int]]:
    """Plan sparse late corrections as (trigger_position, record_index) pairs.

    When the service has served up to ``trigger_position`` (an index into the
    sorted, unfiltered record list), it re-sends the record at
    ``record_index`` with a bumped version and changed value but its ORIGINAL
    ``updated_at`` — i.e. data that lies behind the current cursor. Positions
    refer to the unfiltered list; the plan is deterministic for a given
    (n_records, limit, every_pages, lag_pages).
    """
    plan: List[Tuple[int, int]] = []
    if every_pages <= 0:
        return plan
    page = every_pages
    while page * limit < n_records:
        trigger = page * limit
        target_page = page - lag_pages
        if target_page < 0:
            target_page = 0
        record_index = min(target_page * limit, n_records - 1)
        plan.append((trigger, record_index))
        page += every_pages
    return plan
