"""Correctness tests for the incremental extraction optimization path.

Each test delegates to a check in ``tools.incremental_extract.verify`` so the
same assertions are runnable both under pytest and standalone via
``python -m tools.incremental_extract.verify``.
"""

from pathlib import Path
from typing import Callable, Tuple

import pytest

from tools.incremental_extract import verify


@pytest.mark.parametrize("check", verify.ALL_CHECKS, ids=lambda c: c.__name__)
def test_incremental_extract(check: Callable[[str], Tuple[str, bool, str]], tmp_path: Path) -> None:
    name, ok, detail = check(str(tmp_path))
    assert ok, "%s: %s" % (name, detail)
