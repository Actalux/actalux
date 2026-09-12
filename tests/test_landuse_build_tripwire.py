"""The nightly land-use rebuild's tripwire (scripts/build_land_use_cases.py, G5)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "build_land_use_cases", Path(__file__).parent.parent / "scripts" / "build_land_use_cases.py"
)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["build_land_use_cases"] = _mod
_spec.loader.exec_module(_mod)


class TestNightlyTripwire:
    """--max-unparsed: fail before writing when segmentation regresses."""

    def test_over_the_limit_trips(self) -> None:
        assert _mod.unparsed_exceeds(8, 7) is True

    def test_at_or_under_the_limit_passes(self) -> None:
        assert _mod.unparsed_exceeds(7, 7) is False
        assert _mod.unparsed_exceeds(0, 7) is False

    def test_no_limit_means_manual_run(self) -> None:
        assert _mod.unparsed_exceeds(500, None) is False
