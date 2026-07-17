"""Tests for the identity-proposal writer's overwrite policy (pure; no DB).

The policy is the guard that keeps a biometric guess — the weakest evidence in the system — from
overwriting name-derived attribution. It is a pure function precisely so it can be tested without a
database.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from actalux.errors import ActaluxError

_SPEC = importlib.util.spec_from_file_location(
    "propose_identities",
    Path(__file__).resolve().parent.parent / "scripts" / "linking" / "propose_identities.py",
)
assert _SPEC and _SPEC.loader
propose_identities = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(propose_identities)
skip_reason = propose_identities.skip_reason


def test_absent_row_is_writable() -> None:
    assert skip_reason(None) is None


def test_previous_voiceprint_proposal_is_refreshable() -> None:
    assert skip_reason({"confidence": "inferred_medium", "basis": "voiceprint"}) is None


def test_basis_less_row_is_writable() -> None:
    assert skip_reason({"confidence": "inferred_low", "basis": None}) is None


def test_confirmed_row_is_protected() -> None:
    reason = skip_reason({"confidence": "confirmed", "basis": "manual"})
    assert reason is not None and "protected confidence" in reason


def test_rejected_row_is_protected() -> None:
    assert skip_reason({"confidence": "rejected", "basis": None}) is not None


def test_inferred_high_row_is_protected() -> None:
    # inferred_high is anon-visible; a voiceprint guess must never reach the public gate
    assert skip_reason({"confidence": "inferred_high", "basis": "rollcall"}) is not None


def test_medium_rollcall_row_is_protected_by_basis() -> None:
    # THE regression this policy exists for: a rollcall row held at inferred_medium is not
    # enrollable at that tier, so it never enters the anchor set and the link cannot see it.
    # Skipping on confidence alone would rewrite a spoken-name attribution's basis to 'voiceprint'.
    reason = skip_reason({"confidence": "inferred_medium", "basis": "rollcall"})
    assert reason is not None and "name-anchored basis" in reason


def test_low_self_intro_row_is_protected_by_basis() -> None:
    assert skip_reason({"confidence": "inferred_low", "basis": "self_intro"}) is not None


def test_cli_threshold_override_forces_plain_asnorm() -> None:
    stored = {"id": 7, "threshold": 3.9, "method": "calibrated", "calibrator": {}, "cohort_id": 1}
    resolved = propose_identities.resolve_operating_point(12.5, stored)
    assert resolved["method"] == "asnorm"
    assert resolved["threshold"] == 12.5
    assert resolved["calibrator"] is None
    assert resolved["cohort_id"] is None  # an override is not tied to a measured cohort
    assert resolved["version"].startswith("cli-override/")


def test_stored_operating_point_supplies_method_and_cohort() -> None:
    stored = {
        "id": 7,
        "threshold": 3.898,
        "method": "calibrated",
        "calibrator": {"weights": [0.0]},
        "cohort_id": 1,
    }
    resolved = propose_identities.resolve_operating_point(None, stored)
    assert resolved["method"] == "calibrated"
    assert resolved["threshold"] == 3.898
    assert resolved["calibrator"] == {"weights": [0.0]}
    assert resolved["cohort_id"] == 1
    assert resolved["version"] == "op=7/method=calibrated/thr=3.8980"


def test_no_operating_point_and_no_override_is_an_error() -> None:
    with pytest.raises(ActaluxError, match="operating point"):
        propose_identities.resolve_operating_point(None, None)
