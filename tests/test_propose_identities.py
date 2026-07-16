"""Tests for the identity-proposal writer's overwrite policy (pure; no DB).

The policy is the guard that keeps a biometric guess — the weakest evidence in the system — from
overwriting name-derived attribution. It is a pure function precisely so it can be tested without a
database.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

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
