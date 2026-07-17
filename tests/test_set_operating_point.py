"""Tests for the operating-point setter's pure validation (DB paths covered by integration)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import numpy as np
import pytest

from actalux.diarization.linking.calibration import FEATURE_NAMES, Calibrator
from actalux.errors import ActaluxError

_SPEC = importlib.util.spec_from_file_location(
    "set_operating_point",
    Path(__file__).resolve().parents[1] / "scripts" / "linking" / "set_operating_point.py",
)
set_operating_point = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(set_operating_point)


def _calibrator_dict() -> dict[str, object]:
    n = len(FEATURE_NAMES)
    return Calibrator(
        weights=np.zeros(n + 1), mean=np.zeros(n), std=np.ones(n), feature_names=FEATURE_NAMES
    ).to_dict()


def test_asnorm_needs_no_calibrator_file() -> None:
    assert set_operating_point.load_calibrator_payload(None, "asnorm") is None


def test_asnorm_with_calibrator_file_is_an_error() -> None:
    with pytest.raises(ActaluxError, match="calibrated"):
        set_operating_point.load_calibrator_payload("some.json", "asnorm")


def test_calibrated_without_file_is_an_error() -> None:
    with pytest.raises(ActaluxError, match="calibrator-file"):
        set_operating_point.load_calibrator_payload(None, "calibrated")


def test_calibrated_accepts_measure_summary_and_bare_dict(tmp_path: Path) -> None:
    payload = _calibrator_dict()
    bare = tmp_path / "bare.json"
    bare.write_text(json.dumps(payload))
    summary = tmp_path / "summary.json"
    summary.write_text(json.dumps({"calibrator": payload, "asnorm_best": {}}))
    assert set_operating_point.load_calibrator_payload(str(bare), "calibrated") == payload
    assert set_operating_point.load_calibrator_payload(str(summary), "calibrated") == payload


def test_calibrated_rejects_stale_feature_layout(tmp_path: Path) -> None:
    stale = {"weights": [0.0, 1.0], "mean": [0.0], "std": [1.0], "feature_names": ["cosine"]}
    f = tmp_path / "stale.json"
    f.write_text(json.dumps(stale))
    with pytest.raises(ValueError, match="refit"):
        set_operating_point.load_calibrator_payload(str(f), "calibrated")
