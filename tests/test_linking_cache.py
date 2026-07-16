"""Tests for the embedding-cache layout + manifest guard (pure; tmp_path, no DB)."""

from __future__ import annotations

import numpy as np
import pytest

from actalux.diarization.linking.cache import (
    MODE_ALL,
    MODE_ANCHORED,
    CacheManifest,
    cache_dir,
    ensure_manifest,
    read_manifest,
    require_mode,
    write_manifest,
)
from actalux.errors import ActaluxError

_ANCHORED = CacheManifest(mode=MODE_ANCHORED, min_seconds=10.0, model="wespeaker")
_ALL = CacheManifest(mode=MODE_ALL, min_seconds=10.0, model="wespeaker")


def test_cache_dir_separates_populations() -> None:
    anchored = cache_dir("root", "mo", "clayton", "schools", mode=MODE_ANCHORED)
    everything = cache_dir("root", "mo", "clayton", "schools", mode=MODE_ALL)
    assert anchored.name == "mo_clayton_schools"
    assert everything.name == "mo_clayton_schools_all"
    assert anchored != everything  # the two populations can never land in one directory


def test_manifest_round_trips(tmp_path) -> None:
    write_manifest(tmp_path, _ALL)
    assert read_manifest(tmp_path) == _ALL


def test_read_manifest_missing_is_none(tmp_path) -> None:
    assert read_manifest(tmp_path) is None


def test_ensure_manifest_writes_on_first_use(tmp_path) -> None:
    ensure_manifest(tmp_path, _ANCHORED)
    assert read_manifest(tmp_path) == _ANCHORED


def test_ensure_manifest_rejects_mode_mismatch(tmp_path) -> None:
    ensure_manifest(tmp_path, _ANCHORED)
    with pytest.raises(ActaluxError, match="blend two cluster populations"):
        ensure_manifest(tmp_path, _ALL)


def test_ensure_manifest_rejects_min_seconds_mismatch(tmp_path) -> None:
    ensure_manifest(tmp_path, _ANCHORED)
    other = CacheManifest(mode=MODE_ANCHORED, min_seconds=3.0, model="wespeaker")
    with pytest.raises(ActaluxError, match="min_seconds"):
        ensure_manifest(tmp_path, other)


def test_ensure_manifest_refuses_all_run_over_legacy_anchored_cache(tmp_path) -> None:
    # a pre-manifest cache: doc files, no manifest. An all-cluster run must not adopt it — those
    # meetings would keep their anchored-only clusters and the cache would be silently partial.
    np.savez(tmp_path / "doc_1.npz", embeddings=np.zeros((1, 2)))
    with pytest.raises(ActaluxError, match="legacy"):
        ensure_manifest(tmp_path, _ALL)


def test_ensure_manifest_adopts_legacy_cache_for_anchored_run(tmp_path) -> None:
    np.savez(tmp_path / "doc_1.npz", embeddings=np.zeros((1, 2)))
    ensure_manifest(tmp_path, _ANCHORED)  # legacy caches were anchored -> safe to adopt and stamp
    assert read_manifest(tmp_path) == _ANCHORED


def test_require_mode_rejects_missing_manifest(tmp_path) -> None:
    with pytest.raises(ActaluxError, match="legacy"):
        require_mode(tmp_path, MODE_ALL)


def test_require_mode_rejects_wrong_population(tmp_path) -> None:
    write_manifest(tmp_path, _ANCHORED)
    with pytest.raises(ActaluxError, match="required here"):
        require_mode(tmp_path, MODE_ALL)


def test_require_mode_accepts_matching_population(tmp_path) -> None:
    write_manifest(tmp_path, _ALL)
    require_mode(tmp_path, MODE_ALL)  # no raise
