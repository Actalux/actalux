"""Tests for the frozen linking cohort loader (pure parsing; DB paths covered by integration)."""

from __future__ import annotations

import numpy as np

from actalux.diarization.linking.cohort import _parse_embedding


def test_parse_embedding_from_bracketed_string() -> None:
    # PostgREST commonly returns a pgvector column as a bracketed string
    assert _parse_embedding("[1.0, 2.5, -3.0]") == [1.0, 2.5, -3.0]


def test_parse_embedding_from_list() -> None:
    # already-decoded sequences are coerced to floats
    assert _parse_embedding([1, 2, 3]) == [1.0, 2.0, 3.0]


def test_parse_embedding_round_trips_into_matrix() -> None:
    mat = np.asarray([_parse_embedding(r) for r in ("[1, 0]", [0, 1])], dtype=np.float64)
    assert mat.shape == (2, 2)
    assert mat[0].tolist() == [1.0, 0.0]
