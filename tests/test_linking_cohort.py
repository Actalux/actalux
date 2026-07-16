"""Tests for the frozen linking cohort loader (pure parsing; DB paths covered by integration)."""

from __future__ import annotations

import numpy as np

from actalux.diarization.linking.cohort import parse_pgvector


def test_parse_pgvector_from_bracketed_string() -> None:
    # PostgREST commonly returns a pgvector column as a bracketed string
    assert parse_pgvector("[1.0, 2.5, -3.0]") == [1.0, 2.5, -3.0]


def test_parse_pgvector_from_list() -> None:
    # already-decoded sequences are coerced to floats
    assert parse_pgvector([1, 2, 3]) == [1.0, 2.0, 3.0]


def test_parse_pgvector_round_trips_into_matrix() -> None:
    mat = np.asarray([parse_pgvector(r) for r in ("[1, 0]", [0, 1])], dtype=np.float64)
    assert mat.shape == (2, 2)
    assert mat[0].tolist() == [1.0, 0.0]
