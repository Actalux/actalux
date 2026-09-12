"""Tests for the shared row-wise L2 normalizer (actalux.diarization.vectors).

Pooling, label QA, hygiene, matching and linking each used to carry their own copy
of this. Now that they share one, a bug here is a bug in every voiceprint decision
at once — so the properties they all rely on are pinned here rather than left
implied by whichever caller happens to be covered.
"""

from __future__ import annotations

import numpy as np
import pytest

from actalux.diarization.vectors import l2_normalize_rows, medoid_cosines


class TestL2NormalizeRows:
    def test_rows_become_unit_length(self) -> None:
        out = l2_normalize_rows(np.array([[3.0, 4.0], [0.0, 2.0]]))
        assert np.allclose(np.linalg.norm(out, axis=1), [1.0, 1.0])

    def test_direction_is_preserved(self) -> None:
        out = l2_normalize_rows(np.array([[3.0, 4.0]]))
        assert np.allclose(out, [[0.6, 0.8]])

    def test_zero_row_stays_zero_and_is_not_nan(self) -> None:
        # The guard every caller depends on. Dividing a zero row by its own norm
        # yields NaN, and a single NaN propagates through the dot product to
        # poison an entire similarity matrix — every comparison against that
        # speaker silently stops being a number.
        out = l2_normalize_rows(np.array([[0.0, 0.0], [1.0, 0.0]]))
        assert not np.isnan(out).any()
        assert np.allclose(out[0], [0.0, 0.0])

    def test_zero_row_scores_zero_cosine_rather_than_matching_everything(self) -> None:
        mat = l2_normalize_rows(np.array([[0.0, 0.0], [1.0, 0.0], [0.0, 1.0]]))
        sim = mat @ mat.T
        assert np.allclose(sim[0], [0.0, 0.0, 0.0])

    def test_cosine_equals_dot_product_after_normalizing(self) -> None:
        # Callers normalize once and then use a plain dot product as cosine.
        raw = np.array([[2.0, 1.0], [-1.0, 3.0], [0.5, 0.5]])
        normed = l2_normalize_rows(raw)
        expected = raw[0] @ raw[1] / (np.linalg.norm(raw[0]) * np.linalg.norm(raw[1]))
        assert np.isclose((normed @ normed.T)[0, 1], expected)

    def test_input_is_not_mutated(self) -> None:
        raw = np.array([[3.0, 4.0]])
        before = raw.copy()
        l2_normalize_rows(raw)
        assert np.array_equal(raw, before)

    def test_single_row_and_empty_input(self) -> None:
        assert np.allclose(l2_normalize_rows(np.array([[5.0]])), [[1.0]])
        assert l2_normalize_rows(np.zeros((0, 4))).shape == (0, 4)


class TestMedoidCosines:
    """The medoid helper shared by pooling and labelqa (one implementation, two callers)."""

    def test_medoid_is_the_row_closest_to_the_rest(self) -> None:
        # Two near-identical rows and one orthogonal outlier: the medoid must be
        # one of the pair, and every cosine reported is the medoid's own row.
        vecs = l2_normalize_rows(np.array([[1.0, 0.0], [0.98, 0.2], [0.0, 1.0]]))
        medoid, cos = medoid_cosines(vecs)
        assert medoid in (0, 1)
        assert cos.shape == (3,)
        assert cos[medoid] == pytest.approx(1.0)
        assert cos[2] < cos[1 - medoid]

    def test_singleton_anchors_on_itself(self) -> None:
        medoid, cos = medoid_cosines(l2_normalize_rows(np.array([[3.0, 4.0]])))
        assert medoid == 0
        assert cos.tolist() == pytest.approx([1.0])
