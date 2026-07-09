"""Sample hygiene — quarantine mislabeled calibration data instead of scoring it.

Two mislabeling channels poisoned the honest harness (diagnosis 2026-07-09,
data/audit_sheets/recall0_diagnosis_2026-07-09.md, cal id=15):

  - TWIN NEGATIVES. "Negatives" are the longest unlabeled clusters per meeting, but
    enrolled officials attend nearly every meeting and are text-anchored in a minority
    (cal-15: unlabeled in 50-72 of 79 fold meetings) — so the negative pool is saturated
    with the officials' own voices, and the citizen-FP metric counts the matcher's
    successes (recognizing an unlabeled official) as its failures. That flood kept every
    genuinely-predicting operating point below the precision bar.
  - ALIEN POSITIVES. An unvetted inferred anchor occasionally puts an official's name on
    the wrong voice (cal-15: 2 of 47 gallery rows at cosine 0.06-0.10 to the person's
    confirmed voice); the confirmed-waiver path would persist it into the gallery.

Quarantine is exclusion-with-receipts, never silent: every quarantined sample is returned
with its provenance and offending score, counted in the persisted report (aggregates only),
and cued on the local audit sheet for human adjudication — "yes, that's the official"
becomes a new confirmed anchor; "no" certifies a hard negative. Pure numpy, no DB/GPU.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

import numpy as np

from actalux.diarization.families import CONFIRMED_CONFIDENCE
from actalux.diarization.matching import Sample

# A negative this close (mean cosine) to one official's samples is treated as that official's
# own voice, not a citizen. Measured band (diagnosis above): same-voice twins score 0.785-0.915
# against the official's cross-meeting gallery; every measured different-voice score is <= 0.49
# (between-official centroids <= 0.38). 0.70 splits the empty band erring toward quarantine —
# metric honesty over retention; a quarantined real citizen is reviewed, not lost.
QUARANTINE_BOUND = 0.70
# Negatives just under the bound are kept in the metric but reported, so a drifting boundary
# (new town, different audio) is visible instead of silently re-poisoning the citizen-FP count.
NEAR_BAND_FLOOR = 0.60
# A confirmed official's other anchors must agree with their human-confirmed voice: below this
# cosine-to-confirmed-centroid a sample is provably another voice. Measured band: the two known
# wrong-voice anchors score 0.06-0.10, every genuine sample >= 0.66; 0.40 sits far below any
# genuine sample observed, so the floor can only drop alien voices.
CONFIRMED_CORE_FLOOR = 0.40


@dataclass(frozen=True)
class QuarantinedSample:
    """One excluded sample + the receipt: which person it offends against, and how strongly.

    For a twin negative, ``person_id`` is the official it acoustically matches; for an alien
    positive it is the sample's own (disputed) label. Provenance (meeting/doc/cluster) rides
    on ``sample`` itself. The caller reports counts to the DB and details to the local audit
    sheet only — a quarantined negative may be a citizen, so its identifiers never persist.
    """

    sample: Sample
    person_id: int
    score: float


@dataclass(frozen=True)
class NegativeQuarantine:
    """Result of the twin-negative pass: the cleaned samples plus both receipt lists.

    ``near_band`` samples remain in ``kept`` (they still count in the citizen-FP metric);
    they are surfaced so boundary drift is visible.
    """

    kept: list[Sample]
    quarantined: list[QuarantinedSample]
    near_band: list[QuarantinedSample]


def _unit_rows(vectors: list[tuple[float, ...]]) -> np.ndarray:
    mat = np.asarray(vectors, dtype=np.float64)
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return mat / norms


def vet_confirmed_positives(
    samples: list[Sample], *, floor: float = CONFIRMED_CORE_FLOOR
) -> tuple[list[Sample], list[QuarantinedSample]]:
    """Quarantine an official's samples that disagree with their human-confirmed voice.

    For every person with at least one ``confirmed`` sample, the confirmed samples' unit-mean
    is that person's trusted voice; any of the person's samples (confirmed ones included, so a
    contradicted confirmation also surfaces) below ``floor`` cosine to it is quarantined.
    Persons with no confirmed sample pass through untouched — there is no trusted voice to vet
    against; their coherence is Gate A's job. Negatives pass through untouched.

    Known limit: with very few confirmed samples a WRONG confirmation drags the centroid
    toward itself and can survive the floor (two conflicting confirmations score ~0.7 each);
    conflicting confirmations are the collapse guard's and the human reviewer's territory.

    Returns ``(kept, quarantined)`` with input order preserved in ``kept``.
    """
    by_person: dict[int, list[Sample]] = defaultdict(list)
    for s in samples:
        if s.person_id is not None:
            by_person[s.person_id].append(s)

    alien: dict[int, QuarantinedSample] = {}  # id(sample) -> receipt
    for person, group in by_person.items():
        confirmed = [s for s in group if s.confidence == CONFIRMED_CONFIDENCE]
        if not confirmed:
            continue
        centroid = _unit_rows([s.embedding for s in confirmed]).mean(axis=0)
        norm = float(np.linalg.norm(centroid))
        if norm == 0:
            continue  # degenerate (opposed confirmations); nothing trustworthy to vet against
        centroid = centroid / norm
        scores = _unit_rows([s.embedding for s in group]) @ centroid
        for s, score in zip(group, scores):
            if float(score) < floor:
                alien[id(s)] = QuarantinedSample(s, person, float(score))

    kept = [s for s in samples if id(s) not in alien]
    return kept, list(alien.values())


def quarantine_twin_negatives(
    samples: list[Sample],
    *,
    bound: float = QUARANTINE_BOUND,
    near_floor: float = NEAR_BAND_FLOOR,
) -> NegativeQuarantine:
    """Quarantine negatives that acoustically ARE an enrolled official (unlabeled attendance).

    Each negative's score against a person is the MEAN cosine over that person's positive
    samples — the matcher's own default aggregation, and what a same-voice twin maximizes
    (it sits inside the person's cross-meeting similarity range, where no different voice
    was ever observed). A negative whose best per-person mean is >= ``bound`` is quarantined;
    one in ``[near_floor, bound)`` stays in the metric but is reported (``near_band``).
    Vet positives first (``vet_confirmed_positives``): an alien positive in the comparison
    set would quarantine citizens matching the WRONG voice.

    Input order is preserved in ``kept``.
    """
    positives = [s for s in samples if s.person_id is not None]
    negatives = [s for s in samples if s.person_id is None]
    if not positives or not negatives:
        return NegativeQuarantine(list(samples), [], [])

    pos_mat = _unit_rows([s.embedding for s in positives])
    neg_mat = _unit_rows([s.embedding for s in negatives])
    sim = neg_mat @ pos_mat.T  # (n_neg, n_pos)

    cols_by_person: dict[int, list[int]] = defaultdict(list)
    for col, s in enumerate(positives):
        cols_by_person[s.person_id].append(col)  # type: ignore[index]  # positives filtered above

    quarantined: dict[int, QuarantinedSample] = {}  # id(sample) -> receipt
    near_band: list[QuarantinedSample] = []
    for row, neg in enumerate(negatives):
        best_person, best_score = None, -1.0
        for person, cols in cols_by_person.items():
            score = float(sim[row, cols].mean())
            if score > best_score:
                best_person, best_score = person, score
        assert best_person is not None  # cols_by_person non-empty when positives exist
        if best_score >= bound:
            quarantined[id(neg)] = QuarantinedSample(neg, best_person, best_score)
        elif best_score >= near_floor:
            near_band.append(QuarantinedSample(neg, best_person, best_score))

    kept = [s for s in samples if id(s) not in quarantined]
    return NegativeQuarantine(kept, list(quarantined.values()), near_band)
