"""Voiceprint matcher math — scoring, gating, and leakage-safe calibration.

Pure library (no DB, no GPU): given labeled voiceprint ``Sample``s it scores a query
against a gallery, applies Gate A enablement (labelqa), and estimates the operating point
under leave-one-meeting-out (and nested LOMO, which removes operating-point overfit).

Shared by the calibration CLI (``scripts/voiceprint_calibrate.py``), the recalibration
harness (``scripts/recalibrate_voiceprints.py``), and — later — the live matcher.
Design: docs/architecture/voiceprint-recalibration-plan.md §5.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from actalux.diarization.labelqa import coherent_core, collapse_suspects

# Operating-point search grid (cosine on L2-normalized vectors, so scores in [-1, 1]).
# Threshold grid reaches 0.90 because the diagnostic same-person p90 is 0.937 — a 0.80
# ceiling would stop below where a precise operating point can live.
DEFAULT_THRESHOLDS = (0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90)
DEFAULT_MARGINS = (0.0, 0.05, 0.10, 0.15, 0.20)
DEFAULT_AGGREGATIONS = ("mean", "max")
# Gate A (label quality) selection grid for enabling an official. core_floor is swept;
# min_core and collapse_bound are fixed structural safeguards (plan §7).
DEFAULT_CORE_FLOORS = (0.30, 0.40, 0.50)
GATE_A_MIN_CORE = 2
GATE_A_COLLAPSE_BOUND = 0.85


@dataclass(frozen=True)
class Sample:
    """One gallery (or negative) voiceprint with its true label + leave-out unit.

    ``person_id`` is the true official (``None`` marks a negative — a non-official the
    matcher must reject). ``meeting_key`` is the ``video_id``: the leave-one-out unit, so
    version-chain siblings (same recording) never leak across the split.
    """

    person_id: int | None
    meeting_key: str
    embedding: tuple[float, ...]


def cosine(a: tuple[float, ...], b: tuple[float, ...]) -> float:
    """Cosine similarity. Inputs are L2-normalized at enrollment, so this is a dot product."""
    return sum(x * y for x, y in zip(a, b))


def as_vector(embedding: Any) -> tuple[float, ...]:
    """pgvector round-trips as a JSON string or a list depending on the client; normalize."""
    if isinstance(embedding, str):
        return tuple(float(x) for x in embedding.strip("[]").split(","))
    return tuple(float(x) for x in embedding)


def person_scores(
    query: Sample, gallery: list[Sample], *, aggregation: str, allowed: set[int] | None = None
) -> dict[int, float]:
    """Aggregate cosine(query, sample) per person over ``gallery`` (already leave-out-filtered).

    ``allowed`` restricts the gallery to enabled officials (Gate A); ``None`` means all.
    Negatives (person_id None) are never in the gallery.
    """
    by_person: dict[int, list[float]] = defaultdict(list)
    for s in gallery:
        if s.person_id is None:
            continue
        if allowed is not None and s.person_id not in allowed:
            continue
        by_person[s.person_id].append(cosine(query.embedding, s.embedding))
    if aggregation == "max":
        return {p: max(v) for p, v in by_person.items()}
    # "mean" (default): robust to a single lucky sample.
    return {p: sum(v) / len(v) for p, v in by_person.items()}


def predict(
    query: Sample,
    gallery: list[Sample],
    threshold: float,
    margin: float,
    *,
    aggregation: str,
    allowed: set[int] | None = None,
) -> int | None:
    """The matcher's call: top person if it clears ``threshold`` AND ``margin``, else abstain."""
    scores = person_scores(query, gallery, aggregation=aggregation, allowed=allowed)
    if not scores:
        return None
    ranked = sorted(scores.items(), key=lambda kv: -kv[1])
    top_person, top_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0.0
    if top_score >= threshold and (top_score - second_score) >= margin:
        return top_person
    return None


def leave_one_meeting_out(
    samples: list[Sample],
    threshold: float,
    margin: float,
    *,
    aggregation: str,
    allowed: set[int] | None = None,
) -> list[tuple[int | None, int | None]]:
    """``(true_person, predicted_person)`` per sample, scoring against other meetings only."""
    out: list[tuple[int | None, int | None]] = []
    for q in samples:
        gallery = [s for s in samples if s.meeting_key != q.meeting_key]
        out.append(
            (
                q.person_id,
                predict(q, gallery, threshold, margin, aggregation=aggregation, allowed=allowed),
            )
        )
    return out


def enabled_officials(
    train: list[Sample], *, core_floor: float, min_core: int, collapse_bound: float
) -> set[int]:
    """Gate A: officials from ``train`` with a cross-meeting coherent core and no collapse.

    An official is enabled only if their voiceprints mutually agree (coherent core) AND
    their voice is not near-duplicate with a *different* official's (a roll-call caller
    labeled under several names). Negatives (person_id None) are ignored.
    """
    by_person: dict[int, list[tuple[float, ...]]] = defaultdict(list)
    for s in train:
        if s.person_id is not None:
            by_person[s.person_id].append(s.embedding)

    suspects = collapse_suspects(
        [(p, v) for p, vs in by_person.items() for v in vs], collapse_bound=collapse_bound
    )
    enabled: set[int] = set()
    for person, vecs in by_person.items():
        if person in suspects:
            continue
        if coherent_core(vecs, core_floor=core_floor, min_core=min_core):
            enabled.add(person)
    return enabled


@dataclass
class Metrics:
    """Precision/recall of one operating point, plus the officials that collided."""

    macro_precision: float
    recall: float
    predictions: int
    per_person_precision: dict[int, float] = field(default_factory=dict)
    confusions: list[tuple[int | None, int]] = field(default_factory=list)  # (true, wrong pred)


def score(preds: list[tuple[int | None, int | None]]) -> Metrics:
    """Macro precision (by predicted official), recall, and confusion pairs.

    A prediction for a negative (true=None) is a false positive. Macro precision averages
    per-predicted-official precision so a talkative official can't dominate.
    """
    tp: dict[int, int] = defaultdict(int)
    predicted: dict[int, int] = defaultdict(int)
    positives = recalled = 0
    confusions: list[tuple[int | None, int]] = []
    for true, pred in preds:
        if true is not None:
            positives += 1
        if pred is None:
            continue
        predicted[pred] += 1
        if pred == true:
            tp[pred] += 1
            recalled += 1
        else:
            confusions.append((true, pred))
    per_person = {p: tp[p] / predicted[p] for p in predicted}
    macro = sum(per_person.values()) / len(per_person) if per_person else 1.0
    recall = recalled / positives if positives else 0.0
    return Metrics(macro, recall, sum(predicted.values()), per_person, confusions)


def sweep(
    samples: list[Sample],
    thresholds: tuple[float, ...],
    margins: tuple[float, ...],
    *,
    aggregation: str,
    allowed: set[int] | None = None,
) -> list[tuple[float, float, Metrics]]:
    """Every ``(threshold, margin)`` point with its metrics (one aggregation, fixed gallery)."""
    grid = []
    for t in thresholds:
        for m in margins:
            preds = leave_one_meeting_out(samples, t, m, aggregation=aggregation, allowed=allowed)
            grid.append((t, m, score(preds)))
    return grid


def best_operating_point(
    grid: list[tuple[float, float, Metrics]], precision_bar: float
) -> tuple[float, float, Metrics] | None:
    """Highest-recall point meeting the precision bar; ties -> the MORE conservative point.

    On equal recall prefer the higher threshold, then the higher margin: at the precision-
    first cardinal ("never a wrong name"), a tie should resolve toward stricter matching.
    """
    ok = [g for g in grid if g[2].macro_precision >= precision_bar]
    if not ok:
        return None
    return max(ok, key=lambda g: (g[2].recall, g[0], g[1]))


@dataclass
class OperatingPoint:
    """A selected matcher configuration + the enabled officials + its in-sample metrics."""

    core_floor: float
    threshold: float
    margin: float
    aggregation: str
    enabled: set[int]
    metrics: Metrics


def select_operating_point(
    samples: list[Sample],
    *,
    thresholds: tuple[float, ...] = DEFAULT_THRESHOLDS,
    margins: tuple[float, ...] = DEFAULT_MARGINS,
    aggregations: tuple[str, ...] = DEFAULT_AGGREGATIONS,
    core_floors: tuple[float, ...] = DEFAULT_CORE_FLOORS,
    min_core: int = GATE_A_MIN_CORE,
    collapse_bound: float = GATE_A_COLLAPSE_BOUND,
    precision_bar: float,
) -> OperatingPoint | None:
    """Pick (core_floor, threshold, margin, aggregation) maximizing recall@bar via LOMO.

    Sweeps Gate-A enablement (core_floor) and matcher params, evaluating each by
    leave-one-meeting-out on ``samples``. Returns the highest-recall config that clears the
    precision bar (conservative tie-break: higher threshold, then margin), or ``None`` if
    nothing clears it. Used both per training fold (nested) and for the full-data refit.
    """
    best: OperatingPoint | None = None
    best_key: tuple[float, float, float] | None = None
    for core_floor in core_floors:
        enabled = enabled_officials(
            samples, core_floor=core_floor, min_core=min_core, collapse_bound=collapse_bound
        )
        if not enabled:
            continue
        for agg in aggregations:
            for t in thresholds:
                for mgn in margins:
                    mtr = score(
                        leave_one_meeting_out(samples, t, mgn, aggregation=agg, allowed=enabled)
                    )
                    if mtr.macro_precision >= precision_bar:
                        key = (mtr.recall, t, mgn)
                        if best_key is None or key > best_key:
                            best_key, best = (
                                key,
                                OperatingPoint(core_floor, t, mgn, agg, enabled, mtr),
                            )
    return best


def nested_leave_one_meeting_out(
    samples: list[Sample],
    *,
    thresholds: tuple[float, ...] = DEFAULT_THRESHOLDS,
    margins: tuple[float, ...] = DEFAULT_MARGINS,
    aggregations: tuple[str, ...] = DEFAULT_AGGREGATIONS,
    core_floors: tuple[float, ...] = DEFAULT_CORE_FLOORS,
    min_core: int = GATE_A_MIN_CORE,
    collapse_bound: float = GATE_A_COLLAPSE_BOUND,
    precision_bar: float,
) -> tuple[Metrics, dict[str, Any]]:
    """Honest performance estimate: params/enablement chosen per fold from OTHER meetings.

    Outer loop holds out one meeting; the operating point (enablement + matcher params) is
    selected on the remaining meetings only, then the held-out meeting's positives AND
    negatives are scored unfiltered against the training gallery. This removes the
    lucky-operating-point circularity (plan §5). Returns (overall Metrics, provenance).
    """
    meetings = sorted({s.meeting_key for s in samples})
    preds: list[tuple[int | None, int | None]] = []
    chosen: list[dict[str, float]] = []
    abstained_folds = 0
    for mk in meetings:
        held = [s for s in samples if s.meeting_key == mk]
        train = [s for s in samples if s.meeting_key != mk]
        op = select_operating_point(
            train,
            thresholds=thresholds,
            margins=margins,
            aggregations=aggregations,
            core_floors=core_floors,
            min_core=min_core,
            collapse_bound=collapse_bound,
            precision_bar=precision_bar,
        )
        if op is None:
            abstained_folds += 1
            preds.extend((q.person_id, None) for q in held)
            continue
        chosen.append(
            {
                "threshold": op.threshold,
                "margin": op.margin,
                "core_floor": op.core_floor,
                "n_enabled": float(len(op.enabled)),
            }
        )
        for q in held:
            preds.append(
                (q.person_id, predict(q, train, op.threshold, op.margin,
                                      aggregation=op.aggregation, allowed=op.enabled))
            )  # fmt: skip
    provenance = {
        "folds": len(meetings),
        "abstained_folds": abstained_folds,
        "n_positives": sum(1 for s in samples if s.person_id is not None),
        "n_negatives": sum(1 for s in samples if s.person_id is None),
        "chosen": chosen,
    }
    return score(preds), provenance
