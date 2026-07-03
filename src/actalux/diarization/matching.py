"""Voiceprint matcher math — scoring, gating, and leakage-safe calibration.

Pure library (no DB, no GPU): given labeled voiceprint ``Sample``s it scores a query
against a gallery, applies Gate A enablement (labelqa) + Gate B purity floor, and estimates
the operating point under leave-one-meeting-out (and nested LOMO, which removes
operating-point overfit).

Shared by the calibration CLI (``scripts/voiceprint_calibrate.py``), the recalibration
harness (``scripts/recalibrate_voiceprints.py``), and — later — the live matcher.
Design: docs/architecture/voiceprint-recalibration-plan.md §5.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from actalux.diarization.labelqa import (
    coherent_core,
    coherent_core_asnorm,
    collapse_suspects,
)

# Operating-point search grid (cosine on L2-normalized vectors, so scores in [-1, 1]).
# Threshold grid runs to 0.95 because the diagnostic same-person p90 is 0.937 — a 0.90
# ceiling clips the wespeaker distribution just below where a precise operating point can live.
DEFAULT_THRESHOLDS = (0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.92, 0.95)
DEFAULT_MARGINS = (0.0, 0.05, 0.10, 0.15, 0.20)
DEFAULT_AGGREGATIONS = ("mean", "max")
# Gate A (label quality) core_floor + Gate B purity_floor are swept and refit; min_core is a
# fixed structural safeguard (plan §7).
DEFAULT_CORE_FLOORS = (0.30, 0.40, 0.50)
DEFAULT_PURITY_FLOORS = (0.0, 0.30, 0.50)
# Gate A scoring mode: "none" thresholds raw self-coherence (core_floor); "asnorm" z-scores it
# against the impostor cohort and thresholds z_floor (E#1, plan §5). Both are swept.
DEFAULT_SCORE_NORMS = ("none", "asnorm")
# z-space floors for asnorm mode. A raw cosine floor (0.30–0.50) is meaningless once coherence is
# measured in impostor-cohort standard deviations, so asnorm sweeps its own scale: a genuine
# official sits a few σ above the cross-official cosine cloud.
DEFAULT_Z_FLOORS = (1.0, 2.0, 3.0)
# collapse_bound is cosine-scale-dependent (a wespeaker 0.90 near-duplicate is a lower cosine on
# another embedder), so it is swept rather than pinned to one embedder's scale.
DEFAULT_COLLAPSE_BOUNDS = (0.80, 0.85, 0.90)
GATE_A_MIN_CORE = 2
# An official whose TRAIN samples carry human confirmations from at least this many distinct
# meetings is enabled on that trusted core even if raw coherence fails (lever B). Two-in-train is
# the floor a Gate-A positive needs; the confirm CLI targets three confirmed meetings so at least
# two survive in every leave-one-meeting-out fold.
GATE_A_CONFIRMED_MIN_MEETINGS = 2
# Single-config default for enabled_officials; the sweep varies DEFAULT_COLLAPSE_BOUNDS instead.
GATE_A_COLLAPSE_BOUND = 0.85
# Held-out recall is reported split by the held-out sample's confidence tier: mixing confirmed
# (human-verified) positives with possibly-mislabeled inferred ones dilutes the honest read.
CONFIDENCE_TIERS = ("confirmed", "inferred_high", "other")
# asnorm cohort guards: too few impostor scores, or a near-zero spread, leaves no z-scale, so the
# sample falls back to the raw self-coherence test instead of dividing by ~0.
ASNORM_MIN_COHORT = 3
ASNORM_SIGMA_EPS = 1e-6
# Precision bars the reporting curve is swept over (reporting only; the persisted verdict uses the
# run's own --precision-bar).
CURVE_PRECISION_BARS = (0.80, 0.85, 0.90, 0.95, 0.98)


@dataclass
class Sample:
    """One gallery (or negative) voiceprint with its true label + leave-out unit.

    ``person_id`` is the true official (``None`` marks a negative — a non-official the
    matcher must reject). ``meeting_key`` is the ``video_id``: the leave-one-out unit, so
    version-chain siblings (same recording) never leak across the split. ``purity`` is the
    Gate-B pooling purity (used to sweep a purity floor); ``idx`` is assigned by
    ``build_sim`` for the precomputed-similarity fast path. ``confidence`` is the
    speaker-identity tier the sample was drawn from (``confirmed`` marks a human-verified
    label): Gate A trusts confirmed samples as a core even when raw coherence fails, and the
    honest recall estimate is split by the held-out sample's tier. The default is a neutral
    non-confirmed tier, so a gallery built without confirmations behaves exactly as before.
    """

    person_id: int | None
    meeting_key: str
    embedding: tuple[float, ...]
    purity: float = 1.0
    idx: int = -1
    confidence: str = "inferred_high"


def cosine(a: tuple[float, ...], b: tuple[float, ...]) -> float:
    """Cosine similarity. Inputs are L2-normalized at enrollment, so this is a dot product."""
    return sum(x * y for x, y in zip(a, b))


def as_vector(embedding: Any) -> tuple[float, ...]:
    """pgvector round-trips as a JSON string or a list depending on the client; normalize."""
    if isinstance(embedding, str):
        return tuple(float(x) for x in embedding.strip("[]").split(","))
    return tuple(float(x) for x in embedding)


def build_sim(samples: list[Sample]) -> list[list[float]]:
    """Assign each sample an ``idx`` and return the n×n cosine matrix (fast-path substrate).

    The sweep evaluates thousands of (purity, core, threshold, margin, aggregation) points;
    recomputing 256-d dot products each time is the bottleneck. Precomputing the matrix once
    (numpy) and indexing it turns each score into an O(1) lookup. Rows are L2-normalized
    defensively so the matrix is cosine even if an input drifted.
    """
    if not samples:
        return []
    for i, s in enumerate(samples):
        s.idx = i
    mat = np.asarray([s.embedding for s in samples], dtype=np.float64)
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    mat = mat / norms
    return (mat @ mat.T).tolist()


def person_scores(
    query: Sample,
    gallery: list[Sample],
    *,
    aggregation: str,
    allowed: set[int] | None = None,
    sim: list[list[float]] | None = None,
) -> dict[int, float]:
    """Aggregate cosine(query, sample) per person over ``gallery`` (already leave-out-filtered).

    ``allowed`` restricts the gallery to enabled officials (Gate A); ``None`` means all.
    ``sim`` (from ``build_sim``) is the precomputed-cosine fast path. Negatives (person_id
    None) are never in the gallery.
    """
    by_person: dict[int, list[float]] = defaultdict(list)
    for s in gallery:
        if s.person_id is None:
            continue
        if allowed is not None and s.person_id not in allowed:
            continue
        c = sim[query.idx][s.idx] if sim is not None else cosine(query.embedding, s.embedding)
        by_person[s.person_id].append(c)
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
    sim: list[list[float]] | None = None,
) -> int | None:
    """The matcher's call: top person if it clears ``threshold`` AND ``margin``, else abstain."""
    scores = person_scores(query, gallery, aggregation=aggregation, allowed=allowed, sim=sim)
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
    sim: list[list[float]] | None = None,
) -> list[tuple[int | None, int | None]]:
    """``(true_person, predicted_person)`` per sample, scoring against other meetings only."""
    out: list[tuple[int | None, int | None]] = []
    for q in samples:
        gallery = [s for s in samples if s.meeting_key != q.meeting_key]
        out.append(
            (
                q.person_id,
                predict(
                    q, gallery, threshold, margin, aggregation=aggregation, allowed=allowed, sim=sim
                ),
            )
        )
    return out


def enabled_officials(
    train: list[Sample],
    *,
    core_floor: float,
    min_core: int,
    collapse_bound: float,
    score_norm: str = "none",
    z_floor: float | None = None,
    cohort_min: int = ASNORM_MIN_COHORT,
    sigma_eps: float = ASNORM_SIGMA_EPS,
    confirmed_min_meetings: int = GATE_A_CONFIRMED_MIN_MEETINGS,
) -> set[int]:
    """Gate A: officials from ``train`` with a cross-meeting coherent core and no collapse.

    An official is enabled only if their voiceprints mutually agree (coherent core) AND their
    voice is not near-duplicate with a *different* official's (a roll-call caller labeled under
    several names). ``score_norm='asnorm'`` z-scores each sample's self-coherence against the
    impostor cohort — every OTHER official's train vectors — and thresholds ``z_floor``, so a
    modestly-coherent but clearly-non-impostor official can still enable; ``core_floor`` is then
    only the degenerate-cohort fallback floor. Collapse detection stays on RAW cosine regardless
    of ``score_norm``: it measures absolute similarity to another person's anchors, which
    normalizing away would defeat. Negatives (person_id None) are ignored, so they never enter the
    impostor cohort. Applied ONLY within training folds (leakage-safe).

    Human confirmations (lever B) enter here as a trusted core: an official whose train samples
    carry ``confidence='confirmed'`` from at least ``confirmed_min_meetings`` distinct meetings is
    enabled even if raw coherence fails (a genuine official whose voiceprints scatter across noisy
    rooms is still that official once a human has vouched across meetings). The confirmation relaxes
    ONLY the coherence test — the collapse guard is applied first and unchanged, so a confirmed
    official whose voice near-duplicates a different official's is still excluded: confirming one
    name can't make a one-voice-two-names collapse into two people, and precision comes first. Only
    train confirmations count, so the estimate stays fold-safe (a held-out meeting's confirmations
    never enable an official in its own fold).
    """
    by_person: dict[int, list[tuple[float, ...]]] = defaultdict(list)
    confirmed_meetings: dict[int, set[str]] = defaultdict(set)
    for s in train:
        if s.person_id is None:
            continue
        by_person[s.person_id].append(s.embedding)
        if s.confidence == "confirmed":
            confirmed_meetings[s.person_id].add(s.meeting_key)

    suspects = collapse_suspects(
        [(p, v) for p, vs in by_person.items() for v in vs], collapse_bound=collapse_bound
    )
    enabled: set[int] = set()
    for person, vecs in by_person.items():
        if person in suspects:
            continue  # collapse guard first: a confirmation never rescues a two-name voice
        if len(confirmed_meetings[person]) >= confirmed_min_meetings:
            enabled.add(person)  # trusted core: human-confirmed across meetings, coherence waived
            continue
        if score_norm == "asnorm":
            cohort = [v for other, ovecs in by_person.items() if other != person for v in ovecs]
            core = coherent_core_asnorm(
                vecs,
                cohort,
                z_floor=z_floor if z_floor is not None else 0.0,
                min_core=min_core,
                min_cohort=cohort_min,
                sigma_eps=sigma_eps,
                raw_fallback_floor=core_floor,
            )
        else:
            core = coherent_core(vecs, core_floor=core_floor, min_core=min_core)
        if core:
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
    sim: list[list[float]] | None = None,
) -> list[tuple[float, float, Metrics]]:
    """Every ``(threshold, margin)`` point with its metrics (one aggregation, fixed gallery)."""
    grid = []
    for t in thresholds:
        for m in margins:
            preds = leave_one_meeting_out(
                samples, t, m, aggregation=aggregation, allowed=allowed, sim=sim
            )
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

    purity_floor: float
    core_floor: float
    threshold: float
    margin: float
    aggregation: str
    enabled: set[int]
    metrics: Metrics
    score_norm: str = "none"
    collapse_bound: float = GATE_A_COLLAPSE_BOUND
    z_floor: float | None = None


@dataclass
class GridPoint:
    """One evaluated configuration and its leave-one-meeting-out metrics (``precision_bar``-free).

    Carries every swept axis so a Pareto point can report which knobs produced its (precision,
    recall) — including the AS-norm mode and z-floor. The same grid is selected against every bar
    the curve sweeps, so it is computed once per fold and reused.
    """

    purity_floor: float
    collapse_bound: float
    score_norm: str
    core_floor: float
    z_floor: float | None
    aggregation: str
    threshold: float
    margin: float
    enabled: frozenset[int]
    metrics: Metrics


def _split_confusions(metrics: Metrics) -> tuple[int, int]:
    """Split confusions into (citizen→official false positives, official↔official confusions).

    The operator's split-bar policy treats citizen false positives (a negative matched to an
    official) as hard-zero-forever while official confusions may one day tolerate a small rate;
    reporting them apart at every operating point is what lets that call be made on data.
    """
    citizen_fp = sum(1 for true, _ in metrics.confusions if true is None)
    official = sum(1 for true, _ in metrics.confusions if true is not None)
    return citizen_fp, official


# One query's leave-one-meeting-out ranking: (true, top_person, top_score, second_score).
_Ranking = list[tuple[int | None, int | None, float, float]]


def _rank_lomo(
    samples: list[Sample],
    *,
    aggregation: str,
    allowed: set[int],
    sim: list[list[float]] | None,
) -> _Ranking:
    """Per query: (true, top_person, top_score, second_score) under leave-one-meeting-out.

    The person scores depend only on (aggregation, enabled gallery) — NOT on threshold/margin,
    which merely gate them. Ranking once here lets the (threshold, margin) sweep be O(1) compares
    instead of re-scoring the gallery per grid cell, the sweep's dominant cost. Semantically this
    is exactly what ``predict`` computes, factored out of the inner loop.
    """
    ranked: _Ranking = []
    for q in samples:
        gallery = [s for s in samples if s.meeting_key != q.meeting_key]
        scores = person_scores(q, gallery, aggregation=aggregation, allowed=allowed, sim=sim)
        if not scores:
            ranked.append((q.person_id, None, 0.0, 0.0))
            continue
        order = sorted(scores.items(), key=lambda kv: -kv[1])
        top_person, top_score = order[0]
        second = order[1][1] if len(order) > 1 else 0.0
        ranked.append((q.person_id, top_person, top_score, second))
    return ranked


def _preds_at(
    ranked: _Ranking, threshold: float, margin: float
) -> list[tuple[int | None, int | None]]:
    """Gate precomputed rankings by (threshold, margin) — ``predict``'s body over the sweep."""
    out: list[tuple[int | None, int | None]] = []
    for true, top_person, top_score, second in ranked:
        if top_person is not None and top_score >= threshold and (top_score - second) >= margin:
            out.append((true, top_person))
        else:
            out.append((true, None))
    return out


def evaluate_grid(
    samples: list[Sample],
    *,
    thresholds: tuple[float, ...] = DEFAULT_THRESHOLDS,
    margins: tuple[float, ...] = DEFAULT_MARGINS,
    aggregations: tuple[str, ...] = DEFAULT_AGGREGATIONS,
    core_floors: tuple[float, ...] = DEFAULT_CORE_FLOORS,
    purity_floors: tuple[float, ...] = DEFAULT_PURITY_FLOORS,
    z_floors: tuple[float, ...] = DEFAULT_Z_FLOORS,
    collapse_bounds: tuple[float, ...] = DEFAULT_COLLAPSE_BOUNDS,
    score_norms: tuple[str, ...] = DEFAULT_SCORE_NORMS,
    min_core: int = GATE_A_MIN_CORE,
) -> list[GridPoint]:
    """Every (purity, collapse, score_norm, floor, aggregation, threshold, margin) point + metrics.

    ``precision_bar``-independent: the objective and the precision↔recall curve both select from
    this grid, so the curve reuses one grid per fold across all bars. Raw and asnorm floors live on
    different scales, so each mode sweeps its own floor axis; a degenerate asnorm cohort falls back
    to the raw self-coherence test at the strictest swept raw floor (precision-first: when a sample
    can't be normalized we do not get more permissive).
    """
    unknown = set(score_norms) - {"none", "asnorm"}
    if unknown:
        raise ValueError(f"unknown score_norm(s): {sorted(unknown)} (expected 'none' / 'asnorm')")
    points: list[GridPoint] = []
    asnorm_fallback = max(core_floors)
    for pf in purity_floors:
        filtered = [s for s in samples if s.purity >= pf]
        if len(filtered) < min_core:
            continue
        sim = build_sim(filtered)
        # A positive below the purity floor is rejected by Gate B (not matchable), so it counts
        # as a recall MISS — not silently dropped — or a higher floor would look costless.
        below_floor_misses = [
            (s.person_id, None) for s in samples if s.person_id is not None and s.purity < pf
        ]
        # Rankings depend only on (aggregation, enabled) for this purity subset's fixed sim, so
        # cache them: distinct (collapse, floor) configs often produce the same enabled set.
        rank_cache: dict[tuple[str, frozenset[int]], _Ranking] = {}
        for collapse_bound in collapse_bounds:
            for score_norm in score_norms:
                is_asnorm = score_norm == "asnorm"
                floors = z_floors if is_asnorm else core_floors
                for fl in floors:
                    core_floor = asnorm_fallback if is_asnorm else fl
                    z_floor = fl if is_asnorm else None
                    enabled = enabled_officials(
                        filtered,
                        core_floor=core_floor,
                        min_core=min_core,
                        collapse_bound=collapse_bound,
                        score_norm=score_norm,
                        z_floor=z_floor,
                    )
                    if not enabled:
                        continue
                    frozen = frozenset(enabled)
                    for agg in aggregations:
                        ranked = rank_cache.get((agg, frozen))
                        if ranked is None:
                            ranked = _rank_lomo(filtered, aggregation=agg, allowed=enabled, sim=sim)
                            rank_cache[(agg, frozen)] = ranked
                        for t in thresholds:
                            for mgn in margins:
                                mtr = score(_preds_at(ranked, t, mgn) + below_floor_misses)
                                points.append(
                                    GridPoint(
                                        purity_floor=pf,
                                        collapse_bound=collapse_bound,
                                        score_norm=score_norm,
                                        core_floor=core_floor,
                                        z_floor=z_floor,
                                        aggregation=agg,
                                        threshold=t,
                                        margin=mgn,
                                        enabled=frozen,
                                        metrics=mtr,
                                    )
                                )
    return points


def best_from_grid(grid: list[GridPoint], precision_bar: float) -> OperatingPoint | None:
    """Highest-recall grid point clearing the precision bar; conservative tie-break (plan §5).

    Ties on recall resolve toward the higher threshold, then higher margin — the precision-first
    cardinal ("never a wrong name") settles a tie on stricter matching. This is exactly the
    pre-asnorm objective; the added axes only widen the grid it selects from. Grid order is fixed
    (``evaluate_grid``), so first-wins ties on (recall, threshold, margin) are deterministic.
    """
    best: OperatingPoint | None = None
    best_key: tuple[float, float, float] | None = None
    for gp in grid:
        if gp.metrics.macro_precision < precision_bar:
            continue
        key = (gp.metrics.recall, gp.threshold, gp.margin)
        if best_key is None or key > best_key:
            best_key = key
            best = OperatingPoint(
                purity_floor=gp.purity_floor,
                core_floor=gp.core_floor,
                threshold=gp.threshold,
                margin=gp.margin,
                aggregation=gp.aggregation,
                enabled=set(gp.enabled),
                metrics=gp.metrics,
                score_norm=gp.score_norm,
                collapse_bound=gp.collapse_bound,
                z_floor=gp.z_floor,
            )
    return best


def pareto_frontier(grid: list[GridPoint]) -> list[dict[str, Any]]:
    """The non-dominated (precision, recall) points, each tagged with the knobs that produced it.

    Reporting only: the persisted operating point is chosen by ``best_from_grid`` at the run's
    precision bar, never from this frontier. A point is on the frontier if no other point beats it
    on both precision and recall. Duplicate (precision, recall) collapse to the most conservative
    representative (higher threshold, then margin) so the frontier is one row per trade-off.
    """
    best_by_pr: dict[tuple[float, float], GridPoint] = {}
    for gp in grid:
        pr = (round(gp.metrics.macro_precision, 9), round(gp.metrics.recall, 9))
        cur = best_by_pr.get(pr)
        if cur is None or (gp.threshold, gp.margin) > (cur.threshold, cur.margin):
            best_by_pr[pr] = gp
    candidates = list(best_by_pr.values())
    frontier: list[dict[str, Any]] = []
    for gp in candidates:
        p, r = gp.metrics.macro_precision, gp.metrics.recall
        dominated = any(
            o is not gp
            and o.metrics.macro_precision >= p
            and o.metrics.recall >= r
            and (o.metrics.macro_precision > p or o.metrics.recall > r)
            for o in candidates
        )
        if dominated:
            continue
        citizen_fp, official = _split_confusions(gp.metrics)
        frontier.append(
            {
                "precision": round(p, 4),
                "recall": round(r, 4),
                "threshold": gp.threshold,
                "margin": gp.margin,
                "core_floor": gp.core_floor,
                "z_floor": gp.z_floor,
                "collapse_bound": gp.collapse_bound,
                "score_norm": gp.score_norm,
                "citizen_fp": citizen_fp,
                "official_confusion_count": official,
            }
        )
    frontier.sort(key=lambda d: (d["recall"], d["precision"]))
    return frontier


def select_operating_point(
    samples: list[Sample],
    *,
    thresholds: tuple[float, ...] = DEFAULT_THRESHOLDS,
    margins: tuple[float, ...] = DEFAULT_MARGINS,
    aggregations: tuple[str, ...] = DEFAULT_AGGREGATIONS,
    core_floors: tuple[float, ...] = DEFAULT_CORE_FLOORS,
    purity_floors: tuple[float, ...] = DEFAULT_PURITY_FLOORS,
    z_floors: tuple[float, ...] = DEFAULT_Z_FLOORS,
    collapse_bounds: tuple[float, ...] = DEFAULT_COLLAPSE_BOUNDS,
    score_norms: tuple[str, ...] = DEFAULT_SCORE_NORMS,
    min_core: int = GATE_A_MIN_CORE,
    precision_bar: float,
) -> OperatingPoint | None:
    """Pick the swept config maximizing recall@bar (Gate B purity + Gate A enablement + matcher).

    Evaluates the full grid (``evaluate_grid``) then selects the highest-recall point clearing the
    precision bar (conservative tie-break). Used per training fold (nested) and for the full-data
    refit. Returns ``None`` if nothing clears the bar.
    """
    grid = evaluate_grid(
        samples,
        thresholds=thresholds,
        margins=margins,
        aggregations=aggregations,
        core_floors=core_floors,
        purity_floors=purity_floors,
        z_floors=z_floors,
        collapse_bounds=collapse_bounds,
        score_norms=score_norms,
        min_core=min_core,
    )
    return best_from_grid(grid, precision_bar)


def _score_held_out(
    held: list[Sample], train: list[Sample], op: OperatingPoint
) -> list[tuple[Sample, int | None]]:
    """Score a held-out meeting's clusters against the training gallery at ``op`` (no refit).

    Returns ``(held_sample, predicted_person)`` so the caller can read both the true label and the
    held-out confidence tier off the sample. A held-out cluster below the chosen purity floor is
    not matchable (Gate B would reject it): a positive becomes a recall miss, a negative is simply
    never presented to the matcher (dropped from the output). This is the leakage-safe scoring
    step — enablement/params come only from ``train``.
    """
    gallery = [s for s in train if s.purity >= op.purity_floor]
    out: list[tuple[Sample, int | None]] = []
    for q in held:
        if q.purity < op.purity_floor:
            if q.person_id is not None:
                out.append((q, None))
            continue
        pred = predict(
            q, gallery, op.threshold, op.margin, aggregation=op.aggregation, allowed=op.enabled
        )
        out.append((q, pred))
    return out


def recall_by_confidence(
    records: list[tuple[str, int | None, int | None]],
) -> dict[str, dict[str, Any]]:
    """Held-out recall split by the held-out POSITIVE's confidence tier (reporting only).

    ``records`` are ``(confidence, true_person, predicted_person)`` for held-out samples across all
    folds. Only positives (``true`` is not None) count toward recall; negatives never do. Every
    tier in ``CONFIDENCE_TIERS`` is reported even when empty (``recall`` is None then), so the
    split is stable to read. Officials only — a record never carries a citizen identifier.
    """
    tallies: dict[str, list[int]] = {t: [0, 0] for t in CONFIDENCE_TIERS}  # [recalled, positives]
    for confidence, true, pred in records:
        if true is None:
            continue
        tier = confidence if confidence in CONFIDENCE_TIERS else "other"
        tallies[tier][1] += 1
        if pred == true:
            tallies[tier][0] += 1
    return {
        tier: {
            "positives": positives,
            "recalled": recalled,
            "recall": round(recalled / positives, 4) if positives else None,
        }
        for tier, (recalled, positives) in tallies.items()
    }


def nested_lomo_multi_bar(
    samples: list[Sample],
    *,
    precision_bars: tuple[float, ...],
    thresholds: tuple[float, ...] = DEFAULT_THRESHOLDS,
    margins: tuple[float, ...] = DEFAULT_MARGINS,
    aggregations: tuple[str, ...] = DEFAULT_AGGREGATIONS,
    core_floors: tuple[float, ...] = DEFAULT_CORE_FLOORS,
    purity_floors: tuple[float, ...] = DEFAULT_PURITY_FLOORS,
    z_floors: tuple[float, ...] = DEFAULT_Z_FLOORS,
    collapse_bounds: tuple[float, ...] = DEFAULT_COLLAPSE_BOUNDS,
    score_norms: tuple[str, ...] = DEFAULT_SCORE_NORMS,
    min_core: int = GATE_A_MIN_CORE,
) -> dict[float, tuple[Metrics, dict[str, Any]]]:
    """Nested leave-one-meeting-out at several precision bars, reusing one grid per fold.

    The per-fold grid (``evaluate_grid`` on the OTHER meetings) is the whole cost; selecting a
    different bar from it and re-scoring the held-out meeting is cheap, so the precision↔recall
    curve costs one nested pass, not one per bar. Each bar's result is identical to a standalone
    ``nested_leave_one_meeting_out`` at that bar (same grid, same selection, same held-out
    scoring). The grid is built from training meetings only, so no held-out sample reaches the
    cohort stats or the selection.
    """
    meetings = sorted({s.meeting_key for s in samples})
    preds_by_bar: dict[float, list[tuple[int | None, int | None]]] = {b: [] for b in precision_bars}
    # (confidence, true, pred) per held-out sample, for the recall-by-tier split (reporting only).
    records_by_bar: dict[float, list[tuple[str, int | None, int | None]]] = {
        b: [] for b in precision_bars
    }
    chosen_by_bar: dict[float, list[dict[str, Any]]] = {b: [] for b in precision_bars}
    abstained_by_bar: dict[float, int] = {b: 0 for b in precision_bars}
    for mk in meetings:
        held = [s for s in samples if s.meeting_key == mk]
        train = [s for s in samples if s.meeting_key != mk]
        grid = evaluate_grid(
            train,
            thresholds=thresholds,
            margins=margins,
            aggregations=aggregations,
            core_floors=core_floors,
            purity_floors=purity_floors,
            z_floors=z_floors,
            collapse_bounds=collapse_bounds,
            score_norms=score_norms,
            min_core=min_core,
        )
        for b in precision_bars:
            op = best_from_grid(grid, b)
            if op is None:
                abstained_by_bar[b] += 1
                preds_by_bar[b].extend((q.person_id, None) for q in held)
                records_by_bar[b].extend((q.confidence, q.person_id, None) for q in held)
                continue
            chosen_by_bar[b].append(
                {
                    "purity_floor": op.purity_floor,
                    "core_floor": op.core_floor,
                    "threshold": op.threshold,
                    "margin": op.margin,
                    "n_enabled": float(len(op.enabled)),
                    "score_norm": op.score_norm,
                    "collapse_bound": op.collapse_bound,
                    "z_floor": op.z_floor,
                }
            )
            scored = _score_held_out(held, train, op)
            preds_by_bar[b].extend((q.person_id, pred) for q, pred in scored)
            records_by_bar[b].extend((q.confidence, q.person_id, pred) for q, pred in scored)
    n_positives = sum(1 for s in samples if s.person_id is not None)
    n_negatives = sum(1 for s in samples if s.person_id is None)
    result: dict[float, tuple[Metrics, dict[str, Any]]] = {}
    for b in precision_bars:
        provenance = {
            "folds": len(meetings),
            "abstained_folds": abstained_by_bar[b],
            "n_positives": n_positives,
            "n_negatives": n_negatives,
            "chosen": chosen_by_bar[b],
            "recall_by_confidence": recall_by_confidence(records_by_bar[b]),
        }
        result[b] = (score(preds_by_bar[b]), provenance)
    return result


def nested_leave_one_meeting_out(
    samples: list[Sample],
    *,
    thresholds: tuple[float, ...] = DEFAULT_THRESHOLDS,
    margins: tuple[float, ...] = DEFAULT_MARGINS,
    aggregations: tuple[str, ...] = DEFAULT_AGGREGATIONS,
    core_floors: tuple[float, ...] = DEFAULT_CORE_FLOORS,
    purity_floors: tuple[float, ...] = DEFAULT_PURITY_FLOORS,
    z_floors: tuple[float, ...] = DEFAULT_Z_FLOORS,
    collapse_bounds: tuple[float, ...] = DEFAULT_COLLAPSE_BOUNDS,
    score_norms: tuple[str, ...] = DEFAULT_SCORE_NORMS,
    min_core: int = GATE_A_MIN_CORE,
    precision_bar: float,
) -> tuple[Metrics, dict[str, Any]]:
    """Honest performance estimate: params/enablement chosen per fold from OTHER meetings.

    A single-bar view of ``nested_lomo_multi_bar`` (one mechanism for one bar and for the curve's
    many). Outer loop holds out one meeting; the operating point is selected on the remaining
    meetings only, then the held-out positives AND negatives are scored unfiltered against the
    training gallery — removing the lucky-operating-point circularity (plan §5).
    """
    return nested_lomo_multi_bar(
        samples,
        precision_bars=(precision_bar,),
        thresholds=thresholds,
        margins=margins,
        aggregations=aggregations,
        core_floors=core_floors,
        purity_floors=purity_floors,
        z_floors=z_floors,
        collapse_bounds=collapse_bounds,
        score_norms=score_norms,
        min_core=min_core,
    )[precision_bar]
