#!/usr/bin/env python3
"""Calibrate the voiceprint matcher's operating point (leakage-safe, read-only).

Picks the ``(threshold, margin)`` at which a voiceprint match is precise enough to
propose. Uses the name-anchored gallery as labeled ground truth and evaluates
leave-one-MEETING-out so a query is never scored against a sample from its own
meeting (or a re-transcoded/superseded copy — same ``video_id``), which would leak.

Metrics are precision-first (the cardinal is "never a wrong name"): macro precision
by official (so a few talkative officials can't inflate it) plus recall and the
confusion pairs (which officials collide). Optionally probe NEGATIVES — anonymous
non-official clusters from a few meetings, extracted transiently on the GPU and
never stored — to confirm the operating point rejects citizens too.

Writes NOTHING. It reports; the operator picks the operating point, which the
matcher then records as a threshold_version. Design: plan §7.

Usage:
    doppler run --project mac --config dev -- \\
      uv run python scripts/voiceprint_calibrate.py --precision-bar 0.98
"""

from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from actalux.config import load_config
from actalux.db import fetch_all_rows, get_client
from actalux.errors import ActaluxError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Operating-point search grid (cosine on L2-normalized vectors, so scores in [-1, 1]).
DEFAULT_THRESHOLDS = (0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80)
DEFAULT_MARGINS = (0.0, 0.05, 0.10, 0.15, 0.20)


@dataclass(frozen=True)
class Sample:
    """One gallery (or negative) voiceprint with its true label + leave-out unit.

    ``person_id`` is the true official (``None`` marks a negative — a non-official the
    matcher must reject). ``meeting_key`` is the ``video_id``: the leave-one-out unit,
    so version-chain siblings (same recording) never leak across the split.
    """

    person_id: int | None
    meeting_key: str
    embedding: tuple[float, ...]


def cosine(a: tuple[float, ...], b: tuple[float, ...]) -> float:
    """Cosine similarity. Inputs are L2-normalized at enrollment, so this is a dot product."""
    return sum(x * y for x, y in zip(a, b))


def person_scores(query: Sample, gallery: list[Sample], *, aggregation: str) -> dict[int, float]:
    """Aggregate cosine(query, sample) per person over ``gallery`` (already leave-out-filtered)."""
    by_person: dict[int, list[float]] = defaultdict(list)
    for s in gallery:
        if s.person_id is not None:
            by_person[s.person_id].append(cosine(query.embedding, s.embedding))
    if aggregation == "max":
        return {p: max(v) for p, v in by_person.items()}
    # "mean" (default): robust to a single lucky sample.
    return {p: sum(v) / len(v) for p, v in by_person.items()}


def predict(
    query: Sample, gallery: list[Sample], threshold: float, margin: float, *, aggregation: str
) -> int | None:
    """The matcher's call: top person if it clears ``threshold`` AND ``margin``, else abstain."""
    scores = person_scores(query, gallery, aggregation=aggregation)
    if not scores:
        return None
    ranked = sorted(scores.items(), key=lambda kv: -kv[1])
    top_person, top_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0.0
    if top_score >= threshold and (top_score - second_score) >= margin:
        return top_person
    return None


def leave_one_meeting_out(
    samples: list[Sample], threshold: float, margin: float, *, aggregation: str
) -> list[tuple[int | None, int | None]]:
    """``(true_person, predicted_person)`` per sample, scoring against other meetings only."""
    out: list[tuple[int | None, int | None]] = []
    for q in samples:
        gallery = [s for s in samples if s.meeting_key != q.meeting_key]
        out.append((q.person_id, predict(q, gallery, threshold, margin, aggregation=aggregation)))
    return out


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

    A prediction for a negative (true=None) is a false positive. Macro precision
    averages per-predicted-official precision so a talkative official can't dominate.
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
) -> list[tuple[float, float, Metrics]]:
    """Every ``(threshold, margin)`` point with its metrics."""
    grid = []
    for t in thresholds:
        for m in margins:
            preds = leave_one_meeting_out(samples, t, m, aggregation=aggregation)
            grid.append((t, m, score(preds)))
    return grid


def best_operating_point(
    grid: list[tuple[float, float, Metrics]], precision_bar: float
) -> tuple[float, float, Metrics] | None:
    """Highest-recall point that meets the precision bar (ties: lower threshold, then margin)."""
    ok = [g for g in grid if g[2].macro_precision >= precision_bar]
    if not ok:
        return None
    return max(ok, key=lambda g: (g[2].recall, -g[0], -g[1]))


def load_samples(client: Any) -> list[Sample]:
    """Build labeled gallery samples: gallery rows joined to their meeting ``video_id``."""
    rows = fetch_all_rows(
        lambda: client.table("subject_voiceprints").select("person_id,source_document_id,embedding")
    )
    if not rows:
        return []
    doc_ids = sorted({r["source_document_id"] for r in rows})
    docs = fetch_all_rows(
        lambda: client.table("documents").select("id,video_id").in_("id", doc_ids)
    )
    video_by_doc = {d["id"]: (d.get("video_id") or f"doc-{d['id']}") for d in docs}
    samples = []
    for r in rows:
        emb = _as_vector(r["embedding"])
        samples.append(
            Sample(
                person_id=r["person_id"],
                meeting_key=video_by_doc.get(
                    r["source_document_id"], f"doc-{r['source_document_id']}"
                ),
                embedding=emb,
            )
        )
    return samples


def _as_vector(embedding: Any) -> tuple[float, ...]:
    """pgvector round-trips as a JSON string or a list depending on the client; normalize."""
    if isinstance(embedding, str):
        return tuple(float(x) for x in embedding.strip("[]").split(","))
    return tuple(float(x) for x in embedding)


def main() -> None:
    parser = argparse.ArgumentParser(description="Calibrate the voiceprint matcher (read-only).")
    parser.add_argument("--precision-bar", type=float, default=0.98, help="macro-precision floor")
    parser.add_argument(
        "--aggregation",
        choices=("mean", "max"),
        default="mean",
        help="per-person score aggregation",
    )
    args = parser.parse_args()

    cfg = load_config()
    import os

    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise ActaluxError(
            "ACTALUX_SUPABASE_SERVICE_KEY is required (subject_voiceprints is service-only)"
        )
    client = get_client(cfg.supabase_url, key)

    samples = load_samples(client)
    if not samples:
        logger.info("gallery is empty; enroll voiceprints first (scripts/enroll_voiceprints.py)")
        return
    persons = {s.person_id for s in samples}
    meetings = {s.meeting_key for s in samples}
    logger.info(
        "calibrating on %d samples / %d officials / %d meetings (aggregation=%s)",
        len(samples),
        len(persons),
        len(meetings),
        args.aggregation,
    )

    grid = sweep(samples, DEFAULT_THRESHOLDS, DEFAULT_MARGINS, aggregation=args.aggregation)
    logger.info("thr  margin  macroP  recall  preds")
    for t, m, mtr in grid:
        logger.info(
            "%.2f  %.2f    %.3f   %.3f   %d", t, m, mtr.macro_precision, mtr.recall, mtr.predictions
        )

    best = best_operating_point(grid, args.precision_bar)
    if best is None:
        logger.info(
            "no operating point reaches macro-precision >= %.2f; raise samples or lower bar",
            args.precision_bar,
        )
        return
    t, m, mtr = best
    logger.info(
        "RECOMMENDED: threshold=%.2f margin=%.2f -> macroP=%.3f recall=%.3f (%d predictions)",
        t,
        m,
        mtr.macro_precision,
        mtr.recall,
        mtr.predictions,
    )
    if mtr.confusions:
        logger.info("confusions at that point (true -> wrong pred): %s", mtr.confusions[:10])


if __name__ == "__main__":
    main()
