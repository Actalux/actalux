#!/usr/bin/env python3
"""Phase S0 of the stylometry evidence family: measure before wiring (GO/NO-GO).

Read-only. Builds per-person style profiles from TRUSTED text (confirmed anchors +
inferred_high roll-call/self-intro), then answers three questions on the existing corpus
(docs/architecture/stylometry-evidence-family.md §3):

  1. Discrimination: leave-one-cluster-out — can Burrows's Delta re-identify each held-out
     trusted cluster among the profiled people? Rank-1 accuracy + the same/different-person
     Delta distributions, stratified by cluster word count (this picks the word floor).
  2. Known-truth falsification: the 8 pre-tenure "Growe" anchors (temporally impossible)
     should attribute to Jason Wilson, and the two known alien anchors (Patel doc 2549,
     Poole doc 2127) should MISMATCH their labeled person's profile. If Delta cannot get
     these known cases right, the family is a NO-GO.
  3. Threshold candidates: the measured distributions from (1), so the S2 labeler's score
     and margin floors are chosen from data, never invented.

Method: Burrows's Delta (Burrows 2002; Evert et al. 2017, DSH 32(suppl 2)) — z-scored
relative frequencies of the top-K corpus-frequent words (function-word dominated, hence
topic-resistant); distance = mean |dz|. Deterministic, no LLM.

Usage:
    doppler run --project mac --config dev -- \\
      uv run python scripts/analyze_stylometry.py --state mo --place clayton
"""

from __future__ import annotations

import argparse
import logging
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass

import numpy as np
from supabase import Client

from actalux.config import load_config
from actalux.db import fetch_all_rows, get_client, get_diarization_turns, get_place_by_path
from actalux.errors import ActaluxError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Trusted profile sources (spec §7 Q1, operator-locked): human confirmations plus the two
# deterministic name-anchor bases at their clean tier. Discourse text is deliberately
# excluded so profile errors cannot inherit the discourse labeler's errors.
TRUSTED_HIGH_BASES = ("rollcall", "self_intro")
# Top-K most-frequent-word vocabulary sizes to sweep. The classic Delta literature uses
# 100-500 most-frequent words; the sweep shows whether the verdict is K-sensitive.
K_SWEEP = (50, 150, 300)
# Word-count strata for the discrimination report (picks the S2 word floor from data).
WORD_BINS = ((0, 250), (250, 500), (500, 1000), (1000, 2500), (2500, 10**9))
# The known-truth set (spec §3.3): pre-tenure Growe anchors are Wilson-era meetings where
# Growe could not have served (sworn 2022-04-20); the two aliens were proven wrong-voice
# acoustically (gallery cosine 0.06-0.10 to their person's confirmed centroid, 2026-07-09).
GROWE_PERSON_ID = 19
WILSON_PERSON_ID = 302
KNOWN_ALIENS = ((100, 2549, "SPEAKER_03"), (103, 2127, "SPEAKER_10"))  # (person, doc, cluster)

_WORD_RE = re.compile(r"[a-z']+")


@dataclass(frozen=True)
class ClusterText:
    """One anchored cluster's pooled transcript text (the stylometry sample unit)."""

    person_id: int
    name: str
    document_id: int
    cluster_label: str
    tokens: tuple[str, ...]


def _tokenize(words: list[dict]) -> list[str]:
    """Whisper word entries -> lowercase alphabetic tokens (apostrophes kept: don't/it's)."""
    out: list[str] = []
    for w in words:
        for m in _WORD_RE.finditer(str(w.get("word", "")).lower()):
            out.append(m.group(0))
    return out


def _trusted_clusters(client: Client, place_id: int) -> list[ClusterText]:
    """Every trusted-anchor cluster's pooled text, place-scoped."""
    subjects = {
        s["id"]: s
        for s in fetch_all_rows(
            lambda: client.table("subjects")
            .select("id,person_id,canonical_name")
            .eq("place_id", place_id)
        )
    }
    idents = fetch_all_rows(
        lambda: client.table("speaker_identities").select(
            "document_id,cluster_label,subject_id,confidence,basis"
        )
    )
    picked: list[tuple[int, str, int, str]] = []  # (doc, label, person, name)
    for i in idents:
        subject = subjects.get(i.get("subject_id"))
        if not subject or subject.get("person_id") is None:
            continue
        trusted = i["confidence"] == "confirmed" or (
            i["confidence"] == "inferred_high" and i["basis"] in TRUSTED_HIGH_BASES
        )
        if trusted:
            picked.append(
                (i["document_id"], i["cluster_label"], subject["person_id"], subject["canonical_name"])
            )

    out: list[ClusterText] = []
    for doc_id in sorted({d for d, _, _, _ in picked}):
        turns = get_diarization_turns(client, doc_id)
        words_by_label: dict[str, list[dict]] = defaultdict(list)
        for t in turns:
            words_by_label[t["cluster_label"]].extend(t.get("words") or [])
        for d, lab, person, name in picked:
            if d != doc_id:
                continue
            tokens = _tokenize(words_by_label.get(lab, []))
            if tokens:
                out.append(ClusterText(person, name, d, lab, tuple(tokens)))
    return out


def _cluster_text_for(client: Client, document_id: int, cluster_label: str) -> tuple[str, ...]:
    """One arbitrary cluster's pooled tokens (for the known-truth set)."""
    turns = get_diarization_turns(client, document_id)
    words: list[dict] = []
    for t in turns:
        if t["cluster_label"] == cluster_label:
            words.extend(t.get("words") or [])
    return tuple(_tokenize(words))


def _rel_freq(tokens: tuple[str, ...], vocab: list[str]) -> np.ndarray:
    counts = Counter(tokens)
    total = max(1, len(tokens))
    return np.asarray([counts.get(w, 0) / total for w in vocab], dtype=np.float64)


class DeltaModel:
    """Burrows's Delta over a fixed vocabulary, standardized on the trusted-cluster corpus."""

    def __init__(self, clusters: list[ClusterText], k: int) -> None:
        corpus = Counter()
        for c in clusters:
            corpus.update(c.tokens)
        self.vocab = [w for w, _ in corpus.most_common(k)]
        freqs = np.stack([_rel_freq(c.tokens, self.vocab) for c in clusters])
        self.mu = freqs.mean(axis=0)
        self.sigma = freqs.std(axis=0)
        self.sigma[self.sigma == 0] = 1.0

    def z(self, tokens: tuple[str, ...]) -> np.ndarray:
        return (_rel_freq(tokens, self.vocab) - self.mu) / self.sigma

    def profile(self, clusters: list[ClusterText]) -> np.ndarray:
        pooled: list[str] = []
        for c in clusters:
            pooled.extend(c.tokens)
        return self.z(tuple(pooled))

    @staticmethod
    def delta(a: np.ndarray, b: np.ndarray) -> float:
        return float(np.mean(np.abs(a - b)))


def _rank_against_profiles(
    model: DeltaModel, tokens: tuple[str, ...], profiles: dict[int, np.ndarray]
) -> list[tuple[int, float]]:
    zq = model.z(tokens)
    scored = [(p, model.delta(zq, zp)) for p, zp in profiles.items()]
    return sorted(scored, key=lambda t: t[1])  # smaller Delta = more similar


def _loco_report(clusters: list[ClusterText], k: int) -> dict[str, dict[str, float | int]]:
    """Leave-one-cluster-out attribution across profiled people; per-word-bin accuracy."""
    model = DeltaModel(clusters, k)
    by_person: dict[int, list[ClusterText]] = defaultdict(list)
    for c in clusters:
        by_person[c.person_id].append(c)

    same_deltas: list[float] = []
    diff_deltas: list[float] = []
    bins: dict[tuple[int, int], list[int]] = {b: [] for b in WORD_BINS}  # bin -> hits(0/1)
    margins_correct: list[float] = []
    for q in clusters:
        rest = [c for c in by_person[q.person_id] if c is not q]
        if not rest:
            continue  # a single-cluster person has no held-out profile to test against
        profiles = {
            p: model.profile([c for c in cs if c is not q])
            for p, cs in by_person.items()
            if [c for c in cs if c is not q]
        }
        ranked = _rank_against_profiles(model, q.tokens, profiles)
        top_person, top_delta = ranked[0]
        second_delta = ranked[1][1] if len(ranked) > 1 else float("inf")
        hit = int(top_person == q.person_id)
        same_deltas.append(
            next(d for p, d in ranked if p == q.person_id)
        )
        diff_deltas.extend(d for p, d in ranked if p != q.person_id)
        if hit:
            margins_correct.append(second_delta - top_delta)
        n = len(q.tokens)
        for lo, hi in WORD_BINS:
            if lo <= n < hi:
                bins[(lo, hi)].append(hit)

    def _stats(xs: list[float]) -> str:
        if not xs:
            return "n/a"
        a = np.asarray(xs)
        return f"min={a.min():.3f} med={np.median(a):.3f} max={a.max():.3f}"

    total_hits = sum(sum(v) for v in bins.values())
    total_n = sum(len(v) for v in bins.values())
    logger.info("K=%d: LOCO rank-1 accuracy %d/%d = %.3f", k, total_hits, total_n, total_hits / max(1, total_n))
    logger.info("  same-person Delta:      %s (n=%d)", _stats(same_deltas), len(same_deltas))
    logger.info("  different-person Delta: %s (n=%d)", _stats(diff_deltas), len(diff_deltas))
    logger.info("  margin when correct (second - top): %s", _stats(margins_correct))
    for (lo, hi), hits in bins.items():
        if hits:
            label = f"{lo}-{hi if hi < 10**9 else '+'}"
            logger.info("  words %-12s: %d/%d = %.3f", label, sum(hits), len(hits), sum(hits) / len(hits))
    return {
        "accuracy": {"hits": total_hits, "n": total_n},
    }


def _known_truth(client: Client, clusters: list[ClusterText], k: int) -> None:
    """Score the known-truth set (spec §3.3) against the trusted profiles."""
    model = DeltaModel(clusters, k)
    by_person: dict[int, list[ClusterText]] = defaultdict(list)
    for c in clusters:
        by_person[c.person_id].append(c)
    profiles = {p: model.profile(cs) for p, cs in by_person.items()}
    names = {c.person_id: c.name for c in clusters}

    logger.info("K=%d known-truth set:", k)

    subjects = {
        s["id"]: s.get("person_id")
        for s in fetch_all_rows(lambda: client.table("subjects").select("id,person_id"))
    }
    growe_subjects = [sid for sid, pid in subjects.items() if pid == GROWE_PERSON_ID]
    idents = fetch_all_rows(
        lambda: client.table("speaker_identities")
        .select("document_id,cluster_label,subject_id")
        .in_("subject_id", growe_subjects)
    )
    docs = {
        d["id"]: d.get("meeting_date")
        for d in fetch_all_rows(
            lambda: client.table("documents")
            .select("id,meeting_date")
            .in_("id", sorted({i["document_id"] for i in idents}))
        )
    }
    pre_tenure = [
        i for i in idents if (docs.get(i["document_id"]) or "9999") < "2022-04-20"
    ]
    for i in sorted(pre_tenure, key=lambda r: r["document_id"]):
        tokens = _cluster_text_for(client, i["document_id"], i["cluster_label"])
        if len(tokens) == 0:
            logger.info("  pre-tenure 'Growe' doc %s %s: no text", i["document_id"], i["cluster_label"])
            continue
        ranked = _rank_against_profiles(model, tokens, profiles)
        top3 = ", ".join(f"{names.get(p, p)}={d:.3f}" for p, d in ranked[:3])
        verdict = "-> WILSON" if ranked[0][0] == WILSON_PERSON_ID else ""
        logger.info(
            "  pre-tenure 'Growe' doc %s %s (%d words): %s %s",
            i["document_id"], i["cluster_label"], len(tokens), top3, verdict,
        )

    for person, doc_id, lab in KNOWN_ALIENS:
        tokens = _cluster_text_for(client, doc_id, lab)
        if not tokens or person not in profiles:
            logger.info("  alien doc %s %s: no text or no profile", doc_id, lab)
            continue
        ranked = _rank_against_profiles(model, tokens, profiles)
        own = next(d for p, d in ranked if p == person)
        rank_of_own = next(idx for idx, (p, _) in enumerate(ranked) if p == person) + 1
        logger.info(
            "  alien doc %s %s labeled %s (%d words): Delta-to-own=%.3f (rank %d/%d); best=%s (%.3f)",
            doc_id, lab, names.get(person, person), len(tokens), own, rank_of_own,
            len(ranked), names.get(ranked[0][0], ranked[0][0]), ranked[0][1],
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Stylometry S0: measure discrimination (GO/NO-GO).")
    parser.add_argument("--state", required=True)
    parser.add_argument("--place", required=True)
    args = parser.parse_args()

    cfg = load_config()
    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise ActaluxError("ACTALUX_SUPABASE_SERVICE_KEY is required (speaker tables are service-only)")
    client = get_client(cfg.supabase_url, key)
    place = get_place_by_path(client, args.state, args.place)
    if not place:
        raise ActaluxError(f"no place {args.state}/{args.place}")

    clusters = _trusted_clusters(client, place["id"])
    people = {c.person_id for c in clusters}
    logger.info(
        "trusted clusters: %d across %d people; total words %d",
        len(clusters), len(people), sum(len(c.tokens) for c in clusters),
    )
    for k in K_SWEEP:
        _loco_report(clusters, k)
    _known_truth(client, clusters, K_SWEEP[1])


if __name__ == "__main__":
    main()
