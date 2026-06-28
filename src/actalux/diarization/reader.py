"""Reader-side speaker display: overlay the gated speaker timeline onto a transcript.

Pure + dict-based — operates on the rows from ``db.get_diarization_turns`` /
``db.get_speaker_identities``, so the web layer renders speaker labels (within a chunk,
or for the whole meeting / Ledger clip API) without recomputing attribution or touching
a GPU. Identity gating is already applied by the database (anon RLS returns only
inferred_high / confirmed identities); nothing here re-derives or re-gates identity.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

# A speaker's identity is public ONLY at these confidence levels. The DB enforces this
# via RLS for the anon client; we re-enforce it here so a service-key caller (which sees
# every row) can never surface a name for an ungated cluster (defense in depth).
_PUBLIC_CONFIDENCE = frozenset({"inferred_high", "confirmed"})


def resolve_speakers(identity_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Index gated identities by ``cluster_label -> {name, slug, confidence, basis}``.

    Only publicly-displayable rows are kept: confidence must be high/confirmed AND the
    row must name a subject. This holds even for a service-key caller that can see
    ungated rows — those never surface a name.
    """
    resolved: dict[str, dict[str, Any]] = {}
    for row in identity_rows:
        if row.get("confidence") not in _PUBLIC_CONFIDENCE:
            continue
        subject = row.get("subject") or {}
        name = subject.get("canonical_name")
        if not name:
            continue
        resolved[row["cluster_label"]] = {
            "name": name,
            "slug": subject.get("slug"),
            "confidence": row.get("confidence"),
            "basis": row.get("basis"),
        }
    return resolved


def clusters_in_window(turns: list[dict[str, Any]], start_s: float, end_s: float) -> list[str]:
    """Distinct cluster labels whose turn overlaps ``[start_s, end_s)``, first-appearance order.

    The per-chunk speaker set: intersect a chunk's time window with the turn timeline.
    ``turns`` are time-ordered ``diarization_turns`` rows.
    """
    if start_s >= end_s:  # a zero-width (or inverted) window contains no speaker
        return []
    seen: list[str] = []
    for t in turns:
        if t["start_seconds"] < end_s and start_s < t["end_seconds"]:
            if t["cluster_label"] not in seen:
                seen.append(t["cluster_label"])
    return seen


def speakers_in_window(
    turns: list[dict[str, Any]],
    identities: dict[str, dict[str, Any]],
    start_s: float,
    end_s: float,
) -> list[dict[str, Any]]:
    """The speakers heard in ``[start_s, end_s)``: each cluster + its gated identity (or None).

    The reader's per-chunk overlay ("[Mayor Harris] … [unidentified] …"): a cluster
    with no public identity stays anonymous rather than being dropped.
    """
    return [
        {"cluster_label": c, "speaker": identities.get(c)}
        for c in clusters_in_window(turns, start_s, end_s)
    ]


def _turn_text(words: list[dict[str, Any]]) -> str:
    """Join a turn's words into readable prose (verbatim — whitespace only, never edited).

    Defensive about the JSONB shape: the column only guarantees an array, so a malformed
    element is skipped rather than allowed to error a reader page.
    """
    parts = []
    for w in words:
        token = w.get("word") if isinstance(w, dict) else None
        if isinstance(token, str) and token.strip():
            parts.append(token.strip())
    return " ".join(parts).strip()


def build_reader_transcript(
    turns: list[dict[str, Any]],
    identity_rows: list[dict[str, Any]],
    canonicalize: Callable[[str], str],
) -> list[dict[str, Any]]:
    """Speaker-labeled turn blocks for the transcript reader (canonical + raw per turn).

    Each block carries a display ``label`` — the gated official name for an identified
    cluster, else a stable ``"Speaker N"`` numbered by first appearance so distinct
    anonymous voices stay distinguishable without being named — plus the turn's start
    time and both texts: ``canonical_text`` (proper nouns corrected, the default view)
    and ``raw_text`` (verbatim as transcribed). ``canonicalize`` is injected (the route
    binds it to the place's vetted corrections) so this stays pure and place-agnostic.

    Identity gating is already applied by ``resolve_speakers`` (only high/confirmed name
    a subject); every other cluster stays anonymous. Empty turns are dropped.
    """
    speakers = resolve_speakers(identity_rows)
    anon_numbers: dict[str, int] = {}
    blocks: list[dict[str, Any]] = []
    for turn in turns:
        raw_text = _turn_text(turn.get("words") or [])
        if not raw_text:
            continue
        cluster = turn["cluster_label"]
        identity = speakers.get(cluster)
        if identity:
            label, slug, identified = identity["name"], identity.get("slug"), True
        else:
            number = anon_numbers.setdefault(cluster, len(anon_numbers) + 1)
            label, slug, identified = f"Speaker {number}", None, False
        blocks.append(
            {
                "label": label,
                "slug": slug,
                "identified": identified,
                "start_seconds": turn.get("start_seconds"),
                "canonical_text": canonicalize(raw_text),
                "raw_text": raw_text,
            }
        )
    return blocks


def build_meeting_speakers(
    turns: list[dict[str, Any]], identity_rows: list[dict[str, Any]]
) -> dict[str, Any]:
    """The full speaker layer for one transcript, shaped for the API/reader.

    Combines the word-level turns with their (gated) identities into
    ``{"speakers": {cluster -> identity}, "turns": [{...turn..., "speaker": identity|None}]}``.
    Anonymous clusters keep their turns with ``speaker = None``.
    """
    speakers = resolve_speakers(identity_rows)
    turn_views = [
        {
            "cluster_label": t["cluster_label"],
            "start_seconds": t["start_seconds"],
            "end_seconds": t["end_seconds"],
            "speaker": speakers.get(t["cluster_label"]),
            "words": t.get("words") or [],
        }
        for t in turns
    ]
    return {"speakers": speakers, "turns": turn_views}
