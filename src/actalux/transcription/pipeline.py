"""Assemble the speaker-attribution layer for one meeting transcript.

The off-Mac transcription pipeline (clean WhisperX + pyannote, orchestrated by a
GitHub Action over Modal — never a laptop) produces, per meeting:

* raw verbatim text (WhisperX) -> ``documents.raw_content``,
* canonical name-corrected text -> ``documents.content`` (embedded + displayed),
* a reversible ``name_canonicalizations`` audit trail,
* word-level ``diarization_turns`` (the attribution layer, for reader labels + clips).

This module is the **computation** half: given the backend outputs (a ``WordTranscript``
and a ``SpeakerTimeline``) plus the place's vetted correction rules, it assembles a
``SpeakerLayer`` and maps it to DB rows. It performs no I/O — no GPU, no network, no DB
— so it is unit-testable with plain in-memory inputs. The orchestration half (download,
run the GPU backends, load rules from the DB, stage/persist) lives alongside it and
calls in here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from supabase import Client

from actalux.db import insert_rows_resilient
from actalux.diarization.align import AttributedTurn, attribute_words
from actalux.diarization.backend import DiarizationBackend, SpeakerTimeline
from actalux.glossary.canonicalize import Canonicalization, CorrectionRule, canonicalize_text
from actalux.transcription.backend import TranscriptionBackend, Word, WordTranscript

# A document's attribution layer; cleared wholesale before a re-transcribe re-inserts.
_LAYER_TABLES = (
    "diarization_turns",
    "name_canonicalizations",
    "speaker_identities",
    "media_assets",
)


@dataclass(frozen=True)
class SpeakerLayer:
    """The full speaker-attribution payload for one meeting, ready to persist.

    Computation is decoupled from persistence: the DB writes happen in the
    orchestration layer, so this carries exactly what those writes need and nothing
    DB-specific (no ids, no client).
    """

    raw_text: str  # verbatim as-heard -> documents.raw_content
    canonical_text: str  # name-corrected -> documents.content (embedded + displayed)
    canonicalizations: list[Canonicalization]  # reversible audit of every name fix
    turns: list[AttributedTurn]  # word-level speaker turns -> diarization_turns
    diarization_model: str  # SpeakerTimeline.source_model, for the turn rows

    def canonicalization_rows(self, document_id: int) -> list[dict[str, Any]]:
        """``name_canonicalizations`` rows for this document (one per applied fix)."""
        return [c.to_row(document_id) for c in self.canonicalizations]

    def turn_rows(self, document_id: int) -> list[dict[str, Any]]:
        """``diarization_turns`` rows for this document (one per merged speaker turn)."""
        return [t.to_row(document_id, self.diarization_model) for t in self.turns]

    def to_dict(self) -> dict[str, Any]:
        """Serialize for a sidecar so persistence can run in a later process/step.

        The off-Mac flow stages the layer next to the transcript, then a post-ingest
        step reads it back (once the document id exists) and persists it.
        """
        return {
            "raw_text": self.raw_text,
            "canonical_text": self.canonical_text,
            "diarization_model": self.diarization_model,
            "canonicalizations": [
                {
                    "char_start": c.char_start,
                    "raw_token": c.raw_token,
                    "canonical": c.canonical,
                    "source": c.source,
                    "score": c.score,
                }
                for c in self.canonicalizations
            ],
            "turns": [
                {
                    "cluster_label": t.cluster_label,
                    "start_s": t.start_s,
                    "end_s": t.end_s,
                    "words": [
                        {"word": w.text, "start": w.start_s, "end": w.end_s} for w in t.words
                    ],
                }
                for t in self.turns
            ],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SpeakerLayer:
        """Rebuild a ``SpeakerLayer`` from :meth:`to_dict` (the sidecar round-trip)."""
        canonicalizations = [
            Canonicalization(
                c["char_start"], c["raw_token"], c["canonical"], c["source"], c.get("score")
            )
            for c in data["canonicalizations"]
        ]
        turns = [
            AttributedTurn(
                t["cluster_label"],
                t["start_s"],
                t["end_s"],
                [Word(w["word"], w["start"], w["end"]) for w in t["words"]],
            )
            for t in data["turns"]
        ]
        return cls(
            raw_text=data["raw_text"],
            canonical_text=data["canonical_text"],
            canonicalizations=canonicalizations,
            turns=turns,
            diarization_model=data["diarization_model"],
        )


def assemble_speaker_layer(
    raw: WordTranscript, timeline: SpeakerTimeline, rules: list[CorrectionRule]
) -> SpeakerLayer:
    """Combine a raw transcript + diarization + correction rules into a ``SpeakerLayer``.

    The canonical text and audit come from canonicalizing the full raw text (so a fix
    is caught even across a segment boundary); the turns come from assigning each raw
    word to its diarization cluster. Verbatim words are never altered — only corrected
    for the canonical copy, with the raw retained.
    """
    canonical_text, canonicalizations = canonicalize_text(raw.text, rules)
    turns = attribute_words(raw.all_words(), timeline)
    return SpeakerLayer(
        raw_text=raw.text,
        canonical_text=canonical_text,
        canonicalizations=canonicalizations,
        turns=turns,
        diarization_model=timeline.source_model,
    )


def media_asset_row(
    document_id: int,
    source_url: str,
    *,
    entity_id: int | None = None,
    kind: str = "video",
    duration_seconds: float | None = None,
    content_hash: str | None = None,
) -> dict[str, Any]:
    """A ``media_assets`` row — the stable per-recording id used for clip resolution."""
    return {
        "document_id": document_id,
        "entity_id": entity_id,
        "source_url": source_url,
        "kind": kind,
        "duration_seconds": duration_seconds,
        "content_hash": content_hash,
    }


def transcribe_and_attribute(
    audio_uri: str,
    transcriber: TranscriptionBackend,
    diarizer: DiarizationBackend,
    rules: list[CorrectionRule],
) -> SpeakerLayer:
    """Run the GPU backends over one audio source and assemble its speaker layer.

    The orchestration seam over the injected backends — pure aside from the backend
    calls themselves, so tests pass fakes and never touch a GPU. Transcription and
    diarization run on the *same* audio so their timelines align.
    """
    raw = transcriber.transcribe(audio_uri)
    timeline = diarizer.run(audio_uri)
    return assemble_speaker_layer(raw, timeline, rules)


def persist_speaker_layer(
    service_client: Client,
    document_id: int,
    layer: SpeakerLayer,
    *,
    media_url: str,
    entity_id: int | None = None,
    duration_seconds: float | None = None,
    content_hash: str | None = None,
) -> None:
    """Write a meeting's speaker layer to the new tables, keyed to an ingested document.

    Uses the SERVICE client (these tables' writes bypass RLS; the audit log is
    service-only). The canonical text itself (``documents.content``) is written by the
    ingest step; here we attach the raw copy + the attribution layer.

    Idempotent: a re-transcribe clears the document's entire prior attribution layer —
    turns, the correction audit, the cluster->identity rows, and media assets — before
    re-inserting. Clearing ``speaker_identities`` is essential: a fresh diarization
    re-numbers clusters, so a stale ``SPEAKER_00 -> subject`` mapping (which may be
    high/confirmed and therefore publicly displayed) would mislabel the new turns;
    identities are re-resolved downstream. ``raw_content`` is written LAST so a mid-run
    failure never leaves the public "show raw" text present without its attribution.

    Not atomic across statements: a failed run leaves a repairable partial state (a
    re-run fixes it). A service-key RPC transaction would close that window.
    """
    # Clear the prior layer for this document (idempotent re-transcribe). Identities go
    # too — old cluster numbers no longer mean the same speaker after a fresh diarize.
    for table in _LAYER_TABLES:
        service_client.table(table).delete().eq("document_id", document_id).execute()

    # Resilient batched inserts: a long meeting's word-level turns (hundreds of rows,
    # each carrying a words JSON payload) trip the Supabase free-tier statement timeout
    # as one statement, exactly like the chunks insert did. insert_rows_resilient batches
    # and halves on a timeout so the persist completes.
    turn_rows = layer.turn_rows(document_id)
    if turn_rows:
        insert_rows_resilient(service_client, "diarization_turns", turn_rows)
    canon_rows = layer.canonicalization_rows(document_id)
    if canon_rows:
        insert_rows_resilient(service_client, "name_canonicalizations", canon_rows)
    service_client.table("media_assets").insert(
        media_asset_row(
            document_id,
            media_url,
            entity_id=entity_id,
            duration_seconds=duration_seconds,
            content_hash=content_hash,
        )
    ).execute()

    # Last: only expose the public raw text once its attribution layer is in place.
    service_client.table("documents").update({"raw_content": layer.raw_text}).eq(
        "id", document_id
    ).execute()
