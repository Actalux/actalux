"""Tests for the proper-name mangling discovery engine (pure, no DB).

The engine's contract is its safety property: every proposed correction is *grounded*
(canonical appears in the authoritative vocabulary, mangled appears in the transcript),
multi-word corruptions reach the ``high`` bucket, and everything ambiguous, single-token,
or already-known is held back. These tests pin that contract.
"""

from __future__ import annotations

from actalux.glossary.discovery import (
    Candidate,
    Vocabulary,
    build_vocabulary,
    context_snippet,
    extract_proper_nouns,
    find_manglings,
    norm_key,
)
from scripts.seed_corrections import build_rows

LEXICON = [
    {"canonical_name": "Jeffery Yorg", "aliases": [{"raw": "Yorg", "normalized": "yorg"}]},
    {"canonical_name": "Susan Buse", "aliases": []},
]
MINUTES = [
    {
        "id": 12,
        "document_type": "minutes",
        "meeting_date": "2026-02-19",
        "text": (
            "The Board approved the contract with Musco Sports Lighting for Shaw Park. "
            "Mayor Yorg and Commissioner Buse were present. "
            "Erin Linnenbringer addressed the board."
        ),
    }
]
TRANSCRIPT = (
    "Mayor York welcomed everyone. We discussed the Moscow Sports Lighting bid for Shah Park. "
    "Commissioner Buse spoke. Susan Buse moved to approve. Aaron Linenbringer raised a concern."
)


def _vocab() -> Vocabulary:
    return build_vocabulary(LEXICON, MINUTES)


def _by_mangled(text: str = TRANSCRIPT, vocab: Vocabulary | None = None) -> dict:
    return {m.mangled: m for m in find_manglings(text, vocab or _vocab())}


# --- extraction -----------------------------------------------------------------


def test_extract_breaks_runs_at_sentence_end():
    grams = extract_proper_nouns("Shaw Park. Mayor Yorg arrived.")
    # The period after "Park" must end the run, so the cross-sentence span never forms.
    assert "Park Mayor" not in grams
    assert "Shaw Park" in grams


def test_extract_keeps_intact_multiword_names():
    grams = extract_proper_nouns("Contract with Musco Sports Lighting was approved")
    assert "Musco Sports Lighting" in grams


# --- vocabulary -----------------------------------------------------------------


def test_build_vocabulary_grounds_canonicals():
    vocab = _vocab()
    canon = {c.canonical for c in vocab.candidates}
    assert "Susan Buse" in canon  # from lexicon (person)
    assert "Musco Sports Lighting" in canon  # from minutes (other)
    # Aliases / short surnames are known-correct but not correction targets.
    assert "yorg" in vocab.known_norm
    assert all(len(c.norm) >= 5 for c in vocab.candidates)


def test_lexicon_persons_tagged_person_minutes_tagged_other():
    vocab = _vocab()
    cats = {c.canonical: c.category for c in vocab.candidates}
    assert cats["Susan Buse"] == "person"
    assert cats["Musco Sports Lighting"] == "other"


# --- grounding gate -------------------------------------------------------------


def test_every_proposal_is_grounded():
    vocab = _vocab()
    canon_norms = {c.norm for c in vocab.candidates}
    for m in find_manglings(TRANSCRIPT, vocab):
        assert norm_key(m.canonical) in canon_norms  # canonical never invented
        assert m.mangled in norm_key(TRANSCRIPT)  # mangled appears in transcript
        assert m.mangled != norm_key(m.canonical)  # not a no-op


def test_high_bucket_catches_clear_multiword_mangling():
    hits = _by_mangled()
    assert hits["moscow sports lighting"].canonical == "Musco Sports Lighting"
    assert hits["moscow sports lighting"].confidence == "high"
    assert hits["shah park"].canonical == "Shaw Park"
    assert hits["shah park"].confidence == "high"


def test_hard_phonetic_person_name_lands_in_review():
    # "aaron linenbringer" -> "Erin Linnenbringer" is real but distant: must not auto-apply.
    hit = _by_mangled()["aaron linenbringer"]
    assert hit.canonical == "Erin Linnenbringer"
    assert hit.confidence == "review"


def test_known_name_parts_not_flagged():
    # "susan" and "buse" are correct name-parts, never proposed as manglings.
    hits = _by_mangled()
    assert "susan" not in hits
    assert "buse" not in hits


def test_subspan_manglings_suppressed():
    hits = _by_mangled()
    # The full span is kept; its sub-phrases are dropped.
    assert "moscow sports lighting" in hits
    assert "moscow sports" not in hits
    assert "moscow" not in hits


def test_exact_correct_spelling_not_flagged():
    # A transcript that spells everything correctly yields nothing.
    clean = "Susan Buse and the Musco Sports Lighting contract for Shaw Park."
    assert find_manglings(clean, _vocab()) == []


def test_existing_corrections_are_deduped():
    existing = frozenset({"shah park"})
    hits = {m.mangled for m in find_manglings(TRANSCRIPT, _vocab(), existing_norm=existing)}
    assert "shah park" not in hits
    assert "moscow sports lighting" in hits  # other proposals still surface


def test_single_token_never_high():
    vocab = Vocabulary(
        candidates=[Candidate("Kingsbury", "kingsbury", "doc 1 (minutes, x)", "other", 1)],
        known_norm={"kingsbury"},
    )
    hits = find_manglings("The Kingsberry project was reviewed.", vocab)
    assert hits, "expected the near-match to surface"
    assert all(m.confidence == "review" for m in hits)  # single-token caps at review


def test_ambiguous_canonicals_downgrade_to_review():
    # "smithe" is one edit from BOTH "smith" and "smythe": can't trust one -> review.
    vocab = Vocabulary(
        candidates=[
            Candidate("Anna Smith", "anna smith", "doc 1 (minutes, x)", "person", 2),
            Candidate("Anna Smythe", "anna smythe", "doc 2 (minutes, y)", "person", 2),
        ],
        known_norm={"anna smith", "anna smythe"},
    )
    hits = find_manglings("Then Anna Smithe spoke at length.", vocab)
    assert hits
    assert all(m.confidence == "review" for m in hits)


# --- snippet + seeder coexistence ----------------------------------------------


def test_context_snippet_centers_on_surface():
    snip = context_snippet("a b c Moscow Sports d e f", "Moscow Sports", width=4)
    assert "Moscow Sports" in snip


def test_seed_rows_default_provenance_is_asr():
    # Curated rows must never be NULL-provenance: the re-seed deletes all but 'auto',
    # which relies on every curated row carrying a non-'auto' provenance.
    rows = build_rows(1, [{"mangled": "Foo", "canonical": "Bar"}])
    assert rows[0]["provenance"] == "asr"
    assert rows[0]["mangled"] == "foo"
