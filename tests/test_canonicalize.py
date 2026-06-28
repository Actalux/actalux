"""Unit tests for transcript name-canonicalization (pure core, no DB)."""

from __future__ import annotations

from actalux.glossary.canonicalize import (
    Canonicalization,
    CorrectionRule,
    build_rules,
    canonicalize_text,
)


def _rule(mangled: str, canonical: str, source: str = "lexicon") -> CorrectionRule:
    return CorrectionRule(mangled, canonical, source)


def test_basic_forward_correction():
    raw = "Mr. York moved the motion."
    text, audits = canonicalize_text(raw, [_rule("york", "Yorg")])
    assert text == "Mr. Yorg moved the motion."
    assert len(audits) == 1
    a = audits[0]
    assert a.char_start == raw.index("York")
    assert a.raw_token == "York"
    assert a.canonical == "Yorg"
    assert a.source == "lexicon"


def test_case_insensitive_match_uses_canonical_spelling():
    text, audits = canonicalize_text("YORK and york", [_rule("york", "Yorg")])
    assert text == "Yorg and Yorg"
    assert [a.raw_token for a in audits] == ["YORK", "york"]


def test_word_boundary_no_partial_match():
    text, audits = canonicalize_text("Yorktown Road", [_rule("york", "Yorg")])
    assert text == "Yorktown Road"
    assert audits == []


def test_longest_match_wins_no_double_apply():
    # The contained shorter rule ("moscow") must not also fire inside the long match.
    rules = build_rules(
        [
            {"mangled": "moscow", "canonical": "Musco"},
            {"mangled": "moscow sports lighting", "canonical": "Musco Sports Lighting"},
        ],
        [],
    )
    raw = "the Moscow Sports Lighting bid"
    text, audits = canonicalize_text(raw, rules)
    assert text == "the Musco Sports Lighting bid"
    assert len(audits) == 1
    assert audits[0].raw_token == "Moscow Sports Lighting"
    assert audits[0].char_start == raw.index("Moscow")


def test_char_start_is_raw_offset_not_canonical_offset():
    # First correction lengthens the text; the second audit's char_start must still be
    # the offset into the RAW string, not the (shifted) canonical string.
    raw = "York met Smith here."
    rules = [_rule("york", "Yorgensen"), _rule("smith", "Schmidt")]
    text, audits = canonicalize_text(raw, rules)
    assert text == "Yorgensen met Schmidt here."
    smith_audit = next(a for a in audits if a.canonical == "Schmidt")
    assert smith_audit.char_start == raw.index("Smith") == 9


def test_whitespace_flexible_multiword_match():
    # A multi-word mangling must match across a line wrap / double space in the raw text.
    rules = build_rules(
        [{"mangled": "moscow sports lighting", "canonical": "Musco Sports Lighting"}], []
    )
    raw = "the Moscow\nSports  Lighting bid"
    text, audits = canonicalize_text(raw, rules)
    assert text == "the Musco Sports Lighting bid"
    assert len(audits) == 1
    assert audits[0].raw_token == "Moscow\nSports  Lighting"


def test_build_rules_drops_conflicting_case_variants():
    # Same mangled (case-insensitively) with DIFFERENT canonicals -> drop both.
    rules = build_rules(
        [
            {"mangled": "york", "canonical": "Yorg"},
            {"mangled": "York", "canonical": "Yorke"},
        ],
        [],
    )
    assert rules == []


def test_build_rules_keeps_agreeing_case_variants():
    # Same mangled (case-insensitively) with the SAME canonical -> keep one.
    rules = build_rules(
        [
            {"mangled": "york", "canonical": "Yorg"},
            {"mangled": "York", "canonical": "Yorg"},
        ],
        [],
    )
    assert len(rules) == 1
    assert rules[0].canonical == "Yorg"


def test_no_match_returns_input_unchanged():
    raw = "nothing to correct here"
    text, audits = canonicalize_text(raw, [_rule("york", "Yorg")])
    assert text == raw
    assert audits == []


def test_build_rules_source_classification():
    lexicon = [{"canonical_name": "Jeffery Yorg"}]
    corrections = [
        {"mangled": "york", "canonical": "Jeffery Yorg", "provenance": "asr"},
        {"mangled": "moscow", "canonical": "Musco", "provenance": "reviewed"},
        {"mangled": "shah park", "canonical": "Shaw Park", "provenance": "auto"},
        {"mangled": "brentwood ave", "canonical": "Brentwood Avenue", "provenance": "asr"},
    ]
    by_canonical = {r.canonical: r.source for r in build_rules(corrections, lexicon)}
    assert by_canonical["Jeffery Yorg"] == "lexicon"  # canonical is an official
    assert by_canonical["Musco"] == "manual"  # reviewed/curated
    assert by_canonical["Shaw Park"] == "auto_discovery"  # auto, not an official
    assert by_canonical["Brentwood Avenue"] == "auto_discovery"  # asr-origin, not an official


def test_build_rules_skips_noops_and_empty():
    corrections = [
        {"mangled": "York", "canonical": "york"},  # equal ignoring case -> no-op
        {"mangled": "", "canonical": "X"},  # empty mangled
        {"mangled": "a", "canonical": ""},  # empty canonical
    ]
    assert build_rules(corrections, []) == []


def test_build_rules_orders_longest_mangled_first():
    rules = build_rules(
        [
            {"mangled": "moscow", "canonical": "Musco"},
            {"mangled": "moscow sports lighting", "canonical": "Musco Sports Lighting"},
        ],
        [],
    )
    assert rules[0].mangled == "moscow sports lighting"


def test_to_row_shape():
    row = Canonicalization(5, "York", "Yorg", "lexicon").to_row(42)
    assert row == {
        "document_id": 42,
        "char_start": 5,
        "raw_token": "York",
        "canonical": "Yorg",
        "source": "lexicon",
        "score": None,
    }
