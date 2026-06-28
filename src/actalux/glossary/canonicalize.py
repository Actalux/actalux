"""Canonicalize transcript text against the place's vetted name-corrections.

Clean WhisperX transcribes meetings well but misspells proper nouns the public
record spells correctly ("Musco Sports Lighting" -> "Moscow Sports Lighting",
"Yorg" -> "York"). For accuracy we embed, search, and display a **canonical**
(name-corrected) text while keeping the **raw** verbatim transcript as provenance
(``documents.raw_content``). This module produces the canonical text plus a
reversible audit trail (``name_canonicalizations`` rows).

Safety properties (the cardinal "never invent a spelling"):

* Only **forward** corrections are applied (mangled -> canonical). The bidirectional
  expansion in ``web/retrieval.py`` is for *search recall*; here we are rewriting the
  stored text, so we only ever move toward the authoritative spelling.
* Every canonical side comes from a **vetted ``name_corrections`` row** — a human- or
  discovery-approved spelling already grounded in the place's roster/agenda. Nothing
  novel is synthesized.
* Matching is **place-scoped** (cardinal repo rule: the same string can be a mangling
  in one town and a real name in another), word-boundaried, and case-insensitive.
* The raw text is never destroyed — this returns a new string and an audit record per
  edit (raw offset, surface form, canonical, source), so every change is reversible.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

from supabase import Client

from actalux.db import get_name_corrections
from actalux.graph.resolve import normalize_name
from actalux.graph.store import place_lexicon

logger = logging.getLogger(__name__)

# name_corrections.provenance -> name_canonicalizations.source, used only when the
# canonical is NOT itself an official in the lexicon (lexicon grounding wins below).
# 'reviewed'/'manual' are human-curated; everything else came from auto-discovery.
_PROVENANCE_SOURCE = {"reviewed": "manual", "manual": "manual"}

# Sources trusted to REWRITE the canonical (displayed/embedded) transcript: official
# names from the lexicon and human-curated fixes. Auto-discovered corrections are noisy
# and body-agnostic within a place (a schools-curriculum spelling could hit a council
# transcript), so they are kept for SEARCH expansion only — never applied to stored text.
CANONICAL_SOURCES = frozenset({"lexicon", "manual"})
ALL_SOURCES = frozenset({"lexicon", "manual", "auto_discovery"})


@dataclass(frozen=True)
class CorrectionRule:
    """One forward spelling fix to apply: ``mangled`` -> ``canonical``."""

    mangled: str
    canonical: str
    source: str  # name_canonicalizations.source: lexicon | auto_discovery | manual


@dataclass(frozen=True)
class Canonicalization:
    """Audit record for a single applied correction (offset into the RAW text)."""

    char_start: int
    raw_token: str  # the exact surface form that was replaced
    canonical: str
    source: str
    score: float | None = None  # NULL: corrections are pre-vetted, not fuzzy-scored here

    def to_row(self, document_id: int) -> dict[str, Any]:
        """Row for the ``name_canonicalizations`` audit table."""
        return {
            "document_id": document_id,
            "char_start": self.char_start,
            "raw_token": self.raw_token,
            "canonical": self.canonical,
            "source": self.source,
            "score": self.score,
        }


def build_rules(
    corrections: list[dict[str, Any]],
    lexicon: list[dict[str, Any]],
    *,
    sources: frozenset[str] = CANONICAL_SOURCES,
) -> list[CorrectionRule]:
    """Vetted ``name_corrections`` rows + officials lexicon -> forward rules.

    The canonical side is grounded in the authoritative record: a correction whose
    canonical is an official's name is sourced ``lexicon``; a reviewed/manual one is
    ``manual``; the rest (auto-discovered street/business/etc. spellings) are
    ``auto_discovery``. No spelling is added that isn't already a vetted correction.

    Only rules whose source is in ``sources`` are returned; the default
    (``CANONICAL_SOURCES``) keeps just the high-trust lexicon/manual fixes, so noisy
    auto-discovered corrections never rewrite the stored transcript. Pass
    ``ALL_SOURCES`` to include them (e.g. when testing classification).

    Rules are ordered longest-mangled-first (then lexicographically) so a specific
    multi-word fix claims its span before a shorter contained one, deterministically.

    The table's ``UNIQUE (place_id, mangled)`` is case-sensitive but matching is
    case-insensitive, so two rows like ``York -> Yorg`` and ``york -> Yorke`` collide.
    When colliding rows disagree on the canonical, BOTH are dropped (skip beats a coin
    flip on whose spelling wins); when they agree, one is kept.
    """
    lex_norms = {normalize_name(e["canonical_name"]) for e in lexicon if e.get("canonical_name")}
    by_key: dict[str, list[CorrectionRule]] = {}
    for row in corrections:
        mangled = (row.get("mangled") or "").strip()
        canonical = (row.get("canonical") or "").strip()
        if not mangled or not canonical or mangled.lower() == canonical.lower():
            continue  # empty or no-op
        if normalize_name(canonical) in lex_norms:
            source = "lexicon"
        else:
            source = _PROVENANCE_SOURCE.get((row.get("provenance") or "").lower(), "auto_discovery")
        if source not in sources:
            continue  # not trusted to rewrite the canonical text
        by_key.setdefault(mangled.lower(), []).append(CorrectionRule(mangled, canonical, source))

    rules: list[CorrectionRule] = []
    for key, group in by_key.items():
        if len({r.canonical for r in group}) > 1:
            logger.warning(
                "skipping conflicting name-corrections for %r: %s",
                key,
                sorted({r.canonical for r in group}),
            )
            continue
        rules.append(group[0])
    rules.sort(key=lambda r: (-len(r.mangled), r.mangled, r.canonical))
    return rules


def _match_spans(
    raw_text: str, rules: list[CorrectionRule]
) -> list[tuple[int, int, CorrectionRule]]:
    """Non-overlapping ``(start, end, rule)`` spans, longest-rule-first, in raw order.

    Greedy by rule precedence (``rules`` is already longest-first): once a character
    range is claimed, a shorter or later rule cannot also match inside it, so each
    raw token is corrected at most once.
    """
    taken: list[tuple[int, int]] = []
    spans: list[tuple[int, int, CorrectionRule]] = []
    for rule in rules:
        # Whitespace-flexible: a multi-word mangling matches across a line wrap or
        # double space in the raw transcript (tokens escaped, joined on \s+).
        pattern = r"\s+".join(re.escape(tok) for tok in rule.mangled.split())
        for m in re.finditer(rf"\b{pattern}\b", raw_text, re.IGNORECASE):
            start, end = m.start(), m.end()
            if any(start < te and ts < end for ts, te in taken):
                continue  # overlaps an already-claimed span
            taken.append((start, end))
            spans.append((start, end, rule))
    spans.sort(key=lambda sp: sp[0])
    return spans


def canonicalize_text(
    raw_text: str, rules: list[CorrectionRule]
) -> tuple[str, list[Canonicalization]]:
    """Apply forward name-corrections to ``raw_text`` -> (canonical text, audit rows).

    Word-boundaried + case-insensitive; longest match wins; spans never overlap. The
    ``char_start`` in each audit record is the offset into the **RAW** text (not the
    canonical), so the audit stays aligned to ``documents.raw_content``.
    """
    spans = _match_spans(raw_text, rules)
    if not spans:
        return raw_text, []
    out: list[str] = []
    audits: list[Canonicalization] = []
    cursor = 0
    for start, end, rule in spans:
        out.append(raw_text[cursor:start])
        raw_token = raw_text[start:end]
        out.append(rule.canonical)
        audits.append(Canonicalization(start, raw_token, rule.canonical, rule.source))
        cursor = end
    out.append(raw_text[cursor:])
    return "".join(out), audits


def canonicalize_document(
    client: Client, place_id: int, raw_text: str
) -> tuple[str, list[Canonicalization]]:
    """DB-facing: load the place's corrections + lexicon, then canonicalize ``raw_text``.

    Jurisdiction-scoped (cardinal rule): corrections and lexicon are loaded per place
    so a mangling here is never applied to a town where the same string is a real name.
    """
    rules = build_rules(get_name_corrections(client, place_id), place_lexicon(client, place_id))
    return canonicalize_text(raw_text, rules)
