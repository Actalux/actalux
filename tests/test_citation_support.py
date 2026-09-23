"""Tests for the citation-support check (actalux.search.citation_support)."""

from __future__ import annotations

from actalux.search.citation_support import (
    Judgment,
    check_answer,
    cited_claims,
    parse_quotes_block,
)

BLOCK = """[#q0d54 | 2026-04-11 | Math progression]
CP Algebra 1 - AMPED earns one credit for math and one for CTE.

It is a two-block period.

[#q0d55 | 2026-04-11]
AP Statistics is available to all students.
"""


def test_quotes_block_keeps_multi_paragraph_passages_whole() -> None:
    p = parse_quotes_block(BLOCK)
    assert set(p) == {"#q0d54", "#q0d55"}
    assert "two-block period" in p["#q0d54"]
    assert p["#q0d55"] == "AP Statistics is available to all students."


def test_claims_strip_markers_and_keep_every_cited_id() -> None:
    answer = "AMPED earns CTE credit [#q0d54]. Both apply [#q0d54, #q0d55]. See the site."
    claims = cited_claims(answer)
    assert claims == [
        ("AMPED earns CTE credit.", ("#q0d54",)),
        ("Both apply.", ("#q0d54", "#q0d55")),
    ]


def test_multi_citation_is_judged_against_all_cited_passages_together() -> None:
    seen: list[str] = []

    def judge(claim: str, section: str) -> Judgment:
        seen.append(section)
        return Judgment("supports", 0.95)

    check_answer("Both apply [#q0d54, #q0d55].", parse_quotes_block(BLOCK), judge)
    assert "AMPED" in seen[0] and "AP Statistics" in seen[0]


def test_verdicts_and_review_flags() -> None:
    passages = parse_quotes_block(BLOCK)
    relations = iter(
        [Judgment("supports", 0.95), Judgment("supports", 0.6), Judgment("contradicts", 0.99)]
    )
    out = check_answer(
        "One [#q0d54]. Two [#q0d55]. Three [#q0d55].", passages, lambda c, s: next(relations)
    )
    assert [c.verdict for c in out] == ["verified", "verified", "contradicted"]
    # Confident support stands; low-confidence support and any contradiction go to a person.
    assert [c.needs_review for c in out] == [False, True, True]


def test_missing_passage_is_reported_never_judged_on_partial_evidence() -> None:
    def judge(claim: str, section: str) -> Judgment:
        raise AssertionError("must not be called")

    (c,) = check_answer("Claim [#q9999].", parse_quotes_block(BLOCK), judge)
    assert c.verdict == "missing_passage" and c.needs_review


def test_header_with_a_line_break_in_its_section_title_still_parses() -> None:
    # Real case from eval/answers.json (cur02): the section title wraps.
    block = "[#q1eef | 2026-04-29 | 6. \nPresentations]\n6.1 Facility Update\n"
    assert parse_quotes_block(block) == {"#q1eef": "6.1 Facility Update"}
