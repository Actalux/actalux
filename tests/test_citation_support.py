"""Tests for the citation-support check (actalux.search.citation_support)."""

from __future__ import annotations

import json

import httpx

from actalux.search.citation_support import (
    Judgment,
    check_answer,
    cited_claims,
    make_jev_judge,
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


def test_jev_judge_posts_a_decisions_request_and_reads_the_answer() -> None:
    sent: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        sent["url"] = str(request.url)
        sent["auth"] = request.headers["authorization"]
        sent["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "answers": {
                    "relation": {
                        "type": "choice",
                        "choice": "contradicts",
                        "probabilities": {"supports": 0, "contradicts": 0.96, "says_nothing": 0.04},
                        "confidence": 0.93,
                    }
                }
            },
        )

    judge = make_jev_judge(
        "k-actalux",
        "typesafe/jev-1.13",
        "https://x/api/alpha/decisions",
        transport=httpx.MockTransport(handler),
    )
    j = judge("The board approved the budget.", "The budget was tabled.")
    assert (j.relation, j.confidence) == ("contradicts", 0.93)
    assert sent["url"] == "https://x/api/alpha/decisions"
    assert sent["auth"] == "Bearer k-actalux"
    assert sent["body"]["model"] == "typesafe/jev-1.13"
    assert sent["body"]["state"] == {
        "claim": "The board approved the budget.",
        "section": "The budget was tabled.",
    }
    q = sent["body"]["questions"]["relation"]
    assert q["type"] == "choice" and set(q["criteria"]) == {
        "supports",
        "contradicts",
        "says_nothing",
    }


def test_jev_judge_retries_overload_then_succeeds() -> None:
    calls = {"n": 0}
    waits: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(529, headers={"retry-after": "1"} if calls["n"] == 2 else {})
        return httpx.Response(
            200, json={"answers": {"relation": {"choice": "supports", "confidence": 0.9}}}
        )

    judge = make_jev_judge(
        "k", "m", "https://x/d", transport=httpx.MockTransport(handler), sleep=waits.append
    )
    assert judge("c", "s") == Judgment("supports", 0.9)
    # First wait is backoff (2**0), second honors retry-after.
    assert waits == [1.0, 1.0] and calls["n"] == 3


def test_jev_judge_does_not_retry_a_bad_request() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(400, json={"error": "bad"})

    judge = make_jev_judge(
        "k", "m", "https://x/d", transport=httpx.MockTransport(handler), sleep=lambda s: None
    )
    try:
        judge("c", "s")
    except httpx.HTTPStatusError:
        pass
    else:
        raise AssertionError("a 400 must raise")
    assert calls["n"] == 1
