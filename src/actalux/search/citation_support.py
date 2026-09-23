"""Check that a cited passage actually supports the sentence citing it.

Summary verification (``summarize._verify_sentence``) guarantees every cited hash
ID names a passage the answer model was shown. It cannot tell whether that
passage *says* what the sentence claims: a real quote can sit under a claim its
context contradicts ("the board approved X" citing a passage where X was tabled).
This module closes that gap with one calibrated judgment per cited sentence —
does the cited text support the claim, contradict it, or say nothing about it —
following TypeSafe's citation-check recipe (docs.typesafe.ai/cookbooks/
citation_check).

A sentence citing several passages is judged against all of them together,
because that is how the answer model used them. The judge is an injected
callable so tests run without the network and the provider stays swappable.

Verdicts are advisory: this flags sentences for a person to read. Numeric and
date-comparison claims are the model's documented weak spot (docs.typesafe.ai/
model-jaggedness), so a flagged figure is a prompt to look, not a finding.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass

from actalux.search.summarize import HASH_ID_RE, _split_sentences

# The three relations, worded as in the recipe; each maps to an audit verdict.
RELATIONS = {
    "supports": "The section states the claim or directly implies that it is true",
    "contradicts": "The section states the opposite of the claim or implies it is false",
    "says_nothing": "The section does not address what the claim asserts, either way",
}
VERDICT_OF = {"supports": "verified", "contradicts": "contradicted", "says_nothing": "unsupported"}

# Below this, a verdict goes to a person even when it is "verified". The
# recipe's starting point; to be re-set from this corpus's own reviewed results.
AUTO_ACCEPT_CONFIDENCE = 0.8

# A quotes-block header: "[#q0d54 | 2026-04-11 | title | section]". The section
# title is copied from the source and can itself contain line breaks ("6. \n
# Presentations"), so the header runs to its closing bracket, not to end of line.
_QUOTE_HEADER_RE = re.compile(r"^\[(#q[0-9a-f]{4,})(?: \|[^\]]*)?\]", re.M)


@dataclass(frozen=True)
class Judgment:
    relation: str  # one of RELATIONS
    confidence: float


JudgeFn = Callable[[str, str], Judgment]


@dataclass(frozen=True)
class CitedClaim:
    claim: str  # the sentence with its citation markers removed
    hash_ids: tuple[str, ...]
    section: str  # the cited passages' text, joined


@dataclass(frozen=True)
class CheckedClaim:
    claim: CitedClaim
    verdict: str  # verified | contradicted | unsupported | missing_passage
    confidence: float | None  # None when no model was asked

    @property
    def needs_review(self) -> bool:
        if self.verdict != "verified":
            return True
        return self.confidence is None or self.confidence < AUTO_ACCEPT_CONFIDENCE


def parse_quotes_block(block: str) -> dict[str, str]:
    """Map each hash ID in a summarize quotes block to its passage text."""
    headers = list(_QUOTE_HEADER_RE.finditer(block))
    passages: dict[str, str] = {}
    for i, m in enumerate(headers):
        end = headers[i + 1].start() if i + 1 < len(headers) else len(block)
        passages[m.group(1)] = block[m.end() : end].strip()
    return passages


def cited_claims(answer: str) -> list[tuple[str, tuple[str, ...]]]:
    """Every sentence of an answer that cites at least one hash ID.

    Returns ``(claim_text, hash_ids)`` pairs; the caller attaches passage text.
    Uncited sentences are out of scope here — summarize already drops uncited
    factual sentences.
    """
    out = []
    for sentence in _split_sentences(answer):
        ids = tuple(dict.fromkeys(HASH_ID_RE.findall(sentence)))
        if not ids:
            continue
        claim = re.sub(r"\s*\[[^\]]*#q[0-9a-f]{4,}[^\]]*\]", "", sentence).strip()
        if claim:
            out.append((claim, ids))
    return out


def check_answer(answer: str, passages: dict[str, str], judge: JudgeFn) -> list[CheckedClaim]:
    """Judge every cited sentence of one answer against the passages it cites."""
    checked: list[CheckedClaim] = []
    for claim_text, ids in cited_claims(answer):
        missing = [i for i in ids if i not in passages]
        section = "\n\n".join(passages[i] for i in ids if i in passages)
        cited = CitedClaim(claim=claim_text, hash_ids=ids, section=section)
        if missing:
            # Cannot happen for a verified summary; kept so a corrupted record
            # surfaces instead of being judged against partial evidence.
            checked.append(CheckedClaim(cited, "missing_passage", None))
            continue
        j = judge(claim_text, section)
        checked.append(CheckedClaim(cited, VERDICT_OF[j.relation], j.confidence))
    return checked


def make_typesafe_judge(api_key: str, model: str) -> JudgeFn:
    """Production judge: one TypeSafe Choice question per cited claim."""
    from typesafe_sdk import Choice, TypeSafeClient

    client = TypeSafeClient(api_key=api_key)
    # The recipe's wording, unchanged: telling the model the claim was derived
    # from the section would presuppose the support being tested.
    question = Choice(instructions="How does the section relate to the claim?", criteria=RELATIONS)

    def judge(claim: str, section: str) -> Judgment:
        resp = client.system_one(
            model=model,
            state={"claim": claim, "section": section},
            questions={"relation": question},
        )
        answer = resp.choices["relation"]
        return Judgment(relation=answer.choice, confidence=answer.confidence)

    return judge
