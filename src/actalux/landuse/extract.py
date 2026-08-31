"""LLM extraction of land-use narrative fields, verbatim-verified per field.

The segmenter (``segment.py``) finds the items deterministically; this module
reads each item's narrative and produces the fields a regex cannot: what staff
recommended, what action the board took, the conditions attached, and who the
parties were. Design: docs/architecture/land-use-cases.md.

The discipline mirrors vote extraction and summaries: **every extracted field
carries a quote, and a quote that does not appear verbatim in the item's own
text kills the field** (whitespace-normalized, case-insensitive — PDF text
layers mangle spacing, never words). A rejected field becomes None and is
reported, so the QA layer sees hallucination pressure instead of absorbing it.

The LLM is an injected callable ``(system, user) -> str`` so tests run without a
network and production can route through OpenRouter exactly like summaries do.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

LlmFn = Callable[[str, str], str]

ACTIONS = frozenset(
    {
        "heard",
        "continued",
        "approved",
        "approved_with_conditions",
        "denied",
        "recommended",
        "withdrawn",
    }
)
STAFF_RECS = frozenset({"approve", "approve_with_conditions", "deny", "none_stated"})
PARTY_ROLES = frozenset({"applicant", "owner", "tenant", "architect", "attorney", "other"})

SYSTEM_PROMPT = """\
You extract structured facts from municipal meeting minutes. You are given the \
verbatim text of ONE business item. Answer in strict JSON only, no markdown.

Rules:
- Every value you extract must be supported by a short verbatim quote copied \
EXACTLY from the item text (the "quote" fields). Do not paraphrase inside quotes.
- If the item does not state a fact, use null rather than guessing.
- "action" is what the body DID at this meeting: heard, continued, approved, \
approved_with_conditions, denied, recommended (to another body), or withdrawn.
- "staff_recommendation" is what STAFF recommended, not what the board did: \
approve, approve_with_conditions, deny, or none_stated.
- "conditions" are the enumerated conditions attached to an approval, verbatim.
- "parties" are the people/firms named with a role: applicant, owner, tenant, \
architect, attorney, other. Use the name exactly as written.
- "code_section" is the cited code/ordinance section for a variance or appeal \
("Section 405.330.A.5"), copied exactly, or null.
- "relief" is the quantified relief requested, copied exactly ("A 200 \
square-foot variance from the maximum living area for an Accessory Dwelling \
Unit of 1,000 square-feet"), or null.

JSON shape:
{"action": "...", "action_quote": "...",
 "staff_recommendation": "...", "staff_quote": "... or null",
 "conditions": ["..."] or [],
 "parties": [{"role": "...", "name": "...", "quote": "..."}],
 "code_section": "... or null", "relief": "... or null"}
"""


@dataclass(frozen=True)
class Party:
    role: str
    name_raw: str


@dataclass(frozen=True)
class ExtractedItem:
    """Verified narrative fields for one business item at one meeting."""

    action: str | None
    action_quote: str | None
    staff_recommendation: str | None
    staff_quote: str | None
    conditions_text: str | None
    parties: tuple[Party, ...]
    # Variance-specific, verbatim (G1): the cited code section and the quantified
    # relief. The values ARE their own quotes — verified directly against the body.
    code_section: str | None = None
    relief_raw: str | None = None
    # Field names whose quotes failed verbatim verification — surfaced to QA,
    # never silently dropped.
    rejected: tuple[str, ...] = field(default=())


class ExtractError(Exception):
    """The LLM response could not be used at all (bad JSON, empty)."""


_WS = re.compile(r"\s+")


def _norm(s: str) -> str:
    return _WS.sub(" ", s).strip().casefold()


def quote_in(quote: str, body: str) -> bool:
    """True when `quote` appears verbatim in `body`, tolerating whitespace only."""
    return bool(quote) and _norm(quote) in _norm(body)


def _parse_json(raw: str) -> dict:
    """Parse the LLM reply, tolerating a fenced block but nothing looser."""
    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.S)
    try:
        out = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ExtractError(f"unparseable LLM reply: {exc}") from exc
    if not isinstance(out, dict):
        raise ExtractError(f"expected a JSON object, got {type(out).__name__}")
    return out


def extract_item(body: str, llm: LlmFn) -> ExtractedItem:
    """Extract and verify one item's narrative fields.

    Verification is per-field: a fabricated staff quote rejects the staff
    recommendation but keeps a well-supported action, so one hallucination does
    not cost the whole item. Vocabulary violations reject the same way — an
    action outside the enum is as untrustworthy as an unsupported quote.
    """
    data = _parse_json(llm(SYSTEM_PROMPT, body))
    rejected: list[str] = []

    def gated(name: str, value, quote, vocab: frozenset[str] | None) -> tuple:
        if value is None:
            return None, None
        if vocab is not None and value not in vocab:
            rejected.append(name)
            return None, None
        if not quote_in(quote or "", body):
            rejected.append(name)
            return None, None
        return value, quote

    action, action_quote = gated("action", data.get("action"), data.get("action_quote"), ACTIONS)
    if data.get("staff_recommendation") == "none_stated":
        # Asserts absence, so there is nothing to quote; the QA layer audits
        # these cheaply (grep for "recommend" in the body).
        staff, staff_quote = "none_stated", None
    else:
        staff, staff_quote = gated(
            "staff_recommendation",
            data.get("staff_recommendation"),
            data.get("staff_quote"),
            STAFF_RECS,
        )

    conditions = [
        c for c in (data.get("conditions") or []) if isinstance(c, str) and quote_in(c, body)
    ]
    dropped = len(data.get("conditions") or []) - len(conditions)
    if dropped:
        rejected.append(f"conditions[{dropped}]")

    parties: list[Party] = []
    for p in data.get("parties") or []:
        if not isinstance(p, dict):
            continue
        role, name, quote = p.get("role"), p.get("name"), p.get("quote")
        if role in PARTY_ROLES and isinstance(name, str) and name and quote_in(quote or "", body):
            parties.append(Party(role=role, name_raw=name))
        else:
            rejected.append("party")

    def verbatim_or_none(name: str) -> str | None:
        value = data.get(name)
        if not isinstance(value, str) or not value:
            return None
        if not quote_in(value, body):
            rejected.append(name)
            return None
        return value

    return ExtractedItem(
        action=action,
        action_quote=action_quote,
        staff_recommendation=staff,
        staff_quote=staff_quote,
        conditions_text="\n".join(conditions) if conditions else None,
        parties=tuple(parties),
        code_section=verbatim_or_none("code_section"),
        relief_raw=verbatim_or_none("relief"),
        rejected=tuple(rejected),
    )


def make_openrouter_llm(api_key: str, model: str, base_url: str | None) -> LlmFn:
    """Production LLM callable, same OpenAI-SDK-against-OpenRouter path as summaries.

    Kwargs come from the summarizer's ``_completion_kwargs`` rather than being
    retyped here: GPT-5-family models take ``max_completion_tokens`` plus a
    minimal ``reasoning_effort``, and without the latter they spend the whole
    budget on hidden reasoning and return empty content — which is exactly how
    this function failed on its first live call.
    """
    from openai import OpenAI

    from actalux.search.summarize import _completion_kwargs

    client = OpenAI(api_key=api_key, base_url=base_url)

    def call(system: str, user: str) -> str:
        messages = [{"role": "system", "content": system}, {"role": "user", "content": user}]
        resp = client.chat.completions.create(**_completion_kwargs(model, messages, 1500))
        text = resp.choices[0].message.content
        if not text:
            raise ExtractError("LLM returned empty content")
        return text

    return call
