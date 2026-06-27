"""LLM relevance judge for the retrieval eval.

Grades a (query, passage) pair 0-3 for how well the passage helps answer the
query, judged independently of which retrieval arm surfaced it. Grades are
cached to disk keyed by (query_id, chunk_id) so a passage is graded once and
reused across arms and across runs -- the judge is paid for once.

Judge model is Claude (stronger than the gpt-4o-mini summary model, which
matters because these grades decide a methods question), reached through
OpenRouter like every other LLM call so offline eval needs only the one
OpenRouter key — same model and prompts, just a different transport. Spot-check
a sample of grades against your own relevance sense before trusting the aggregate.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

from openai import OpenAI

logger = logging.getLogger(__name__)

# Provider-prefixed so the OpenAI SDK reaches Claude through OpenRouter.
JUDGE_MODEL = "anthropic/claude-sonnet-4-6"
DEFAULT_BASE_URL = "https://openrouter.ai/api/v1"
MAX_PASSAGE_CHARS = 4000

GRADE_SYSTEM = """\
You grade how well a passage from Clayton, MO school district public records \
answers a citizen's search query. Output ONLY a single digit 0-3:

3 = directly answers the query; a citizen would cite this passage for it.
2 = relevant and useful, but partial or supporting (not the whole answer).
1 = tangentially related; same topic area but does not address the query.
0 = unrelated, or boilerplate/headers/page furniture with no informational value.

Judge only whether the passage's CONTENT addresses the QUERY. Do not reward a \
passage for merely repeating query keywords. Output the digit and nothing else.\
"""

GRADE_USER = """\
Query: {query}

Passage:
{passage}

Relevance grade (0, 1, 2, or 3):"""

_DIGIT_RE = re.compile(r"[0-3]")


def cache_key(query_id: str, chunk_id: int) -> str:
    """Stable judgment-cache key for a (query, chunk) pair."""
    return f"{query_id}::{chunk_id}"


def load_cache(path: Path) -> dict[str, Any]:
    """Load the judgment cache, or an empty shell if it does not exist yet."""
    if path.exists():
        return json.loads(path.read_text())
    return {"model": JUDGE_MODEL, "grades": {}}


def save_cache(path: Path, cache: dict[str, Any]) -> None:
    """Persist the judgment cache (pretty-printed for readable diffs)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, indent=2, sort_keys=True) + "\n")


def grade_relevance(
    query: str,
    passage: str,
    api_key: str,
    model: str = JUDGE_MODEL,
    base_url: str = DEFAULT_BASE_URL,
) -> int:
    """Grade one (query, passage) pair 0-3 via Claude (through OpenRouter).

    Raises on an API error or an unparseable response so the caller can decide
    whether to skip the pair (and report reduced coverage) rather than silently
    treating a failure as grade 0.
    """
    client = OpenAI(api_key=api_key, base_url=base_url)
    resp = client.chat.completions.create(
        model=model,
        max_tokens=8,
        messages=[
            {"role": "system", "content": GRADE_SYSTEM},
            {
                "role": "user",
                "content": GRADE_USER.format(query=query, passage=passage[:MAX_PASSAGE_CHARS]),
            },
        ],
    )
    text = (resp.choices[0].message.content or "").strip()
    match = _DIGIT_RE.search(text)
    if not match:
        raise ValueError(f"judge returned no 0-3 digit: {text!r}")
    return int(match.group())


# --- Answer-quality judge (synthesis eval) ---------------------------------
#
# Grades the generated summary, not retrieval. All three dimensions are judged
# ONLY against the quotes the answer was given, so the score isolates synthesis
# quality from retrieval recall (recall is measured separately by the relevance
# judge above). A cross-model judge (Claude grading an OpenAI-written answer)
# avoids self-preference bias.

ANSWER_DIMENSIONS = ("faithfulness", "completeness", "directness")

ANSWER_GRADE_SYSTEM = """\
You grade the quality of an AI-generated answer for Actalux, an independent, \
nonpartisan service that makes Clayton, MO school district public records \
searchable. The answer must \
ground every claim in the provided source quotes and cite them like [#q003f].

Grade THREE dimensions, each an integer 0-3, judging the ANSWER ONLY against \
the provided quotes (never outside knowledge):

faithfulness -- are the answer's claims supported by the quotes it cites?
  3 = every claim is directly supported by its cited quote; nothing invented.
  2 = mostly supported; one minor unsupported detail or loose citation.
  1 = a notable claim is not supported by its citation.
  0 = significant unsupported or fabricated claims.

completeness -- does the answer capture the key relevant information PRESENT IN \
THE QUOTES for this query? Judge only against the supplied quotes, not what the \
full archive might contain.
  3 = uses the relevant quotes well; covers the main points they contain.
  2 = covers most; misses a secondary available point.
  1 = misses important information that the quotes contain.
  0 = barely uses the relevant quotes.

directness -- does it answer the question asked, plainly and without filler?
  3 = directly and clearly answers the query.
  2 = answers but with hedging or filler.
  1 = partial or evasive.
  0 = does not answer the query (or says nothing was found when quotes exist).

Check each claim against the quotes, then FINISH your reply with the grades as a \
JSON object on its own line -- this exact shape, with nothing after it:
{"faithfulness": <0-3>, "completeness": <0-3>, "directness": <0-3>}\
"""

ANSWER_GRADE_USER = """\
Query: {query}

Provided source quotes (the only evidence the answer was allowed to use):
{quotes}

AI-generated answer:
{answer}

Assess each claim, then end with the JSON grades:"""

# Flat object only (no nested braces); the judge may reason first, so take the
# LAST object in the reply -- its final verdict.
_JSON_OBJ_RE = re.compile(r"\{[^{}]*\}")


def grade_answer(
    query: str,
    answer: str,
    quotes: str,
    api_key: str,
    model: str = JUDGE_MODEL,
    base_url: str = DEFAULT_BASE_URL,
) -> dict[str, int]:
    """Grade a generated answer on faithfulness/completeness/directness (each 0-3).

    Raises on an API error or an unparseable response so the caller skips the
    query (reduced coverage) rather than scoring on a hole.
    """
    client = OpenAI(api_key=api_key, base_url=base_url)
    resp = client.chat.completions.create(
        # Headroom for the judge to reason through every claim before the JSON; a
        # tight cap truncated thorough analyses mid-JSON.
        model=model,
        max_tokens=1024,
        messages=[
            {"role": "system", "content": ANSWER_GRADE_SYSTEM},
            {
                "role": "user",
                "content": ANSWER_GRADE_USER.format(query=query, quotes=quotes, answer=answer),
            },
        ],
    )
    text = resp.choices[0].message.content or ""
    matches = _JSON_OBJ_RE.findall(text)
    if not matches:
        raise ValueError(f"answer judge returned no JSON object: {text!r}")
    parsed = json.loads(matches[-1])
    scores = {}
    for dim in ANSWER_DIMENSIONS:
        value = parsed.get(dim)
        if not isinstance(value, int) or not 0 <= value <= 3:
            raise ValueError(f"answer judge {dim!r} not a 0-3 int: {parsed!r}")
        scores[dim] = value
    return scores
