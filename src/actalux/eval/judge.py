"""LLM relevance judge for the retrieval eval.

Grades a (query, passage) pair 0-3 for how well the passage helps answer the
query, judged independently of which retrieval arm surfaced it. Grades are
cached to disk keyed by (query_id, chunk_id) so a passage is graded once and
reused across arms and across runs -- the judge is paid for once.

Judge model is Claude (stronger than the gpt-4o-mini summary model, which
matters because these grades decide a methods question). Spot-check a sample
of grades against your own relevance sense before trusting the aggregate.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

import anthropic
from anthropic.types import TextBlock

logger = logging.getLogger(__name__)

JUDGE_MODEL = "claude-sonnet-4-6"
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


def grade_relevance(query: str, passage: str, api_key: str, model: str = JUDGE_MODEL) -> int:
    """Grade one (query, passage) pair 0-3 via Claude.

    Raises on an API error or an unparseable response so the caller can decide
    whether to skip the pair (and report reduced coverage) rather than silently
    treating a failure as grade 0.
    """
    client = anthropic.Anthropic(api_key=api_key)
    resp = client.messages.create(
        model=model,
        max_tokens=8,
        system=GRADE_SYSTEM,
        messages=[
            {
                "role": "user",
                "content": GRADE_USER.format(query=query, passage=passage[:MAX_PASSAGE_CHARS]),
            }
        ],
    )
    text = "".join(b.text for b in resp.content if isinstance(b, TextBlock)).strip()
    match = _DIGIT_RE.search(text)
    if not match:
        raise ValueError(f"judge returned no 0-3 digit: {text!r}")
    return int(match.group())
