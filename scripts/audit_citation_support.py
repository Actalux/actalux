"""Audit whether cited passages support the sentences that cite them.

Reads the answer-quality eval's recorded answers (eval/answers.json: per query,
per model, the answer text and the quotes block recorded with it). The recorded
passages are capped at 700 characters, so each is expanded to its full chunk
text from the database (see full_passages) before judging,
judges every cited sentence with actalux.search.citation_support, and writes a
report. Offline and read-only against the corpus: nothing here touches the live
answer path or the database.

Run (prefix with `doppler run --project actalux --config dev --`):
  uv run python scripts/audit_citation_support.py                  # production model's answers
  uv run python scripts/audit_citation_support.py --model all      # every model in the file
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from actalux.config import load_config  # noqa: E402
from actalux.db import get_client  # noqa: E402
from actalux.search.citation_support import (  # noqa: E402
    check_answer,
    make_jev_judge,
    parse_quotes_block,
)

logger = logging.getLogger(__name__)

ANSWERS_PATH = Path("eval/answers.json")
REPORT_PATH = Path("eval/citation_support.json")
# The key eval/answers.json uses for the production answer model.
PRODUCTION_KEY = "gpt-5-mini"


_WS = re.compile(r"\s+")
# Stable citation IDs are 8 hex characters; shorter IDs are legacy chunk row ids
# rendered as hex (models.chunk_hash_id).
_CITATION_ID_LEN = 8


def _norm(text: str) -> str:
    return _WS.sub(" ", text).strip()


def full_passages(client, recorded: dict[str, str]) -> dict[str, str]:
    """Replace each recorded (700-char-capped) passage with its full chunk text.

    The answer eval stores quotes cut to QUOTE_CHARS for its judge prompt, but the
    answer model saw whole chunks — judging against the cut text calls a claim
    "unsupported" whenever its fact sits past the cut. The full text is used only
    when the database chunk still BEGINS with the recorded text (chunk row ids can
    change on re-ingest); otherwise the passage is omitted, so the claim is
    reported as missing evidence rather than judged against the wrong passage.
    """
    out: dict[str, str] = {}
    for hash_id, recorded_text in recorded.items():
        key = hash_id.removeprefix("#q")
        query = client.table("chunks").select("content")
        if len(key) == _CITATION_ID_LEN:
            rows = query.eq("citation_id", key).limit(1).execute().data
        else:
            rows = query.eq("id", int(key, 16)).limit(1).execute().data
        content = (rows[0]["content"] if rows else "") or ""
        if content and _norm(content).startswith(_norm(recorded_text)):
            out[hash_id] = content
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--model",
        default=PRODUCTION_KEY,
        help="which recorded model's answers to audit, or 'all' (default: %(default)s)",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    cfg = load_config()
    if not cfg.openrouter_api_key:
        raise SystemExit("OPENROUTER_ACTALUX_KEY is not set.")
    judge = make_jev_judge(cfg.openrouter_api_key, cfg.jev_model, cfg.decisions_url)
    client = get_client(cfg.supabase_url, cfg.supabase_service_key or cfg.supabase_key)

    answers = json.loads(ANSWERS_PATH.read_text())
    rows: list[dict] = []
    for query_id, by_model in sorted(answers.items()):
        for model, rec in sorted(by_model.items()):
            if args.model != "all" and model != args.model:
                continue
            passages = full_passages(client, parse_quotes_block(rec.get("quotes") or ""))
            for c in check_answer(rec.get("answer") or "", passages, judge):
                rows.append(
                    {
                        "query_id": query_id,
                        "answer_model": model,
                        "claim": c.claim.claim,
                        "hash_ids": list(c.claim.hash_ids),
                        "verdict": c.verdict,
                        "confidence": c.confidence,
                        "needs_review": c.needs_review,
                    }
                )

    verdicts = Counter(r["verdict"] for r in rows)
    review = sum(r["needs_review"] for r in rows)
    REPORT_PATH.write_text(
        json.dumps({"judge_model": cfg.jev_model, "rows": rows}, indent=2) + "\n"
    )
    logger.info("claims judged: %d  verdicts: %s", len(rows), dict(verdicts))
    logger.info("flagged for review: %d  report: %s", review, REPORT_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
