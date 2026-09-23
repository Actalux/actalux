"""Audit whether cited passages support the sentences that cite them.

Reads the answer-quality eval's recorded answers (eval/answers.json: per query,
per model, the answer text and the exact quotes block the model was shown),
judges every cited sentence with actalux.search.citation_support, and writes a
report. Offline and read-only against the corpus: nothing here touches the live
answer path or the database.

Run (prefix with `doppler run --project mac --config dev --`):
  uv run python scripts/audit_citation_support.py                  # production model's answers
  uv run python scripts/audit_citation_support.py --model all      # every model in the file
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from actalux.config import load_config  # noqa: E402
from actalux.search.citation_support import (  # noqa: E402
    check_answer,
    make_typesafe_judge,
    parse_quotes_block,
)

logger = logging.getLogger(__name__)

ANSWERS_PATH = Path("eval/answers.json")
REPORT_PATH = Path("eval/citation_support.json")
# The key eval/answers.json uses for the production answer model.
PRODUCTION_KEY = "gpt-5-mini"


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
    if not cfg.typesafe_api_key:
        raise SystemExit("ACTALUX_TYPESAFE_API_KEY is not set.")
    judge = make_typesafe_judge(cfg.typesafe_api_key, cfg.typesafe_model)

    answers = json.loads(ANSWERS_PATH.read_text())
    rows: list[dict] = []
    for query_id, by_model in sorted(answers.items()):
        for model, rec in sorted(by_model.items()):
            if args.model != "all" and model != args.model:
                continue
            passages = parse_quotes_block(rec.get("quotes") or "")
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
        json.dumps({"judge_model": cfg.typesafe_model, "rows": rows}, indent=2) + "\n"
    )
    logger.info("claims judged: %d  verdicts: %s", len(rows), dict(verdicts))
    logger.info("flagged for review: %d  report: %s", review, REPORT_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
