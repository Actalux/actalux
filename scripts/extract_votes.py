"""Extract structured board votes from official minutes into the votes table.

For each current minutes document this parses its text (actalux.ingest.votes_parser)
into structured vote records, links each record to the verbatim minutes chunk it
came from, and writes it to the votes table. Extraction is deterministic and
verbatim — counts are read off the literal per-member roll call, results off the
literal "Carried"/"Failed" word — so nothing is invented; a block that cannot be
cited to a chunk is skipped rather than stored uncited.

Idempotent per document: the freshly parsed votes are inserted, then the
document's prior vote rows are deleted (insert-before-delete, so a failed run
cannot leave a document with no votes). Re-running reproduces the same set.

This populates the corpus + JSON API only; no live page renders votes, so no
deploy is required. Dry-run by default.

Run (prefix each with `doppler run --project mac --config dev --`):
  uv run python scripts/extract_votes.py                 # dry run, all minutes
  uv run python scripts/extract_votes.py --doc 438       # dry run, one document
  uv run python scripts/extract_votes.py --apply         # write all minutes
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from actalux.config import load_config  # noqa: E402
from actalux.db import (  # noqa: E402
    delete_votes,
    get_client,
    get_document,
    get_document_chunks,
    get_document_vote_ids,
    insert_votes,
    list_documents,
)
from actalux.ingest.votes_parser import (  # noqa: E402
    ParsedVote,
    build_details,
    find_citing_chunk,
    parse_votes,
)
from actalux.models import Vote  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MINUTES_TYPE = "minutes"
_MOVED_BY_LINE_RE = re.compile(r"(?im)^moved by:")


def _to_vote(parsed: ParsedVote, doc_id: int, meeting_date: date, chunk: dict) -> Vote:
    """Build a Vote citing ``chunk`` (which is verified to contain the anchor)."""
    return Vote(
        document_id=doc_id,
        meeting_date=meeting_date,
        motion=parsed.motion,
        result=parsed.result,
        result_basis=parsed.result_basis,
        vote_count_yes=parsed.vote_count_yes,
        vote_count_no=parsed.vote_count_no,
        vote_count_abstain=parsed.vote_count_abstain,
        details=build_details(parsed),
        chunk_id=chunk["id"],
        citation_id=chunk.get("citation_id") or "",
        source_quote=parsed.source_quote,
    )


def _build_votes(doc: dict, chunks: list[dict]) -> tuple[list[Vote], int]:
    """Parse a document into citeable Vote records. Returns (votes, skipped_uncited)."""
    parsed = parse_votes(doc.get("content") or "")
    meeting_date = date.fromisoformat(doc["meeting_date"])
    votes: list[Vote] = []
    skipped = 0
    for pv in parsed:
        chunk = find_citing_chunk(pv.anchors, chunks)
        if chunk is None:
            skipped += 1
            logger.warning(
                "doc %s: vote not citeable to any chunk, skipping: %.80s",
                doc["id"],
                pv.motion,
            )
            continue
        votes.append(_to_vote(pv, doc["id"], meeting_date, chunk))
    return votes, skipped


def _tally_str(v: Vote) -> str:
    if v.vote_count_yes is None:
        return "no roll call"
    return f"{v.vote_count_yes}-{v.vote_count_no}-{v.vote_count_abstain} (y-n-a)"


def process_document(client, doc_id: int, *, apply: bool) -> tuple[int, int]:
    """Extract one document's votes. Returns (votes_loaded, skipped_uncited)."""
    doc = get_document(client, doc_id)
    if not doc or doc.get("document_type") != MINUTES_TYPE:
        raise SystemExit(f"doc {doc_id} is not a current minutes document")
    chunks = get_document_chunks(client, doc_id)
    votes, skipped = _build_votes(doc, chunks)

    # Surface "Moved by:" blocks that produced no record (no recognizable motion
    # or result) so a parser gap is visible for audit rather than silent.
    moved_blocks = len(_MOVED_BY_LINE_RE.findall(doc.get("content") or ""))
    unparsed = moved_blocks - (len(votes) + skipped)
    logger.info(
        "doc %s %s: %d votes, %d uncited, %d of %d moved-by blocks unparsed",
        doc_id,
        doc.get("meeting_date"),
        len(votes),
        skipped,
        max(unparsed, 0),
        moved_blocks,
    )
    for v in votes:
        logger.info(
            "  [%s/%s] %s — %s — %s",
            v.result,
            v.result_basis,
            _tally_str(v),
            v.citation_id,
            v.motion[:66],
        )

    if apply:
        # Re-derive idempotently: insert the freshly parsed votes (if any), then
        # delete the document's prior rows. Deleting unconditionally — even when
        # this parse yields nothing — clears stale votes so a re-run reproduces the
        # current set exactly (insert-before-delete keeps a failed run non-lossy).
        prior = get_document_vote_ids(client, doc_id)
        if votes:
            insert_votes(client, votes)
        delete_votes(client, prior)
    return len(votes), skipped


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--doc", type=int, action="append", help="document id (repeatable)")
    parser.add_argument("--apply", action="store_true", help="write to the DB (default: dry run)")
    args = parser.parse_args()

    config = load_config()
    key = config.supabase_service_key or config.supabase_key
    if args.apply and not config.supabase_service_key:
        raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required to --apply.")
    client = get_client(config.supabase_url, key)

    if args.doc:
        doc_ids = args.doc
    else:
        doc_ids = [d["id"] for d in list_documents(client, document_type=MINUTES_TYPE)]

    total_votes = 0
    total_skipped = 0
    docs_with_votes = 0
    for doc_id in doc_ids:
        loaded, skipped = process_document(client, doc_id, apply=args.apply)
        total_votes += loaded
        total_skipped += skipped
        if loaded:
            docs_with_votes += 1

    verb = "Loaded" if args.apply else "Would load"
    logger.info(
        "%s %d votes across %d/%d minutes docs (%d skipped as uncited).",
        verb,
        total_votes,
        docs_with_votes,
        len(doc_ids),
        total_skipped,
    )
    if not args.apply:
        logger.info("Dry run. Re-run with --apply to write.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
