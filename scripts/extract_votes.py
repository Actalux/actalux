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
  uv run python scripts/extract_votes.py --entity-path mo/clayton/council --apply  # one body
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
    fetch_all_rows,
    get_client,
    get_document,
    get_document_chunks,
    get_document_vote_ids,
    get_entity_by_path,
    insert_votes,
)
from actalux.ingest import votes_parser, votes_parser_civicplus  # noqa: E402
from actalux.ingest.votes_parser import ParsedVote, build_details  # noqa: E402
from actalux.models import Vote  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MINUTES_TYPE = "minutes"
# City-body minutes (entity 2/3) record votes in prose and use a different parser
# from the line-anchored Diligent school-board minutes; dispatch is by portal.
CIVICPLUS_PORTAL = "civicplus"
_MOVED_BY_LINE_RE = re.compile(r"(?im)^moved by:")


def _is_pc_format(content: str) -> bool:
    """True when a CivicPlus doc reads in the Plan Commission prose style.

    Council and Plan Commission both publish on CivicPlus but in different prose:
    council uses "Motion made by <title> ..." / "<title> moved" / "introduced Bill
    No."; PC uses "<name> made a motion to ...". Dispatch by which lead-in the
    document actually carries (more PC lead-ins than council lead-ins) so routing
    does not depend on the database's entity-id assignment.
    """
    return votes_parser_civicplus.count_lead_ins_pc(
        content
    ) > votes_parser_civicplus.count_lead_ins(content)


def _parser_for(doc: dict):
    """(parse_votes, find_citing_chunk) for the document's minutes format."""
    if doc.get("source_portal") != CIVICPLUS_PORTAL:
        return votes_parser.parse_votes, votes_parser.find_citing_chunk
    if _is_pc_format(doc.get("content") or ""):
        return votes_parser_civicplus.parse_votes_pc, votes_parser_civicplus.find_citing_chunk_pc
    return votes_parser_civicplus.parse_votes, votes_parser_civicplus.find_citing_chunk


def _lead_in_count(doc: dict) -> int:
    """Audit denominator: motion lead-ins the document carries, per its format."""
    content = doc.get("content") or ""
    if doc.get("source_portal") != CIVICPLUS_PORTAL:
        return len(_MOVED_BY_LINE_RE.findall(content))
    return max(
        votes_parser_civicplus.count_lead_ins(content),
        votes_parser_civicplus.count_lead_ins_pc(content),
    )


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
    parse_votes, find_citing_chunk = _parser_for(doc)
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

    # Surface motion lead-ins that produced no record (no recognizable motion or
    # result) so a parser gap is visible for audit rather than silent.
    lead_ins = _lead_in_count(doc)
    unparsed = lead_ins - (len(votes) + skipped)
    logger.info(
        "doc %s %s: %d votes, %d uncited, %d of %d motion lead-ins unparsed",
        doc_id,
        doc.get("meeting_date"),
        len(votes),
        skipped,
        max(unparsed, 0),
        lead_ins,
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
    parser.add_argument(
        "--portal", help="scope to one source_portal (e.g. 'civicplus', 'diligent')"
    )
    parser.add_argument(
        "--entity", type=int, help="scope to one public body (entities.id, e.g. 2 = council)"
    )
    parser.add_argument(
        "--entity-path",
        help="scope to one body by path 'state/place/body' (e.g. mo/clayton/council); "
        "resolved to entities.id so CI/workflows need not hardcode the numeric id",
    )
    parser.add_argument("--apply", action="store_true", help="write to the DB (default: dry run)")
    args = parser.parse_args()

    config = load_config()
    key = config.supabase_service_key or config.supabase_key
    if args.apply and not config.supabase_service_key:
        raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required to --apply.")
    client = get_client(config.supabase_url, key)

    # --entity-path resolves a 'state/place/body' slug to entities.id (same lookup
    # ingest.py uses), so the (re)ingest workflows can scope by body without baking
    # in a serial id. It folds into the same entity-scoped query as --entity.
    entity_id = args.entity
    if args.entity_path:
        parts = args.entity_path.strip("/").split("/")
        if len(parts) != 3:
            raise SystemExit(f"--entity-path must be 'state/place/body', got {args.entity_path!r}")
        entity = get_entity_by_path(client, *parts)
        if not entity:
            raise SystemExit(
                f"Unknown entity {args.entity_path!r}; seed it (see migrate_012) first."
            )
        entity_id = entity["id"]

    if args.doc:
        doc_ids = args.doc
    else:
        # Current minutes, optionally scoped to one portal/body. Page past the
        # PostgREST row cap via fetch_all_rows: a bare query returns only the first
        # ~1000 rows and list_documents caps at 500, so an exhaustive vote
        # re-extraction would silently skip the oldest minutes once the corpus
        # grows (610 current minutes already exceed 500) — the same silent vote
        # loss this script exists to prevent.
        def _current_minutes_query():
            q = (
                client.table("documents")
                .select("id")
                .eq("document_type", MINUTES_TYPE)
                .is_("replaces_id", "null")
            )
            if args.portal:
                q = q.eq("source_portal", args.portal)
            if entity_id is not None:
                q = q.eq("entity_id", entity_id)
            return q

        # order by 'id' (default): a unique key gives stable page boundaries
        # (meeting_date has ties). Processing order doesn't matter — docs are
        # independent.
        doc_ids = [r["id"] for r in fetch_all_rows(_current_minutes_query)]

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
