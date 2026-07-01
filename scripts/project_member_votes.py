"""Project member votes and council matters into graph edges (connections-graph §4).

For each current minutes document, this resolves every roll-call name + mover/seconder
against the curated roster and writes the citation-backed person edges (voted_aye_on /
voted_no_on / voted_abstain_on / moved / seconded). Names the roster cannot resolve go
to the subject_resolution_queue rather than being guessed.

For council, it also mints a matter subject per bill/resolution number found in the
motion text (Phase 2) and writes a 'considered' edge from each matter to the vote that
acted on it — so a matter's whole timeline is one cited read. Both edge kinds are
written together per document (one delete-then-insert), so the two projections never
clobber each other. Deterministic and verbatim — every edge carries the vote's durable
identity + citation, nothing is invented.

Idempotent: a document's edges are rebuilt (delete-then-insert) each run, and a
full run prunes edges left on superseded documents (§4.5). Cheap enough to run
nightly after vote re-extraction; reads gate on
``projection_complete = true AND documents.replaces_id IS NULL``.

Run (prefix with `doppler run --project mac --config dev --`):
  uv run python scripts/project_member_votes.py            # dry run, all bodies
  uv run python scripts/project_member_votes.py --doc 826  # dry run, one document
  uv run python scripts/project_member_votes.py --apply    # write all bodies
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from actalux.config import load_config  # noqa: E402
from actalux.db import get_client, get_document_chunks, get_entity_by_path  # noqa: E402
from actalux.graph.matters import (  # noqa: E402
    collect_matters,
    derive_document_matter_mentions,
    derive_matter_edges,
)
from actalux.graph.project import derive_document_edges  # noqa: E402
from actalux.graph.store import (  # noqa: E402
    current_documents,
    current_minutes,
    document_votes,
    load_roster,
    prune_stale_graph,
    prune_stale_mentions,
    replace_document_graph,
    replace_document_mentions,
    upsert_matters,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Bodies the projector covers. Schools + council record per-member roll calls
# (voted_* edges) plus mover/seconder; the Plan Commission and Board of Adjustment
# record only mover/seconder + an aggregate outcome (no roll call), so they yield
# moved/seconded edges only. derive_document_edges handles both shapes uniformly.
PLACE = ("mo", "clayton")
BODIES = ("schools", "council", "plan-commission", "board-of-adjustment")


def _resolve_scope(client) -> tuple[int, dict[int, str]]:
    """Return (place_id, {entity_id: body_slug}) for the in-scope bodies."""
    place_id = None
    entities: dict[int, str] = {}
    for body in BODIES:
        entity = get_entity_by_path(client, *PLACE, body)
        if not entity:
            raise SystemExit(f"Unknown body {'/'.join(PLACE)}/{body}; seed the entity first.")
        place_id = entity["place_id"]
        entities[entity["id"]] = body
    if place_id is None:
        raise SystemExit("No in-scope bodies resolved.")
    return place_id, entities


def _project_matter_mentions(
    client, council_eid: int, matter_ids: dict[str, int], apply: bool, doc_filter: set[int] | None
) -> tuple[int, int, int]:
    """Project matter mentions across all current council documents.

    A mention is a cited occurrence of a minted matter (bill/resolution number) in any
    current council document — minutes, agenda, staff report, or transcript — so a
    matter's page can show where it was discussed beyond the formal roll-call votes.
    Rebuilt per document (delete-then-insert); a full run prunes mentions left on
    superseded/out-of-scope docs. Returns (mention_count, docs_scanned, docs_pruned).
    """
    docs = current_documents(client, [council_eid])
    if doc_filter is not None:
        docs = [d for d in docs if d["id"] in doc_filter]
    total = 0
    scanned: set[int] = set()
    for doc in docs:
        scanned.add(doc["id"])
        chunks = get_document_chunks(client, doc["id"])
        mentions = derive_document_matter_mentions(doc["id"], chunks, matter_ids)
        total += len(mentions)
        if apply:
            replace_document_mentions(client, doc["id"], mentions)
    pruned = 0
    if apply and doc_filter is None:
        pruned = prune_stale_mentions(client, scanned)
    return total, len(scanned), pruned


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--doc", type=int, action="append", help="document id (repeatable)")
    parser.add_argument("--apply", action="store_true", help="write to the DB (default: dry run)")
    args = parser.parse_args()

    config = load_config()
    if args.apply and not config.supabase_service_key:
        raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required to --apply.")
    client = get_client(config.supabase_url, config.supabase_service_key or config.supabase_key)

    place_id, entities = _resolve_scope(client)
    roster = load_roster(client, place_id)
    logger.info("Loaded roster: %d subjects.", len(roster))

    if args.doc:
        docs = [d for d in current_minutes(client, list(entities)) if d["id"] in set(args.doc)]
    else:
        docs = current_minutes(client, list(entities))

    # Matter pre-pass (council only): scan council motions for bill/resolution numbers
    # and mint a matter subject per number, so per-doc derivation can attach matter
    # edges. Council votes are cached here to avoid a second read in the loop below.
    council_eid = next((eid for eid, body in entities.items() if body == "council"), None)
    council_votes: dict[int, list] = {}
    matter_ids: dict[str, int] = {}
    if council_eid is not None:
        for doc in docs:
            if doc["entity_id"] == council_eid:
                council_votes[doc["id"]] = document_votes(client, doc["id"], council_eid)
        matters = collect_matters([v for vs in council_votes.values() for v in vs])
        if args.apply:
            matter_ids = upsert_matters(client, place_id, matters)
        else:  # dry run: synthetic ids so matter edges still derive for the count
            matter_ids = {slug: -(i + 1) for i, slug in enumerate(matters)}
        logger.info(
            "Matters: %d council bills/resolutions (%s).",
            len(matters),
            "upserted" if args.apply else "dry run",
        )

    edge_types: Counter[str] = Counter()
    total_edges = total_queue = 0
    current_doc_ids: set[int] = set()
    for doc in docs:
        doc_id = doc["id"]
        current_doc_ids.add(doc_id)
        votes = (
            council_votes[doc_id]
            if doc_id in council_votes
            else document_votes(client, doc_id, doc["entity_id"])
        )
        edges, queue = derive_document_edges(votes, roster)
        if doc["entity_id"] == council_eid and matter_ids:
            edges = edges + derive_matter_edges(votes, matter_ids)
        for e in edges:
            edge_types[e["type"]] += 1
        total_edges += len(edges)
        total_queue += len(queue)
        # Rebuild this document's graph even when it now derives nothing: a current doc
        # that previously had edges but no longer does (votes removed, or every name went
        # unresolvable) must have its stale edges CLEARED, not skipped. The empty case is
        # the clear — replace_document_graph deletes by source_document_id then inserts
        # nothing (a no-op delete for a doc that never had edges).
        if args.apply:
            replace_document_graph(client, doc_id, edges, queue)
        if edges or queue:
            logger.info(
                "  doc %s (%s): %d edges, %d queued",
                doc_id,
                entities.get(doc["entity_id"], "?"),
                len(edges),
                len(queue),
            )

    pruned = 0
    if args.apply and not args.doc:
        pruned = prune_stale_graph(client, current_doc_ids)

    verb = "Wrote" if args.apply else "Would write"
    logger.info(
        "%s %d edges across %d docs (%d queued for review). By type: %s. Stale docs pruned: %d.",
        verb,
        total_edges,
        len(current_doc_ids),
        total_queue,
        dict(edge_types),
        pruned,
    )

    # Matter mentions (council only): every current council doc, references to minted
    # matters. Runs after minting so matter_ids is populated (synthetic in a dry run).
    if council_eid is not None and matter_ids:
        doc_filter = set(args.doc) if args.doc else None
        m_total, m_docs, m_pruned = _project_matter_mentions(
            client, council_eid, matter_ids, args.apply, doc_filter
        )
        logger.info(
            "%s %d matter mentions across %d council docs. Stale mention docs pruned: %d.",
            verb,
            m_total,
            m_docs,
            m_pruned,
        )

    if not args.apply:
        logger.info("Dry run. Re-run with --apply to write.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
