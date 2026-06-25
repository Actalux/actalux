"""Project member votes into graph edges (connections-graph Phase 1).

For each current minutes document of the bodies that record per-member roll calls
(schools + council), this resolves every roll-call name + mover/seconder against
the curated roster and writes the citation-backed edges (voted_aye_on / voted_no_on
/ voted_abstain_on / moved / seconded). Names the roster cannot resolve go to the
subject_resolution_queue rather than being guessed. Deterministic and verbatim —
every edge carries the vote's durable identity + citation, nothing is invented.

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
from actalux.db import get_client, get_entity_by_path  # noqa: E402
from actalux.graph.project import derive_document_edges  # noqa: E402
from actalux.graph.store import (  # noqa: E402
    current_minutes,
    document_votes,
    load_roster,
    prune_stale_graph,
    replace_document_graph,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Bodies that record per-member roll calls (the only ones that yield voted_* edges).
# PC + BoA minutes record only mover/seconder + an aggregate outcome (no roll call),
# so they are out of Phase 1 — see connections-graph §9 / project memory.
PLACE = ("mo", "clayton")
BODIES = ("schools", "council")


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

    edge_types: Counter[str] = Counter()
    total_edges = total_queue = 0
    current_doc_ids: set[int] = set()
    for doc in docs:
        doc_id = doc["id"]
        current_doc_ids.add(doc_id)
        votes = document_votes(client, doc_id, doc["entity_id"])
        edges, queue = derive_document_edges(votes, roster)
        if not edges and not queue:
            continue
        for e in edges:
            edge_types[e["type"]] += 1
        total_edges += len(edges)
        total_queue += len(queue)
        if args.apply:
            replace_document_graph(client, doc_id, edges, queue)
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
    if not args.apply:
        logger.info("Dry run. Re-run with --apply to write.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
