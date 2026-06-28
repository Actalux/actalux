#!/usr/bin/env python3
"""Print the speaker-identity review queue for a body (operator tool).

Lists below-gate cluster -> official proposals (``inferred_low`` / ``inferred_medium``)
that the deterministic resolver wasn't confident enough to publish, so the operator can
confirm or reject them. Reads with the service key — these rows are hidden from the
public by RLS, so this is intentionally a local operator tool, not a web surface.

Run:
  doppler run --project mac --config dev -- \
    uv run python scripts/review_identities.py --body council
"""

from __future__ import annotations

import argparse
import logging

from actalux.config import load_config
from actalux.db import get_client, get_entity_by_path, get_identity_review_queue
from actalux.ingest.bodies import get_body

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main() -> None:
    parser = argparse.ArgumentParser(description="Show the speaker-identity review queue.")
    parser.add_argument("--body", default="council", help="public body (default: %(default)s)")
    args = parser.parse_args()

    cfg = load_config()
    if not cfg.supabase_service_key:
        raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required to read the review queue")
    service = get_client(cfg.supabase_url, cfg.supabase_service_key)

    body = get_body(args.body)
    entity = get_entity_by_path(service, *body.entity_path.split("/"))
    if not entity:
        raise SystemExit(f"Unknown entity {body.entity_path!r}; seed it first.")

    queue = get_identity_review_queue(service, entity["id"])
    if not queue:
        print("review queue empty")
        return
    for r in queue:
        print(
            f"{r['meeting_date'] or '????-??-??'}  doc {r['document_id']:>5}  "
            f"{r['cluster_label']:<12}  {r['confidence']:<16}  {r['basis'] or '-':<10}  "
            f"-> {r['candidate_subject'] or '(none)'}"
        )
    print(f"\n{len(queue)} proposal(s) awaiting review for {body.entity_path}")


if __name__ == "__main__":
    main()
