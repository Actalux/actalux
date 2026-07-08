"""Label diarization clusters from meeting discourse (the LLM discourse-labeler batch pass).

A second, independent evidence family alongside the deterministic resolver
(scripts/reresolve_identities.py). For every live transcript document of a place (or one
body), a language model reads how the meeting addresses its speakers and proposes
cluster -> roster-member labels (identity/discourse.py). Every proposal is basis='discourse',
inferred_medium — held below the public-display gate, enrollable, gate-contained.

Without --apply this runs the FULL pipeline (including the LLM) but only PRINTS the proposals
and their evidence — the read-only validation mode. With --apply it merges the proposals into
speaker_identities ALONGSIDE the resolver's rows (persist_identities scopes retraction to the
'discourse' basis, so a resolver re-pass never deletes a discourse row and vice versa).

Usage:
    doppler run --project actalux --config dev -- uv run python scripts/label_discourse.py \
        --state mo --place clayton --body council --limit 12          # print-only validation
    doppler run --project actalux --config dev -- uv run python scripts/label_discourse.py \
        --state mo --place clayton --body council --apply             # persist
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from supabase import Client  # noqa: E402

from actalux.config import load_config  # noqa: E402
from actalux.db import fetch_all_rows, get_client, get_place_by_path  # noqa: E402
from actalux.diarization.enrollment import superseded_doc_ids  # noqa: E402
from actalux.errors import ActaluxError  # noqa: E402
from actalux.identity.discourse import (  # noqa: E402
    DISCOURSE_BASES,
    DiscourseClaim,
    label_discourse,
)
from actalux.identity.resolve import (  # noqa: E402
    _place_canonical_rules,
    members_active_on,
    members_for_entity,
    persist_identities,
    turns_for_document,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("label_discourse")


def _service_client() -> Client:
    """A service-key Supabase client (speaker_identities writes are service-only)."""
    cfg = load_config()
    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise ActaluxError("ACTALUX_SUPABASE_SERVICE_KEY is required")
    return get_client(cfg.supabase_url, key)


def _docs_with_turns(client: Client, place_id: int, body: str | None) -> list[dict]:
    """Live (non-superseded) transcript documents of the place that have diarization turns."""
    entities = fetch_all_rows(
        lambda: client.table("entities").select("id,body_slug").eq("place_id", place_id)
    )
    if body:
        entities = [e for e in entities if e.get("body_slug") == body]
    if not entities:
        raise ActaluxError(f"no entities for place {place_id} (body={body!r})")
    entity_ids = [e["id"] for e in entities]
    docs = fetch_all_rows(
        lambda: (
            client.table("documents")
            .select("id,entity_id,replaces_id,meeting_date")
            .in_("entity_id", entity_ids)
            .eq("document_type", "transcript")
        )
    )
    superseded = superseded_doc_ids(docs)
    live_ids = [d["id"] for d in docs if d["id"] not in superseded]
    with_turns = {
        r["document_id"]
        for r in fetch_all_rows(
            lambda: (
                client.table("diarization_turns").select("document_id").in_("document_id", live_ids)
            )
        )
    }
    return sorted((d for d in docs if d["id"] in with_turns), key=lambda d: d["id"])


def _report_doc(doc_id: int, proposals: list, claims: list[DiscourseClaim]) -> None:
    """Print a document's proposals and one supporting quote per proposed cluster."""
    quote_by_cluster: dict[str, DiscourseClaim] = {}
    for c in claims:
        quote_by_cluster.setdefault(c.cluster_label, c)
    logger.info(
        "doc %s: %d proposal(s) from %d validated claim(s)", doc_id, len(proposals), len(claims)
    )
    for p in sorted(proposals, key=lambda x: x.cluster_label):
        ev = quote_by_cluster.get(p.cluster_label)
        sample = f' [{ev.signal}] "{ev.quote[:70]}"' if ev else ""
        logger.info("  %s -> %s (%s/%s)%s", p.cluster_label, p.slug, p.confidence, p.basis, sample)


def main() -> None:
    parser = argparse.ArgumentParser(description="Label speaker clusters from meeting discourse.")
    parser.add_argument("--state", required=True, help="place state slug, e.g. mo")
    parser.add_argument("--place", required=True, help="place slug, e.g. clayton")
    parser.add_argument("--body", help="restrict to one body_slug; default all bodies")
    parser.add_argument("--limit", type=int, help="cap the number of documents processed")
    parser.add_argument(
        "--docs", help="comma-separated document ids to target (for spot-check / validation)"
    )
    parser.add_argument("--apply", action="store_true", help="persist (default: print-only)")
    args = parser.parse_args()

    cfg = load_config()
    if not cfg.openrouter_api_key:
        raise ActaluxError("OPENROUTER_ACTALUX_KEY / OPENROUTER_API_KEY is required")

    service = _service_client()
    place = get_place_by_path(service, args.state, args.place)
    if not place:
        raise ActaluxError(f"no place {args.state}/{args.place}")

    docs = _docs_with_turns(service, place["id"], args.body)
    if args.docs:
        wanted = {int(x) for x in args.docs.split(",") if x.strip()}
        docs = [d for d in docs if d["id"] in wanted]
        missing = wanted - {d["id"] for d in docs}
        if missing:
            logger.warning(
                "requested doc ids not eligible (no turns / superseded): %s", sorted(missing)
            )
    if args.limit:
        docs = docs[: args.limit]
    logger.info(
        "%s/%s%s: %d transcript document(s) with turns (model=%s, apply=%s)",
        args.state,
        args.place,
        f"/{args.body}" if args.body else "",
        len(docs),
        cfg.summary_model,
        args.apply,
    )

    total_proposals = written = 0
    usage: dict[str, int] = {}
    start = time.monotonic()
    for doc in docs:
        # Tenure guard (same as resolve_document): restrict the roster enum the LLM sees to
        # members whose term covers this meeting, so an out-of-tenure official can't be part of
        # the closed vocabulary. Fail-open on an undated document (members_active_on).
        members = members_active_on(
            members_for_entity(service, doc["entity_id"]), doc.get("meeting_date")
        )
        rules = _place_canonical_rules(service, doc["entity_id"])
        turns = turns_for_document(service, doc["id"], rules)
        claims: list[DiscourseClaim] = []
        proposals = label_discourse(
            turns,
            members,
            cfg.openrouter_api_key,
            model=cfg.summary_model,
            base_url=cfg.openrouter_base_url,
            claims_out=claims,
            usage_out=usage,
        )
        total_proposals += len(proposals)
        _report_doc(doc["id"], proposals, claims)
        if args.apply:
            written += persist_identities(
                service, doc["id"], proposals, managed_bases=DISCOURSE_BASES
            )

    elapsed = time.monotonic() - start
    logger.info(
        "%d document(s): %d proposal(s)%s in %.1fs | tokens prompt=%d completion=%d",
        len(docs),
        total_proposals,
        f" ({written} rows written)" if args.apply else " (print-only, no writes)",
        elapsed,
        usage.get("prompt_tokens", 0),
        usage.get("completion_tokens", 0),
    )
    if not args.apply:
        logger.info("print-only — pass --apply to persist discourse proposals")


if __name__ == "__main__":
    main()
