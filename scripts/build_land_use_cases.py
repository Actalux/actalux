"""Build the land-use case dataset from PC-ARB and BoA minutes.

Pipeline per document: segment (deterministic, two grammars) -> extract
narrative fields (LLM, per-field verbatim verification) -> link appearances
into cases -> write cases/events/parties. Design and operator decisions:
docs/architecture/land-use-cases.md.

Idempotent as a whole run: linking is global (a case spans documents), so
--apply rebuilds the three tables from scratch inside this script rather than
patching per document. That is cheap at this corpus size and removes a whole
class of drift; per-document incremental rebuild can come with the nightly
hook once the dataset is trusted.

Documents neither regex grammar parses — BoA prose minutes and the Zoom-era
table layouts — route to the LLM segmenter (G1), whose item spans are admitted
only when each item's opening quote locates verbatim in the document. Vote
linkage (G4) is positional: votes parse in document order, so the k-th
occurrence of a repeated motion belongs to the k-th vote carrying it, and an
item links to the last vote positioned inside its span (actalux.landuse.
votes_link).

Run (prefix with `doppler run --project mac --config dev --`):
  uv run python scripts/build_land_use_cases.py                 # dry run + QA report
  uv run python scripts/build_land_use_cases.py --limit 10      # first N docs per body
  uv run python scripts/build_land_use_cases.py --apply         # write the dataset
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections import Counter
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from actalux.config import load_config  # noqa: E402
from actalux.db import fetch_all_rows, get_client  # noqa: E402
from actalux.landuse.extract import (  # noqa: E402
    ExtractError,
    extract_item,
    make_openrouter_llm,
)
from actalux.landuse.link import Appearance, LinkedCase, link_appearances  # noqa: E402
from actalux.landuse.segment import entitlement_items, segment_items  # noqa: E402
from actalux.landuse.segment_llm import llm_segment_items  # noqa: E402
from actalux.landuse.votes_link import link_votes  # noqa: E402

logger = logging.getLogger(__name__)

LAND_USE_BODIES = ("plan-commission", "board-of-adjustment")
REVIEW_PATH = Path("data/landuse_review.jsonl")


def gather_appearances(client, llm, entity_ids: dict[str, int], limit: int | None) -> tuple:
    """Segment + extract every minutes doc for the land-use bodies."""
    appearances: list[Appearance] = []
    qa = Counter()
    unsegmented: list[dict] = []
    extract_failures: list[dict] = []
    rejected_fields = Counter()

    for slug, eid in entity_ids.items():
        docs = fetch_all_rows(
            lambda eid=eid: (
                client.table("documents")
                .select("id,meeting_date")
                .eq("entity_id", eid)
                .eq("document_type", "minutes")
                .is_("replaces_id", "null")
                .order("meeting_date")
            )
        )
        if limit:
            docs = docs[:limit]
        logger.info("%s: %d minutes documents", slug, len(docs))
        votes_by_doc: dict[int, list[dict]] = {}
        for v in fetch_all_rows(
            lambda: (
                client.table("votes")
                .select("id,document_id,motion")
                .in_("document_id", [d["id"] for d in docs])
            )
        ):
            votes_by_doc.setdefault(v["document_id"], []).append(v)
        for doc in docs:
            content = (
                client.table("documents")
                .select("content")
                .eq("id", doc["id"])
                .execute()
                .data[0]["content"]
                or ""
            )
            items = entitlement_items(content)
            segmenter = "regex"
            qa["docs"] += 1
            if not items:
                # A meeting that was all ARB work (segmentation fine, nothing in
                # scope) is routine; a document neither grammar parses goes to the
                # LLM segmenter (G1) — BoA prose and the Zoom-era tables — whose
                # spans are admitted only when the item's opening locates verbatim.
                if segment_items(content):
                    qa["docs_no_entitlements"] += 1
                    continue
                segmenter = "llm"
                try:
                    llm_items, unlocated = llm_segment_items(content, llm)
                except ExtractError as exc:
                    qa["docs_unparsed"] += 1
                    unsegmented.append(
                        {
                            "document_id": doc["id"],
                            "meeting_date": doc["meeting_date"],
                            "error": str(exc),
                        }
                    )
                    continue
                for q in unlocated:
                    extract_failures.append({"document_id": doc["id"], "unlocated_opening": q})
                items = [it for it in llm_items if it.application_type != "arb"]
                if not items:
                    if llm_items:
                        qa["docs_no_entitlements"] += 1
                    else:
                        qa["docs_unparsed"] += 1
                        unsegmented.append(
                            {"document_id": doc["id"], "meeting_date": doc["meeting_date"]}
                        )
                    continue
            vote_links = link_votes(content, items, votes_by_doc.get(doc["id"], []))
            for it in items:
                qa["items"] += 1
                qa[f"items_{segmenter}"] += 1
                try:
                    ext = extract_item(it.body, llm)
                except ExtractError as exc:
                    qa["extract_errors"] += 1
                    extract_failures.append(
                        {"document_id": doc["id"], "address": it.address_raw, "error": str(exc)}
                    )
                    continue
                rejected_fields.update(ext.rejected)
                vote_id = vote_links.get(it.start)
                if vote_id:
                    qa["events_with_vote"] += 1
                appearances.append(
                    Appearance(
                        document_id=doc["id"],
                        entity_id=eid,
                        event_date=date.fromisoformat(str(doc["meeting_date"])[:10]),
                        address_raw=it.address_raw,
                        application_type=it.application_type,
                        subtype_raw=it.subtype_raw,
                        action=ext.action,
                        staff_recommendation=ext.staff_recommendation,
                        conditions_text=ext.conditions_text,
                        video_timestamp=it.video_timestamp,
                        # The item's verbatim opening is the event's citation
                        # anchor; narrative quotes live per-field in extraction.
                        source_quote=it.body[:300],
                        code_section=ext.code_section,
                        relief_raw=ext.relief_raw,
                        vote_id=vote_id,
                        parties=ext.parties,
                    )
                )
    return appearances, qa, unsegmented, extract_failures, rejected_fields


def _retrying(execute, attempts: int = 4):
    """Run one PostgREST call, retrying transient network failures with backoff.

    The rebuild makes ~800 sequential requests; a single dropped connection used
    to kill the run mid-write and leave the tables partial (2026-09-08,
    httpcore.ReadTimeout right after the QA report). Errors that persist through
    the retries still raise — this absorbs blips, not bugs.
    """
    for i in range(attempts):
        try:
            return execute()
        except Exception:
            if i == attempts - 1:
                raise
            time.sleep(2 ** (i + 1))


def write_cases(client, cases: list[LinkedCase], place_by_entity: dict[int, int]) -> None:
    """Rebuild the three tables from the linked cases (delete-then-insert)."""
    _retrying(lambda: client.table("land_use_case_parties").delete().gte("id", 0).execute())
    _retrying(lambda: client.table("land_use_case_events").delete().gte("id", 0).execute())
    _retrying(lambda: client.table("land_use_cases").delete().gte("id", 0).execute())
    for case in cases:
        row = _retrying(
            lambda case=case: (
                client.table("land_use_cases")
                .insert(
                    {
                        "place_id": place_by_entity[case.entity_id],
                        "entity_id": case.entity_id,
                        "address_raw": case.address_raw,
                        "address_norm": case.address_norm,
                        "application_type": case.application_type,
                        "subtype_raw": case.subtype_raw,
                        "status": case.status(),
                        "decision_role": case.decision_role(),
                        "staff_recommendation": case.staff_recommendation(),
                        "outcome_matches_staff": case.outcome_matches_staff(),
                        "first_seen": str(case.first_seen),
                        "resolved_date": (
                            str(case.resolved_date()) if case.resolved_date() else None
                        ),
                    }
                )
                .execute()
            )
        ).data[0]
        events = [
            {
                "case_id": row["id"],
                "document_id": app.document_id,
                "event_date": str(app.event_date),
                "action": app.action or "heard",
                "conditions_text": app.conditions_text,
                "video_timestamp": app.video_timestamp,
                "source_quote": app.source_quote,
                "code_section": app.code_section,
                "relief_raw": app.relief_raw,
                "vote_id": app.vote_id,
            }
            for app in case.appearances
        ]
        _retrying(
            lambda events=events: client.table("land_use_case_events").insert(events).execute()
        )
        seen: set[tuple[str, str]] = set()
        parties = []
        for app in case.appearances:
            for p in app.parties:
                if (p.role, p.name_raw) not in seen:
                    seen.add((p.role, p.name_raw))
                    parties.append({"case_id": row["id"], "role": p.role, "name_raw": p.name_raw})
        if parties:
            _retrying(
                lambda parties=parties: (
                    client.table("land_use_case_parties").insert(parties).execute()
                )
            )


def unparsed_exceeds(unparsed: int, limit: int | None) -> bool:
    """The nightly tripwire: True when the unparsed-document count is over the limit.

    Checked before any write, so a run that trips it leaves the previous dataset
    in place — the same catch-it-in-the-run-that-introduces-it philosophy as
    check_meeting_dates.py. ``None`` disables the check (manual runs).
    """
    return limit is not None and unparsed > limit


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="write to the DB (default: dry run)")
    parser.add_argument("--limit", type=int, help="first N minutes docs per body")
    parser.add_argument(
        "--max-unparsed",
        type=int,
        help=(
            "fail (exit 1, nothing written) if more than N documents parse to no "
            "items — the nightly tripwire against a segmentation regression"
        ),
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    cfg = load_config()
    if not cfg.openrouter_api_key:
        raise SystemExit("OpenRouter key required (extraction is LLM-backed).")
    if args.apply and not cfg.supabase_service_key:
        raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required to --apply.")
    client = get_client(cfg.supabase_url, cfg.supabase_service_key or cfg.supabase_key)
    llm = make_openrouter_llm(cfg.openrouter_api_key, cfg.summary_model, cfg.openrouter_base_url)

    ents = {
        e["body_slug"]: e["id"]
        for e in client.table("entities").select("id,body_slug,place_id").execute().data
        if e["body_slug"] in LAND_USE_BODIES
    }
    place_by_entity = {
        e["id"]: e["place_id"]
        for e in client.table("entities").select("id,place_id").execute().data
    }

    appearances, qa, unsegmented, extract_failures, rejected = gather_appearances(
        client, llm, ents, args.limit
    )
    cases, review = link_appearances(appearances)

    # --- QA report ------------------------------------------------------
    by_type = Counter(c.application_type for c in cases)
    by_status = Counter(c.status() for c in cases)
    resolved = [c for c in cases if c.outcome_matches_staff() is not None]
    followed = sum(1 for c in resolved if c.outcome_matches_staff())
    logger.info("---- QA ----")
    logger.info(
        "docs=%d  no-entitlement-items=%d (ARB-only meetings)  unparsed=%d (coverage gap)",
        qa["docs"],
        qa["docs_no_entitlements"],
        qa["docs_unparsed"],
    )
    logger.info("items by segmenter: regex=%d llm=%d", qa["items_regex"], qa["items_llm"])
    logger.info("vote linkage: %d events linked (positional, verbatim)", qa["events_with_vote"])
    logger.info("items=%d extract_errors=%d", qa["items"], qa["extract_errors"])
    logger.info("rejected fields (hallucination pressure): %s", dict(rejected) or "none")
    logger.info("cases=%d by type: %s", len(cases), dict(by_type))
    logger.info("by status: %s", dict(by_status))
    logger.info(
        "staff-alignment computable for %d cases; board followed staff in %d (%.0f%%)",
        len(resolved),
        followed,
        100 * followed / len(resolved) if resolved else 0,
    )
    logger.info("link review queue: %d", len(review))

    REVIEW_PATH.parent.mkdir(exist_ok=True)
    with REVIEW_PATH.open("w") as f:
        for r in review:
            f.write(
                json.dumps(
                    {
                        "reason": r.reason,
                        "document_id": r.appearance.document_id,
                        "event_date": str(r.appearance.event_date),
                        "address_raw": r.appearance.address_raw,
                        "application_type": r.appearance.application_type,
                        "candidate_case_index": r.candidate_case_index,
                    }
                )
                + "\n"
            )
        for u in unsegmented:
            f.write(json.dumps({"reason": "unsegmented document", **u}) + "\n")
        for x in extract_failures:
            f.write(json.dumps({"reason": "extract error", **x}) + "\n")
    logger.info("review file: %s", REVIEW_PATH)

    if unparsed_exceeds(qa["docs_unparsed"], args.max_unparsed):
        logger.error(
            "unparsed=%d exceeds --max-unparsed %d: a segmentation regression, or new "
            "minutes in a format neither grammar nor the LLM segmenter handles. "
            "Nothing written; the previous dataset stands.",
            qa["docs_unparsed"],
            args.max_unparsed,
        )
        return 1
    if not args.apply:
        logger.info("Dry run. Re-run with --apply to write.")
        return 0
    write_cases(client, cases, place_by_entity)
    logger.info("Wrote %d cases.", len(cases))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
