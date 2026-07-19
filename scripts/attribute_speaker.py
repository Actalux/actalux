"""Attribute a rejected speaker cluster to the official a human identified.

A rejection (confirm_speaker.py deny) locks a (document, cluster) slot at
confidence='rejected' so the wrong name is never re-proposed — but it stores nothing
about who the voice actually is (Option B). When a human later identifies the true
speaker (by ear, or from the minutes), this tool records that decision: it deletes
the rejected row (the documented un-decide path — migrate_043's trigger blocks moving
a locked row off its tier by UPDATE, and DELETE is deliberately left open) and inserts
a confirmed/manual row for the identified official. The replacement row is itself a
locked tier, so the slot stays protected against the automatic resolver.

Guards (all hard, mirroring confirm_speaker.py's Option-B scope):
  * the slot must currently hold exactly one row, at confidence='rejected' — empty or
    auto-tier slots belong to the normal resolver/review pipeline, not this tool;
  * the named subject must be publishable, person-linked, and hold a roster membership
    in the document's body (officials only — a citizen is never attributable);
  * the document must belong to the requested body.

The membership term window is checked as a WARNING only: a meeting date outside the
official's roster term is suspicious but the roster window may itself be observed-basis
(first/last sighting), so the human decision wins.

Dry-run by default. Usage (one attribution per invocation, deliberately):

    doppler run --project mac --config dev -- uv run python scripts/attribute_speaker.py \
        --state mo --place clayton --body schools \
        --doc 2145 --cluster SPEAKER_13 --name "Kim Hurst" --apply
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from supabase import Client  # noqa: E402

from actalux.config import load_config  # noqa: E402
from actalux.db import get_client, get_entity_by_path  # noqa: E402
from actalux.errors import ActaluxError  # noqa: E402

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AttributionPlan:
    """A validated replace-rejection-with-attribution, ready to execute."""

    rejected_row_id: int
    rejected_subject_name: str
    document_id: int
    cluster_label: str
    subject_id: int
    subject_name: str
    warnings: tuple[str, ...]


def plan_attribution(
    *,
    document: dict[str, Any],
    entity_id: int,
    slot_rows: list[dict[str, Any]],
    subjects: list[dict[str, Any]],
    membership: dict[str, Any] | None,
    cluster_label: str,
    name: str,
    subject_names: dict[int, str],
) -> AttributionPlan:
    """Validate one attribution and return the executable plan (pure; raises on any guard)."""
    if document.get("entity_id") != entity_id:
        raise ActaluxError(
            f"document {document['id']} belongs to entity {document.get('entity_id')}, "
            f"not the requested body (entity {entity_id})"
        )
    if len(slot_rows) != 1:
        raise ActaluxError(
            f"expected exactly one identity row for doc {document['id']} {cluster_label}, "
            f"found {len(slot_rows)} — this tool only replaces an existing rejection"
        )
    slot = slot_rows[0]
    if slot.get("confidence") != "rejected":
        raise ActaluxError(
            f"doc {document['id']} {cluster_label} is at {slot.get('confidence')!r}, not "
            "'rejected' — confirm/deny flows own non-rejected slots (confirm_speaker.py)"
        )
    if len(subjects) != 1:
        found = [s.get("canonical_name") for s in subjects]
        raise ActaluxError(f"name {name!r} matched {len(subjects)} place subjects: {found}")
    subject = subjects[0]
    if not subject.get("publishable") or subject.get("person_id") is None:
        raise ActaluxError(f"subject {name!r} is not a publishable, person-linked official")
    if membership is None:
        raise ActaluxError(
            f"subject {name!r} holds no roster membership in the document's body — "
            "officials only (Option B); roster-add first (scripts/seed_roster.py)"
        )

    warnings = []
    meeting_date = document.get("meeting_date")
    start, end = membership.get("start_date"), membership.get("end_date")
    if meeting_date and start and str(meeting_date) < str(start):
        warnings.append(f"meeting {meeting_date} predates roster term start {start}")
    if meeting_date and end and str(meeting_date) > str(end):
        warnings.append(f"meeting {meeting_date} postdates roster term end {end}")

    return AttributionPlan(
        rejected_row_id=slot["id"],
        rejected_subject_name=subject_names.get(slot.get("subject_id"), "?"),
        document_id=document["id"],
        cluster_label=cluster_label,
        subject_id=subject["id"],
        subject_name=subject["canonical_name"],
        warnings=tuple(warnings),
    )


def _service_client() -> Client:
    """A service-key Supabase client (speaker_identities writes are service-only)."""
    cfg = load_config()
    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise ActaluxError("ACTALUX_SUPABASE_SERVICE_KEY is required")
    return get_client(cfg.supabase_url, key)


def _build_plan(client: Client, args: argparse.Namespace) -> AttributionPlan:
    """Load everything the guards need and validate."""
    entity = get_entity_by_path(client, args.state, args.place, args.body)
    if entity is None:
        raise ActaluxError(f"no entity for {args.state}/{args.place}/{args.body}")

    docs = (
        client.table("documents")
        .select("id,entity_id,meeting_title,meeting_date")
        .eq("id", args.doc)
        .execute()
        .data
    )
    if not docs:
        raise ActaluxError(f"document {args.doc} not found")
    document = docs[0]

    slot_rows = (
        client.table("speaker_identities")
        .select("id,subject_id,confidence,basis")
        .eq("document_id", args.doc)
        .eq("cluster_label", args.cluster)
        .execute()
        .data
    )
    subjects = (
        client.table("subjects")
        .select("id,person_id,publishable,canonical_name,place_id")
        .eq("place_id", entity["place_id"])
        .eq("canonical_name", args.name)
        .execute()
        .data
    )
    membership = None
    if len(subjects) == 1:
        memberships = (
            client.table("memberships")
            .select("subject_id,entity_id,role,start_date,end_date")
            .eq("entity_id", entity["id"])
            .eq("subject_id", subjects[0]["id"])
            .execute()
            .data
        )
        membership = memberships[0] if memberships else None

    prior_ids = {r.get("subject_id") for r in slot_rows if r.get("subject_id") is not None}
    subject_names = {}
    if prior_ids:
        for s in (
            client.table("subjects")
            .select("id,canonical_name")
            .in_("id", sorted(prior_ids))
            .execute()
            .data
        ):
            subject_names[s["id"]] = s["canonical_name"]

    return plan_attribution(
        document=document,
        entity_id=entity["id"],
        slot_rows=slot_rows,
        subjects=subjects,
        membership=membership,
        cluster_label=args.cluster,
        name=args.name,
        subject_names=subject_names,
    )


def _execute(client: Client, plan: AttributionPlan) -> None:
    """Replace the rejected row with the human attribution (delete, then insert)."""
    client.table("speaker_identities").delete().eq("id", plan.rejected_row_id).execute()
    client.table("speaker_identities").insert(
        {
            "document_id": plan.document_id,
            "cluster_label": plan.cluster_label,
            "subject_id": plan.subject_id,
            "confidence": "confirmed",
            "basis": "manual",
        }
    ).execute()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--state", required=True)
    parser.add_argument("--place", required=True)
    parser.add_argument("--body", required=True)
    parser.add_argument("--doc", type=int, required=True, help="document id")
    parser.add_argument("--cluster", required=True, help="cluster label, e.g. SPEAKER_13")
    parser.add_argument("--name", required=True, help="official's canonical roster name")
    parser.add_argument("--apply", action="store_true", help="write (dry-run without)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = _service_client()
    plan = _build_plan(client, args)

    logger.info(
        "doc %d %s: rejected-under %s (row %d) -> %s (subject %d, confirmed/manual)",
        plan.document_id,
        plan.cluster_label,
        plan.rejected_subject_name,
        plan.rejected_row_id,
        plan.subject_name,
        plan.subject_id,
    )
    for w in plan.warnings:
        logger.warning("%s", w)

    if not args.apply:
        logger.info("dry run — pass --apply to record the attribution")
        return
    _execute(client, plan)
    logger.info(
        "attributed: doc %d %s = %s", plan.document_id, plan.cluster_label, plan.subject_name
    )


if __name__ == "__main__":
    main()
