#!/usr/bin/env python3
"""Re-date documents whose date lives in their content, not their filename.

A few documents (the facilities-plan volumes) carry no date in their filename, so
``parse_meeting_date`` returns None and ingest falls back to the ingest day. Their
real date is stated on the cover ("Delivered to District on: ..."). This corrects
those, deriving each date from a verbatim string in the document's own content.

Honest by construction: a date is written only if its ``anchor`` string is still
present verbatim in the document's content. If the anchor is missing (content
changed, wrong doc), the script refuses that row rather than writing an unsourced
date. The mapping is explicit — no content-date guessing — so a reviewer can
trace every date to the line it came from.

Dry-run by default; --apply writes (needs ACTALUX_SUPABASE_SERVICE_KEY).
Idempotent: a doc already on its target date is skipped.

Usage:
  doppler run --project mac --config dev -- uv run python scripts/redate_from_content.py
  doppler run --project mac --config dev -- uv run python scripts/redate_from_content.py --apply
"""

from __future__ import annotations

import argparse
import logging
import os

from actalux.db import get_client

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# doc_id -> the date stated in that document, with the verbatim anchor it is read
# from. The anchor must appear in the document's content for the date to be
# written (see module docstring). Dates are ISO; anchors are quoted exactly as
# they appear in the source.
CONTENT_DATES = [
    {
        "doc_id": 87,
        "date": "2025-02-19",
        "anchor": "Delivered to District on: 02.19.2025",
        "note": "LRFMP Volume I cover delivery date",
    },
    {
        "doc_id": 88,
        "date": "2024-11-13",
        "anchor": "to the district on Nov. 13, 2024",
        "note": "LRFMP Volume II (demographic study) delivery date",
    },
    {
        "doc_id": 245,
        "date": "2020-08-04",
        "anchor": "Adopted this August 4, 2020",
        "note": "Resolution adoption date stated verbatim in body",
    },
    # Diligent library items carry a "Release Date" (the date the district posted
    # the item); for governance/reference docs with no meeting date that is the
    # authoritative publication date stated verbatim in the page body.
    {
        "doc_id": 163,
        "date": "2005-11-26",
        "anchor": "Release Date November 26, 2005",
        "note": "Diligent portal Release Date for governance library item",
    },
    {
        "doc_id": 250,
        "date": "2020-12-02",
        "anchor": "Release Date December 2, 2020",
        "note": "Diligent portal Release Date for Strategic Plan 2020-2023 library item",
    },
    {
        "doc_id": 251,
        "date": "2020-08-04",
        "anchor": "approved at our August 4, 2020 meeting",
        "note": "ABAR Resolution adoption date (HTML twin of doc 245), stated in body",
    },
    {
        "doc_id": 252,
        "date": "2020-06-02",
        "anchor": "Release Date June 2, 2020",
        "note": "Diligent portal Release Date for BOE Orientation Materials library item",
    },
    {
        "doc_id": 253,
        "date": "2020-01-13",
        "anchor": "Release Date January 13, 2020",
        "note": "Diligent portal Release Date for Board Candidate Resource Guide library item",
    },
    {
        "doc_id": 255,
        "date": "2018-07-23",
        "anchor": "Release Date July 23, 2018",
        "note": "Diligent portal Release Date for Missouri Sunshine Law library item",
    },
    {
        "doc_id": 256,
        "date": "2018-07-22",
        "anchor": "Release Date July 22, 2018",
        "note": "Diligent portal Release Date for Gifted Education Documents library item",
    },
    {
        "doc_id": 271,
        "date": "2022-01-20",
        "anchor": "Release Date January 20, 2022",
        "note": "Diligent portal Release Date for Livestream instructions library item",
    },
    {
        "doc_id": 272,
        "date": "2020-03-23",
        "anchor": "Release Date March 23, 2020",
        "note": "Diligent portal Release Date for public-comment instructions library item",
    },
    {
        "doc_id": 164,
        "date": "2022-11-20",
        "anchor": "Created - Nov 20, 2022",
        "note": "Meeting-schedule document creation date in its footer (covers SY2023-2024)",
    },
]


def plan(client) -> tuple[list[dict], list[dict], list[dict]]:
    """Return (to_apply, already, refused) for the configured re-dates.

    ``to_apply`` rows need the date written (and date_source='content').
    ``already`` rows are fully correct: same date AND date_source='content'.
    Rows whose date is already right but whose date_source is stale (e.g.
    'default' or 'unknown') are returned in ``to_apply`` as provenance-only
    updates, so the column converges on 'content' after a single --apply run.
    """
    to_apply: list[dict] = []
    already: list[dict] = []
    refused: list[dict] = []
    for spec in CONTENT_DATES:
        rows = (
            client.table("documents")
            .select("id, source_file, meeting_date, date_source, content")
            .eq("id", spec["doc_id"])
            .is_("replaces_id", "null")
            .execute()
        ).data or []
        if not rows:
            refused.append({**spec, "reason": "document not found (or superseded)"})
            continue
        doc = rows[0]
        # Match against whitespace-normalized content: PDF extraction sprinkles
        # newlines/odd spacing through the text, so the anchor is written in clean
        # single-space form and compared the same way.
        haystack = " ".join((doc.get("content") or "").split())
        if spec["anchor"] not in haystack:
            refused.append({**spec, "reason": "anchor string not present in content"})
            continue
        record = {
            **spec,
            "source_file": doc.get("source_file", ""),
            "from": doc.get("meeting_date"),
        }
        date_correct = str(doc.get("meeting_date")) == spec["date"]
        provenance_correct = doc.get("date_source") == "content"
        if date_correct and provenance_correct:
            # Both the date and its recorded provenance are already right.
            already.append(record)
        else:
            # Either date is wrong, or provenance is stale (e.g. 'default'/'unknown')
            # even though the date happened to be set correctly some other way.
            # Write both together so a single --apply run fully converges.
            to_apply.append(record)
    return to_apply, already, refused


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write changes (default: dry-run).")
    args = parser.parse_args()

    url = os.environ["ACTALUX_SUPABASE_URL"]
    key_var = "ACTALUX_SUPABASE_SERVICE_KEY" if args.apply else "ACTALUX_SUPABASE_KEY"
    try:
        key = os.environ[key_var]
    except KeyError as exc:
        raise SystemExit(
            f"Missing {exc}; run under doppler run --project mac --config dev -- ..."
        ) from exc

    client = get_client(url, key)
    to_apply, already, refused = plan(client)

    logger.info(
        "Planned: %d to re-date, %d already correct, %d refused.",
        len(to_apply),
        len(already),
        len(refused),
    )
    for c in to_apply:
        logger.info(
            "   #%s  %s -> %s | %s | anchor: %r",
            c["doc_id"],
            c["from"],
            c["date"],
            c["note"],
            c["anchor"],
        )
    for c in already:
        logger.info("   #%s  already %s (skip)", c["doc_id"], c["date"])
    for c in refused:
        logger.info("   #%s  REFUSED: %s", c["doc_id"], c["reason"])

    if not args.apply:
        logger.info("\nDRY RUN — no changes written. Re-run with --apply to write.")
        return 0

    for c in to_apply:
        # Write both the corrected date and its provenance together so the column
        # is never left with a stale 'default'/'unknown' value after re-dating.
        client.table("documents").update({"meeting_date": c["date"], "date_source": "content"}).eq(
            "id", c["doc_id"]
        ).execute()
    logger.info("\nApplied %d re-dates.", len(to_apply))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
