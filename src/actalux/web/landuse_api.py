"""Stealth keyed API for the land-use case dataset.

No public surface: these routes appear in no sitemap, no UI, and no link — and
an unauthorized request gets a plain 404, indistinguishable from the route not
existing. The dataset is the future paid product
(docs/architecture/land-use-cases.md); until keys are issued, nobody outside
this repo can tell it is here.

Two gates, both required, both failing to 404:

- **A key.** The operator's global key (``ACTALUX_API_KEY``) or a per-holder
  key via the existing keyed path. Unlike the public API — which is open when
  no global key is set — these routes never serve anonymous traffic, whatever
  the global toggles say.
- **A service client.** The tables are RLS-denied to the publishable key, and
  the production web host deliberately carries no service key. Until
  ``ACTALUX_SUPABASE_SERVICE_KEY`` is deployed there, the routes 404 for
  everyone — turning the product on for customers is a secrets decision, made
  on purpose, not a side effect of shipping this module.

The payload is cases and their citation-backed events, never derived rankings
or approval-rate aggregates: sell events, not conclusions (council verdict,
2026-08). Interpretation happens on the buyer's side.
"""

from __future__ import annotations

import hmac
import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from actalux.db import get_client
from actalux.web.api import _authorize_keyed, _presented_key, resolve_api_place
from actalux.web.retrieval import get_config

logger = logging.getLogger(__name__)

_MAX_PAGE = 100


def _service_db():
    """Service-role client, or None where no service key is configured (prod today)."""
    cfg = get_config()
    if not cfg.supabase_service_key:
        return None
    return get_client(cfg.supabase_url, cfg.supabase_service_key)


def stealth_gate(request: Request) -> None:
    """Admit only keyed callers, and only where the dataset is servable; else 404.

    401 would confirm the route exists and invite probing; 404 says nothing.
    The one deliberate exception: a *valid* key over its monthly quota keeps its
    429 from the keyed path — that caller already knows the surface exists.
    """
    cfg = get_config()
    presented = _presented_key(request)
    if not presented or _service_db() is None:
        raise HTTPException(status_code=404, detail="Not Found")
    if cfg.api_key and hmac.compare_digest(presented, cfg.api_key):
        return
    try:
        _authorize_keyed(request, presented)
    except HTTPException as exc:
        if exc.status_code == 429:
            raise
        raise HTTPException(status_code=404, detail="Not Found") from None


landuse_router = APIRouter(prefix="/api/v1", dependencies=[Depends(stealth_gate)])


@landuse_router.get("/{state}/{place}/land-use/cases", include_in_schema=False)
def list_cases(
    place_row: dict = Depends(resolve_api_place),
    application_type: str | None = Query(None),
    status: str | None = Query(None),
    date_from: str | None = Query(None, description="first_seen >= (YYYY-MM-DD)"),
    date_to: str | None = Query(None, description="first_seen <= (YYYY-MM-DD)"),
    limit: int = Query(50, ge=1, le=_MAX_PAGE),
    offset: int = Query(0, ge=0),
) -> dict:
    """Entitlement cases for one place, newest first. Events come per-case."""
    db = _service_db()
    q = (
        db.table("land_use_cases")
        .select(
            "id,entity_id,address_raw,application_type,subtype_raw,status,"
            "decision_role,staff_recommendation,outcome_matches_staff,"
            "first_seen,resolved_date",
            count="exact",
        )
        .eq("place_id", place_row["id"])
    )
    if application_type:
        q = q.eq("application_type", application_type)
    if status:
        q = q.eq("status", status)
    if date_from:
        q = q.gte("first_seen", date_from)
    if date_to:
        q = q.lte("first_seen", date_to)
    result = q.order("first_seen", desc=True).range(offset, offset + limit - 1).execute()
    return {
        "place": f"{place_row.get('state', '')}/{place_row.get('slug', '')}",
        "count": result.count,
        "offset": offset,
        "cases": result.data or [],
    }


@landuse_router.get("/{state}/{place}/land-use/cases/{case_id}", include_in_schema=False)
def get_case(
    case_id: int,
    place_row: dict = Depends(resolve_api_place),
) -> dict:
    """One case with its citation-backed events and per-record parties.

    The place filter is part of the lookup, not a decoration: a case id from
    another jurisdiction 404s here, per the scoping cardinal rule.
    """
    db = _service_db()
    rows = (
        db.table("land_use_cases")
        .select("*")
        .eq("id", case_id)
        .eq("place_id", place_row["id"])
        .execute()
        .data
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Not Found")
    case = rows[0]
    events = (
        db.table("land_use_case_events")
        .select(
            "event_date,action,conditions_text,video_timestamp,document_id,"
            "chunk_id,citation_id,source_quote"
        )
        .eq("case_id", case_id)
        .order("event_date")
        .execute()
        .data
    )
    parties = (
        db.table("land_use_case_parties")
        .select("role,name_raw")
        .eq("case_id", case_id)
        .execute()
        .data
    )
    return {"case": case, "events": events or [], "parties": parties or []}
