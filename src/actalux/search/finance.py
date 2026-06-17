"""Structured-finance routing for the answer path.

The text retriever reads budget figures out of fragmented OCR'd table chunks,
which the answer-quality eval showed is where finance faithfulness collapses
(the LLM mis-reads or re-sums column fragments). This module instead answers a
figure-shaped finance query from the structured ``budget_line_items`` table,
where every figure is already parsed, audited, and carries a verbatim
``source_quote`` plus the ``chunk_id`` it was read from -- so a structured
answer still cites a real source chunk through the unchanged citation pipeline.

Two pieces:

- ``finance_intent(query)`` -- a deterministic keyword router. It returns a
  ``FinanceIntent`` (which slice of the table to pull) only when the query is a
  figure-shaped finance ask we can serve from structured data; otherwise None,
  and the caller falls back to text retrieval. It is deliberately conservative:
  per-pupil and tax-rate/levy asks have no structured figure and are guarded out
  so they stay on the text path.

- ``build_finance_evidence(client, intent)`` -- pulls the rows and renders each
  as a citeable evidence dict shaped like an enriched search result, so the
  existing ``generate_summary`` -> citation-verify -> result-card -> reader-pane
  pipeline runs unchanged.

The vocabulary constants below (fund names, function/source labels) are the
distinct values present in ``budget_line_items`` as of 2026-06-07; they are data,
not invented.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

from supabase import Client

from actalux.db import get_budget_line_items, get_documents
from actalux.models import chunk_hash_id

logger = logging.getLogger(__name__)

MAX_FINANCE_ITEMS = 16  # cap on evidence rows handed to the summary LLM

# --- Vocabulary (distinct budget_line_items values, 2026-06-07) --------------
# Each alias list maps natural-language phrasings to the canonical label stored
# in the table. Matching is case-insensitive substring against the query.

# Fund aliases require the word "fund" (or an unambiguous fund phrase) so a
# query about the *debt service function* (principal/interest) does not collide
# with the *Debt Service fund*.
FUND_ALIASES: dict[str, list[str]] = {
    "General": ["general fund", "general operating", "operating fund"],
    "Special Revenue (Teachers)": ["special revenue", "teachers fund", "teacher fund"],
    "Debt Service": ["debt service fund"],
    "Capital Projects": ["capital projects", "capital fund", "capital project"],
}

# Expenditure functions (dimension='function', category='expenditure').
FUNCTION_ALIASES: dict[str, list[str]] = {
    "Instruction": ["instruction", "instructional", "teaching"],
    "Operation of plant": [
        "operation of plant",
        "plant operation",
        "facilities maintenance",
        "facility maintenance",
        "building maintenance",
        "operations and maintenance",
        "maintenance costs",
    ],
    "Food services": ["food service", "school meals", "cafeteria", "lunch program"],
    "Health services": ["health services", "nursing services"],
    "Security services": ["security services", "school security"],
    "Media services": ["media services", "library services"],
    "Debt service - Principal retirements": [
        "principal retirement",
        "principal payment",
        "debt principal",
        "principal and interest",
    ],
    "Debt service - Interest and other charges": [
        "interest and other",
        "debt interest",
        "interest payment",
        "interest charges",
        "principal and interest",
    ],
    "Improvement of instruction and professional development": [
        "professional development",
        "improvement of instruction",
    ],
    "Business services": ["business services"],
    "Executive administration": ["executive administration"],
    "Building level administration": ["building administration", "building-level administration"],
    "Board of Education services": ["board of education services", "board services"],
}

# Revenue sources (dimension='source', category='revenue').
SOURCE_ALIASES: dict[str, list[str]] = {
    "Local": ["local revenue", "local source", "local funding", "local tax revenue"],
    "County": ["county revenue", "county source", "county funding"],
    "State": ["state revenue", "state source", "state funding", "state aid"],
    "Federal": ["federal revenue", "federal source", "federal funding", "federal grant"],
}

EXPENDITURE_WORDS = (
    "spend", "spent", "spending", "expenditure", "expenditures",
    "cost", "costs", "expense", "expenses", "paid", "payment", "payments",
)  # fmt: skip
REVENUE_WORDS = (
    "revenue", "revenues", "income", "funding source", "receipts",
)  # fmt: skip
BALANCE_WORDS = (
    "fund balance", "fund balances", "reserve", "reserves", "ending balance", "surplus",
)  # fmt: skip

# A figure ask with no concrete label routes to a district total only on an
# explicit total word -- not a bare "how much", which is too loose (e.g. "how
# much does the math program cost" has no structured figure).
TOTAL_WORDS = ("total", "overall", "combined", "district-wide", "districtwide", "all funds")

# Asks that sound financial but have no structured figure -> force the text path.
NEGATIVE_GUARDS = (
    "per-pupil", "per pupil", "per student", "per-student",
    "tax rate", "tax levy", "levy", "millage", "mill rate", "tax hearing",
)  # fmt: skip

# "over time" style asks span all years -> ignore any single-year filter.
TREND_WORDS = (
    "over time",
    "trend",
    "history",
    "historical",
    "each year",
    "by year",
    "year over year",
)

_YEAR_RE = re.compile(r"\b(20\d{2})\b")


@dataclass(frozen=True)
class FinanceIntent:
    """A resolved structured-finance query: which rows of budget_line_items to cite."""

    measure: str  # human-readable description, for logging/transparency
    dimension: str  # "fund" | "function" | "source"
    category: str  # "revenue" | "expenditure" | "fund_balance"
    funds: tuple[str, ...] = ()  # restrict to these funds; () = all funds
    subcategories: tuple[str, ...] = ()  # restrict to these labels; () = all in dim/category
    fiscal_year: str | None = None  # single-year filter; None = all years
    aggregate_funds: bool = False  # sum across funds per (year, subcategory) into one citeable row


def _matched_labels(ql: str, aliases: dict[str, list[str]]) -> list[str]:
    """Canonical labels whose any alias appears in the lowercased query."""
    return [label for label, phrases in aliases.items() if any(p in ql for p in phrases)]


def _detect_category(ql: str) -> str | None:
    """Pick the figure category from measure words (balance > revenue > expenditure)."""
    if any(w in ql for w in BALANCE_WORDS):
        return "fund_balance"
    if any(w in ql for w in REVENUE_WORDS):
        return "revenue"
    if any(w in ql for w in EXPENDITURE_WORDS):
        return "expenditure"
    return None


def _detect_fiscal_year(ql: str) -> str | None:
    """Map a bare year token to a stored fiscal_year label.

    Fiscal years are named by their ending year (the corpus stores "2023-2024"),
    so "fiscal year 2024" -> "2023-2024". A trend ask ("over time") spans all
    years, so no single-year filter is applied even if a year is mentioned.
    """
    if any(w in ql for w in TREND_WORDS):
        return None
    m = _YEAR_RE.search(ql)
    if not m:
        return None
    year = int(m.group(1))
    return f"{year - 1}-{year}"


def finance_intent(query: str) -> FinanceIntent | None:
    """Resolve a query to a structured-finance intent, or None for the text path.

    Conservative by design: returns an intent only when the query carries a
    figure category (spend/revenue/fund balance) AND either a concrete label
    (a fund, function, or source) or an explicit total ask. Anything that trips
    a negative guard (per-pupil, tax levy) or names no structured figure stays
    on the text path.
    """
    ql = query.lower()

    if any(g in ql for g in NEGATIVE_GUARDS):
        return None

    category = _detect_category(ql)
    if category is None:
        return None

    funds = _matched_labels(ql, FUND_ALIASES)
    functions = _matched_labels(ql, FUNCTION_ALIASES)
    sources = _matched_labels(ql, SOURCE_ALIASES)
    fiscal_year = _detect_fiscal_year(ql)

    # Fund balance: per-fund if a fund is named, else the district total over years.
    if category == "fund_balance":
        return FinanceIntent(
            measure=f"ending fund balance ({', '.join(funds) if funds else 'all funds'})",
            dimension="fund",
            category="fund_balance",
            funds=tuple(funds),
            subcategories=("Ending fund balance",),
            fiscal_year=fiscal_year,
            aggregate_funds=not funds,
        )

    # A named expenditure function -> the function rows, summed across funds per year.
    if functions and category != "revenue":
        return FinanceIntent(
            measure=f"expenditure on {', '.join(functions)}",
            dimension="function",
            category="expenditure",
            funds=tuple(funds),  # optionally narrow to one fund
            subcategories=tuple(functions),
            fiscal_year=fiscal_year,
            aggregate_funds=True,
        )

    # A named revenue source -> the source rows (already district-level).
    if sources and category == "revenue":
        return FinanceIntent(
            measure=f"revenue from {', '.join(sources)}",
            dimension="source",
            category="revenue",
            subcategories=tuple(sources),
            fiscal_year=fiscal_year,
        )

    # A named fund, no function/source -> that fund's total revenue or expenditure.
    if funds:
        total_label = "Total expenditures" if category == "expenditure" else "Total revenues"
        return FinanceIntent(
            measure=f"{', '.join(funds)} fund {category}",
            dimension="fund",
            category=category,
            funds=tuple(funds),
            subcategories=(total_label,),
            fiscal_year=fiscal_year,
        )

    # No label, but an explicit total ask -> district total over years.
    if any(w in ql for w in TOTAL_WORDS):
        total_label = "Total expenditures" if category == "expenditure" else "Total revenues"
        return FinanceIntent(
            measure=f"district total {category}",
            dimension="fund",
            category=category,
            subcategories=(total_label,),
            fiscal_year=fiscal_year,
            aggregate_funds=True,
        )

    # Figure category but nothing concrete to pull -> narrative; use text.
    return None


# --- Evidence assembly -------------------------------------------------------


@dataclass
class _Row:
    """One rendered budget figure, pre-aggregation-agnostic."""

    fiscal_year: str
    category: str
    amount: float
    chunk_id: int | None
    document_id: int | None
    source_quote: str
    subcategory: str = ""
    fund: str = ""
    scope: str = ""  # display scope, e.g. "General Fund" or "all governmental funds"
    extra_funds: list[str] = field(default_factory=list)


def _select_rows(client: Client, intent: FinanceIntent) -> list[dict[str, Any]]:
    """Pull and filter the budget_line_items rows the intent points at."""
    items = get_budget_line_items(client, category=intent.category, dimension=intent.dimension)
    rows = items
    if intent.funds:
        rows = [r for r in rows if r.get("fund") in intent.funds]
    if intent.subcategories:
        rows = [r for r in rows if r.get("subcategory") in intent.subcategories]
    if intent.fiscal_year:
        rows = [r for r in rows if r.get("fiscal_year") == intent.fiscal_year]
    return rows


def _aggregate_across_funds(rows: list[dict[str, Any]]) -> list[_Row]:
    """Sum amounts across funds per (fiscal_year, subcategory) into one citeable row.

    Mirrors the public Budget page's all-funds aggregation (web.charts): a
    function like Instruction is stored once per fund, and the governmental-funds
    total is their sum -- which is also the row total printed in the shared source
    statement, so the representative chunk's source_quote contains the figure.
    """
    groups: dict[tuple[str, str], _Row] = {}
    for r in rows:
        key = (r["fiscal_year"], r.get("subcategory") or "")
        existing = groups.get(key)
        if existing is None:
            groups[key] = _Row(
                fiscal_year=r["fiscal_year"],
                category=r.get("category") or "",
                amount=float(r["amount"]),
                chunk_id=r.get("chunk_id"),
                document_id=r.get("document_id"),
                source_quote=r.get("source_quote") or "",
                subcategory=r.get("subcategory") or "",
                scope="all governmental funds",
                extra_funds=[r.get("fund") or ""],
            )
        else:
            existing.amount += float(r["amount"])
            existing.extra_funds.append(r.get("fund") or "")
            if existing.chunk_id is None:
                existing.chunk_id = r.get("chunk_id")
    return list(groups.values())


def _as_rows(rows: list[dict[str, Any]]) -> list[_Row]:
    """Wrap raw line items as per-fund display rows (no aggregation)."""
    out: list[_Row] = []
    for r in rows:
        fund = r.get("fund") or ""
        out.append(
            _Row(
                fiscal_year=r["fiscal_year"],
                category=r.get("category") or "",
                amount=float(r["amount"]),
                chunk_id=r.get("chunk_id"),
                document_id=r.get("document_id"),
                source_quote=r.get("source_quote") or "",
                subcategory=r.get("subcategory") or "",
                fund=fund,
                scope=f"{fund} Fund" if fund else "",
            )
        )
    return out


def _usd(amount: float) -> str:
    """Format a dollar amount with thousands separators, no cents."""
    return f"${amount:,.0f}"


def _render_content(row: _Row) -> str:
    """A clean, self-contained figure line plus the verbatim source quote."""
    fy = row.fiscal_year
    amt = _usd(row.amount)
    scope = row.scope or "all governmental funds"
    if row.category == "fund_balance":
        head = f"FY{fy} - ending fund balance ({scope}): {amt}"
    elif row.subcategory in ("Total expenditures", "Total revenues") or not row.subcategory:
        kind = "total expenditures" if row.category == "expenditure" else "total revenues"
        head = f"FY{fy} - {scope} {kind}: {amt}"
    else:
        kind = "expenditure" if row.category == "expenditure" else "revenue"
        head = f"FY{fy} - {row.subcategory} ({scope}, {kind}): {amt}"
    quote = row.source_quote.strip()
    return f'{head}. Verbatim source: "{quote}"' if quote else head


def _doc_summary_label(row: _Row) -> str:
    """Short descriptor for the result card's Document line."""
    what = row.subcategory or (
        "ending fund balance" if row.category == "fund_balance" else row.category
    )
    return f"Audited budget figure - {what}, {row.scope or 'all funds'}, FY{row.fiscal_year}"


def build_finance_evidence(
    client: Client, intent: FinanceIntent, *, max_items: int = MAX_FINANCE_ITEMS
) -> list[dict[str, Any]]:
    """Render the intent's rows as citeable evidence dicts (enriched-result shape).

    Returns at most ``max_items`` rows, most recent fiscal year first. Each dict
    carries the figure's real ``chunk_id`` -> ``hash_id``, so citations verify and
    the reader pane opens the verbatim source statement. Returns [] if the table
    has no matching rows (caller then falls back to text retrieval).
    """
    raw = _select_rows(client, intent)
    if not raw:
        return []

    display = _aggregate_across_funds(raw) if intent.aggregate_funds else _as_rows(raw)
    # Recent first; stable secondary sort by subcategory so multi-label asks group.
    display.sort(key=lambda r: (r.fiscal_year, r.subcategory), reverse=True)
    display = [r for r in display if r.chunk_id is not None][:max_items]
    if not display:
        return []

    docs = get_documents(client, [r.document_id for r in display if r.document_id is not None])
    evidence: list[dict[str, Any]] = []
    for r in display:
        doc = docs.get(r.document_id or -1, {})
        chunk_id = r.chunk_id
        # Phase 2: finance citations still route on the numeric chunk id (the
        # legacy form the /chunk resolver still serves); Phase 3 swaps in the
        # budget figure's stable citation_id. cite_ref keeps the shape uniform
        # with the text path so the citation linker treats both identically.
        evidence.append(
            {
                "chunk_id": chunk_id,
                "citation_id": "",
                "cite_ref": chunk_id,
                "hash_id": chunk_hash_id(chunk_id),
                "content": _render_content(r),
                "section": "Budget figure",
                "speaker": "",
                "rrf_score": 0.0,  # present for enriched-result shape compatibility
                "meeting_date": doc.get("meeting_date", ""),
                "meeting_title": doc.get("meeting_title", ""),
                "document_id": r.document_id,
                "document_type": doc.get("document_type", ""),
                "summary": _doc_summary_label(r),
            }
        )
    logger.info(
        "finance routing: %s -> %d citeable rows (dimension=%s, aggregate=%s)",
        intent.measure,
        len(evidence),
        intent.dimension,
        intent.aggregate_funds,
    )
    return evidence
