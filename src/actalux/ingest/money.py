"""Shared money-cell parsing/formatting for the simple DESE financial XML reports.

Per-Pupil, Indirect Cost, and Local Effort cells share one shape: a dollar-formatted
or bare decimal, or a dash/empty meaning zero. The ASBR report's cells are a richer
superset (parenthetical negatives, a "$-" sentinel) and keep their own parser/formatter
in ``asbr_xml.py`` rather than reuse this one.
"""

from __future__ import annotations

from decimal import Decimal

from actalux.errors import ParseError


def parse_money(raw: str | None, *, label: str) -> Decimal:
    """Parse a $-formatted or bare money/number cell into a Decimal; ``-``/empty is zero.

    ``label`` names the report in the raised ``ParseError`` ("Per-Pupil", "Indirect
    Cost", "Local Effort", ...), matching each report's own message text.
    """
    if raw is None or raw.strip() in ("", "-"):
        return Decimal(0)
    try:
        return Decimal(raw.replace("$", "").replace(",", "").strip())
    except (ArithmeticError, ValueError) as exc:
        raise ParseError(f"Unparseable {label} cell {raw!r}: {exc}") from exc


def format_money(amount: Decimal) -> str:
    """Render a Decimal as ``$1,234.56`` (no negative-parenthesization)."""
    return f"${amount:,.2f}"
