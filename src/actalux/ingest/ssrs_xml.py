"""Shared primitives for the Missouri DESE SSRS XML report family.

ASBR, Indirect Cost, Local Effort, and Per-Pupil are each exported by DESE's SSRS
report engine into XML with a report-specific namespace, and each report's dollar
figures use the same simple cell format ($-formatted or bare, with ``-`` or blank
meaning zero). ASBR alone also carries parenthetical negatives, a format the
others don't use, so it keeps its own money cell parser/formatter rather than
sharing these.
"""

from __future__ import annotations

from decimal import Decimal

from actalux.errors import ParseError


def qualify_tag(ns: str, tag: str) -> str:
    """An ElementTree-qualified tag name for a report's own XML namespace."""
    return f"{{{ns}}}{tag}"


def parse_money_cell(raw: str | None, *, label: str) -> Decimal:
    """Parse a $-formatted or bare money/number cell into a Decimal; '-' or blank is zero."""
    if raw is None or raw.strip() in ("", "-"):
        return Decimal(0)
    try:
        return Decimal(raw.replace("$", "").replace(",", "").strip())
    except (ArithmeticError, ValueError) as exc:
        raise ParseError(f"Unparseable {label} cell {raw!r}: {exc}") from exc


def format_money_cell(amount: Decimal) -> str:
    """Render a Decimal as ``$x,xxx.xx`` (no parenthetical-negative handling)."""
    return f"${amount:,.2f}"


def clean_label(raw: str | None) -> str:
    """Collapse a label cell's CR/LF + surrounding whitespace into one clean line."""
    return " ".join((raw or "").split())
