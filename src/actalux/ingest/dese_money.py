"""Shared money-cell parsing/formatting for the Missouri DESE XML report parsers.

Local Effort, Indirect Cost, and Per-Pupil reports all render dollar figures as
plain cells: ``$75,837,799.36`` (or bare ``59125445.29``), with ``-``/``$-``/empty
meaning zero. The ASBR report additionally uses parenthetical negatives
(``($362,049.22)``) and is parsed by its own ``asbr_xml.parse_money`` instead of
sharing this path.
"""

from __future__ import annotations

from decimal import Decimal

from actalux.errors import ParseError


def parse_money(raw: str | None, descriptor: str) -> Decimal:
    """Parse a bare or $-formatted money cell into a Decimal.

    ``descriptor`` names the report/cell kind for the error message on an
    unparseable cell, e.g. ``"Local Effort"`` -> "Unparseable Local Effort cell ...".
    """
    if raw is None or raw.strip() in ("", "-"):
        return Decimal(0)
    try:
        return Decimal(raw.replace("$", "").replace(",", "").strip())
    except (ArithmeticError, ValueError) as exc:
        raise ParseError(f"Unparseable {descriptor} cell {raw!r}: {exc}") from exc


def format_money(amount: Decimal) -> str:
    """Render a Decimal as ``$1,234.56``."""
    return f"${amount:,.2f}"


def clean_label(raw: str | None) -> str:
    """Collapse a label cell's CR/LF + surrounding whitespace into one clean line."""
    return " ".join((raw or "").split())
