"""Shared money-cell parsing for the Missouri DESE XML financial reports.

Local Effort, Indirect Cost, and Per-Pupil cells all record a dollar amount the
same way: blank or a bare dash means zero, and a value may carry a leading
``$`` and thousands commas. ASBR's report additionally carries parenthetical
negatives and a bare ``$-`` zero marker that these three reports never use, so
it keeps its own parser (``actalux.ingest.asbr_xml.parse_money``).
"""

from __future__ import annotations

from decimal import Decimal

from actalux.errors import ParseError


def parse_money_cell(raw: str | None, *, context: str) -> Decimal:
    """Parse a ``$``/comma-formatted or bare money cell into a ``Decimal``.

    ``context`` names the report/cell kind for the raised ``ParseError`` message
    (e.g. ``"Local Effort cell"``).
    """
    if raw is None or raw.strip() in ("", "-"):
        return Decimal(0)
    try:
        return Decimal(raw.replace("$", "").replace(",", "").strip())
    except (ArithmeticError, ValueError) as exc:
        raise ParseError(f"Unparseable {context} {raw!r}: {exc}") from exc
