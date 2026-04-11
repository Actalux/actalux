"""Convert documents to structured markdown using pymupdf4llm.

The PDF is retained as the original source. This module converts it
to markdown for chunking, display, and human review.

Uses pymupdf4llm (thin PyMuPDF extension) for PDF → markdown.
No ML models, no GPU. Handles tables, headings, bold/italic natively.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)


def pdf_to_markdown(
    pdf_path: Path,
    meeting_title: str = "",
    meeting_date: date | None = None,
) -> str:
    """Convert a PDF to markdown using pymupdf4llm.

    Adds YAML frontmatter with meeting metadata.
    Returns the full markdown string.
    """
    import pymupdf4llm

    md_body = pymupdf4llm.to_markdown(str(pdf_path))

    # Prepend frontmatter
    frontmatter_lines: list[str] = ["---"]
    if meeting_date:
        frontmatter_lines.append(f"meeting_date: {meeting_date.isoformat()}")
    if meeting_title:
        frontmatter_lines.append(f"meeting_title: {meeting_title}")
    frontmatter_lines.append(f"source_file: {pdf_path.name}")
    frontmatter_lines.append("---")
    frontmatter_lines.append("")

    return "\n".join(frontmatter_lines) + md_body


def save_markdown(
    markdown: str,
    output_dir: Path,
    filename: str,
) -> Path:
    """Save markdown to a file. Returns the path."""
    output_dir.mkdir(parents=True, exist_ok=True)
    md_filename = Path(filename).stem + ".md"
    output_path = output_dir / md_filename
    output_path.write_text(markdown, encoding="utf-8")
    logger.info("Saved markdown: %s (%d chars)", output_path, len(markdown))
    return output_path
