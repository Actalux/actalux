"""Parse official documents (PDF, HTML, Markdown) into plain text.

Supported formats:
- PDF (agendas, minutes, board packets) via PyMuPDF
- HTML (web-scraped meeting pages) via BeautifulSoup
- Markdown (existing transcripts from founder's pipeline)
"""

from __future__ import annotations

import logging
from pathlib import Path

from actalux.errors import ParseError

logger = logging.getLogger(__name__)


def parse_file(path: Path) -> str:
    """Dispatch to the right parser based on file extension.

    Returns extracted text content. Raises ParseError on failure.
    """
    suffix = path.suffix.lower()
    parsers = {
        ".pdf": parse_pdf,
        ".html": parse_html,
        ".htm": parse_html,
        ".md": parse_markdown,
        ".markdown": parse_markdown,
        ".txt": parse_text,
    }
    parser_fn = parsers.get(suffix)
    if parser_fn is None:
        raise ParseError(f"Unsupported file format: {suffix} ({path.name})")

    try:
        text = parser_fn(path)
    except ParseError:
        raise
    except Exception as exc:
        raise ParseError(f"Failed to parse {path.name}: {exc}") from exc

    text = text.strip()
    if not text:
        raise ParseError(f"Document is empty after parsing: {path.name}")

    logger.info("Parsed %s: %d characters", path.name, len(text))
    return text


def parse_pdf(path: Path) -> str:
    """Extract text from a PDF using PyMuPDF."""
    import fitz  # pymupdf

    try:
        doc = fitz.open(str(path))
    except Exception as exc:
        raise ParseError(f"Cannot open PDF {path.name}: {exc}") from exc

    pages: list[str] = []
    for page_num, page in enumerate(doc):
        text = page.get_text("text")
        if text.strip():
            pages.append(text)
        else:
            logger.debug("Page %d of %s has no extractable text", page_num + 1, path.name)

    doc.close()

    if not pages:
        raise ParseError(f"PDF has no extractable text (images only?): {path.name}")

    return "\n\n".join(pages)


def parse_html(path: Path) -> str:
    """Extract text from an HTML file using BeautifulSoup."""
    from bs4 import BeautifulSoup

    raw = path.read_text(encoding="utf-8", errors="replace")

    soup = BeautifulSoup(raw, "lxml")

    # Remove script and style elements
    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.decompose()

    text = soup.get_text(separator="\n", strip=True)
    return text


def parse_markdown(path: Path) -> str:
    """Read a markdown file as plain text (preserving structure)."""
    return path.read_text(encoding="utf-8", errors="replace")


def parse_text(path: Path) -> str:
    """Read a plain text file."""
    return path.read_text(encoding="utf-8", errors="replace")
