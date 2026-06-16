"""Parse official documents (PDF, HTML, Markdown) into plain text.

Supported formats:
- PDF (agendas, minutes, board packets) via PyMuPDF
- HTML (web-scraped meeting pages) via BeautifulSoup
- Markdown (existing transcripts from founder's pipeline)
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from actalux.errors import ParseError

logger = logging.getLogger(__name__)

# C0/C1 control characters except tab, newline, and carriage return. PDF text
# extraction sometimes emits these (stray 0x08, 0x01, file-separator runs, and
# C1 bytes from broken fonts); they are never document text. Replaced with a
# space rather than removed so adjacent words can't fuse.
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")

# A page whose extracted text is mostly non-Latin codepoints is almost certainly
# broken-font mojibake (the glyphs map to the wrong Unicode); OCR recovers it.
_PAGE_GARBLE_THRESHOLD = 0.05
_OCR_DPI = 300
_OCR_LANG = "eng"


def strip_control_chars(text: str) -> str:
    """Replace control-character extraction artifacts with spaces (keeps \\t\\n\\r)."""
    return _CONTROL_CHARS_RE.sub(" ", text)


def exotic_char_ratio(text: str) -> float:
    """Fraction of characters in non-Latin scripts — a broken-font mojibake signal.

    Counts codepoints at or above U+0250 (past Latin Extended-A/B), excluding the
    General Punctuation block (smart quotes, dashes) which is legitimate.
    """
    if not text:
        return 0.0
    exotic = sum(1 for ch in text if ord(ch) >= 0x250 and not (0x2000 <= ord(ch) <= 0x206F))
    return exotic / len(text)


def _ocr_page(page, dpi: int = _OCR_DPI, lang: str = _OCR_LANG) -> str:
    """OCR one PDF page via Tesseract; '' if Tesseract is unavailable or fails.

    Degrading to '' (rather than raising) means a host without an OCR toolchain
    keeps the native text instead of crashing the whole ingest.
    """
    if shutil.which("tesseract") is None:
        logger.warning("tesseract not found on PATH; cannot OCR garbled page")
        return ""
    pix = page.get_pixmap(dpi=dpi)
    tmp = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as fh:
            fh.write(pix.tobytes("png"))
            tmp = fh.name
        proc = subprocess.run(
            ["tesseract", tmp, "stdout", "-l", lang],
            capture_output=True,
            timeout=180,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("OCR failed: %s", exc)
        return ""
    finally:
        if tmp and os.path.exists(tmp):
            os.unlink(tmp)
    if proc.returncode != 0:
        logger.warning(
            "tesseract exited %d: %s", proc.returncode, proc.stderr.decode("utf-8", "replace")[:200]
        )
        return ""
    return proc.stdout.decode("utf-8", "replace")


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

    text = strip_control_chars(text).strip()
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
        # Broken-font pages extract as mojibake; OCR recovers the real text.
        if text.strip() and exotic_char_ratio(strip_control_chars(text)) > _PAGE_GARBLE_THRESHOLD:
            ocr_text = _ocr_page(page)
            if ocr_text.strip():
                logger.info("OCR-recovered garbled page %d of %s", page_num + 1, path.name)
                text = ocr_text
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
