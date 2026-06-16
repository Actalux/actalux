"""Tests for the document parser."""

from pathlib import Path
from textwrap import dedent

import pytest

from actalux.errors import ParseError
from actalux.ingest.parser import parse_file, strip_control_chars


@pytest.fixture
def tmp_dir(tmp_path: Path) -> Path:
    return tmp_path


class TestStripControlChars:
    def test_replaces_control_chars_with_space(self) -> None:
        # The 0x08 / 0x01 artifacts seen in extracted PDFs become spaces.
        assert strip_control_chars("Planning\x08\n3") == "Planning \n3"
        assert strip_control_chars("COST\x01\x14ESTIMATE") == "COST  ESTIMATE"

    def test_keeps_tab_newline_carriage_return(self) -> None:
        assert strip_control_chars("a\tb\nc\rd") == "a\tb\nc\rd"

    def test_strips_c1_controls(self) -> None:
        # C1 bytes (0x80-0x9f) from broken fonts are artifacts, not punctuation.
        assert strip_control_chars("bullet\x82 item") == "bullet  item"

    def test_clean_text_unchanged(self) -> None:
        text = "Ordinary minutes — approved 5-0. The “levy” passed."
        assert strip_control_chars(text) == text

    def test_applied_during_parse(self, tmp_dir: Path) -> None:
        md = tmp_dir / "dirty.md"
        md.write_bytes(b"# Heading\x08\n\nBody\x01text here.")
        result = parse_file(md)
        assert "\x08" not in result and "\x01" not in result
        assert "Body text here." in result


class TestParseMarkdown:
    def test_basic_markdown(self, tmp_dir: Path) -> None:
        md = tmp_dir / "minutes.md"
        md.write_text("# Board Meeting\n\nThe meeting was called to order.")
        result = parse_file(md)
        assert "Board Meeting" in result
        assert "called to order" in result

    def test_empty_file_raises(self, tmp_dir: Path) -> None:
        md = tmp_dir / "empty.md"
        md.write_text("")
        with pytest.raises(ParseError, match="empty"):
            parse_file(md)

    def test_whitespace_only_raises(self, tmp_dir: Path) -> None:
        md = tmp_dir / "blank.md"
        md.write_text("   \n\n   ")
        with pytest.raises(ParseError, match="empty"):
            parse_file(md)


class TestParseHtml:
    def test_basic_html(self, tmp_dir: Path) -> None:
        html = tmp_dir / "agenda.html"
        html.write_text(
            dedent("""\
            <html><body>
            <h1>Board Meeting Agenda</h1>
            <p>1. Call to Order</p>
            <p>2. Budget Discussion</p>
            </body></html>
        """)
        )
        result = parse_file(html)
        assert "Board Meeting Agenda" in result
        assert "Budget Discussion" in result

    def test_strips_script_and_style(self, tmp_dir: Path) -> None:
        html = tmp_dir / "messy.html"
        html.write_text(
            dedent("""\
            <html><body>
            <script>alert('xss')</script>
            <style>body { color: red; }</style>
            <p>Real content here.</p>
            </body></html>
        """)
        )
        result = parse_file(html)
        assert "alert" not in result
        assert "color: red" not in result
        assert "Real content here" in result


class TestParseText:
    def test_plain_text(self, tmp_dir: Path) -> None:
        txt = tmp_dir / "notes.txt"
        txt.write_text("Board discussed the proposed tax levy increase.")
        result = parse_file(txt)
        assert "tax levy" in result


class TestUnsupportedFormat:
    def test_unsupported_extension(self, tmp_dir: Path) -> None:
        doc = tmp_dir / "spreadsheet.xlsx"
        doc.write_bytes(b"not a real xlsx")
        with pytest.raises(ParseError, match="Unsupported"):
            parse_file(doc)
