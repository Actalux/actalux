"""Tests for the document parser."""

from pathlib import Path
from textwrap import dedent

import pytest

from actalux.errors import ParseError
from actalux.ingest.parser import parse_file


@pytest.fixture
def tmp_dir(tmp_path: Path) -> Path:
    return tmp_path


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
