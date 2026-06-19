"""Tests for the district-communications crawler's parsing (no network).

Exercises post parsing, body isolation (excluding nav/footer), date extraction,
and the minimal-HTML rendering against synthetic post HTML.
"""

from __future__ import annotations

from types import SimpleNamespace

import scripts.crawl_comms as comms

_POST_HTML = """
<html><head>
  <meta property="page-published" content="2020-01-01T00:00:00Z">
  <meta property="article:published" content="2026-06-04T18:14:00Z">
</head><body>
  <nav><p>Skip to main content</p></nav>
  <h1>Board Approves Balanced Budget for 2026-27 Fiscal Year</h1>
  <article><div class="fsElementContent">
    <p>At its June 3 meeting, the Board approved a balanced budget for 2026-27.</p>
    <p>The plan maintains the district's AAA bond rating.</p>
  </div></article>
  <footer><p>Copyright District of Clayton</p></footer>
</body></html>
"""


def _resp(text: str) -> SimpleNamespace:
    return SimpleNamespace(text=text)


class TestParsePost:
    def test_extracts_title_date_body(self) -> None:
        parsed = comms.parse_post(_resp(_POST_HTML))
        assert parsed is not None
        title, date, paragraphs = parsed
        assert title == "Board Approves Balanced Budget for 2026-27 Fiscal Year"
        # the authoritative article:published date wins over page-published housekeeping
        assert date == "2026-06-04"
        assert len(paragraphs) == 2
        assert paragraphs[0].startswith("At its June 3 meeting")

    def test_body_excludes_nav_and_footer(self) -> None:
        _, _, paragraphs = comms.parse_post(_resp(_POST_HTML))
        joined = " ".join(paragraphs)
        assert "Skip to main content" not in joined
        assert "Copyright" not in joined

    def test_missing_date_returns_none(self) -> None:
        html_no_date = _POST_HTML.replace(
            '<meta property="article:published" content="2026-06-04T18:14:00Z">', ""
        )
        assert comms.parse_post(_resp(html_no_date)) is None

    def test_missing_body_returns_none(self) -> None:
        html_no_body = "<html><body><h1>Title Only</h1></body></html>"
        assert comms.parse_post(_resp(html_no_body)) is None


class TestHelpers:
    def test_slug_of(self) -> None:
        url = (
            "https://www.claytonschools.net/post-details/~board/district-news/post/balanced-budget"
        )
        assert comms.slug_of(url) == "balanced-budget"
        assert comms.slug_of(url + "/") == "balanced-budget"

    def test_render_html_escapes_and_wraps(self) -> None:
        out = comms.render_html("Title <X>", ["Para with <script>alert(1)</script>", "Second."])
        assert "<h1>Title &lt;X&gt;</h1>" in out
        assert "<p>Para with &lt;script&gt;alert(1)&lt;/script&gt;</p>" in out
        assert "<p>Second.</p>" in out
        # no raw injected markup survived
        assert "<script>" not in out
