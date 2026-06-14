"""Tests for sentence-level extractive snippeting (text_snippets module)."""

from actalux.web.text_snippets import (
    best_sentence_index,
    extract_query_terms,
    extractive_snippet,
    mark_terms,
    split_for_highlight,
    split_sentences,
)


class TestExtractQueryTerms:
    def test_drops_stopwords_and_short_tokens(self) -> None:
        assert extract_query_terms("the budget for FY") == ["budget"]

    def test_dedupes_preserving_order(self) -> None:
        assert extract_query_terms("budget Budget spending budget") == ["budget", "spending"]

    def test_empty_query(self) -> None:
        assert extract_query_terms("") == []


class TestSplitSentences:
    def test_splits_on_terminal_punctuation(self) -> None:
        assert split_sentences("One thing. Two things! Three?") == [
            "One thing.",
            "Two things!",
            "Three?",
        ]

    def test_collapses_whitespace(self) -> None:
        assert split_sentences("  A\n\n  sentence here.  ") == ["A sentence here."]

    def test_empty(self) -> None:
        assert split_sentences("   ") == []


class TestBestSentenceIndex:
    def test_picks_densest_sentence_not_first_hit(self) -> None:
        sentences = [
            "The meeting was called to order at budget time.",
            "The board approved the budget and the budget levy together.",
            "Adjourned.",
        ]
        # Sentence 1 has one budget hit; sentence 1 (index 1) has two -> wins.
        assert best_sentence_index(sentences, ["budget"]) == 1

    def test_returns_negative_one_when_no_match(self) -> None:
        assert best_sentence_index(["Nothing relevant here."], ["budget"]) == -1


class TestMarkTerms:
    def test_wraps_matches_and_escapes(self) -> None:
        out = mark_terms("Budget <b> rises", ["budget"])
        assert "<mark>Budget</mark>" in out
        assert "&lt;b&gt;" in out  # angle brackets escaped, not rendered

    def test_no_terms_just_escapes(self) -> None:
        assert mark_terms("a < b", []) == "a &lt; b"


class TestExtractiveSnippet:
    def test_lands_on_relevant_sentence_not_boilerplate(self) -> None:
        content = (
            "Pledge of allegiance was recited. Roll was taken and a quorum present. "
            "The board approved the FY2024 operating budget of $58.3 million. "
            "The meeting adjourned at 9 p.m."
        )
        out = extractive_snippet(content, "operating budget", max_chars=120)
        assert "budget" in out.lower()
        assert "<mark>" in out
        # Boilerplate opener should not be what we surface.
        assert "Pledge of allegiance" not in out

    def test_fallback_head_truncates_when_no_match(self) -> None:
        out = extractive_snippet("A short note about nothing in particular here.", "budget")
        assert out.startswith("A short note")
        assert "<mark>" not in out

    def test_escapes_html(self) -> None:
        out = extractive_snippet("The <script> budget tag is here.", "budget")
        assert "<script>" not in out
        assert "&lt;script&gt;" in out

    def test_empty_content(self) -> None:
        assert extractive_snippet("", "budget") == ""


class TestSplitForHighlight:
    def test_isolates_key_sentence(self) -> None:
        content = "Opening remarks were made. The budget was approved. Then they left."
        before, key, after = split_for_highlight(content, "budget")
        assert key == "The budget was approved."
        assert before == "Opening remarks were made."
        assert after == "Then they left."

    def test_no_match_returns_whole_as_key(self) -> None:
        content = "Nothing relevant. Still nothing."
        before, key, after = split_for_highlight(content, "budget")
        assert before == ""
        assert key == "Nothing relevant. Still nothing."
        assert after == ""

    def test_empty_content(self) -> None:
        assert split_for_highlight("", "budget") == ("", "", "")
