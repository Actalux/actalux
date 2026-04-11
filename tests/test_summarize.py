"""Tests for citation-backed summary verification logic.

Tests the pure functions (citation extraction, verification, sentence splitting).
No LLM calls — the _call_llm function is not tested here.
"""

from actalux.search.summarize import (
    Summary,
    _split_sentences,
    _verify_citations,
    extract_citation_ids,
    generate_summary,
)


class TestExtractCitationIds:
    """Extract hash IDs from text."""

    def test_single_citation(self) -> None:
        assert extract_citation_ids("The budget was approved. [#q003f]") == ["#q003f"]

    def test_multiple_citations(self) -> None:
        text = "Point A [#q003f] and point B [#q0042]."
        assert extract_citation_ids(text) == ["#q003f", "#q0042"]

    def test_no_citations(self) -> None:
        assert extract_citation_ids("No citations here.") == []

    def test_five_char_hash(self) -> None:
        assert extract_citation_ids("[#q1a2b3]") == ["#q1a2b3"]

    def test_hash_without_brackets(self) -> None:
        assert extract_citation_ids("See #q003f for details.") == ["#q003f"]


class TestSplitSentences:
    """Sentence splitting that preserves citation brackets."""

    def test_basic_split(self) -> None:
        text = "First sentence. Second sentence. Third sentence."
        sentences = _split_sentences(text)
        assert len(sentences) == 3

    def test_preserves_citations(self) -> None:
        text = "The budget passed [#q003f]. The vote was unanimous [#q0042]."
        sentences = _split_sentences(text)
        assert len(sentences) == 2
        assert "#q003f" in sentences[0]
        assert "#q0042" in sentences[1]

    def test_single_sentence(self) -> None:
        assert _split_sentences("Just one sentence.") == ["Just one sentence."]

    def test_empty_string(self) -> None:
        assert _split_sentences("") == []


class TestVerifyCitations:
    """Verify citations against a set of valid IDs."""

    def test_all_valid(self) -> None:
        text = "The budget was $10M. [#q003f] Taxes increased. [#q0042]"
        valid = {"#q003f", "#q0042"}
        result, stats = _verify_citations(text, valid)
        assert "#q003f" in result
        assert "#q0042" in result
        assert stats["verified"] == 2
        assert stats["dropped"] == 0

    def test_invalid_citation_dropped(self) -> None:
        text = "True claim. [#q003f] Fake claim. [#qffff]"
        valid = {"#q003f"}
        result, stats = _verify_citations(text, valid)
        assert "#q003f" in result
        assert "#qffff" not in result
        assert stats["verified"] == 1
        assert stats["dropped"] == 1

    def test_all_invalid_returns_fallback(self) -> None:
        text = "This is a longer bad claim that exceeds eight words easily [#qffff]."
        valid = {"#q003f"}
        result, stats = _verify_citations(text, valid)
        assert "Could not generate" in result
        assert stats["dropped"] == 1

    def test_short_uncited_sentences_kept(self) -> None:
        """Short transitional sentences without citations are kept."""
        text = "Here is what we found. The budget was $10M. [#q003f]"
        valid = {"#q003f"}
        result, stats = _verify_citations(text, valid)
        assert "Here is what we found" in result
        assert "#q003f" in result

    def test_empty_valid_set(self) -> None:
        text = "This is a claim about the budget. [#q003f]"
        _, stats = _verify_citations(text, set())
        assert stats["dropped"] == 1


class TestGenerateSummaryNoResults:
    """generate_summary with empty results (no LLM call)."""

    def test_empty_results(self) -> None:
        summary = generate_summary("budget", [], api_key="fake")
        assert isinstance(summary, Summary)
        assert "No matching records" in summary.text
        assert summary.citations_found == 0
