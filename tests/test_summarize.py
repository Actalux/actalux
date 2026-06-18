"""Tests for citation-backed summary verification logic.

Tests the pure functions (citation extraction, verification, sentence splitting).
No LLM calls — the _call_llm function is not tested here.
"""

from types import SimpleNamespace
from unittest.mock import patch

from actalux.search.summarize import (
    Summary,
    _drain_complete_sentences,
    _split_sentences,
    _verify_citations,
    _verify_sentence,
    extract_citation_ids,
    generate_summary,
    generate_summary_stream,
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

    def test_long_hash(self) -> None:
        assert extract_citation_ids("[#q10000]") == ["#q10000"]

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


# --- Streaming (Phase B): per-sentence verified streaming -------------------

_STREAM_RESULTS = [
    {
        "hash_id": "#q0001",
        "content": "The board approved the budget.",
        "meeting_date": "2024-07-01",
        "meeting_title": "Budget",
        "section": "Approval",
    },
    {
        "hash_id": "#q0002",
        "content": "Revenue rose to $80M.",
        "meeting_date": "2024-07-01",
        "meeting_title": "Budget",
        "section": "Revenue",
    },
]


def _fake_stream(text: str, *, piece: int = 7):
    """Fake OpenAI streaming response: yield `text` in small token-like chunks."""
    for i in range(0, len(text), piece):
        chunk = text[i : i + piece]
        yield SimpleNamespace(choices=[SimpleNamespace(delta=SimpleNamespace(content=chunk))])


def _run_stream(text: str, results=_STREAM_RESULTS):
    with patch("actalux.search.summarize.OpenAI") as mock_openai:
        mock_openai.return_value.chat.completions.create.return_value = _fake_stream(text)
        return list(generate_summary_stream("budget", results, api_key="fake"))


class TestDrainCompleteSentences:
    def test_holds_trailing_partial_until_boundary(self) -> None:
        # No boundary yet (one segment) -> nothing complete, all buffered.
        assert _drain_complete_sentences("The board approved the budget.") == (
            [],
            "The board approved the budget.",
        )

    def test_emits_complete_keeps_partial(self) -> None:
        complete, partial = _drain_complete_sentences("First done. [#q0001] Second is still")
        assert complete == ["First done. [#q0001]"]
        assert partial == "Second is still"


class TestVerifySentence:
    def test_valid_citation_kept(self) -> None:
        kept, stats = _verify_sentence("The budget rose. [#q0001]", {"#q0001"})
        assert kept == "The budget rose. [#q0001]"
        assert stats == {"found": 1, "verified": 1, "dropped": 0}

    def test_invalid_citation_dropped(self) -> None:
        kept, stats = _verify_sentence("Made up claim. [#q9999]", {"#q0001"})
        assert kept is None
        assert stats == {"found": 1, "verified": 0, "dropped": 1}


class TestGenerateSummaryStream:
    def test_streams_verified_sentences_then_summary(self) -> None:
        text = "The board approved the budget. [#q0001] Revenue rose to $80M. [#q0002]"
        out = _run_stream(text)
        sentences = [o for o in out if isinstance(o, str)]
        summary = out[-1]
        assert isinstance(summary, Summary)
        # Both valid sentences were streamed, in order.
        assert sentences == [
            "The board approved the budget. [#q0001]",
            "Revenue rose to $80M. [#q0002]",
        ]
        # Final text matches the batch verifier on the same text (no drift).
        expected_text, _ = _verify_citations(text, {"#q0001", "#q0002"})
        assert summary.text == expected_text
        assert " ".join(sentences) == summary.text
        assert summary.citations_verified == 2

    def test_drops_sentence_with_invalid_citation_while_streaming(self) -> None:
        text = "The board approved the budget. [#q0001] Reserves tripled overnight. [#q9999]"
        out = _run_stream(text)
        sentences = [o for o in out if isinstance(o, str)]
        summary = out[-1]
        # The unverifiable claim never streams to the client.
        assert sentences == ["The board approved the budget. [#q0001]"]
        assert "Reserves tripled" not in summary.text
        assert summary.citations_dropped == 1

    def test_empty_results_yields_only_summary_no_llm_call(self) -> None:
        with patch("actalux.search.summarize.OpenAI") as mock_openai:
            out = list(generate_summary_stream("budget", [], api_key="fake"))
        mock_openai.assert_not_called()
        assert len(out) == 1
        assert isinstance(out[0], Summary)
        assert "No matching records" in out[0].text
