"""Tests for sentence-level extractive snippeting (text_snippets module)."""

from actalux.web.text_snippets import (
    TRANSCRIPT_CAPTION_LABEL,
    best_sentence_index,
    clean_text_light,
    content_paragraphs,
    dedup_rolling_captions,
    extract_query_terms,
    extractive_snippet,
    lead_sentence,
    mark_terms,
    reflow_transcript,
    split_for_highlight,
    split_sentences,
    strip_transcript_timestamps,
)


class TestContentParagraphs:
    """Full-document text is reflowed into readable paragraphs."""

    def test_collapses_single_newlines_within_a_block(self) -> None:
        # A transcript wrapped mid-sentence becomes one flowing paragraph.
        text = "welcome. I apologize that we are a few\nminutes late getting\nstarted."
        assert content_paragraphs(text) == [
            "welcome. I apologize that we are a few minutes late getting started."
        ]

    def test_splits_on_blank_lines(self) -> None:
        assert content_paragraphs("First para.\n\nSecond para.") == ["First para.", "Second para."]

    def test_empty_returns_empty_list(self) -> None:
        assert content_paragraphs("") == []
        assert content_paragraphs("   \n  \n") == []


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


# ── Transcript cleanup helpers ────────────────────────────────────────────────


class TestStripTranscriptTimestamps:
    """Standalone timestamp lines are dropped; embedded text is preserved."""

    def test_drops_bracket_mm_ss(self) -> None:
        text = "Some caption.\n[02:14]\nMore caption."
        result = strip_transcript_timestamps(text)
        assert "[02:14]" not in result
        assert "Some caption." in result
        assert "More caption." in result

    def test_drops_hh_mm_ss(self) -> None:
        text = "First line.\n0:12:34\nSecond line."
        result = strip_transcript_timestamps(text)
        assert "0:12:34" not in result
        assert "First line." in result
        assert "Second line." in result

    def test_drops_mm_ss_without_brackets(self) -> None:
        text = "Before.\n05:30\nAfter."
        result = strip_transcript_timestamps(text)
        assert "05:30" not in result
        assert "Before." in result
        assert "After." in result

    def test_preserves_time_embedded_in_sentence(self) -> None:
        # A timestamp within a sentence must NOT be stripped.
        text = "The meeting began at 7:30 and adjourned."
        result = strip_transcript_timestamps(text)
        assert "7:30" in result

    def test_empty_input(self) -> None:
        assert strip_transcript_timestamps("") == ""

    def test_no_timestamps(self) -> None:
        text = "Nothing to strip here."
        assert strip_transcript_timestamps(text) == text


class TestDedupRollingCaptions:
    """Rolling-caption prefix repetition is collapsed when the overlap is long enough."""

    def test_removes_duplicate_prefix_line(self) -> None:
        # Classic rolling-caption pattern: previous line reappears at start of next.
        # The prefix is 4 words, which meets the minimum-overlap threshold.
        text = "and the board approved\nand the board approved the budget"
        result = dedup_rolling_captions(text)
        # Only the longer (supersetting) line survives.
        assert "and the board approved the budget" in result
        # The shorter duplicate prefix is gone.
        assert result.count("and the board approved") == 1

    def test_short_prefix_not_collapsed_for_verbatim_safety(self) -> None:
        # "Thank you" → "Thank you for coming": a 2-word prefix — NOT collapsed
        # because the threshold guards against removing intentional repeated speech.
        text = "Thank you\nThank you for coming"
        result = dedup_rolling_captions(text)
        # Both lines survive (verbatim safety).
        assert "Thank you\n" in result
        assert "Thank you for coming" in result

    def test_non_overlapping_lines_both_kept(self) -> None:
        text = "The president called the meeting to order.\nThe secretary read the minutes."
        result = dedup_rolling_captions(text)
        assert "president" in result
        assert "secretary" in result

    def test_empty_lines_preserved(self) -> None:
        text = "First.\n\nSecond."
        result = dedup_rolling_captions(text)
        assert "\n\n" in result

    def test_empty_input(self) -> None:
        assert dedup_rolling_captions("") == ""

    def test_single_line_unchanged(self) -> None:
        text = "Just one line."
        assert dedup_rolling_captions(text) == text


class TestReflowTranscript:
    """End-to-end transcript reflow: timestamps stripped, paragraphed (no dedup in pipeline)."""

    def test_removes_timestamps_and_collapses_lines(self) -> None:
        text = "[00:05]\nwelcome. I apologize that we are a few\nminutes late getting\nstarted."
        result = reflow_transcript(text)
        # Timestamp gone, intra-block newlines collapsed.
        assert "[00:05]" not in " ".join(result)
        assert any("welcome" in p for p in result)
        # All words are joined into flowing text.
        assert any("minutes late getting started" in p for p in result)

    def test_splits_on_blank_lines(self) -> None:
        # Blocks must be long enough (>= _MIN_PARAGRAPH_WORDS=8) to not be merged.
        text = (
            "The board called the meeting to order at seven PM.\n\n"
            "The superintendent presented the annual report to the board."
        )
        result = reflow_transcript(text)
        assert len(result) == 2
        assert "called the meeting" in result[0]
        assert "superintendent" in result[1]

    def test_merges_short_trailing_fragment(self) -> None:
        # A very short block (fewer than _MIN_PARAGRAPH_WORDS) gets merged.
        text = (
            "The board discussed the curriculum and budget planning.\n\n"
            "Thank you."  # too short to stand alone
        )
        result = reflow_transcript(text)
        # Short fragment merged into the previous paragraph.
        assert len(result) == 1
        assert "Thank you." in result[0]

    def test_empty_input(self) -> None:
        assert reflow_transcript("") == []

    def test_verbatim_word_preservation(self) -> None:
        # Only whitespace is modified — every word must survive.
        text = "the board voted unanimously to approve the FY2025 operating budget"
        result = reflow_transcript(text)
        joined = " ".join(result)
        assert "unanimously" in joined
        assert "FY2025" in joined
        assert "operating budget" in joined

    def test_caption_label_constant_is_non_empty(self) -> None:
        # The label constant is used in the template; verify it's non-empty.
        assert TRANSCRIPT_CAPTION_LABEL
        assert "captions" in TRANSCRIPT_CAPTION_LABEL.lower()


class TestCleanTextLight:
    """Light whitespace normalizer for non-transcript chunk text."""

    def test_collapses_whitespace_runs(self) -> None:
        assert clean_text_light("hello   world") == "hello world"

    def test_collapses_newlines_to_space(self) -> None:
        # Does NOT split on blank lines (contrast with content_paragraphs).
        text = "line one\n\nline two"
        result = clean_text_light(text)
        assert result == "line one line two"

    def test_strips_leading_trailing(self) -> None:
        assert clean_text_light("  trim me  ") == "trim me"

    def test_empty_returns_empty(self) -> None:
        assert clean_text_light("") == ""
        assert clean_text_light("   ") == ""

    def test_words_unchanged(self) -> None:
        # Word characters are never modified — only whitespace.
        text = "FY2025 budget: $58.3 million (audited)"
        assert clean_text_light(text) == text


class TestLeadSentence:
    """One clean verbatim sentence for the topic citation lists."""

    def test_picks_query_relevant_sentence(self) -> None:
        content = (
            "The meeting opened at 7pm. The budget officer submits a proposed budget. Adjourned."
        )
        assert lead_sentence(content, "budget") == "The budget officer submits a proposed budget."

    def test_falls_back_to_first_sentence_when_no_term_matches(self) -> None:
        content = "First sentence here. Second sentence here."
        assert lead_sentence(content, "nonexistentterm") == "First sentence here."
        # No query at all -> first sentence.
        assert lead_sentence(content) == "First sentence here."

    def test_strips_leading_extraction_noise(self) -> None:
        # The "[]" / bullet artifacts seen at the head of PDF-extracted chunks.
        assert lead_sentence("[] Prior to July the officer submits.", "officer") == (
            "Prior to July the officer submits."
        )
        assert lead_sentence("• A bulleted budget line.", "budget") == "A bulleted budget line."

    def test_keeps_words_and_currency_verbatim(self) -> None:
        # Currency, digits, and words are never stripped — only layout glyphs.
        content = "The reserve fund holds $58.3 million as of FY2025."
        assert lead_sentence(content, "reserve") == content

    def test_truncates_overlong_sentence_at_word_boundary(self) -> None:
        long_sentence = "word " * 100 + "end."
        out = lead_sentence(long_sentence, "word", max_chars=40)
        assert out.endswith("…")
        assert len(out) <= 41
        assert " wor…" not in out  # no mid-word cut

    def test_empty_returns_empty(self) -> None:
        assert lead_sentence("") == ""
        assert lead_sentence("   ", "budget") == ""
