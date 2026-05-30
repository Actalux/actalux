"""Tests for the ingest-time PII guard.

The guard must catch clear-cut PII tokens (SSN, dated DOB) while NOT
false-positiving on public legal/policy text about private topics -- the exact
failure mode that made a retrieval-time integrity probe unworkable -- nor on
the finance numbers that fill this corpus.
"""

from __future__ import annotations

from actalux.ingest import pii_guard


def test_catches_ssn() -> None:
    findings = pii_guard.scan_text("Employee record: SSN 123-45-6789 on file.")
    assert [f.pattern_name for f in findings] == ["ssn"]


def test_catches_dated_dob() -> None:
    findings = pii_guard.scan_text("Student DOB: 03/15/2010, grade 4.")
    assert any(f.pattern_name == "date_of_birth" for f in findings)


def test_value_is_masked_never_in_the_clear() -> None:
    raw = "123-45-6789"
    findings = pii_guard.scan_text(f"SSN {raw}")
    assert len(findings) == 1
    masked = findings[0].masked
    assert masked == "XXX-XX-XXXX"
    assert raw not in masked
    assert not any(ch.isdigit() for ch in masked)


def test_public_sunshine_law_text_does_not_trip() -> None:
    # Verbatim-style public legal text that the retrieval probe wrongly flagged.
    text = (
        "(5) Nonjudicial mental or physical health proceedings involving "
        "identifiable persons, including medical, psychiatric, psychological, "
        "or alcoholism or drug dependency matters. Any vote on a final decision "
        "to hire, fire, promote or discipline an employee shall be public."
    )
    assert pii_guard.scan_text(text) == []


def test_policy_mentioning_date_of_birth_without_a_date_does_not_trip() -> None:
    text = "Families must provide each student's date of birth at enrollment."
    assert pii_guard.scan_text(text) == []


def test_finance_and_phone_numbers_do_not_trip_ssn() -> None:
    # Phone (3-3-4), an accounting code (2-4-4), and a dollar figure.
    text = "Call 314-935-9000. Account 10-1111-6311 spent $1,234,567 this year."
    assert pii_guard.scan_text(text) == []


def test_should_block_only_in_block_mode() -> None:
    findings = pii_guard.scan_text("SSN 123-45-6789")
    assert pii_guard.should_block(findings, "block") is True
    assert pii_guard.should_block(findings, "warn") is False
    assert pii_guard.should_block(findings, "off") is False
    assert pii_guard.should_block([], "block") is False


def test_summarize_is_value_free() -> None:
    findings = pii_guard.scan_text("SSN 123-45-6789 and DOB: 1/2/2003")
    summary = pii_guard.summarize(findings)
    assert "ssn" in summary
    assert "123" not in summary  # no raw values leak into the summary
