"""Tests for the land-use narrative extractor (actalux.landuse.extract).

The LLM is a canned callable, so what these pin is the verification gate: a
quote that is not verbatim in the item body kills that field and only that
field, and vocabulary violations die the same way.
"""

from __future__ import annotations

import json

import pytest

from actalux.landuse.extract import ExtractError, extract_item, quote_in

BODY = """\
1. 42 North Central Avenue – Conditional Use Permit – Restaurant Use (00:01:10)
Consideration of a request by Lauren Talley, Applicant, on behalf of Tre Cuori Gelateria,
Tenant, for a conditional use permit to operate a gelateria.

Ryan Helle provided a summary of the staff report with a recommendation for the Planning
Commission to recommend approval of the conditional use permit to the City Council as
submitted. Helen DiFate made a motion to recommend approval of the CUP to the city council
as submitted. The motion carried unanimously.
"""


def _llm_returning(payload: dict):
    return lambda system, user: json.dumps(payload)


GOOD = {
    "action": "recommended",
    "action_quote": "made a motion to recommend approval of the CUP to the city council",
    "staff_recommendation": "approve",
    "staff_quote": "a recommendation for the Planning\nCommission to recommend approval",
    "conditions": [],
    "parties": [
        {"role": "applicant", "name": "Lauren Talley", "quote": "Lauren Talley, Applicant"},
        {"role": "tenant", "name": "Tre Cuori Gelateria", "quote": "Tre Cuori Gelateria,\nTenant"},
    ],
}


class TestVerification:
    def test_supported_fields_pass(self) -> None:
        out = extract_item(BODY, _llm_returning(GOOD))
        assert out.action == "recommended"
        assert out.staff_recommendation == "approve"
        assert {p.name_raw for p in out.parties} == {"Lauren Talley", "Tre Cuori Gelateria"}
        assert out.rejected == ()

    def test_quotes_survive_whitespace_mangling_only(self) -> None:
        # PDF text layers break lines mid-phrase; the quote still verifies.
        assert quote_in("Tre Cuori Gelateria, Tenant", BODY)
        # But a reworded "quote" does not.
        assert not quote_in("Tre Cuori Gelateria (the tenant)", BODY)

    def test_fabricated_quote_kills_only_that_field(self) -> None:
        bad = dict(GOOD, staff_quote="staff recommended denial of the permit")
        out = extract_item(BODY, _llm_returning(bad))
        assert out.staff_recommendation is None
        assert "staff_recommendation" in out.rejected
        assert out.action == "recommended"  # the well-supported field survives

    def test_vocabulary_violation_rejected(self) -> None:
        bad = dict(GOOD, action="tabled", action_quote=GOOD["action_quote"])
        out = extract_item(BODY, _llm_returning(bad))
        assert out.action is None
        assert "action" in out.rejected

    def test_none_stated_needs_no_quote(self) -> None:
        payload = dict(GOOD, staff_recommendation="none_stated", staff_quote=None)
        out = extract_item(BODY, _llm_returning(payload))
        assert out.staff_recommendation == "none_stated"
        assert out.staff_quote is None

    def test_fabricated_condition_dropped_and_counted(self) -> None:
        payload = dict(GOOD, conditions=["The applicant shall repaint the facade mauve."])
        out = extract_item(BODY, _llm_returning(payload))
        assert out.conditions_text is None
        assert any(r.startswith("conditions[") for r in out.rejected)

    def test_party_with_bad_role_or_quote_rejected(self) -> None:
        payload = dict(
            GOOD,
            parties=[{"role": "developer", "name": "X", "quote": "Lauren Talley, Applicant"}],
        )
        out = extract_item(BODY, _llm_returning(payload))
        assert out.parties == ()
        assert "party" in out.rejected

    def test_null_fields_are_absent_not_rejected(self) -> None:
        payload = {"action": None, "staff_recommendation": None, "conditions": [], "parties": []}
        out = extract_item(BODY, _llm_returning(payload))
        assert out.action is None and out.rejected == ()


class TestParsing:
    def test_fenced_json_tolerated(self) -> None:
        llm = lambda s, u: "```json\n" + json.dumps(GOOD) + "\n```"  # noqa: E731
        assert extract_item(BODY, llm).action == "recommended"

    def test_garbage_raises_extract_error(self) -> None:
        with pytest.raises(ExtractError):
            extract_item(BODY, lambda s, u: "I could not find any items.")

    def test_json_array_raises(self) -> None:
        with pytest.raises(ExtractError):
            extract_item(BODY, lambda s, u: "[]")
