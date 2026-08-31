"""Tests for the LLM segmenter (actalux.landuse.segment_llm).

The LLM is canned; what these pin is the evidence gate — an item exists only
where its opening quote locates verbatim, and the located positions, not the
LLM's claims, define the spans.
"""

from __future__ import annotations

import json

import pytest

from actalux.landuse.extract import ExtractError
from actalux.landuse.segment_llm import llm_segment_items

BOA = """\
Chairman Bliss called the meeting to order at 5:00 pm.

1. Minutes from November 2, 2023 were presented for approval. Liza Streett made a
motion to approve the minutes with corrections.

1. An appeal from Sanford Talley, Applicant, on behalf of Dawn Kotva, Owner of
7451 Bland Avenue, for the following variance from the City of Clayton's
Zoning Regulations:

A 200 square-foot variance from the maximum living area for
an Accessory Dwelling Unit of 1,000 square-feet, Section
405.330.A.5 of Article XIII ("R-2") Single-Family Dwelling
District.

Rich Lintz made a motion to approve the variance. The motion carried unanimously.
"""


def _llm(payload: dict):
    return lambda system, user: json.dumps(payload)


class TestEvidenceGate:
    def test_located_opening_defines_the_span(self) -> None:
        items, rejected = llm_segment_items(
            BOA,
            _llm(
                {
                    "items": [
                        {
                            "opening_quote": "An appeal from Sanford Talley, Applicant,",
                            "address": "7451  Bland Avenue",
                            "type": "variance",
                            "subtype": None,
                        }
                    ]
                }
            ),
        )
        assert rejected == []
        (it,) = items
        assert it.application_type == "variance"
        assert it.address_raw == "7451 Bland Avenue"  # whitespace collapsed
        # The span is real document text, so downstream verbatim checks work.
        assert BOA[it.start : it.end].strip() == it.body
        assert "Section" in it.body and "motion carried unanimously" in it.body
        # And it starts at the located opening, not at the LLM's whim.
        assert it.body.startswith("An appeal from Sanford Talley")

    def test_unlocatable_opening_is_rejected_not_guessed(self) -> None:
        items, rejected = llm_segment_items(
            BOA,
            _llm(
                {
                    "items": [
                        {
                            "opening_quote": "A request by a totally invented applicant",
                            "address": "1 Fake St",
                            "type": "variance",
                            "subtype": None,
                        }
                    ]
                }
            ),
        )
        assert items == []
        assert len(rejected) == 1

    def test_quote_wrapped_across_lines_still_locates(self) -> None:
        # The opening crosses the PDF line wrap ("Owner of\n7451 Bland Avenue").
        items, _ = llm_segment_items(
            BOA,
            _llm(
                {
                    "items": [
                        {
                            "opening_quote": "on behalf of Dawn Kotva, Owner of 7451 Bland Avenue",
                            "address": "7451 Bland Avenue",
                            "type": "variance",
                            "subtype": None,
                        }
                    ]
                }
            ),
        )
        assert len(items) == 1

    def test_out_of_order_items_are_relocated_and_sorted(self) -> None:
        items, rejected = llm_segment_items(
            BOA,
            _llm(
                {
                    "items": [
                        {
                            "opening_quote": "An appeal from Sanford Talley",
                            "address": "7451 Bland Avenue",
                            "type": "variance",
                            "subtype": None,
                        },
                        {
                            "opening_quote": "Minutes from November 2, 2023 were presented",
                            "address": None,
                            "type": None,
                            "subtype": None,
                        },
                    ]
                }
            ),
        )
        assert rejected == []
        assert [it.body[:10] for it in items] == ["Minutes fr", "An appeal "]

    def test_garbage_reply_raises(self) -> None:
        with pytest.raises(ExtractError):
            llm_segment_items(BOA, lambda s, u: "no items here")
        with pytest.raises(ExtractError):
            llm_segment_items(BOA, lambda s, u: json.dumps({"items": "nope"}))


def test_duplicate_openings_yield_one_item_not_an_empty_span() -> None:
    # Two identical openings locate at the same position; the zero-width span
    # must be rejected, because an event's source_quote is NOT NULL by CHECK and
    # an empty body would die at insert — as the first full backfill proved.
    dup = {
        "opening_quote": "An appeal from Sanford Talley, Applicant,",
        "address": "7451 Bland Avenue",
        "type": "variance",
        "subtype": None,
    }
    items, rejected = llm_segment_items(BOA, _llm({"items": [dup, dict(dup)]}))
    assert len(items) == 1
    assert len(rejected) == 1
    assert all(it.body for it in items)
