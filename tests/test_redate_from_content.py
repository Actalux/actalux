"""Tests for scripts/redate_from_content.py.

The script re-dates documents from a verbatim "delivered on" string in their own
content, writing a date only when that anchor is present. These tests exercise the
three planning branches (apply / already / refused) with a fake Supabase client.
"""

from types import SimpleNamespace

import scripts.redate_from_content as mod


class _FakeQuery:
    """Minimal stand-in for the supabase query chain used by plan()."""

    def __init__(self, rows: list[dict]):
        self._rows = rows
        self._eq: tuple[str, object] | None = None

    def select(self, *_a, **_k):
        return self

    def eq(self, col, val):
        self._eq = (col, val)
        return self

    def is_(self, _col, _val):
        # plan() filters replaces_id IS NULL; the test rows are all live.
        return self

    def execute(self):
        col, val = self._eq
        return SimpleNamespace(data=[r for r in self._rows if r.get(col) == val])


class _FakeClient:
    def __init__(self, rows: list[dict]):
        self._rows = rows

    def table(self, _name):
        return _FakeQuery(self._rows)


class TestContentDatesTable:
    def test_specs_are_well_formed(self) -> None:
        for spec in mod.CONTENT_DATES:
            assert spec["anchor"], "empty anchor"
            assert len(spec["date"]) == 10 and spec["date"][4] == "-"
            assert isinstance(spec["doc_id"], int)


def _satisfied_row(spec: dict) -> dict:
    """A live row that lands `spec` in `already` (anchor present, target date and
    date_source='content' both set correctly)."""
    return {
        "id": spec["doc_id"],
        "source_file": "x.pdf",
        "meeting_date": spec["date"],
        "date_source": "content",
        "content": spec["anchor"],
    }


def _rows_for_all_specs() -> list[dict]:
    """One satisfied row per configured spec, so plan() finds every doc."""
    return [_satisfied_row(s) for s in mod.CONTENT_DATES]


class TestPlan:
    def test_redates_when_anchor_present_and_date_differs(self) -> None:
        spec = mod.CONTENT_DATES[0]
        rows = _rows_for_all_specs()
        # Flip the doc under test: wrong stored date, anchor amid newlines/spacing.
        rows[0]["meeting_date"] = "2026-04-11"
        rows[0]["content"] = f"cover\n{spec['anchor'].replace(' ', '  ')}\nbody"
        to_apply, already, refused = mod.plan(_FakeClient(rows))
        assert [c["doc_id"] for c in to_apply] == [spec["doc_id"]]
        assert not refused

    def test_skips_when_already_on_target_date(self) -> None:
        to_apply, already, refused = mod.plan(_FakeClient(_rows_for_all_specs()))
        assert not to_apply and not refused
        assert {c["doc_id"] for c in already} == {s["doc_id"] for s in mod.CONTENT_DATES}

    def test_refuses_when_anchor_absent(self) -> None:
        spec = mod.CONTENT_DATES[0]
        rows = _rows_for_all_specs()
        rows[0]["meeting_date"] = "2026-04-11"
        rows[0]["content"] = "no date here at all"
        to_apply, already, refused = mod.plan(_FakeClient(rows))
        assert spec["doc_id"] not in [c["doc_id"] for c in to_apply]
        assert any(
            c["doc_id"] == spec["doc_id"] and c["reason"] == "anchor string not present in content"
            for c in refused
        )

    def test_refuses_when_document_missing(self) -> None:
        # Empty DB -> every spec refused as "not found", nothing written.
        to_apply, already, refused = mod.plan(_FakeClient([]))
        assert not to_apply and not already
        assert len(refused) == len(mod.CONTENT_DATES)
        assert all("not found" in c["reason"] for c in refused)

    def test_updates_provenance_when_date_correct_but_date_source_stale(self) -> None:
        # A doc already on the target date but with date_source='default' (or any
        # value other than 'content') must land in to_apply for a provenance-only
        # write.  Without this, running --apply on previously-redated docs would
        # leave date_source stuck at 'unknown'/'default'.
        spec = mod.CONTENT_DATES[0]
        rows = _rows_for_all_specs()
        # Correct date, wrong provenance.
        rows[0]["date_source"] = "default"
        to_apply, already, refused = mod.plan(_FakeClient(rows))
        assert any(c["doc_id"] == spec["doc_id"] for c in to_apply), (
            "stale provenance should queue a provenance-only write"
        )
        assert not any(c["doc_id"] == spec["doc_id"] for c in already)
