"""Tests for scripts/fix_portal_renames.py.

The script reconciles portal filename changes by renaming an existing row's
source_file ONLY when the new-named file on disk is content-identical to the
stored row. These tests exercise the three planning branches (apply / already
applied / refused) with a fake Supabase client and a stubbed parse+hash path.
"""

from types import SimpleNamespace

import scripts.fix_portal_renames as mod


class _FakeQuery:
    """Minimal stand-in for the supabase query chain used by plan_renames."""

    def __init__(self, rows: list[dict]):
        self._rows = rows
        self._eq: tuple[str, object] | None = None

    def select(self, *_a, **_k):
        return self

    def eq(self, col, val):
        self._eq = (col, val)
        return self

    def is_(self, col, val):
        # plan_renames filters replaces_id IS NULL; the test rows are all live.
        return self

    def execute(self):
        col, val = self._eq
        return SimpleNamespace(data=[r for r in self._rows if r.get(col) == val])


class _FakeClient:
    def __init__(self, rows: list[dict]):
        self._rows = rows

    def table(self, _name):
        return _FakeQuery(self._rows)


def _stub_hash(monkeypatch, tmp_path, disk_hash: str) -> None:
    """Point DOCS_DIR at tmp_path and make parse+hash return a fixed disk hash."""
    monkeypatch.setattr(mod, "DOCS_DIR", tmp_path)
    monkeypatch.setattr(mod, "parse_file", lambda _p: "PARSED")
    monkeypatch.setattr(mod, "content_hash", lambda _t: disk_hash)


class TestRenamesTable:
    def test_old_and_new_differ_and_new_values_unique(self) -> None:
        for old, new in mod.RENAMES.items():
            assert old != new, f"no-op rename for {old!r}"
        news = list(mod.RENAMES.values())
        assert len(news) == len(set(news)), "two olds map to the same new name"


class TestPlanRenames:
    def test_applies_when_content_matches(self, monkeypatch, tmp_path) -> None:
        monkeypatch.setattr(mod, "RENAMES", {"old.pdf": "new.pdf"})
        _stub_hash(monkeypatch, tmp_path, "HASH1")
        (tmp_path / "new.pdf").write_text("x")
        client = _FakeClient(
            [{"id": 9, "source_file": "old.pdf", "content_hash": "HASH1", "replaces_id": None}]
        )

        to_apply, already, refused = mod.plan_renames(client)

        assert to_apply == [{"id": 9, "old": "old.pdf", "new": "new.pdf"}]
        assert already == []
        assert refused == []

    def test_already_applied_when_old_name_absent(self, monkeypatch, tmp_path) -> None:
        monkeypatch.setattr(mod, "RENAMES", {"old.pdf": "new.pdf"})
        _stub_hash(monkeypatch, tmp_path, "HASH1")
        (tmp_path / "new.pdf").write_text("x")
        client = _FakeClient([])  # no row carries the old name

        to_apply, already, refused = mod.plan_renames(client)

        assert to_apply == []
        assert already == ["old.pdf"]
        assert refused == []

    def test_refuses_when_content_differs(self, monkeypatch, tmp_path) -> None:
        # Same date/filename change but DIFFERENT content -> a new version, not a
        # rename; the script must refuse rather than silently relabel.
        monkeypatch.setattr(mod, "RENAMES", {"old.pdf": "new.pdf"})
        _stub_hash(monkeypatch, tmp_path, "DISKHASH")
        (tmp_path / "new.pdf").write_text("x")
        client = _FakeClient(
            [{"id": 9, "source_file": "old.pdf", "content_hash": "STORED", "replaces_id": None}]
        )

        to_apply, already, refused = mod.plan_renames(client)

        assert to_apply == []
        assert len(refused) == 1 and "content differs" in refused[0]

    def test_refuses_when_new_file_missing(self, monkeypatch, tmp_path) -> None:
        monkeypatch.setattr(mod, "RENAMES", {"old.pdf": "new.pdf"})
        _stub_hash(monkeypatch, tmp_path, "HASH1")
        # new.pdf intentionally not created on disk
        client = _FakeClient(
            [{"id": 9, "source_file": "old.pdf", "content_hash": "HASH1", "replaces_id": None}]
        )

        to_apply, already, refused = mod.plan_renames(client)

        assert to_apply == []
        assert len(refused) == 1 and "not on disk" in refused[0]
