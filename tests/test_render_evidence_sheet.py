"""Unit tests for the evidence-sheet section classifier (pure; no DB)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_path = Path(__file__).resolve().parent.parent / "scripts" / "render_evidence_sheet.py"
_spec = importlib.util.spec_from_file_location("render_evidence_sheet", _path)
res = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = res
_spec.loader.exec_module(res)


def _resolve(moved_by: str) -> str | None:
    return {"Alderman Feder": "Gary Feder", "Councilmember Buse": "Susan Buse"}.get(moved_by)


def _row(**kw):
    base = {
        "node_id": "solo1",
        "n_meetings": 1,
        "total_seconds": 120.0,
        "members": [{"document_id": 1, "cluster_label": "SPEAKER_01", "speech_seconds": 120.0}],
        "gallery": None,
        "minutes_movers": [],
        "minutes_evidence": [],
    }
    base.update(kw)
    return base


class TestClassifyRows:
    def test_resolved_mover_lands_in_minutes_section(self) -> None:
        rows = [_row(minutes_movers=["Alderman Feder"])]
        out = res.classify_rows(rows, _resolve)
        assert [r["resolved_mover"] for r in out["minutes"]] == ["Gary Feder"]
        assert out["gallery"] == [] and out["conflict"] == []

    def test_unresolvable_mover_is_silent(self) -> None:
        out = res.classify_rows([_row(minutes_movers=["Alderman Nobody"])], _resolve)
        assert out == {"minutes": [], "gallery": [], "conflict": []}

    def test_gallery_at_batch_bar_lands_in_gallery_section(self) -> None:
        g = {
            "best_official": "Susan Buse",
            "mean": 0.91,
            "min": 0.88,
            "margin": 0.2,
            "runner_up": "X",
            "gallery_clusters": 5,
            "scored_members": 1,
        }
        out = res.classify_rows([_row(gallery=g)], _resolve)
        assert len(out["gallery"]) == 1

    def test_below_bar_gallery_is_silent(self) -> None:
        g = {
            "best_official": "Susan Buse",
            "mean": 0.78,
            "min": 0.7,
            "margin": 0.3,
            "runner_up": "X",
            "gallery_clusters": 5,
            "scored_members": 1,
        }
        assert res.classify_rows([_row(gallery=g)], _resolve)["gallery"] == []

    def test_disagreeing_signals_become_a_conflict(self) -> None:
        g = {
            "best_official": "Susan Buse",
            "mean": 0.9,
            "min": 0.88,
            "margin": 0.2,
            "runner_up": "X",
            "gallery_clusters": 5,
            "scored_members": 1,
        }
        out = res.classify_rows([_row(gallery=g, minutes_movers=["Alderman Feder"])], _resolve)
        assert len(out["conflict"]) == 1
        assert out["minutes"] == [] and out["gallery"] == []

    def test_agreeing_signals_prefer_the_minutes_section(self) -> None:
        g = {
            "best_official": "Gary Feder",
            "mean": 0.9,
            "min": 0.88,
            "margin": 0.2,
            "runner_up": "X",
            "gallery_clusters": 5,
            "scored_members": 1,
        }
        out = res.classify_rows([_row(gallery=g, minutes_movers=["Alderman Feder"])], _resolve)
        assert [r["resolved_mover"] for r in out["minutes"]] == ["Gary Feder"]
