"""Every model the app calls is configuration (actalux.config)."""

from __future__ import annotations

import pytest

from actalux.config import Config

_MODEL_VARS = (
    "ACTALUX_SUMMARY_MODEL",
    "ACTALUX_DOC_SUMMARY_MODEL",
    "ACTALUX_LANDUSE_MODEL",
    "ACTALUX_DISCOURSE_MODEL",
    "ACTALUX_CONDENSE_MODEL",
    "ACTALUX_EXPANSION_MODEL",
    "ACTALUX_TRANSCRIBE_MODEL",
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in _MODEL_VARS:
        monkeypatch.delenv(var, raising=False)


def test_unset_variables_keep_the_validated_defaults() -> None:
    cfg = Config()
    assert cfg.summary_model == "openai/gpt-5-mini"
    assert cfg.landuse_model == "openai/gpt-5-mini"
    assert cfg.condense_model == "openai/gpt-4o-mini"
    assert cfg.transcribe_model == "whisper-large-v3"


def test_each_job_can_move_independently(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ACTALUX_LANDUSE_MODEL", "openai/gpt-6-luna")
    cfg = Config()
    assert cfg.landuse_model == "openai/gpt-6-luna"
    # The public answer model does not move with an offline job.
    assert cfg.summary_model == "openai/gpt-5-mini"


def test_offline_jobs_follow_the_summary_model_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ACTALUX_SUMMARY_MODEL", "openai/some-model")
    cfg = Config()
    assert cfg.doc_summary_model == "openai/some-model"
    assert cfg.discourse_model == "openai/some-model"
    assert cfg.landuse_model == "openai/some-model"


def test_empty_variable_falls_back_rather_than_blanking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ACTALUX_CONDENSE_MODEL", "")
    assert Config().condense_model == "openai/gpt-4o-mini"
