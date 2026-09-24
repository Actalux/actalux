"""Every model the app calls is configuration (actalux.config)."""

from __future__ import annotations

import pytest

from actalux.config import MODEL_SETTINGS, Config

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


def test_every_model_comes_from_the_settings_file() -> None:
    # With no overrides set, each job's model is exactly its line in the file.
    cfg = Config()
    llm = MODEL_SETTINGS["llm"]
    assert cfg.summary_model == llm["summary"]
    assert cfg.doc_summary_model == llm["doc_summary"]
    assert cfg.landuse_model == llm["landuse"]
    assert cfg.discourse_model == llm["discourse"]
    assert cfg.condense_model == llm["condense"]
    assert cfg.expansion_model == llm["expansion"]
    assert cfg.jev_model == llm["citation_judge"]
    assert cfg.transcribe_model == MODEL_SETTINGS["speech"]["transcribe_groq"]
    assert cfg.embedding_model == MODEL_SETTINGS["pinned"]["embedding"]
    assert dict(cfg.rerank_models) == dict(MODEL_SETTINGS["rerank"])


def test_pinned_models_ignore_environment_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    # Stored vectors came from the pinned embedding model; a run-time override
    # would silently search the wrong space, so none is honored.
    monkeypatch.setenv("ACTALUX_EMBEDDING_MODEL", "some/other-embedder")
    assert Config().embedding_model == MODEL_SETTINGS["pinned"]["embedding"]


def test_no_model_name_is_hardcoded_outside_the_settings_file() -> None:
    # Every model default in code must be read from model_settings.toml. Parses the
    # code (docstrings and comments are prose, not settings). The GPU containers
    # (modal_*) cannot import actalux and keep mirrored copies, pinned to the file by
    # test_speech_models_in_gpu_containers_match_the_settings_file; experiment
    # scripts define their own arms and are excluded.
    import ast
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parent.parent
    # Full model names only; bare family prefixes like "gpt-5" select a request
    # format, not a model, and are allowed.
    pattern = re.compile(
        r"^(?:openai|anthropic|google|typesafe|BAAI|pyannote)/[\w.-]+$"
        r"|^(?:gpt-\d[\w.]*-[\w.-]+|whisper-[\w-]+|rerank-[\w.-]+|zerank-[\w-]+|large-v\d)$"
    )
    allowed = {
        "src/actalux/transcription/modal_whisperx.py",
        "src/actalux/diarization/modal_runner.py",
        "src/actalux/diarization/modal_embedding_spike.py",
        "src/actalux/eval/rerank.py",
        "scripts/whisperx_modal.py",
        "scripts/whisperx_gpu_bench.py",
        "scripts/audit_citation_support.py",
    }

    def docstring_nodes(tree: ast.AST) -> set[int]:
        ids = set()
        for node in ast.walk(tree):
            body = getattr(node, "body", None)
            if isinstance(body, list) and body and isinstance(body[0], ast.Expr):
                if isinstance(body[0].value, ast.Constant) and isinstance(body[0].value.value, str):
                    ids.add(id(body[0].value))
        return ids

    offenders = []
    for path in [*root.glob("src/actalux/**/*.py"), *root.glob("scripts/**/*.py")]:
        rel = path.relative_to(root).as_posix()
        if rel in allowed:
            continue
        tree = ast.parse(path.read_text())
        skip = docstring_nodes(tree)
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and id(node) not in skip
                and pattern.match(node.value)
            ):
                offenders.append(f"{rel}:{node.lineno}: {node.value!r}")
    assert offenders == [], "model names belong in model_settings.toml:\n" + "\n".join(offenders)


def test_speech_models_in_gpu_containers_match_the_settings_file() -> None:
    # The Modal containers never import actalux, so they carry their own copies of
    # the pinned speech models. Those copies must equal model_settings.toml: a
    # change to the file fails here until the container is updated AND redeployed,
    # and the voiceprints recalibrated — which a pinned-model change requires anyway.
    from actalux.diarization import modal_runner
    from actalux.transcription import modal_whisperx

    pinned = MODEL_SETTINGS["pinned"]
    assert modal_whisperx.WHISPER_MODEL == pinned["transcription"]
    assert modal_runner.PYANNOTE_MODEL == pinned["diarization"]
    assert modal_runner.EMBED_MODEL == pinned["voice_embedding"]


def test_empty_variable_falls_back_rather_than_blanking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ACTALUX_CONDENSE_MODEL", "")
    assert Config().condense_model == "openai/gpt-4o-mini"


def test_only_the_actalux_openrouter_key_is_ever_read(monkeypatch: pytest.MonkeyPatch) -> None:
    # A shared Doppler project's generic key belongs to another account; picking
    # it up silently billed Actalux work to it. It must be ignored outright.
    monkeypatch.delenv("OPENROUTER_ACTALUX_KEY", raising=False)
    monkeypatch.setenv("OPENROUTER_API_KEY", "other-account-key")
    monkeypatch.setenv("VOYAGE_API_KEY", "other-account-key")
    monkeypatch.setenv("COHERE_API_KEY", "other-account-key")
    cfg = Config()
    assert cfg.openrouter_api_key == ""
    assert cfg.voyage_api_key == ""
    assert cfg.cohere_api_key == ""
    monkeypatch.setenv("OPENROUTER_ACTALUX_KEY", "actalux-key")
    assert Config().openrouter_api_key == "actalux-key"
