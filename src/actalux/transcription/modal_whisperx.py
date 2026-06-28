"""Modal serverless-GPU adapter: clean WhisperX word-level transcription.

The production transcription backend. WhisperX (faster-whisper ``large-v3`` +
wav2vec2 forced alignment) gives word-level timestamps; the **clean config is baked
in** — no ``initial_prompt``, no ``hotwords`` (both regurgitate into the transcript;
see the A/B), with ``no_repeat_ngram_size`` + ``repetition_penalty`` to curb repeats.
Name spelling is fixed downstream by the glossary, never at decode time.

The heavy deps (torch, whisperx) live in the Modal *image*, never the repo env;
locally we only need the ``modal`` client. Decoupled like ``diarization.modal_runner``:
the remote container loads this module to find ``transcribe_remote`` and must not
import ``actalux`` — so the domain import is local-only.

Run (Modal tokens from Doppler ``actalux``):

    MODAL_TOKEN_ID="$(doppler secrets get MODAL_TOKEN_ID --plain --project actalux --config dev)" \
    MODAL_TOKEN_SECRET="$(doppler secrets get MODAL_TOKEN_SECRET --plain --project actalux --config dev)" \
    uv run --group diarization modal deploy src/actalux/transcription/modal_whisperx.py
"""  # noqa: E501

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import modal

if TYPE_CHECKING:
    from actalux.transcription.backend import WordTranscript

WHISPER_MODEL = "large-v3"
SOURCE_MODEL = "whisperx/large-v3"
APP_NAME = "actalux-whisperx"

app = modal.App(APP_NAME)

# whisperx pulls a CUDA torch + ctranslate2 + the wav2vec2 align stack.
image = modal.Image.debian_slim(python_version="3.11").apt_install("ffmpeg").pip_install("whisperx")


@app.function(image=image, gpu="T4", timeout=60 * 60)
def transcribe_remote(audio_bytes: bytes) -> dict:
    """Clean word-level transcribe of one audio file; return ``{language, segments[...]}``.

    No name biasing (verbatim integrity); repeats curbed in WhisperX's batched path.
    """
    import gc
    import tempfile

    import torch
    import whisperx

    device = "cuda"
    gc.collect()
    torch.cuda.empty_cache()  # the warm container may carry a prior call's VRAM

    with tempfile.NamedTemporaryFile(suffix=".mp3") as f:
        f.write(audio_bytes)
        f.flush()
        audio = whisperx.load_audio(f.name)

    # Clean config: no initial_prompt / hotwords (they leak); curb repeats.
    asr_options = {
        "condition_on_previous_text": False,
        "no_repeat_ngram_size": 5,
        "repetition_penalty": 1.05,
    }
    model = whisperx.load_model(
        WHISPER_MODEL, device, compute_type="float16", asr_options=asr_options
    )
    result = model.transcribe(audio, batch_size=16)
    language = result["language"]

    align_model, metadata = whisperx.load_align_model(language_code=language, device=device)
    aligned = whisperx.align(
        result["segments"], align_model, metadata, audio, device, return_char_alignments=False
    )

    segments = []
    for s in aligned["segments"]:
        words = [
            {"word": w.get("word", ""), "start": float(w["start"]), "end": float(w["end"])}
            for w in s.get("words", [])
            if w.get("start") is not None and w.get("end") is not None
        ]
        segments.append(
            {
                "start": float(s.get("start", 0.0)),
                "end": float(s.get("end", 0.0)),
                "text": (s.get("text") or "").strip(),
                "words": words,
            }
        )

    del model, align_model, aligned, result, audio
    gc.collect()
    torch.cuda.empty_cache()
    return {"language": language, "segments": segments}


class WhisperXRunner:
    """``TranscriptionBackend`` backed by the deployed Modal ``transcribe_remote``.

    Structurally implements the ``TranscriptionBackend`` Protocol (not declared as a
    base class so loading this module on the GPU container never imports ``actalux``).
    Requires the app to be deployed first (``modal deploy``).
    """

    def __init__(self, model: str = SOURCE_MODEL) -> None:
        self._model = model
        self._fn = modal.Function.from_name(APP_NAME, "transcribe_remote")

    def transcribe(self, audio_uri: str) -> WordTranscript:
        from actalux.transcription.backend import WordTranscript

        payload = self._fn.remote(Path(audio_uri).read_bytes())
        return WordTranscript.from_payload(payload, self._model)

    def spawn(self, audio_bytes: bytes) -> modal.FunctionCall:
        """Kick off a transcription without blocking; returns a handle for ``collect``.

        Lets a backfill spawn every meeting's GPU work up front so it all runs in
        parallel across Modal containers, instead of one blocking ``transcribe`` at a
        time. Pair with ``collect`` to retrieve the result.
        """
        return self._fn.spawn(audio_bytes)

    def collect(self, call: modal.FunctionCall) -> WordTranscript:
        """Block for a spawned transcription's result and map it to ``WordTranscript``."""
        from actalux.transcription.backend import WordTranscript

        return WordTranscript.from_payload(call.get(), self._model)

    @staticmethod
    def cancel(call: modal.FunctionCall) -> None:
        """Best-effort cancel a spawned call so a half-failed pair leaves no orphan running."""
        try:
            call.cancel()
        except Exception:  # noqa: BLE001 - cleanup must never mask the original error
            pass


@app.local_entrypoint()
def main(audio_path: str) -> None:
    """`modal run modal_whisperx.py --audio-path FILE` — transcribe one local file."""
    from actalux.transcription.backend import WordTranscript

    payload = transcribe_remote.remote(Path(audio_path).read_bytes())
    tx = WordTranscript.from_payload(payload, SOURCE_MODEL)
    n_words = len(tx.all_words())
    print(f"\nlanguage={tx.language}  segments={len(tx.segments)}  words={n_words}")
    for s in tx.segments[:5]:
        print(f"  {s.start_s:7.1f}-{s.end_s:7.1f}s  {s.text[:90]}")
