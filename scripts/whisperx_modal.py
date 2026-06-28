"""Modal serverless-GPU WhisperX transcribe+align (A/B Path B, transcription side).

WhisperX's value-add over our hosted Groq Whisper is **word-level timestamps**
from wav2vec2 forced alignment. This function does exactly that — faster-whisper
``large-v3`` transcription (name-biased with the same ``initial_prompt`` we pass
Groq) plus forced alignment — and returns segments carrying their words.

It deliberately does NOT diarize: the A/B reuses the same pyannote turns
(``actalux-diarization``) for both paths, so the only variable under test is the
transcription engine + timestamp granularity, not which diarizer. That also keeps
this image free of whisperx's pinned pyannote (and its ``use_auth_token`` churn).

Run (Modal tokens from Doppler ``actalux``):

    MODAL_TOKEN_ID="$(doppler secrets get MODAL_TOKEN_ID --plain --project actalux --config dev)" \
    MODAL_TOKEN_SECRET="$(doppler secrets get MODAL_TOKEN_SECRET --plain --project actalux --config dev)" \
    uv run --group diarization modal run scripts/whisperx_modal.py --audio-path smoke.mp3

    # deploy so the A/B harness can call it by name
    ... modal deploy scripts/whisperx_modal.py
"""  # noqa: E501

from __future__ import annotations

from pathlib import Path

import modal

WHISPER_MODEL = "large-v3"
APP_NAME = "actalux-whisperx-eval"  # eval-only; the production app is actalux-whisperx

app = modal.App(APP_NAME)

# whisperx pulls a CUDA torch + ctranslate2 + the wav2vec2 align stack. cuDNN 8 is
# needed by ctranslate2's CUDA backend; the pip wheel provides it.
image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("ffmpeg")
    .pip_install("whisperx")
)


@app.function(image=image, gpu="T4", timeout=60 * 60)
def transcribe_align_remote(
    audio_bytes: bytes,
    initial_prompt: str = "",
    hotwords: str = "",
    no_repeat_ngram_size: int = 0,
    repetition_penalty: float = 1.0,
) -> dict:
    """Transcribe + word-align one audio file; return ``{language, segments[{...,words}]}``.

    Name biasing is a knob, not a default: ``initial_prompt``/``hotwords`` both feed
    Whisper's decoder prefix (and in WhisperX's batched path are re-applied per VAD
    chunk, so they can echo into the transcript). ``no_repeat_ngram_size`` /
    ``repetition_penalty`` curb the repeated-phrase failure mode in that same path.
    """
    import gc
    import tempfile

    import torch
    import whisperx

    device = "cuda"
    # Modal keeps the container warm across calls; whisperx models otherwise pile up
    # in VRAM and OOM a T4 by the 3rd config. Clear any residue from a prior call.
    gc.collect()
    torch.cuda.empty_cache()
    with tempfile.NamedTemporaryFile(suffix=".mp3") as f:
        f.write(audio_bytes)
        f.flush()
        audio = whisperx.load_audio(f.name)

    # Only set keys we mean to change, so WhisperX keeps its other defaults.
    asr_options: dict = {"condition_on_previous_text": False}
    if initial_prompt:
        asr_options["initial_prompt"] = initial_prompt
    if hotwords:
        asr_options["hotwords"] = hotwords
    if no_repeat_ngram_size:
        asr_options["no_repeat_ngram_size"] = no_repeat_ngram_size
    if repetition_penalty != 1.0:
        asr_options["repetition_penalty"] = repetition_penalty
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
    # Release GPU memory before the container handles the next config.
    del model, align_model, aligned, result, audio
    gc.collect()
    torch.cuda.empty_cache()
    return {"language": language, "segments": segments}


@app.local_entrypoint()
def main(audio_path: str, initial_prompt: str = "") -> None:
    """`modal run whisperx_modal.py --audio-path FILE` — transcribe+align one file."""
    out = transcribe_align_remote.remote(Path(audio_path).read_bytes(), initial_prompt)
    segs = out["segments"]
    n_words = sum(len(s["words"]) for s in segs)
    print(f"\nlanguage={out['language']}  segments={len(segs)}  words={n_words}")
    for s in segs[:5]:
        print(f"  {s['start']:7.1f}-{s['end']:7.1f}s  {s['text'][:90]}")
