"""One-off GPU benchmark: WhisperX large-v3 transcribe+align on T4 vs L4 vs A10.

Ships ONE meeting's audio to the same transcription workload production runs
(whisperx_modal.py), on each GPU, and reports the GPU-side timings + Modal cost so
the GPU choice is made from measured numbers, not estimates. Measures REMOTE
(in-function) time, so client upload/queue latency never pollutes the comparison.

Reported per GPU:
  load_s       model download + load (one-time per warm container; ~network-bound)
  transcribe_s faster-whisper large-v3 (the decode-heavy part)
  align_s      wav2vec2 forced alignment
  RTF          (transcribe+align) / audio_seconds  -- lower is faster
  warm $/mtg   (transcribe+align) x $/sec  -- the batch steady-state cost driver
  cold $/mtg   (load+transcribe+align) x $/sec  -- a one-off cold call

Run (Modal tokens from Doppler actalux):
  MODAL_TOKEN_ID="$(doppler secrets get MODAL_TOKEN_ID --plain --project actalux --config dev)" \
  MODAL_TOKEN_SECRET="$(doppler secrets get MODAL_TOKEN_SECRET --plain --project actalux --config dev)" \
  uv run --group diarization modal run scripts/whisperx_gpu_bench.py --audio-path <file.mp3>
"""  # noqa: E501

from __future__ import annotations

import time
from pathlib import Path

import modal

WHISPER_MODEL = "large-v3"
# modal.com/pricing (per-second), verified 2026-06-28.
PRICE_PER_SEC = {"T4": 0.000164, "L4": 0.000222, "A10": 0.000306}

app = modal.App("actalux-whisperx-bench")
image = modal.Image.debian_slim(python_version="3.11").apt_install("ffmpeg").pip_install("whisperx")


def _run(audio_bytes: bytes) -> dict:
    """Load large-v3, transcribe+align one audio file; return GPU-side timings."""
    import gc
    import tempfile

    import torch
    import whisperx

    device = "cuda"
    gc.collect()
    torch.cuda.empty_cache()
    with tempfile.NamedTemporaryFile(suffix=".mp3") as f:
        f.write(audio_bytes)
        f.flush()
        audio = whisperx.load_audio(f.name)
    audio_s = len(audio) / 16000.0  # whisperx.load_audio resamples to 16 kHz mono

    t0 = time.perf_counter()
    model = whisperx.load_model(
        WHISPER_MODEL,
        device,
        compute_type="float16",
        asr_options={"condition_on_previous_text": False},
    )
    load_s = time.perf_counter() - t0

    t0 = time.perf_counter()
    result = model.transcribe(audio, batch_size=16)
    transcribe_s = time.perf_counter() - t0
    language = result["language"]

    t0 = time.perf_counter()
    align_model, metadata = whisperx.load_align_model(language_code=language, device=device)
    aligned = whisperx.align(
        result["segments"], align_model, metadata, audio, device, return_char_alignments=False
    )
    align_s = time.perf_counter() - t0

    n_words = sum(len(s.get("words", [])) for s in aligned["segments"])
    del model, align_model, aligned, result, audio
    gc.collect()
    torch.cuda.empty_cache()
    return {
        "audio_s": audio_s,
        "load_s": load_s,
        "transcribe_s": transcribe_s,
        "align_s": align_s,
        "language": language,
        "n_words": n_words,
    }


@app.function(image=image, gpu="T4", timeout=60 * 60)
def bench_t4(audio_bytes: bytes) -> dict:
    return _run(audio_bytes)


@app.function(image=image, gpu="L4", timeout=60 * 60)
def bench_l4(audio_bytes: bytes) -> dict:
    return _run(audio_bytes)


@app.function(image=image, gpu="A10", timeout=60 * 60)
def bench_a10(audio_bytes: bytes) -> dict:
    return _run(audio_bytes)


@app.local_entrypoint()
def main(audio_path: str) -> None:
    audio_bytes = Path(audio_path).read_bytes()
    fns = {"T4": bench_t4, "L4": bench_l4, "A10": bench_a10}
    print(f"\naudio: {audio_path} ({len(audio_bytes) / 1e6:.1f} MB)\n")
    rows = []
    for gpu, fn in fns.items():
        r = fn.remote(audio_bytes)
        work_s = r["transcribe_s"] + r["align_s"]
        rtf = work_s / r["audio_s"] if r["audio_s"] else 0
        warm = work_s * PRICE_PER_SEC[gpu]
        cold = (r["load_s"] + work_s) * PRICE_PER_SEC[gpu]
        rows.append((gpu, r, work_s, rtf, warm, cold))
        print(
            f"{gpu:4}  audio={r['audio_s'] / 60:5.1f}min  load={r['load_s']:5.1f}s  "
            f"transcribe={r['transcribe_s']:6.1f}s  align={r['align_s']:5.1f}s  "
            f"RTF={rtf:.3f}  warm=${warm:.4f}/mtg  cold=${cold:.4f}/mtg  words={r['n_words']}"
        )
    base = next(x for x in rows if x[0] == "T4")
    print("\nspeedup vs T4 (transcribe+align):")
    for gpu, _r, work_s, _rtf, warm, _cold in rows:
        print(f"  {gpu:4}  {base[2] / work_s:.2f}x faster   warm-cost {warm / base[4]:.2f}x of T4")
