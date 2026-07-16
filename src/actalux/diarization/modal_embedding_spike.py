"""Phase 0 spike — prove pyannote 3.1's per-cluster speaker embeddings.

A THROWAWAY validation harness for the voiceprint plan
(``docs/architecture/voiceprint-speaker-id-plan.md`` §4). It answers the
empirical questions the ``migrate_040`` schema depends on and that must NOT be
guessed (see the "never invent values" cardinal):

  - embedding dimension → the ``VECTOR(<DIM>)`` column
  - which model pyannote loads for embeddings, and its pinned version
  - whether the returned embeddings are L2-normalized (decides whether we
    normalize before storing so cosine == dot product)
  - repeatability: identical audio → identical vector across two runs in the
    same container (a proxy for run-to-run / rebuild stability)
  - NaN rows: pyannote emits NaN for a cluster with too little speech to embed

It does NOT touch the database or the production ``diarize_remote`` path, so the
production diarization stays byte-stable while we measure. Delete this file once
the model id + ``<DIM>`` are frozen into ``migrate_040``.

Run (billable Modal GPU — needs operator go-ahead and a local audio file):

    doppler run --project actalux --config dev -- \\
      uv run --group diarization modal run \\
      src/actalux/diarization/modal_embedding_spike.py --audio-path meeting.mp3
"""

from __future__ import annotations

import json
from pathlib import Path

import modal

PYANNOTE_MODEL = "pyannote/speaker-diarization-3.1"
# The embedding half of the 3.1 pipeline, loaded directly. Extracting embeddings
# ourselves (rather than via the diarization pipeline's return) keeps us stable
# across pyannote 3.x/4.x — the spike proved 4.x ignores `return_embeddings` and
# returns a DiarizeOutput — and lets us control per-cluster aggregation.
EMBED_MODEL = "pyannote/wespeaker-voxceleb-resnet34-LM"
APP_NAME = "actalux-embedding-spike"
# Cap the speech fed to one speaker's embedding: deterministic (for the
# repeatability check) and bounds memory on a talkative official.
SPIKE_EMBED_SECONDS = 180.0
SPIKE_MIN_SECONDS = 3.0  # below this the pooling std collapses to NaN

app = modal.App(APP_NAME)

# Mirror the production diarization image exactly (modal_runner.py) so the spike
# measures what production would actually load. pyannote is intentionally
# unpinned here too — the point of the spike is to READ the resolved version so
# we can pin it afterward.
image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("ffmpeg")
    .pip_install("torch", "torchaudio", "pyannote.audio>=3.1")
)


def _decode_16k_mono(audio_bytes: bytes, tmp: str) -> tuple:
    """Decode arbitrary audio to a 16 kHz mono waveform tensor (as production does)."""
    import os
    import subprocess

    import torchaudio

    src = os.path.join(tmp, "audio.in")
    wav = os.path.join(tmp, "audio.wav")
    with open(src, "wb") as f:
        f.write(audio_bytes)
    subprocess.run(
        ["ffmpeg", "-nostdin", "-loglevel", "error", "-i", src,
         "-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le", "-y", wav],
        check=True,
    )  # fmt: skip
    return torchaudio.load(wav)


def _annotation(result):  # noqa: ANN001, ANN202 - pyannote types not importable locally
    """The diarization Annotation, across pyannote 3.x/4.x return shapes.

    3.x returns an Annotation directly (has ``itertracks``); 4.x wraps it in a
    ``DiarizeOutput`` whose Annotation is at ``.speaker_diarization`` — the same
    branch ``modal_runner.diarize_remote`` already uses.
    """
    return result if hasattr(result, "itertracks") else result.speaker_diarization


@app.function(
    image=image,
    gpu="L4",
    secrets=[modal.Secret.from_name("actalux-hf")],
    timeout=60 * 60,
)
def embedding_spike(audio_bytes: bytes, hint_num_speakers: int | None = None) -> dict:
    """Diarize once, then embed each speaker's own speech twice; report the metrics."""
    import inspect
    import os
    import tempfile

    import numpy as np
    import torch
    from pyannote.audio import Pipeline
    from pyannote.audio.pipelines.speaker_verification import PretrainedSpeakerEmbedding

    def _version(pkg: str) -> str:
        try:
            import importlib.metadata as im

            return im.version(pkg)
        except Exception as exc:  # noqa: BLE001 - version read is diagnostic, never fatal
            return f"unread: {exc}"

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    token = os.environ["HF_TOKEN"]

    pipeline = Pipeline.from_pretrained(PYANNOTE_MODEL, token=token)
    pipeline.to(device)

    # Load the embedding model directly. The HF-token kwarg was renamed across
    # versions (use_auth_token -> token); pass whichever this build accepts.
    emb_kwargs: dict = {"device": device}
    sig = inspect.signature(PretrainedSpeakerEmbedding.__init__)
    if "use_auth_token" in sig.parameters:
        emb_kwargs["use_auth_token"] = token
    elif "token" in sig.parameters:
        emb_kwargs["token"] = token
    embedder = PretrainedSpeakerEmbedding(EMBED_MODEL, **emb_kwargs)

    with tempfile.TemporaryDirectory() as tmp:
        waveform, sample_rate = _decode_16k_mono(audio_bytes, tmp)  # (channels, samples)

    kwargs = {"num_speakers": hint_num_speakers} if hint_num_speakers else {}
    result = pipeline({"waveform": waveform, "sample_rate": sample_rate}, **kwargs)
    annotation = _annotation(result)

    # Per-speaker: concatenate that speaker's audio (capped, deterministic), embed.
    def collect_speech(label: str) -> tuple:
        slices, secs = [], 0.0
        for seg, _, lab in annotation.itertracks(yield_label=True):
            if lab != label or secs >= SPIKE_EMBED_SECONDS:
                continue
            a, b = int(seg.start * sample_rate), int(seg.end * sample_rate)
            if b > a:
                slices.append(waveform[:, a:b])
                secs += (b - a) / sample_rate
        if not slices:
            return None, 0.0
        return torch.cat(slices, dim=1), secs  # (channels, total_samples)

    def embed(label: str) -> tuple:
        speech, secs = collect_speech(label)
        if speech is None or secs < SPIKE_MIN_SECONDS:
            return None, secs
        # PretrainedSpeakerEmbedding wants (batch, channel, samples).
        batch = speech.unsqueeze(0).to(device)
        vec = np.asarray(embedder(batch), dtype=np.float64).reshape(-1)
        return vec, secs

    labels = sorted(annotation.labels())
    run1 = {lab: embed(lab) for lab in labels}
    run2 = {lab: embed(lab) for lab in labels}

    dims = {v.shape[0] for v, _ in run1.values() if v is not None}
    norms = [round(float(np.linalg.norm(v)), 4) for v, _ in run1.values() if v is not None]
    seconds = {lab: round(s, 1) for lab, (_, s) in run1.items()}
    skipped = [lab for lab, (v, _) in run1.items() if v is None]

    # Repeatability: cosine between the same label's vector across the two runs.
    repeat_cos: list[float] = []
    for lab in labels:
        a = run1[lab][0]
        b = run2[lab][0]
        if a is None or b is None:
            continue
        denom = float(np.linalg.norm(a) * np.linalg.norm(b))
        if denom > 0:
            repeat_cos.append(round(float(np.dot(a, b) / denom), 6))

    return {
        "pyannote_version": _version("pyannote.audio"),
        "torch_version": torch.__version__,
        "cuda": bool(torch.cuda.is_available()),
        "diarization_return_type": type(result).__name__,
        "embedding_model": EMBED_MODEL,
        "num_speakers": len(labels),
        "embedded_speakers": len(labels) - len(skipped),
        "skipped_short_speakers": skipped,
        "dims_seen": sorted(dims),
        "dim": dims.pop() if len(dims) == 1 else None,
        "row_l2_norms": norms,
        "seconds_per_speaker": seconds,
        "repeatability_cosine": repeat_cos,
        "repeatability_min": min(repeat_cos) if repeat_cos else None,
    }


@app.local_entrypoint()
def main(audio_path: str, hint_num_speakers: int = 0) -> None:
    """`modal run modal_embedding_spike.py --audio-path FILE` — report the metrics."""
    audio_bytes = Path(audio_path).read_bytes()
    result = embedding_spike.remote(audio_bytes, hint_num_speakers or None)
    print("\n=== voiceprint embedding spike ===")
    print(json.dumps(result, indent=2))
    dim = result.get("dim")
    norms = result.get("row_l2_norms") or []
    rmin = result.get("repeatability_min")
    print("\n--- decisions this freezes ---")
    print(f"  VECTOR(<DIM>)          -> {dim}")
    print(f"  embedding model        -> {result.get('embedding_model')}")
    print(f"  pyannote pin           -> pyannote.audio=={result.get('pyannote_version')}")
    normalized = all(abs(n - 1.0) < 1e-3 for n in norms) if norms else None
    print(f"  already L2-normalized? -> {normalized} (norms={norms})")
    print(f"  repeatability (min cos)-> {rmin}  (want ~1.0)")
