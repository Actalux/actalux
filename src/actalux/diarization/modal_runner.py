"""Modal serverless-GPU adapter for diarization (pyannote.audio).

The ``DiarizationBackend`` implementation that runs pyannote on a GPU. The heavy
deps (torch, pyannote.audio) live in the Modal *image*, never the repo's own
environment — locally we only need the lightweight ``modal`` client. The gated
pyannote weights are fetched with ``HF_TOKEN`` from the Modal secret ``actalux-hf``.

Run (under doppler so MODAL_TOKEN_ID/SECRET authenticate the client):

    # one-off smoke test of a local audio file (ephemeral app, builds the image)
    doppler run --project actalux --config dev -- \\
      uv run --group diarization modal run \\
      src/actalux/diarization/modal_runner.py --audio-path meeting.mp3

    # deploy so ModalRunner (and the A/B harness) can call the function by name
    doppler run --project actalux --config dev -- \\
      uv run --group diarization modal deploy src/actalux/diarization/modal_runner.py
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import modal

if TYPE_CHECKING:
    # Only the local side (ModalRunner / the entrypoint) maps the wire format onto
    # our domain type. The remote container loads this module to find
    # ``diarize_remote`` and must not import ``actalux`` — its image carries only
    # torch + pyannote, never the package.
    from actalux.diarization.backend import SpeakerTimeline

PYANNOTE_MODEL = "pyannote/speaker-diarization-3.1"
# The embedding half of the 3.1 pipeline, loaded directly. The Phase 0 spike
# (docs/architecture/voiceprint-speaker-id-plan.md §4) showed pyannote 4.x returns
# a DiarizeOutput and ignores ``return_embeddings``; extracting embeddings
# ourselves is version-stable and gives us per-cluster aggregation control.
EMBED_MODEL = "pyannote/wespeaker-voxceleb-resnet34-LM"  # 256-d, cosine
# Per-cluster embedding: cap the speech fed to the embedder (deterministic, bounds
# memory on a talkative official) and skip clusters too short to embed reliably
# (below this the pooling std collapses to NaN).
EMBED_MAX_SECONDS = 180.0
EMBED_MIN_SECONDS = 3.0
APP_NAME = "actalux-diarization"

app = modal.App(APP_NAME)

# torch + pyannote live here, not in the repo env. Default PyPI torch ships the
# CUDA runtime, so it uses the GPU on a GPU-backed function. pyannote.audio + torch
# are PINNED to what the Phase 0 spike validated (4.0.5 / 2.12.1): the embedding
# vectors this pipeline emits are the gallery's substrate, so an unpinned upgrade
# that shifted them would silently invalidate stored voiceprints. torchaudio is
# left to resolve (it has no 2.12.1 release; the spike ran it unpinned) — it is only
# used to decode audio to a waveform, which does not affect the embeddings.
image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("ffmpeg")
    .pip_install("torch==2.12.1", "torchaudio", "pyannote.audio==4.0.5")
)


def _decode_16k_mono(audio_bytes: bytes):  # noqa: ANN202 - torch tensor type not importable locally
    """Decode arbitrary audio to a 16 kHz mono waveform tensor; return (waveform, sr).

    MP3 frame padding makes pyannote's file cropper miscount samples ("got N instead
    of expected M"); a decoded tensor has exact sample counts. The tensor is read
    fully into memory, so it outlives the temp file.
    """
    import os
    import subprocess
    import tempfile

    import torchaudio

    with tempfile.TemporaryDirectory() as tmp:
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


def _load_embedder(token: str, device):  # noqa: ANN001, ANN202 - pyannote/torch types not importable locally
    """Load the pinned wespeaker speaker-embedding model onto ``device``.

    The HF-token kwarg was renamed across pyannote versions (use_auth_token ->
    token); pass whichever this build's signature accepts.
    """
    import inspect

    from pyannote.audio.pipelines.speaker_verification import PretrainedSpeakerEmbedding

    emb_kwargs: dict = {"device": device}
    sig = inspect.signature(PretrainedSpeakerEmbedding.__init__)
    if "use_auth_token" in sig.parameters:
        emb_kwargs["use_auth_token"] = token
    elif "token" in sig.parameters:
        emb_kwargs["token"] = token
    return PretrainedSpeakerEmbedding(EMBED_MODEL, **emb_kwargs)


def _embed_spans(waveform, sample_rate, spans, embedder, device):  # noqa: ANN001, ANN202 - pyannote/torch types not importable locally
    """L2-normalized voice embedding over the given ``[(start_s, end_s), ...]`` spans.

    Concatenates the spans in order (capped for determinism + memory), embeds with
    the wespeaker model, and L2-normalizes so cosine == dot product. Returns
    ``(vector, seconds)`` with ``vector`` a ``list[float]``, or ``(None, seconds)``
    when the speech is too short / degenerate to embed reliably (NaN-prone).
    """
    import numpy as np
    import torch

    slices, secs = [], 0.0
    for start_s, end_s in spans:
        if secs >= EMBED_MAX_SECONDS:
            break
        a, b = int(start_s * sample_rate), int(end_s * sample_rate)
        # truncate a single long span to the remaining budget (a hard cap, not just a
        # per-span check) so one long diarization turn cannot blow past EMBED_MAX_SECONDS.
        b = min(b, a + int((EMBED_MAX_SECONDS - secs) * sample_rate))
        if b > a:
            slices.append(waveform[:, a:b])
            secs += (b - a) / sample_rate
    if not slices or secs < EMBED_MIN_SECONDS:
        return None, secs
    speech = torch.cat(slices, dim=1).unsqueeze(0).to(device)  # (1, channel, samples)
    vec = np.asarray(embedder(speech), dtype=np.float64).reshape(-1)
    norm = float(np.linalg.norm(vec))
    if norm == 0 or not np.isfinite(norm):
        return None, secs
    return [float(x) for x in (vec / norm).tolist()], secs


def _embed_turns(waveform, sample_rate, spans, embedder, device) -> list[dict]:  # noqa: ANN001, ANN202 - pyannote/torch types not importable locally
    """Embed each of a cluster's turns individually -> ``[{vector, seconds}, ...]``.

    The enrollment path embeds turns SEPARATELY (not a whole-cluster concat) so local
    pooling (Gate B, ``diarization/pooling.py``) can trim contaminated turns and reject a
    cluster with no coherent core. Longest turns first, bounded by ``EMBED_MAX_SECONDS`` of
    cumulative speech (determinism + memory); each turn goes through ``_embed_spans`` so it
    inherits the same ``EMBED_MIN_SECONDS`` floor and L2 normalization.
    """
    ordered = sorted(spans, key=lambda s: s[1] - s[0], reverse=True)
    out: list[dict] = []
    total = 0.0
    for start_s, end_s in ordered:
        if total >= EMBED_MAX_SECONDS:
            break
        vector, secs = _embed_spans(waveform, sample_rate, [(start_s, end_s)], embedder, device)
        if vector is None:
            continue
        out.append({"vector": vector, "seconds": round(secs, 2)})
        total += secs
    return out


def _extract_cluster_embeddings(annotation, waveform, sample_rate, embedder, device) -> list[dict]:  # noqa: ANN001 - pyannote/torch types not importable locally
    """Per-cluster L2-normalized voice embeddings over each cluster's own speech.

    The vectors are anonymous — they name no one; enrollment (officials-only) is
    downstream. Clusters too short to embed reliably are dropped.
    """
    rows: list[dict] = []
    for label in sorted(annotation.labels()):
        spans = [
            (seg.start, seg.end)
            for seg, _, lab in annotation.itertracks(yield_label=True)
            if lab == label
        ]
        vector, seconds = _embed_spans(waveform, sample_rate, spans, embedder, device)
        if vector is None:
            continue
        rows.append(
            {
                "cluster_label": label,
                "vector": vector,
                "seconds": round(seconds, 2),
                "model": EMBED_MODEL,
            }
        )
    return rows


@app.function(
    image=image,
    gpu="L4",
    secrets=[modal.Secret.from_name("actalux-hf")],
    timeout=60 * 60,
)
def diarize_remote(
    audio_bytes: bytes,
    hint_num_speakers: int | None = None,
    return_embeddings: bool = False,
) -> dict:
    """Diarize one audio file on the GPU; optionally also emit per-cluster embeddings.

    Returns ``{"segments": [{speaker,start,end}, ...], "embeddings": [...]}``. The
    default transcribe path passes ``return_embeddings=False`` — diarization only,
    no extra GPU work, behaviour unchanged. Enrollment and matching pass ``True``:
    voiceprints are extracted ON DEMAND for a specific meeting, never persisted for
    un-confirmed speakers (a private citizen's voice is never stored — see the plan).
    """
    import os

    import torch
    from pyannote.audio import Pipeline

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    token = os.environ["HF_TOKEN"]

    pipeline = Pipeline.from_pretrained(PYANNOTE_MODEL, token=token)
    if torch.cuda.is_available():
        pipeline.to(device)

    waveform, sample_rate = _decode_16k_mono(audio_bytes)

    kwargs = {"num_speakers": hint_num_speakers} if hint_num_speakers else {}
    result = pipeline({"waveform": waveform, "sample_rate": sample_rate}, **kwargs)

    # pyannote 3.x returns an Annotation directly; 4.x wraps it in a DiarizeOutput
    # whose Annotation is at ``.speaker_diarization``. Support both.
    annotation = result if hasattr(result, "itertracks") else result.speaker_diarization
    segments = [
        {"speaker": label, "start": float(turn.start), "end": float(turn.end)}
        for turn, _, label in annotation.itertracks(yield_label=True)
    ]

    embeddings: list[dict] = []
    if return_embeddings:
        embedder = _load_embedder(token, device)
        embeddings = _extract_cluster_embeddings(
            annotation, waveform, sample_rate, embedder, device
        )
    return {"segments": segments, "embeddings": embeddings}


@app.function(
    image=image,
    gpu="L4",
    secrets=[modal.Secret.from_name("actalux-hf")],
    timeout=60 * 60,
)
def embed_cluster_turns_remote(audio_bytes: bytes, clusters: list) -> list:
    """Embed each cluster's STORED turns individually -> per-turn voiceprints.

    The enrollment path. A confirmed cluster's turns live in ``diarization_turns``;
    re-diarizing a meeting would renumber the ``SPEAKER_NN`` labels, so we embed the stored
    spans directly rather than re-clustering. Turns are embedded SEPARATELY (not a
    whole-cluster concat) so the caller can pool with contamination-trimming + no-core
    rejection (Gate B). One GPU load per meeting.

    ``clusters``: ``[{"cluster_label": str, "spans": [[start_s, end_s], ...]}, ...]``.
    Returns ``[{"cluster_label", "turns": [{"vector": [...], "seconds"}, ...], "model"}]``.
    """
    import os

    import torch

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    embedder = _load_embedder(os.environ["HF_TOKEN"], device)
    waveform, sample_rate = _decode_16k_mono(audio_bytes)

    out: list[dict] = []
    for c in clusters:
        spans = [(float(a), float(b)) for a, b in c["spans"]]
        turns = _embed_turns(waveform, sample_rate, spans, embedder, device)
        out.append(
            {
                "cluster_label": c["cluster_label"],
                "turns": turns,
                "model": EMBED_MODEL,
            }
        )
    return out


class ModalRunner:
    """``DiarizationBackend`` backed by the deployed Modal ``diarize_remote`` function.

    Structurally implements the ``DiarizationBackend`` Protocol (it is not
    declared as a base class so loading this module on the GPU container never
    imports ``actalux``). Requires the app to be deployed first (``modal deploy``);
    audio is read locally and shipped to the GPU as bytes.
    """

    def __init__(self, model: str = PYANNOTE_MODEL) -> None:
        self._model = model
        self._fn = modal.Function.from_name(APP_NAME, "diarize_remote")

    def run(
        self,
        audio_uri: str,
        *,
        hint_num_speakers: int | None = None,
        return_embeddings: bool = False,
    ) -> SpeakerTimeline:
        from actalux.diarization.backend import SpeakerTimeline

        audio_bytes = Path(audio_uri).read_bytes()
        payload = self._fn.remote(audio_bytes, hint_num_speakers, return_embeddings)
        return SpeakerTimeline.from_remote(payload, self._model)

    def spawn(
        self,
        audio_bytes: bytes,
        *,
        hint_num_speakers: int | None = None,
        return_embeddings: bool = False,
    ) -> modal.FunctionCall:
        """Kick off a diarization without blocking; returns a handle for ``collect``.

        Lets a backfill spawn every meeting's GPU work up front so it all runs in
        parallel across Modal containers, instead of one blocking ``run`` at a time.
        Pair with ``collect`` to retrieve the result. ``return_embeddings`` requests
        per-cluster voiceprints (enrollment / matching), off by default.
        """
        return self._fn.spawn(audio_bytes, hint_num_speakers, return_embeddings)

    def collect(self, call: modal.FunctionCall) -> SpeakerTimeline:
        """Block for a spawned diarization's result and map it to ``SpeakerTimeline``."""
        from actalux.diarization.backend import SpeakerTimeline

        return SpeakerTimeline.from_remote(call.get(), self._model)

    def embed_cluster_turns(
        self, audio_uri: str, clusters: list[dict]
    ) -> dict[str, list[tuple[tuple[float, ...], float]]]:
        """Embed stored cluster turns for one meeting -> ``{cluster_label: [(vector, seconds)]}``.

        ``clusters`` is ``[{"cluster_label": str, "spans": [[start_s, end_s], ...]}]`` (the
        enrollment path: spans come from the stored ``diarization_turns``, robust to
        re-diarization renumbering). Returns per-turn embeddings so the caller pools with
        Gate B (``diarization/pooling.py``). Clusters with no embeddable turn are absent.
        One GPU load per meeting.
        """
        audio_bytes = Path(audio_uri).read_bytes()
        fn = modal.Function.from_name(APP_NAME, "embed_cluster_turns_remote")
        out: dict[str, list[tuple[tuple[float, ...], float]]] = {}
        for r in fn.remote(audio_bytes, clusters):
            turns = [(tuple(t["vector"]), float(t["seconds"])) for t in r["turns"]]
            if turns:
                out[r["cluster_label"]] = turns
        return out

    @staticmethod
    def cancel(call: modal.FunctionCall) -> None:
        """Best-effort cancel a spawned call so a half-failed pair leaves no orphan running."""
        try:
            call.cancel()
        except Exception:  # noqa: BLE001 - cleanup must never mask the original error
            pass


@app.local_entrypoint()
def main(audio_path: str, hint_num_speakers: int = 0, return_embeddings: bool = False) -> None:
    """`modal run modal_runner.py --audio-path FILE` — diarize one local file."""
    from actalux.diarization.backend import SpeakerTimeline

    audio_bytes = Path(audio_path).read_bytes()
    payload = diarize_remote.remote(audio_bytes, hint_num_speakers or None, return_embeddings)
    timeline = SpeakerTimeline.from_remote(payload, PYANNOTE_MODEL)
    print(
        f"\nspeakers={timeline.num_speakers}  turns={len(timeline.turns)}  "
        f"embeddings={len(timeline.embeddings)}"
    )
    for turn in timeline.turns[:10]:
        print(f"  {turn.cluster_label}  {turn.start_s:8.1f} - {turn.end_s:8.1f}s")
