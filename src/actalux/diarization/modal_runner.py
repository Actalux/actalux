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
APP_NAME = "actalux-diarization"

app = modal.App(APP_NAME)

# torch + pyannote live here, not in the repo env. Default PyPI torch ships the
# CUDA runtime, so it uses the GPU on a GPU-backed function.
image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("ffmpeg")
    .pip_install("torch", "torchaudio", "pyannote.audio>=3.1")
)


@app.function(
    image=image,
    gpu="T4",
    secrets=[modal.Secret.from_name("actalux-hf")],
    timeout=60 * 60,
)
def diarize_remote(audio_bytes: bytes, hint_num_speakers: int | None = None) -> list[dict]:
    """Diarize one audio file on the GPU; return ``[{speaker,start,end}, ...]``."""
    import os
    import subprocess
    import tempfile

    import torch
    import torchaudio
    from pyannote.audio import Pipeline

    pipeline = Pipeline.from_pretrained(PYANNOTE_MODEL, token=os.environ["HF_TOKEN"])
    if torch.cuda.is_available():
        pipeline.to(torch.device("cuda"))

    # Decode to 16 kHz mono PCM with ffmpeg, then hand pyannote an in-memory
    # waveform. MP3 frame padding makes pyannote's file cropper miscount samples
    # ("got N instead of expected M"); a decoded tensor has exact sample counts.
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
        waveform, sample_rate = torchaudio.load(wav)

    kwargs = {"num_speakers": hint_num_speakers} if hint_num_speakers else {}
    result = pipeline({"waveform": waveform, "sample_rate": sample_rate}, **kwargs)

    # pyannote 3.x returns an Annotation directly; 4.x wraps it in a DiarizeOutput
    # whose Annotation is at ``.speaker_diarization``. Support both.
    annotation = result if hasattr(result, "itertracks") else result.speaker_diarization
    return [
        {"speaker": label, "start": float(turn.start), "end": float(turn.end)}
        for turn, _, label in annotation.itertracks(yield_label=True)
    ]


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

    def run(self, audio_uri: str, *, hint_num_speakers: int | None = None) -> SpeakerTimeline:
        from actalux.diarization.backend import SpeakerTimeline

        audio_bytes = Path(audio_uri).read_bytes()
        segments = self._fn.remote(audio_bytes, hint_num_speakers)
        return SpeakerTimeline.from_segments(segments, self._model)


@app.local_entrypoint()
def main(audio_path: str, hint_num_speakers: int = 0) -> None:
    """`modal run modal_runner.py --audio-path FILE` — diarize one local file."""
    from actalux.diarization.backend import SpeakerTimeline

    audio_bytes = Path(audio_path).read_bytes()
    segments = diarize_remote.remote(audio_bytes, hint_num_speakers or None)
    timeline = SpeakerTimeline.from_segments(segments, PYANNOTE_MODEL)
    print(f"\nspeakers={timeline.num_speakers}  turns={len(timeline.turns)}")
    for turn in timeline.turns[:10]:
        print(f"  {turn.cluster_label}  {turn.start_s:8.1f} - {turn.end_s:8.1f}s")
