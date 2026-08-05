"""Modal serverless-GPU adapter: self-hosted zerank-1-small reranking.

ZeroEntropy's hosted endpoint shuts down 2026-09-04. The weights it serves —
``zeroentropy/zerank-1-small-reranker`` — are Apache-2.0, so this runs the *same
model* rather than substituting a different one. Quality therefore carries over
by construction: the eval measured nDCG@10 0.889 against a 0.72 RRF baseline for
this checkpoint, and nothing here changes the scoring. Load parameters are taken
from ``actalux.eval.rerank`` deliberately, so the self-hosted arm cannot silently
drift from the arm that was measured.

(``zerank-2`` is not an option regardless of quality: CC-BY-NC-4.0, and Actalux
is an LLC.)

Reranking is interactive, which makes this differ from the WhisperX adapter in
one important way. That one is a plain function because a nightly batch job can
absorb a cold start; a search request cannot — ``search/rerank.py`` gives up after
10 s and falls back to RRF order, so a cold container would silently serve worse
results rather than fail visibly. The model is therefore loaded in
``@modal.enter()``, once per container rather than once per call, and the weights
live in a Volume so a cold start does not re-download 1.7B of checkpoint from
Hugging Face.

The remote container loads this module to find the class and must not import
``actalux`` — the domain imports stay local-only, same rule as the WhisperX
adapter.

Deploy (Modal tokens from Doppler ``actalux``):

    MODAL_TOKEN_ID="$(doppler secrets get MODAL_TOKEN_ID --plain --project actalux --config dev)" \
    MODAL_TOKEN_SECRET="$(doppler secrets get MODAL_TOKEN_SECRET --plain --project actalux --config dev)" \
    uv run --group diarization modal deploy src/actalux/search/modal_rerank.py
"""  # noqa: E501

from __future__ import annotations

import modal

APP_NAME = "actalux-rerank"
MODEL_REPO = "zeroentropy/zerank-1-small-reranker"

# Mirrors actalux.eval.rerank so the hosted arm scores identically to the measured
# one. zerank-1-small is a causal-LM cross-encoder computing full-vocab logits, so
# it needs a small predict batch to stay inside memory.
RERANK_MAX_LENGTH = 1024
RERANK_DTYPE = "auto"
# Smaller than the eval registry's 8, and the difference is not cosmetic. This is a
# causal-LM cross-encoder that computes full-vocab logits, so one forward pass holds
# a [batch, 1024, ~152k] tensor — at batch 8 that is tens of GB of logits against a
# 3.4 GB model, and it OOMs a 24 GB L4 outright. The registry's 8 was tuned where
# the model was partly on CPU; resident on GPU the activations, not the weights, set
# the ceiling. `score` takes an override so the bench can sweep this without a
# redeploy (an env var would be read at container import, which is too early).
PREDICT_CHUNK = 2
# Passages scored per predict() call, with the CUDA cache dropped between slices.
# This, not batch_size, is what actually bounds peak memory across a deep pool.
SCORE_SLICE = 20

# Where the Volume is mounted, and the HF cache pointed at it.
CACHE_DIR = "/cache"

app = modal.App(APP_NAME)

image = (
    modal.Image.debian_slim(python_version="3.11")
    # accelerate is not optional: the checkpoint's own modelling code loads with a
    # `device_map`, and transformers refuses that without it.
    .pip_install("sentence-transformers", "torch", "transformers", "einops", "accelerate")
    # trust_remote_code pulls the checkpoint's own modelling code at load time.
    .env(
        {
            "HF_HOME": CACHE_DIR,
            "HF_HUB_ENABLE_HF_TRANSFER": "1",
            # Full-vocab logit tensors fragment the allocator badly between calls.
            "PYTORCH_CUDA_ALLOC_CONF": "expandable_segments:True",
        }
    )
)

# Cold start is the whole design problem here, and re-downloading the checkpoint
# is the largest part of it. The Volume keeps the weights across containers.
weights = modal.Volume.from_name("actalux-rerank-weights", create_if_missing=True)


@app.cls(
    image=image,
    gpu="L4",
    volumes={CACHE_DIR: weights},
    # L4 for the same reason the transcription path chose it: cheapest tier whose
    # VRAM clears this model comfortably. Sized by measurement, not preference —
    # see scripts/bench_modal_rerank.py.
    scaledown_window=300,
    timeout=120,
)
class Reranker:
    """One resident zerank-1-small, scoring (query, passage) pairs on GPU."""

    @modal.enter()
    def load(self) -> None:
        """Load once per container. Everything expensive belongs here, not in score()."""
        import torch
        from sentence_transformers import CrossEncoder

        self.model = CrossEncoder(
            MODEL_REPO,
            trust_remote_code=True,
            max_length=RERANK_MAX_LENGTH,
            # Pin the whole model to the GPU. Left to itself the checkpoint loads
            # with device_map="auto", which split it across CPU and GPU here — 1.7B
            # in bf16 is ~3.4 GB against the L4's 24 GB, so the split buys nothing
            # and costs a host<->device transfer on every forward pass. Measured at
            # depth 100 it was the difference between seconds and milliseconds.
            model_kwargs={"dtype": RERANK_DTYPE, "device_map": {"": 0}},
        )
        # Constructing the CrossEncoder does NOT load the weights: this checkpoint
        # ships its own modelling code, which defers the real load to the first
        # predict() call. Without this throwaway scoring pass the weights would land
        # on whichever search request arrived first, so a "warm" container would
        # still pay the full cold cost once — the exact failure a warm container is
        # bought to prevent, and an invisible one.
        self.model.predict([("warmup", "warmup passage")], show_progress_bar=False)
        # A silent fp32 fallback would roughly double resident memory and change
        # latency, so the loaded precision is asserted rather than assumed.
        self.dtype = str(next(self.model.model.parameters()).dtype)
        self.device = str(self.model.device)
        torch.cuda.empty_cache()

    @modal.method()
    def score(self, query: str, passages: list[str], batch_size: int | None = None) -> list[float]:
        """Relevance score per passage, aligned to ``passages``.

        Only the induced *order* is used in production, so absolute values need
        not be comparable across models — but they are returned unmodified rather
        than pre-sorted, so the caller keeps the mapping back to its own objects.
        """
        if not passages:
            return []
        import torch

        bs = batch_size or PREDICT_CHUNK
        out: list[float] = []
        # Sliced deliberately, rather than handing the whole pool to predict() and
        # trusting batch_size to bound memory — it does not. A 100-passage pool OOMs
        # a 24 GB L4 even at batch 2, because the full-vocab logits accumulate across
        # the call instead of being freed per batch. Production reranks a fused pool
        # of about this size, so the failure would have been the common case, not an
        # edge one. Slicing with an explicit cache drop keeps peak memory flat in the
        # depth.
        for start in range(0, len(passages), SCORE_SLICE):
            window = passages[start : start + SCORE_SLICE]
            scores = self.model.predict(
                [(query, p) for p in window], batch_size=bs, show_progress_bar=False
            )
            out.extend(float(s) for s in scores)
            torch.cuda.empty_cache()
        return out

    @modal.method()
    def health(self) -> dict:
        """Where the weights actually landed, not where they were asked to land.

        The checkpoint loads itself through its own modelling code with a
        ``device_map``, so sentence-transformers' ``.device`` describes the wrapper
        and can read ``cpu`` while the scorer sits on the GPU — or while it really
        is on the CPU, silently paying for an idle L4. The only trustworthy answer
        is where a scoring parameter physically is, so that is what is reported.
        """
        import torch

        placements: set[str] = set()
        for module in (getattr(self.model, "model", None), self.model):
            try:
                placements |= {str(p.device) for p in module.parameters()}
            except (AttributeError, TypeError):
                continue
        return {
            "model": MODEL_REPO,
            "wrapper_device": self.device,
            "dtype": self.dtype,
            "parameter_devices": sorted(placements),
            "cuda_available": torch.cuda.is_available(),
            "cuda_device_count": torch.cuda.device_count(),
        }
