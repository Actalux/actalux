# Deploying Actalux

The web app (FastAPI + HTMX) runs as a single always-on container on
[Fly.io](https://fly.io). Search is hybrid retrieval (RRF) with an optional
ZeroEntropy reranker stage, gated by `ACTALUX_RERANK` (see the bottom of this
file). The data lives in Supabase, so the app itself is effectively stateless
and cheap to move or rebuild.

Artifacts in the repo:

- `Dockerfile` — uv-based image; bakes the bge-small query-embedding model in.
- `.dockerignore` — keeps `.venv`, `data/`, `eval/`, `tests/` out of the image.
- `fly.toml` — 1 GB always-on machine in `ord` (Chicago), `/healthz` check.

## Prerequisites

- A Fly.io account and `flyctl` (`brew install flyctl`, then `fly auth login`).
- The Doppler project `mac` / config `dev` holding the app's secrets.

## First deploy

```bash
# From the repo root. --no-deploy creates the app + uses our fly.toml/Dockerfile
# without deploying yet, so we can set secrets first.
fly launch --no-deploy
```

Accept the existing `fly.toml` when prompted. Pick a real app name (it replaces
the `actalux` placeholder) and keep the `ord` region.

Then push the secrets the app reads. `fly secrets import` takes `KEY=VALUE`
lines on stdin, so the values flow through the pipe and never appear in your
shell history or on a command line:

```bash
doppler secrets download --no-file --format env \
  --project mac --config dev | \
grep -E '^(ACTALUX_SUPABASE_URL|ACTALUX_SUPABASE_KEY|OPENAI_API_KEY|ZEROENTROPY_API_KEY)=' | \
fly secrets import
```

Only `ACTALUX_SUPABASE_URL` and `ACTALUX_SUPABASE_KEY` (the publishable key)
are required at startup. `OPENAI_API_KEY` powers the citation-backed summaries
(`gpt-5-mini` via the OpenAI SDK) -- omit it and the summary feature silently
disables itself. Swap it for a dedicated, spend-capped key before relying on it
in public. `ZEROENTROPY_API_KEY` plus `ACTALUX_RERANK=api` turn on the reranker
stage (default off → RRF only); set the flag with
`fly secrets set ACTALUX_RERANK=api`. Add `BUTTONDOWN_API_KEY` to the `grep` set
if/when that feature is live. `ANTHROPIC_API_KEY` is eval-only (the judge), not
used by the web app; `ACTALUX_PII_GUARD` is ingest-only -- neither is needed on
the web host.

Deploy:

```bash
fly deploy
```

Check it:

```bash
fly status
fly logs
curl -fsS https://<your-app>.fly.dev/healthz   # -> {"status":"ok"}
```

## Redeploying

```bash
fly deploy
```

Rotate or change a secret the same way as the import above (or
`fly secrets set NAME=value` for a one-off — note that puts the value on the
command line). Setting a secret triggers a rolling restart.

## Sizing

The image carries torch + bge, so resident memory is ~1 GB. The machine starts
at 1 GB; if you see OOM restarts under load (`fly logs` shows the kill), bump it:

```bash
fly scale memory 2048
```

The on-disk image is ~1.8 GB (torch is the bulk at ~620 MB; see "CPU-only
torch" below for how the CUDA stack is kept out). Note: `docker images` may
report ~4 GB for a buildx image because of attestation/manifest-list
double-counting — `docker image inspect` and the deployed footprint are the
real ~1.8 GB.

## Notes / follow-ups

- **Supabase free-tier resume.** The free tier pauses on inactivity; the
  existing keepalive cron still applies. A live, always-on app with real
  traffic largely keeps the database warm on its own.
- **CPU-only torch — how `requirements-cpu.txt` works.** torch would otherwise
  pull its CUDA Linux build plus the `nvidia-cu13` libraries (cuDNN, cuSPARSELt,
  NCCL, nvSHMEM) — together ~13 GB this GPU-less host never uses. The image
  installs from `requirements-cpu.txt`, a CPU-pinned dependency set compiled
  for the Linux deploy target, so torch resolves to `2.12.0+cpu` with no CUDA.

  This is a separate artifact from `uv.lock` by necessity: torch 2.12 declares
  the `nvidia-cu13` packages as Linux-marked *dependencies*, so the declarative
  `[tool.uv.sources]` index override can't strip them from the lock, and uv's
  `--torch-backend` flag exists only on `uv pip` (install/compile), not on
  `uv lock`/`uv sync`. `uv.lock` stays CUDA-flavored and governs dev — on macOS
  torch is CPU-only regardless, so dev is unaffected; the eval reranker keeps
  its `uv.lock` torch. Only the Linux image needs the override.

  **Regenerate `requirements-cpu.txt` whenever `pyproject.toml` deps change:**

  ```bash
  uv pip compile pyproject.toml \
    --torch-backend=cpu \
    --python-platform=x86_64-unknown-linux-gnu \
    --python-version=3.11 \
    -o requirements-cpu.txt
  ```

  (The compile picks the latest compatible torch, so the deploy may run a newer
  torch patch than `uv.lock`'s — both are torch 2.x with the same bge inference
  behavior. Pin torch in `pyproject.toml` if you need them identical.)
- **Reranker.** Live. An optional `ACTALUX_RERANK=off|api` stage that calls the
  ZeroEntropy hosted API (model `zerank-1-small`) to rerank the RRF candidate
  pool after `hybrid_search`. Default `off` (RRF fallback); enabled on the host
  via `ACTALUX_RERANK=api` + `ZEROENTROPY_API_KEY`. See `eval/README.md` for the
  retrieval eval that selected the model (+~24% nDCG@10).
