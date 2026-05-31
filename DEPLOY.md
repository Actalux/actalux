# Deploying Actalux

The web app (FastAPI + HTMX) runs as a single always-on container on
[Fly.io](https://fly.io). Search is hybrid retrieval with RRF; the reranker is
**not** in this image (it's a future ZeroEntropy API call — see the bottom of
this file). The data lives in Supabase, so the app itself is effectively
stateless and cheap to move or rebuild.

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
grep -E '^(ACTALUX_SUPABASE_URL|ACTALUX_SUPABASE_KEY|ANTHROPIC_API_KEY)=' | \
fly secrets import
```

Only `ACTALUX_SUPABASE_URL` and `ACTALUX_SUPABASE_KEY` are required at startup.
`ANTHROPIC_API_KEY` powers the citation-backed summaries; add `OPENAI_API_KEY`
and `BUTTONDOWN_API_KEY` to the `grep` set if/when those features are live.
`ACTALUX_PII_GUARD` is ingest-only and not needed on the web host.

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

The on-disk **image** is large (~17.5 GB) because torch pulls its CUDA Linux
build — see "CPU-only torch" below, which is the remaining pre-production task.

## Notes / follow-ups

- **Supabase free-tier resume.** The free tier pauses on inactivity; the
  existing keepalive cron still applies. A live, always-on app with real
  traffic largely keeps the database warm on its own.
- **CPU-only torch — REQUIRED before production (still open).** torch pulls its
  CUDA Linux build plus the `nvidia-cu13` libraries (cuDNN, cuSPARSELt, NCCL,
  nvSHMEM) — together the bulk of the ~17.5 GB image — even though this host has
  no GPU. A CPU build cuts the image to ~2–3 GB.

  The clean declarative fix (a `pytorch-cpu` index + `[tool.uv.sources]` torch
  override) **silently no-ops on the repo's uv (0.8.15)**: torch's latest is
  2.12.0, the PyTorch CPU index can't satisfy that exact version (the `+cpu`
  local-version mismatch), so uv falls back to PyPI's CUDA wheel with zero
  warning. Verified 2026-05-30 — the lock came back with 0 references to the CPU
  index. Working options, in rough order of cleanliness:

  1. **Upgrade uv** (newer releases support markers in index sources) and use a
     Linux-scoped source:
     `torch = [{ index = "pytorch-cpu", marker = "sys_platform == 'linux'" }]`.
  2. **Pin torch** to a version the CPU index actually carries (e.g. an explicit
     `torch==2.x.y` that exists at download.pytorch.org/whl/cpu) alongside the
     explicit index source, then `uv lock`.
  3. **Install in the Dockerfile** via `uv export --frozen` → `uv pip install
     --torch-backend cpu -r requirements.txt`, keeping the lock CUDA-flavored
     but overriding torch's backend at image-build time.

  This is **repo-wide**, not deploy-only: torch is also the eval reranker's
  dependency (`src/actalux/eval/rerank.py`), so changing its version/build
  affects that path too — pick the option and validate the embedder still
  produces identical vectors (same model, CPU build → numerically equivalent)
  AND that the eval reranker still loads before committing the lock change.
- **Reranker (Phase 2).** Not in this image. When wired, it will be an optional
  `ACTALUX_RERANK=off|api` stage calling the ZeroEntropy hosted API after
  `hybrid_search`, default off (RRF fallback). The API serves the zerank-2
  family, not the locally-evaluated zerank-1-small, so it gets its own eval-arm
  validation first.
