# Actalux web app — FastAPI + HTMX, served by uvicorn.
#
# Single-stage image built with uv. Dependencies install in their own layer so
# they cache across source-only changes. The bge-small embedding model is baked
# in at build time so the first search pays no HuggingFace download and the box
# needs no network at boot.
#
# Reranking is handled by the ZeroEntropy hosted API, not in this process, so no
# large reranker model loads here. Note: torch (via sentence-transformers) still
# resolves to its CUDA Linux wheel, which makes the image large; slimming it to a
# CPU-only torch is the remaining pre-production step — see DEPLOY.md
# "CPU-only torch". Fly's remote builder produces an amd64 image on `fly deploy`.

FROM python:3.11-slim

# uv: copy the static binary from the official image, pinned to the version the
# repo builds with locally (uv.lock was produced by 0.8.15).
COPY --from=ghcr.io/astral-sh/uv:0.8.15 /uv /uvx /bin/

# Create the non-root user up front so the virtualenv it builds is owned
# correctly from the start. A post-hoc `chown -R /app` would copy the whole
# multi-GB venv into a second image layer, doubling the image.
RUN useradd --create-home --uid 10001 app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    HF_HOME=/home/app/.cache/huggingface \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app
RUN chown app:app /app
USER app

# Resolve and install dependencies first (cached unless lock/pyproject change).
# --no-dev drops pytest/ruff/accelerate; --no-install-project defers the app
# itself to the next layer so dependency caching survives source edits.
COPY --chown=app:app pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Copy the application and install the package.
COPY --chown=app:app . .
RUN uv sync --frozen --no-dev

# Bake bge-small (the query-embedding model) into the image.
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('BAAI/bge-small-en-v1.5')"

EXPOSE 8080
CMD ["uvicorn", "actalux.web.app:app", "--host", "0.0.0.0", "--port", "8080"]
