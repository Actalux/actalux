# Actalux web app — FastAPI + HTMX, served by uvicorn.
#
# Single-stage image built with uv. Dependencies install from a CPU-pinned
# requirements file (requirements-cpu.txt) so torch resolves to its CPU build
# instead of the ~13 GB CUDA stack this GPU-less host never uses. bge-small is
# baked in so the first search needs no network. Fly's remote builder produces
# an amd64 image on `fly deploy` — the platform requirements-cpu.txt targets.
#
# Regenerate requirements-cpu.txt when pyproject deps change — see DEPLOY.md.

FROM python:3.11-slim

# uv: copy the static binary from the official image, pinned to the repo's uv.
COPY --from=ghcr.io/astral-sh/uv:0.11.17 /uv /uvx /bin/

# Create the non-root user up front so the venv it builds is owned correctly
# from the start; a post-hoc `chown -R /app` would copy the whole venv into a
# second image layer.
RUN useradd --create-home --uid 10001 app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1 \
    UV_PYTHON_DOWNLOADS=never \
    VIRTUAL_ENV=/app/.venv \
    HF_HOME=/home/app/.cache/huggingface \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app
RUN chown app:app /app
USER app

# Install the CPU-pinned dependency set (no CUDA libraries). --torch-backend=cpu
# points uv at the PyTorch CPU index so the +cpu torch wheel resolves; the file
# pins every version. Own layer, cached unless requirements-cpu.txt changes.
COPY --chown=app:app requirements-cpu.txt ./
RUN uv venv && uv pip install --torch-backend=cpu -r requirements-cpu.txt

# Install the application package itself (its deps are satisfied above).
COPY --chown=app:app . .
RUN uv pip install --no-deps .

# Bake bge-small (the query-embedding model) into the image.
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('BAAI/bge-small-en-v1.5')"

EXPOSE 8080
CMD ["uvicorn", "actalux.web.app:app", "--host", "0.0.0.0", "--port", "8080"]
