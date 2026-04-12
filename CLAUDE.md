# Actalux

Citation-first searchable archive of Clayton, MO school district public records. 501(c)(3) nonprofit.

## Content policy

- Every AI-generated statement must cite a verbatim source quote
- Board and administration policy only — no individual personnel, teachers, or students
- Closed session content is never published
- Actalux is nonpartisan — no mention of PropO bond campaign, MEC complaints, or advocacy content
- No editorializing, no opinions, no inferred intent

## Tech stack

- Python 3.11+, uv, ruff
- FastAPI + HTMX/Jinja2 (no JS build step)
- Supabase (PostgreSQL + pgvector) — project "Actalux Clayton"
- bge-small-en-v1.5 (384-dim embeddings, local inference)
- Claude Sonnet for citation-backed summaries (anthropic SDK pinned to 0.93.0)
- Secrets in Doppler (project: mac, config: dev)

## Architecture

```
src/actalux/
  config.py       — env var config (Supabase, Anthropic, embedding model)
  models.py       — frozen dataclasses: Document, Chunk, Vote, Speaker, etc.
  db.py            — all Supabase operations, version-aware queries
  errors.py        — typed exceptions
  ingest/
    parser.py      — PDF (PyMuPDF), HTML (BeautifulSoup), markdown, text
    chunker.py     — section-aware chunking, ~200 words, 2-sentence overlap
    embedder.py    — bge-small-en-v1.5, cached model
    hashing.py     — SHA-256 content hash for change detection
    to_markdown.py — pymupdf4llm PDF→markdown with frontmatter
  search/
    hybrid.py      — semantic + keyword search, RRF (k=60)
    summarize.py   — citation-backed LLM summaries with post-verification
  web/
    app.py         — FastAPI routes
    templates/     — Jinja2 + HTMX

scripts/
  setup_db.sql              — full schema (run in Supabase SQL editor)
  migrate_001_provenance.sql — provenance columns migration
  ingest.py                 — hash-based dedup, manifest ingestion
  download_documents.py     — Diligent portal recursive crawl
  crawl_curriculum.py       — Clayton schools website PDF/Google Doc crawler
  crawl_canva_maps.py       — Canva curriculum maps via headless browser
  crawl_youtube.py          — YouTube board meeting discovery
  backfill_provenance.py    — one-off provenance backfill
```

## Document provenance

Every document has:
- `content_hash` — SHA-256 for change detection
- `source_portal` — one of: `diligent`, `claytonschools`, `youtube`, `manual`
- `source_url` — where it came from
- `version` / `replaces_id` — version chain for updated documents

Dedup is by `source_file` (filename only, portal-agnostic). When content changes, a new version is created and the old row gets `replaces_id` set to the new one's ID.

## Crawlers and ingestion

Crawlers write manifest JSON files to `data/documents/`. Ingestion reads them:
```bash
doppler run --project mac --config dev -- uv run python scripts/ingest.py data/documents/
doppler run --project mac --config dev -- uv run python scripts/ingest.py --manifest data/documents/diligent_manifest.json
```

All commands require `doppler run` for env vars.

## Source portals

| Portal | Content | Crawler |
|--------|---------|---------|
| `diligent` | Minutes, budgets, resolutions, calendars | `download_documents.py` (root: `16823c15-705e-49b3-b2b5-ee1fe9d381fb`) |
| `claytonschools` | Curriculum maps, LRFMP, strategic plan, assessment guides | `crawl_curriculum.py`, `crawl_canva_maps.py` |
| `youtube` | Board meeting transcripts | `crawl_youtube.py` (channel: @SchoolDistrictofClayton) |
| `manual` | Manually added documents | Direct file placement |

## Search

Hybrid retrieval: pgvector cosine similarity + PostgreSQL FTS, combined with reciprocal rank fusion (k=60). Top 50 candidates from each path, final 20 results. Minimum similarity threshold: 0.35.

## Database

Supabase project "Actalux Clayton". Schema in `scripts/setup_db.sql`. Key tables: `documents`, `chunks` (with HNSW vector index), `votes`, `speakers`, `corrections`, `ingest_runs`, `sources`.

RPC functions: `semantic_search()`, `keyword_search()`.

## Testing

```bash
uv run python -m pytest tests/
uv run ruff check . && uv run ruff format --check .
```

## Git

- Branch: master (not main)
- Conventional commits: feat:, fix:, docs:, refactor:, test:
- No Co-Authored-By lines
- Push to Actalux/actalux on GitHub

## Design System

Always read `DESIGN.md` before making any visual or UI decisions. All font
choices, colors, spacing, layout patterns, and interaction patterns are
defined there. Do not deviate without explicit user approval.

Key constraints pulled from DESIGN.md:

- **Fonts:** Fraunces (display), Newsreader (body), Geist (UI), IBM Plex
  Mono (citations). Never use Inter, Roboto, Arial, Helvetica, Poppins,
  Montserrat, Raleway, or Clash Display.
- **Accent:** Vermillion `#C8553D`. Used sparingly for highlights, active
  states, primary CTA hover. Never for body text or decorative fill.
- **Layout:** App shell — sticky top bar (with always-visible search),
  sticky 256px left sidebar (collapsible nav sections), main content
  area. Reader pane opens to the right when a result is clicked, with
  the source document or YouTube embed cued to the citation.
- **Cited passage highlight:** `background: #F4E9B0` + 3px vermillion
  left-border. This is the product's core visual motif. Apply
  consistently wherever a cited passage appears in source context.
- **No:** border-radius, icon libraries, gradients, purple, civic-blue,
  modals for primary flows, skeleton shimmer, scroll-triggered animation.
- **Content accuracy:** Never claim completeness of the record. Use
  phrases like "public records" or "documents we have gathered."
  Sunshine Law records are a first-class source portal (`source_portal =
  "sunshine"`).

In QA and code review, flag any code that doesn't match DESIGN.md.
Reference the specific section of DESIGN.md when flagging.
