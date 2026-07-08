# Actalux

Citation-first searchable archive of Clayton, MO school district public records. Independent and nonpartisan. Operated by Actalux LLC; running as an LLC for the foreseeable future (a 501(c)(3) nonprofit is not planned). Do not describe Actalux as a nonprofit or 501(c)(3) in any user-facing copy.

## Scale across municipalities (cardinal rule)

Clayton is the first jurisdiction, not the only one. **Everything must be built to scale across many municipalities** — no feature, table, crawler, or piece of data may be Clayton-specific in a way that blocks adding another town.

- **Jurisdiction-scope all data.** Anything town-specific carries a `place_id` (or resolves through `state`/`place`); routes are `/{state}/{place}/...` and `/{state}/{place}/{body}/...`. Never hardcode `clayton`/`mo` in schema, queries, search, or business logic — resolve the place from the request/config.
- **Per-place config, not constants.** Town-specific inputs (rosters, corrections, portal IDs, channels) live in per-place files like `scripts/roster/<state>_<place>.json`, keyed and loaded by place — never inlined.
- **Watch for cross-jurisdiction collisions.** The same string can be correct in one town and a mangling in another (e.g. a name-correction `merrimack → Meramec` valid in Clayton must not apply to a town that has a real "Merrimack"). Scope every lookup to its place.
- When a change *can't* be made jurisdiction-agnostic, stop and surface the tradeoff rather than hardcoding.

## Architectural decisions default to scalability (cardinal rule)

When an architectural fork has no clear winner, **default to the more scalable
option** — the one that stays correct and cheap as the corpus, jurisdictions,
bodies, and people grow — even when it costs more up front. Scalability is the
tie-breaker, and often the decider.

- **Structural over procedural.** Prefer a guarantee enforced by the schema/types
  (a constraint, a separate row, a trigger) over one that depends on a code path
  staying correct (a guard a future change can regress). The integrity that
  matters at scale is the integrity the database enforces, not the convention.
- **One mechanism over two.** Prefer a single uniform model over special-casing;
  parallel mechanisms diverge and breed bugs as things multiply.
- **Reversible over one-way.** Prefer designs where a wrong decision (a bad merge,
  a misclassification) is undone by a cheap repoint, not destructive surgery —
  because at scale, wrong decisions are a *when*, not an *if*.
- **Pay the one-time cost now, while the corpus is tiny.** A restructuring that is
  cheap today (few rows, regenerable derived data) is expensive after expansion.
  "Do it now" beats "retrofit later" for anything load-bearing.
- This generalizes the municipalities cardinal above: jurisdiction-scoping is one
  instance of defaulting to scale. When the scalable path can't be taken, **stop
  and surface the tradeoff** rather than quietly choosing the convenient one.

## Content policy

Universal (all bodies):
- Every AI-generated statement must cite a verbatim source quote
- Closed/executive session content is never published
- Nonpartisan — no advocacy, campaign, or complaint framing; never adjudicate
  baseline-dependent claims (e.g. tax/levy "increase vs no increase")
- No editorializing, no opinions, no inferred intent
- The ingest-time PII guard (SSN/DOB and similar) blocks records pre-DB
  regardless of body — "as published" never means publishing an SSN
- **Named-in-transcript ≠ tracked entity — two different things.** (a) *Naming a
  turn in one transcript* attributes a passage to a speaker from their own
  self-identification ("my name is …") or an on-the-record introduction. (b) A
  *tracked entity* is a persistent, cross-meeting record of a person — a
  person/subject row and a voiceprint gallery used to recognize them in future
  meetings. **Only tracked officials get (b).** Everyone else who is nameable is
  named per-document only: no persistent entity, no voiceprint, no cross-meeting
  linkage. The speaker's own words are the source for (a); (b) additionally
  requires that the person be an official of that body. Self-identification is
  necessary but never sufficient for (b), and the per-body protected classes
  below can forbid even (a).

School district (mo/clayton/schools):
- **Tracked entities: board and administration only.** Only elected board members
  and district administration (superintendent, cabinet) get a persistent
  cross-meeting record + voiceprint.
- **Never named at all: individual personnel, teachers, students** (protects
  minors and employees). This overrides self-identification — a district employee
  who self-identifies ("I work in the counseling office") is still not named.
- **Named-in-transcript only: public participants.** A member of the public giving
  comment, an outside presenter, a contractor, or a City/other-body official
  appearing before the board may be identified *in that transcript* when they
  state their own name or are introduced on the record — but is never tracked (no
  entity, no voiceprint). The protected class above always wins over this.

City government (mo/clayton/council, mo/clayton/plan-commission, …):
- The full public record as the body published it — public officials, land-use
  applicants, the subject property, and hearing participants appear as they do
  in the official minutes; no redaction beyond the universal PII guard.
- Tracked entities + voiceprints are for the body's own officials; other named
  participants (applicants, presenters, members of the public) are named
  per-record only, per the universal tracked-vs-named rule above.

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
    pii_guard.py   — ingest-time high-precision PII guard (SSN/DOB); blocks private records pre-DB
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

Hybrid retrieval: pgvector cosine similarity + PostgreSQL FTS, combined with reciprocal rank fusion (k=60). Top 50 candidates from each path, final 20 results. Minimum similarity threshold: 0.35. A ZeroEntropy cross-encoder reranks the fused pool when `ACTALUX_RERANK=api`.

Optional query expansion (`ACTALUX_QUERY_EXPANSION=on`, off by default) widens recall when the query's wording differs from the records': a cheap LLM rewrites the query into a few alternate phrasings ("did the bond measure pass" → "Proposition O", "bond referendum"), each is embedded and searched alongside the original, and all candidate pools are fused before RRF + rerank. The variant searches run concurrently and are best-effort — a failure degrades to plain single-query retrieval. Reranking always uses the user's original query.

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
