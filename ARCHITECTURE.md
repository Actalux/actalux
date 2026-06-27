# Actalux — Architecture

A citation-first, searchable archive of Clayton, MO public records. Every
AI-generated statement cites a verbatim source quote; closed-session content is
never published; the system is nonpartisan and never adjudicates
baseline-dependent claims. Operated by Actalux LLC. Live at **actalux.org**.

This document is the consolidated architecture overview. Deeper dives live in
`docs/` (`API.md`, `SECRETS.md`, `COSTS.md`, `city-budget-ingest.md`,
`architecture/multi-tenancy.md`, `architecture/orchestration-evaluation.md`).
Design system: `DESIGN.md`. Working rules: `CLAUDE.md`.

---

## 1. System shape

```
        Sources                Ingestion (GitHub Actions)         Store                 Serve
  ┌───────────────────┐    ┌──────────────────────────────┐   ┌──────────────┐   ┌──────────────────┐
  │ Diligent (schools)│    │ crawlers → parser → PII guard │   │  Supabase    │   │ FastAPI + HTMX   │
  │ CivicPlus (city)  │───▶│   → classify → chunker        │──▶│  Postgres    │──▶│  web app (Fly)   │
  │ YouTube (videos)  │    │   → embedder → dedup/version  │   │  + pgvector  │   │  JSON API v1     │
  │ claytonschools.org│    │   → ingest (Supabase)         │   │  (HNSW+FTS)  │   │  reader pane     │
  │ DESE finance XML  │    └──────────────────────────────┘   └──────────────┘   └──────────────────┘
  │ Sunshine requests │            ▲ daily / nightly / weekly crons        │ hybrid retrieval +
  └───────────────────┘            coverage-check cron → GitHub issue       │ rerank + cited answers
```

- **One canonical brain, many jurisdictions.** A single deployment under one
  domain serves every body at `/{state}/{place}/{body}` (e.g.
  `/mo/clayton/council`). Bodies are rows, not forks.
- **Read path is public and RLS-protected; write path is service-key only**, run
  exclusively from CI (no laptop writes to prod).

### Two execution surfaces

Everything runs in one of two places — keeping them straight is the key to the
whole system:

- **(A) The autonomous pipeline — cloud-only, no developer machine.** A meeting
  lands → it gets ingested → everything is built out (transcript, summary,
  chapters, name-corrections, votes, newsletter draft) and the site serves it.
  This is entirely **GitHub Actions** (the crons in §10) plus the **Fly** web app,
  running on cloud secrets. It needs no laptop and no manual trigger; if the Mac
  is offline it still runs.
- **(B) Operator tools — local, manual, never on the serving or ingest path.**
  The evaluation harness (the LLM **judge**, §7) and the latency bench are run by
  hand from a dev machine to *measure* quality. A meeting landing never invokes
  them — they are instruments, not part of "build-out."

## 2. Tech stack

| Layer | Choice |
|---|---|
| Language / tooling | Python 3.11, `uv` (lockfile committed), `ruff` (lint + format) |
| Web | FastAPI + Jinja2 + HTMX (no JS build step) |
| Store | Supabase — Postgres + `pgvector` (project "Actalux Clayton") |
| Embeddings | `BAAI/bge-small-en-v1.5`, 384-dim, local inference, cached model |
| Rerank | ZeroEntropy `zerank-1-small` (Apache-2.0) via hosted API, opt-in |
| LLM (all chat) | **One gateway — OpenRouter, single key.** `openai/gpt-5-mini` (reasoning, minimal) for answers/summaries; `openai/gpt-4o-mini` for the condense/expansion hops; `anthropic/claude-sonnet-4-6` for the eval judge. Direct OpenAI/Anthropic keys are retired (nothing in code reads them). |
| Transcription | Groq `whisper-large-v3` (free tier) — a non-LLM audio service, its own key |
| Secrets | Doppler is the source of truth, synced out to Fly (web) + GitHub Actions (CI); see `docs/SECRETS.md` for the per-secret map and the `actalux`-project consolidation |
| Hosting | Fly.io app `actalux`; Supabase managed Postgres |

## 3. Data model

Base schema: `scripts/setup_db.sql`. Evolution: `scripts/migrate_0NN_*.sql`
(applied via `scripts/apply_migrations.py`, tracked in a `schema_migrations`
ledger). Frozen dataclasses mirror the tables in `src/actalux/models.py`.

### Jurisdiction (multi-body)
- **`places`** — `(state, slug, display_name, county)`, unique on `(state, slug)`.
  Currently one row: `mo/clayton`.
- **`entities`** — a public body under a place: `(place_id, body_slug, type,
  display_name, external_ids jsonb)`, unique on `(place_id, body_slug)`. The four
  Clayton bodies:

  | id | body_slug | type | display_name | sources |
  |----|-----------|------|--------------|---------|
  | 1 | schools | school_district | Clayton School District | Diligent, YouTube, claytonschools, DESE, Sunshine |
  | 2 | council | city_council | Clayton City Council | CivicPlus, YouTube |
  | 3 | plan-commission | plan_commission | Plan Commission & ARB | CivicPlus, YouTube |
  | 4 | board-of-adjustment | board_of_adjustment | Board of Adjustment | CivicPlus, YouTube |

  Adding a body = a seed migration + a `bodies.py` entry + a `crawl_*` category +
  web display dict entries. No schema change, no fork.

### Content
- **`documents`** — `meeting_date, meeting_title, document_type, source_url,
  source_file, content, summary, created_at`, plus provenance/versioning columns
  added by migrations: `content_hash` (SHA-256), `source_portal`
  (`diligent|civicplus|youtube|claytonschools|sunshine|manual|dese`), `source_ref`
  (canonical dedup key), `version` / `replaces_id` (version chain), `entity_id`
  (FK → entities), `video_id` (YouTube), `date_source` (provenance of the date).
- **`chunks`** — verbatim passages: `document_id, content, section, speaker,
  chunk_index, embedding vector(384)`, plus `start_seconds` (video cue point) and
  `citation_id` (stable per-chunk citation token). Indexes:
  - `chunks_embedding_hnsw` — HNSW `vector_cosine_ops` (`m=16, ef_construction=64`);
    `ef_search` raised to 100 (migrate_010) to close a per-body recall hole.
  - `chunks_fts` — GIN `to_tsvector('english', content)`.
- **`votes`** — structured, parsed from official minutes: `document_id,
  meeting_date, motion, result, vote_count_{yes,no,abstain}` (nullable: NULL = no
  per-member count printed, distinct from 0), `details jsonb`, `chunk_id` +
  `citation_id` + `source_quote` (the verbatim passage), `result_basis`
  (`stated` vs `derived` from the roll call). ~2,500 cited votes; every stored
  vote carries a citation.
- **`budget_line_items`** — finance figures with `dimension`, `basis`, and a
  `citation_id` back to the source passage (audited ACFR / adopted budget / DESE).
- **`document_chapters`** — topic chapters for transcripts (timestamped).
- **`speakers`, `corrections`, `ingest_runs`, `sources`, `topic_alerts`** —
  speaker profiles, public correction submissions, run telemetry, the crawl-target
  registry, and email-alert subscriptions.

### Access control & API
- **RLS** (migrate_007): anon/publishable key has read-only access to public
  tables; the service key (CI only) writes. Web app uses the anon key + SECURITY
  DEFINER RPCs.
- **`api_keys` / `api_key_usage`** (migrate_026): per-customer keys (sha256-hashed,
  raw never stored) → tier → quota, authorized by the SECURITY DEFINER
  `api_key_authorize(p_key_hash)` RPC. Dormant until `ACTALUX_API_KEYS=on`.

### Search RPCs
`semantic_search()` (cosine KNN) and `keyword_search()` (FTS) — both
entity-scoped and doctype-filterable (migrate_024/025), superseded-version-aware
(migrate_011).

## 4. Provenance, dedup & versioning

- **Identity ladder** (`scripts/ingest.py`): match an incoming document on
  `source_ref` → `content_hash` → `source_file`. Identical bytes are skipped;
  changed content under a known ref creates a **new version** and sets the old
  row's `replaces_id` (superseded rows drop out of browse + search).
- **`content_hash`** is computed on parsed text, portal-scoped — this is what
  caught cross-body and within-body duplicate PDFs during the Board of Adjustment
  ingest (the city attaches the same/ wrong PDF to multiple meeting entries).
- **Dates** carry a `date_source`; the ingest never invents a date.
- **`source_ref`** canonicalizes the source identity (YouTube URLs are normalized
  so the query-string `?v=` id is preserved — a portal-specific case).

## 5. Ingestion

Pipeline (`src/actalux/ingest/`): **parser → PII guard → classify → chunker →
embedder → dedup/version → insert**.

- **`parser.py`** — PDF (PyMuPDF), HTML (BeautifulSoup), markdown, text; OCR
  fallback (Tesseract) for scanned PDFs; raises on image-only PDFs with no text.
- **`pii_guard.py`** — ingest-time, high-precision SSN/DOB guard. Blocks private
  records **pre-DB** regardless of body (`ACTALUX_PII_GUARD=block` default). "As
  published" never means publishing an SSN.
- **`classify.py`** — shared document-type classifier (ingest + recategorize).
- **`chunker.py`** — section-aware, ~200 words, 2-sentence overlap. (Unpunctuated
  transcripts defeat the sentence-aware splitter — see the caption→Whisper note.)
- **`embedder.py`** — bge-small-en-v1.5, cached.
- **`hashing.py`** — content hash + citation-id assignment + stable doc key.

### Source portals & crawlers

| Portal | Body(ies) | Crawler | Notes |
|---|---|---|---|
| `diligent` | schools | `download_documents.py` | Diligent portal recursive crawl (minutes, budgets, resolutions, calendars) |
| `civicplus` | council, plan-commission, board-of-adjustment | `crawl_civicplus.py` | claytonmo.gov is behind Akamai → real Chrome (Playwright `channel=chrome`) clears it; agendas ingested as **docket text only** (full packet linked via `source_url`); low-confidence dockets quarantined, not clipped. MeetingTypes categories: council 93, PC/ARB 121, BoA 89 |
| `youtube` | all four | `transcribe_meetings.py` + `ingest/transcribe.py` | Whisper transcripts; see §6 |
| `claytonschools` | schools | `crawl_curriculum.py`, `crawl_canva_maps.py`, `crawl_comms.py` | curriculum maps, LRFMP, strategic plan, district communications |
| `dese` | schools | `crawl_clayton_finance.py` + `ingest/*_xml.py` | DESE ASBR / per-pupil / indirect-cost / local-effort XML → cited finance rows |
| `sunshine` | schools | `stage_sunshine.py` | Sunshine-Law records, first-class portal |
| `manual` | any | direct | manually added documents |

Crawlers write manifest JSON to `data/documents/`; `scripts/ingest.py` consumes
them (`--manifest … --body <body>`). `data/` is gitignored.

### Transcription (off-machine, cloud)
`transcribe.yml`: discover @CityofClayton / @SchoolDistrictofClayton videos
filtered to a body by title (`ingest/bodies.py`) → **Cloudflare WARP** (clears
YouTube's datacenter bot-check) + `yt-dlp` (native downloader; never
`--download-sections`) → **Groq whisper-large-v3** → ingest with `video_id` +
per-chunk `start_seconds` from the Whisper segment sidecar → summary + topic
chapters. Idempotent and deduped per `(entity, meeting_date)`.

## 6. Retrieval

`src/actalux/search/`:

1. **Hybrid** (`hybrid.py`) — pgvector cosine KNN + Postgres FTS, top 50 each,
   fused with **Reciprocal Rank Fusion** (`k=60`), final 20, min cosine 0.35,
   entity-scoped.
2. **Query expansion** (optional, `ACTALUX_QUERY_EXPANSION=on`) — a cheap LLM
   (`gpt-4o-mini`) rewrites the query into ~3 alternate phrasings ("did the bond
   pass" → "Proposition O", "bond referendum"); each is embedded and searched
   concurrently; all pools fused before RRF. Best-effort: a failure degrades to
   plain single-query. Reranking always uses the user's original query.
3. **Rerank** (optional, `ACTALUX_RERANK=api`) — ZeroEntropy `zerank-1-small`
   reranks the fused pool (top 50). +24% nDCG@10 in the eval (`eval/README.md`).
4. **Finance routing** (`finance.py`) — finance-figure queries route to the
   structured `budget_line_items` table (cited rows) instead of prose chunks.

## 7. Answer generation

- **`summarize.py` / `answer.py`** — citation-backed summaries with
  **post-verification**: every generated statement must map to a retrieved
  verbatim quote, or it's dropped. Neutrality guardrails (e.g. never adopt "no tax
  rate increase" framing — state the levy rate figures). Summary model
  `openai/gpt-5-mini` (via OpenRouter).
- **Chatbot** (`/ask`, `/ask/stream`) — multi-turn: condense (`openai/gpt-4o-mini`)
  → retrieve → (rerank) → generate, streamed. Per-IP and per-day caps
  (`rate_limit_ask_per_minute=8`, `ask_daily_message_cap=400`).
- **Eval — operator-run measurement (surface B), not a serving/ingest step.**
  `src/actalux/eval/` retrieves + generates an answer on a committed query set,
  then an LLM **judge** (`anthropic/claude-sonnet-4-6`, via OpenRouter) grades each
  result for relevance and answer faithfulness; it also reports retrieval metrics
  (nDCG/MRR/recall) and supports model A/B. The judge is a measuring instrument —
  a stronger model grading the production model's output — run by hand, never by a
  meeting landing. Drove the "keep gpt-5-mini" + reranker decisions.

## 8. Web app

`src/actalux/web/app.py` (+ `api.py`, `charts.py`, `display.py`, `retrieval.py`,
`storage.py`, `text_snippets.py`, `facilities_plan_data.py`).

- **Shell** (per `DESIGN.md`): sticky top bar with always-visible search, sticky
  256px left sidebar, main content, and a **reader pane** that opens to the right
  cued to the citation (source PDF or YouTube embed at the timestamped moment).
  Cited passages use the `#F4E9B0` highlight + 3px vermillion left-border motif.
- **Routing** — data-driven from `entities`:
  - `/{state}/{place}` — place directory (body cards)
  - `/{state}/{place}/{body}` and `/search`, `/browse/{kind}`, `/meetings`,
    `/meeting/{date}`, `/budget[/breakdown|/detail]`, `/facilities-plan`,
    `/summarize`, `/ask[/stream]`, `/methodology`
  - `/document/{id}[/pane]`, `/chunk/{ref}/source[-pane]` — reader-pane targets
  - `/healthz`, apex `/` redirect to the default jurisdiction
  - Per-`type` display dicts (`_NAV_BY_TYPE`, `_MEETING_KIND`, `_BODY_KIND`,
    `_BODY_BLURB`, `_BODY_NOUN`) drive each body's nav/labels; a new body needs an
    entry in each (the missing-entry fallback is what caused the BoA "Facilities
    Master Plan" cross-talk before the entries were added).
- **Server-SVG budget charts** (`charts.py`) — drill-down by fund/function/object/
  source via HTMX, no JS chart lib (ECharts only if a Sankey is ever needed).

## 9. JSON API (`/api/v1`)

`src/actalux/web/api.py` (`docs/API.md`). Read-only endpoints (votes, search,
etc.) over the same store. Monetization-ready but **dormant**:

- No key + `ACTALUX_API_KEYS` unset → "anonymous" tier (no auth DB call;
  unchanged public behavior).
- A presented key, when `ACTALUX_API_KEYS=on` → per-IP auth throttle (before any
  RPC) → `api_key_authorize` RPC → tier + monthly quota. The global
  `ACTALUX_API_KEY` is the "admin" tier.
- Tiers (`config.API_TIERS`): anonymous / developer / pro / admin, each with
  search-per-min, general-per-min, monthly-quota. Keys minted by
  `scripts/issue_api_key.py` (sha256 stored; tier restricted to developer|pro).
- **No billing/Stripe plumbing** — metering only.

## 10. Automation (GitHub Actions)

| Workflow | Schedule (UTC) | Does |
|---|---|---|
| `transcribe.yml` | nightly, 1 body/fire (06:07/06:22/06:37/06:52) | new meeting-video transcripts (all 4 bodies) |
| `crawl_minutes.yml` | **daily**, 1 body/fire (07:00/07:20/07:40) | CivicPlus agendas + minutes (council, PC, BoA), then re-extracts that body's votes |
| `ingest.yml` | weekly (Mon) | Diligent (school-board) documents, then re-extracts school-board votes |
| `coverage_check.yml` | daily 08:30 (after crawls) | flags recent meetings missing minutes → upserts **one** GitHub issue, auto-closes when cleared |
| `civicplus_probe.yml` | dispatch | source-structure probe |

- Every ingest/transcribe/coverage job runs `apply_migrations.py --check` at
  startup. **A repo migration not yet applied to prod breaks the nightly jobs** —
  apply migrations before/with the deploy.
- GitHub delays scheduled runs (~hours); times are nominal. Crons are staggered
  and use separate `concurrency` groups so crawl and transcribe never race.

## 11. Security & content policy

- **Secrets** (`docs/SECRETS.md`, names-only register): source of truth is the
  Doppler **`actalux`** project, synced out to GitHub Actions + Fly. **One LLM key
  (OpenRouter)** now covers every chat model — the OpenAI and Anthropic keys are
  retired (dead in code). Non-LLM service keys stay separate (Groq = Whisper audio,
  ZeroEntropy = reranker, Supabase = DB). Never inline a secret value; web uses the
  anon key + RLS; the service key is CI-only and never reaches Fly.
- **PII guard** blocks SSN/DOB pre-DB for every body.
- **Content policy** (`CLAUDE.md`): every AI statement cites a verbatim quote;
  closed/executive session content never published; nonpartisan, no editorializing,
  no inferred intent, never adjudicate baseline-dependent claims. Schools: board/
  admin policy only (no individual personnel/teachers/students). City bodies: the
  full public record as published (officials, applicants, the subject property,
  hearing participants), redaction only via the universal PII guard.
- Never claim completeness of the record — "public records" / "documents we have
  gathered."

## 12. Deployment

- **Migrations:** `scripts/apply_migrations.py` (Supabase Management API,
  `SUPABASE_PAT`); `--check` verifies applied, bare applies pending; ledger in
  `schema_migrations`.
- **Web:** `fly deploy` (app `actalux`); the running app uses the anon key + RPCs,
  so most changes need no new secrets. Opt-in features are env flags on Fly
  (`ACTALUX_RERANK`, `ACTALUX_QUERY_EXPANSION`, `ACTALUX_API_KEYS`).
- **Order:** commit → apply migrations to prod → deploy → smoke-test
  (anon API 200, site 200). Free-tier Supabase auto-pause is mitigated by a
  keepalive cron (upgrade to Pro when traffic warrants).

## 13. Repository layout

```
src/actalux/
  config.py            models.py   db.py   errors.py
  ingest/   parser, pii_guard, classify, chunker, embedder, hashing, docket,
            bodies, to_markdown, transcribe, youtube,
            votes_parser(+_civicplus), {asbr,perpupil,indirect_cost,local_effort}_xml
  search/   hybrid, rerank, finance, summarize, answer
  eval/     harness, judge, metrics, rerank, answer_quality
  digest/   change_digest, drafter, themes, delivery   (email change-digests, Buttondown)
  web/      app, api, charts, display, retrieval, storage, text_snippets, facilities_plan_data
scripts/    setup_db.sql, migrate_0NN_*.sql, apply_migrations.py, ingest.py,
            crawl_*.py, download_documents.py, transcribe_meetings.py,
            issue_api_key.py, check_minutes_coverage.py, backfill_*.py
.github/workflows/  transcribe, crawl_minutes, ingest, coverage_check, civicplus_probe
docs/       API, SECRETS, COSTS, SCALING, city-budget-ingest, architecture/*
```

## 14. Related downstream project

**Clayton Ledger** (`Actalux/clayton-ledger`) — the cited-newsletter layer
("article-writing layer only"). A read-only consumer of the actalux JSON API;
every factual claim traces to an actalux source. Named ordinances/resolutions in
articles link to the meeting's **minutes** document (the authoritative adopted
record), since the city publishes no standalone resolution archive.
