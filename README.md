# Actalux

Citation-first, searchable archive of Clayton, Missouri public records. Every
AI-generated statement is backed by a verbatim source quote — no unconstrained
summaries, no editorializing.

Live at **[actalux.org](https://actalux.org)**.

Independent and nonpartisan. Actalux surfaces the public records it has
gathered; it does not claim to hold the complete record, and it does not
advocate, adjudicate, or infer intent.

## Coverage

| Body | Source | Content |
|------|--------|---------|
| School District of Clayton — Board of Education | Diligent, district website, YouTube | Minutes, budgets, resolutions, curriculum maps, facilities plan, meeting transcripts |
| City of Clayton — City Council | CivicPlus, YouTube | Minutes, ordinances, budgets/CIP, ACFRs, meeting transcripts |
| City of Clayton — Plan Commission / ARB | CivicPlus, YouTube | Minutes, meeting transcripts |

Records are also surfaced through Sunshine-Law requests (`source_portal = "sunshine"`)
as a first-class portal.

## Principles

- **Every AI statement cites a verbatim quote.** Summaries are post-verified
  against the records they cite.
- **Nonpartisan.** No advocacy, campaign, or complaint framing; baseline-dependent
  claims (e.g. tax/levy "increase vs. no increase") are reported as figures, never
  adjudicated.
- **Closed/executive-session content is never published.**
- **An ingest-time PII guard** blocks SSN/DOB-class records before they reach the
  database, regardless of body.
- **School coverage is board and administration policy only** — no individual
  personnel, teachers, or students. City coverage is the full public record as the
  body published it (public officials, land-use applicants, hearing participants).

## Stack

- Python 3.11+, [uv](https://docs.astral.sh/uv/), ruff
- FastAPI + HTMX/Jinja2 (no JS build step)
- Supabase (PostgreSQL + pgvector)
- `bge-small-en-v1.5` (384-dim embeddings, local inference)
- Claude Sonnet for citation-backed summaries
- Deployed on Fly.io; secrets in Doppler

## Architecture

```
src/actalux/
  config.py     env-driven config
  models.py     frozen dataclasses (Document, Chunk, Vote, Speaker, …)
  db.py         all Supabase operations, version-aware queries
  ingest/       PDF/HTML/markdown parsing, section-aware chunking, embedding,
                content hashing, PII guard, deterministic vote parsing
  search/       hybrid retrieval (pgvector + FTS, RRF) + reranking,
                citation-backed summarization with post-verification
  web/          FastAPI app, HTMX templates, read-only JSON API
scripts/        crawlers, ingestion, vote extraction, migrations
```

Retrieval is hybrid: pgvector cosine similarity + PostgreSQL full-text search,
fused with reciprocal rank fusion, optionally reranked by a cross-encoder. Vote
extraction is deterministic (not LLM): structured, cited board-vote records are
parsed from the verbatim minutes, and a record that cannot be cited to a passage
is skipped rather than stored uncited.

## Setup

```bash
uv sync
cp .env.example .env   # fill in Supabase credentials
```

All runtime commands expect environment variables (Supabase, Anthropic, embedding
model) to be present; in development they are injected via Doppler.

## Tests

```bash
uv run python -m pytest tests/
uv run ruff check . && uv run ruff format --check .
```

## JSON API

A read-only `v1` API mirrors the site's retrieval over already-public records, so
it can never expose more than the site does. All endpoints are entity-scoped under
`/api/v1/{state}/{place}/{body}/…`:

| Endpoint | Returns |
|----------|---------|
| `GET …/search?q=` | Ranked verbatim passages with citations and source links |
| `GET …/meetings/{date}` | All documents for one meeting date |
| `GET …/recent?since=` | Recent meeting documents (a "what's new" feed) |
| `GET …/votes?since=` | Structured, cited board-vote records |

Example: `GET /api/v1/mo/clayton/council/votes`. Every endpoint is rate-limited;
key-based authentication is optional and configurable.

## Contributing

Issues and pull requests are welcome — corrections to the record, parser edge
cases, and coverage gaps especially. Please keep the content principles above in
mind. By submitting a contribution you agree it is provided under the project's
license.

## License

Source-available under the **Business Source License 1.1** (BUSL-1.1) — see
[LICENSE](LICENSE). Non-production use (reading, evaluation, development, and
testing) is free for everyone. **Any production use — by any individual,
company, nonprofit, or government/municipal entity — requires a commercial
license from Actalux LLC.** Each released version automatically converts to the
GNU Affero General Public License v3.0-or-later four years after its release.

For a commercial or government/municipal license, email
[admin@actalux.org](mailto:admin@actalux.org) or open an issue.
