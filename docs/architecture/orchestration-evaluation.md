# Orchestration & agent-framework evaluation

_Last updated 2026-06-17. Reframed from an external brief that assumed Actalux
runs on Vercel — it does not. Actalux is **FastAPI + HTMX/Jinja2 on Fly.io**
(single machine, region `ord`) with **Supabase** (managed Postgres + pgvector),
local bge-small embeddings, OpenAI summaries, and the ZeroEntropy reranker.
There is no Vercel/Next.js anywhere in the stack._

## TL;DR / verdict

**Do not adopt a new orchestration platform or agent framework right now.**
The work we actually want — *watch the sources of truth → auto-ingest new
documents → draft a Substack post → human review → publish* — is mostly already
built, and the missing half can be added on the infrastructure we already run
for free. A heavyweight durable-workflow engine or agent framework would add
always-on services to solve a problem that is currently a low-volume weekly
batch.

- **Vercel Eve → NO-GO** (permanently, for this stack). It launched 2026-06-17,
  is Apache-2.0, but is **TypeScript-only, no Python SDK, and "at launch
  deploys to Vercel"** (off-platform support "on the way"). Wrong shape on every
  axis for a Python/FastAPI/Fly app.
- **Agent frameworks (LangGraph/CrewAI/OpenAI Agents SDK/PydanticAI) → not
  needed now.** Actalux's citation-verified RAG core is already in-house and is
  the product's value. If we ever want tool-calling agents, **PydanticAI** is the
  lightest typed fit — but agent frameworks are *not* durable workflow engines.
- **Durable workflow engines (Temporal/Restate/Hatchet/Inngest) → defer.** If we
  ever need crash-resumable, long-lived human-in-the-loop pipelines, the
  lightest path is an **in-process, Postgres-native** option (**DBOS** library or
  **Procrastinate**) on the Supabase Postgres we already pay for — not a separate
  cluster. Of the standalone engines, **Hatchet** is the best fit (Postgres-only,
  Python-first, MIT); reserve it/Windmill for if the pipeline truly outgrows
  in-process.

## What we already have (verified 2026-06-17)

| Capability | Status | Where |
|---|---|---|
| Scheduled source monitoring + ingest | **Built & running** | `.github/workflows/ingest.yml` |
| Dedup / versioning of changed docs | **Built** | `scripts/ingest.py` (`content_hash`, `replaces_id`) |
| Ingest run log | **Built** | `ingest_runs` table |
| Substack-grounding read API | **Built** | `src/actalux/web/api.py` (read-only JSON v1) |
| Substack drafter | **Missing** | — |
| "New docs → draft" trigger | **Missing** | — |
| YouTube / Canva auto-refresh | **Missing (by design)** | manual; see below |

`ingest.yml` is a **GitHub Actions cron — Mondays 09:00 UTC** (plus manual
dispatch) — that crawls **Diligent, curriculum, and Clayton finance**, then runs
`ingest.py` with `content_hash` dedup and version chaining (new/changed docs land,
superseded rows get `replaces_id`). It is schema-gated (fails if migrations are
pending) and is **genuinely running**: the last five scheduled runs all succeeded
(most recent 2026-06-15, ~2m42s). Board documents change ~monthly, so a weekly
crawl keeps the corpus current without hammering district servers — on
GitHub-hosted runners, at zero Fly cost.

**So the "monitor + auto-ingest" half is solved.** The gap is the *downstream*
half: nothing reads "what changed this run" and drafts anything, and there is no
Substack drafter at all (only the grounding API).

### The one structural exception: YouTube + Canva

These two sources are deliberately **not** in the cron, for a real reason
documented in `ingest.yml`: **YouTube blocks datacenter IPs** (GitHub runners)
and **Canva needs a browser daemon**. That is a *hosting* problem (need a
non-datacenter IP / a headless browser), **not** an orchestration-framework
problem, and should be solved independently — e.g. a small scheduled Fly machine
(or a residential-egress proxy) running `crawl_youtube.py` / `crawl_canva_maps.py`.
No workflow engine changes this.

## The real question

Given the backbone exists, the only genuine decision is how to build:

```
ingest run detects new/changed docs
        │
        ▼
  change digest  ──►  substack_drafter (reuse generate_summary, cited + nonpartisan)
        │
        ▼
   DRAFT post  ──►  human review gate  ──►  publish to Substack
                    (NEVER auto-publish — content policy)
```

Two properties separate every candidate tool, and they are the only ones that
matter for us:

1. **Can it run on the existing Postgres with no Redis / no new always-on
   service?** (We want fewest moving parts on one Fly machine.)
2. **Does it natively pause-for-approval-and-resume?** (The human-review gate.)

## Candidate fit matrix

"In-process" = runs inside the FastAPI process, no separate service.

| Tool | Class | PG-only, no Redis | In-process | Durable multi-step | Native approval pause/resume | License | Fit |
|---|---|:--:|:--:|:--:|:--:|---|---|
| **Procrastinate** | PG task queue | ✅ | ✅ | ⚠️ defer-next-step | ⚠️ DB-status gate | MIT | **Strong** |
| **DBOS** | durable lib | ✅ | ✅ | ✅ | ✅ (await/notify) | MIT‡ | **Strong (verify)** |
| APScheduler | scheduler | ✅ | ✅ | ❌ | ❌ | MIT | Possible (cron only) |
| Hatchet | durable engine | ✅ | ❌ (multi-container) | ✅ | ✅ (`wait_for` event) | MIT | Possible (later) |
| Windmill | workflow platform | ✅ | ❌ (~6 services) | ✅ (visual) | ✅ (suspend/approve URL†) | AGPLv3 | Possible (later) |
| Prefect | orchestrator | ✅ (single-server) | ❌ | ✅ | ✅ (pause/suspend + UI form) | Apache-2.0 | Possible, heavy |
| Restate | durable engine | n/a (own RocksDB) | ❌ (sidecar) | ✅ | ✅ (awakeables) | **BSL 1.1** | Possible, lightest container |
| Temporal | durable engine | ⚠️ (PG ok, multi-role) | ❌ | ✅ (best-in-class) | ✅ (signals) | MIT | Overkill |
| Inngest | durable engine | ❌ (needs Redis) | ❌ | ✅ | ✅ (`wait_for_event`) | Fair-source→Apache | Weak (self-host) |
| arq | Redis task queue | ❌ | ❌ | ❌ | ❌ | MIT (maint-only) | Weak |
| Dramatiq | task queue | ⚠️ 3rd-party PG broker | ❌ | ⚠️ pipelines | ❌ | LGPL | Weak |
| Celery | task queue | ❌ (DB broker unsupported) | ❌ | ✅ canvas | ❌ | BSD | Overkill |
| Dagster | data orchestrator | ✅ (core) | ❌ | ✅ | ❌ (no native approval) | Apache-2.0 | Overkill |
| Eve | agent framework | n/a | ❌ (TS, Vercel) | ✅ (Workflow SDK) | ✅ | Apache-2.0 | **No-Go** (TS/Vercel) |
| PydanticAI | agent framework | n/a | ✅ | ❌ (needs Temporal) | ❌ | MIT | If agents ever needed |

† Windmill's suspend/approve-via-URL is in the AGPL community edition; the rich
approval *form* is Enterprise-gated. ‡ DBOS license/approval specifics are from a
single research pass — verify before committing (see Confidence & gaps).

## Recommended architecture (phased)

### Phase 0 — now: finish the pipeline on the infra we already run

No new framework. The weekly ingest cron *is* the trigger.

1. **Emit a change digest** from each ingest run — read `ingest_runs` /
   newly-created-or-superseded `documents` to produce "what's new/changed this
   run."
2. **Add `substack_drafter`** — a module that consumes the digest and reuses
   `generate_summary` (already citation-verified: drops uncited sentences,
   abstains when nothing grounds) to produce a **draft**. Same content rules as
   the site: every claim cited, nonpartisan, no editorializing, board/admin
   policy only.
3. **Run it as a follow-on step in `ingest.yml`** after ingest — "new docs landed
   → draft."
4. **Human-review gate, never auto-publish** (hard content-policy line): write the
   draft to a review surface — a GitHub issue/PR, an email, or a Substack *draft*
   via API — and a human publishes.

```
GitHub Actions (weekly cron)
  └─ crawl ─ ingest ─ change-digest ─ substack_drafter ─► review surface ─► human ─► publish
Fly machine (FastAPI/HTMX): serves site + the read-only JSON API (grounding)
Small scheduled Fly worker (separate): YouTube/Canva crawlers (non-datacenter IP / browser)
```

### Phase 1 — only if/when we move off weekly-batch

If we want near-real-time drafting, in-app triggering, or resumable
human-in-the-loop with real retries, add an **in-process, Postgres-native** layer
— **Procrastinate** (cron + durable tasks + defer-next-step; approval = a
`pending_approval` row resumed from an HTMX route) or **DBOS** (durable-workflow
library with built-in await/notify). Both reuse Supabase Postgres and add **zero
new services**. Caveat: Procrastinate's `LISTEN/NOTIFY` needs a session-mode/
direct Postgres connection (not Supabase's transaction-mode pooler).

### Phase 2 — only if it outgrows in-process

A separate platform earns its keep only at higher volume / multi-district / a
UI-driven approval queue: **Windmill** (durable visual flows + native
suspend/approve, Postgres-only) for a built-in review UI, or **Hatchet**
(Postgres-only, Python-first, MIT) for heavier durable orchestration. Avoid
Temporal/Inngest/Celery here (operational weight / Redis).

## Risks & what would change the recommendation

- **Volume growth (many districts).** Multi-tenant, high-frequency ingest with
  many concurrent pipelines is where a real durable engine (Hatchet) or platform
  (Windmill) starts to pay off. Until then it's premature.
- **Long human pauses (days).** A DB-status gate handles this fine; a platform's
  UI just makes it nicer. Not a reason to adopt one yet.
- **Real-time expectation.** If "draft within minutes of a doc appearing" becomes
  a requirement, the weekly-cron trigger no longer fits → Phase 1.
- **Auto-publish pressure.** Resist. Content policy requires a human gate; no
  framework choice should remove it.

## Confidence & gaps

- **High confidence:** current-state facts (verified in repo + Actions history);
  Eve's nature/licensing/launch (multiple independent sources); the Postgres-only
  vs Redis split per tool; Procrastinate/APScheduler/Prefect/Hatchet/Temporal
  specifics.
- **Lower confidence (verify before committing):** **DBOS** specifics (license,
  in-process model, built-in approval) come from a single research pass — do a
  focused spike before relying on it. Exact current version numbers for Dramatiq,
  Celery, Windmill, Dagster were not all pinned; Dagster's "no native approval"
  and "no Redis" are best-effort from search, not deep doc verification.

## Sources

- Eve: github.com/vercel/eve · vercel.com/docs/eve · vercel.com/blog/introducing-eve · thenewstack.io (Eve launch) · workflow-sdk.dev (self-host/portable)
- Durable engines: docs.temporal.io/self-hosted-guide/visibility · github.com/restatedev/restate · github.com/hatchet-dev/hatchet · inngest.com/docs/self-hosting
- Lightweight tier: github.com/procrastinate-org/procrastinate · apscheduler.readthedocs.io · docs.prefect.io/v3/advanced/interactive · windmill.dev/docs/flows/flow_approval · python-arq/arq · dramatiq.io · docs.celeryq.dev · docs.dagster.io
- Agent frameworks: ai.pydantic.dev/durable_execution · github.com/openai/openai-agents-python · docs.crewai.com · llamaindex.ai (Workflows 1.0) · diagrid.io ("Checkpoints are not durable execution")
