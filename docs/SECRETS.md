# Secrets inventory

A register of the credentials Actalux uses and **where each one lives**. The goal
is a single source of truth that propagates everywhere, so a key is set (or
rotated) once rather than juggled across stores.

**This file contains secret _names_ only — never values.** Values live exclusively
in their stores. Last reviewed 2026-06-27.

## Source of truth & sync (target)

- **Source of truth: the Doppler `actalux` project (config `dev`).** Every Actalux
  runtime secret is set here, once.
- It **syncs outward** via Doppler's native integrations:
  - **Doppler `actalux` → Fly app `actalux`** (the live web app — read path only).
  - **Doppler `actalux` → GitHub Actions `Actalux/actalux`** (the ingest /
    transcribe / coverage crons).
- **Local dev reads the same project**: `doppler run --project actalux --config dev -- …`.

A runtime store can't be eliminated (Fly, GitHub Actions, and local dev are
separate runtimes that each read their own injected env) — but with the sync
integrations there is one *source* and the rest are push targets.

> **Current state (sync wiring pending):** the integrations are created in the
> Doppler **dashboard** via OAuth (the CLI cannot create them) — an operator step.
> Until that's done, Fly and GitHub secrets are still set manually (see the table),
> and local dev/manual scripts historically used `--project mac`. The `actalux`
> project already holds the LLM + Whisper + rerank keys; the Supabase keys still
> need to be copied in for it to be a complete source.

### `mac` Doppler is NOT Actalux's source of truth

The Doppler **`mac`** project is the operator's **shared, general** local-dev
project (Canvas, Todoist, Zotero, Wolfram, and *general* `OPENAI_API_KEY` /
`ANTHROPIC_API_KEY` / `OPENROUTER_API_KEY` used by unrelated tools). Do **not**
treat those general LLM keys as "Actalux duplicates" to delete — other tooling
reads them. Only the Actalux-scoped names there (`ACTALUX_*`, `GROQ_ACTALUX_API_KEY`)
relate to this project.

## Stores

| Store | What it serves | Naming |
|-------|----------------|--------|
| **Doppler `actalux/dev`** | Source of truth → local dev + sync targets | `ACTALUX_*` / `OPENROUTER_ACTALUX_KEY` / `ACTALUX_GROQ` / `ACTALUX_ZE` |
| **Fly.io** app `actalux` | The live web app only (**read** path; never the service key) | `ACTALUX_*` / provider names verbatim |
| **GitHub Actions** repo `Actalux/actalux` | `ingest.yml`, `transcribe.yml`, `crawl_minutes.yml`, `coverage_check.yml` | Supabase secrets currently use **short** names (`SUPABASE_URL`) mapped to `ACTALUX_*` in the workflow `env:` block; others use their full name |
| **Doppler `mac/dev`** | Operator's shared general local-dev project (legacy Actalux home) | mixed |

## Secrets

| Purpose | Source name (`actalux`) | GitHub | Fly | Sensitivity / notes |
|---------|-------------------------|--------|-----|---------------------|
| Supabase project URL | `ACTALUX_SUPABASE_URL` | `SUPABASE_URL` | `ACTALUX_SUPABASE_URL` | Public endpoint; infra-identifying. |
| Supabase **publishable** (anon) key | `ACTALUX_SUPABASE_KEY` | `SUPABASE_KEY` | `ACTALUX_SUPABASE_KEY` | RLS-enforced; safe for the public web app. |
| Supabase **service** key | `ACTALUX_SUPABASE_SERVICE_KEY` | `SUPABASE_SERVICE_KEY` | — (**never on Fly**) | **High.** Bypasses RLS. Writers only (ingest / backfills / cron). |
| Supabase Management **PAT** | `ACTALUX_SUPABASE_PAT` | `SUPABASE_PAT` | — | **High.** DDL migrations via the Management API (`apply_migrations.py`). |
| **OpenRouter** API key (all chat LLM) | `OPENROUTER_ACTALUX_KEY` | `OPENROUTER_ACTALUX_KEY` | `OPENROUTER_ACTALUX_KEY` | **High.** The single gateway: summaries, Ask, condense, query expansion, transcript chapters, the newsletter draft, **and** the eval judge — all via `openai/*` + `anthropic/*` model ids. |
| Groq API key (Whisper) | `ACTALUX_GROQ` | `GROQ_ACTALUX_API_KEY` | — | **High.** Board-meeting *audio* transcription (not a chat model → not on OpenRouter). |
| ZeroEntropy API key (reranker) | `ACTALUX_ZE` | — | `ZEROENTROPY_API_KEY` | **High.** Hosted cross-encoder reranker (web + retrieval eval). A separate service, not on OpenRouter. |
| ~~OpenAI API key~~ | — | `OPENAI_API_KEY` *(dead)* | `OPENAI_API_KEY` *(dead)* | **Retired.** Nothing in code reads it (LLM is OpenRouter). Slated for removal from Fly + GitHub. |
| ~~Anthropic API key~~ | — | — | — | **Retired.** The eval judge now reaches Sonnet via OpenRouter; no direct Anthropic key needed by Actalux. |
| Actalux JSON API key | `ACTALUX_API_KEY` *(unset)* | — | — | Optional. Unset ⇒ read-only JSON API is open (still rate-limited). |
| Newsletter / SMTP (Clayton Ledger) | `GHOST_*`, `MAILGUN_ACTALUX_*` | — | — | Downstream-project delivery; lives in `actalux` Doppler. |

Config flags carried as env/secrets (not credentials, no rotation): `ACTALUX_RERANK`
(Fly = `api`), `ACTALUX_QUERY_EXPANSION` (Fly = `on`), `ACTALUX_PII_GUARD`,
`ACTALUX_API_KEYS`, `ACTALUX_SITE_BASE_URL`.

### Non-breaking name migration

`config.py` reads each consolidated name with the legacy name as a fallback, so
moving a secret is non-breaking:

- `OPENROUTER_ACTALUX_KEY` → falls back to `OPENROUTER_API_KEY`
- `ACTALUX_GROQ` → falls back to `GROQ_ACTALUX_API_KEY`
- `ACTALUX_ZE` → falls back to `ZEROENTROPY_API_KEY`

Once the `actalux`-project sync is live and verified, the legacy-named copies on
Fly/GitHub (and the dead `OPENAI_API_KEY`) can be dropped and the fallbacks retired.

## Sync topology constraint: the service key must never reach Fly

The web app is read-only and must **never** receive `ACTALUX_SUPABASE_SERVICE_KEY`
or `ACTALUX_SUPABASE_PAT`. A naive whole-config sync to Fly would push them, so the
Fly target must be scoped to the read subset (URL + anon key + OpenRouter + ZE +
the feature flags) — either via per-sync secret selection in the Fly integration or
a dedicated read-only config. The GitHub target gets the full set (it runs the
writers). Decide this when wiring the integrations.

## Safe write pattern (no value in the transcript)

```bash
doppler secrets set NAME="$(doppler secrets get NAME --plain --project SRC --config dev)" \
  --project actalux --config dev > /dev/null
gh secret set NAME --repo Actalux/actalux --body "$(doppler secrets get NAME --plain --project actalux --config dev)"
fly secrets set NAME="$(doppler secrets get NAME --plain --project actalux --config dev)" -a actalux --stage
```

The value only ever passes through command substitution; `> /dev/null` suppresses
Doppler's confirmation echo.

## Rotation plan (when Actalux's own accounts exist)

Most keys are still issued under the operator's personal/shared accounts. To rotate:

1. Create the replacement under the **Actalux-owned** account (Supabase org,
   OpenRouter, Groq, ZeroEntropy).
2. Set the new value **once** in Doppler `actalux/dev`; the sync propagates it to
   Fly + GitHub. (Until the sync is wired, update each store with the safe pattern
   above.)
3. Redeploy / dispatch to confirm: a Fly deploy for the web app; a `transcribe.yml`
   / `ingest.yml` run for CI.
4. **Revoke the old key** at the provider once the new one is confirmed working.

Priority order: the two Supabase secret keys (service key, PAT) and the
OpenRouter / Groq / ZeroEntropy keys first (high blast radius), then the
publishable key + project URL, then optional/unset ones.
