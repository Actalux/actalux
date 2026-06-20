# Secrets inventory

A register of the credentials Actalux uses and **where each one lives**, so they
can all be rotated when Actalux moves onto its own accounts (currently several
live under the operator's personal/shared accounts and the `mac` Doppler project).

**This file contains secret _names_ only — never values.** Values live exclusively
in their stores below. Verified 2026-06-20.

## Stores

| Store | What it serves | How names map |
|-------|----------------|---------------|
| **Doppler** project `mac`, config `dev` | Local dev + all manual scripts (`doppler run --project mac --config dev -- …`). Source of truth for local work. | Uses the `ACTALUX_*` / provider names verbatim. |
| **GitHub Actions** secrets, repo `Actalux/actalux` | The `ingest.yml` (weekly) and `transcribe.yml` (nightly) workflows. | Supabase secrets use **short** names (`SUPABASE_URL`); the workflow `env:` block maps them to the `ACTALUX_*` vars the code reads. Others keep their full name. |
| **Fly.io** app `actalux` (`fly secrets list`) | The live web app only (read path). | Uses the `ACTALUX_*` / provider names verbatim. The web app never gets write credentials. |

## Secrets

| Purpose | Doppler `mac/dev` | GitHub `Actalux/actalux` | Fly `actalux` | Sensitivity / notes |
|---------|-------------------|--------------------------|---------------|---------------------|
| Supabase project URL | `ACTALUX_SUPABASE_URL` | `SUPABASE_URL` | `ACTALUX_SUPABASE_URL` | Not secret (public endpoint), but infra-identifying. |
| Supabase **publishable** (anon) key | `ACTALUX_SUPABASE_KEY` | `SUPABASE_KEY` | `ACTALUX_SUPABASE_KEY` | RLS-enforced; safe for the public web app. Low-sensitivity but still rotate. |
| Supabase **service** (secret) key | `ACTALUX_SUPABASE_SERVICE_KEY` | `SUPABASE_SERVICE_KEY` | — (deliberately not on web) | **High.** Bypasses RLS. Writers only (ingest / backfills / cron). |
| Supabase Management **PAT** | `ACTALUX_SUPABASE_PAT` | `SUPABASE_PAT` | — | **High.** DDL migrations via the Management API (`apply_migrations.py`). |
| OpenAI API key | `OPENAI_API_KEY` | `OPENAI_API_KEY` | `OPENAI_API_KEY` | **High.** Summaries, transcript chapters, Ask chatbot, query expansion, condense. |
| Groq API key (Whisper) | `GROQ_ACTALUX_API_KEY` | `GROQ_ACTALUX_API_KEY` | — | **High.** Board-meeting transcription (cloud). Namespaced separately from other Groq use. |
| ZeroEntropy API key (reranker) | `ZEROENTROPY_API_KEY` | — | `ZEROENTROPY_API_KEY` | **High.** Hosted cross-encoder reranker (web + retrieval eval). |
| Anthropic API key | `ANTHROPIC_API_KEY` | — | — | **High.** Present for model A/B; not on a live request path today (summaries/Ask use OpenAI). |
| OpenRouter API key | `OPENROUTER_API_KEY` | — | — | **High.** Offline synthesis A/B in `eval/` only; not used in production. |
| Actalux JSON API key | `ACTALUX_API_KEY` *(unset)* | — | — | Optional. Unset ⇒ the read-only JSON API is open (still rate-limited). Set to require `X-API-Key`. |
| Buttondown API key | `BUTTONDOWN_API_KEY` *(unset)* | — | — | Optional newsletter integration; currently unused. |
| SMTP host / user / password / from / to | `ACTALUX_SMTP_*` *(unset)* | `ACTALUX_SMTP_*` *(unset)* | — | Optional. Digest-email delivery in `ingest.yml`; unset ⇒ the drafter writes the draft file but does not send. |

Non-secret config flags also set as env/secrets (no rotation needed): `ACTALUX_RERANK`
(Fly = `api`), `ACTALUX_QUERY_EXPANSION` (Fly = `on`), `ACTALUX_PII_GUARD`,
`ACTALUX_SITE_BASE_URL`.

## Rotation plan (when Actalux's own accounts exist)

Each key below is currently issued under a personal/shared account. To rotate one:

1. Create the replacement under the **Actalux-owned** account (Supabase org, OpenAI
   org, Groq, ZeroEntropy, and ideally a dedicated Doppler project, e.g. `actalux`).
2. Update every store the secret appears in (see the table): Doppler, GitHub, Fly.
   - Doppler: `doppler secrets set NAME='…' --project mac --config dev` (move to an
     `actalux` project when one exists).
   - GitHub: `gh secret set NAME --repo Actalux/actalux --body "$(…)"`.
   - Fly: `fly secrets set NAME=… -a actalux` (triggers a redeploy).
3. Re-run / redeploy: a Fly deploy for the web app; a `transcribe.yml` /`ingest.yml`
   dispatch to confirm the workflows still pass.
4. **Revoke the old key** at the provider once the new one is confirmed working.

Rotate in this priority order: the two Supabase secret keys (service key, PAT) and
the OpenAI / Groq / ZeroEntropy keys first (high blast radius), then the publishable
key and project URL, then the optional/unset ones if/when they are adopted.
