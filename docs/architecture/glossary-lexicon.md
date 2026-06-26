# Glossary / lexicon unification — Actalux ⇄ Clayton Ledger

**Status:** built 2026-06-26 (task #67). The recommended direction below was
implemented: Actalux owns the canonical lexicon (`GET /api/v1/{state}/{place}/lexicon`,
migration 032) and the Clayton Ledger consumes it (`clayton_ledger.lexicon_sync` →
committed cache → `glossary.py`). The "Decisions made" section records how each open
question was resolved during the build.

## Problem

Two repos independently maintain "what is the correct spelling of this Clayton proper
noun, and what are its known manglings":

- **Actalux** (`scripts/roster/mo_clayton.json` → `subjects` / `subject_aliases`):
  the authoritative roster of officials, built from the human-prepared minutes, with a
  resolver (`graph/resolve.py`) that maps a raw vote-roll name to a subject via
  normalized aliases. Source of canonical *people*; its manglings are **OCR-of-minutes**.
- **Clayton Ledger** (`src/clayton_ledger/glossary.py`): two hand-maintained dicts —
  `CANONICAL_NAMES` (~150 correct names, grouped: members, staff, schools, facilities,
  streets, businesses, commenters) and `NAME_CORRECTIONS` (~150 `mangling → canonical`
  fixes). `validate.find_name_issues` runs the corrections, word-boundaried, against the
  newsletter's *own* prose (verbatim quotes are exempt). Its manglings are
  **ASR-of-YouTube-captions** (the ledger drafts from transcripts, which mangle more than
  minutes do). The ledger is already a read-only **consumer of the actalux.org API**
  (`ActaluxClient`).

The operator's framing: "probably better to maintain only once," and the plumbing
"should live in actalux rather than the newsletter."

## What overlaps, what doesn't

| Glossary content | Actalux home | Overlap |
|---|---|---|
| **People** (board / council / BoA / PC members) | `subjects` (roster) | **High** — same people; ledger lists a current subset, Actalux holds the full historical roster. |
| **Person manglings** | `subject_aliases` | Conceptually identical, but **source-specific**: Actalux=OCR-of-minutes, ledger=ASR-of-captions. Different error distributions. |
| **Staff / administrators** (non-voting) | none | Actalux tracks only members (people who vote/move/second), not staff. |
| **Streets, businesses, facilities, places** | none yet | **Phase 3** (places & orgs) territory; no Actalux subject type for them today. |
| **One-off public commenters, operator-confirmed ambiguities** | none | Inherently ledger-local / per-article. |

Minor canonical divergences to reconcile during a build: e.g. ledger **"Jeffery Yorg"**
vs Actalux roster **"Jeff Yorg"** (the ledger's is operator-confirmed against the city's
2026 roster and is likely the better current canonical).

## Recommendation — Actalux owns the canonical lexicon; the ledger consumes it

This matches the existing one-way data flow (ledger already pulls from the Actalux API)
and the principle that the records archive, not the newsletter, is the authority on who
the officials are.

1. **People (first build):**
   - Actalux stays the source of truth (it already is).
   - Add a read-only **lexicon endpoint**, e.g. `GET /api/v1/{state}/{place}/lexicon`,
     returning per subject `{canonical_name, kind, body, aliases: [raw forms], current}`.
     It exposes the full roster (historical + current); the consumer filters.
   - The ledger replaces the *people* half of `glossary.py` with a **cached build-time
     sync** from that endpoint (cache committed so builds stay offline-stable), keeping
     only ledger-specific people (staff, one-off commenters).

2. **Non-person names (streets / businesses / places): defer to Phase 3.**
   Keep them ledger-local for now. When Actalux models place/org subjects (connections
   graph Phase 3), they join the same lexicon endpoint. Do **not** build a parallel
   lexicon table now — it would be throwaway once Phase 3 lands.

3. **Feedback loop (the ongoing maintain-once win):** when the ledger drafter discovers a
   *new* person mangling (its `NAME_CORRECTIONS` grows constantly), it **proposes the
   mangling back to Actalux** as a `subject_alias` (tagged provenance, e.g. `asr`) rather
   than only adding it locally. `DRAFTING.md` already forbids parallel drafting agents
   from editing the glossary directly — the report path becomes "propose an Actalux
   alias." Canonical people then live in exactly one place.

### Why not merge the mangling lists wholesale

Because the two corpora make *different errors*. Actalux resolves names off
CivicPlus-OCR'd minutes; the ledger fixes Google ASR captions. Folding every ledger
mangling into Actalux's resolver aliases could introduce false matches against
minutes-derived names that never appear that way in minutes. So: **unify the canonical
names; keep manglings provenance-tagged** and let each consumer apply the set that fits
its source. The lexicon API carries aliases with provenance so this stays honest.

## Decisions made (build, 2026-06-26)

- **Canonical reconciliation:** the operator-confirmed current-roster spelling wins,
  recorded once in Actalux. `Jeff Yorg` (minutes) → `Jeffery Yorg` (city's 2026 roster),
  with `Jeff Yorg`/`Yorg` kept as aliases and the subject **slug pinned** (`jeff-yorg`)
  so the rename did not orphan his vote/motion edges (94 records preserved). Four current
  PC/BoA members the archive had not minted (never a mover/seconder in the record) were
  added to the roster per operator decision — they carry no motions, so their dossiers
  show the honest "no cited record yet" empty state.
- **Alias provenance model:** reused the existing `subject_aliases.source` column instead
  of adding a redundant `provenance` column (two columns meaning "origin" is a smell). The
  endpoint reports `source` on every variant; it is uniformly `'roster'` today, with
  `ocr`/`asr`/`reviewed` reserved for the feedback loop. **Do not blind-merge manglings:**
  the ledger's ASR caption manglings were NOT copied into Actalux's OCR-of-minutes resolver.
- **Endpoint shape & auth:** place-scoped `GET /api/v1/{state}/{place}/lexicon` under the
  same dormant-key/tier model + rate limit as the other `/api/v1` endpoints (a person on
  two bodies is one entry with both memberships, so it cannot be body-scoped). Reads
  through the anon/RLS path; migration 032 opens `subject_aliases` to anon for publishable
  subjects only (the resolution queue stays denied).
- **Sync cadence in the ledger:** build-time pull + committed cache
  (`src/clayton_ledger/data/actalux_lexicon.json`), refreshed with
  `python -m clayton_ledger.lexicon_sync`. Offline-stable; deterministic write (sorted +
  trailing newline) so a re-sync with no upstream change is a no-op diff. `glossary.py`
  reads the cache at import; only the flattened union of canonical names is consumed.

## Scope boundary

The people half is **built** (2026-06-26): lexicon endpoint + migration 032 + ledger
sync. Still deferred: the **feedback loop** (the ledger proposing newly-seen ASR
manglings back to Actalux as provenance-tagged aliases) and the **non-person half**
(streets, businesses, places), which waits on connections-graph Phase 3.
