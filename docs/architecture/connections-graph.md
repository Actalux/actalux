# Connections Graph — scope & plan

**Status:** scoping — **build-ready after the fourth-round fixes folded in below**
(four review rounds: adversarial agent + codex). Owner: Actalux.
**Goal:** add a citation-backed entity-link layer over the corpus so the archive
can answer *"tell me about X"* (a person, place, matter, or organization) — not
just *"find documents about X."*

Native build chosen over importing gbrain (one store, one provenance chain — see
`ARCHITECTURE.md` and the gbrain comparison). We borrow the typed-graph idea + the
cheap zero-LLM number extraction; we keep **citation-first on every asserted
edge**, which gbrain (a private brain with heuristic edges) does not need.

> **Review history.** Draft → review 1 (both reviewers flagged the same blockers:
> version-chain interaction, edge uniqueness, entity resolution, privacy gate,
> missing metric) → revision → review 2 (substantive; a second, narrower round of
> schema-level blockers) → tightening → **review 3** (both reviewers: *DO NOT
> PROCEED* — the prior fixes were directionally right but not yet **buildable**
> against this stack: `vote_ref` churned on re-version, edge uniqueness keyed on a
> column that nulls on re-ingest, "one transaction" had no mechanism in the Supabase
> client, and privacy was enforced on writes but not reads) → tightening → **review
> 4** (RLS read-gate + backfill count now HOLD on both reviewers — the FastAPI read
> path uses the anon/RLS key, confirmed; codex *DO NOT PROCEED* / agent *PROCEED WITH
> CHANGES*, converging on the same residue: the round-3 `vote_ref` still churned on
> chunk-content change, the round-3 `document_id` CHECK wrongly forbade provenance on
> non-vote cited edges, NULL `citation_id` could silently drop votes, and the
> projection/`replaces_id` ordering was unpinned) → **this version**, which states the
> edge-lifecycle model (projected = rebuilt, confirmed = persisted+re-resolved), an
> atomic-swap RPC, split target/provenance document columns, and a NULL-`citation_id`
> gate. The standout finding across all rounds was a **pre-existing bug** unrelated to
> the graph — votes are silently lost on document re-version (Prereqs §4.1).

---

## 1. Why — where the graph helps

The store is document → chunk. It finds documents that *mention* X; it cannot
**aggregate** mentions into "X's record," **order** them into "X's timeline," or
**traverse** "person → vote → matter → body." Queries it unlocks:

- **A. Member voting records.** "Member Siwak's complete record" · "every
  non-unanimous vote" · "who moved/seconded the bond?" — partly served already by
  `/votes` over 2,549 cited votes, but without resolved member identity (name
  variants split today) or a per-member page. Powers Clayton Ledger.
- **B. Matters / cross-body threads.** "Trace this development PC → BoA →
  Council." **The largest net-new capability** — bodies are siloed today.
- **C. Places / parcels.** "Everything at 6800 Wydown Boulevard" across bodies.
- **D. People & organizations.** "Which firms appear most before the Plan
  Commission?"

**Honest value framing:** Phase 1 (member votes) is the *lowest-risk* slice and a
clean foundation, but much of its surface value already exists via `/votes`; the
*highest net-new* value is B/C/D (the riskier phases). We build the foundation
first deliberately, not because it's the biggest single win.

## 2. Design principles

1. **Two edge kinds, never conflated.** *Cited edge* — backed by a verbatim quote
   for that exact claim (a vote); rendered as fact. *Inferred link* — a connection
   we draw (a cross-body matter join from shared parcel + applicant); rendered
   **labeled** ("Actalux linked these — same parcel + applicant") with its cited
   legs shown, never as a quote. `status ∈ {cited, inferred, confirmed}`; a human
   may promote an inferred link to `confirmed`. (Resolves "cited-only vs
   heuristic": heuristic joins are allowed *as labeled inferences*.)
2. **Native in Supabase**, beside `documents`/`chunks`/`votes` — one provenance.
3. **Seed from structured data before extraction.** Votes are a projection (no
   NLP); regex for stable numbers; NER only in the gated late phase.
4. **Conservative resolution.** Mis-merging two people is a published error: auto
   only on unambiguous match against the curated roster; everything else queues.
5. **Version-aware by construction** (§4) — *projected* graph rows (votes,
   auto-derived facts) are **rebuilt per document version** (delete by `document_id`,
   re-derive), so they never need to survive a re-version; only **human-`confirmed`**
   edges persist across versions and re-resolve their citation (§4.4). The
   new-version swap is made atomic by a single Postgres RPC (§4.3).
6. **Integrity enforced in the schema, not by convention** — privacy, target
   shape, and no-contradiction are DB constraints/triggers, not extractor manners.

## 3. Decisions (settled with operator)

- **Subject eligibility / privacy:** **public officials** + **organizations** get
  standing dossiers; **private individuals** (an individual applicant, an
  open-forum speaker) do **not** — they remain in document text *as published* but
  get no aggregated profile. Enforced in DB (§6). → **TODO: public Privacy Policy
  + Terms pages (footer)** stating this — tracked as a separate task.
- **All four bodies** in scope from the start.
- **Phase 1 = the full graph, scoped to members** (subjects + curated roster +
  member→vote edges), not a throwaway name-string view. Ships a dossier + JSON API.
- **Cross-body joins allowed as labeled inferences.**

## 4. Prerequisites (Phase 0 — must land before `migrate_028`)

1. **Fix vote re-extraction in CI (pre-existing bug).** `extract_votes.py` is in
   **no** workflow; `crawl_minutes.yml` re-ingests minutes daily but never
   re-parses votes, and `get_entity_votes` filters `replaces_id IS NULL` — so on a
   re-version, a doc's votes silently drop from the feed until extraction is re-run
   manually. Fix: re-run vote extraction whenever a minutes doc is (re)ingested.
2. **Durable-within-version vote identity (not the SERIAL `votes.id`, not a motion
   string).** `extract_votes` delete/reinserts vote rows, minting new `votes.id` each
   run, so it cannot be a stable key; and motion text / document-position ordinals
   churn on re-version (OCR/footer cleanup, an inserted motion shifts later
   positions). Anchor instead to the vote's chunk identity: each vote carries a
   `citation_id` (`migrate_020`), content-addressed over its chunk
   (`hashing.compute_citation_id`). Define
   **`vote_ref = sha256(citation_id + ':' + ordinal_within_chunk)`**, where
   `ordinal_within_chunk` = appearance order *among the votes resolving to the same
   `citation_id` within one document* (the parser already yields votes in document
   order; a vote added/removed in a *different* chunk does not perturb it). Add
   `UNIQUE (document_id, vote_ref)` on `votes`; edges carry
   `(vote_document_id, vote_ref)` referencing `votes(document_id, vote_ref)`, **not**
   an FK to `votes.id`. `vote_ref` need only be stable **within a document version**:
   projected vote edges are *rebuilt* per version (§2.5, §4.3), so a `citation_id`
   that changes on re-version does **not** orphan anything — it just yields a new ref
   in the freshly rebuilt set (this is what dissolves the §4.2↔§4.4 tension; the
   re-resolution machinery in §4.4 is for *persisted* edges, not these). **Hard
   rule — never substitute `""` for a missing `citation_id`:** `extract_votes` today
   does `chunk.get("citation_id") or ""`, which on an un-backfilled chunk collapses
   every vote in the doc to `sha256(":"+ordinal)` and trips `UNIQUE (document_id,
   vote_ref)` — silently dropping a real cited vote (and it delete/reinserts, so the
   failure is a *partial* vote set). The projection must **error/queue** on any vote
   whose chunk has a NULL `citation_id`, and Phase 0 gates on
   `backfill_citation_ids.py --apply` having run for all current minutes
   (postcondition: zero NULL-`citation_id` chunks on current minutes docs).
3. **Atomic version swap via one Postgres RPC.** The Supabase client cannot wrap
   multiple writes in a transaction (today `insert_document` → `insert_chunks` →
   set-`replaces_id` are separate PostgREST calls in `ingest.py`). A
   `projection_complete` flag *alone* does not close the window: if the new doc's
   edges are marked complete *before* the old doc's `replaces_id` flips, both
   versions briefly satisfy the read gate (duplicate edges); if the flip happens
   first, neither does (edges vanish). So the **final swap is one Postgres function
   invoked as a single `client.rpc(...)`** that, in one transaction: (a) marks the
   new document's graph rows `projection_complete = true`, (b) sets the **old**
   document's `replaces_id`, (c) deletes/unpublishes the old document's graph rows.
   Edge/vote derivation runs *before* the swap, writing rows
   `projection_complete = false` (invisible); §4.5 assertions run before the swap;
   ordering inside the projection is fixed (votes before edges). Every dossier/API
   read filters **`projection_complete = true` AND `documents.replaces_id IS NULL`**.
   This RPC also subsumes the pre-existing non-atomic document version-flip, closing
   both the graph window and that latent gap. (This is the codebase's first write
   RPC; the swap is small and well-scoped — it does not rewrite the rest of ingest.)
4. **Citation-durability with a real schema home.** A new version exists *because*
   content changed, so an edge's `citation_id` may no longer resolve and its
   `chunk_id` is nulled. Edges keep `source_quote` + a **normalized quote-hash**
   (normalization pinned to `hashing._normalize_for_citation` — casefold +
   whitespace-collapse — **verbatim**, or it won't match stored chunk text); on
   re-resolution, match the quote-hash against the new version's chunks with a
   **uniqueness rule: exactly one match → re-point (`re_resolved`); zero → `stale`;
   more than one → `ambiguous`. Never guess** — the same content-match discipline
   already implemented in `resolve_canonical_chunk` / `resolve_source_anchor`, **not**
   the lowest-chunk fallback used elsewhere in `db.py`. Tracked in a dedicated
   **`citation_state` column** (`live | re_resolved | stale | ambiguous`) — *not*
   overloaded onto `status`. Stale/ambiguous edges render with a "source updated —
   citation under review" label (or are withheld); never as a live citation. **Scope:
   this re-resolution applies to *persisted* edges** — human-`confirmed` links and
   manual `mentions` that carry across versions. *Projected* vote/auto edges are
   never re-resolved: they are rebuilt from the new version's votes (§4.3), so they
   never reach `stale`.
5. **Backfill contract with executable postconditions.** Dry-run by default; emits
   the exact inserts/deletes + the resolution-queue rows. Apply is gated by
   `projection_complete` (§4.3) — never a partial published state. Postconditions
   (asserted SQL), computed over **current (`replaces_id IS NULL`) minutes only**:
   - expected **vote-outcome** edges = count of `details.members[*]` whose
     `vote ∈ {aye, no, abstain}` **and** that resolve to a roster member
     (`absent`/`present` entries carry no edge type and are excluded; names that
     don't resolve are **queued**, not counted);
   - plus resolvable `moved_by`/`seconded_by` (votes with no roll call / NULL counts
     contribute *only* these — no per-member edges);
   - **zero** NULL-`citation_id` chunks on current minutes (else `vote_ref` collides,
     §4.2);
   - **zero** graph rows reference a superseded document;
   - **zero** contradictory vote edges (§5 constraint);
   - every `publishable=true` `person` has a roster membership or
     `minting_basis='reviewed'` (§6 trigger);
   - unresolved/ambiguous names are **queued, never dropped**.
6. **Eval set before Phase 4b** (§11).

## 5. Data model (`migrate_028_connections_graph.sql`)

```
subjects
  id, place_id -> places(id), type ['person'|'org'|'place'|'matter'],
  subject_role ['official'|'organization'|'matter'|'place'],
  canonical_name, slug, metadata jsonb,
  publishable bool not null default false,        -- privacy gate (trigger, §6)
  minting_basis ['roster'|'regex_number'|'reviewed'|'manual'],
  created_at,
  unique (place_id, type, slug)

memberships                          -- curated roster = resolution ground truth
  id, subject_id -> subjects(id), entity_id -> entities(id),
  role, start_date, end_date,        -- date windows drive date-bounded resolution
  unique (subject_id, entity_id, start_date)

subject_aliases
  id, subject_id -> subjects(id), normalized_alias, raw_alias, source,
  unique (subject_id, normalized_alias)
  -- auto-resolve ONLY when (place_id, type, normalized_alias) -> exactly one
  -- subject AND (for members) a membership covers the vote's meeting_date.

subject_resolution_queue             -- unknown/ambiguous names land here, never auto-mint
  id, raw_alias, normalized_alias, entity_id, meeting_date,
  document_id, vote_ref, reason, status ['open'|'resolved'|'rejected'],
  resolved_subject_id -> subjects(id), created_at

mentions                             -- a cited occurrence (derived; replaced per document_id)
  id, subject_id -> subjects(id), document_id, chunk_id,
  citation_id not null, source_quote, quote_hash,
  projection_complete bool not null default false,
  unique (subject_id, document_id, citation_id)   -- durable key, NOT chunk_id (nulls on re-ingest);
                                                   -- citation_id NOT NULL or the key lets dupes
                                                   -- through (multiple NULLs pass a unique) — §4.2

edges
  id, from_subject -> subjects(id),
  to_subject  -> subjects(id),         -- target: another subject     ┐ exactly
  to_entity_id -> entities(id),        -- target: a body               ┤ ONE of
  vote_document_id, vote_ref,          -- target: a vote (doc+ref)     ┘ these three
  source_document_id -> documents(id), -- PROVENANCE (≠ target); required for status='cited'
  type, status ['cited'|'inferred'|'confirmed'], inference_basis,
  chunk_id, citation_id, source_quote,
  quote_hash,                          -- normalized, for re-resolution
  citation_state ['live'|'re_resolved'|'stale'|'ambiguous'] default 'live',
  resolved_chunk_id, resolved_at,
  as_of_date, as_of_date_source,       -- derived from the vote/document; provenance kept
  projection_complete bool not null default false,  -- §4.3 publish gate; reads filter = true
  created_at
```

**Constraints (in `migrate_028`):**
- `CHECK (num_nonnulls(vote_ref, to_subject, to_entity_id) = 1)` — exactly one target.
- `CHECK ((vote_ref IS NULL) = (vote_document_id IS NULL))` — a vote target is always
  the durable pair `(vote_document_id, vote_ref)`, never a bare ref. `vote_document_id`
  is the *target* document; it is **separate** from `source_document_id` (provenance)
  — a cited non-vote edge has a `source_document_id` but no vote target.
- `CHECK (status <> 'cited' OR source_document_id IS NOT NULL)` — every cited edge
  keeps durable document-level provenance even after `chunk_id` nulls on re-ingest.
- `CHECK (status <> 'inferred' OR inference_basis IS NOT NULL)`.
- On the existing `votes` table (Phase 0, §4.2): add `vote_ref` (computed only from a
  **non-empty** `citation_id`) + `UNIQUE (document_id, vote_ref)`; edges reference
  `votes(document_id, vote_ref)`.
- **Partial unique indexes** (Postgres lets multiple NULLs through a plain unique
  key, so target types are separated — and the key must be a column that **survives
  re-ingest** and is **non-NULL**, never `chunk_id`, which is `ON DELETE SET NULL`
  per `migrate_020`):
  - vote outcome: `UNIQUE (vote_document_id, from_subject, vote_ref) WHERE type IN
    ('voted_aye_on','voted_no_on','voted_abstain_on')` — a member can't be both
    aye and no on the same vote.
  - vote role: `UNIQUE (vote_document_id, from_subject, type, vote_ref) WHERE type IN
    ('moved','seconded')` — so a member can move *and* vote on the same motion.
  - subject-target: `UNIQUE (from_subject, to_subject, type, quote_hash) WHERE
    to_subject IS NOT NULL AND quote_hash IS NOT NULL` — durable `quote_hash`, not
    `chunk_id`; cited non-vote edges always carry `quote_hash`.
  - entity-target: `UNIQUE (from_subject, to_entity_id, type, quote_hash) WHERE
    to_entity_id IS NOT NULL AND quote_hash IS NOT NULL`.

**Edge taxonomy (v1):** `voted_aye_on`/`voted_no_on`/`voted_abstain_on`/`moved`/
`seconded` (person → vote, by `vote_ref`); `heard_by` (matter → `to_entity_id`);
`applied_for`/`represents`/`owns` (org/person → matter/place, cited); `located_at`
(matter → place, cited); `same_matter_as`/`part_of` (matter ↔ matter — **inferred**
unless a shared identifier is quoted).

## 6. Subject eligibility & privacy — enforced in DB

**Minting gate (trigger).** A `BEFORE INSERT/UPDATE` **trigger** on `subjects` (a
CHECK can't cross tables): `publishable = true` for a `type='person'` row requires
**either** a `memberships` row (an official) **or** `minting_basis = 'reviewed'`.
Non-member persons default `publishable=false` and need manual approval. A trigger
fires even under the service (secret) key — which *bypasses RLS* — so this holds on
every write path, including the projection job and the backfill. (This is the
codebase's first trigger; add it to the CI schema check, §9.)

**Read gate (RLS) — required, because the trigger does not protect reads.** Today
every public read goes through the anon client against blanket `USING (true)` SELECT
policies (`migrate_007_rls.sql`), so a future plain `.table('subjects').select('*')`
would return `publishable=false` rows — a zero-tolerance privacy violation the
trigger is powerless to stop (the row legitimately exists, just unpublishable).
`migrate_028` therefore ships **deny-by-default RLS** on `subjects`, `edges`,
`mentions`: anon `SELECT USING (publishable = true)` (edges/mentions gated to
publishable subjects); the service key still bypasses for the projection. Dossier
pages and the JSON API read **only through `publishable`-filtered, security-barrier
views**. Any such view/RPC must be **`SECURITY INVOKER`** (as the existing search
RPCs already are, `migrate_007_rls.sql`) — a `SECURITY DEFINER` object runs as owner
and bypasses RLS, re-opening the hole. **Add anon-direct-`SELECT` denial tests** —
then the gate is a DB constraint on reads, not a convention.

| Candidate | Subject? | publishable |
|---|---|---|
| Seated member / mayor / official | yes | true (roster) |
| Organization (firm, LLC, institution) | yes | true (reviewed) |
| Matter (bill/resolution/project/parcel) | yes | true |
| **Private individual** (applicant, open-forum speaker) | no standing subject; raw `mentions` allowed but non-dossierable | — |

PII guard (SSN/DOB) is unchanged and orthogonal.

## 7. Entity resolution

- **Curated roster is ground truth.** Phase 1 seeds `subjects` + `memberships` +
  `subject_aliases` from a hand-listed roster per body (small, known — ~7/body),
  **not** raw `votes.details` text.
- **Date-bounded match:** a vote name on meeting-date *D* resolves to the member
  whose `memberships` window covers *D* for that `entity_id`, on `normalized_alias`
  (honorifics/titles stripped). This handles last-name-only roll calls and
  mid-term roster changes.
- Unknown/ambiguous/conflicting → `subject_resolution_queue`, **never** an
  auto-minted subject. Never auto-merge subjects with conflicting strong signals
  (same surname, different given name). Covers name drift, initials, OCR garble,
  same-surname members, a member who is also an applicant elsewhere.

## 8. Seeding by subject type

| Subject | Source | Method | Risk |
|---|---|---|---|
| person (members) | curated roster; vote names resolved against it (date-bounded) | projection + roster match | low |
| matter | stable ids (bill/resolution numbers via regex); reviewed | regex + review | low–med |
| place (parcels) | land-use text | regex (addresses) → review | med |
| org / non-member person | prose | NER → review (privacy-gated, §6) | higher |

## 9. Phased plan

| Phase | Scope | Ships | Gate |
|---|---|---|---|
| **0. Prereqs** | §4: vote re-extraction in CI; durable `vote_ref` + `citation_id` backfill gate (zero NULL on current minutes); atomic-swap RPC + `projection_complete` gate; citation-state machine; deny-by-default RLS + minting trigger + `SECURITY INVOKER` views/RPCs (**all CI-checked**); backfill contract | (infrastructure) | **blocks all** |
| **1. Members & votes** | curated roster → person subjects; `voted_*`/`moved`/`seconded` edges (4 bodies) | member **dossier** + **JSON API** | Phase 0 done |
| **2. Matters** | matter subjects from stable ids; cross-body timelines as **labeled inferred** joins | matter dossier | written matter-identity resolver |
| **3. Places & orgs** | parcel/firm subjects (privacy-gated minting) | place & org dossiers | resolution queue live |
| **4a. Prose people** | NER for non-member people/orgs | subjects only | §6 trigger enforced |
| **4b. Graph retrieval** | traversal as a 4th retrieval signal | relationship answers | **eval + metric (§11)** |

## 10. Surfaces

- **Dossier pages** — `/{state}/{place}/{body}/member/{slug}` (Phase 1), later
  `/matter/{slug}`, `/place/{slug}`. Cited-passage motif (`DESIGN.md`). **Cited
  edges and inferred links render differently** (inferred carry a "linked by…"
  label + cited legs; stale/ambiguous citations carry a "source updated" label).
- **JSON API** — read-only under `/api/v1/...`, same dormant-key/tier model, via
  `publishable`-filtered **`SECURITY INVOKER`** views/RPCs (a `SECURITY DEFINER`
  object would bypass RLS); per-member voting record is the first endpoint.
- **Retrieval (4b)** — graph traversal joins vector + FTS + RRF; rerank unchanged.

## 11. Success metrics (before 4b)

A relationship-query eval set (queries A–D) on the existing `eval/` harness:
**P@5 / nDCG@10** vs current baseline, plus **citation correctness**,
**stale-citation rate**, **entity-resolution precision**, and a **zero-tolerance
privacy-violation gate** (any dossier on an ineligible person = fail). 4b ships
only if it beats baseline — not on gbrain's number. *Note:* the existing eval keys
judgments on `chunk_id`, which is not durable across re-ingest; the relationship
eval set must key on a durable ref (document + normalized quote), to be designed
with the eval set.

## 12. Maintenance (version chain)

Per §4: graph reads filter `projection_complete = true AND replaces_id IS NULL`; the
projection rebuilds per `document_id` behind that gate and commits the new-version
swap (mark-complete + `replaces_id` flip + old-row cleanup) in **one Postgres RPC**
(§4.3), so no both-visible/neither-visible window exists. **Persisted** (`confirmed`)
edges survive via `quote_hash` re-resolution; **projected** edges are rebuilt.
Backfill asserts zero graph rows on superseded documents.

## 13. Open questions (remaining — non-blocking for `migrate_028`)

- **Matter identity resolver** (gate before Phase 2): when is a motion its own
  matter vs. part of a bill? Routine procedural motions are **not** matters.
- **Inferred-join thresholds**: minimum shared signals + confidence to *show* a
  cross-body link vs. withhold.
- **Roster operations**: source of truth for the curated roster and who maintains
  `memberships` date windows (the data is small; the process needs an owner).
- **Eval durable IDs** (§11): finalize with the eval set, before 4b.
- **`vote_ref` collision monitoring**: `citation_id` is not globally unique (~2.6% at
  corpus scale, `hashing.py`); `(document_id, vote_ref)` contains it for Phase 1, but
  log cross-document `vote_ref` collisions before any future resolution-by-ref-alone.

## 14. Relationship to gbrain

Reference, not dependency. The "BrainBench +31 P@5" figure is a **directional
prior only — unverified, from a private prose brain with heuristically-inferred
edges and a different task distribution; not transferable 1:1.** Our own eval
(§11) justifies Phase 4b.
