# Name-the-public-record — per-document, non-tracked speaker naming (spec, 2026-07-08)

Status: **APPROVED** (operator confirmed all five §9 decisions 2026-07-08). Building P1 (§10).

## 1. Goal

Attribute transcript turns to **self-identified public participants** — applicants, architects,
developers, residents, outside presenters, other-body officials — using the speaker's own
words on the record, **without** building a persistent cross-meeting entity or a voiceprint.

This is the "(B)" lever from the 2026-07-08 self-introduction measurement
(`scripts/analyze_self_intro_coverage.py`). That scan found the recall headroom splits sharply:

- **Tracked-official headroom is tiny** — only ~17 distinct officials are *ever* self- or
  presenter-introduced across 343 meetings (seated officials are announced by roll call, so they
  rarely self-introduce). Removing the roster-gate barely moves official voiceprint recall.
- **Named-participant headroom is large** — ~824 distinct self-/introduced non-officials
  (71 recurring across ≥2 meetings), ~88% via self-introduction. Overwhelmingly applicants,
  architects/developers, and residents (plan-commission 38 recurring, council 19), plus, on
  schools, mostly *protected staff*.

So the conservatism the operator sensed lives in **transcript naming coverage**, not the
voiceprint gallery. Today the public record is full of people who state exactly who they are
("Tyler Stevens, Core 10 Architecture") and we render them "Speaker 4." This feature names them.

Highest value: **plan-commission and council**, where project applicants and their architects
are precisely the parties a researcher wants named, and the city content policy already publishes
"the full public record as the body published it."

## 2. Policy grounding — the three tiers (CLAUDE.md, 2026-07-08)

1. **Tracked entity** — persistent `person`/`subject` row + voiceprint gallery + cross-meeting
   recognition. **Officials only** (schools: board + administration). Unchanged by this feature.
2. **Named-in-transcript** — per-document attribution from a speaker's own self-ID or an
   on-the-record introduction, **no persistent entity, no voiceprint, no cross-meeting linkage**.
   *This feature builds this tier.*
3. **Anonymous** — citizens; unchanged.

Hard constraint from the policy: **the schools protected class (individual personnel, teachers,
students) is never named, even when it self-identifies** ("I work in the counseling office" =
staff → not named). Self-identification is necessary but never sufficient.

## 3. The city/schools asymmetry (the safety spine)

The measurement showed schools "headroom" is dominated by protected staff, while city headroom is
public applicants/architects. The precision cardinal ("never a wrong public name") plus the
protected class means the default policy per body must differ:

- **City bodies (council, plan-commission, board-of-adjustment): AUTO.** A self-identified
  participant is named in that transcript automatically — they appear "as published," matching the
  city content policy. Low risk, high value.
- **Schools: GATED.** Named-in-transcript does **not** auto-apply. Only board + administration
  (tracked officials) are named automatically. Any public-participant naming on a schools
  transcript goes through an **operator review queue** before it displays, because on schools a
  self-identifier is more likely a protected employee than a nameable presenter, and the cost of
  naming a teacher/employee is a policy violation.

This asymmetry is **config, not code** (per the municipalities cardinal): a per-body policy flag
`public_participant_naming ∈ {auto, review, off}`, stored per body (defaulting from the body's
content-policy class), resolved from the request — never hardcoded `schools`/`clayton`.

## 4. Detection — reuse the resolver, drop the roster-gate for *naming* only

The deterministic resolver (`src/actalux/identity/resolve.py`) already detects self-introductions
(`_INTRO_RE`: "my name is / I'm / I am / this is" at turn start) and presenter-introductions
(cue-verb / title-appositive / presence templates). Today it **discards** any hit whose name is
not a roster member. This feature adds a second consumer of the same hits:

- When an intro hit names a **roster official** → unchanged (tracked path; may enroll a voiceprint).
- When an intro hit names a **non-roster person** → emit a **named-in-transcript** proposal for
  that cluster: the extracted literal name + the **verbatim self-ID/introduction quote** + the
  basis (`self_intro` | `presenter_intro`) + the timestamp.

Name extraction reuses the conservative logic already written in
`scripts/analyze_self_intro_coverage.py` (honorifics skipped, 1–3 capitalized tokens, non-name
stop-list, presenter names require ≥2 tokens). ASR mangling is expected; the name is the person's
own stated name and is sourced, and the place-scoped name-correction/glossary layer can clean
common manglings (e.g. the "jerry hockman" → "Jere Hochman" case).

## 5. Data model (recommendation: a separate per-document naming table)

A named-in-transcript label has **no subject** — that is the whole point. Two options:

- **(Recommended) New table `transcript_speaker_names`** — `(document_id, cluster_label,
  display_name, basis, evidence_quote, start_seconds, confidence, status)`, unique on
  `(document_id, cluster_label)`. Having no `subject_id` column makes it **structurally impossible**
  to voiceprint or cross-link one of these rows — the "no persistent entity" guarantee is enforced
  by the schema, not by a code path (CLAUDE.md: structural over procedural). `status ∈
  {proposed, approved, rejected}` drives the schools review gate; city rows are inserted `approved`.
- **(Alternative) Nullable `subject_id` + `display_name` on `speaker_identities`** — fewer tables,
  but it puts tracked and untracked labels in one table and relies on a code path to never treat a
  `display_name` row as trackable. Weaker guarantee; not recommended.

## 6. Display & provenance

The transcript renderer already resolves a cluster's name via `speaker_identities → subject`.
Extend the lookup: for a cluster, prefer a tracked subject name; else use an **approved**
`transcript_speaker_names.display_name`; else "Speaker N". The cited self-ID quote is the
provenance (clickable to its timestamp), consistent with the universal "every statement cites a
verbatim source quote" rule. The UI must never imply tracking (no person page, no "appears in N
meetings" for a named-in-transcript speaker).

## 7. Non-goals (explicit)

- **No voiceprint, no enrollment, no effect on the official-recall recalibration.** This is a
  display/attribution layer over clusters that already exist.
- **No cross-meeting linkage.** Two identical self-IDs in different meetings are independent rows.
  If a developer appears in 8 plan-commission hearings, that is 8 rows, not one entity.
- **No automatic promotion to a tracked entity.** A recurring participant *may* later be promoted
  to a tracked entity (a "matter participant"), but only as a deliberate, per-body-policy decision —
  never automatic. Out of scope here; the design does not foreclose it (§8).

## 8. Reversibility & scale

- A wrong name is a single-row repoint/delete (`transcript_speaker_names`) — cheap, reversible
  (CLAUDE.md: reversible over one-way).
- Jurisdiction-agnostic: the per-body policy flag + place-scoped name extraction carry no town
  wording; a new town inherits `auto` for its city bodies and `review`/`off` for a school board by
  content-policy class.
- Non-foreclosure: because each row records the name + quote + timestamps, a later entity-resolution
  pass could cluster recurring participants into tracked entities without re-deriving anything.

## 9. Decisions (operator-confirmed 2026-07-08)

1. **Schools default = `review`.** Schools writes proposed names (never displayed) that a human
   approves/rejects via the P3 queue; the protected class is never auto-exposed. (In P1, before the
   queue exists, schools rows are written `status='proposed'` and simply never display — RLS gates
   anon reads to `approved` — so no protected name can leak.)
2. **City = `auto`.** Council / plan-commission / board-of-adjustment auto-name self-identified
   participants (inserted `status='approved'`), relying on the verbatim-quote requirement + the
   "full public record" policy. No pre-display review.
3. **Data model = separate `transcript_speaker_names` table** (no `subject_id` column — the
   non-tracked guarantee is structural).
4. **Minors = universal suppression** above the per-body flag: a self-identified student/minor is
   never named, on any body (city or schools).
5. **Backfill = city bodies now** (P4), forward-on-ingest thereafter.

## 10. Rough phasing (post-approval)

- **P1** — schema (`transcript_speaker_names` + per-body policy flag), the non-roster branch in the
  resolver, name extraction, unit tests. City-auto only; schools defaults `off`.
- **P2** — display integration (transcript renderer + speaker labels + cited-quote provenance).
- **P3** — schools review queue (a small operator UI/CLI to approve/reject proposed names).
- **P4** — corpus backfill (city bodies first), with the same >10%-failure abort guard the
  recalibration uses.

Effort: P1 ≈ 3–4 d, P2 ≈ 2 d, P3 ≈ 2 d, P4 ≈ 1 d. P1+P2 on city bodies is the shippable core.
