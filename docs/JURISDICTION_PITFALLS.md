# Jurisdiction pitfalls

Things that were wrong in Clayton and will be wrong again in the next town.

This is a working list, not a retrospective. Every entry is a mistake the data
actually made — not a hypothetical — written so that whoever onboards
jurisdiction #2 can check for it deliberately instead of rediscovering it from a
wrong number on a public page. Each entry says **what broke**, **why it is easy
to get wrong**, and **what to check**.

Add to this file whenever a jurisdiction-shaped assumption turns out to be
false. Fixing the bug is not enough; the next town has the same shape.

Related: `CLAUDE.md` ("Scale across municipalities"), `docs/multi-tenancy.md`,
`docs/SCALING.md`.

---

## 1. Officials

### 1.1 A swearing-in date is a term boundary, not the start of service

**What broke.** Susan Buse's council term was recorded as starting 2020-06-23,
sourced to the minutes that record her being sworn in. She had in fact been
voting since 2019-08-13, having won a special election to fill a Ward II
vacancy; the June 2020 date was her swearing-in for a *second*, regular term.
The tenure check then refused to credit 37 of her votes, and would have deleted
them from her public record.

**Why it is easy to get wrong.** Searching the minutes for "sworn" finds a real,
citable date, and it looks authoritative. Officials serving consecutive terms
are sworn in repeatedly, so the search finds *a* boundary — usually the most
recent one, which is the wrong end.

**What to check.** For each official, compare the recorded start against the
earliest meeting where they actually cast a vote. If they voted before their
recorded start, you have the wrong swearing-in. Look specifically for special
elections filling a vacancy — those produce a mid-cycle start plus a second
swearing-in a few months later.

### 1.2 Service is not always continuous

**What broke.** Michelle Harris sat on the Clayton council through April 2015,
was term-limited out, and returned via the April 2017 election. The roster held
a single window covering 2015 → 2025, which silently asserted she was seated
during two years when she was not.

**Why it is easy to get wrong.** A single `term_start` / `term_end` pair is the
obvious data shape, and it is right for most people. The gap is invisible unless
you look at the distribution of a name's appearances over time rather than its
first and last.

**What to check.** Plot each official's votes per quarter. A run of empty
quarters in the middle is a gap, not a data problem. The roster supports a
`terms` list for this; use it. Term limits, a lost election followed by a
comeback, and a resignation-then-reappointment all produce this shape.

### 1.3 Titles change, and the same person accumulates several

**What broke.** Buse appears as "Alderman Buse" (2019–2025), then "Alderwoman
Buse" (mid-2025), then "Councilmember Buse" (late 2025 onward) — one person,
three titles, because the body renamed its members twice. Harris appears as
"Mayor Pro Tempore Harris", "Alderman Harris", and "Mayor Harris".

**Why it is easy to get wrong.** An alias search keyed on the dominant title
("Alderman X") silently misses the periods when a different title was in use.
That is how Harris's start date landed one meeting late: the earliest appearance
was under "Mayor Pro Tempore".

**What to check.** Search by surname alone, then group by the leading title, and
look at the date range of each form. Body-wide renames show up as a clean
handover date across every member at once. Individual role changes (member →
chair, alderman → mayor) show up for one person.

### 1.4 Being named in a motion is not the same as voting on one

**What broke.** Cynthia Garnholz's last *appearance* in the vote records is
2017-10-10, but her last *vote* is 2017-04-25. The later mentions are motions
appointing her to a board — she is the subject of the motion, not a participant
in it. Deriving her term end from "last appearance" would have extended it by
six months.

**Why it is easy to get wrong.** Both forms put the surname in the same field.

**What to check.** When deriving a bound from appearances, restrict to roll-call
positions (the voter list), not free text in the motion body. Appointments,
commendations, and public comment all name people who are not voting.

### 1.5 A term bound derived from the archive is a lower bound, not a fact

**What broke.** Nothing yet — this is the trap the roster's `basis` field
exists to avoid, and it held. Harris and Garnholz both first appear at
2015-01-13, which is simply the earliest council document in the corpus. Their
service began earlier; the archive cannot say when.

**What to check.** Distinguish `cited` (a verbatim date in the record) from
`observed` (derived from appearances) and keep `null` for genuinely unknown.
Never write a date the record does not contain. An official present at the
earliest archived meeting should carry an open start, not the corpus edge — a
bound derived from where the archive happens to begin is an artifact of
ingestion, not a fact about the person.

---

## 2. Verification

### 2.1 A tenure check is only as good as the term data behind it

**What broke.** Gating vote attribution on term windows is correct, and it
immediately surfaced three roster errors. But when the underlying dates are
wrong, the gate does not fail loudly — it silently *removes* correct edges. Of
Clayton's 114 memberships only 31 carry any dates at all, and all three that
this exercised were wrong at a boundary.

**What to check.** Before enabling a date-gated check in a new jurisdiction,
diff what it would produce against what already exists, and read the deletions
before writing. A gate that removes existing published data needs the same
scrutiny as one that adds it. The dry run is the artifact to review, not the
summary count.

---

## 3. Backfill from the 2026-07-30 audits

These are not jurisdiction-specific in origin, but each will recur per town
because each scales with the corpus. Full detail in `docs/audit/`.

### 3.1 Per-body row counts outgrow single-request reads

Council reached 556 current documents; the database caps a response at ~1000
rows and truncates silently. Any query that reads "all documents for a body" and
then filters by their ids has two ceilings — the row cap and the request URL
length. Both fail by returning less data, not by erroring. Each new jurisdiction
adds bodies that climb the same curve. Page the reads; batch the id filters.

### 3.2 Identical text is not always the same document

Content-hash deduplication collapsed two genuinely different records that read
identically — a boilerplate notice reissued for a later meeting. The second was
treated as an unchanged duplicate and never stored. Towns that reuse standard
notice templates will hit this immediately.

### 3.3 Fixed-width content ids collide as the corpus grows

Citation ids are 32 bits: about a 2.6% chance of a collided pair at Clayton's
current size, and roughly 69% at 100k chunks. Multi-jurisdiction growth reaches
that range fast. Widening the id is not a constant change — vote identities are
derived from it — so plan the migration before the corpus makes it expensive.

### 3.4 Protected-class rules are per-body, and the strictest wins

School district records never name individual personnel, teachers, or students;
city records publish the full public record. A crawler or naming pass written
against one body's rules and reused for another is a policy breach, not a bug.
Every new body needs its content policy stated before its first ingest.

## Portal identity can live in the query string (Diligent meetings, 2026-08-14)

`normalize_source_ref` drops query strings by design — rotating tracking params must
not mint new document identities. But three portals now put the *real* identity in the
query: YouTube (`watch?v=`), CivicPlus (`ShowPrimaryDocument?...ID=`), and Diligent
meeting pages (`MeetingInformation.aspx?Id=`). When the Clayton BoE held two meetings
on one day (business meeting + retreat), both agenda pages normalized to the same ref
and the second agenda ingested as a "new version" that superseded the first.

Onboarding a new town: before trusting dedup, check where its portal keys documents.
A `page.aspx?Id=N` shape needs its own carve-out in `normalize_source_ref`, and the
symptom of missing one is silent — documents replace each other instead of coexisting.

Also from the same incident: a district's **meeting agendas may live on a different
portal surface than its documents**. Clayton schools' Diligent folder tree contains
packets, minutes and attachments but never agendas — those are rendered from
`/Services/MeetingsService.svc/`. A town can look fully crawled while an entire
document class is structurally invisible.
