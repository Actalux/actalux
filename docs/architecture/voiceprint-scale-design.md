# Voiceprint speaker-ID at scale — zero-human-labeling architecture (2026-07-04)

Synthesis of three design reviews (vote-sequence alignment, LLM discourse labeling, consensus
enablement + promotion policy), each grounded in the live corpus. Question answered: how does
speaker-ID stay accurate across thousands of towns/meetings without a human in the loop?

## The finding that frames everything

Human confirmation is a *substitute for evidence independence*. Today every machine anchor
(rollcall / self_intro / presenter_intro) is ONE evidence family — they all read the same ASR
transcript with the same "sustained speaker adjacent to a cue" logic, so they share failure
modes and their mutual agreement is near-worthless. Add genuinely independent families and
machine consensus replaces per-cluster human judgment; the acoustic gates (coherent core +
collapse detection) remove the failure channel no text evidence can (diarization impurity).

## The three independent evidence families

1. **Adjacency-cue** (exists): rollcall + self_intro + presenter_intro. One family.
2. **Vote-sequence alignment** (`vote_anchor`, basis reserved in schema) — aligns the structured
   vote record's ordered member list (DB, immune to ASR mangling) against the ordered short-
   response turns via monotone DP; whole-sequence consistency + clerk-exclusion + 1:1 matching +
   count checks make wrong labels structurally hard. **Measured reality (Clayton): a precision
   play, not a coverage play** — only 41/343 transcripts have detectable clerk-call audio
   (council 33; schools 0 — no audible roll calls), pyannote glues most responses into the
   clerk's turn, ~68 separable responses → ~15-40 anchors, council-only. Its real value:
   replaces the poisoned `_rollcall_hits` heuristic and yields the cleanest gallery seeds.
   ~3-4 days (`src/actalux/identity/vote_align.py`).
3. **LLM discourse labeling** (`discourse`) — **the coverage lever.** gpt-5-mini reads a meeting
   (turns + anonymous cluster labels + closed roster enum) and emits (cluster, person,
   verbatim-quote evidence, confidence) from addressing semantics: chair recognitions → next
   speaker, gratitude-handoffs → previous, role-claims → self, Q&A name chains. Catches exactly
   the members who speak for minutes but never say "here". Cost ≈ **$15-25 per 1,000 meetings**
   (~32K tokens/meeting, one pass + focused verify on contested clusters). Containment: roster-
   closed vocabulary (can never invent a name), quotes verified as verbatim substrings, private
   citizens structurally unlabelable (Option B), proposals only ever feed the acoustic gates —
   never a publish path. ~3-5 days (`src/actalux/identity/discourse.py`).

## Consensus enablement (replaces human label confirmation)

Gate A enables an official without a human iff: collapse guard passes AND acoustic coherent
core across ≥3 meetings AND **≥2 independent families** anchor samples landing on the SAME
coherent voice (agreement checked in embedding space, not name strings) in ≥2 core meetings.
Human confirmation remains the strongest single family (keeps its coherence waiver); machine
consensus never waives coherence. Shared-failure channels (Whisper errors → both text families;
diarization boundaries → all families) are the reason the acoustic layer stays mandatory.
Calibration report gains per-official audit diagnostics: families present, agreement matrix,
enable-path. ~5-7 days including audit sheet + auto-demotion.

## Promotion policy (the honest residual human act)

Full zero-human promotion is NOT defensible under "never a wrong public name": the fatal error
class — a coherent, self-consistent wrong label — poisons the eval's own ground truth, so every
metric reads perfect over it. Metrics-SLA is necessary but not sufficient. Recommendation:
- **Zero-human LABELING** (consensus above), plus
- **One-click human PROMOTION per town** from an auto-generated audit sheet: per enabled
  official — evidence families, a ~10s voice snippet, the verbatim quote, the metric block.
  ~2-5 minutes per town, O(officials) not O(meetings); batchable by a part-time reviewer.
- **Auto-DEMOTION fully automatic** (regression on any later run → candidate). Adding a name
  gates on a human; removing one never waits.

## Tenure guard (roster is date-scoped to the meeting)

Every text family draws candidates from the body roster, but membership is time-bounded: an
official holds a seat only within their `memberships.[start_date, end_date]` window. Without a
date filter a name spoken at a meeting *before* a member was seated — a same-surname
predecessor, an incidental mention — can anchor that not-yet-seated member. Observed failure:
2020-2021 board meetings were labeled with an official first sworn in on 2022-04-20.

`RosterMember` now carries `term_start` / `term_end` (ISO `YYYY-MM-DD` or None; None = open on
that side / still seated), populated by `members_for_entity` from the membership row.
`members_active_on(members, meeting_date)` keeps a member iff `term_start <= meeting_date <=
term_end` **inclusive** (lexicographic string compare, correct for ISO dates). Both the
deterministic resolver (`resolve_document`, gating `resolve_identities` **and** `align_votes`)
and the LLM discourse labeler's caller (`scripts/label_discourse.py`, gating the closed roster
enum the model sees) filter the roster through it *before* any anchoring runs, so an
out-of-tenure official is structurally unanchorable for that meeting.

**Fail-open on undated documents (deliberate):** when `meeting_date` is null/empty, tenure is
indeterminate and the guard returns the roster **unchanged** — it never excludes everyone.
Failing closed would erase legitimate current officials on any transcript we couldn't date;
the guard exists to remove *provably out-of-window* candidates, not to require a date. Analytic
consequence: an undated transcript gets no tenure protection (accepted — the alternative drops
real anchors), and correctness of the guard depends on membership `start_date`/`end_date` being
populated (a member with a null window is treated as open-ended and always eligible).

## Coverage compounding (why this reaches the 65-85% ceiling)

Textual evidence only needs to label each official SOMEWHERE; the enrolled voiceprints then
propagate names to every meeting — including the ~300 transcripts with no roll call and the
bodies where no textual family fires. Labels seed; acoustics scale.

## Scale ops (1000 towns)

Per-town: one recalibration GPU run per trigger (roster change / ≥N new meetings / new basis /
quarterly), ~$few each; persist per-turn vectors keyed by video_id so re-runs embed only new
meetings. Replace workflow_dispatch fan-out with a place-keyed job queue; per-town WARP egress +
the >10%-download-failure abort (already built). voiceprint_calibration is already per-place.

## Recommended build order

- **Phase A — discourse labeler** (3-5d): biggest recall; validate against the Hummell/Dilber
  acoustic ground truth + per-basis recall_by_confidence in the harness.
- **Phase B — vote_align** (3-4d): second independent family; retires the poisoned rollcall
  heuristic at the source.
- **Phase C — consensus Gate A + audit sheet + auto-demote** (5-7d): turns A+B into zero-human
  enablement; the batch confirm CLI stays as an optional tie-breaker, no longer the pipeline.
- Then recalibrate: expectation is most of the council roster enabled with no operator labeling.

Design reviews grounded in: 343 transcripts / 106,849 turns / 3,517 vote edges / 553 roll-call
events (queried live); real Clayton turn examples for every discourse signal; id=4 verdict
(recall 0.130, macroP 1.000, presenter_intro-tier recall 0.571 vs rollcall-tier 0.064).
