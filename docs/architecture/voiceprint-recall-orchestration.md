# Voiceprint recall — orchestration prompt (Fable driver)

Paste-ready prompt for a fresh session where **Fable orchestrates**, fanning out **Opus/Sonnet
subagents** for deep reasoning + coding and **codex** for code review, all disciplined by the
nested-LOMO harness so nothing overfits. Written 2026-07-03, after the recalibration verdict
(`voiceprint_calibration id=2`, candidate: macroP=1.000, recall=0.064, 1 official enabled).

---

GOAL: Crack cross-meeting voiceprint speaker-ID for Actalux. Raise recall from the current
6.4% toward something shippable — a majority of officials enrolled AND a majority of their
speaking turns correctly named — WITHOUT dropping below the precision bar. You are the
orchestrator: hold the plan and the precision cardinal, fan work out to Opus/Sonnet subagents,
and use codex (~/.claude/scripts/codex_call.sh, one-shot, read-only) to review every code change.

=== READ FIRST (do NOT re-derive; the diagnosis is done) ===
- Memory: project_actalux_speaker_diarization.md  (auto-loaded — read the ★ VERDICT + diagnosis)
- HANDOFF.md  (the id=2 candidate verdict + the enrollment-quality lever)
- docs/architecture/voiceprint-recalibration-plan.md  (the two-gate design, nested-LOMO harness)
- docs/architecture/voiceprint-speaker-id-plan.md and speaker-attribution.md
- Code: src/actalux/diarization/{matching,pooling,labelqa,enrollment,modal_runner}.py
        scripts/recalibrate_voiceprints.py  (THE measurement harness)
        scripts/enroll_voiceprints.py

=== WHAT WE KNOW (grounding — treat as established) ===
- The embedding MODEL is fine (wespeaker-voxceleb-resnet34-LM, 256-d cosine). A clean official
  scores 0.9+ same-speaker across meetings; p90 same-person cosine = 0.937.
- The ENROLLMENT GALLERY is the ceiling: 94% roll-call anchors, poisoned by clerk-role/crosstalk
  clusters. Only 1 of 12 officials (Rick Hummell) survives cross-meeting coherence.
- The full recalibration verdict (voiceprint_calibration id=2, candidate): macroP=1.000,
  recall=0.064, 0 false positives on 64 citizen/negative clusters. Precision is SOLVED; recall
  is the problem.

=== HARD CONSTRAINTS (never violate — these are cardinals) ===
1. PRECISION FIRST: never publish a wrong public name. Every candidate solution is judged at the
   0.98 macro-precision bar with ZERO false positives on negative (citizen) clusters. A recall
   win that costs precision is a LOSS.
2. OPTION B: a private citizen's voiceprint is NEVER stored. Officials-only gallery; negatives
   are scored to measure rejection, never persisted.
3. candidate ≠ cleared is STRUCTURAL (calibration_id FK → status). A HUMAN promotes candidate→
   cleared. Never auto-promote, never build a live matcher or publish a name without an explicit go.
4. JURISDICTION-GENERALIZABLE: no Clayton-specific hacks. Every fix must be an algorithm/anchor
   change that works when a new town's corpus is dropped in. Per-place calibration is the gate.
5. MEASURE, DON'T OVERFIT: every change is scored by the SAME nested-LOMO harness (params +
   enablement chosen per held-out meeting from OTHER meetings). Report recall AT the precision
   bar and FP-on-negatives, vs the id=2 baseline. No hand-tuned operating points.

=== SOLUTION SPACE (fan subagents out over these; not exhaustive — think first-principles too) ===
A. Enrollment anchor quality (diagnosed #1 lever): replace/augment roll-call anchors with
   self-intro + sustained speech (reports, extended questions). Enroll appointed staff (City
   Manager, CFO, dept heads) who never roll-call but present at length.
B. Human-in-the-loop confirmation: we have 0 confirmed labels. A lightweight confirm/deny tool
   (play a cluster, operator labels it) → Gate A trusts more officials. Likely highest-precision,
   fastest path to real recall.
C. Embedding model A/B: wespeaker is 2021-era. Test a modern far-field-robust embedder (ECAPA-
   TDNN, ReDimNet, NeMo TitaNet, pyannote community-1, WavLM) on the SAME harness. Secondary
   (model isn't the diagnosed bottleneck) but cheap to falsify.
D. Diarization purity: overlap-aware / retuned pyannote to stop clerk-crosstalk smearing, so
   enrollment clusters are single-speaker.
E. Matching algorithm: per-turn scoring + aggregation, PLDA vs cosine, score normalization
   (AS-norm), quality/purity-weighted voting, require agreement across ≥2 enrolled meetings.

=== ORCHESTRATION PLAN ===
Phase 1 — UNDERSTAND (parallel): spawn one subagent per lever (A–E). Each reads the code +
  diagnostics and returns {feasibility, expected recall payoff, precision risk, how to measure
  it on the harness, rough effort}. Use Opus for the reasoning-heavy levers (E, C), Sonnet for
  the rest. Do NOT let them write code yet.
Phase 2 — DECIDE: synthesize (an Opus subagent + you). Pick the 1–2 highest-payoff, lowest-
  precision-risk experiments. Write the decision + expected-effect + measurement plan to a scratch
  design note. Prefer the lever that generalizes and needs the least human labeling to prove out.
Phase 3 — BUILD: implement the chosen experiment as a measurable change (extend the recalibrate
  harness or a new eval script — never a one-off REPL result). Sonnet/Opus subagents code; codex
  reviews EVERY diff (--reasoning high --sandbox read-only). Fold all blockers. Unit tests + ruff.
Phase 4 — MEASURE: run it. GPU/long work goes OFF-SESSION via GitHub Actions (in-session bg jobs
  get reaped) — reuse/extend recalibrate_voiceprints.yml. Read the nested-LOMO verdict, compare
  to id=2 (recall 0.064 / precision 1.0). Report the delta honestly (below-floor positives are
  misses, not omissions).
Iterate Phase 3–4 until recall clears a useful bar at the precision cardinal, or you've shown
  which lever is the real unlock and what human input (labels) it needs.

=== OPS ===
- Doppler: DB creds = --project mac; Modal/HF = --project actalux. Never print secret values.
- Commits: conventional, author Actalux (auto via includeIf), NO AI attribution anywhere, branch
  off master + FF-merge, show message + get approval before committing.
- Report progress as you go; surface the go/no-go, don't promote to cleared yourself.

Start with Phase 1. Give me the lever-by-lever payoff/risk table before building anything.
