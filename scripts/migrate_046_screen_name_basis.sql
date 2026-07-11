-- Migration 046: admit the 'screen_name' name-anchor basis.
-- Run in the Supabase SQL editor (or via apply_migrations.py).
--
-- Design: docs/architecture/zoom-name-extraction.md (Z2 write policy).
-- Extends migrate_044 (which added 'discourse' to the same two CHECK constraints).
--
-- WHY: the identity layer gains a visual evidence family — Zoom-era recordings
-- render each participant's display name on screen (a labeled gallery tile with a
-- green active-speaker border, or a full-frame speaker-view label), and the Z1
-- probe OCRs that label at diarization-turn midpoints and roster-matches it. A
-- platform-rendered attribution is a mechanism independent of adjacency, discourse,
-- and vote evidence, so it carries its own basis. Tier is decided per verdict by
-- rendering mode (tile -> inferred_high, full-frame -> inferred_medium); both
-- CHECK constraints that enumerate the allowed bases must admit it:
--   1. speaker_identities.basis        -- apply_zoom_verdicts.py writes 'screen_name';
--   2. subject_voiceprints.source_basis -- an enrolled screen_name cluster carries its
--      source basis into the gallery, so the gallery constraint must allow it too.
--
-- Both original constraints (migrate_034 / migrate_040, last widened in migrate_044)
-- are named <table>_<column>_check by Postgres.
--
-- Additive + idempotent (DROP CONSTRAINT IF EXISTS then ADD).

-- 1. speaker_identities.basis (last set in migrate_044).
ALTER TABLE speaker_identities DROP CONSTRAINT IF EXISTS speaker_identities_basis_check;
ALTER TABLE speaker_identities ADD CONSTRAINT speaker_identities_basis_check
    CHECK (basis IN
      ('rollcall', 'vote_anchor', 'self_intro', 'manual', 'voiceprint', 'presenter_intro',
       'discourse', 'screen_name'));

-- 2. subject_voiceprints.source_basis (last set in migrate_044).
ALTER TABLE subject_voiceprints DROP CONSTRAINT IF EXISTS subject_voiceprints_source_basis_check;
ALTER TABLE subject_voiceprints ADD CONSTRAINT subject_voiceprints_source_basis_check
    CHECK (source_basis IN
      ('rollcall', 'vote_anchor', 'self_intro', 'manual', 'presenter_intro', 'discourse',
       'screen_name'));
