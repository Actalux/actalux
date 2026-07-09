-- Migration 045: name-the-public-record — per-document, non-tracked speaker names.
-- Run in the Supabase SQL editor (or via apply_migrations.py).
--
-- Design: docs/architecture/name-the-public-record.md (approved 2026-07-08).
--
-- The third tier of the speaker-naming policy (CLAUDE.md Content policy, 2026-07-08):
--   1. TRACKED ENTITY   -> speaker_identities.subject_id (persistent, voiceprinted) — officials.
--   2. NAMED-IN-TRANSCRIPT -> this table (per-document, NON-tracked) — public participants who
--                          self-identify or are introduced on the record.
--   3. ANONYMOUS        -> no row — citizens.
--
-- The whole point of tier 2 is that it is NOT a persistent entity. That guarantee is STRUCTURAL:
-- this table has NO subject_id column, so a named-in-transcript row can never be voiceprinted or
-- linked across meetings — the schema enforces it, not a code path (CLAUDE.md: structural over
-- procedural). Two identical self-IDs in different meetings are two independent rows.
--
-- Additive + idempotent (CREATE/ALTER ... IF NOT EXISTS; DROP POLICY then CREATE).

-- 1. Per-document participant names. One row per (document, cluster), like speaker_identities,
--    but with a literal display_name (from the speaker's own words) instead of a subject_id.
CREATE TABLE IF NOT EXISTS transcript_speaker_names (
    id             SERIAL PRIMARY KEY,
    document_id    INT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    cluster_label  TEXT NOT NULL,             -- the diarization cluster this names ("SPEAKER_07")
    display_name   TEXT NOT NULL,             -- the participant's self-stated / introduced name
    basis          TEXT NOT NULL CHECK (basis IN ('self_intro', 'presenter_intro')),
    evidence_quote TEXT NOT NULL,             -- verbatim self-ID / introduction (the source cite)
    start_seconds  REAL CHECK (start_seconds >= 0),  -- where the self-ID occurs (clip cue)
    -- 'auto' bodies insert 'approved'; 'review' bodies (schools) insert 'proposed' (never shown
    -- until a human approves via the P3 queue); 'rejected' is a human "do not name" (e.g. a
    -- protected employee who self-identified).
    status         TEXT NOT NULL DEFAULT 'proposed'
                     CHECK (status IN ('proposed', 'approved', 'rejected')),
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (document_id, cluster_label)
    -- INTENTIONALLY no subject_id: tier 2 is non-tracked by construction.
);
CREATE INDEX IF NOT EXISTS idx_transcript_names_doc ON transcript_speaker_names (document_id);

-- RLS: anon may read a name ONLY when approved (the public-display gate — mirrors the
-- high/confirmed gate on speaker_identities). Proposed/rejected rows are internal until a
-- human acts on them. All writes go through the service key (bypasses RLS).
ALTER TABLE transcript_speaker_names ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS anon_read_approved_transcript_names ON transcript_speaker_names;
CREATE POLICY anon_read_approved_transcript_names ON transcript_speaker_names
    FOR SELECT TO anon, authenticated
    USING (status = 'approved');

-- 2. Per-body naming policy. Drives whether tier-2 names auto-publish, queue for review, or are
--    off. Keyed per body so it scales: a city body publishes the full record (auto); a school
--    board protects employees (review). Default 'off' is the safe stance for any new body until
--    its content-policy class is set (ideally at body creation).
ALTER TABLE entities ADD COLUMN IF NOT EXISTS public_participant_naming TEXT NOT NULL DEFAULT 'off'
    CHECK (public_participant_naming IN ('auto', 'review', 'off'));

-- Seed the existing bodies per the 2026-07-08 content policy. City government = full public
-- record -> auto; school district = board + admin published, public participants gated to protect
-- staff/teachers/students -> review. Keyed on body_slug (city vs school-board policy is a function
-- of body type, not town), so this is jurisdiction-agnostic for the bodies we run today.
UPDATE entities SET public_participant_naming = 'auto'
    WHERE body_slug IN ('council', 'plan-commission', 'board-of-adjustment');
UPDATE entities SET public_participant_naming = 'review'
    WHERE body_slug = 'schools';
