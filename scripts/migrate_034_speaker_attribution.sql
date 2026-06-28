-- Migration 034: speaker attribution + name canonicalization (transcripts).
-- Run in the Supabase SQL editor (or via apply_migrations.py).
--
-- Foundation for the going-forward transcription pipeline (clean WhisperX +
-- pyannote, off-Mac). Design: docs/architecture/speaker-attribution.md.
--
-- Two decoupled layers over the same transcript, linked by time:
--   * SEARCH  — the existing `chunks` (coherent ~200-word, structure-aware), built
--               over the CANONICAL text and embedded. Stays the retrieval + citation
--               unit. (No speaker columns on chunks — see the RLS note below.)
--   * ATTRIBUTION — `diarization_turns`: word-level, speaker-labeled turns used for
--               reader labels, clip cutting, the podcast. NOT embedded.
--
-- Two text representations on a transcript document:
--   * documents.content      = CANONICAL (name-corrected) text — embedded + displayed.
--   * documents.raw_content  = RAW verbatim (as-heard, clean WhisperX). PUBLIC BY
--     DESIGN: the reader offers a "show raw transcript" toggle. It passes the same
--     ingest PII / closed-session guards as `content`. PDFs/minutes leave it NULL.
-- Only proper nouns sourced from the place lexicon are canonicalized; every change
-- is logged in `name_canonicalizations` (reversible + transparent).
--
-- Additive + idempotent (CREATE/ALTER ... IF NOT EXISTS; DROP POLICY then CREATE).

-- 1. Raw verbatim alongside canonical (documents.content stays the canonical text).
ALTER TABLE documents ADD COLUMN IF NOT EXISTS raw_content TEXT;

-- NOTE: deliberately NO speaker column on `chunks`. `chunks` is public; a denormalized
-- subject_id there would bypass the high/confirmed display gate on speaker_identities
-- (codex review 2026-06-27). Speaker-filtered search derives from speaker_identities
-- (gated) joined to diarization_turns by time, at query time.

-- 2. Name-canonicalization audit log (internal provenance for every raw->canonical edit).
CREATE TABLE IF NOT EXISTS name_canonicalizations (
    id          SERIAL PRIMARY KEY,
    document_id INT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    char_start  INT CHECK (char_start >= 0),   -- offset into raw_content
    raw_token   TEXT NOT NULL,                 -- the as-heard form
    canonical   TEXT NOT NULL,                 -- the corrected spelling
    source      TEXT CHECK (source IN ('lexicon', 'auto_discovery', 'manual')),
    score       REAL CHECK (score BETWEEN 0 AND 1),  -- fuzzy confidence (NULL = exact/manual)
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_name_canon_doc ON name_canonicalizations (document_id, char_start);

-- 3. Media assets — a stable id per meeting recording (for clip resolution). One per
--    transcript document; turns reach their asset via document_id (no direct FK).
CREATE TABLE IF NOT EXISTS media_assets (
    id          SERIAL PRIMARY KEY,
    document_id INT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    entity_id   INT REFERENCES entities(id) ON DELETE SET NULL,
    source_url  TEXT NOT NULL,
    kind        TEXT NOT NULL CHECK (kind IN ('video', 'audio')) DEFAULT 'video',
    duration_seconds REAL CHECK (duration_seconds >= 0),
    content_hash TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (document_id, source_url)
);
CREATE INDEX IF NOT EXISTS idx_media_assets_doc ON media_assets (document_id);
CREATE INDEX IF NOT EXISTS idx_media_assets_entity ON media_assets (entity_id);

-- 4. Diarization turns — the attribution layer (anonymous speaker clusters in time).
CREATE TABLE IF NOT EXISTS diarization_turns (
    id            SERIAL PRIMARY KEY,
    document_id   INT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    cluster_label TEXT NOT NULL,        -- "SPEAKER_00" (per-document, anonymous)
    start_seconds REAL NOT NULL CHECK (start_seconds >= 0),
    end_seconds   REAL NOT NULL,
    words         JSONB CHECK (words IS NULL OR jsonb_typeof(words) = 'array'),  -- [{word,start,end}]
    source_model  TEXT,                 -- "pyannote/speaker-diarization-3.1"
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    CHECK (end_seconds > start_seconds)
);
CREATE INDEX IF NOT EXISTS idx_diar_turns_doc ON diarization_turns (document_id, start_seconds);

-- 5. Cluster -> identity. One row per (document, cluster). Public display name is gated
--    to high/confirmed at the RLS layer below; a high/confirmed row must name a subject.
CREATE TABLE IF NOT EXISTS speaker_identities (
    id           SERIAL PRIMARY KEY,
    document_id  INT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    cluster_label TEXT NOT NULL,
    subject_id   INT REFERENCES subjects(id) ON DELETE SET NULL,
    confidence   TEXT NOT NULL DEFAULT 'unknown' CHECK (confidence IN
                   ('unknown', 'inferred_low', 'inferred_medium', 'inferred_high', 'confirmed')),
    basis        TEXT CHECK (basis IN ('rollcall', 'vote_anchor', 'self_intro', 'manual')),
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (document_id, cluster_label),
    -- a publicly-displayable identity must actually name someone
    CHECK (confidence NOT IN ('inferred_high', 'confirmed') OR subject_id IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS idx_speaker_ident_doc ON speaker_identities (document_id);
CREATE INDEX IF NOT EXISTS idx_speaker_ident_subject ON speaker_identities (subject_id);

-- RLS. Public-record data (no PII beyond public officials): anon may read the
-- anonymous turns and the media index; identity is exposed only when high/confirmed
-- (the public-display gate, enforced here). The canonicalization audit log is
-- internal provenance: RLS on + no anon policy + explicit REVOKE (defense in depth).
-- All writes go through the service key (the cloud pipeline), which bypasses RLS.

ALTER TABLE name_canonicalizations ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON name_canonicalizations FROM anon, authenticated;
REVOKE ALL ON SEQUENCE name_canonicalizations_id_seq FROM anon, authenticated;

ALTER TABLE media_assets ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS anon_read_media_assets ON media_assets;
CREATE POLICY anon_read_media_assets ON media_assets
    FOR SELECT TO anon, authenticated USING (TRUE);

ALTER TABLE diarization_turns ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS anon_read_diar_turns ON diarization_turns;
CREATE POLICY anon_read_diar_turns ON diarization_turns
    FOR SELECT TO anon, authenticated USING (TRUE);

ALTER TABLE speaker_identities ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS anon_read_confident_identities ON speaker_identities;
CREATE POLICY anon_read_confident_identities ON speaker_identities
    FOR SELECT TO anon, authenticated
    USING (confidence IN ('inferred_high', 'confirmed'));
