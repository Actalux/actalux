-- Migration 033: name-corrections lexicon (proper-noun spelling fixes).
-- Run in the Supabase SQL editor (or via apply_migrations.py).
--
-- A jurisdiction-scoped map of mangling -> canonical spelling for Clayton proper
-- nouns (officials, staff, streets, businesses, schools). The transcripts are ASR
-- of meeting video and the minutes are OCR of scans, so both carry name manglings;
-- this lexicon is the single home for the fixes. Two consumers:
--   1. Search recall — a query for the canonical name also matches chunks that carry
--      a known mangling (and vice versa). Stored text is NEVER rewritten; the
--      corrections widen the query, preserving verbatim citation integrity.
--   2. The downstream newsletter, which replaces its hand-maintained correction list
--      with this one (served via GET /api/v1/{state}/{place}/corrections).
--
-- Jurisdiction-scoped by place_id (cardinal repo rule): the same string can be a
-- mangling in one town and a real name in another, so every row + lookup is per
-- place. Distinct from the existing `corrections` table (the public bug-report
-- inbox) — this one holds spelling rules, not reports.
--
-- Additive + idempotent (CREATE ... IF NOT EXISTS; DROP POLICY then CREATE).

CREATE TABLE IF NOT EXISTS name_corrections (
    id          SERIAL PRIMARY KEY,
    place_id    INT NOT NULL REFERENCES places(id),
    mangled     TEXT NOT NULL,   -- the wrong form; consumers match it case-insensitively, word-boundaried
    canonical   TEXT NOT NULL,   -- the correct spelling
    category    TEXT,            -- person | staff | street | business | school | place | org | other
    provenance  TEXT,            -- asr | ocr | reviewed
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (place_id, mangled)
);

CREATE INDEX IF NOT EXISTS idx_name_corrections_place ON name_corrections (place_id) WHERE active;

ALTER TABLE name_corrections ENABLE ROW LEVEL SECURITY;

-- Public-record spelling data (no PII): anon may read active rows. Writes go through
-- the service key (the seeder), like the roster.
DROP POLICY IF EXISTS anon_read_active_name_corrections ON name_corrections;
CREATE POLICY anon_read_active_name_corrections ON name_corrections
    FOR SELECT TO anon, authenticated USING (active = TRUE);
