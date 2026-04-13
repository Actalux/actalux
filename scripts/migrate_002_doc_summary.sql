-- Migration 002: per-document one-sentence summary
-- Run in Supabase SQL editor.
--
-- The summary describes what the document is. It is intrinsic to the
-- document (not a search result), so it is generated once at ingest
-- time and stored here. Backfill existing rows with
-- scripts/backfill_doc_summaries.py.

ALTER TABLE documents ADD COLUMN IF NOT EXISTS summary TEXT DEFAULT '';
