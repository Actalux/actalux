-- migrate_017_date_source.sql
--
-- Adds date_source provenance column to documents.
--
-- Tracks how meeting_date was derived so auditors can distinguish
-- filename-parsed dates from ingest-day fallbacks:
--   'filename' — parsed from the document title or source_file by parse_meeting_date
--   'content'  — written by redate_from_content.py from a verbatim body anchor
--   'manual'   — set by an operator for one-off corrections
--   'default'  — fell back to date.today() at ingest time (suspect; needs review)
--   'unknown'  — legacy rows ingested before this column existed
--
-- Idempotent: IF NOT EXISTS means repeated runs are safe.

ALTER TABLE documents
  ADD COLUMN IF NOT EXISTS date_source TEXT DEFAULT 'unknown';
