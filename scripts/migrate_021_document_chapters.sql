-- Migration 021: chapters on documents (transcript topic navigation)
-- Run via apply_migrations.py (records in schema_migrations); never the SQL editor.
--
-- For YouTube board-meeting transcripts, `chapters` holds an ordered list of topic
-- sections with the video offset each begins at, so the reader can jump the video
-- to a topic. Shape: [{"t": <int seconds>, "title": "<neutral label>"}, ...].
-- NULL for every non-transcript document and for transcripts not yet processed.
-- RLS is unchanged -- the existing anon SELECT on documents is table-level, so the
-- new column reads fine; chapters are written with the service key.

ALTER TABLE documents ADD COLUMN IF NOT EXISTS chapters JSONB;
