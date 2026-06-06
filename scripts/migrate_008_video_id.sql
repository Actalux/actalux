-- Migration 008: video_id on documents (YouTube board-meeting embeds)
-- Run via apply_migrations.py (records in schema_migrations); never the SQL editor.
--
-- YouTube board-meeting docs store a transcript as their source, so the reader
-- pane currently shows them as plain text. Holding the source video's id lets
-- the pane embed the player instead. Empty-string default so non-video docs are
-- unaffected; scripts/backfill_video_ids.py populates it for the docs whose
-- meeting has a public channel video. RLS is unchanged -- the existing anon
-- SELECT policy on documents is table-level, so the new column reads fine.

ALTER TABLE documents ADD COLUMN IF NOT EXISTS video_id TEXT DEFAULT '';
