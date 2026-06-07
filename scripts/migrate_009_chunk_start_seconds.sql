-- Migration 009: start_seconds on chunks (YouTube citation cue)
-- Run via apply_migrations.py (records in schema_migrations); never the SQL editor.
--
-- For YouTube board-meeting docs, a chunk is a passage spoken at a point in the
-- video. Holding that offset lets the reader pane cue the embedded player to the
-- cited moment (?start=<seconds>). NULL for every non-video chunk and for video
-- chunks the timestamp backfill could not align; the reader pane then just starts
-- the video at 0:00. RLS is unchanged -- the existing anon SELECT on chunks is
-- table-level, so the new column reads fine.

ALTER TABLE chunks ADD COLUMN IF NOT EXISTS start_seconds INT;
