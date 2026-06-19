-- Migration 020: cited, durable vote records.
--
-- The votes table predates the citation-first guarantee: it stored motion,
-- result, and tallies but had no link to the verbatim minutes passage they were
-- read from. This adds that link, mirroring budget_line_items (migrations 004 +
-- 019):
--   chunk_id      FK to chunks(id) ON DELETE SET NULL -- best-effort numeric
--                 reference; re-ingesting the minutes deletes its chunks and nulls
--                 this, so it is not the durable anchor.
--   citation_id   the chunk's stable, content-addressed id (migration 018), so a
--                 vote keeps citing the same passage across re-ingest; routing
--                 resolves it back to whatever chunk currently carries that id.
--   source_quote  the verbatim motion/tally/result block the record was parsed
--                 from (the hard provenance anchor: every count traces to text).
--   created_at    load timestamp, for "what's new" feeds.
--
-- The tally columns stay nullable (no NOT NULL): a NULL count means the minutes
-- recorded a result with no per-member tally, distinct from a recorded 0.
--
-- Additive and idempotent. RLS: votes already has anon SELECT from migrate_007.

ALTER TABLE votes ADD COLUMN IF NOT EXISTS chunk_id INT REFERENCES chunks(id) ON DELETE SET NULL;
ALTER TABLE votes ADD COLUMN IF NOT EXISTS citation_id TEXT;
ALTER TABLE votes ADD COLUMN IF NOT EXISTS source_quote TEXT DEFAULT '';
ALTER TABLE votes ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();
-- 'stated'  = the minutes printed a result word ("Carried"/"Motion Carries N-N").
-- 'derived' = no result line was printed, so passed/failed was computed from the
--             verbatim roll call (majority rule). Counts are verbatim either way;
--             this flags the result as inferred so a consumer can label it as such.
ALTER TABLE votes ADD COLUMN IF NOT EXISTS result_basis TEXT DEFAULT 'stated';

-- The loader re-derives a document's votes idempotently (delete-by-document then
-- insert), and the JSON API filters votes by their documents; index document_id.
CREATE INDEX IF NOT EXISTS idx_votes_document ON votes(document_id);
