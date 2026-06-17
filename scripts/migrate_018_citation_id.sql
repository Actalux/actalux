-- Migration 018: stable, content-addressed citation id for chunks.
--
-- The displayed citation hash and the /chunk/{id}/source deep-link were both
-- derived from the chunks.id SERIAL. Re-ingesting a document deletes and
-- re-inserts its chunks, so those ids are reassigned and every published
-- citation -- the #qXXXX hash, the reader-pane link, the GitHub error-report
-- URL, budget_line_items.chunk_id, and the hardcoded facilities chunk ids --
-- silently breaks or re-points.
--
-- citation_id is a content-addressed identity:
-- sha256(stable_doc_key + "\n" + normalized_content)[:8] (see
-- ingest.hashing.compute_citation_id), so re-ingesting the same source
-- reproduces the same id. Citations render and route on it; the numeric id
-- stays the internal primary key and keeps serving legacy /chunk/{int} links.
--
-- Additive and idempotent: the column is nullable (rows ingested before it
-- existed read NULL until the backfill fills them; render falls back to the row
-- id while NULL). The partial index serves /chunk/{citation_id} lookups and
-- skips the not-yet-backfilled NULLs. citation_id is not globally unique --
-- 8 hex tolerates a rare collision, resolved at routing by preferring the
-- current-version chunk -- so no UNIQUE constraint is added.
--
-- RLS: citation_id is a plain column on chunks; it inherits the table's
-- existing policies from migrate_007 (anon SELECT, service-key full access).

ALTER TABLE chunks ADD COLUMN IF NOT EXISTS citation_id TEXT;

CREATE INDEX IF NOT EXISTS chunks_citation_id
    ON chunks (citation_id)
    WHERE citation_id IS NOT NULL;
