-- Migration 028: durable per-vote identity (vote_ref).
--
-- Phase 0 prerequisite for the connections graph
-- (docs/architecture/connections-graph.md §4.2). The votes table keys on a SERIAL
-- id, but extract_votes delete/reinserts vote rows on every run (reassigning it),
-- so it cannot anchor a graph edge. vote_ref is content-addressed over the vote's
-- citing chunk, computed by extract_votes (ingest.hashing.compute_vote_ref):
--
--     vote_ref = sha256(citation_id || ':' || ordinal_within_chunk)
--
-- where ordinal_within_chunk is the vote's appearance order among the votes that
-- resolve to the same citation_id within one document (so two motions sharing a
-- chunk earn distinct refs). citation_id is itself stable across re-ingest
-- (migration 018), so vote_ref is stable within a document version; the graph
-- projects vote edges per version (rebuilt on re-ingest), so it need not survive a
-- re-version. extract_votes hard-errors on a citing chunk with no citation_id
-- rather than emit sha256(':N') and collide — run backfill_citation_ids.py first.
--
-- Additive + idempotent. RLS: votes already has anon SELECT (migration 007).

ALTER TABLE votes ADD COLUMN IF NOT EXISTS vote_ref TEXT;

-- One vote_ref per document. Partial (WHERE vote_ref IS NOT NULL) because Postgres
-- lets multiple NULLs through a plain unique index, and rows written before this
-- migration (or before their citation_id is backfilled) carry a NULL vote_ref;
-- the partial index excludes them so they neither collide nor block the backfill.
CREATE UNIQUE INDEX IF NOT EXISTS idx_votes_document_voteref
    ON votes (document_id, vote_ref) WHERE vote_ref IS NOT NULL;
