-- Migration 031: matters read seam (connections-graph Phase 2).
-- Run in the Supabase SQL editor (or via apply_migrations.py).
--
-- Phase 2 adds MATTER subjects (type='matter') — a council bill or resolution with
-- a stable number — and links each council vote that acts on it via a 'considered'
-- edge (from_subject=matter, vote target by the durable (vote_document_id, vote_ref)
-- pair). A matter is publishable on its own (the minting trigger gates only persons),
-- and the existing anon_read_publishable_edges policy already admits a matter->vote
-- edge (from_subject publishable, to_subject NULL). So this migration is additive:
-- one partial unique index + one read view. No table or policy change.
--
-- Idempotent (CREATE ... IF NOT EXISTS / CREATE OR REPLACE VIEW; GRANT repeatable).

-- One 'considered' edge per (matter, vote) — mirrors the vote-role partial index in
-- migration 029, scoped to the new edge type. Keyed on columns that survive
-- re-ingest (never chunk_id).
CREATE UNIQUE INDEX IF NOT EXISTS idx_edges_vote_matter
    ON edges (vote_document_id, from_subject, vote_ref)
    WHERE type = 'considered';

-- The matter dossier + JSON API read a matter's cited timeline as one denormalized
-- row set: each 'considered' edge joined to the vote it cites (motion/result/tally)
-- and that vote's current document (date/title/origin). SECURITY INVOKER so the
-- deny-by-default RLS from migration 029 still applies (a DEFINER view would run as
-- owner and bypass it). publishable + replaces_id + projection_complete are baked in
-- so every consumer reads the same gated, current, complete set.
CREATE OR REPLACE VIEW matter_vote_records
WITH (security_invoker = true) AS
SELECT
    e.id                    AS edge_id,
    e.from_subject          AS subject_id,
    s.slug                  AS subject_slug,
    s.canonical_name        AS subject_name,
    s.place_id              AS place_id,
    s.metadata              AS subject_metadata,   -- {kind, number, title, source}
    e.as_of_date            AS as_of_date,
    e.vote_document_id      AS document_id,
    e.citation_id           AS citation_id,
    e.source_quote          AS source_quote,
    v.id                    AS vote_id,
    v.motion                AS motion,
    v.result                AS result,
    v.result_basis          AS result_basis,
    v.vote_count_yes        AS vote_count_yes,
    v.vote_count_no         AS vote_count_no,
    v.vote_count_abstain    AS vote_count_abstain,
    d.entity_id             AS entity_id,
    d.meeting_date          AS meeting_date,
    d.meeting_title         AS meeting_title,
    d.video_id              AS video_id,
    d.source_url            AS source_url,
    d.source_portal         AS source_portal,
    d.source_file           AS source_file
FROM edges e
-- publishable + matter filter is belt-and-suspenders under RLS; keeps the view
-- correct even when read by the service key (which bypasses RLS).
JOIN subjects s   ON s.id = e.from_subject AND s.publishable AND s.type = 'matter'
JOIN documents d  ON d.id = e.vote_document_id AND d.replaces_id IS NULL
-- INNER: an edge whose vote is momentarily unresolvable carries no motion to show,
-- so it is withheld until the projection is consistent again.
JOIN votes v      ON v.document_id = e.vote_document_id AND v.vote_ref = e.vote_ref
WHERE e.projection_complete
  AND e.type = 'considered';

GRANT SELECT ON matter_vote_records TO anon, authenticated;
