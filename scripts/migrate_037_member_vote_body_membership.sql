-- Migration 037: member_vote_records must require body membership (Model B safety).
-- Run via apply_migrations.py (or the Supabase SQL editor).
--
-- migrate_030's member_vote_records gated rows on s.publishable + current document +
-- complete projection, but NOT on the edge's subject actually being a member of the
-- vote document's body. With the per-board person model (Model B), that gap can serve
-- a CROSS-BODY mis-attribution during the window between the roster seeder splitting a
-- person into per-board subjects and the projector finishing its re-projection: a
-- not-yet-reprojected Plan Commission vote edge still points at the person's COUNCIL
-- subject (the old lumped row's id), and the view would happily serve it as a council
-- member "voting" on a PC matter.
--
-- This recreates the view with an EXISTS check that the edge's from_subject holds a
-- membership in the vote document's body. The integrity is now in the schema, not in
-- the projector running to completion (connections-graph doctrine):
--   * pre-migration: the lumped subject is a member of BOTH bodies, so its council AND
--     PC edges both pass — a no-op.
--   * post-migration (complete): every edge is on the per-board subject whose membership
--     matches the body — a no-op.
--   * mid-migration / partial projection: a stale council-subject edge on a PC document
--     has no PC membership for that subject -> filtered out (not served). Safe by
--     construction, regardless of projector progress.
-- It also drops the inert matter 'considered' edges that were technically in this view
-- (a matter subject has no memberships); those belong to matter_vote_records, never the
-- member view, so member_records is unaffected.
--
-- Body-LEVEL (not date-bounded): any membership on the body keeps the edge, so a vote
-- outside a poorly-sourced term window is never wrongly dropped. The membership lookup
-- rides idx_memberships_subject (migrate_029).
--
-- Idempotent (CREATE OR REPLACE VIEW; GRANT is repeatable). The column list is
-- unchanged from migrate_030, so CREATE OR REPLACE is valid.

CREATE OR REPLACE VIEW member_vote_records
WITH (security_invoker = true) AS
SELECT
    e.id                    AS edge_id,
    e.from_subject          AS subject_id,
    s.slug                  AS subject_slug,
    s.canonical_name        AS subject_name,
    s.place_id              AS place_id,
    e.type                  AS edge_type,
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
JOIN subjects s   ON s.id = e.from_subject AND s.publishable
JOIN documents d  ON d.id = e.vote_document_id AND d.replaces_id IS NULL
JOIN votes v      ON v.document_id = e.vote_document_id AND v.vote_ref = e.vote_ref
WHERE e.projection_complete
  AND e.vote_ref IS NOT NULL
  -- The subject must be a member of the body the vote happened in. Closes the
  -- cross-body mis-attribution gap during partial re-projection (Model B).
  AND EXISTS (
      SELECT 1 FROM memberships m
      WHERE m.subject_id = e.from_subject AND m.entity_id = d.entity_id
  );

GRANT SELECT ON member_vote_records TO anon, authenticated;
