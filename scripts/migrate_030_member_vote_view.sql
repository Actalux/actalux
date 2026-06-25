-- Migration 030: read seam for member voting records (connections-graph Phase 1).
-- Run in the Supabase SQL editor (or via apply_migrations.py).
--
-- The dossier page and JSON API need a member's voting record as one denormalized
-- read: each edge joined to the vote it cites (motion/result/tally) and that vote's
-- document (date/title/origin). edges reference votes by the durable pair
-- (vote_document_id, vote_ref) -- a composite, not an FK -- which PostgREST cannot
-- embed, so the join lives here in a view (migrate_029 deferred the read views to
-- this Phase 1 migration so their columns match the actual dossier query).
--
-- SECURITY INVOKER (not DEFINER): the view runs with the CALLER's privileges, so
-- the deny-by-default RLS from migrate_029 still applies -- anon sees a row only
-- for a publishable subject. A SECURITY DEFINER view would run as owner and bypass
-- RLS, re-opening the privacy hole the trigger cannot guard (see migrate_007/026).
-- The publishable + replaces_id + projection_complete filters are baked in so every
-- consumer reads the same gated, current, complete set.
--
-- Idempotent (CREATE OR REPLACE VIEW; GRANT is repeatable).

CREATE OR REPLACE VIEW member_vote_records
WITH (security_invoker = true) AS
SELECT
    e.id                    AS edge_id,
    e.from_subject          AS subject_id,
    s.slug                  AS subject_slug,
    s.canonical_name        AS subject_name,
    s.place_id              AS place_id,
    e.type                  AS edge_type,   -- voted_aye_on|voted_no_on|voted_abstain_on|moved|seconded
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
-- publishable filter is belt-and-suspenders under RLS; keeps the view correct even
-- when read by the service key (which bypasses RLS).
JOIN subjects s   ON s.id = e.from_subject AND s.publishable
JOIN documents d  ON d.id = e.vote_document_id AND d.replaces_id IS NULL
-- INNER: a vote edge whose vote is momentarily unresolvable carries no motion to
-- show, so it is withheld until the projection is consistent again (never rendered
-- as an empty record).
JOIN votes v      ON v.document_id = e.vote_document_id AND v.vote_ref = e.vote_ref
WHERE e.projection_complete
  AND e.vote_ref IS NOT NULL;   -- vote-anchored edges only (Phase 1)

-- PostgREST exposes the view to the anon/publishable key; SELECT must be granted.
GRANT SELECT ON member_vote_records TO anon, authenticated;
