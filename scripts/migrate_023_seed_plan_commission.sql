-- Migration 023: seed the Clayton Plan Commission / Architectural Review Board.
--
-- The second Tier-1 city body (land-use / zoning), so /mo/clayton/plan-commission
-- resolves and its meeting transcripts (YouTube @CityofClayton, filtered to
-- PC/ARB) ingest under it. The Plan Commission and the Architectural Review Board
-- are one entity here: Clayton runs them as a single body with the same members
-- and joint minutes. Additive + idempotent; no existing rows change.

INSERT INTO entities (place_id, body_slug, type, display_name, external_ids)
SELECT p.id, 'plan-commission', 'plan_commission',
       'Plan Commission & Architectural Review Board', '{}'::jsonb
FROM places p
WHERE p.state = 'mo' AND p.slug = 'clayton'
ON CONFLICT (place_id, body_slug) DO NOTHING;
