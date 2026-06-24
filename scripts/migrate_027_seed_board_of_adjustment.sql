-- Migration 027: seed the Clayton Board of Adjustment.
--
-- A third Tier-1 city body on the zoning side: the Board of Adjustment hears
-- zoning variances and appeals of the zoning administrator's decisions. It is a
-- distinct body from the Plan Commission / ARB (separate members, separate
-- CivicPlus category, its own meeting videos), so /mo/clayton/board-of-adjustment
-- resolves and its agendas, minutes, and transcripts ingest under it. The board
-- meets only when a case is filed, so its record is sparse by nature.
-- Additive + idempotent; no existing rows change.

INSERT INTO entities (place_id, body_slug, type, display_name, external_ids)
SELECT p.id, 'board-of-adjustment', 'board_of_adjustment',
       'Board of Adjustment', '{}'::jsonb
FROM places p
WHERE p.state = 'mo' AND p.slug = 'clayton'
ON CONFLICT (place_id, body_slug) DO NOTHING;
