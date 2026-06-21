-- Migration 022: seed the Clayton City Council entity (multi-body Phase 1).
--
-- Adds City Council as a second public body under the existing mo/clayton place,
-- so /mo/clayton/council resolves and council meeting transcripts (YouTube
-- @CityofClayton, filtered to council) ingest under it. Additive + idempotent:
-- no existing rows change, and re-running is a no-op.
--
-- The YouTube channel + the title filter that separates council videos from the
-- city channel's other bodies (Plan Commission/ARB, Board of Adjustment, etc.)
-- live in code (actalux.ingest.bodies), so external_ids stays empty until a
-- verified civic identifier exists — no invented values.

INSERT INTO entities (place_id, body_slug, type, display_name, external_ids)
SELECT p.id, 'council', 'city_council', 'Clayton City Council', '{}'::jsonb
FROM places p
WHERE p.state = 'mo' AND p.slug = 'clayton'
ON CONFLICT (place_id, body_slug) DO NOTHING;
