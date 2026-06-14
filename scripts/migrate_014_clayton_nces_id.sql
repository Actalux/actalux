-- Migration 014: record the Clayton entity's NCES district ID.
--
-- migrate_012 deliberately seeded external_ids with only the DESE
-- county-district code ('096102') and left the NCES district ID blank until it
-- could be verified rather than guessed. Verified 2026-06-14 against the NCES
-- Common Core of Data district detail page (LEAID 2909720, State District ID
-- MO-096102 — the trailing 096102 cross-checks the DESE code already on file):
--   https://nces.ed.gov/ccd/districtsearch/district_detail.asp?ID2=2909720
--
-- Idempotent: the JSONB merge sets the same value on re-run.
UPDATE entities e
SET external_ids = e.external_ids || '{"nces": "2909720"}'::jsonb
FROM places p
WHERE e.place_id = p.id
  AND p.state = 'mo' AND p.slug = 'clayton'
  AND e.body_slug = 'schools';
