-- Migration 039: make `places` a first-class geography layer (geography half of
-- the org/geography refactor). Run in the Supabase SQL editor or via
-- apply_migrations.py. Spec: docs/architecture/org-geography-refactor.md.
--
-- Phase 0 (migrate_036) added the organization tier and the org<->geography
-- RELATIONSHIP (org_serves_place; serves != owns). What stayed thin was geography
-- itself: `places` was a routing table (state/slug/display_name/county). This
-- migration promotes it to a typed, hierarchy-capable, geometry-READY first-class
-- layer -- the "two free forward-compat choices" from the 2026-06-28 design review:
-- geography is first-class and can later carry geometry, with no schema rebuild.
--
-- DEFERRED by design (not in this migration): the boundary/overlay ENGINE
-- (PostGIS geometry + point-in-polygon "which governments apply to this location").
-- The geometry seam is documented at the bottom so that build is a pure ALTER.
--
-- Schema only (pure DDL), jurisdiction-agnostic: which place is a city/county, its
-- county name, and its Census GEOID are Clayton-specific DATA and land in the
-- backfill (scripts/backfill_places.py reading scripts/places/<state>_<place>.json),
-- never here -- CLAUDE.md "per-place config, not constants", same split as 036.
--
-- Additive + idempotent (ADD COLUMN IF NOT EXISTS, CREATE INDEX IF NOT EXISTS).
-- No existing column, row, or id changes; documents/chunks/votes untouched. RLS is
-- already on for `places` with an anon read policy (migrate_038); new columns
-- inherit it, so no RLS change is needed here.

-- ---- Typed geography ----

-- What KIND of geography this row is. Free text + a documented vocabulary (matching
-- organizations.organization_type), so a new kind needs no migration. The geography
-- object layer (county/census_place/service_area rows) is greenfield/deferred; this
-- column types the rows that exist now and the ones added later.
ALTER TABLE places ADD COLUMN IF NOT EXISTS place_type TEXT;
--   place_type vocabulary: 'city' | 'county' | 'state' | 'census_place'
--                        | 'service_area' | 'school_district' | 'special_district'
COMMENT ON COLUMN places.place_type IS
    'Kind of geography: city|county|state|census_place|service_area|school_district|special_district|...';

-- ---- Geography hierarchy (self-reference) ----

-- The containing geography (city -> county -> state). NULL until the parent place
-- exists; the minimal-seam build does not mint county/state rows yet, so this stays
-- the seam, not populated data. ON DELETE SET NULL: removing a parent must not
-- cascade-delete its children.
ALTER TABLE places ADD COLUMN IF NOT EXISTS parent_place_id INT
    REFERENCES places(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_places_parent ON places (parent_place_id);
CREATE INDEX IF NOT EXISTS idx_places_type ON places (place_type);

-- ---- Census GEOID: the stable external join key ----

-- The Census GEOID (state FIPS + place/county FIPS) for this geography. This is the
-- key the deferred overlay build joins TIGER/Line boundary polygons on, so the
-- geometry can attach by a stable id rather than a name match. Globally unique per
-- geography, hence a partial unique index (NULLs allowed while unpopulated).
ALTER TABLE places ADD COLUMN IF NOT EXISTS geoid TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS uq_places_geoid ON places (geoid) WHERE geoid IS NOT NULL;

-- ---- Forward-compat metadata ----

-- Provenance + parent GEOIDs + future spatial attributes (e.g. centroid, area,
-- vintage) without a column per attribute. Mirrors organizations.metadata.
ALTER TABLE places ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;

-- ---- The geometry seam (DEFERRED -- documented so the future build is a pure ALTER) ----
--
-- When the boundary/overlay engine is built, geography carries real polygons with
-- NO schema rebuild -- it is exactly these statements, run as a later migration:
--
--   CREATE EXTENSION IF NOT EXISTS postgis;
--   ALTER TABLE places ADD COLUMN IF NOT EXISTS boundary geometry(MultiPolygon, 4326);
--   CREATE INDEX IF NOT EXISTS idx_places_boundary ON places USING GIST (boundary);
--
-- Polygons are loaded by joining `places.geoid` to the authoritative source for
-- that place_type (Census TIGER/Line for city/county/state; Census EDGE / School
-- District Review Program for school_district; MSDIS / St. Louis County GIS for
-- special_district). "Which governments apply to this location?" then becomes
-- geocode -> point-in-polygon over places.boundary, joined to org_serves_place.
