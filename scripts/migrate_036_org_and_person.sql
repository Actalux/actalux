-- Migration 036: organization tier + per-board person model (Phase 0 of the
-- org/geography + person refactor). Run in the Supabase SQL editor or via
-- apply_migrations.py. Spec: docs/architecture/phase0-person-org-schema.md.
--
-- This is *schema only* (pure DDL). The Clayton-specific data — which
-- organizations exist, the per-board subject split, the persons rows, the edge
-- re-projection — lands in the Phase 0b data-migration (Python, reads per-place
-- config), NOT here: a migration must stay jurisdiction-agnostic (CLAUDE.md
-- "per-place config, not constants").
--
-- Model B (chosen on the safe-scalability axis): person identity is STRUCTURAL.
-- A person's record on each governing body is its own `subjects` row (one per
-- body), and the rows are tied together by a `persons` row via `subjects.person_id`
-- (the "is also this person" pointer). Cross-body / cross-place sameness is
-- therefore an explicit, reversible link (repoint person_id), never an implicit
-- name collapse — different boards are different rows by construction, so a buggy
-- seeder cannot merge two people. This replaces the procedural seeder guard with a
-- schema guarantee (the doctrine of migrate_029: integrity in the schema, not by
-- convention).
--
-- Geography stays the door we keep open: an org relates to geography through the
-- `org_serves_place` link (serves != owns), so `places` can later carry boundary
-- geometry and "all governments for a location" becomes a point-in-polygon query
-- with no schema rewrite.
--
-- Additive + idempotent throughout (CREATE ... IF NOT EXISTS, ADD COLUMN IF NOT
-- EXISTS, CREATE OR REPLACE, DROP-then-CREATE for trigger/policy, guarded DROP
-- CONSTRAINT). All existing document/chunk/vote ids + citations are untouched.

-- ---- Organization tier (public reference data, like entities/places: no RLS) ----

-- A legal public entity that creates records: a municipality, school district,
-- county, transit/sewer/water/library/fire district, authority, etc. The body
-- (entities) is what meets; the organization is what the body belongs to.
CREATE TABLE IF NOT EXISTS organizations (
    id                SERIAL PRIMARY KEY,
    slug              TEXT NOT NULL,
    name              TEXT NOT NULL,
    organization_type TEXT,   -- municipality|school_district|county|transit_authority|special_district|...
    state             TEXT,   -- 'mo'
    website           TEXT,
    description       TEXT,
    metadata          JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (state, slug)
);

-- Geography as a RELATIONSHIP, not ownership: which geographies an organization
-- serves (serves != owns). 1:1 today (each Clayton org serves place clayton); the
-- seam that later upgrades to boundary polygons + spatial overlay. relation lets a
-- future "headquartered_in" etc. coexist with "serves".
CREATE TABLE IF NOT EXISTS org_serves_place (
    id              SERIAL PRIMARY KEY,
    organization_id INT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    place_id        INT NOT NULL REFERENCES places(id),
    relation        TEXT NOT NULL DEFAULT 'serves',
    UNIQUE (organization_id, place_id, relation)
);

-- Body -> organization. Additive: keep every entities.id and the existing
-- UNIQUE(place_id, body_slug); add the org parent + org-scoped body uniqueness so
-- two organizations serving one place can each own a 'planning-commission'.
ALTER TABLE entities ADD COLUMN IF NOT EXISTS organization_id INT REFERENCES organizations(id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_entities_org_body ON entities (organization_id, body_slug);
CREATE INDEX IF NOT EXISTS idx_entities_organization ON entities (organization_id);

-- ---- Person identity (the global unique id) ----

-- The canonical human. Sits ABOVE place/state: the slug is globally unique, so two
-- different people who share a name get two distinct slugs (john-smith,
-- john-p-smith) and two pages. publishable mirrors the subjects privacy gate.
CREATE TABLE IF NOT EXISTS persons (
    id             SERIAL PRIMARY KEY,
    slug           TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    publishable    BOOLEAN NOT NULL DEFAULT FALSE,   -- privacy gate (trigger below)
    metadata       JSONB NOT NULL DEFAULT '{}'::jsonb,   -- merge provenance / sources
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (slug)
);

-- ---- subjects: per-board attestation + the is-also pointer ----

-- person_id ties a person's per-board subjects together (the reversible merge).
-- entity_id is the body THIS person-subject attests (NULL for non-person subjects
-- like matters). The Phase 0b data-migration backfills both and splits the 7
-- existing multi-board person-subjects into one row per board.
ALTER TABLE subjects ADD COLUMN IF NOT EXISTS person_id INT REFERENCES persons(id);
ALTER TABLE subjects ADD COLUMN IF NOT EXISTS entity_id INT REFERENCES entities(id);
CREATE INDEX IF NOT EXISTS idx_subjects_person ON subjects (person_id);
CREATE INDEX IF NOT EXISTS idx_subjects_entity ON subjects (entity_id);

-- IMPORTANT: KEEP the existing blanket UNIQUE(place_id, type, slug). Dropping it
-- would break auto-running consumers that upsert ON CONFLICT (place_id, type, slug):
-- seed_roster.py (run by ingest.yml + crawl_minutes.yml) and store.upsert_matters
-- (the projector minting matter-subjects). So this migration does NOT touch that
-- constraint — it stays purely additive and safe to apply standalone.
--
-- Per-board person identity is achieved WITHOUT dropping the constraint: when the
-- 0b data-migration splits a multi-board person, the non-primary board rows get a
-- distinct *internal* slug (e.g. '{slug}--{body_slug}'), so the blanket key still
-- holds. The PUBLIC person identity is persons.slug (global, clean); the member-in-
-- board page resolves by persons.slug + body (via person_id + entity_id), which
-- preserves every existing member URL (a single-board person's persons.slug equals
-- its old subject slug). subjects.slug thus becomes an internal attestation key, not
-- a public URL.

-- ---- persons privacy: minting gate (trigger) + deny-by-default RLS ----

-- Mirrors subjects_minting_gate (migrate_029 §6): a publishable person needs at
-- least one publishable subject (an attested official), unless explicitly reviewed.
-- Fires even under the service key (which bypasses RLS but not triggers), so the
-- data-migration cannot mint a publishable person off the street. Seed order:
-- insert person publishable=false, create+publish its subject, then flip the person
-- publishable=true (the subject now exists -> gate passes).
CREATE OR REPLACE FUNCTION persons_minting_gate()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public, pg_temp
AS $$
BEGIN
    IF NEW.publishable THEN
        IF (NEW.metadata->>'minting_basis') IS DISTINCT FROM 'reviewed'
           AND NOT EXISTS (
               SELECT 1 FROM subjects s
               WHERE s.person_id = NEW.id AND s.publishable
           ) THEN
            RAISE EXCEPTION
                'person % cannot be publishable without a publishable subject '
                'or metadata.minting_basis=reviewed (phase0-person-org-schema)', NEW.id;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_persons_minting_gate ON persons;
CREATE TRIGGER trg_persons_minting_gate
    BEFORE INSERT OR UPDATE ON persons
    FOR EACH ROW EXECUTE FUNCTION persons_minting_gate();

-- RLS: anon reads only publishable persons (the public read path uses the anon
-- key; the service key bypasses RLS for the data-migration + projector).
ALTER TABLE persons ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS anon_read_publishable_persons ON persons;
CREATE POLICY anon_read_publishable_persons ON persons
    FOR SELECT TO anon, authenticated USING (publishable = TRUE);
