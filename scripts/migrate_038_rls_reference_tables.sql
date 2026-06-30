-- Migration 038: enable RLS on the tables created after migration 007
-- Run in the Supabase SQL editor (or via apply_migrations.py).
--
-- Migration 007 turned RLS on for every table that existed then. Tables added
-- since (entities/places in the multi-body work, organizations/org_serves_place
-- in the org refactor, and the schema_migrations ledger) shipped with RLS off,
-- which the Supabase linter flags as "RLS Disabled in Public": the publishable
-- (anon) key could read/write them. This closes that gap, following 007's split:
-- public reference data gets an anon SELECT policy; operator-only tables get
-- RLS with no policy (deny anon; the secret/service key and the Management-API
-- PAT both bypass RLS, so ingest, the web reader, and apply_migrations keep
-- working).

-- ---- Enable RLS (deny-by-default for anon) ----
ALTER TABLE entities         ENABLE ROW LEVEL SECURITY;
ALTER TABLE places           ENABLE ROW LEVEL SECURITY;
ALTER TABLE organizations    ENABLE ROW LEVEL SECURITY;
ALTER TABLE org_serves_place ENABLE ROW LEVEL SECURITY;
ALTER TABLE schema_migrations ENABLE ROW LEVEL SECURITY;

-- ---- Public read of the reference tables (anon = the publishable key) ----
-- These hold the jurisdictions, governing bodies, government organizations, and
-- org<->place mappings the whole archive is keyed on -- categorically public,
-- no PII. The web reader queries them with the publishable key, so anon needs
-- SELECT; without a policy, RLS would silently return zero rows and break page
-- resolution / org dossiers. Drop-then-create keeps the migration re-runnable.
DROP POLICY IF EXISTS anon_read_entities ON entities;
CREATE POLICY anon_read_entities ON entities
    FOR SELECT TO anon, authenticated USING (true);

DROP POLICY IF EXISTS anon_read_places ON places;
CREATE POLICY anon_read_places ON places
    FOR SELECT TO anon, authenticated USING (true);

DROP POLICY IF EXISTS anon_read_organizations ON organizations;
CREATE POLICY anon_read_organizations ON organizations
    FOR SELECT TO anon, authenticated USING (true);

DROP POLICY IF EXISTS anon_read_org_serves_place ON org_serves_place;
CREATE POLICY anon_read_org_serves_place ON org_serves_place
    FOR SELECT TO anon, authenticated USING (true);

-- No anon policy for schema_migrations: it is the operator's migration ledger
-- (version/filename/checksum/applied_at). RLS is on with no matching policy, so
-- anon is denied. apply_migrations.py reads/writes it through the Management API
-- (PAT), which bypasses RLS -- same posture as ingest_runs/sources/topic_alerts.
