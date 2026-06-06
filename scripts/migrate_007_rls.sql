-- Migration 007: Row-Level Security for public deployment
-- Run in the Supabase SQL editor (or via apply_migrations.py).
--
-- Until now RLS was off, so the publishable key had full read/write -- safe
-- only because the app was never public. Before the web app goes live on a
-- public host, lock the publishable (anon) key down to exactly what it needs:
-- read the public archive, and file an error report. Everything else is denied
-- to anon; the service (secret) key bypasses RLS, so ingest still writes.
--
-- IMPORTANT -- ship this WITH the code change that points ingest +
-- backfill/load scripts at ACTALUX_SUPABASE_SERVICE_KEY. Once RLS is on, the
-- publishable key can no longer write documents/chunks/budget, so any writer
-- still on the publishable key will start failing.

-- ---- Enable RLS on every application table (deny-by-default for anon) ----
ALTER TABLE documents         ENABLE ROW LEVEL SECURITY;
ALTER TABLE chunks            ENABLE ROW LEVEL SECURITY;
ALTER TABLE votes             ENABLE ROW LEVEL SECURITY;
ALTER TABLE speakers          ENABLE ROW LEVEL SECURITY;
ALTER TABLE budget_line_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE transcripts       ENABLE ROW LEVEL SECURITY;
ALTER TABLE corrections       ENABLE ROW LEVEL SECURITY;
ALTER TABLE topic_alerts      ENABLE ROW LEVEL SECURITY;
ALTER TABLE ingest_runs       ENABLE ROW LEVEL SECURITY;
ALTER TABLE sources           ENABLE ROW LEVEL SECURITY;

-- ---- Public read of the archive (anon = the publishable key) ----
-- Drop-then-create keeps the migration safely re-runnable.
DROP POLICY IF EXISTS anon_read_documents ON documents;
CREATE POLICY anon_read_documents ON documents
    FOR SELECT TO anon, authenticated USING (true);

DROP POLICY IF EXISTS anon_read_chunks ON chunks;
CREATE POLICY anon_read_chunks ON chunks
    FOR SELECT TO anon, authenticated USING (true);

DROP POLICY IF EXISTS anon_read_votes ON votes;
CREATE POLICY anon_read_votes ON votes
    FOR SELECT TO anon, authenticated USING (true);

DROP POLICY IF EXISTS anon_read_speakers ON speakers;
CREATE POLICY anon_read_speakers ON speakers
    FOR SELECT TO anon, authenticated USING (true);

DROP POLICY IF EXISTS anon_read_budget ON budget_line_items;
CREATE POLICY anon_read_budget ON budget_line_items
    FOR SELECT TO anon, authenticated USING (true);

-- Transcripts are public only once reviewed -- unreviewed drafts stay private.
DROP POLICY IF EXISTS anon_read_reviewed_transcripts ON transcripts;
CREATE POLICY anon_read_reviewed_transcripts ON transcripts
    FOR SELECT TO anon, authenticated USING (reviewed = true);

-- ---- Public may file an error report, but not read others' reports ----
-- corrections holds reporter_email (PII): INSERT only, no SELECT for anon.
DROP POLICY IF EXISTS anon_insert_corrections ON corrections;
CREATE POLICY anon_insert_corrections ON corrections
    FOR INSERT TO anon, authenticated WITH CHECK (true);

-- No anon policies for topic_alerts (subscriber emails), ingest_runs (operator
-- logs), or sources (crawl config) -- RLS is on with no matching policy, so anon
-- is denied. The service key bypasses RLS, so ingest keeps full access to them.

-- semantic_search / keyword_search are LANGUAGE sql (SECURITY INVOKER), so anon
-- runs them under these policies: anon has SELECT on chunks + documents, so the
-- search returns rows. Their default PUBLIC EXECUTE grant already lets anon call
-- them -- no extra grant needed, and no SECURITY DEFINER (which would bypass the
-- row policies we just set).
