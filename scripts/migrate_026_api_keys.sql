-- Migration 026: API keys + per-period usage for the v1 JSON API
-- Run in the Supabase SQL editor (or via apply_migrations.py).
--
-- Makes the read-only JSON API monetization-READY (no billing here): a presented
-- key is looked up by its sha256 hash, mapped to a tier (rate limits + monthly
-- quota), and its usage is counted per calendar month. The raw key is never
-- stored — only its sha256 hex — so a DB leak cannot reconstruct live keys.
--
-- Safety property: this path is dormant until a key is actually issued. A request
-- with no key (the only state in prod today) never touches these tables.
--
-- Both tables get RLS with NO anon policy, so the publishable (anon) key cannot
-- read or write them directly (keys + usage are sensitive). Access is exclusively
-- through api_key_authorize(), a SECURITY DEFINER function that runs as its owner
-- and so reaches the tables despite RLS — mirroring how migrate_007 reasoned about
-- search RPCs, but inverted: search stayed SECURITY INVOKER because anon already
-- had row access; here anon has none, so the function must be DEFINER. EXECUTE is
-- granted to anon + authenticated to match the search RPCs' callable surface.
-- Idempotent throughout (IF NOT EXISTS / CREATE OR REPLACE / DROP-then-CREATE).

-- ---- Tables ----

CREATE TABLE IF NOT EXISTS api_keys (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    key_hash      TEXT UNIQUE NOT NULL,        -- sha256 hex of the raw key; raw key never stored
    label         TEXT NOT NULL,               -- human label for the holder (who/what it's for)
    tier          TEXT NOT NULL DEFAULT 'developer',
    active        BOOLEAN NOT NULL DEFAULT TRUE,
    monthly_quota INTEGER,                      -- NULL = unlimited (no quota gate)
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    expires_at    TIMESTAMPTZ                   -- NULL = never expires
);

CREATE TABLE IF NOT EXISTS api_key_usage (
    api_key_id BIGINT NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
    period     TEXT NOT NULL,                   -- usage window key, e.g. 'YYYY-MM'
    count      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (api_key_id, period)
);

-- ---- RLS: deny anon all direct access (no policy = denied under RLS) ----
ALTER TABLE api_keys      ENABLE ROW LEVEL SECURITY;
ALTER TABLE api_key_usage ENABLE ROW LEVEL SECURITY;
-- No CREATE POLICY for either table: with RLS on and no matching policy, anon and
-- authenticated are denied SELECT/INSERT/UPDATE/DELETE. The service key bypasses
-- RLS (issue_api_key.py uses it to INSERT new keys), and api_key_authorize()
-- reaches the rows via SECURITY DEFINER.

-- ---- Authorize + meter one call ----
-- Returns exactly one row. valid=false when no active, non-expired key matches the
-- hash (caller -> 401). When it matches, atomically increments this period's usage
-- and returns the tier plus over_quota = (monthly_quota set AND new count exceeds
-- it) so the caller can 429 a key that is valid but over its monthly allowance. The
-- usage period (calendar month, UTC) is derived in SQL, not taken from the caller,
-- so a client cannot point its metering at an arbitrary period.
CREATE OR REPLACE FUNCTION api_key_authorize(p_key_hash TEXT)
RETURNS TABLE (valid BOOLEAN, tier TEXT, over_quota BOOLEAN)
LANGUAGE plpgsql
SECURITY DEFINER
-- Pin search_path so a DEFINER function can't be hijacked by a caller-set path.
SET search_path = public, pg_temp
AS $$
DECLARE
    v_id     BIGINT;
    v_tier   TEXT;
    v_quota  INTEGER;
    v_count  INTEGER;
    v_period TEXT := to_char(timezone('utc', now()), 'YYYY-MM');
BEGIN
    SELECT k.id, k.tier, k.monthly_quota
      INTO v_id, v_tier, v_quota
      FROM api_keys k
     WHERE k.key_hash = p_key_hash
       AND k.active = TRUE
       AND (k.expires_at IS NULL OR k.expires_at > NOW());

    IF v_id IS NULL THEN
        RETURN QUERY SELECT FALSE, NULL::TEXT, FALSE;
        RETURN;
    END IF;

    -- Atomic upsert-and-increment: the row is created on first call in a period,
    -- and incremented thereafter. RETURNING gives the post-increment count.
    INSERT INTO api_key_usage (api_key_id, period, count)
    VALUES (v_id, v_period, 1)
    ON CONFLICT (api_key_id, period)
    DO UPDATE SET count = api_key_usage.count + 1
    RETURNING api_key_usage.count INTO v_count;

    RETURN QUERY
    SELECT TRUE, v_tier, (v_quota IS NOT NULL AND v_count > v_quota);
END;
$$;

-- Strip the default PUBLIC EXECUTE grant before re-granting, so only anon +
-- authenticated (the search RPCs' callable surface) can invoke this DEFINER
-- function — not every role implicitly via PUBLIC.
REVOKE ALL ON FUNCTION api_key_authorize(TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION api_key_authorize(TEXT) TO anon, authenticated;
