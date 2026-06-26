-- Migration 032: lexicon read seam (glossary unification, task #67).
-- Run in the Supabase SQL editor (or via apply_migrations.py).
--
-- The place-level lexicon endpoint (GET /api/v1/{state}/{place}/lexicon) exposes,
-- for each public official, the canonical name and its known name variants so a
-- downstream consumer (the Clayton Ledger newsletter) can maintain proper-name
-- spellings in ONE place instead of duplicating them. The variants live in
-- subject_aliases, which migration 029 deliberately left anon-denied as "resolution
-- internals". But an alias of an ALREADY-publishable subject is just a name variant
-- of a public official, taken from the public minutes (including OCR drift) — it is
-- public-record-safe, exactly like the membership term windows opened in 029.
--
-- So this migration opens subject_aliases to anon for publishable subjects ONLY,
-- mirroring anon_read_publishable_memberships. subject_resolution_queue stays
-- anon-denied (it can hold private individuals' names); nothing about it changes.
-- The existing `source` column carries each alias's provenance ('roster' today;
-- 'ocr'/'asr'/'reviewed' as the feedback loop tags them later) and is what the
-- endpoint reports — no new column.
--
-- Additive + idempotent (DROP POLICY IF EXISTS then CREATE; no table change).

DROP POLICY IF EXISTS anon_read_publishable_aliases ON subject_aliases;
CREATE POLICY anon_read_publishable_aliases ON subject_aliases
    FOR SELECT TO anon, authenticated USING (
        EXISTS (
            SELECT 1 FROM subjects s
            WHERE s.id = subject_aliases.subject_id AND s.publishable
        )
    );
