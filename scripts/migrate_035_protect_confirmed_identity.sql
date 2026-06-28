-- Migration 035: protect human-confirmed speaker identities (atomic guard).
-- Run in the Supabase SQL editor (or via apply_migrations.py).
--
-- A speaker_identities row at confidence='confirmed' is a human decision (the
-- operator review tool). The automatic resolver re-runs on every re-transcribe and
-- upserts inferred_* rows with ON CONFLICT (document_id, cluster_label) DO UPDATE;
-- its DO UPDATE would clobber a confirmed row. persist_identities already excludes
-- confirmed clusters from its payload (read-then-write), but that is not atomic — a
-- confirmation landing between the read and the write would still be overwritten.
--
-- This trigger makes the protection a database invariant: a confirmed row may only
-- be updated by something that keeps it confirmed (a deliberate human re-confirmation
-- or subject fix). Any update that would move it off 'confirmed' is silently skipped
-- (BEFORE UPDATE returning NULL), so the auto pass's DO UPDATE becomes a no-op on
-- confirmed rows instead of raising and failing the whole persist batch. To
-- un-confirm, delete the row (not blocked here); the next auto pass re-proposes it.
--
-- SCOPE — what this does NOT guarantee: it does NOT make a confirmed row survive a
-- full re-transcribe. persist_speaker_layer clears all of a document's attribution
-- rows (speaker_identities included) before re-inserting, because re-diarization
-- renumbers the SPEAKER_NN cluster labels — a kept confirmed row would then point at a
-- different voice and mislabel it. That wholesale clear is a DELETE, intentionally not
-- guarded here. The invariant is precisely: "the automatic resolver's upsert cannot
-- downgrade a confirmed row within a persist," not "confirmed rows survive a
-- re-transcribe." A re-transcribe is a reset; the operator re-confirms against the new
-- diarization. persist_identities keeps its own read-then-write guard for the retract
-- (DELETE) path this trigger does not cover.
--
-- Additive + idempotent (CREATE OR REPLACE FUNCTION; DROP TRIGGER then CREATE).

CREATE OR REPLACE FUNCTION protect_confirmed_speaker_identity()
RETURNS TRIGGER AS $$
BEGIN
    -- Keep a confirmed identity intact unless the update itself keeps it confirmed.
    -- IS DISTINCT FROM so a malformed NULL update is treated as a downgrade too.
    IF OLD.confidence = 'confirmed' AND NEW.confidence IS DISTINCT FROM 'confirmed' THEN
        RETURN NULL;  -- skip this row's update; the confirmed row is preserved
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_protect_confirmed_speaker_identity ON speaker_identities;
CREATE TRIGGER trg_protect_confirmed_speaker_identity
    BEFORE UPDATE ON speaker_identities
    FOR EACH ROW
    EXECUTE FUNCTION protect_confirmed_speaker_identity();
