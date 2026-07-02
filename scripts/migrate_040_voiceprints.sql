-- Migration 040: cross-meeting voiceprints for speaker identification (Phase 2).
-- Run in the Supabase SQL editor (or via apply_migrations.py).
--
-- Design: docs/architecture/voiceprint-speaker-id-plan.md (codex-reviewed v2).
-- Extends the shipped speaker-attribution layer (migrate_034/035).
--
-- WHY: the name-anchored resolver (identity/resolve.py) can only name a speaker
-- whose name is spoken (roll call, vote, self-intro). Appointed officials (City
-- Manager, CFO, directors, counsel) speak often but are never in a roll call, so
-- they stay anonymous forever. A voiceprint is the compounding cross-meeting
-- signal: once a cluster is human-confirmed to an official, store its voice
-- embedding; later meetings match anonymous clusters against the gallery of known
-- official voiceprints -> a *candidate* identity before any name is spoken.
--
-- CARDINAL GUARDRAIL (enforced across the stack, mirrored here): a voiceprint-only
-- match NEVER auto-publishes a name. Matching writes at most confidence
-- 'inferred_medium' (BELOW the 'inferred_high'/'confirmed' public-display RLS gate
-- in migrate_034), routed to human review. Publication requires a human
-- confirmation or an independent same-meeting name anchor. inferred_high is
-- anon-readable, so a thresholded biometric match must never reach it in v1.
--
-- Embedding parameters FROZEN by the Phase 0 spike (2026-07-01, see the plan §4):
--   model = pyannote/wespeaker-voxceleb-resnet34-LM  (the embedding half of the
--           speaker-diarization-3.1 pipeline; pyannote.audio 4.0.5)
--   dim   = 256, cosine metric, NOT L2-normalized by the model (we normalize
--           before storing so cosine == dot product), fully repeatable (cos 1.0).
--
-- Keyed on person_id (Model B: global persons + per-body subjects via
-- subjects.person_id, migrate_036). One human has one voice across bodies.
--
-- Additive + idempotent (CREATE ... IF NOT EXISTS; DROP POLICY/TRIGGER then CREATE;
-- CREATE OR REPLACE FUNCTION).

-- 1. Gallery: per-sample voice embeddings for HUMAN-CONFIRMED official speakers.
--    Derived + rebuildable: never the record of who a speaker is (that stays
--    speaker_identities); it can be dropped and re-enrolled.
CREATE TABLE IF NOT EXISTS subject_voiceprints (
    id                 SERIAL PRIMARY KEY,
    person_id          INT NOT NULL REFERENCES persons(id) ON DELETE CASCADE,
    source_subject_id  INT REFERENCES subjects(id) ON DELETE SET NULL,    -- provenance
    source_document_id INT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    -- the confirmed speaker_identities row this sample came from; a wholesale
    -- re-transcribe clear (persist_speaker_layer) cascades this sample away.
    source_identity_id INT REFERENCES speaker_identities(id) ON DELETE CASCADE,
    cluster_label      TEXT NOT NULL,
    embedding          VECTOR(256) NOT NULL,   -- frozen by the Phase 0 spike
    source_basis       TEXT NOT NULL CHECK (source_basis IN
                          ('rollcall', 'vote_anchor', 'self_intro', 'manual')),
    model              TEXT NOT NULL,          -- embedding model id + version
    seconds            REAL CHECK (seconds IS NULL OR seconds >= 0),  -- speech behind it
    created_at         TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (person_id, source_document_id, cluster_label)
);
CREATE INDEX IF NOT EXISTS subject_voiceprints_embedding_hnsw
    ON subject_voiceprints USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS idx_subject_voiceprints_person
    ON subject_voiceprints (person_id);
CREATE INDEX IF NOT EXISTS idx_subject_voiceprints_source_doc
    ON subject_voiceprints (source_document_id);

-- 2. Officials-only enrollment, ENFORCED in the DB (not just app code). A row is
--    rejected unless its person has at least one PUBLISHABLE subject that also
--    holds a membership (an attested official with a seat). A recurring public
--    commenter must never be fingerprinted (no surveillance-shaped dossier).
CREATE OR REPLACE FUNCTION enforce_voiceprint_official()
RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM subjects s
        JOIN memberships m ON m.subject_id = s.id
        WHERE s.person_id = NEW.person_id
          AND s.publishable = TRUE
    ) THEN
        RAISE EXCEPTION
            'voiceprint enrollment rejected: person % has no publishable official subject',
            NEW.person_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_voiceprint_officials_only ON subject_voiceprints;
CREATE TRIGGER trg_voiceprint_officials_only
    BEFORE INSERT OR UPDATE ON subject_voiceprints
    FOR EACH ROW
    EXECUTE FUNCTION enforce_voiceprint_official();

-- 3. Audit every voiceprint DECISION. speaker_identities stores only
--    subject/confidence/basis (migrate_034) and cannot record the score, margin,
--    or alternatives behind an automatic proposal. Internal, service-only.
CREATE TABLE IF NOT EXISTS voiceprint_match_evidence (
    id                 SERIAL PRIMARY KEY,
    document_id        INT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    cluster_label      TEXT NOT NULL,
    proposed_person_id INT REFERENCES persons(id) ON DELETE SET NULL,
    score              REAL NOT NULL,   -- aggregated cosine to the winner
    margin             REAL NOT NULL,   -- winner - runner-up
    model              TEXT NOT NULL,
    threshold_version  TEXT NOT NULL,   -- which calibrated operating point produced this
    aggregation        TEXT NOT NULL,   -- mean|best-k|...
    target_seconds     REAL,
    alternatives       JSONB,           -- top-N [{person_id, score}]
    created_at         TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_vp_evidence_doc
    ON voiceprint_match_evidence (document_id, cluster_label);

-- 4. Extend the speaker_identities basis CHECK to admit the new signal. The
--    original constraint (migrate_034) is inline + unnamed, so Postgres named it
--    speaker_identities_basis_check.
ALTER TABLE speaker_identities DROP CONSTRAINT IF EXISTS speaker_identities_basis_check;
ALTER TABLE speaker_identities ADD CONSTRAINT speaker_identities_basis_check
    CHECK (basis IN ('rollcall', 'vote_anchor', 'self_intro', 'manual', 'voiceprint'));

-- 5. RLS: both tables are internal (gallery embeddings + decision audit). Service
--    key only; no anon/authenticated read. Defense in depth: RLS on + REVOKE.
ALTER TABLE subject_voiceprints ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON subject_voiceprints FROM anon, authenticated;
REVOKE ALL ON SEQUENCE subject_voiceprints_id_seq FROM anon, authenticated;

ALTER TABLE voiceprint_match_evidence ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON voiceprint_match_evidence FROM anon, authenticated;
REVOKE ALL ON SEQUENCE voiceprint_match_evidence_id_seq FROM anon, authenticated;
