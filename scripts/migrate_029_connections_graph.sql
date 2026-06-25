-- Migration 029: connections graph — subjects, memberships, edges, mentions.
-- Run in the Supabase SQL editor (or via apply_migrations.py).
--
-- Adds the citation-backed entity-link layer over the corpus
-- (docs/architecture/connections-graph.md §5/§6). It sits beside
-- documents/chunks/votes so there is one provenance chain. This migration is
-- *schema only* — the projector that populates subjects/edges from the curated
-- roster + votes is Phase 1 application code; the publishable-filtered
-- SECURITY INVOKER dossier/API views land with that code (their columns must
-- match the not-yet-written dossier query, and a later CREATE OR REPLACE VIEW
-- migration is cleaner than guessing the shape now — editing an applied
-- migration is checksum drift, a hard error).
--
-- (Doc §5 calls this file migrate_028_connections_graph; the durable per-vote
-- vote_ref it depends on shipped on its own as migration 028, so the graph
-- tables are 029. Same content, renumbered after the split.)
--
-- Two integrity models, both enforced here in the schema, not by convention:
--
--   Privacy (§6). Only public officials + organizations get standing dossiers;
--   private individuals stay in document text as published but get no aggregated
--   profile. Enforced two ways because a trigger guards writes and RLS guards
--   reads, and neither covers the other:
--     * a BEFORE INSERT/UPDATE trigger on subjects (the minting gate) — fires
--       even under the service key, which bypasses RLS, so the projection and
--       backfill cannot mint a publishable person without a roster membership;
--     * deny-by-default RLS — the public (anon) key reads subjects/edges/mentions
--       only for publishable subjects, so a future plain .table('subjects')
--       select cannot leak an unpublishable row (the trigger is powerless there,
--       the row legitimately exists).
--
--   Edge lifecycle (§2.5, §4.3). Projected edges (votes, auto-derived) are
--   REBUILT per document version by the projector (delete by document_id +
--   re-derive), so they never need to survive a re-version; vote targets are the
--   durable pair (vote_document_id, vote_ref) from migration 028, not the SERIAL
--   votes.id (extract_votes reassigns it). Only human-confirmed edges persist
--   across versions and re-resolve their citation via quote_hash (citation_state,
--   §4.4) — Phase 2 machinery, not exercised by Phase 1's projected vote edges.
--
-- Additive + idempotent throughout (CREATE TABLE/INDEX IF NOT EXISTS,
-- CREATE OR REPLACE FUNCTION, DROP-then-CREATE trigger/policy).

-- ---- Tables ----

-- A resolved entity the archive can profile: a person (official), an
-- organization, a matter (bill/resolution/project/parcel), or a place. slug is
-- the URL segment for the dossier; publishable is the privacy gate (default
-- false; the trigger below governs flipping a person to true). minting_basis
-- records why the subject exists, for audit.
CREATE TABLE IF NOT EXISTS subjects (
    id            SERIAL PRIMARY KEY,
    place_id      INT REFERENCES places(id),
    type          TEXT NOT NULL CHECK (type IN ('person', 'org', 'place', 'matter')),
    subject_role  TEXT CHECK (subject_role IN ('official', 'organization', 'matter', 'place')),
    canonical_name TEXT NOT NULL,
    slug          TEXT NOT NULL,
    metadata      JSONB NOT NULL DEFAULT '{}'::jsonb,
    publishable   BOOLEAN NOT NULL DEFAULT FALSE,   -- privacy gate (trigger below, §6)
    minting_basis TEXT CHECK (minting_basis IN ('roster', 'regex_number', 'reviewed', 'manual')),
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (place_id, type, slug)
);

-- The curated roster: ground truth for resolving a vote's name to a member.
-- The [start_date, end_date] window drives date-bounded resolution (a last-name
-- roll call on meeting-date D resolves to the member whose term covers D), which
-- also handles mid-term roster changes. end_date NULL = still seated.
CREATE TABLE IF NOT EXISTS memberships (
    id          SERIAL PRIMARY KEY,
    subject_id  INT NOT NULL REFERENCES subjects(id) ON DELETE CASCADE,
    entity_id   INT NOT NULL REFERENCES entities(id),
    role        TEXT,
    start_date  DATE,
    end_date    DATE,
    UNIQUE (subject_id, entity_id, start_date)
);

-- Known name variants of a subject (honorific/initial/spelling drift). Auto-
-- resolution uses normalized_alias ONLY when (place_id, type, normalized_alias)
-- maps to exactly one subject and (for members) a membership covers the vote's
-- meeting_date; everything else queues. Resolution internals, not a read surface.
CREATE TABLE IF NOT EXISTS subject_aliases (
    id               SERIAL PRIMARY KEY,
    subject_id       INT NOT NULL REFERENCES subjects(id) ON DELETE CASCADE,
    normalized_alias TEXT NOT NULL,
    raw_alias        TEXT,
    source           TEXT,
    UNIQUE (subject_id, normalized_alias)
);

-- Names that did not resolve unambiguously. Conservative resolution (§4 cardinal):
-- a name that matches zero, or more than one, roster member — or conflicts on a
-- strong signal — lands here for human review and is NEVER auto-minted as a
-- subject. raw_alias can be a private individual's name, so anon is denied this
-- table entirely (no RLS policy below).
CREATE TABLE IF NOT EXISTS subject_resolution_queue (
    id                  SERIAL PRIMARY KEY,
    raw_alias           TEXT NOT NULL,
    normalized_alias    TEXT,
    entity_id           INT REFERENCES entities(id),
    meeting_date        DATE,
    document_id         INT REFERENCES documents(id) ON DELETE CASCADE,
    vote_ref            TEXT,
    reason              TEXT,
    status              TEXT NOT NULL DEFAULT 'open'
                        CHECK (status IN ('open', 'resolved', 'rejected')),
    resolved_subject_id INT REFERENCES subjects(id) ON DELETE SET NULL,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- A cited occurrence of a subject in a document. Derived + replaced per
-- document_id by the projector. citation_id is the durable key (chunk_id nulls
-- on re-ingest, so it cannot be); it is NOT NULL because the uniqueness key
-- below would otherwise let duplicates through (Postgres treats multiple NULLs
-- as distinct, §4.2). quote_hash is the normalized source_quote for
-- re-resolution. projection_complete gates publication (§4.3): the projector
-- writes false (invisible), the atomic swap flips it true.
CREATE TABLE IF NOT EXISTS mentions (
    id            SERIAL PRIMARY KEY,
    subject_id    INT NOT NULL REFERENCES subjects(id) ON DELETE CASCADE,
    document_id   INT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    chunk_id      INT REFERENCES chunks(id) ON DELETE SET NULL,
    citation_id   TEXT NOT NULL,
    source_quote  TEXT,
    quote_hash    TEXT,
    projection_complete BOOLEAN NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (subject_id, document_id, citation_id)
);

-- A typed link from one subject to a target. Exactly one target kind per edge
-- (CHECK below): another subject, a body (entity), or a vote — the vote target
-- is the durable pair (vote_document_id, vote_ref) referencing votes(document_id,
-- vote_ref), NOT a SQL FK to votes: that pair is backed by a PARTIAL unique index
-- (migration 028, WHERE vote_ref IS NOT NULL) which Postgres cannot use as an FK
-- target, and projected vote edges are rebuilt per version anyway, so referential
-- integrity is the projector's contract (§4.3, §4.5 postconditions), not an FK.
--
-- source_document_id is PROVENANCE (where the citation came from), separate from
-- the vote target: a cited non-vote edge has source_document_id but no vote
-- target. status drives rendering (cited = fact; inferred = labeled "Actalux
-- linked these"; confirmed = a human-promoted inference). citation_state tracks
-- re-resolution of PERSISTED (confirmed) edges across versions (§4.4); projected
-- edges stay 'live' (they are rebuilt, never re-resolved).
CREATE TABLE IF NOT EXISTS edges (
    id              SERIAL PRIMARY KEY,
    from_subject    INT NOT NULL REFERENCES subjects(id) ON DELETE CASCADE,
    to_subject      INT REFERENCES subjects(id) ON DELETE CASCADE,   -- target: a subject
    to_entity_id    INT REFERENCES entities(id),                     -- target: a body
    vote_document_id INT REFERENCES documents(id) ON DELETE CASCADE, -- target: a vote (with vote_ref)
    vote_ref        TEXT,
    source_document_id INT REFERENCES documents(id) ON DELETE CASCADE, -- provenance (≠ target)
    type            TEXT NOT NULL,   -- taxonomy v1: voted_aye_on|voted_no_on|voted_abstain_on|
                                     -- moved|seconded|heard_by|applied_for|represents|owns|
                                     -- located_at|same_matter_as|part_of (see doc §5)
    status          TEXT NOT NULL DEFAULT 'cited'
                    CHECK (status IN ('cited', 'inferred', 'confirmed')),
    inference_basis TEXT,
    chunk_id        INT REFERENCES chunks(id) ON DELETE SET NULL,
    citation_id     TEXT,
    source_quote    TEXT,
    quote_hash      TEXT,            -- normalized source_quote, for re-resolution (§4.4)
    citation_state  TEXT NOT NULL DEFAULT 'live'
                    CHECK (citation_state IN ('live', 're_resolved', 'stale', 'ambiguous')),
    resolved_chunk_id INT REFERENCES chunks(id) ON DELETE SET NULL,
    resolved_at     TIMESTAMPTZ,
    as_of_date      DATE,            -- derived from the vote/document
    as_of_date_source TEXT,
    projection_complete BOOLEAN NOT NULL DEFAULT FALSE,   -- §4.3 publish gate; reads filter = true
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    -- exactly one target kind
    CONSTRAINT edges_one_target CHECK (num_nonnulls(vote_ref, to_subject, to_entity_id) = 1),
    -- a vote target is always the durable pair, never a bare ref
    CONSTRAINT edges_vote_pair CHECK ((vote_ref IS NULL) = (vote_document_id IS NULL)),
    -- every cited edge keeps document-level provenance even after chunk_id nulls
    CONSTRAINT edges_cited_has_provenance
        CHECK (status <> 'cited' OR source_document_id IS NOT NULL),
    -- every inferred edge says why it was inferred
    CONSTRAINT edges_inferred_has_basis
        CHECK (status <> 'inferred' OR inference_basis IS NOT NULL)
);

-- ---- Indexes ----

-- Resolution + read joins (dossier groups by subject; RLS policies probe
-- from_subject/to_subject/subject_id).
CREATE INDEX IF NOT EXISTS idx_subjects_place_type ON subjects (place_id, type);
CREATE INDEX IF NOT EXISTS idx_memberships_subject ON memberships (subject_id);
CREATE INDEX IF NOT EXISTS idx_memberships_entity ON memberships (entity_id);
CREATE INDEX IF NOT EXISTS idx_aliases_normalized ON subject_aliases (normalized_alias);
CREATE INDEX IF NOT EXISTS idx_queue_status ON subject_resolution_queue (status);
CREATE INDEX IF NOT EXISTS idx_mentions_subject ON mentions (subject_id);
CREATE INDEX IF NOT EXISTS idx_mentions_document ON mentions (document_id);
CREATE INDEX IF NOT EXISTS idx_edges_from_subject ON edges (from_subject);
CREATE INDEX IF NOT EXISTS idx_edges_to_subject ON edges (to_subject);
CREATE INDEX IF NOT EXISTS idx_edges_vote ON edges (vote_document_id, vote_ref);

-- Partial unique indexes enforce "one edge per relationship" per target kind.
-- Keyed on columns that SURVIVE re-ingest and are non-NULL — never chunk_id
-- (ON DELETE SET NULL, migration 020). Postgres lets multiple NULLs through a
-- plain unique key, so each is scoped (WHERE) to its target kind (§5).
-- A member cannot be both aye and no on the same vote:
CREATE UNIQUE INDEX IF NOT EXISTS idx_edges_vote_outcome
    ON edges (vote_document_id, from_subject, vote_ref)
    WHERE type IN ('voted_aye_on', 'voted_no_on', 'voted_abstain_on');
-- type in the key so a member can move AND vote on the same motion:
CREATE UNIQUE INDEX IF NOT EXISTS idx_edges_vote_role
    ON edges (vote_document_id, from_subject, type, vote_ref)
    WHERE type IN ('moved', 'seconded');
-- subject- and entity-target edges dedup on the durable quote_hash:
CREATE UNIQUE INDEX IF NOT EXISTS idx_edges_subject_target
    ON edges (from_subject, to_subject, type, quote_hash)
    WHERE to_subject IS NOT NULL AND quote_hash IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_edges_entity_target
    ON edges (from_subject, to_entity_id, type, quote_hash)
    WHERE to_entity_id IS NOT NULL AND quote_hash IS NOT NULL;

-- ---- Minting gate: publishable persons need a roster membership (§6) ----
-- A CHECK can't cross tables, so this is a trigger. It fires on every write path
-- including the service key (which bypasses RLS but NOT triggers), so the
-- projection/backfill cannot mint a publishable person off the street. A person
-- becomes publishable only via a membership (an official) or an explicit
-- minting_basis='reviewed' (a human approved it). Non-person subjects
-- (org/matter/place) are unrestricted here — their publishability is governed by
-- review, not by board membership. Roster seeding inserts the person
-- publishable=false, adds the membership, then flips publishable=true (the
-- membership now exists → the gate passes).
CREATE OR REPLACE FUNCTION subjects_minting_gate()
RETURNS TRIGGER
LANGUAGE plpgsql
-- Pin search_path so the function resolves memberships in public regardless of
-- the caller's search_path (defense-in-depth, mirroring migrate_026).
SET search_path = public, pg_temp
AS $$
BEGIN
    IF NEW.publishable AND NEW.type = 'person' THEN
        IF NEW.minting_basis IS DISTINCT FROM 'reviewed'
           AND NOT EXISTS (SELECT 1 FROM memberships m WHERE m.subject_id = NEW.id) THEN
            RAISE EXCEPTION
                'subject % (person) cannot be publishable without a roster membership '
                'or minting_basis=reviewed (connections-graph §6)', NEW.id;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_subjects_minting_gate ON subjects;
CREATE TRIGGER trg_subjects_minting_gate
    BEFORE INSERT OR UPDATE ON subjects
    FOR EACH ROW EXECUTE FUNCTION subjects_minting_gate();

-- ---- RLS: deny-by-default; anon reads only publishable subjects (§6) ----
-- The public read path uses the anon (publishable) key, so RLS is the real read
-- gate. Enable on every graph table; add anon SELECT policies only where a read
-- is publishable-safe. The service key bypasses RLS, so the projector keeps full
-- access. Dossier/API reads (Phase 1) go through SECURITY INVOKER views, which
-- run under these same anon policies (a SECURITY DEFINER object would bypass them
-- and re-open the hole — see migrate_007 / migrate_026).
ALTER TABLE subjects                ENABLE ROW LEVEL SECURITY;
ALTER TABLE memberships             ENABLE ROW LEVEL SECURITY;
ALTER TABLE subject_aliases         ENABLE ROW LEVEL SECURITY;
ALTER TABLE subject_resolution_queue ENABLE ROW LEVEL SECURITY;
ALTER TABLE mentions                ENABLE ROW LEVEL SECURITY;
ALTER TABLE edges                   ENABLE ROW LEVEL SECURITY;

-- subjects: only publishable rows are anon-visible.
DROP POLICY IF EXISTS anon_read_publishable_subjects ON subjects;
CREATE POLICY anon_read_publishable_subjects ON subjects
    FOR SELECT TO anon, authenticated USING (publishable = TRUE);

-- memberships: roster term windows are public record, but only for a publishable
-- subject (a membership of an unpublishable person stays hidden). The dossier
-- needs this through its SECURITY INVOKER view, so anon must have the policy.
DROP POLICY IF EXISTS anon_read_publishable_memberships ON memberships;
CREATE POLICY anon_read_publishable_memberships ON memberships
    FOR SELECT TO anon, authenticated USING (
        EXISTS (SELECT 1 FROM subjects s WHERE s.id = memberships.subject_id AND s.publishable)
    );

-- mentions: a cited occurrence is anon-visible only for a publishable subject.
DROP POLICY IF EXISTS anon_read_publishable_mentions ON mentions;
CREATE POLICY anon_read_publishable_mentions ON mentions
    FOR SELECT TO anon, authenticated USING (
        EXISTS (SELECT 1 FROM subjects s WHERE s.id = mentions.subject_id AND s.publishable)
    );

-- edges: anon-visible only when BOTH endpoints are publishable (from_subject
-- always present; to_subject may be NULL for vote/entity targets, which are then
-- gated by from_subject alone).
DROP POLICY IF EXISTS anon_read_publishable_edges ON edges;
CREATE POLICY anon_read_publishable_edges ON edges
    FOR SELECT TO anon, authenticated USING (
        EXISTS (SELECT 1 FROM subjects s WHERE s.id = edges.from_subject AND s.publishable)
        AND (
            edges.to_subject IS NULL
            OR EXISTS (SELECT 1 FROM subjects s2 WHERE s2.id = edges.to_subject AND s2.publishable)
        )
    );

-- subject_aliases + subject_resolution_queue: NO anon policy (resolution
-- internals; the queue can hold private individuals' names). RLS on with no
-- matching policy = anon denied; the service key still reaches them.
