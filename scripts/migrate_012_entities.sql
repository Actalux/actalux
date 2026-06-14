-- Migration 012: multi-jurisdiction entity model (Phase A)
--
-- Introduces the place/entity model from docs/architecture/multi-tenancy.md so
-- the corpus can hold many public bodies. A "place" is a discovery grouping
-- (mo/clayton), NOT a legal boundary; an "entity" is one public body we archive
-- (a school district, a city council). Documents belong to one entity.
--
-- Phase A is additive and behaviour-preserving: it seeds the single Clayton
-- entity, backfills every existing document to it, and gives the search RPCs an
-- optional entity filter (NULL = no filter, so existing callers are unaffected).
-- No URL change (that is Phase B). Idempotent throughout.

CREATE TABLE IF NOT EXISTS places (
    id           SERIAL PRIMARY KEY,
    state        TEXT NOT NULL,            -- 2-char, e.g. 'mo'
    slug         TEXT NOT NULL,            -- 'clayton'
    display_name TEXT NOT NULL,            -- 'Clayton'
    county       TEXT,                     -- nullable; disambiguation only
    UNIQUE (state, slug)
);

CREATE TABLE IF NOT EXISTS entities (
    id           SERIAL PRIMARY KEY,
    place_id     INT REFERENCES places(id),   -- nullable: a body need not map to a place
    body_slug    TEXT NOT NULL,               -- URL segment under the place, e.g. 'schools'
    type         TEXT NOT NULL,               -- 'school_district' | 'city_council' | ...
    display_name TEXT NOT NULL,               -- 'Clayton School District'
    external_ids JSONB NOT NULL DEFAULT '{}'::jsonb,  -- {"nces": "...", "dese": "..."}
    UNIQUE (place_id, body_slug)
);

ALTER TABLE documents ADD COLUMN IF NOT EXISTS entity_id INT REFERENCES entities(id);
CREATE INDEX IF NOT EXISTS idx_documents_entity ON documents (entity_id);

-- Seed the one place + one body live today. external_ids carries the DESE
-- county-district code (096102, sourced); the NCES district ID is intentionally
-- omitted until verified against nces.ed.gov rather than guessed.
INSERT INTO places (state, slug, display_name)
VALUES ('mo', 'clayton', 'Clayton')
ON CONFLICT (state, slug) DO NOTHING;

INSERT INTO entities (place_id, body_slug, type, display_name, external_ids)
SELECT p.id, 'schools', 'school_district', 'Clayton School District',
       '{"dese": "096102"}'::jsonb
FROM places p
WHERE p.state = 'mo' AND p.slug = 'clayton'
ON CONFLICT (place_id, body_slug) DO NOTHING;

-- Backfill every existing document to the Clayton entity.
UPDATE documents
SET entity_id = (
    SELECT e.id FROM entities e
    JOIN places p ON p.id = e.place_id
    WHERE p.state = 'mo' AND p.slug = 'clayton' AND e.body_slug = 'schools'
)
WHERE entity_id IS NULL;

-- Re-create the search RPCs with an optional entity filter (NULL = no filter).
-- Bodies are otherwise identical to migrate_010/011 (ef_search set_config,
-- replaces_id IS NULL).
CREATE OR REPLACE FUNCTION semantic_search(
    query_embedding VECTOR(384),
    match_threshold FLOAT DEFAULT 0.35,
    match_count INT DEFAULT 50,
    filter_date_from DATE DEFAULT NULL,
    filter_date_to DATE DEFAULT NULL,
    filter_doc_type TEXT DEFAULT NULL,
    filter_entity_id INT DEFAULT NULL
)
RETURNS TABLE (
    chunk_id INT,
    document_id INT,
    content TEXT,
    section TEXT,
    speaker TEXT,
    similarity FLOAT
)
LANGUAGE plpgsql STABLE
AS $$
BEGIN
    PERFORM set_config('hnsw.ef_search', '100', true);
    RETURN QUERY
    SELECT
        c.id AS chunk_id,
        c.document_id,
        c.content,
        c.section,
        c.speaker,
        1 - (c.embedding <=> query_embedding) AS similarity
    FROM chunks c
    JOIN documents d ON d.id = c.document_id
    WHERE c.embedding IS NOT NULL
      AND d.replaces_id IS NULL
      AND (filter_entity_id IS NULL OR d.entity_id = filter_entity_id)
      AND 1 - (c.embedding <=> query_embedding) >= match_threshold
      AND (filter_date_from IS NULL OR d.meeting_date >= filter_date_from)
      AND (filter_date_to IS NULL OR d.meeting_date <= filter_date_to)
      AND (filter_doc_type IS NULL OR d.document_type = filter_doc_type)
    ORDER BY c.embedding <=> query_embedding
    LIMIT match_count;
END;
$$;

CREATE OR REPLACE FUNCTION keyword_search(
    search_query TEXT,
    match_count INT DEFAULT 50,
    filter_date_from DATE DEFAULT NULL,
    filter_date_to DATE DEFAULT NULL,
    filter_doc_type TEXT DEFAULT NULL,
    filter_entity_id INT DEFAULT NULL
)
RETURNS TABLE (
    chunk_id INT,
    document_id INT,
    content TEXT,
    section TEXT,
    speaker TEXT,
    rank FLOAT
)
LANGUAGE sql STABLE
AS $$
    SELECT
        c.id AS chunk_id,
        c.document_id,
        c.content,
        c.section,
        c.speaker,
        ts_rank(to_tsvector('english', c.content), websearch_to_tsquery('english', search_query)) AS rank
    FROM chunks c
    JOIN documents d ON d.id = c.document_id
    WHERE to_tsvector('english', c.content) @@ websearch_to_tsquery('english', search_query)
      AND d.replaces_id IS NULL
      AND (filter_entity_id IS NULL OR d.entity_id = filter_entity_id)
      AND (filter_date_from IS NULL OR d.meeting_date >= filter_date_from)
      AND (filter_date_to IS NULL OR d.meeting_date <= filter_date_to)
      AND (filter_doc_type IS NULL OR d.document_type = filter_doc_type)
    ORDER BY rank DESC
    LIMIT match_count;
$$;
