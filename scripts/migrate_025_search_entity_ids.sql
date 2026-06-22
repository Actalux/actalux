-- Migration 025: multi-entity scope filter for cross-body search
--
-- The place-level "Ask the archive" lets a reader scope to one body OR to ALL
-- bodies of a place (Clayton: Board of Education + City Council + Plan
-- Commission). "All" must mean all bodies OF THIS PLACE, not all entities
-- everywhere — otherwise a second municipality added later would leak into
-- Clayton's results. The single ``filter_entity_id`` can't express "this set of
-- bodies", so add ``filter_entity_ids INT[]``: when given, a row matches if its
-- entity is in the list. ``filter_entity_id`` is kept for the many single-body
-- callers; the two filters are independent (both NULL = no entity scope).
--
-- Adding a parameter changes the signature, so each function is dropped at its
-- exact current (migrate_024) signature and recreated (cf. migrate_013, to avoid
-- a dual overload / PGRST203). RETURNS TABLE is unchanged from migrate_024
-- (document_type included). Bodies otherwise identical. Idempotent.

DROP FUNCTION IF EXISTS semantic_search(VECTOR(384), FLOAT, INT, DATE, DATE, TEXT, INT);
DROP FUNCTION IF EXISTS keyword_search(TEXT, INT, DATE, DATE, TEXT, INT);

CREATE OR REPLACE FUNCTION semantic_search(
    query_embedding VECTOR(384),
    match_threshold FLOAT DEFAULT 0.35,
    match_count INT DEFAULT 50,
    filter_date_from DATE DEFAULT NULL,
    filter_date_to DATE DEFAULT NULL,
    filter_doc_type TEXT DEFAULT NULL,
    filter_entity_id INT DEFAULT NULL,
    filter_entity_ids INT[] DEFAULT NULL
)
RETURNS TABLE (
    chunk_id INT,
    document_id INT,
    content TEXT,
    section TEXT,
    speaker TEXT,
    document_type TEXT,
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
        d.document_type,
        1 - (c.embedding <=> query_embedding) AS similarity
    FROM chunks c
    JOIN documents d ON d.id = c.document_id
    WHERE c.embedding IS NOT NULL
      AND d.replaces_id IS NULL
      AND (filter_entity_id IS NULL OR d.entity_id = filter_entity_id)
      AND (filter_entity_ids IS NULL OR d.entity_id = ANY(filter_entity_ids))
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
    filter_entity_id INT DEFAULT NULL,
    filter_entity_ids INT[] DEFAULT NULL
)
RETURNS TABLE (
    chunk_id INT,
    document_id INT,
    content TEXT,
    section TEXT,
    speaker TEXT,
    document_type TEXT,
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
        d.document_type,
        ts_rank(to_tsvector('english', c.content), websearch_to_tsquery('english', search_query)) AS rank
    FROM chunks c
    JOIN documents d ON d.id = c.document_id
    WHERE to_tsvector('english', c.content) @@ websearch_to_tsquery('english', search_query)
      AND d.replaces_id IS NULL
      AND (filter_entity_id IS NULL OR d.entity_id = filter_entity_id)
      AND (filter_entity_ids IS NULL OR d.entity_id = ANY(filter_entity_ids))
      AND (filter_date_from IS NULL OR d.meeting_date >= filter_date_from)
      AND (filter_date_to IS NULL OR d.meeting_date <= filter_date_to)
      AND (filter_doc_type IS NULL OR d.document_type = filter_doc_type)
    ORDER BY rank DESC
    LIMIT match_count;
$$;
