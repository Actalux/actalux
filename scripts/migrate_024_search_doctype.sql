-- Migration 024: surface document_type from the search RPCs
--
-- Hybrid ranking wants to down-weight agendas (forward-looking item lists)
-- relative to minutes/transcripts (the actual record). To do that in the fusion
-- layer, each candidate row must carry its document_type — which the RPCs did not
-- return. Add a document_type column to the RETURNS TABLE of both search RPCs.
--
-- A RETURNS TABLE change is a return-type change, which CREATE OR REPLACE cannot
-- do, so each function is dropped (exact current signature) and recreated. The
-- argument lists are UNCHANGED, so no new overload is created (cf. migrate_013).
-- Bodies are otherwise identical to migrate_012 (ef_search set_config,
-- replaces_id IS NULL, entity/date/doc_type filters). Idempotent.

DROP FUNCTION IF EXISTS semantic_search(VECTOR(384), FLOAT, INT, DATE, DATE, TEXT, INT);
DROP FUNCTION IF EXISTS keyword_search(TEXT, INT, DATE, DATE, TEXT, INT);

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
      AND (filter_date_from IS NULL OR d.meeting_date >= filter_date_from)
      AND (filter_date_to IS NULL OR d.meeting_date <= filter_date_to)
      AND (filter_doc_type IS NULL OR d.document_type = filter_doc_type)
    ORDER BY rank DESC
    LIMIT match_count;
$$;
