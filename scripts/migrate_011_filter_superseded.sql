-- Migration 011: exclude superseded document versions from search
--
-- When a document's content changes, ingest creates a new version and sets
-- replaces_id on the old row (see _ingest_with_dedup), but it does not delete
-- the old chunks. semantic_search and keyword_search joined documents without
-- filtering replaces_id, so superseded versions stayed searchable -- a citation
-- archive must never surface a stale version with outdated figures. The latent
-- bug became material when the curriculum-map re-ingest created 43 new versions
-- (44 superseded docs / 59 stale chunks searchable). Both RPCs now require
-- d.replaces_id IS NULL so only the current version of each document is returned.
--
-- Idempotent: CREATE OR REPLACE of both functions with their existing bodies
-- plus the replaces_id filter (semantic_search keeps the migrate_010 ef_search
-- set_config).

CREATE OR REPLACE FUNCTION semantic_search(
    query_embedding VECTOR(384),
    match_threshold FLOAT DEFAULT 0.35,
    match_count INT DEFAULT 50,
    filter_date_from DATE DEFAULT NULL,
    filter_date_to DATE DEFAULT NULL,
    filter_doc_type TEXT DEFAULT NULL
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
    filter_doc_type TEXT DEFAULT NULL
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
      AND (filter_date_from IS NULL OR d.meeting_date >= filter_date_from)
      AND (filter_date_to IS NULL OR d.meeting_date <= filter_date_to)
      AND (filter_doc_type IS NULL OR d.document_type = filter_doc_type)
    ORDER BY rank DESC
    LIMIT match_count;
$$;
