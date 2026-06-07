-- Migration 010: raise hnsw.ef_search for semantic_search
--
-- The chunks_embedding_hnsw index is an HNSW (approximate) index. pgvector's
-- default hnsw.ef_search = 40 caps the graph traversal at ~40 candidates, which
-- silently drops true nearest neighbours when a tightly-clustered, recently
-- inserted document group sits in a graph region the bounded search never
-- reaches. Observed concretely: for "per-pupil expenditure by building" the six
-- DESE per-pupil documents (chunks 7770-7779) are the corpus's top neighbours by
-- exact cosine (sim 0.747) yet semantic_search returned 0 of them -- it returned
-- 40 rows topping out at 0.743 and reaching as deep as exact-rank 54.
--
-- Raising ef_search to 100 recovers all of them (validated: ef_search 40 -> 5/10
-- of the exact top-10, 0 per-pupil; ef_search >= 80 -> 10/10, all per-pupil;
-- saturates at 80). 100 leaves margin above saturation at negligible latency.
--
-- ef_search is a USERSET parameter, so a runtime set_config() is permitted for
-- the PostgREST roles. We use a transaction-local set_config (is_local => true)
-- inside the function rather than a function-level SET clause: managed Postgres
-- denies attaching a SET clause for a custom-class GUC at function-definition
-- time ("permission denied to set parameter"), but the runtime call is allowed.
-- PostgREST runs each RPC in its own transaction, so the local setting applies
-- to this query and is discarded afterwards -- no effect on other queries, and
-- it survives connection pooling. No re-embedding or schema change.
--
-- Idempotent: CREATE OR REPLACE FUNCTION with the identical signature.

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
      AND 1 - (c.embedding <=> query_embedding) >= match_threshold
      AND (filter_date_from IS NULL OR d.meeting_date >= filter_date_from)
      AND (filter_date_to IS NULL OR d.meeting_date <= filter_date_to)
      AND (filter_doc_type IS NULL OR d.document_type = filter_doc_type)
    ORDER BY c.embedding <=> query_embedding
    LIMIT match_count;
END;
$$;
