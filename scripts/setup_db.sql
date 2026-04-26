-- Actalux database schema for Supabase
-- Run this in the Supabase SQL editor to set up the database.

-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Official documents (agendas, minutes, packets, resolutions)
CREATE TABLE IF NOT EXISTS documents (
    id SERIAL PRIMARY KEY,
    meeting_date DATE,
    meeting_title TEXT,
    document_type TEXT,
    source_url TEXT DEFAULT '',
    source_file TEXT DEFAULT '',
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Searchable chunks (verbatim text from documents)
CREATE TABLE IF NOT EXISTS chunks (
    id SERIAL PRIMARY KEY,
    document_id INT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    section TEXT DEFAULT '',
    speaker TEXT DEFAULT '',
    chunk_index INT NOT NULL DEFAULT 0,
    embedding VECTOR(384)
);

-- Full-text search index
CREATE INDEX IF NOT EXISTS chunks_fts
    ON chunks USING gin(to_tsvector('english', content));

-- HNSW vector index (can be created on empty table, unlike IVFFlat)
CREATE INDEX IF NOT EXISTS chunks_embedding_hnsw
    ON chunks USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- Vote records (structured, from official minutes)
CREATE TABLE IF NOT EXISTS votes (
    id SERIAL PRIMARY KEY,
    document_id INT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    meeting_date DATE,
    motion TEXT NOT NULL,
    result TEXT NOT NULL,
    vote_count_yes INT DEFAULT 0,
    vote_count_no INT DEFAULT 0,
    vote_count_abstain INT DEFAULT 0,
    details JSONB
);

-- Speaker profiles
CREATE TABLE IF NOT EXISTS speakers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    role TEXT DEFAULT '',
    active BOOLEAN DEFAULT TRUE
);

-- Email subscriptions
CREATE TABLE IF NOT EXISTS topic_alerts (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    topic TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(email, topic)
);

-- Error reports from users
CREATE TABLE IF NOT EXISTS corrections (
    id SERIAL PRIMARY KEY,
    chunk_id INT NOT NULL REFERENCES chunks(id) ON DELETE CASCADE,
    reporter_email TEXT DEFAULT '',
    description TEXT NOT NULL,
    status TEXT DEFAULT 'open',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Ingestion run tracking (for operator visibility)
CREATE TABLE IF NOT EXISTS ingest_runs (
    id SERIAL PRIMARY KEY,
    meeting_date DATE,
    meeting_title TEXT,
    docs_found INT DEFAULT 0,
    docs_ingested INT DEFAULT 0,
    docs_failed INT DEFAULT 0,
    errors JSONB DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Phase 2: Video transcripts (future)
CREATE TABLE IF NOT EXISTS transcripts (
    id SERIAL PRIMARY KEY,
    meeting_date DATE,
    meeting_title TEXT,
    source_url TEXT DEFAULT '',
    source_file TEXT DEFAULT '',
    content TEXT NOT NULL,
    reviewed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Migration: Document provenance tracking
-- ============================================================

-- Content hash for change detection
ALTER TABLE documents ADD COLUMN IF NOT EXISTS content_hash TEXT DEFAULT '';

-- Where this document was acquired from
ALTER TABLE documents ADD COLUMN IF NOT EXISTS source_portal TEXT DEFAULT '';

-- Version tracking
ALTER TABLE documents ADD COLUMN IF NOT EXISTS version INT DEFAULT 1;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS replaces_id INT REFERENCES documents(id);
ALTER TABLE documents ADD COLUMN IF NOT EXISTS last_checked_at TIMESTAMPTZ;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

-- Document-local chunk ordering for reliable context lookup
ALTER TABLE chunks ADD COLUMN IF NOT EXISTS chunk_index INT NOT NULL DEFAULT 0;

WITH ranked AS (
    SELECT
        id,
        ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY id) - 1 AS new_index
    FROM chunks
)
UPDATE chunks
SET chunk_index = ranked.new_index
FROM ranked
WHERE chunks.id = ranked.id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_chunks_document_index
    ON chunks (document_id, chunk_index);

-- Fast lookup for latest version of a document
CREATE INDEX IF NOT EXISTS idx_documents_latest
    ON documents (source_file, source_portal)
    WHERE replaces_id IS NULL;

CREATE INDEX IF NOT EXISTS idx_documents_portal
    ON documents (source_portal);

-- Source registry: tracks known crawl targets
CREATE TABLE IF NOT EXISTS sources (
    id SERIAL PRIMARY KEY,
    portal TEXT NOT NULL,
    name TEXT NOT NULL,
    url TEXT NOT NULL DEFAULT '',
    crawl_config JSONB DEFAULT '{}'::jsonb,
    last_crawled_at TIMESTAMPTZ,
    doc_count INT DEFAULT 0,
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(portal, url)
);

-- Seed with existing Diligent folders
INSERT INTO sources (portal, name, url, crawl_config) VALUES
    ('diligent', '2024-2025 Minutes', '8826a219-6b40-47bb-8b39-d0006eb6bf46',
     '{"folder_id": "8826a219-6b40-47bb-8b39-d0006eb6bf46"}'::jsonb),
    ('diligent', '2023-2024 Minutes', '5091d99e-9702-4d55-b2a5-9d8e809fa2f5',
     '{"folder_id": "5091d99e-9702-4d55-b2a5-9d8e809fa2f5"}'::jsonb),
    ('diligent', 'District Finance', '47cca4ec-dce9-46f1-8956-26e586b09283',
     '{"folder_id": "47cca4ec-dce9-46f1-8956-26e586b09283"}'::jsonb)
ON CONFLICT (portal, url) DO NOTHING;

-- ============================================================
-- RPC functions for hybrid search
-- ============================================================

-- Semantic search: returns chunks ranked by cosine similarity
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
LANGUAGE sql STABLE
AS $$
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
$$;

-- Keyword search: returns chunks ranked by full-text relevance
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
      AND (filter_date_from IS NULL OR d.meeting_date >= filter_date_from)
      AND (filter_date_to IS NULL OR d.meeting_date <= filter_date_to)
      AND (filter_doc_type IS NULL OR d.document_type = filter_doc_type)
    ORDER BY rank DESC
    LIMIT match_count;
$$;
