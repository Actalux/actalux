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
