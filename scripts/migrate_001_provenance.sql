-- Migration 001: Add document provenance tracking
-- Run in Supabase SQL editor

-- Content hash for change detection
ALTER TABLE documents ADD COLUMN IF NOT EXISTS content_hash TEXT DEFAULT '';

-- Where this document was acquired from
ALTER TABLE documents ADD COLUMN IF NOT EXISTS source_portal TEXT DEFAULT '';

-- Version tracking
ALTER TABLE documents ADD COLUMN IF NOT EXISTS version INT DEFAULT 1;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS replaces_id INT REFERENCES documents(id);
ALTER TABLE documents ADD COLUMN IF NOT EXISTS last_checked_at TIMESTAMPTZ;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

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
