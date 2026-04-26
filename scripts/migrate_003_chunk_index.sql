-- Migration 003: document-local chunk ordering
-- Run in Supabase SQL editor.
--
-- chunk_index is the stable order of a chunk within its source document.
-- Context lookup should use this instead of assuming global chunk IDs are
-- contiguous per document.

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
