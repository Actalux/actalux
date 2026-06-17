-- Migration 019: durable citation reference for budget figures.
--
-- budget_line_items.chunk_id is a FK to chunks(id) ON DELETE SET NULL, so
-- re-ingesting a cited budget/audit document deletes its chunks and nulls the
-- figure's citation -- the figure loses its source link. citation_id stores the
-- chunk's stable, content-addressed id (migration 018) instead, so the figure
-- keeps citing the same passage across re-ingest; routing resolves it back to
-- whatever chunk currently carries that citation_id.
--
-- Additive and idempotent: the column is nullable and backfilled from each row's
-- current chunk. chunk_id stays as the (best-effort) numeric reference; rendering
-- prefers citation_id.
--
-- RLS: inherits budget_line_items' policies from migrate_007.

ALTER TABLE budget_line_items ADD COLUMN IF NOT EXISTS citation_id TEXT;
