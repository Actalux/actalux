-- Migration 015: backfill entity_id on documents that ingested without one
--
-- migrate_012 added documents.entity_id and backfilled every then-existing row to
-- the Clayton entity, but the ingest pipeline did not set entity_id on new docs.
-- Any document ingested between 012 and the ingest fix therefore carried a NULL
-- entity_id and was invisible to the entity-scoped browse/search (which filter on
-- entity_id). This reassigns those orphans to the single live body.
--
-- Idempotent: only touches rows where entity_id IS NULL. Safe to re-run. Once the
-- ingest pipeline sets entity_id at insert time, this should find nothing to do.

UPDATE documents
SET entity_id = (
    SELECT e.id FROM entities e
    JOIN places p ON p.id = e.place_id
    WHERE p.state = 'mo' AND p.slug = 'clayton' AND e.body_slug = 'schools'
)
WHERE entity_id IS NULL;
