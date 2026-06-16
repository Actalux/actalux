-- Migration 016: stable external identity for documents (source_ref).
--
-- Dedup keyed on source_file (filename only) conflated distinct documents and
-- let PDF/HTML twins of the same record co-exist as two current rows. The
-- crawler manifests already carry a stable per-document origin id -- the
-- canonical origin URL (Diligent .../document/{guid}, claytonschools
-- .../resource-manager/view/{guid}, Google Docs .../d/{id}, Canva
-- .../design/{designId}/...). source_ref stores the normalized form of that
-- origin so ingest can dedup on a stable identity rather than the filename.
--
-- Additive and idempotent: the column defaults to '' so existing rows and any
-- writer that does not set it are unaffected. The partial index covers only the
-- current version of documents that actually have a source_ref, which keeps it
-- small and makes (source_portal, source_ref) lookups in
-- find_document_by_source_ref index-served.
--
-- RLS: source_ref is a plain column on documents; it inherits the table's
-- existing policies from migrate_007 (anon SELECT, service-key full access). No
-- new grant or policy is required.

ALTER TABLE documents ADD COLUMN IF NOT EXISTS source_ref TEXT DEFAULT '';

CREATE INDEX IF NOT EXISTS documents_source_ref
    ON documents (source_portal, source_ref)
    WHERE source_ref <> '' AND replaces_id IS NULL;
