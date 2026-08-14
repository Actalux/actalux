-- Link-only document stubs (issue #1): a document row that points at an official
-- record we could not parse into searchable text — an agenda packet whose docket
-- extraction failed. It appears in meeting bundles and browse, labeled, but has
-- no chunks and therefore never appears in content search.
--
-- The flag is a column rather than a convention because ingest's chunkless-repair
-- path rebuilds chunks for any current document that has none (the died-mid-ingest
-- failure mode). A stub is chunkless BY DESIGN; without a structural marker the
-- repair would "fix" it into a searchable document of garbage OCR on the next run.

alter table documents add column if not exists link_only boolean not null default false;
