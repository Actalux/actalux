-- ============================================================
-- Migration 004: Budget line items (structured, citation-anchored)
-- ============================================================
-- Structured financial figures read from audited finance documents.
-- Every row carries the verbatim source_quote it was read from and a
-- chunk_id pointing at that passage, so each number on the Budget page
-- drills down to its source via /chunk/<id>/source. document_id is the
-- hard provenance anchor (citation-first); chunk_id is best-effort and
-- may go NULL if a document is re-chunked on re-ingest.

CREATE TABLE IF NOT EXISTS budget_line_items (
    id SERIAL PRIMARY KEY,
    fiscal_year TEXT NOT NULL,             -- e.g. '2023-2024'
    fund TEXT DEFAULT '',                  -- General, Teachers, Capital, Debt Service, ...
    category TEXT NOT NULL,                -- 'revenue' | 'expenditure' | 'fund_balance'
    subcategory TEXT DEFAULT '',           -- object/source detail within a category
    amount NUMERIC(14, 2) NOT NULL,
    document_id INT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    chunk_id INT REFERENCES chunks(id) ON DELETE SET NULL,
    source_quote TEXT DEFAULT '',          -- verbatim text supporting the figure
    note TEXT DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Charts read by fiscal year + category.
CREATE INDEX IF NOT EXISTS idx_budget_year_category
    ON budget_line_items (fiscal_year, category);
