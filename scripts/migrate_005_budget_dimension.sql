-- ============================================================
-- Migration 005: Budget line-item breakdown dimension
-- ============================================================
-- A single fiscal year's figures are broken down several ways (by fund,
-- by revenue source, by expenditure function). Each breakdown sums to the
-- same year total, so a chart must sum exactly one dimension or it would
-- double-count. The dimension column names which breakdown a row belongs
-- to; existing rows are the by-fund breakdown.

ALTER TABLE budget_line_items
    ADD COLUMN IF NOT EXISTS dimension TEXT NOT NULL DEFAULT 'fund';

CREATE INDEX IF NOT EXISTS idx_budget_dimension
    ON budget_line_items (dimension, fiscal_year, category);
