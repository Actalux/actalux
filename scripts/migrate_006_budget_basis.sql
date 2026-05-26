-- ============================================================
-- Migration 006: Budget-vs-actual basis
-- ============================================================
-- The audited budget-and-actual schedules report each figure on three
-- bases: the originally adopted budget, the final amended budget, and the
-- actual (budgetary / cash basis) result. The basis column names which one
-- a row carries. It is NULL for every existing row (those are GAAP figures
-- from the fund financial statements); only dimension='budget' rows set it.

ALTER TABLE budget_line_items
    ADD COLUMN IF NOT EXISTS basis TEXT;

CREATE INDEX IF NOT EXISTS idx_budget_basis
    ON budget_line_items (dimension, basis, fiscal_year, fund);
