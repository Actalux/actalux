-- G1 of the land-use gap-closing plan (docs/architecture/land-use-cases-gap-closing.md):
-- BoA variance items state the cited code section ("Section 405.330.A.5") and the
-- quantified relief ("A 200 square-foot variance from the maximum living area") in
-- their text. For a variance dataset these are the first two columns a reader asks
-- for, so events carry them — verbatim, nullable, present only when the item states
-- them.

alter table land_use_case_events add column if not exists code_section text;
alter table land_use_case_events add column if not exists relief_raw text;
