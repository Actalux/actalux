-- G2 follow-up (docs/architecture/land-use-cases-gap-closing.md): the measured
-- `other` residual was dominated by Planned Unit Development items, which the
-- v1 vocabulary had no type for. Operator decision 2026-09-12: add the type.
-- (The other residual cluster, Plan Commission impervious-coverage relief,
-- folds into site_plan — no schema change.) PUDs go to the City Council on a
-- PC recommendation, so they are advisory, like conditional uses.

alter table land_use_cases drop constraint if exists land_use_cases_application_type_check;
alter table land_use_cases add constraint land_use_cases_application_type_check
    check (application_type in (
        'conditional_use','site_plan','variance','subdivision',
        'rezoning','text_amendment','planned_unit_development','other'));
