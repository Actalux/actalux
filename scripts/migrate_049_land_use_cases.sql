-- Land-use case dataset (stealth v1). Design: docs/architecture/land-use-cases.md
-- Entitlement cases before Clayton's PC-ARB and Board of Adjustment, linked
-- across meetings, every event citation-backed. No public surface reads these
-- tables; access is the keyed API only.

create table if not exists land_use_cases (
    id bigint generated always as identity primary key,
    place_id bigint not null references places(id),
    entity_id bigint not null references entities(id),
    address_raw text not null,
    -- lowercased/collapsed form used only for case linking; display always uses address_raw
    address_norm text not null,
    application_type text not null check (application_type in (
        'conditional_use','site_plan','variance','subdivision',
        'rezoning','text_amendment','other')),
    subtype_raw text,
    status text not null default 'pending' check (status in (
        'pending','approved','approved_with_conditions','denied',
        'continued','withdrawn','recommended_to_council')),
    -- PC recommends CUPs/rezonings to council (advisory); decides site plans and
    -- the BoA decides variances (final).
    decision_role text check (decision_role in ('final','advisory')),
    staff_recommendation text check (staff_recommendation in (
        'approve','approve_with_conditions','deny','none_stated')),
    -- null until the case resolves; the comparative product's core column
    outcome_matches_staff boolean,
    first_seen date not null,
    resolved_date date,
    council_matter_id bigint references subjects(id),
    created_at timestamptz not null default now(),
    updated_at timestamptz
);

create index if not exists land_use_cases_place_idx
    on land_use_cases (place_id, entity_id, application_type);
create index if not exists land_use_cases_addr_idx
    on land_use_cases (place_id, address_norm);

create table if not exists land_use_case_events (
    id bigint generated always as identity primary key,
    case_id bigint not null references land_use_cases(id) on delete cascade,
    document_id bigint not null references documents(id),
    chunk_id bigint references chunks(id),
    citation_id text,
    video_timestamp text,
    event_date date not null,
    action text not null check (action in (
        'heard','continued','approved','approved_with_conditions',
        'denied','recommended','withdrawn')),
    conditions_text text,
    vote_id bigint references votes(id),
    -- Citation-first is structural: an event with no verbatim quote cannot exist.
    source_quote text not null check (length(source_quote) > 0),
    created_at timestamptz not null default now()
);

create index if not exists land_use_case_events_case_idx
    on land_use_case_events (case_id, event_date);
create index if not exists land_use_case_events_doc_idx
    on land_use_case_events (document_id);

create table if not exists land_use_case_parties (
    id bigint generated always as identity primary key,
    case_id bigint not null references land_use_cases(id) on delete cascade,
    role text not null check (role in (
        'applicant','owner','tenant','architect','attorney','other')),
    name_raw text not null
    -- Deliberately NO subject_id and no person linkage of any kind: applicants
    -- and owners are private individuals, named per-record only. Enforced by
    -- absence, the same way transcript_speaker_names enforces tier-2 naming.
    -- Adding person-entity linkage here requires an explicit policy decision.
);

create index if not exists land_use_case_parties_case_idx
    on land_use_case_parties (case_id);

-- RLS: deny-all for the anon (publishable) key. These tables have no public
-- surface; only the service key (writers, keyed API handlers) reads them.
alter table land_use_cases enable row level security;
alter table land_use_case_events enable row level security;
alter table land_use_case_parties enable row level security;
