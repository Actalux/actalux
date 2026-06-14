# Multi-jurisdiction architecture

Actalux launched covering one public body (the School District of Clayton, MO).
This document defines how it generalizes to many public bodies — additional
school districts, and eventually non-school bodies like city councils — without
a redesign or URL breakage. It is the reference the implementation builds
against.

## Core concepts

- **Public body (`entity`)** — the unit we archive: one school district, one city
  council, one county board. First-class, with its own boundaries and official
  IDs. Documents belong to exactly one entity.
- **Place** — a *discovery grouping*, **not** a legal boundary. `mo/clayton`
  means "the public bodies we cover associated with Clayton," a curated hub for
  findability. Districts and cities do not share boundaries (a district can span
  several municipalities; some districts map to no single city), so a place never
  asserts that the bodies under it are the same jurisdiction.

## URL scheme

| URL | Meaning |
|---|---|
| `actalux.org` | Global landing — mission + browse by state/place |
| `actalux.org/{state}` | *(optional)* state hub |
| `actalux.org/{state}/{place}` | Place hub — lists covered bodies for that place |
| `actalux.org/{state}/{place}/{body}` | One body's archive — search, results, reader |

- `{state}` — lowercase 2-char (`mo`). No country segment: this is a US civic
  tool; international expansion (very unlikely) would be special-cased rather
  than carried as a `/us` prefix on every URL forever.
- `{place}` — human slug (`clayton`). Unique per `(state, place)`. Collisions
  (two MO Claytons) get a county-qualified slug; rare.
- `{body}` — short stable slug: `schools` (school district), `council` (city
  council), `county`, …. Unique per `(place, body)`.
- Official IDs (NCES for school districts; Census Place / GNIS for
  municipalities) are **entity attributes, never URL segments** — stable
  internal keys, not user-facing.

Clayton today: `actalux.org/mo/clayton/schools`.

## Data model

```
places
  id           SERIAL PK
  state        TEXT   -- 2-char, e.g. 'mo'
  slug         TEXT   -- 'clayton'
  display_name TEXT   -- 'Clayton'
  county       TEXT   -- nullable, disambiguation only
  UNIQUE (state, slug)

entities                       -- a public body we archive
  id            SERIAL PK
  place_id      INT REFERENCES places(id)   -- nullable: a body need not map to a place
  body_slug     TEXT           -- URL segment under the place, e.g. 'schools'
  type          TEXT           -- 'school_district' | 'city_council' | 'county' | ...
  display_name  TEXT           -- 'School District of Clayton'
  external_ids  JSONB          -- {"nces": "2909000", ...}; NCES lives here, not in the URL
  UNIQUE (place_id, body_slug)

documents
  + entity_id   INT REFERENCES entities(id)   -- chunks inherit via their document
```

Search, retrieval, budget, and the reader all scope by `entity_id`. The search
RPCs (`semantic_search`, `keyword_search`) gain an optional `filter_entity_id`.

Seed (today): one `places` row (`mo` / `clayton` / "Clayton") and one `entities`
row (`schools`, `school_district`, `display_name` "School District of Clayton",
`external_ids` `{"nces": "<id>"}`, `place_id` → Clayton). Every existing document
backfills to that entity.

## Phasing

- **Phase A — schema + de-hardcode (no URL change).** Add the three tables;
  backfill the single Clayton entity and `documents.entity_id`. Make queries and
  templates entity-aware (display name from the entity, search filters by
  `entity_id`). Site behaves identically; it is just entity-scoped underneath.
- **Phase B — URL scheme + landing (the pre-promotion lock-in).** Mount routes
  under `/{state}/{place}/{body}`; Clayton at `/mo/clayton/schools`. Apex `/`
  becomes a landing (features Clayton while there is one body; a real directory
  as bodies are added). `/mo/clayton` is the place hub. Old unprefixed paths
  (`/search`, `/budget`, `/document/…`, `/chunk/…/source`) 301-redirect to their
  `/mo/clayton/schools/…` equivalents so nothing already linked breaks.
- **Phase C — second body (later).** Add `mo/clayton/council` or a second
  district when there is a real target. Schema and routing already support it,
  so it is data + a crawler, not a redesign.

A + B land **before** `actalux.org` is promoted (almost nothing is linked yet —
the cheapest moment to change URLs). C waits for a real second body.

## Migration & carry-over

- Additive, idempotent migrations (`migrate_0NN`): new tables + `ADD COLUMN
  entity_id`, with the Clayton backfill in the same migration. Search RPCs are
  re-created with the `filter_entity_id` parameter.
- The reranker, structured-finance routing, recall fixes (HNSW `ef_search`, FTS
  de-hyphen, superseded-version filter), and RLS all carry over unchanged — they
  gain an `entity_id` scope, nothing more.
- The one real refactor is Phase B's route reorganization (prefix every route +
  add redirects).

## Decisions log

- 2026-06-14: URL = `/{state}/{place}/{body}` with a place hub; `schools` /
  `council` body slugs; NCES (and municipal IDs) as `external_ids` attributes,
  not URL segments; no country segment; Phase A + B before promotion, C later.
