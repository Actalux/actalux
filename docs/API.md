# Actalux JSON API (v1)

A read-only HTTP/JSON API over the Actalux archive of Clayton, MO public records.
Every result is a verbatim passage from a public record, carrying a citation and a
deep link back to the source document or meeting video. The API mirrors the public
site's retrieval and never exposes more than the site does.

Base path: `/api/v1`. Routes are entity-scoped under `/{state}/{place}/{body}`,
e.g. `/api/v1/mo/clayton/schools/search`.

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/{state}/{place}/{body}/search?q=...` | Ranked verbatim passages with citations and source links |
| `GET` | `/{state}/{place}/{body}/meetings/{date}` | Every minutes / transcript / resolution document for one meeting date |
| `GET` | `/{state}/{place}/{body}/recent` | Recent meeting documents, newest first ("what's new since a date") |
| `GET` | `/{state}/{place}/{body}/votes` | Structured, cited board-vote records |

Interactive schema: `/docs` (Swagger UI) and `/openapi.json`.

## Authentication

Pass a key as `X-API-Key: <key>` or `Authorization: Bearer <key>`.

- **No key** — the request runs at the anonymous (free) tier: open, read-only,
  rate-limited.
- **A key** — the request runs at the tier the key was issued under, with that
  tier's rate limits and monthly call quota. An invalid key returns `401`; a valid
  key that has exceeded its monthly quota returns `429`.

Keys are issued out-of-band by the operator. Treat a key as a secret: it grants
your tier's allowance and is attributable to you.

## Rate limits and quotas

Rate limits are per client IP, per minute, per endpoint class (the search endpoint
runs a reranker and carries a tighter limit than the cheaper document endpoints).
A `429` response includes a `Retry-After` header (seconds). Monthly quotas, where
they apply, count total calls per calendar month (UTC); a key over its monthly
quota returns `429` until the next month.

## Terms of Use

By using this API you agree to the following. These terms supplement, and do not
replace, the project's license (see `LICENSE`, Business Source License 1.1).

1. **Preserve citations.** Every passage is returned with its source citation and
   a link to the originating record. When you reuse, display, or quote a passage,
   you must keep the citation and link intact. Do not present archive content
   stripped of its source.

2. **Do not misrepresent the records.** Do not alter the verbatim text of a
   passage, attribute it to the wrong record, paraphrase it as if it were a quote,
   or otherwise present it in a way that changes what the underlying public record
   says. Actalux makes no claim that the archive is a complete record of any body's
   proceedings; do not represent it as one.

3. **Commercial / production use requires a license.** Under the Business Source
   License 1.1 the Additional Use Grant is "None," so production use — including
   any commercial, revenue-generating, or government/municipal deployment — is not
   permitted without a separate commercial license from the Licensor (Actalux LLC).
   Non-production use (evaluation, research, personal, development) is permitted
   under the BUSL terms. Contact the Licensor for a production or commercial
   license. (On the license's Change Date the work converts to AGPL v3.0-or-later;
   until then, the BUSL terms govern.)

4. **Rate limits apply.** Do not attempt to circumvent rate limits or quotas
   (e.g. by rotating IPs or keys to evade a cap). Automated bulk extraction that
   reconstructs the archive wholesale is a production use and requires a license.

5. **No warranty.** The API and its content are provided "AS IS," without
   warranties of any kind, to the extent permitted by law (consistent with the
   BUSL disclaimer in `LICENSE`). The records are sourced from public bodies; the
   authoritative version of any record is the originating document, which every
   result links to. Verify against the source before relying on a passage.

## Versioning & Deprecation policy

The API is versioned in the path (`/api/v1`). The reported version follows
semantic versioning (currently `1.0.0`; see `/openapi.json`).

- **v1 is stable and additive-only.** Within v1 we will not make breaking changes
  to existing endpoints. We may add new endpoints, add new optional query
  parameters, and add new fields to existing response objects. Clients must
  tolerate unknown fields and must not assume the set of response fields is fixed.

- **Breaking changes get a new version.** Any change that removes or renames an
  endpoint or response field, changes a field's type or meaning, or changes
  default behavior in a way existing clients cannot ignore will ship under a new
  version path (`/api/v2`, ...), not by mutating `/api/v1`.

- **Deprecation window.** When a version is slated for removal it is first marked
  deprecated and continues to serve for a published deprecation window before
  removal, so integrators have time to migrate. Deprecated responses are
  documented in the changelog; the endpoint keeps working during the window.

What is *not* a breaking change (and so may happen within v1 without notice):
adding endpoints or optional parameters, adding response fields, changing result
ordering within documented bounds, tightening abuse/rate-limit controls, and
fixing a documented behavior to match its documentation.
