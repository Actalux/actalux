# Actalux

Searchable, citable local government records. Starting with Clayton, MO school district board meetings.

Every AI-generated statement cites a verbatim source quote. No unconstrained summaries.

## Status

Early development. Ingest pipeline (parse, chunk, embed, store) is built and tested. Search and web layers are next.

## Stack

Python 3.11+, FastAPI, Supabase (PostgreSQL + pgvector), HTMX + Jinja2.

## Setup

```bash
uv sync
cp .env.example .env  # fill in Supabase credentials
```

## License

TBD
