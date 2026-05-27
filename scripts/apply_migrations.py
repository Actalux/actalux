#!/usr/bin/env python3
"""Apply pending Supabase schema migrations and track them in a ledger.

Migrations 002 and 003 once sat as committed files that were never run
against the live database, silently breaking search and per-card summaries
(the code expected columns that did not exist). This runner closes that gap:
a `schema_migrations` ledger records what has actually been applied, so
"is the schema up to date?" becomes a checkable, automatable question.

DDL runs through the Supabase Management API (the only programmatic DDL path
that works for this project). Every migration file MUST be idempotent
(`ADD COLUMN IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, ...): the runner
re-applies any migration missing from the ledger, and CI may run it
unattended.

Usage (always under doppler for ACTALUX_SUPABASE_URL + ACTALUX_SUPABASE_PAT):
    doppler run --project mac --config dev -- uv run python scripts/apply_migrations.py
    doppler run --project mac --config dev -- uv run python scripts/apply_migrations.py --dry-run
    doppler run --project mac --config dev -- uv run python scripts/apply_migrations.py --check
    doppler run --project mac --config dev -- uv run python scripts/apply_migrations.py --bootstrap

Modes:
    (default)     Apply every migration not yet in the ledger, then reload PostgREST.
    --dry-run     Report what would be applied. Mutates nothing. Always exits 0.
    --check       CI gate. Exits non-zero if any migration is pending or drifted.
    --bootstrap   Provision a fresh database from setup_db.sql, then apply migrations.

Checksum drift (an already-applied migration file edited after the fact) is a
hard error in every mode except --dry-run: edit history must live in new
migration files, never in applied ones.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCRIPTS_DIR = Path(__file__).resolve().parent
SETUP_SQL = SCRIPTS_DIR / "setup_db.sql"
MIGRATION_GLOB = "migrate_*.sql"
VERSION_RE = re.compile(r"^migrate_(\d+)_")
# Supabase project refs are 20 lowercase alphanumeric chars (the URL subdomain).
PROJECT_REF_RE = re.compile(r"^[a-z0-9]{20}$")
MGMT_API = "https://api.supabase.com/v1/projects/{ref}/database/query"
HTTP_TIMEOUT = 180.0  # bootstrap builds the HNSW index; give it room

LEDGER_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     TEXT PRIMARY KEY,
    filename    TEXT NOT NULL,
    checksum    TEXT NOT NULL,
    applied_at  TIMESTAMPTZ DEFAULT NOW()
);
"""

RELOAD_SCHEMA = "NOTIFY pgrst, 'reload schema';"


@dataclass(frozen=True)
class Migration:
    """A migration file discovered on disk."""

    version: str  # zero-padded digits parsed from the filename, e.g. "002"
    filename: str
    path: Path
    checksum: str  # sha256 hex of the file bytes
    sql: str


def parse_project_ref(supabase_url: str) -> str:
    """Extract the Supabase project ref from the project URL's subdomain.

    e.g. https://zeblohpnlznsmvzumpir.supabase.co -> "zeblohpnlznsmvzumpir".
    Avoids hardcoding the ref so the script follows whatever project the
    injected ACTALUX_SUPABASE_URL points at.
    """
    host = urlparse(supabase_url).hostname or ""
    ref = host.split(".")[0]
    if not PROJECT_REF_RE.match(ref):
        raise SystemExit(
            f"Could not parse a project ref from ACTALUX_SUPABASE_URL ({supabase_url!r}); "
            f"got {ref!r}, expected 20 lowercase alphanumeric chars."
        )
    return ref


def discover_migrations(scripts_dir: Path) -> list[Migration]:
    """Find migrate_*.sql files, parse versions, hash contents. Sorted by version."""
    migrations: list[Migration] = []
    for path in sorted(scripts_dir.glob(MIGRATION_GLOB)):
        match = VERSION_RE.match(path.name)
        if not match:
            logger.warning("Skipping %s: name does not match migrate_<digits>_*.sql", path.name)
            continue
        data = path.read_bytes()
        migrations.append(
            Migration(
                version=match.group(1),
                filename=path.name,
                path=path,
                checksum=hashlib.sha256(data).hexdigest(),
                sql=data.decode("utf-8"),
            )
        )
    migrations.sort(key=lambda m: int(m.version))
    return migrations


class SupabaseAdmin:
    """Thin wrapper over the Supabase Management API query endpoint."""

    def __init__(self, client: httpx.Client, ref: str, pat: str) -> None:
        self._client = client
        self._url = MGMT_API.format(ref=ref)
        self._headers = {"Authorization": f"Bearer {pat}", "Content-Type": "application/json"}
        self.ref = ref

    def query(self, sql: str) -> list[dict]:
        """Run a SQL body. Returns the row array (empty for DDL). Raises on error."""
        resp = self._client.post(self._url, headers=self._headers, json={"query": sql})
        if resp.status_code >= 400:
            raise SystemExit(f"Management API {resp.status_code}: {resp.text}")
        body = resp.json()
        return body if isinstance(body, list) else []


def ensure_ledger(admin: SupabaseAdmin) -> None:
    """Create the ledger table if it does not exist (idempotent)."""
    admin.query(LEDGER_DDL)


def fetch_applied(admin: SupabaseAdmin) -> dict[str, str]:
    """Return {version: checksum} of applied migrations. Read-only.

    Returns an empty mapping when the ledger does not exist yet, so the
    read-only modes (--dry-run, --check) never have to create it.
    """
    present = admin.query("SELECT to_regclass('public.schema_migrations') IS NOT NULL AS present;")
    if not (present and present[0]["present"]):
        return {}
    rows = admin.query("SELECT version, checksum FROM schema_migrations;")
    return {row["version"]: row["checksum"] for row in rows}


def detect_drift(discovered: list[Migration], applied: dict[str, str]) -> list[str]:
    """Versions whose on-disk checksum no longer matches the recorded checksum."""
    drifted = []
    for m in discovered:
        recorded = applied.get(m.version)
        if recorded is not None and recorded != m.checksum:
            drifted.append(m.version)
    return drifted


def apply_migration(admin: SupabaseAdmin, m: Migration) -> None:
    """Run one migration's SQL, then record it in the ledger.

    The Management API query endpoint accepts only a raw SQL string (no bound
    parameters), so the ledger INSERT is built directly. The three values are
    fully controlled — version is digits, filename matches migrate_<digits>_*.sql,
    checksum is sha256 hex — and dollar-quoted ($v$...$v$) so no value can
    contain the closing tag.
    """
    logger.info("Applying %s (%s)...", m.version, m.filename)
    admin.query(m.sql)
    admin.query(
        "INSERT INTO schema_migrations (version, filename, checksum) VALUES "
        f"($v${m.version}$v$, $v${m.filename}$v$, $v${m.checksum}$v$) "
        "ON CONFLICT (version) DO NOTHING;"
    )
    logger.info("Recorded %s in ledger.", m.version)


def report(
    discovered: list[Migration], applied: dict[str, str], drifted: list[str]
) -> list[Migration]:
    """Log current state; return the pending migrations (not yet in the ledger)."""
    pending = [m for m in discovered if m.version not in applied]
    logger.info(
        "Discovered %d migration(s); %d applied, %d pending, %d drifted.",
        len(discovered),
        len(applied),
        len(pending),
        len(drifted),
    )
    for m in discovered:
        if m.version in drifted:
            state = "DRIFTED (file edited after apply)"
        elif m.version in applied:
            state = "applied"
        else:
            state = "PENDING"
        logger.info("  %s  %-40s  %s", m.version, m.filename, state)
    return pending


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Report the plan; mutate nothing.")
    mode.add_argument(
        "--check", action="store_true", help="CI gate: non-zero exit on pending/drift."
    )
    mode.add_argument(
        "--bootstrap",
        action="store_true",
        help="Provision a fresh DB from setup_db.sql, then apply migrations.",
    )
    args = parser.parse_args()

    try:
        supabase_url = os.environ["ACTALUX_SUPABASE_URL"]
        pat = os.environ["ACTALUX_SUPABASE_PAT"]
    except KeyError as exc:
        raise SystemExit(
            f"Missing required env var {exc}. Run under: "
            f"doppler run --project mac --config dev -- ..."
        ) from exc

    ref = parse_project_ref(supabase_url)
    discovered = discover_migrations(SCRIPTS_DIR)
    if not discovered:
        logger.warning("No migration files found in %s", SCRIPTS_DIR)
        return 0

    with httpx.Client(timeout=HTTP_TIMEOUT) as client:
        admin = SupabaseAdmin(client, ref, pat)
        logger.info("Target project ref: %s", ref)

        if args.bootstrap:
            logger.warning("BOOTSTRAP: running setup_db.sql against project %s", ref)
            admin.query(SETUP_SQL.read_text(encoding="utf-8"))
            logger.info("Baseline schema applied from %s", SETUP_SQL.name)

        # Read-only modes never create the ledger; apply modes do.
        if not (args.dry_run or args.check):
            ensure_ledger(admin)
        applied = fetch_applied(admin)
        drifted = detect_drift(discovered, applied)
        pending = report(discovered, applied, drifted)

        if drifted and not args.dry_run:
            raise SystemExit(
                f"Checksum drift in migration(s) {drifted}: an applied migration file was "
                f"edited. Revert the edit or capture the change in a NEW migrate_*.sql file."
            )

        if args.dry_run:
            if pending:
                logger.info("Would apply: %s", [m.version for m in pending])
            else:
                logger.info("Up to date; nothing to apply.")
            return 0

        if args.check:
            if pending or drifted:
                logger.error(
                    "Schema not up to date (pending=%s drifted=%s).",
                    [m.version for m in pending],
                    drifted,
                )
                return 1
            logger.info("Schema up to date.")
            return 0

        # default + bootstrap: apply pending
        if not pending:
            logger.info("Up to date; nothing to apply.")
            return 0
        for m in pending:
            apply_migration(admin, m)
        admin.query(RELOAD_SCHEMA)
        logger.info("Applied %d migration(s); PostgREST schema reloaded.", len(pending))
        return 0


if __name__ == "__main__":
    sys.exit(main())
