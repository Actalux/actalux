#!/usr/bin/env python3
"""Issue an Actalux v1 JSON API key.

Generates a random ``ak_<token>`` key, stores ONLY its sha256 hex in the
``api_keys`` table (so a DB leak can't reconstruct live keys), and prints the raw
key to stdout exactly once — it is unrecoverable afterward, so capture it now and
hand it to the holder over a secure channel.

Writes use the SERVICE (secret) key, which bypasses RLS — required because
migrate_026 denies anon all direct access to ``api_keys``. The service key must be
present (ACTALUX_SUPABASE_SERVICE_KEY); the script refuses to fall back to the
publishable key, which RLS would block.

Usage (always under doppler for the Supabase credentials):
    doppler run --project mac --config dev -- \\
        uv run python scripts/issue_api_key.py --label "Acme newsletter" --tier developer
    doppler run --project mac --config dev -- \\
        uv run python scripts/issue_api_key.py --label x --tier pro \\
            --expires 2027-01-01 --monthly-quota 1000000

``--tier`` defaults to ``developer``. ``--expires`` is an ISO date (YYYY-MM-DD);
omit for a non-expiring key. ``--monthly-quota`` overrides the tier's default
quota for this one key (omit to use the tier default the API enforces, or pass 0
explicitly only if you intend an immediately-exhausted key).
"""

from __future__ import annotations

import argparse
import hashlib
import secrets
import sys
from datetime import date

from actalux.config import API_TIERS, load_config
from actalux.db import get_client

KEY_PREFIX = "ak_"
# 32 bytes -> ~43 url-safe chars; ample entropy, and the sha256 of the full string
# (prefix + token) is what we store and what the API hashes to authorize.
TOKEN_BYTES = 32


def _generate_key() -> str:
    """A fresh ``ak_<token_urlsafe>`` API key."""
    return f"{KEY_PREFIX}{secrets.token_urlsafe(TOKEN_BYTES)}"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--label", required=True, help="Human label for the key holder.")
    parser.add_argument(
        "--tier",
        default="developer",
        # Only issuable tiers: 'anonymous' is the no-key path, and 'admin' is solely
        # the global ACTALUX_API_KEY (never a DB-issued key).
        choices=["developer", "pro"],
        help="Tier governing rate limits + default monthly quota.",
    )
    parser.add_argument(
        "--expires",
        default=None,
        help="Optional expiry date, ISO YYYY-MM-DD (omit for no expiry).",
    )
    parser.add_argument(
        "--monthly-quota",
        type=int,
        default=None,
        help="Override the tier's default monthly call quota for this key.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    expires_at: str | None = None
    if args.expires:
        try:
            expires_at = date.fromisoformat(args.expires).isoformat()
        except ValueError:
            raise SystemExit(f"--expires must be ISO YYYY-MM-DD, got {args.expires!r}") from None

    cfg = load_config()
    if not cfg.supabase_service_key:
        raise SystemExit(
            "ACTALUX_SUPABASE_SERVICE_KEY is required to write api_keys (RLS blocks the "
            "publishable key). Run under: doppler run --project mac --config dev -- ..."
        )

    raw_key = _generate_key()
    key_hash = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

    # The monthly quota stored on the row: explicit override if given, else the
    # tier's default. Both issuable tiers (developer, pro) carry a real default
    # quota, so an issued key is never NULL-unlimited by accident.
    monthly_quota = args.monthly_quota
    if monthly_quota is None:
        monthly_quota = API_TIERS[args.tier].monthly_quota

    row: dict[str, object] = {
        "key_hash": key_hash,
        "label": args.label,
        "tier": args.tier,
        "active": True,
        "monthly_quota": monthly_quota,
    }
    if expires_at is not None:
        row["expires_at"] = expires_at

    client = get_client(cfg.supabase_url, cfg.supabase_service_key)
    result = client.table("api_keys").insert(row).execute()
    inserted = (result.data or [{}])[0]

    quota_label = "unlimited" if monthly_quota is None else f"{monthly_quota:,} calls/month"
    print(f"Issued API key id={inserted.get('id', '?')} tier={args.tier} ({quota_label})")
    print(f"  label:   {args.label}")
    print(f"  expires: {expires_at or 'never'}")
    print()
    print("Raw key (shown ONCE — copy it now, it cannot be recovered):")
    print(f"  {raw_key}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
