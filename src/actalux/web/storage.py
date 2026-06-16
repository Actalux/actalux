"""Supabase Storage public-URL helpers for the web layer.

The ``documents`` bucket holds uploaded source files keyed by ``source_file``
(the plain filename). Public-bucket URLs are deterministic, so we build them
from config + key rather than carrying a per-row ``storage_url`` column. Used
only to embed/download stored files (PDFs); the canonical "Open original" link
still comes from ``documents.source_url``.
"""

from __future__ import annotations

from urllib.parse import quote

from actalux.config import Config, load_config

# Supabase serves a public bucket at
# ``{base}/storage/v1/object/public/{bucket}/{key}``. Kept here as the single
# source of truth for the path so audit/restore tooling can match on it.
_PUBLIC_OBJECT_PREFIX = "storage/v1/object/public"
BUCKET = "documents"


def build_stored_file_url(base: str, source_file: str) -> str:
    """Build the public bucket URL for a stored file, URL-encoding the key.

    Pure helper (no env access) so it is unit-testable and reusable by tooling.

    Parameters
    ----------
    base
        Supabase project URL (e.g. ``https://abc.supabase.co``). A trailing
        slash is tolerated.
    source_file
        The storage key, equal to ``documents.source_file`` (a plain filename,
        which may contain spaces, commas, or other characters unsafe in a URL).

    Returns
    -------
    str
        The percent-encoded public URL, or ``""`` when ``source_file`` is empty
        (no file to link to).
    """
    if not source_file:
        return ""
    # Encode the key as a single path segment: spaces, commas, etc. must be
    # escaped, but "/" is left safe so nested keys (if ever used) stay intact.
    # "%" is safe so an already-encoded key is not double-encoded.
    encoded = quote(source_file, safe="/%")
    return f"{base.rstrip('/')}/{_PUBLIC_OBJECT_PREFIX}/{BUCKET}/{encoded}"


def stored_file_url(source_file: str, cfg: Config | None = None) -> str:
    """Public bucket URL for ``source_file``, with the base taken from config.

    Parameters
    ----------
    source_file
        The storage key (``documents.source_file``).
    cfg
        Optional preloaded config; loaded from the environment when omitted.

    Returns
    -------
    str
        The percent-encoded public URL, or ``""`` when ``source_file`` is empty.
    """
    if not source_file:
        return ""
    cfg = cfg or load_config()
    return build_stored_file_url(cfg.supabase_url, source_file)
