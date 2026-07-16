"""Embedding-cache layout + manifest — which meetings a cache holds, and in what population.

The ``[E]`` cache comes in two populations that must never mix:

- ``anchored`` — only officials' anchored clusters. The measurement benchmark.
- ``all`` — every diarization cluster in every meeting. What the identity proposer needs, because a
  cluster it might name is by definition one no anchor covers.

They get separate directories AND a manifest, because the build is *resumable*: it skips any meeting
whose ``doc_<id>.npz`` already exists. Point an all-cluster run at a directory holding anchored
files and those meetings keep their anchored-only clusters — the cache is then partial in a way
nothing downstream can detect (the proposer simply never sees the missing voices and reports fewer
proposals, with no error). The manifest turns that silent corruption into a loud refusal.

Pure: json + paths, no DB, no Modal.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from actalux.errors import ActaluxError

MANIFEST_NAME = "manifest.json"
MODE_ANCHORED = "anchored"
MODE_ALL = "all"


@dataclass(frozen=True)
class CacheManifest:
    """What a cache directory holds; pinned so a resume cannot change the population mid-cache."""

    mode: str  # MODE_ANCHORED | MODE_ALL
    min_seconds: float
    model: str


def cache_dir(root: Path | str, state: str, place: str, body: str, *, mode: str) -> Path:
    """The cache dir for one body + population; all-cluster caches get their own ``_all`` dir."""
    suffix = "_all" if mode == MODE_ALL else ""
    return Path(root) / f"{state}_{place}_{body}{suffix}"


def read_manifest(directory: Path | str) -> CacheManifest | None:
    """The directory's manifest, or ``None`` when it has none (a legacy, pre-manifest cache)."""
    path = Path(directory) / MANIFEST_NAME
    if not path.is_file():
        return None
    data = json.loads(path.read_text())
    return CacheManifest(
        mode=data["mode"], min_seconds=float(data["min_seconds"]), model=data["model"]
    )


def write_manifest(directory: Path | str, manifest: CacheManifest) -> None:
    """Stamp a cache directory with the population it holds."""
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)
    (path / MANIFEST_NAME).write_text(json.dumps(asdict(manifest), indent=2) + "\n")


def has_cached_documents(directory: Path | str) -> bool:
    """Does this directory already hold per-document caches (i.e. is a resume in progress)?"""
    return any(Path(directory).glob("doc_*.npz"))


def ensure_manifest(directory: Path | str, expected: CacheManifest) -> None:
    """Pin a cache dir to one population: write the manifest, or hard-error on a mismatch.

    A missing manifest next to existing ``doc_*.npz`` is a LEGACY cache (built before manifests
    existed). Those were always anchored-only, so an anchored run may adopt it, but an all-cluster
    run must refuse: it would inherit anchored-only meetings as though they were complete.
    """
    found = read_manifest(directory)
    if found is None:
        if expected.mode != MODE_ANCHORED and has_cached_documents(directory):
            raise ActaluxError(
                f"{directory} holds cached meetings but no {MANIFEST_NAME} — a legacy "
                f"'{MODE_ANCHORED}' cache. Refusing to extend it in '{expected.mode}' mode: the "
                f"meetings already cached would keep their anchored-only clusters and the result "
                f"would be a silently partial cache. Build into a fresh directory."
            )
        write_manifest(directory, expected)
        return
    for field, found_value, want_value in (
        ("mode", found.mode, expected.mode),
        ("min_seconds", found.min_seconds, expected.min_seconds),
        ("model", found.model, expected.model),
    ):
        if found_value != want_value:
            raise ActaluxError(
                f"{directory} was built with {field}={found_value!r} but this run uses "
                f"{want_value!r} — a resume would blend two cluster populations in one cache."
            )


def require_mode(directory: Path | str, mode: str) -> None:
    """Refuse to CONSUME a cache built for a different population than the caller needs."""
    found = read_manifest(directory)
    if found is None:
        raise ActaluxError(
            f"{directory} has no {MANIFEST_NAME}, so it is a legacy '{MODE_ANCHORED}' cache, but a "
            f"'{mode}' cache is required here. An anchored cache holds only already-named "
            f"clusters, so this run would find nothing to do and report success anyway."
        )
    if found.mode != mode:
        raise ActaluxError(
            f"{directory} is a '{found.mode}' cache but a '{mode}' cache is required here."
        )
