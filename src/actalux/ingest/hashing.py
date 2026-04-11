"""Content hashing for document change detection."""

import hashlib


def content_hash(text: str) -> str:
    """SHA-256 hex digest of document content, whitespace-normalized."""
    normalized = " ".join(text.split())
    return hashlib.sha256(normalized.encode()).hexdigest()
