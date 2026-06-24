"""Application configuration from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from types import MappingProxyType


@dataclass(frozen=True)
class ApiTier:
    """Per-tier limits for an issued API key.

    ``search_per_min`` / ``general_per_min`` are the per-IP, per-minute rate caps
    for the search endpoint (runs the paid reranker) and the cheap DB endpoints
    respectively. ``monthly_quota`` caps total calls per calendar month — ``None``
    means unmetered (no quota gate).
    """

    search_per_min: int
    general_per_min: int
    monthly_quota: int | None


# Tier table for the v1 JSON API. The tier name a key carries (column ``tier`` in
# ``api_keys``, or the special ``admin`` tier granted by the global ACTALUX_API_KEY)
# selects its limits here. ``anonymous`` is the no-key/free tier; its numbers MUST
# equal the historical flat ``rate_limit_*`` config (so the open path is unchanged),
# and ``Config`` reads them straight from those fields below rather than hardcoding
# them twice. A key whose stored tier is not in this table falls back to
# ``developer`` at the api.py call site. Read-only mapping so it can't be mutated.
API_TIERS: MappingProxyType[str, ApiTier] = MappingProxyType(
    {
        # No key: identical to today's open path. monthly_quota=None (unmetered).
        # Numbers are placeholders here; the live anonymous limits come from the
        # Config.rate_limit_* fields via Config.tier("anonymous").
        "anonymous": ApiTier(search_per_min=30, general_per_min=60, monthly_quota=None),
        "developer": ApiTier(search_per_min=60, general_per_min=120, monthly_quota=50_000),
        "pro": ApiTier(search_per_min=120, general_per_min=300, monthly_quota=500_000),
        # Admin = the operator's global key; unmetered, highest ceilings.
        "admin": ApiTier(search_per_min=600, general_per_min=1200, monthly_quota=None),
    }
)


@dataclass(frozen=True)
class Config:
    """Immutable application configuration."""

    supabase_url: str = field(default_factory=lambda: os.environ["ACTALUX_SUPABASE_URL"])
    # Publishable key: RLS-enforced, safe for the public web app.
    supabase_key: str = field(default_factory=lambda: os.environ["ACTALUX_SUPABASE_KEY"])
    # Service (secret) key: bypasses RLS. Used only by ingest/backfill/load
    # writers, never by the web app, so the web host doesn't need to carry it.
    supabase_service_key: str = field(
        default_factory=lambda: os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    )
    buttondown_api_key: str = field(
        default_factory=lambda: os.environ.get("BUTTONDOWN_API_KEY", "")
    )
    anthropic_api_key: str = field(default_factory=lambda: os.environ.get("ANTHROPIC_API_KEY", ""))
    openai_api_key: str = field(default_factory=lambda: os.environ.get("OPENAI_API_KEY", ""))
    # OpenRouter: one key reaches many models for synthesis A/B (eval/eval_answers).
    openrouter_api_key: str = field(
        default_factory=lambda: os.environ.get("OPENROUTER_API_KEY", "")
    )
    summary_model: str = "gpt-5-mini"
    # Follow-ups are condensed into a standalone retrieval query — a mechanical
    # rewrite, not a reasoning task — so a fast non-reasoning model keeps that
    # extra LLM hop off the answer's critical path (the reasoning summary model
    # added ~1.4s per follow-up; see task #19 latency measurement).
    condense_model: str = "gpt-4o-mini"
    # Query expansion: also retrieve LLM-generated alternate phrasings of the
    # query and fuse the candidate pools, so a question whose wording differs
    # from the records ("did the bond measure pass" vs "Proposition O") still
    # surfaces the right document. Off by default — a deliberate deploy-time
    # opt-in like the reranker (set ACTALUX_QUERY_EXPANSION=on), since it adds
    # one cheap LLM hop plus parallel extra retrieval round-trips per search.
    query_expansion_mode: str = field(
        default_factory=lambda: os.environ.get("ACTALUX_QUERY_EXPANSION", "off")
    )
    # Cheap non-reasoning model for the expansion hop (same class as condense).
    expansion_model: str = "gpt-4o-mini"
    # Number of alternate phrasings retrieved alongside the original query.
    expansion_count: int = 3
    # ZeroEntropy hosted reranker. Key gates the API call; zerank-1-small is the
    # Apache-2.0 model that won the retrieval eval (+24% nDCG@10; see eval/README.md).
    zeroentropy_api_key: str = field(
        default_factory=lambda: os.environ.get("ZEROENTROPY_API_KEY", "")
    )
    rerank_model: str = "zerank-1-small"
    # "off" (RRF only, default) or "api" (rerank the RRF pool via ZeroEntropy).
    # Default off so the reranker is a deliberate, deploy-time opt-in (set
    # ACTALUX_RERANK=api in the web host's secrets) with no surprise cost/latency.
    rerank_mode: str = field(default_factory=lambda: os.environ.get("ACTALUX_RERANK", "off"))
    # RRF candidates reranked before truncating to search_max_results. Reranking
    # a deeper pool is what lets the cross-encoder lift a buried-but-relevant hit.
    rerank_pool_size: int = 50
    embedding_model: str = "BAAI/bge-small-en-v1.5"
    embedding_dim: int = 384
    # Board-meeting transcription (Whisper). Audio is transcribed via Groq's
    # OpenAI-compatible API (free tier, whisper-large-v3 — better than whisper-1
    # and faster), keyed by GROQ_ACTALUX_API_KEY (namespaced separately from any
    # other Groq usage). transcribe.py also accepts these as plain args, so the
    # provider can be swapped (e.g. back to OpenAI) without code change.
    groq_api_key: str = field(default_factory=lambda: os.environ.get("GROQ_ACTALUX_API_KEY", ""))
    transcribe_model: str = "whisper-large-v3"
    transcribe_base_url: str = "https://api.groq.com/openai/v1"
    chunk_target_words: int = 200
    chunk_overlap_sentences: int = 2
    # Ingest-time PII guard: "block" (skip flagged docs, default), "warn", "off".
    pii_guard_mode: str = field(
        default_factory=lambda: os.environ.get("ACTALUX_PII_GUARD", "block")
    )
    search_similarity_threshold: float = 0.35
    search_max_results: int = 20
    search_rrf_k: int = 60
    topic_cache_ttl_seconds: int = 3600
    rate_limit_search_per_minute: int = 30
    rate_limit_corrections_per_hour: int = 5
    # JSON API (v1). The key is optional: unset -> the API is open (read-only,
    # rate-limited); set -> a valid X-API-Key header is required. Lets the API
    # be locked down at deploy time with no code change.
    api_key: str = field(default_factory=lambda: os.environ.get("ACTALUX_API_KEY", ""))
    rate_limit_api_per_minute: int = 60
    # Per-holder issued API keys (the keyed-DB path). Off by default so the keyed
    # path stays fully dormant in prod — a presented non-global key 401s WITHOUT
    # any DB call until this is turned on (ACTALUX_API_KEYS=on) once keys exist.
    api_keys_enabled: bool = field(
        default_factory=lambda: (
            os.environ.get("ACTALUX_API_KEYS", "").strip().lower() in ("on", "true", "1")
        )
    )
    # When the keyed path IS enabled, a cheap per-IP minute cap on key-auth attempts
    # gates the api_key_authorize RPC, so a flood of bogus keys can't hammer the DB.
    rate_limit_auth_attempts_per_minute: int = 20
    # Ask page (the cited chatbot). It is the most expensive public endpoint
    # (condense + retrieve + rerank + generate per turn) and has no API key, so
    # it carries both a per-IP minute limit and a global per-day message cap to
    # bound LLM spend. The caps are in-process (single-instance deploy); a
    # multi-instance deploy would need a shared store.
    rate_limit_ask_per_minute: int = 8
    ask_daily_message_cap: int = 400
    # Bounds on the client-carried conversation history honored per turn, so a
    # crafted request cannot inflate condense token cost without limit.
    ask_history_max_turns: int = 8
    ask_history_max_chars: int = 8000
    # Upper bound on a single question before any LLM work, so a crafted large
    # post cannot inflate condense/embed cost. Genuine questions are far shorter.
    ask_question_max_chars: int = 2000
    # Public site origin, used to turn the digest drafter's [#qXXXX] citations
    # into absolute links a Substack draft / email can resolve.
    site_base_url: str = field(
        default_factory=lambda: os.environ.get("ACTALUX_SITE_BASE_URL", "https://actalux.org")
    )
    # SMTP delivery for the change-digest drafter (the weekly "what's new" email).
    # All optional and provider-agnostic (Gmail app-password, Resend SMTP, Fastmail,
    # ...): when host/from/to are unset the drafter still writes the draft file, it
    # just does not email. No secret is required for the pipeline to run.
    smtp_host: str = field(default_factory=lambda: os.environ.get("ACTALUX_SMTP_HOST", ""))
    # `or "587"` (not a default arg) so an env var present-but-empty -- which is how
    # CI renders an unset secret -- still parses, instead of int("") raising.
    smtp_port: int = field(
        default_factory=lambda: int(os.environ.get("ACTALUX_SMTP_PORT") or "587")
    )
    smtp_user: str = field(default_factory=lambda: os.environ.get("ACTALUX_SMTP_USER", ""))
    smtp_password: str = field(default_factory=lambda: os.environ.get("ACTALUX_SMTP_PASSWORD", ""))
    draft_email_from: str = field(
        default_factory=lambda: os.environ.get("ACTALUX_DRAFT_EMAIL_FROM", "")
    )
    draft_email_to: str = field(
        default_factory=lambda: os.environ.get("ACTALUX_DRAFT_EMAIL_TO", "")
    )

    def tier(self, name: str) -> ApiTier:
        """Resolve a tier name to its limits.

        The ``anonymous`` (no-key) tier reads its per-minute caps from this
        instance's flat ``rate_limit_*`` fields, so the open path keeps the exact
        numbers it has always used (and stays adjustable by the same env knobs).
        Every other tier comes from the static ``API_TIERS`` table. An unknown
        name resolves to ``developer`` — the conservative paid floor — so a stale
        tier string in the DB can never accidentally grant more than that.
        """
        if name == "anonymous":
            return ApiTier(
                search_per_min=self.rate_limit_search_per_minute,
                general_per_min=self.rate_limit_api_per_minute,
                monthly_quota=None,
            )
        return API_TIERS.get(name, API_TIERS["developer"])


def load_config() -> Config:
    """Load config from environment. Raises KeyError if required vars are missing."""
    return Config()
