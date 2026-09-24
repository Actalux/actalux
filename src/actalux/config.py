"""Application configuration from environment variables."""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType

MODEL_SETTINGS_PATH = Path(__file__).with_name("model_settings.toml")


def _load_model_settings() -> dict[str, dict[str, str]]:
    """Read model_settings.toml — the single configuration for every model."""
    with MODEL_SETTINGS_PATH.open("rb") as f:
        return tomllib.load(f)


MODEL_SETTINGS = MappingProxyType(_load_model_settings())


def _model(section: str, key: str, env_var: str | None = None) -> str:
    """A model ID from model_settings.toml, optionally overridden for one run.

    ``env_var`` is the per-run override for experiments; pinned models pass none,
    so they can only change by editing the file. An empty variable is ignored
    rather than blanking the model.
    """
    if env_var and os.environ.get(env_var):
        return os.environ[env_var]
    return MODEL_SETTINGS[section][key]


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
    # OpenRouter is the single gateway for every chat/completion LLM call (search
    # summaries, the ask chatbot, follow-up condense, query expansion, the digest):
    # the OpenAI SDK targets ``openrouter_base_url`` with this key and reaches the
    # same models by provider-prefixed id (``openai/gpt-5-mini``, ...). One key,
    # one place for all LLMs. New actalux-scoped name, old name kept as a fallback
    # so moving the secret is non-breaking.
    # Actalux's own OpenRouter key ONLY. There is deliberately no fallback to the
    # generic OPENROUTER_API_KEY: on a machine that also runs other projects, that
    # name belongs to a different account, and a silent fallback billed Actalux
    # work to it (and inherited its privacy settings) until 2026-09-23. Every
    # third-party key below follows the same rule: Actalux-named, or nothing.
    openrouter_api_key: str = field(
        default_factory=lambda: os.environ.get("OPENROUTER_ACTALUX_KEY", "")
    )
    openrouter_base_url: str = "https://openrouter.ai/api/v1"
    # The public answer model (/ask, search summaries). Changing it changes what
    # citizens read, so validate first with scripts/eval_answers.py.
    summary_model: str = field(
        default_factory=lambda: _model("llm", "summary", "ACTALUX_SUMMARY_MODEL")
    )
    # Offline jobs get their own settings so one can move without moving the
    # public answer model. Each falls back to the summary model's variable, then
    # to the same default, so an unconfigured deploy behaves exactly as before.
    doc_summary_model: str = field(
        default_factory=lambda: _model("llm", "doc_summary", "ACTALUX_DOC_SUMMARY_MODEL")
    )
    landuse_model: str = field(
        default_factory=lambda: _model("llm", "landuse", "ACTALUX_LANDUSE_MODEL")
    )
    discourse_model: str = field(
        default_factory=lambda: _model("llm", "discourse", "ACTALUX_DISCOURSE_MODEL")
    )
    # Follow-ups are condensed into a standalone retrieval query — a mechanical
    # rewrite, not a reasoning task — so a fast non-reasoning model keeps that
    # extra LLM hop off the answer's critical path (the reasoning summary model
    # added ~1.4s per follow-up; see task #19 latency measurement).
    condense_model: str = field(
        default_factory=lambda: _model("llm", "condense", "ACTALUX_CONDENSE_MODEL")
    )
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
    expansion_model: str = field(
        default_factory=lambda: _model("llm", "expansion", "ACTALUX_EXPANSION_MODEL")
    )
    # Number of alternate phrasings retrieved alongside the original query.
    expansion_count: int = 3
    # ZeroEntropy hosted reranker. Key gates the API call; zerank-1-small is the
    # Apache-2.0 model that won the retrieval eval (+24% nDCG@10; see eval/README.md).
    zeroentropy_api_key: str = field(default_factory=lambda: os.environ.get("ACTALUX_ZE", ""))
    # Model per reranker provider, from model_settings.toml [rerank].
    rerank_models: MappingProxyType = field(
        default_factory=lambda: MappingProxyType(
            {
                provider: _model("rerank", provider, f"ACTALUX_RERANK_{provider.upper()}_MODEL")
                for provider in MODEL_SETTINGS["rerank"]
            }
        )
    )
    # Candidate replacements for the ZeroEntropy sunset (2026-09-04). Keys are
    # read here so the eval can run a provider arm; production still selects
    # zeroentropy until the eval says which one earns the swap. An unset key
    # simply means that arm cannot run — it is not an error.
    # ACTALUX_-prefixed first: third-party tokens carry a product prefix so a
    # shared Doppler project cannot collide with another consumer's key. The bare
    # names are accepted as a fallback, matching how the ZeroEntropy key resolves.
    cohere_api_key: str = field(
        default_factory=lambda: os.environ.get("ACTALUX_COHERE_API_KEY", "")
    )
    voyage_api_key: str = field(
        default_factory=lambda: os.environ.get("ACTALUX_VOYAGE_API_KEY", "")
    )
    # Which hosted reranker production calls. See search/rerank.py PROVIDERS.
    rerank_provider: str = field(
        default_factory=lambda: os.environ.get("ACTALUX_RERANK_PROVIDER", "zeroentropy")
    )
    # "off" (RRF only, default) or "api" (rerank the RRF pool via ZeroEntropy).
    # Default off so the reranker is a deliberate, deploy-time opt-in (set
    # ACTALUX_RERANK=api in the web host's secrets) with no surprise cost/latency.
    rerank_mode: str = field(default_factory=lambda: os.environ.get("ACTALUX_RERANK", "off"))
    # RRF candidates reranked before truncating to search_max_results. Reranking
    # a deeper pool is what lets the cross-encoder lift a buried-but-relevant hit.
    rerank_pool_size: int = 50
    # Pinned (model_settings.toml [pinned]): every stored chunk vector came from this
    # model, so a different query-time model would search a space it does not share —
    # silently wrong results. No run-time override; changing it means re-embedding.
    embedding_model: str = field(default_factory=lambda: _model("pinned", "embedding"))
    # Jev (TypeSafe's decision model), reached through OpenRouter's Decisions
    # API on the Actalux OpenRouter key — no separate TypeSafe account. Pinned to a
    # versioned model rather than the jev-latest alias so an audit rerun stays
    # comparable to the last one until the pin is moved deliberately.
    decisions_url: str = "https://openrouter.ai/api/alpha/decisions"
    jev_model: str = field(
        default_factory=lambda: _model("llm", "citation_judge", "ACTALUX_JEV_MODEL")
    )
    embedding_dim: int = 384
    # Board-meeting transcription (Whisper). Audio is transcribed via Groq's
    # OpenAI-compatible API (free tier, whisper-large-v3 — better than whisper-1
    # and faster), keyed by GROQ_ACTALUX_API_KEY (namespaced separately from any
    # other Groq usage). transcribe.py also accepts these as plain args, so the
    # provider can be swapped (e.g. back to OpenAI) without code change.
    groq_api_key: str = field(
        default_factory=lambda: (
            os.environ.get("ACTALUX_GROQ") or os.environ.get("GROQ_ACTALUX_API_KEY", "")
        )
    )
    transcribe_model: str = field(
        default_factory=lambda: _model("speech", "transcribe_groq", "ACTALUX_TRANSCRIBE_MODEL")
    )
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
    # Citation-source pages (/chunk/{ref}/source[-pane]) are cheap single-row
    # lookups, but a crawler following the many citation links on budget/matter/
    # member pages can still flood them. 60/min is well above any human's clicking
    # while throttling a rogue crawler to a trickle (robots.txt disallows /chunk/).
    rate_limit_chunk_per_minute: int = 60
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
