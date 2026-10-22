# Moving actalux.org behind Cloudflare

Status: step 1 (client-IP change) in review; steps 2–10 not started.

## Where things stand

| Piece | Today |
|---|---|
| Registrar | Hover (Tucows) |
| DNS | Hover nameservers (`ns1/ns2.hover.com`); DNSSEC off |
| Apex `actalux.org` | A 66.241.124.205, AAAA 2a09:8280:1::121:21de:0 (Fly) |
| `www` | CNAME `9383nwx.actalux.fly.dev` |
| TLS | Fly-issued certificates for both hostnames |
| Email | Google Workspace: MX `smtp.google.com`, SPF `include:_spf.google.com`, DMARC `p=none` |
| Other TXT | `google-site-verification=…` (Search Console) |
| API consumers | clayton-ledger calls `actalux.org/api/v1/...` from scripts and CI |

## The one code change that has to land first

Rate limiting keys on the client IP (`_client_ip` in `src/actalux/web/api.py`),
which reads `Fly-Client-IP`. Behind Cloudflare, that header holds the
address of the Cloudflare edge server, not the visitor. Every visitor routed
through the same Cloudflare location would then share one rate-limit bucket,
and ordinary readers would start getting 429 errors under modest traffic.

The fix is to read `CF-Connecting-IP` — but only when the request arrived from
a Cloudflare address. The origin stays reachable directly at `actalux.fly.dev`,
so a header trusted unconditionally would let anyone pick their own rate-limit
key. Implementation:

- Keep Cloudflare's published IPv4/IPv6 ranges in a small config file (no
  hardcoded list in code), refreshed by a scheduled job or by hand.
- If `Fly-Client-IP` falls inside those ranges, use `CF-Connecting-IP`;
  otherwise keep today's behavior.
- Tests for: direct request, request via Cloudflare, spoofed
  `CF-Connecting-IP` from a non-Cloudflare address.

This change is harmless before the move (no requests come from Cloudflare yet),
so it ships first, on its own PR.

## Steps

1. **Ship the client-IP change** (above). Deploy, confirm rate limiting still works.
2. **Create the Cloudflare zone** (Free plan) for `actalux.org`. Let it import
   records, then compare against the table above record by record. Set every
   record to **DNS only (grey cloud)** for now, so the move changes nameservers
   and nothing else.
3. **Lower TTLs at Hover** to 300 s a day ahead, so a rollback propagates fast.
4. **Switch nameservers at Hover** to the two Cloudflare assigns. The domain stays
   registered at Hover; only DNS moves. *(Operator action at Hover.)*
5. **Verify after propagation:** site loads on both hostnames, mail to
   `admin@actalux.org` arrives, Search Console still verified, `dig NS` returns
   Cloudflare.
6. **Keep Fly's certificates renewing.** With the proxy on, Fly can no longer
   complete its default certificate challenge, so add the `_acme-challenge`
   CNAME records that `fly certs show <host>` lists (DNS-only). Confirm with
   `fly certs check`.
7. **Set SSL/TLS mode to Full (strict).** Fly serves a valid certificate, so
   strict works; "Flexible" would create a redirect loop with `force_https`.
8. **Turn on the proxy (orange cloud) for `www` first**, verify, then the apex.
   Check `cf-ray` appears in response headers and the rate limiter sees real
   visitor IPs (log line or a test endpoint).
9. **Turn on the defense settings** (see "Defense configuration" below), with
   the API exempt from challenges so clayton-ledger's scripts and CI keep working.
10. **Leave caching at default.** The app sets no `Cache-Control` headers, so
    Cloudflare caches only static files (CSS, images, fonts). Do not add a
    "Cache Everything" rule: search results and pages update nightly and are
    per-query.

## Defense configuration (Free plan)

The goal is defense: absorbing floods and filtering hostile traffic before it
reaches the single Fly machine.

| Setting | Choice | Why |
|---|---|---|
| DDoS protection | On (automatic, unmetered) | The main reason for the move. |
| WAF Free Managed Ruleset | On | Blocks known exploit patterns (e.g. probes for WordPress/PHP paths). |
| Security Level | Medium | Challenges IPs with a bad reputation. |
| Rate-limiting rule (Free allows one) | On `/search` and `/ask`, per visitor IP | The expensive endpoints (embedding, rerank, LLM); the app's own limiter stays as a second layer. |
| Custom rule: skip challenges | `/api/v1/*`, `/healthz` | Machine clients cannot solve a challenge; the API already has its own keys and limits. |
| Bot Fight Mode | **Off** | On the Free plan it cannot be exempted per path, so it would challenge clayton-ledger's API calls. Revisit on a paid plan (Super Bot Fight Mode can skip paths). Confirm this limitation in the dashboard before deciding. |
| Always Use HTTPS | On | Matches Fly's `force_https`. |
| Under Attack Mode | Off; emergency switch | Challenges every browser visit; turn on only during an active attack. |

## Rollback

- Fast (seconds): set the proxied records back to **DNS only**. Traffic goes
  straight to Fly again; nothing else changes.
- Full: point Hover back at its own nameservers. Slower (up to the old NS TTL)
  and only needed if Cloudflare DNS itself is the problem.

## Optional, later

- Lock the origin so only Cloudflare can reach it (Fly has no built-in IP allow
  list; would need an app-level check or a shared-secret header set by a
  Cloudflare Transform Rule). Only worth it if direct-to-origin abuse appears.
- Move the registrar to Cloudflare (at-cost renewals). Independent of this plan.
- Raise DMARC from `p=none` once mail flow is confirmed — unrelated to the
  proxy, but the DNS records will be in front of us.

## Decisions

- Purpose: defense (2026-10-21). Caching stays at default.
- DNS moves fully to Cloudflare, Free plan; registration stays at Hover.
