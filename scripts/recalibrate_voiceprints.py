#!/usr/bin/env python3
"""Recalibrate a jurisdiction's voiceprints -> a trustworthy CANDIDATE verdict (plan §5).

Purity + label-gated re-enrollment, negatives, and nested leave-one-meeting-out, per place:

  1. embed each official + negative cluster's turns once on Modal (per-turn, not concat);
  2. pool each cluster into one voiceprint (Gate B, contamination-trimmed);
  3. nested LOMO: for each held-out meeting, enablement (Gate A) + matcher params are chosen
     from OTHER meetings, then the held-out positives AND negatives are scored unfiltered —
     so the operating point is not overfit and non-official rejection is measured;
  4. report the honest nested estimate + the full-data refit operating point;
  5. --apply persists the CANDIDATE gallery (calibration_id-stamped, replace-per-meeting) and
     a voiceprint_calibration row (status 'candidate', or 'not_cleared' if nothing clears the
     bar). NEGATIVES ARE NEVER STORED; the report carries aggregate counts only.

A human promotes candidate -> cleared after reviewing the report; only then does
enroll_voiceprints.py / a future matcher trust the gallery. Nothing here publishes a name.

Usage:
    # dry-run (no GPU/writes): officials + negatives + meeting counts
    doppler run --project mac --config dev -- \\
      uv run python scripts/recalibrate_voiceprints.py --state mo --place clayton

    # apply (GPU embed + nested LOMO + persist candidate)
    doppler run --project actalux --config dev -- \\
      uv run --group diarization python scripts/recalibrate_voiceprints.py \\
      --state mo --place clayton --apply --proxy socks5h://127.0.0.1:40000
"""

from __future__ import annotations

import argparse
import html
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any

from supabase import Client

from actalux.config import load_config
from actalux.db import fetch_all_rows, get_client, get_diarization_turns, get_place_by_path
from actalux.diarization.enrollment import (
    EnrollableCluster,
    cluster_spans,
    pool_cluster,
    select_enrollable,
    span_seconds,
    superseded_doc_ids,
    voiceprint_row,
)
from actalux.diarization.families import family_of
from actalux.diarization.matching import (
    CURVE_PRECISION_BARS,
    DEFAULT_CORE_FLOORS,
    GATE_A_COLLAPSE_BOUND,
    GATE_A_MIN_CORE,
    Sample,
    enablement_delta,
    evaluate_grid,
    gate_officials,
    nested_lomo_multi_bar,
    pareto_frontier,
    select_operating_point,
)
from actalux.errors import ActaluxError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

AUDIO_DIR = Path("data/audio")
# The generated audit sheets are a review artifact (cued YouTube watch links + the metric block),
# never a publish surface; they live in a gitignored dir by default so a run leaves nothing tracked.
AUDIT_DIR = Path("data/audit_sheets")
# A cued watch link opens YouTube ~this many seconds before the reviewer should stop — long enough
# to identify a voice by ear, short enough to keep the sheet skimmable (matches the blind-ID sheet).
AUDIT_CUE_SECONDS = 10
# At most this many evidence pointers per enabled official on the sheet (distinct meetings first) —
# enough to corroborate by ear without turning the sheet into a full transcript dump.
AUDIT_MAX_EVIDENCE = 3
WARP_DOWNLOAD_RETRIES = 6
# A verdict computed on a partial corpus is not comparable to the baseline and must never
# overwrite the gallery: run 28684759549 measured only 46/74 meetings (YouTube bot-checks)
# and looked like a recall collapse. Abort before persisting instead.
MAX_DOWNLOAD_FAILURE_FRACTION = 0.10
# Gate B pooling is FIXED (rationale, plan §5): trimmed-mean robustness + require >=2 turns;
# purity_floor 0 so pooling only trims — within-cluster purity rejection is delegated to
# Gate A cross-meeting coherence, the stronger label-aware signal.
POOL_PARAMS = {"trim_fraction": 0.25, "min_coherent_turns": 2, "purity_floor": 0.0}
NEG_PER_MEETING = 3  # cap negatives per meeting (GPU cost bound); longest clusters first
NEG_MIN_SECONDS = 10.0  # a negative needs enough speech to be a fair distractor


def _parse_embedders(raw: str, *, primary: str) -> list[str]:
    """Parse ``--embedders`` into an ordered, de-duplicated model list; primary is always first.

    Blank -> ``[primary]`` (the default single-embedder run, behaviourally unchanged). Otherwise
    the first entry MUST be ``primary``: only the primary model's gallery is persisted, and that
    column is 256-d wespeaker, so a non-primary head would try to store an off-dimension vector.
    Enforcing it structurally keeps alt embedders strictly append-only measurement (Option B).
    """
    ids = [m.strip() for m in raw.split(",") if m.strip()]
    if not ids:
        return [primary]
    if ids[0] != primary:
        raise ActaluxError(
            f"--embedders first entry must be the primary model {primary!r} (only its 256-d "
            f"gallery is persisted); got {ids[0]!r}"
        )
    ordered: list[str] = []
    for m in ids:  # preserve order, drop duplicates
        if m not in ordered:
            ordered.append(m)
    return ordered


def _place_documents(client: Client, place_id: int, body: str | None) -> dict[int, dict[str, Any]]:
    """Documents for a place's entities (optionally one body) -> ``{doc_id: doc}``."""
    entities = fetch_all_rows(
        lambda: client.table("entities").select("id,body_slug").eq("place_id", place_id)
    )
    if body:
        entities = [e for e in entities if e.get("body_slug") == body]
    if not entities:
        raise ActaluxError(f"no entities for place {place_id} (body={body!r})")
    entity_ids = [e["id"] for e in entities]
    docs = fetch_all_rows(
        lambda: (
            client.table("documents")
            .select("id,video_id,replaces_id,entity_id")
            .in_("entity_id", entity_ids)
        )
    )
    return {d["id"]: d for d in docs}


def negative_labels(
    turns: list[dict[str, Any]], official_labels: set[str], *, min_seconds: float, cap: int
) -> list[str]:
    """Non-official cluster labels with enough speech, longest first, capped (the distractors)."""
    secs: dict[str, float] = defaultdict(float)
    for t in turns:
        secs[t["cluster_label"]] += max(0.0, float(t["end_seconds"]) - float(t["start_seconds"]))
    candidates = [
        (lab, s) for lab, s in secs.items() if lab not in official_labels and s >= min_seconds
    ]
    candidates.sort(key=lambda x: -x[1])
    return [lab for lab, _ in candidates[:cap]]


def build_meeting_samples(
    turns_by_model: dict[str, dict[str, list[tuple[tuple[float, ...], float]]]],
    clusters: list[EnrollableCluster],
    neg_labels: list[str],
    video_id: str,
    *,
    min_seconds: float,
    primary_model: str,
) -> tuple[dict[str, list[Sample]], list[tuple[EnrollableCluster, Any]]]:
    """Pool one meeting's per-model cluster turns into per-model calibration Samples.

    Officials pool to labeled Samples for every embedder; negatives pool to ``person_id=None``
    Samples (scored, never persisted). Pooling is identical across models, so a downstream recall
    difference is attributable to the embedder alone. The second return — ``pooled_officials`` — is
    the PRIMARY model's officials only; it is the sole persistable artifact, so an alternate
    embedder can never produce a gallery row (Option B). For a single-model run this yields the same
    Samples, in the same order, that the pre-A/B path built inline.
    """
    per_model: dict[str, list[Sample]] = {m: [] for m in turns_by_model}
    pooled_officials: list[tuple[EnrollableCluster, Any]] = []
    for model_id, turns_by_label in turns_by_model.items():
        for ec in clusters:
            pooled = pool_cluster(turns_by_label.get(ec.cluster_label, []), **POOL_PARAMS)
            if pooled is None or pooled.seconds < min_seconds:
                continue
            per_model[model_id].append(
                Sample(
                    ec.person_id,
                    video_id,
                    pooled.vector,
                    purity=pooled.purity,
                    confidence=ec.confidence,
                    basis=ec.source_basis,  # carries the evidence family into the consensus gate
                )
            )
            if model_id == primary_model:
                pooled_officials.append((ec, pooled))
        for lab in neg_labels:  # negatives: scored, NEVER persisted
            pooled = pool_cluster(turns_by_label.get(lab, []), **POOL_PARAMS)
            if pooled is not None and pooled.seconds >= min_seconds:
                per_model[model_id].append(
                    Sample(None, video_id, pooled.vector, purity=pooled.purity)
                )
    return per_model, pooled_officials


def _confusion_report(metrics: Any) -> dict[str, Any]:
    """Aggregate-only slice of the metrics for the calibration row (no negative identifiers).

    Keeps official-level precision + official->official confusion pairs (public people) and
    the negative false-positive COUNT, but never any negative's cluster/doc/timestamp.
    """
    fp = sum(1 for true, _ in metrics.confusions if true is None)
    official_confusions = [[t, p] for t, p in metrics.confusions if t is not None]
    return {
        "macro_precision": round(metrics.macro_precision, 4),
        "recall": round(metrics.recall, 4),
        "predictions": metrics.predictions,
        "fp_negatives": fp,
        "per_official_precision": {
            str(p): round(v, 4) for p, v in metrics.per_person_precision.items()
        },
        "official_confusions": official_confusions[:50],
    }


def _curve_point(precision_bar: float, metrics: Any, provenance: dict[str, Any]) -> dict[str, Any]:
    """One honest nested-LOMO point for the precision↔recall curve, false positives split by type.

    citizen_fp (a negative matched to an official) and official_confusion_count (one official
    matched as another) are reported apart so the operator's split-bar policy can be decided on
    data. Aggregate counts only — never a negative's identifier.
    """
    citizen_fp = sum(1 for true, _ in metrics.confusions if true is None)
    official = sum(1 for true, _ in metrics.confusions if true is not None)
    return {
        "precision_bar": precision_bar,
        "macro_precision": round(metrics.macro_precision, 4),
        "recall": round(metrics.recall, 4),
        "citizen_fp": citizen_fp,
        "official_confusion_count": official,
        "abstained_folds": provenance["abstained_folds"],
    }


def _ab_report(
    samples_by_model: dict[str, list[Sample]],
    *,
    primary_model: str,
    precision_bar: float,
    curve_bars: tuple[float, ...],
) -> dict[str, dict[str, Any]]:
    """Per-ALTERNATE-embedder nested LOMO + curve (measurement only), keyed by model id.

    The primary model's verdict and gallery are computed and persisted by ``_finish``; this reruns
    the identical honest harness on each alternate embedder so recall can be compared at equal
    precision. Only aggregate metrics are returned — no alternate vector is ever written anywhere
    (Option B). Empty (``{}``) when there is no alternate, so a default single-model run adds no
    ``ab`` block and its persisted report stays byte-identical to the pre-A/B path.
    """
    ab: dict[str, dict[str, Any]] = {}
    for model_id, samples in samples_by_model.items():
        if model_id == primary_model:
            continue
        if not any(s.person_id is not None for s in samples):
            ab[model_id] = {"n_enabled": 0, "note": "no official voiceprints pooled"}
            continue
        multi = nested_lomo_multi_bar(samples, precision_bars=curve_bars)
        nested, prov = multi[precision_bar]
        refit = select_operating_point(samples, precision_bar=precision_bar)
        entry = _confusion_report(nested)
        entry["n_enabled"] = len(refit.enabled) if refit else 0
        entry["recall_by_confidence"] = prov["recall_by_confidence"]
        entry["curve"] = {
            "nested_by_bar": [_curve_point(b, *multi[b]) for b in CURVE_PRECISION_BARS],
            "pareto": pareto_frontier(evaluate_grid(samples)),
        }
        ab[model_id] = entry
    return ab


def _gate_decisions_for_report(samples: list[Sample], refit: Any) -> dict[int, Any]:
    """Per-official Gate-A decisions reproducing the refit's enabled set (audit substrate).

    At the refit operating point when one exists (same purity filter + core/collapse/asnorm knobs),
    so the audit's enable paths match exactly who was persisted. When the run is not_cleared (no
    refit), a diagnostic pass at the most permissive swept core floor explains WHY nobody enabled
    (single-family / too-few-meetings), not a bare "not enabled".
    """
    if refit is not None:
        filtered = [s for s in samples if s.purity >= refit.purity_floor]
        return gate_officials(
            filtered,
            core_floor=refit.core_floor,
            min_core=GATE_A_MIN_CORE,
            collapse_bound=refit.collapse_bound,
            score_norm=refit.score_norm,
            z_floor=refit.z_floor,
        )
    return gate_officials(
        samples,
        core_floor=min(DEFAULT_CORE_FLOORS),
        min_core=GATE_A_MIN_CORE,
        collapse_bound=GATE_A_COLLAPSE_BOUND,
    )


def _top_evidence(
    cues: list[dict[str, Any]], allowed_families: set[str] | None = None
) -> list[dict[str, Any]]:
    """Up to AUDIT_MAX_EVIDENCE cued clips for an official, preferring distinct meetings.

    ``allowed_families`` (the families that landed on the official's coherent voice) restricts the
    clips to that evidence — so the sheet never cues an anchor the coherence gate DISCARDED (a
    Hummell-style scattered discourse clip pointing at the wrong voice). ``None`` (a confirmed-
    waiver official, with no computed core) keeps all cues: a human label is trusted regardless.
    """
    picked: list[dict[str, Any]] = []
    seen: set[int] = set()
    for c in cues:
        if allowed_families is not None:
            if family_of(c["basis"], c["confidence"]) not in allowed_families:
                continue  # the coherence gate discarded this family's anchors; don't cue them
        if c["document_id"] in seen:
            continue
        seen.add(c["document_id"])
        picked.append(
            {
                "document_id": c["document_id"],
                "video_id": c["video_id"],
                "cluster_label": c["cluster_label"],
                "basis": c["basis"],
                "start_seconds": c["start_seconds"],
                "end_seconds": c["end_seconds"],
            }
        )
        if len(picked) >= AUDIT_MAX_EVIDENCE:
            break
    return picked


def _build_audit(
    decisions: dict[int, Any],
    name_by_person: dict[int, str],
    evidence_cues: dict[int, list[dict[str, Any]]],
    enabled: set[int],
) -> tuple[dict[str, Any], dict[int, str]]:
    """Machine-readable per-official audit block + a ``{person_id: reason}`` map for the delta.

    Each block carries the families present, the family-agreement summary (which families landed on
    the coherent voice, in how many meetings, which anchors were discarded), the enable path
    (confirmed_waiver | consensus | not_enabled+reason), and — for enabled officials — the top
    evidence pointers (document_id, cluster_label, a start_seconds cue). No citizen identifier.
    """
    audit: dict[str, Any] = {}
    reasons: dict[int, str] = {}
    for person_id, d in sorted(decisions.items()):
        reasons[person_id] = d.reason
        entry: dict[str, Any] = {
            "name": name_by_person.get(person_id),
            # the RUN's actual enablement (empty on a not_cleared verdict), NOT the bare Gate-A
            # pass: a not_cleared run persists nobody even if officials clear Gate A, so this must
            # agree with enabled_person_ids. path/reason still describe the Gate-A decision (why).
            "enabled": person_id in enabled,
            "enable_path": d.path,
            "reason": d.reason,
            "families": d.families,
            "family_agreement": {
                "agreeing_families": sorted(d.core_families),
                "in_core": d.core_families,
                "core_meetings": d.core_meetings,
                "discarded": d.discarded_by_family,
            },
            "confirmed_meetings": d.confirmed_meetings,
        }
        if person_id in enabled:
            # restrict evidence to the families on the coherent voice (consensus); a confirmed
            # waiver has no computed core (core_families empty -> None -> no family filter).
            allowed = set(d.core_families) or None
            entry["evidence"] = _top_evidence(evidence_cues.get(person_id, []), allowed)
        audit[str(person_id)] = entry
    return audit, reasons


def _previous_calibration(
    client: Client, place_id: int, entity_id: int | None
) -> dict[str, Any] | None:
    """The most recent prior voiceprint_calibration row for this place+entity (for the delta).

    Read BEFORE this run's row is inserted, so it is the true predecessor. Scoped to the same
    ``entity_id`` (a place-wide run compares to the previous place-wide run, a per-body run to that
    body's) so a delta never mixes bodies. ``id`` is serial/monotonic, so max-id is most recent.
    """
    rows = fetch_all_rows(
        lambda: (
            client.table("voiceprint_calibration")
            .select("id,entity_id,status,report")
            .eq("place_id", place_id)
        )
    )
    same = [r for r in rows if r.get("entity_id") == entity_id]
    return max(same, key=lambda r: r.get("id") or 0) if same else None


def _watch_url(video_id: str, start: float) -> str:
    """A YouTube watch link cued to the evidence clip's start second.

    Embeds are unusable for these sources: the source channels disable embedding (YouTube
    error 153), so an iframe renders only an error screen. A cued watch link opens YouTube at
    the clip's exact second — the same reliable path the blind-ID sheet uses.
    """
    s = max(0, int(start))
    return f"https://www.youtube.com/watch?v={video_id}&t={s}s"


def _hms(start: float) -> str:
    """``start`` seconds as a compact ``m:ss`` / ``h:mm:ss`` stamp for link text."""
    s = max(0, int(start))
    m, sec = divmod(s, 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"


def _fam_summary(counts: dict[str, Any]) -> str:
    """'adjacency×2, vote×2' from a family->count map (audit sheet text)."""
    return ", ".join(f"{html.escape(str(k))}×{v}" for k, v in sorted(counts.items())) or "—"


def render_audit_sheet(
    *, title: str, calibration_id: int | None, status: str, report: dict[str, Any]
) -> str:
    """Static, self-contained HTML review sheet (pure — returns the HTML string).

    A metric block (trusted-tier recall headline + nested macro-precision/recall + citizen FP), one
    row per ENABLED official with cued YouTube watch links (open at the clip second, no audio
    download) and its enable path, plus the run-over-run enablement delta (who gained / lost).
    Not a publish surface — no citizen data, only public officials the body already names.
    """
    audit = report.get("audit", {})
    delta = report.get("delta", {})
    tr = report.get("trusted_recall") or {}
    esc = html.escape
    tr_txt = f"{tr['recall']:.3f}" if tr.get("recall") is not None else "n/a"
    parts: list[str] = [
        "<!doctype html><html lang='en'><head><meta charset='utf-8'>",
        "<meta name='viewport' content='width=device-width, initial-scale=1'>",
        f"<title>{esc(title)}</title>",
        "<style>"
        "body{font-family:system-ui,-apple-system,sans-serif;margin:0;color:#1a1a1a;background:#faf8f4}"
        ".wrap{max-width:900px;margin:0 auto;padding:24px}"
        "h1{font-size:20px;margin:0 0 4px}.sub{color:#666;font-size:13px;margin-bottom:20px}"
        ".metrics{display:flex;gap:24px;flex-wrap:wrap;border-top:2px solid #1a1a1a;"
        "border-bottom:1px solid #ddd;padding:12px 0;margin-bottom:24px}"
        ".metric b{display:block;font-size:22px}.metric span{font-size:12px;color:#666}"
        ".accent{color:#C8553D}"
        ".official{border-left:3px solid #C8553D;background:#fff;"
        "padding:14px 16px;margin-bottom:16px}"
        ".official h2{font-size:16px;margin:0 0 2px}"
        ".mono{font-family:'IBM Plex Mono',ui-monospace,monospace;font-size:12px;color:#555}"
        ".clips{display:flex;gap:12px;flex-wrap:wrap;margin-top:10px}"
        ".clip{margin:0;display:flex;flex-direction:column;gap:4px}"
        ".clip .play{display:block;padding:10px 12px;background:#1a1a1a;color:#fff;"
        "text-decoration:none;font-size:13px}.clip .play:hover{background:#333}"
        ".clip figcaption{font-size:11px;color:#666;"
        "font-family:'IBM Plex Mono',ui-monospace,monospace}"
        ".delta{margin-top:8px}.gain{color:#2c7a3f}.loss{color:#C8553D}"
        "table{border-collapse:collapse;font-size:13px}td{padding:2px 10px 2px 0}"
        "</style></head><body><div class='wrap'>",
        f"<h1>{esc(title)}</h1>",
        f"<div class='sub'>calibration id {calibration_id} · status "
        f"<b class='accent'>{esc(status)}</b> · review artifact, not a publish surface</div>",
        "<div class='metrics'>",
        f"<div class='metric'><b class='accent'>{tr_txt}</b>"
        f"<span>trusted-tier recall "
        f"({tr.get('recalled', 0)}/{tr.get('positives', 0)})</span></div>",
        f"<div class='metric'><b>{report.get('macro_precision', 0):.3f}</b>"
        "<span>nested macro-precision</span></div>",
        f"<div class='metric'><b>{report.get('recall', 0):.3f}</b>"
        "<span>nested recall (raw)</span></div>",
        f"<div class='metric'><b>{report.get('fp_negatives', 0)}</b>"
        "<span>citizen false positives</span></div>",
        "</div>",
    ]

    enabled_entries = [(pid, e) for pid, e in audit.items() if e.get("enabled")]
    parts.append(f"<h2 style='font-size:15px'>Enabled officials ({len(enabled_entries)})</h2>")
    if not enabled_entries:
        parts.append("<p class='mono'>No officials enabled this run.</p>")
    for _pid, e in enabled_entries:
        name = esc(str(e.get("name") or _pid))
        fa = e.get("family_agreement", {})
        parts.append("<div class='official'>")
        parts.append(f"<h2>{name}</h2>")
        parts.append(
            f"<div class='mono'>path: {esc(e.get('enable_path', ''))} · "
            f"{esc(e.get('reason', ''))}</div>"
        )
        parts.append(
            f"<div class='mono'>families: {_fam_summary(e.get('families', {}))} · "
            f"on coherent voice: {_fam_summary(fa.get('in_core', {}))} "
            f"across {fa.get('core_meetings', 0)} meetings · "
            f"discarded: {_fam_summary(fa.get('discarded', {}))}</div>"
        )
        clips = e.get("evidence", [])
        if clips:
            parts.append("<div class='clips'>")
            for c in clips:
                url = _watch_url(str(c["video_id"]), c["start_seconds"])
                cap = (
                    f"doc {c['document_id']} · {esc(str(c['cluster_label']))} · "
                    f"@{c['start_seconds']}s ({esc(str(c['basis']))})"
                )
                parts.append(
                    f"<figure class='clip'>"
                    f"<a class='play' href='{esc(url)}' target='_blank' rel='noopener'>"
                    f"&#9654; Play @ {_hms(c['start_seconds'])} (~{AUDIT_CUE_SECONDS}s)</a>"
                    f"<figcaption>{cap}</figcaption></figure>"
                )
            parts.append("</div>")
        parts.append("</div>")

    gained, lost = delta.get("gained", []), delta.get("lost", [])
    parts.append("<h2 style='font-size:15px'>Enablement delta vs previous calibration</h2>")
    prev_id = delta.get("previous_calibration_id")
    parts.append(
        f"<div class='mono'>previous calibration id: {prev_id if prev_id else 'none'}</div>"
    )
    parts.append("<table class='delta'>")
    for who in gained:
        nm = esc(str(who.get("name") or who["person_id"]))
        rs = esc(str(who.get("reason", "")))
        parts.append(
            f"<tr><td class='gain'>+ gained</td><td>{nm}</td><td class='mono'>{rs}</td></tr>"
        )
    for who in lost:
        nm = esc(str(who.get("name") or who["person_id"]))
        rs = esc(str(who.get("reason", "")))
        parts.append(
            f"<tr><td class='loss'>− lost (demoted)</td><td>{nm}</td>"
            f"<td class='mono'>{rs}</td></tr>"
        )
    if not gained and not lost:
        parts.append("<tr><td class='mono' colspan='3'>No change in enablement.</td></tr>")
    parts.append("</table></div></body></html>")
    return "".join(parts)


def _write_audit_sheet(
    args: argparse.Namespace,
    place_id: int,
    calibration_id: int,
    status: str,
    report: dict[str, Any],
) -> None:
    """Render + write the audit sheet to the (gitignored) audit dir. Jurisdiction-agnostic."""
    state = getattr(args, "state", None)
    place = getattr(args, "place", None)
    body = getattr(args, "body", None)
    out_dir = Path(getattr(args, "audit_dir", None) or AUDIT_DIR)
    scope = "/".join(x for x in (state, place, body) if x) or f"place-{place_id}"
    title = f"Voiceprint audit — {scope}"
    html_str = render_audit_sheet(
        title=title, calibration_id=calibration_id, status=status, report=report
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    slug = "_".join(x for x in (state, place, body) if x) or f"place{place_id}"
    path = out_dir / f"{slug}_cal{calibration_id}.html"
    path.write_text(html_str, encoding="utf-8")
    logger.info(
        "wrote audit sheet %s (%d enabled official(s))",
        path,
        sum(1 for e in report["audit"].values() if e.get("enabled")),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Recalibrate voiceprints for a place (candidate).")
    parser.add_argument("--state", required=True, help="place state slug, e.g. mo")
    parser.add_argument("--place", required=True, help="place slug, e.g. clayton")
    parser.add_argument("--body", help="restrict to one body_slug; default all bodies (place-wide)")
    parser.add_argument("--apply", action="store_true", help="GPU embed + persist candidate")
    parser.add_argument("--precision-bar", type=float, default=0.98, help="macro-precision floor")
    parser.add_argument("--confirmed-only", action="store_true", help="exclude name-anchored high")
    parser.add_argument("--min-seconds", type=float, default=10.0, help="min pooled speech seconds")
    parser.add_argument(
        "--neg-per-meeting", type=int, default=NEG_PER_MEETING, help="negatives/mtg"
    )
    parser.add_argument("--limit", type=int, help="cap the number of meetings processed")
    parser.add_argument("--proxy", help="SOCKS proxy for yt-dlp audio download (WARP in CI)")
    parser.add_argument("--keep-audio", action="store_true", help="don't delete downloaded audio")
    parser.add_argument(
        "--audit-dir",
        default=str(AUDIT_DIR),
        help="dir for the generated per-run audit sheet (gitignored review artifact)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="re-stage even if the place already has a cleared calibration (downgrades it)",
    )
    parser.add_argument(
        "--embedders",
        default="",
        help="comma-separated embedder model ids; first (primary) is persisted, any others are "
        "measured only (report.ab). Blank = the primary wespeaker model alone (unchanged).",
    )
    args = parser.parse_args()

    client = _service_client()
    place = get_place_by_path(client, args.state, args.place)
    if not place:
        raise ActaluxError(f"no place {args.state}/{args.place}")
    place_id = place["id"]
    entity_id = _body_entity_id(client, place_id, args.body)

    docs_by_id = _place_documents(client, place_id, args.body)
    superseded = superseded_doc_ids(list(docs_by_id.values()))
    doc_ids = sorted(docs_by_id)

    identities = fetch_all_rows(
        lambda: (
            client.table("speaker_identities")
            .select("id,document_id,cluster_label,subject_id,confidence,basis")
            .in_("document_id", doc_ids)
        )
    )
    subjects_by_id = {
        s["id"]: s
        for s in fetch_all_rows(
            lambda: (
                client.table("subjects")
                .select("id,person_id,publishable,canonical_name")
                .eq("place_id", place_id)
            )  # place-scoped: a stale cross-place subject_id can't enroll
        )
    }
    enrollable = select_enrollable(identities, subjects_by_id, confirmed_only=args.confirmed_only)

    by_doc: dict[int, list[EnrollableCluster]] = defaultdict(list)
    for ec in enrollable:
        doc = docs_by_id.get(ec.document_id, {})
        if ec.document_id not in superseded and doc.get("video_id"):
            by_doc[ec.document_id].append(ec)
    docs_to_process = sorted(by_doc)
    if args.limit:
        docs_to_process = docs_to_process[: args.limit]

    logger.info(
        "recalibrating %s/%s%s: %d officials / %d meetings (precision bar %.2f)",
        args.state,
        args.place,
        f"/{args.body}" if args.body else "",
        len({ec.person_id for ec in enrollable}),
        len(docs_to_process),
        args.precision_bar,
    )

    if not args.apply:
        _dry_run(client, by_doc, docs_to_process, args)
        return

    # Don't silently downgrade a cleared production gallery: the candidate upsert would restamp
    # cleared rows with this run's candidate calibration_id. Re-staging must be deliberate.
    if not args.force:
        cleared = fetch_all_rows(
            lambda: (
                client.table("voiceprint_calibration")
                .select("id,entity_id")
                .eq("place_id", place_id)
                .eq("status", "cleared")
            )
        )
        relevant = [
            c for c in cleared if entity_id is None or c.get("entity_id") in (entity_id, None)
        ]
        if relevant:
            raise ActaluxError(
                f"{args.state}/{args.place} already has a cleared calibration; re-running would "
                f"downgrade the cleared gallery to candidate. Pass --force to re-stage."
            )

    _apply(client, place_id, entity_id, docs_by_id, by_doc, docs_to_process, superseded, args)


def _service_client() -> Client:
    """A service-key Supabase client (the voiceprint tables are service-only)."""
    cfg = load_config()
    import os

    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise ActaluxError(
            "ACTALUX_SUPABASE_SERVICE_KEY is required (voiceprint tables service-only)"
        )
    return get_client(cfg.supabase_url, key)


def _body_entity_id(client: Client, place_id: int, body: str | None) -> int | None:
    """The entity id for a body_slug (for the calibration row), or None for place-wide."""
    if not body:
        return None
    rows = fetch_all_rows(
        lambda: client.table("entities").select("id,body_slug").eq("place_id", place_id)
    )
    match = [e for e in rows if e.get("body_slug") == body]
    return match[0]["id"] if match else None


def _dry_run(
    client: Client,
    by_doc: dict[int, list[EnrollableCluster]],
    docs_to_process: list[int],
    args: argparse.Namespace,
) -> None:
    """Report officials + negatives + per-person counts without GPU/writes."""
    per_person: dict[str, int] = defaultdict(int)
    negatives = 0
    for doc_id in docs_to_process:
        turns = get_diarization_turns(client, doc_id)
        official_labels = {ec.cluster_label for ec in by_doc[doc_id]}
        for ec in by_doc[doc_id]:
            if span_seconds(cluster_spans(turns, ec.cluster_label)) >= args.min_seconds:
                per_person[ec.canonical_name] += 1
        negatives += len(
            negative_labels(
                turns, official_labels, min_seconds=NEG_MIN_SECONDS, cap=args.neg_per_meeting
            )
        )
    for name, n in sorted(per_person.items(), key=lambda kv: -kv[1]):
        logger.info("  %-28s %d sample(s)", name, n)
    logger.info(
        "DRY RUN — would embed ~%d official + ~%d negative cluster(s) across %d meeting(s)",
        sum(per_person.values()),
        negatives,
        len(docs_to_process),
    )
    logger.info("re-run with --apply (needs `--group diarization` + a deployed Modal app)")


def _apply(
    client: Client,
    place_id: int,
    entity_id: int | None,
    docs_by_id: dict[int, dict[str, Any]],
    by_doc: dict[int, list[EnrollableCluster]],
    docs_to_process: list[int],
    superseded: set[int],
    args: argparse.Namespace,
) -> None:
    """Embed per-turn on Modal, pool, run nested LOMO, report, and persist the candidate."""
    import sys

    from actalux.diarization.modal_runner import EMBED_MODEL, ModalRunner
    from actalux.ingest.youtube import download_audio

    # Same-dir import, like backfill_whisperx.py: rotate the WARP egress between download
    # retries — without it every retry re-hits the same bot-flagged exit IP.
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from transcribe_meetings import reconnect_warp

    models = _parse_embedders(args.embedders, primary=EMBED_MODEL)
    logger.info("embedders: %s (first persisted; any others measured only)", ", ".join(models))
    runner = ModalRunner()
    retries = WARP_DOWNLOAD_RETRIES if args.proxy else 1
    samples_by_model: dict[str, list[Sample]] = {m: [] for m in models}
    pooled_officials: list[tuple[EnrollableCluster, Any]] = []  # (cluster, Pooled), primary only
    evidence_cues: dict[int, list[dict[str, Any]]] = defaultdict(list)  # person_id -> cued clips
    processed_docs: set[int] = set()

    for doc_id in docs_to_process:
        clusters = by_doc[doc_id]
        video_id = docs_by_id[doc_id]["video_id"]
        turns = get_diarization_turns(client, doc_id)
        official_labels = {ec.cluster_label for ec in clusters}
        neg_labels = negative_labels(
            turns, official_labels, min_seconds=NEG_MIN_SECONDS, cap=args.neg_per_meeting
        )
        payload = [
            {"cluster_label": lab, "spans": cluster_spans(turns, lab)}
            for lab in official_labels | set(neg_labels)
        ]
        try:
            audio = download_audio(
                video_id,
                AUDIO_DIR,
                proxy=args.proxy,
                retries=retries,
                on_retry=reconnect_warp if args.proxy else None,
            )
        except Exception:  # noqa: BLE001 - one meeting's download failure must not abort the batch
            logger.exception("audio download failed for doc %d (%s); skipping", doc_id, video_id)
            continue
        try:
            # one download -> one Modal call embeds the same spans with every model
            turns_by_model = runner.embed_cluster_turns_multi(str(audio), payload, models)
        finally:
            if not args.keep_audio:
                audio.unlink(missing_ok=True)
        # embedded -> this meeting's stale gallery rows will be refreshed or cleared, even if
        # zero clusters survive pooling (so a now-empty poisoned meeting doesn't keep old rows).
        processed_docs.add(doc_id)
        # One cued clip per official cluster this meeting: the LONGEST span (clearest voice to ID by
        # ear), for the audit sheet's YouTube embeds + the machine-readable evidence pointers. No
        # negative/citizen cluster is ever cued.
        for ec in clusters:
            spans = cluster_spans(turns, ec.cluster_label)
            if not spans:
                continue
            start, end = max(spans, key=lambda s: s[1] - s[0])
            evidence_cues[ec.person_id].append(
                {
                    "person_id": ec.person_id,
                    "name": ec.canonical_name,
                    "document_id": doc_id,
                    "video_id": video_id,
                    "cluster_label": ec.cluster_label,
                    "basis": ec.source_basis,
                    "confidence": ec.confidence,  # for the evidence-family filter (audit sheet)
                    "start_seconds": round(start, 1),
                    "end_seconds": round(end, 1),
                }
            )

        meeting_samples, meeting_pooled = build_meeting_samples(
            turns_by_model,
            clusters,
            neg_labels,
            video_id,
            min_seconds=args.min_seconds,
            primary_model=models[0],
        )
        for model_id, model_samples in meeting_samples.items():
            samples_by_model[model_id].extend(model_samples)
        pooled_officials.extend(meeting_pooled)

    failed = len(docs_to_process) - len(processed_docs)
    if docs_to_process and failed / len(docs_to_process) > MAX_DOWNLOAD_FAILURE_FRACTION:
        raise ActaluxError(
            f"{failed}/{len(docs_to_process)} meetings failed to download "
            f"(> {MAX_DOWNLOAD_FAILURE_FRACTION:.0%}); verdict on a partial corpus is not "
            f"comparable to the baseline — aborting before persisting anything."
        )

    _finish(
        client,
        place_id,
        entity_id,
        models,
        samples_by_model,
        pooled_officials,
        processed_docs,
        superseded,
        args,
        evidence_cues=evidence_cues,
    )


def _finish(
    client: Client,
    place_id: int,
    entity_id: int | None,
    models: list[str],
    samples_by_model: dict[str, list[Sample]],
    pooled_officials: list[tuple[EnrollableCluster, Any]],
    processed_docs: set[int],
    superseded: set[int],
    args: argparse.Namespace,
    evidence_cues: dict[int, list[dict[str, Any]]] | None = None,
) -> None:
    """Nested-LOMO estimate + full-data refit, then persist the candidate gallery + record.

    Only the primary model (``models[0]``) drives the verdict and gallery — its samples and pooled
    officials are exactly what the pre-A/B path produced. Alternate embedders are measured off the
    same meetings and folded into ``report['ab']`` only; none of their vectors are persisted.
    """
    from actalux.diarization.modal_runner import EMBED_MODEL

    primary = models[0]
    samples = samples_by_model[primary]
    n_pos = sum(1 for s in samples if s.person_id is not None)
    n_neg = sum(1 for s in samples if s.person_id is None)
    if n_pos == 0:
        logger.warning("no official voiceprints pooled; nothing to calibrate")
        return

    # One nested pass over the union of the run's bar and the reporting curve's bars, reusing each
    # fold's grid across bars. The verdict below reads nested@(args.precision_bar) — identical to a
    # standalone nested_leave_one_meeting_out at that bar — so the persisted decision is unchanged.
    curve_bars = tuple(sorted({args.precision_bar, *CURVE_PRECISION_BARS}))
    multi = nested_lomo_multi_bar(samples, precision_bars=curve_bars)
    nested, prov = multi[args.precision_bar]
    refit = select_operating_point(samples, precision_bar=args.precision_bar)

    # CANDIDATE only if the honest (nested, no-circular) estimate clears the bar; the refit is
    # the full-data operating point that would be deployed. Otherwise not_cleared.
    cleared = refit is not None and nested.macro_precision >= args.precision_bar
    status = "candidate" if cleared else "not_cleared"
    # A not_cleared verdict enrolls NOTHING (enabled empty -> no rows upserted -> stale rows for
    # processed meetings are deleted, leaving no untrustworthy gallery behind).
    enabled = refit.enabled if cleared else set()
    purity_floor = refit.purity_floor if cleared else 0.0

    report = _confusion_report(nested)
    report["provenance"] = prov
    report["enabled_person_ids"] = sorted(enabled)
    # Honest recall split by the held-out sample's confidence tier: confirmed (human-verified)
    # positives are the trustworthy read; inferred held-out positives may themselves be mislabeled,
    # so mixing them would inflate or deflate the estimate. Lifted to the top level for prominence
    # (it already rides inside `prov`). Reporting only — the verdict/selection below is untouched.
    report["recall_by_confidence"] = prov["recall_by_confidence"]
    # HEADLINE recall the operator reads: over TRUSTED positives only (human-confirmed OR multi-
    # family-consensus in-fold). The id=5 discourse flood made raw recall meaningless — dominated by
    # unverified single-family positives the gate refused to enable; this excludes them.
    report["trusted_recall"] = prov["trusted_recall"]
    if refit is not None:
        report["refit"] = {
            "purity_floor": refit.purity_floor,
            "core_floor": refit.core_floor,
            "threshold": refit.threshold,
            "margin": refit.margin,
            "aggregation": refit.aggregation,
            "score_norm": refit.score_norm,
            "collapse_bound": refit.collapse_bound,
            "z_floor": refit.z_floor,
            "n_enabled": len(refit.enabled),
            "in_sample_macroP": round(refit.metrics.macro_precision, 4),
            "in_sample_recall": round(refit.metrics.recall, 4),
        }
    # Reporting-only precision↔recall curve: honest nested metrics at each bar (citizen vs official
    # FPs split) + the full-data Pareto frontier. Neither drives the persisted verdict/gallery.
    report["curve"] = {
        "nested_by_bar": [_curve_point(b, *multi[b]) for b in CURVE_PRECISION_BARS],
        "pareto": pareto_frontier(evaluate_grid(samples)),
    }
    # Dual-embedder A/B: same honest harness on each alternate embedder, measurement only. Absent
    # (no key) for a default single-model run, so its persisted report stays byte-identical.
    ab = _ab_report(
        samples_by_model,
        primary_model=primary,
        precision_bar=args.precision_bar,
        curve_bars=curve_bars,
    )
    if ab:
        report["ab"] = ab
        for model_id, entry in ab.items():
            logger.info(
                "  A/B [%s]: macroP=%.3f recall=%.3f (%d enabled)",
                model_id,
                entry.get("macro_precision", 0.0),
                entry.get("recall", 0.0),
                entry.get("n_enabled", 0),
            )

    # Per-official audit block (families, agreement, enable path, discarded anchors, evidence cues)
    # + run-over-run enablement delta vs the previous calibration for this entity. The delta reads
    # the previous row BEFORE this run's row is inserted (so it is the true predecessor); demotion
    # (loss of enablement) is automatic — the block just makes it visible. Reporting only.
    name_by_person = {ec.person_id: ec.canonical_name for ec, _ in pooled_officials}
    # A not_cleared run enables nobody; drive the audit's diagnostic pass (why nobody enabled)
    # not the refit's would-be enable set, so the audit never disagrees with the persisted verdict.
    decisions = _gate_decisions_for_report(samples, refit if cleared else None)
    audit, reasons = _build_audit(decisions, name_by_person, evidence_cues or {}, enabled)
    report["audit"] = audit
    prev = _previous_calibration(client, place_id, entity_id)
    prev_enabled = (
        set(prev["report"].get("enabled_person_ids", []))
        if prev and isinstance(prev.get("report"), dict)
        else set()
    )
    delta = enablement_delta(prev_enabled, enabled, current_reasons=reasons, names=name_by_person)
    delta["previous_calibration_id"] = prev["id"] if prev else None
    report["delta"] = delta

    logger.info(
        "nested LOMO: macroP=%.3f recall=%.3f fp_neg=%d (%d pos/%d neg, %d abstained folds)",
        nested.macro_precision,
        nested.recall,
        report["fp_negatives"],
        n_pos,
        n_neg,
        prov["abstained_folds"],
    )
    tr = report["trusted_recall"]
    logger.info(
        "TRUSTED-tier recall: %s (%d/%d positives)",
        f"{tr['recall']:.3f}" if tr["recall"] is not None else "n/a (no trusted positives)",
        tr["recalled"],
        tr["positives"],
    )
    logger.info("verdict: %s (%d officials enabled)", status, len(enabled))
    for who in delta["gained"]:
        logger.info(
            "  + gained enablement: %s (%s)", who["name"] or who["person_id"], who["reason"]
        )
    for who in delta["lost"]:
        logger.info("  - lost enablement:  %s (%s)", who["name"] or who["person_id"], who["reason"])
    for tier, stat in report["recall_by_confidence"].items():
        if stat["positives"]:
            logger.info(
                "  held-out recall [%s]: %.3f (%d/%d)",
                tier,
                stat["recall"],
                stat["recalled"],
                stat["positives"],
            )
    if refit is not None:
        logger.info(
            "refit: purity>=%.2f core>=%.2f threshold=%.2f margin=%.2f agg=%s",
            refit.purity_floor,
            refit.core_floor,
            refit.threshold,
            refit.margin,
            refit.aggregation,
        )

    row = {
        "place_id": place_id,
        "entity_id": entity_id,
        "precision_bar": args.precision_bar,
        "threshold": refit.threshold if refit else None,
        "margin": refit.margin if refit else None,
        "aggregation": refit.aggregation if refit else None,
        "trim_fraction": POOL_PARAMS["trim_fraction"],
        "min_coherent_turns": POOL_PARAMS["min_coherent_turns"],
        "purity_floor": refit.purity_floor if refit else None,
        "macro_precision": round(nested.macro_precision, 4),
        "recall": round(nested.recall, 4),
        "fp_count": report["fp_negatives"],
        "n_officials": len({s.person_id for s in samples if s.person_id is not None}),
        "n_enabled_officials": len(enabled),
        "n_negatives": n_neg,
        "gallery_size": n_pos,
        "model": EMBED_MODEL,
        "status": status,
        "report": report,
    }
    inserted = client.table("voiceprint_calibration").insert(row).execute()
    calibration_id = inserted.data[0]["id"]
    logger.info("wrote voiceprint_calibration id=%s status=%s", calibration_id, status)

    # Persist ONLY enabled officials whose pooled purity clears the refit floor (a not_cleared
    # verdict enrolls nothing). Upsert first, THEN delete stale rows for every processed meeting
    # -> no empty-gallery window, and no poison / non-enabled / now-rejected row survives.
    rows = [
        voiceprint_row(ec, pooled, EMBED_MODEL, calibration_id=calibration_id)
        for ec, pooled in pooled_officials
        if ec.person_id in enabled and pooled.purity >= purity_floor
    ]
    if rows:
        client.table("subject_voiceprints").upsert(
            rows, on_conflict="person_id,source_document_id,cluster_label"
        ).execute()
    stale = _delete_stale(client, processed_docs, calibration_id)
    pruned = _prune_superseded(client, superseded)
    logger.info(
        "enrolled %d candidate voiceprint(s); removed %d stale + %d superseded",
        len(rows),
        stale,
        pruned,
    )

    # Generate the static audit sheet (cued YouTube embeds + metric block + delta). Only on a real
    # run (evidence_cues threaded from _apply); the unit tests call _finish without cues, so no file
    # is written. Never a publish surface — a gitignored review artifact.
    if evidence_cues is not None:
        _write_audit_sheet(args, place_id, calibration_id, status, report)


def _delete_stale(client: Client, processed_docs: set[int], calibration_id: int) -> int:
    """Delete gallery rows for processed meetings not refreshed by this run (poison / rejected)."""
    if not processed_docs:
        return 0
    rows = fetch_all_rows(
        lambda: (
            client.table("subject_voiceprints")
            .select("id,calibration_id")
            .in_("source_document_id", sorted(processed_docs))
        )
    )
    stale = [r["id"] for r in rows if r.get("calibration_id") != calibration_id]
    if stale:
        client.table("subject_voiceprints").delete().in_("id", stale).execute()
    return len(stale)


def _prune_superseded(client: Client, superseded: set[int]) -> int:
    """Delete gallery samples whose source document has been superseded."""
    if not superseded:
        return 0
    rows = fetch_all_rows(
        lambda: (
            client.table("subject_voiceprints")
            .select("id")
            .in_("source_document_id", sorted(superseded))
        )
    )
    ids = [r["id"] for r in rows]
    if ids:
        client.table("subject_voiceprints").delete().in_("id", ids).execute()
    return len(ids)


if __name__ == "__main__":
    main()
