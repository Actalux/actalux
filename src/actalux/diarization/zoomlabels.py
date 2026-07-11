"""Read the participant name Zoom printed on the active-speaker's video tile.

COVID-era meetings are Zoom recordings: the platform already attributed each
speaker and rendered their display name onto every tile, and it draws a
saturated yellow-green outline around whichever tile is speaking. Reading that
rendered name is *not* biometric processing — there is no face or voice template
of anyone — so it is policy-cheap in exactly the way face-ID is not, and it can
mint an anchor for a person who has no trusted text at all (recon + rationale:
docs/architecture/zoom-name-extraction.md).

This module is the pure, deterministic core of that pipeline: given a decoded
frame it finds the highlighted tile, OCRs the corner label, and fuzzy-matches it
against a caller-supplied roster alias map. It never touches the database, the
network, or ``speaker_identities`` — the CLI (scripts/probe_zoom_labels.py) owns
frame extraction and all I/O; keeping the vision + matching logic pure is what
makes it unit-testable against fixtures. Everything jurisdiction-specific (the
roster) arrives as an argument; the only literals here are Zoom-UI platform
constants, measured from the fixtures (see the band comment below).
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass

import numpy as np
import pytesseract
from PIL import Image, ImageOps
from rapidfuzz import fuzz

# Zoom draws the active-speaker highlight as a saturated yellow-green outline
# around the speaking participant's tile. Band measured from the gallery
# fixtures (tests/fixtures/zoom/{NcZxWmRoSE4_20,roUDWBmyHK0_50}.jpg): PIL-HSV
# hue 63-69 (mean 66), saturation p10 134, value p10 131 across both frames,
# while the in-person room-camera fixture (9qLqGr9XYb8_20.jpg) has zero pixels
# in this band. The bounds are widened slightly past the observed spread so the
# same outline reads across compression/resolution variation.
_HUE_MIN, _HUE_MAX = 60, 72  # PIL HSV hue channel, 0-255
_SATURATION_MIN = 110
_VALUE_MIN = 110

# A Zoom video tile is ~16:9 and, in gallery view, spans roughly a third of the
# frame width. These bound a plausible active tile so highlight-coloured pixels
# that are not an outline (a green plant, a UI accent, subtitle bleed) do not get
# read as a speaker tile.
_TILE_ASPECT_MIN, _TILE_ASPECT_MAX = 0.8, 3.0  # bbox width / height
_PERIMETER_BAND = 0.10  # "on the outline" == within 10% of a bbox edge
_MIN_OUTLINE_DENSITY = 0.15  # green px must total >= this * bbox perimeter length

# Zoom renders the name white-on-dark in the tile's bottom-left corner. This crop
# keeps the very bottom strip and left ~three-quarters: tight enough to exclude
# the bottom-centre live-caption overlay (which bleeds in past ~18% height) yet
# wide enough for a long "First Last (she/her)" label. Upscaling before OCR is
# what makes the small label legible to tesseract.
_LABEL_BOTTOM_FRAC = 0.15
_LABEL_LEFT_FRAC = 0.75
_LABEL_UPSCALE = 4
_TESSERACT_CONFIG = "--psm 7"  # treat the crop as a single text line

# Active-speaker FULL-FRAME view (no gallery, no green border): one participant
# fills the frame with the name printed white-on-dark in the bottom-left corner.
# That label is larger than a tile's and sits on a darker pill, so a bright-pixel
# threshold isolates the white text far more reliably than the autocontrast used
# for the small tile labels (measured on the plan-commission fixtures). Pixels
# brighter than the threshold become black-on-white for tesseract.
_FULLFRAME_BOTTOM_FRAC = 0.12
_FULLFRAME_LEFT_FRAC = 0.40
_FULLFRAME_BRIGHT_THRESHOLD = 180

# Gallery vs full-frame layout guard. A gallery is a grid of tiles separated by
# near-black seams; a full-frame view is one person edge to edge. A grid always
# has an interior dark seam in BOTH axes (at a 1/3, 1/2, or 2/3 grid line), while
# a single speaker has at most one (a dark background band), so requiring a seam on
# both axes rejects full-frame and room-camera frames. Measured on the fixtures:
# galleries peak >=0.73 dark on each axis; full-frame/room-cam frames <=0.38 on
# their weaker axis.
_SEAM_DARK_MAX = 50  # grayscale below this counts as a near-black seam pixel
_SEAM_GRID_FRACTIONS = (1 / 3, 1 / 2, 2 / 3)
_SEAM_BAND_FRAC = 0.02  # search within +/- 2% of the dimension around each grid line
_SEAM_CENTRAL_MARGIN = 0.15  # ignore the outer 15% (frame border, UI, label strip)
_GALLERY_SEAM_FRAC = 0.55  # min dark fraction on both axes to call a frame a gallery

# Default roster acceptance floor (rapidfuzz ratio, 0-100). Zoom display names
# are informal ("jasonwilson") and OCR is imperfect, so an exact match is too
# strict; below the floor the label is treated as a non-roster participant.
_MATCH_FLOOR = 85

_PARENTHETICAL = re.compile(r"\([^)]*\)")
_NON_ALPHA = re.compile(r"[^a-z ]+")
_WHITESPACE = re.compile(r"\s+")


@dataclass(frozen=True)
class ActiveTile:
    """Bounding box of the highlighted active-speaker tile, in frame pixels.

    ``green_score`` is the fraction of highlight-coloured pixels lying on the
    bounding box's perimeter band — near 1.0 for a clean rectangular outline,
    low for a filled blob — and is the confidence signal ``detect_active_tile``
    thresholds on.
    """

    left: int
    top: int
    right: int
    bottom: int
    green_score: float


@dataclass(frozen=True)
class FrameEvidence:
    """One sampled frame's full attribution receipt, for the JSON evidence file.

    ``mode`` records which reader produced the label: ``"tile"`` (a gallery active-
    speaker tile), ``"fullframe"`` (a full-frame active-speaker view), or ``"none"``
    (no readable label — no tile in a gallery frame, or a skipped read). ``matched_slug``
    is ``None`` when the label matched no roster member (a non-roster participant) or
    when there was no label to read; ``tile`` is set only in ``"tile"`` mode.
    """

    t_seconds: float
    frame_path: str
    tile: ActiveTile | None
    ocr_raw: str
    matched_slug: str | None
    match_score: int
    mode: str = "none"


def _green_mask(img: Image.Image) -> np.ndarray:
    """Boolean HxW mask of pixels in Zoom's active-speaker highlight band."""
    hsv = np.asarray(img.convert("HSV"), dtype=np.int16)
    hue, sat, val = hsv[..., 0], hsv[..., 1], hsv[..., 2]
    return (hue >= _HUE_MIN) & (hue <= _HUE_MAX) & (sat >= _SATURATION_MIN) & (val >= _VALUE_MIN)


def _perimeter_fraction(
    xs: np.ndarray, ys: np.ndarray, left: int, top: int, right: int, bottom: int
) -> float:
    """Fraction of the masked pixels lying within the perimeter band of the bbox."""
    band_x = max(1, int(_PERIMETER_BAND * (right - left)))
    band_y = max(1, int(_PERIMETER_BAND * (bottom - top)))
    on_edge = (
        (xs < left + band_x) | (xs > right - band_x) | (ys < top + band_y) | (ys > bottom - band_y)
    )
    return float(on_edge.mean())


def detect_active_tile(
    img: Image.Image,
    *,
    min_green_frac: float = 0.5,
    min_tile_frac: float = 1 / 6,
    max_tile_frac: float = 0.5,
) -> ActiveTile | None:
    """Locate Zoom's highlighted active-speaker tile, or ``None`` if there is none.

    The active tile is the one Zoom outlines in saturated yellow-green. This masks
    the highlight band, takes the mask's bounding box, and accepts it only when it
    is shaped like a tile outline: the box's width is a plausible fraction of the
    frame, its aspect ratio is tile-like, enough masked pixels are present to form
    an outline (not a few stray dots), and those pixels are concentrated on the
    box perimeter rather than filling it (a green background object). A room-camera
    frame with no tiles has no highlight pixels and returns ``None``.

    Parameters
    ----------
    img
        The decoded frame.
    min_green_frac
        Minimum ``green_score`` (perimeter concentration) to accept the box as an
        outline. A clean outline scores ~1.0; a filled blob ~0.36.
    min_tile_frac, max_tile_frac
        The box width must fall between these fractions of the frame width for the
        box to be a plausible gallery tile.

    Returns
    -------
    ActiveTile or None
        ``None`` is a first-class outcome meaning "no active-speaker highlight in
        this frame" (share mode, room camera, or a frame between highlights).
    """
    mask = _green_mask(img)
    ys, xs = np.nonzero(mask)
    if xs.size == 0:
        return None
    left, right = int(xs.min()), int(xs.max())
    top, bottom = int(ys.min()), int(ys.max())
    width, height = right - left, bottom - top
    if width <= 0 or height <= 0:
        return None

    frame_width = img.width
    if not (min_tile_frac * frame_width <= width <= max_tile_frac * frame_width):
        return None
    aspect = width / height
    if not (_TILE_ASPECT_MIN <= aspect <= _TILE_ASPECT_MAX):
        return None
    perimeter = 2 * (width + height)
    if xs.size < _MIN_OUTLINE_DENSITY * perimeter:
        return None

    green_score = _perimeter_fraction(xs, ys, left, top, right, bottom)
    if green_score < min_green_frac:
        return None
    return ActiveTile(left=left, top=top, right=right, bottom=bottom, green_score=green_score)


def _label_strip(img: Image.Image, tile: ActiveTile) -> Image.Image:
    """Crop the tile's bottom-left name strip (before upscale/threshold)."""
    height = tile.bottom - tile.top
    width = tile.right - tile.left
    strip_top = tile.bottom - int(_LABEL_BOTTOM_FRAC * height)
    strip_right = tile.left + int(_LABEL_LEFT_FRAC * width)
    return img.crop((tile.left, strip_top, strip_right, tile.bottom))


def _upscale_gray(strip: Image.Image) -> Image.Image:
    """Upscale a label crop and grayscale it — the shared OCR preprocessing front-end."""
    upscaled = strip.resize(
        (strip.width * _LABEL_UPSCALE, strip.height * _LABEL_UPSCALE), Image.Resampling.LANCZOS
    )
    return ImageOps.grayscale(upscaled)


def read_tile_label(img: Image.Image, tile: ActiveTile) -> str:
    """OCR the display-name label from a gallery tile's bottom-left corner.

    Returns the raw OCR string, stripped of surrounding whitespace. It is left
    un-normalized (curly punctuation, pronoun parentheticals, leading UI glyphs
    all intact) so the evidence receipt records exactly what tesseract saw;
    ``normalize_display_name`` cleans it for matching.
    """
    prepared = ImageOps.autocontrast(_upscale_gray(_label_strip(img, tile)))
    return pytesseract.image_to_string(prepared, config=_TESSERACT_CONFIG).strip()


def read_fullframe_label(
    img: Image.Image,
    *,
    bottom_frac: float = _FULLFRAME_BOTTOM_FRAC,
    left_frac: float = _FULLFRAME_LEFT_FRAC,
    bright_threshold: int = _FULLFRAME_BRIGHT_THRESHOLD,
) -> str:
    """OCR the display-name label from a full-frame active-speaker view.

    Zoom's active-speaker (non-gallery) view fills the frame with one participant and
    prints their name white-on-dark in the bottom-left corner. A bright-pixel threshold
    isolates that white text from the darker body/background above it — autocontrast, the
    tile reader's approach, is unreliable at this scale. Returns the raw OCR string,
    stripped. Guard with ``looks_like_gallery`` so this never runs on a gallery frame,
    whose bottom-left is a *tile's* label (the wrong speaker).
    """
    width, height = img.width, img.height
    strip = img.crop((0, height - int(bottom_frac * height), int(left_frac * width), height))
    gray = np.asarray(_upscale_gray(strip))
    binary = Image.fromarray(np.where(gray > bright_threshold, 0, 255).astype(np.uint8))
    return pytesseract.image_to_string(binary, config=_TESSERACT_CONFIG).strip()


def _peak_seam(profile: np.ndarray, span: int) -> float:
    """Strongest dark-seam fraction near any grid line (1/3, 1/2, 2/3) along one axis."""
    band = max(1, int(_SEAM_BAND_FRAC * span))
    peak = 0.0
    for fraction in _SEAM_GRID_FRACTIONS:
        center = int(fraction * span)
        window = profile[center - band : center + band + 1]
        if window.size:
            peak = max(peak, float(window.max()))
    return peak


def looks_like_gallery(img: Image.Image, *, seam_dark_frac: float = _GALLERY_SEAM_FRAC) -> bool:
    """True when the frame is a tiled gallery, so the full-frame reader must not run.

    A gallery's tiles are separated by near-black seams that cross the frame, so a dark
    seam appears near a grid line on BOTH axes; a full-frame single speaker (or a room
    camera) has at most one such band. Returns True only when the column and row dark
    profiles each have a seam at or above ``seam_dark_frac`` near a 1/3, 1/2, or 2/3
    grid line, measured over the central band (outer 15% ignored to skip the frame
    border and the label strip).
    """
    gray = np.asarray(ImageOps.grayscale(img))
    height, width = gray.shape
    dark = gray < _SEAM_DARK_MAX
    top, bottom = int(_SEAM_CENTRAL_MARGIN * height), int((1 - _SEAM_CENTRAL_MARGIN) * height)
    left, right = int(_SEAM_CENTRAL_MARGIN * width), int((1 - _SEAM_CENTRAL_MARGIN) * width)
    column_dark = dark[top:bottom, :].mean(axis=0)  # per-column dark fraction, central rows
    row_dark = dark[:, left:right].mean(axis=1)  # per-row dark fraction, central columns
    return (
        _peak_seam(column_dark, width) >= seam_dark_frac
        and _peak_seam(row_dark, height) >= seam_dark_frac
    )


def normalize_display_name(raw: str) -> str:
    """Fold an OCR'd display name to a lowercase, alpha-only, space-collapsed form.

    Drops pronoun parentheticals like ``(she/her)``, then everything that is not a
    letter or space, then collapses runs of whitespace. ``"Jason Wilson"`` becomes
    ``"jason wilson"`` and ``"JasonWilson"`` becomes ``"jasonwilson"``; squashing
    out the spaces (done by ``match_roster``) makes the two comparable.
    """
    lowered = raw.lower()
    without_parens = _PARENTHETICAL.sub(" ", lowered)
    alpha_only = _NON_ALPHA.sub(" ", without_parens)
    return _WHITESPACE.sub(" ", alpha_only).strip()


def match_roster(
    raw_label: str, aliases: dict[str, str], *, floor: int = _MATCH_FLOOR
) -> tuple[str, int] | None:
    """Fuzzy-match an OCR'd label to a roster member, or ``None`` below the floor.

    ``aliases`` maps an already-normalized alias to a canonical person slug; the
    caller builds it from the jurisdiction's roster rows (this library never
    touches the database). Both the spaced form and the space-free "squashed" form
    are scored with ``rapidfuzz.ratio`` — the squashed form is what recovers
    informal one-word display names ("jasonwilson") against a "First Last" roster
    alias — and the best score across all aliases and both forms wins.

    Returns ``(person_slug, score)`` for the best alias at or above ``floor``, or
    ``None``. ``None`` is a first-class outcome: a non-roster participant (a member
    of the public, a student) is expected and must not be forced onto a roster member.
    """
    label = normalize_display_name(raw_label)
    if not label:
        return None
    label_squashed = label.replace(" ", "")

    best_slug: str | None = None
    best_score = -1.0
    for alias, slug in aliases.items():
        alias_squashed = alias.replace(" ", "")
        score = max(
            fuzz.ratio(label, alias),
            fuzz.ratio(label_squashed, alias_squashed),
        )
        if score > best_score:
            best_slug, best_score = slug, score

    if best_slug is None or best_score < floor:
        return None
    return best_slug, int(round(best_score))


def cluster_verdict(
    frames: list[FrameEvidence], *, min_agree: int = 2
) -> tuple[str, list[str]] | None:
    """Majority vote over a cluster's matched frames, with a crosstalk guard.

    Zoom's active-speaker highlight can flick to whoever interjects, so a single
    frame is not trustworthy. Returns ``(winning_slug, supporting_frame_paths)``
    only when one slug is matched in at least ``min_agree`` frames AND no *other*
    slug also reaches ``min_agree`` (an unresolved conflict). Otherwise ``None``.
    """
    supporters: dict[str, list[str]] = {}
    for frame in frames:
        if frame.matched_slug is None:
            continue
        supporters.setdefault(frame.matched_slug, []).append(frame.frame_path)
    if not supporters:
        return None

    ranked = sorted(supporters.items(), key=lambda item: len(item[1]), reverse=True)
    winner_slug, winner_frames = ranked[0]
    if len(winner_frames) < min_agree:
        return None
    for _, paths in ranked[1:]:
        if len(paths) >= min_agree:
            return None  # a second slug also cleared the floor — unresolved crosstalk
    return winner_slug, winner_frames


def feed_label_slugs(
    cluster_verdict_map: Mapping[str, str | None], *, max_clusters_per_slug: int = 2
) -> set[str]:
    """Slugs that won too many of a document's clusters to be a real speaker — feed labels.

    In a full-frame room-camera recording the on-screen name is the *streaming account*
    (the Zoom feed owner), not whoever is speaking on the room's shared microphone, so
    every distinct speaker cluster reads the same name. A single voice can legitimately
    over-split into a couple of diarization clusters, but a slug that wins more than
    ``max_clusters_per_slug`` distinct clusters in one meeting is that feed/account label,
    not an attribution — the caller demotes those clusters rather than enrolling them.

    ``cluster_verdict_map`` is ``{cluster_label: winning_slug or None}`` for ONE document;
    ``None`` verdicts are ignored. Returns the set of offending slugs (empty when none).
    """
    counts = Counter(slug for slug in cluster_verdict_map.values() if slug is not None)
    return {slug for slug, count in counts.items() if count > max_clusters_per_slug}
