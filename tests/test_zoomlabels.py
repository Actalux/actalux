"""Tests for the Zoom active-speaker label reader (src/actalux/diarization/zoomlabels.py).

The real fixtures under tests/fixtures/zoom/ are the ground truth from the recon in
docs/architecture/zoom-name-extraction.md: two Zoom gallery frames with a known green-
outlined active tile, two plan-commission full-frame active-speaker frames (one person
edge to edge, name in the bottom-left), and one in-person room-camera frame with no
tiles. The OCR assertions run tesseract for real (no mocking) so a regression in the
crop/preprocess pipeline fails the test rather than passing on a stub.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from PIL import Image

from actalux.diarization.zoomlabels import (
    ActiveTile,
    FrameEvidence,
    cluster_verdict,
    detect_active_tile,
    feed_label_slugs,
    looks_like_gallery,
    match_roster,
    normalize_display_name,
    read_fullframe_label,
    read_tile_label,
)

FIXTURES = Path(__file__).parent / "fixtures" / "zoom"

# Roster alias maps a test builds by hand (the CLI builds the real one from the DB).
# Keys are already normalized, exactly as match_roster expects.
WILSON_ROSTER = {
    "jason wilson": "jason-wilson",
    "jasonwilson": "jason-wilson",
    "sean doherty": "sean-doherty",
}
SIWAK_ROSTER = {
    "stacy siwak": "stacy-siwak",
    "gary pierson": "gary-pierson",
    "amy rubin": "amy-rubin",
}
# Plan-commission full-frame fixtures.
PC_ROSTER = {
    "carolyn gaidis": "carolyn-gaidis",
    "helen difate": "helen-difate",
    "gary carter": "gary-carter",
}

GALLERY_FIXTURES = ("NcZxWmRoSE4_20.jpg", "roUDWBmyHK0_50.jpg")
NON_GALLERY_FIXTURES = ("aUBryPz7PZs_1130.jpg", "aUBryPz7PZs_1536.jpg", "9qLqGr9XYb8_20.jpg")


def _load(name: str) -> Image.Image:
    return Image.open(FIXTURES / name).convert("RGB")


# --- detect_active_tile on real frames --------------------------------------


def test_detect_active_tile_gallery_wilson():
    tile = detect_active_tile(_load("NcZxWmRoSE4_20.jpg"))
    assert tile is not None
    # The jasonwilson tile is the bottom-right one: right-of-centre, lower half.
    assert tile.left > 640 * 0.4
    assert tile.top > 360 * 0.5
    assert tile.green_score >= 0.5


def test_detect_active_tile_gallery_siwak():
    tile = detect_active_tile(_load("roUDWBmyHK0_50.jpg"))
    assert tile is not None
    # The Stacy Siwak tile is the middle-right one: right column, vertical middle.
    assert tile.left > 640 * 0.4
    assert tile.green_score >= 0.5


def test_detect_active_tile_room_camera_returns_none():
    # In-person room camera: no tiles, no highlight -> a first-class None.
    assert detect_active_tile(_load("9qLqGr9XYb8_20.jpg")) is None


# --- read + match end to end on real frames ---------------------------------


def test_read_and_match_wilson():
    img = _load("NcZxWmRoSE4_20.jpg")
    tile = detect_active_tile(img)
    assert tile is not None
    raw = read_tile_label(img, tile)
    match = match_roster(raw, WILSON_ROSTER)
    assert match is not None
    slug, score = match
    assert slug == "jason-wilson"
    assert score >= 85


def test_read_and_match_siwak():
    img = _load("roUDWBmyHK0_50.jpg")
    tile = detect_active_tile(img)
    assert tile is not None
    raw = read_tile_label(img, tile)
    match = match_roster(raw, SIWAK_ROSTER)
    assert match is not None
    slug, score = match
    assert slug == "stacy-siwak"
    assert score >= 85


# --- normalize_display_name -------------------------------------------------


def test_normalize_strips_pronoun_parenthetical():
    assert normalize_display_name("Stacy Siwak (she/her)") == "stacy siwak"


def test_normalize_squash_makes_oneword_comparable():
    # The informal one-word Zoom name and the roster spelling squash to the same string.
    assert normalize_display_name("JasonWilson").replace(" ", "") == "jasonwilson"
    assert normalize_display_name("Jason Wilson").replace(" ", "") == "jasonwilson"


def test_normalize_drops_leading_ui_glyphs_and_digits():
    assert normalize_display_name("| Aitana Rosas (202...)") == "aitana rosas"


def test_normalize_empty_on_all_punctuation():
    assert normalize_display_name("—  |  =") == ""


# --- match_roster floor behaviour -------------------------------------------


def test_match_roster_exact():
    assert match_roster("Jason Wilson", WILSON_ROSTER) == ("jason-wilson", 100)


def test_match_roster_below_floor_returns_none():
    # A non-roster participant must not be forced onto a member.
    assert match_roster("Kaitlyn Tran", WILSON_ROSTER) is None


def test_match_roster_custom_floor():
    # A near-miss OCR that clears a relaxed floor but not the default.
    assert match_roster("jasonwilsen", WILSON_ROSTER, floor=100) is None
    hit = match_roster("jasonwilsen", WILSON_ROSTER, floor=80)
    assert hit is not None and hit[0] == "jason-wilson"


def test_match_roster_empty_label_returns_none():
    assert match_roster("", WILSON_ROSTER) is None


# --- cluster_verdict --------------------------------------------------------


def _frame(slug: str | None, path: str) -> FrameEvidence:
    tile = ActiveTile(left=0, top=0, right=1, bottom=1, green_score=1.0)
    score = 95 if slug else 0
    return FrameEvidence(
        t_seconds=0.0,
        frame_path=path,
        tile=tile,
        ocr_raw=slug or "",
        matched_slug=slug,
        match_score=score,
    )


def test_cluster_verdict_majority():
    frames = [_frame("jason-wilson", f"f{i}.jpg") for i in range(3)]
    frames.append(_frame(None, "f3.jpg"))
    verdict = cluster_verdict(frames)
    assert verdict is not None
    slug, supporting = verdict
    assert slug == "jason-wilson"
    assert supporting == ["f0.jpg", "f1.jpg", "f2.jpg"]


def test_cluster_verdict_conflict_returns_none():
    # Two different slugs each reach the floor -> unresolved crosstalk.
    frames = [
        _frame("jason-wilson", "a.jpg"),
        _frame("jason-wilson", "b.jpg"),
        _frame("sean-doherty", "c.jpg"),
        _frame("sean-doherty", "d.jpg"),
    ]
    assert cluster_verdict(frames, min_agree=2) is None


def test_cluster_verdict_insufficient_agreement_returns_none():
    frames = [_frame("jason-wilson", "a.jpg"), _frame(None, "b.jpg")]
    assert cluster_verdict(frames, min_agree=2) is None


def test_cluster_verdict_no_matches_returns_none():
    frames = [_frame(None, "a.jpg"), _frame(None, "b.jpg")]
    assert cluster_verdict(frames) is None


def test_cluster_verdict_dominant_over_single_flick():
    # A lone crosstalk flick (1 frame) does not block a 3-frame winner at min_agree=2.
    frames = [
        _frame("jason-wilson", "a.jpg"),
        _frame("jason-wilson", "b.jpg"),
        _frame("jason-wilson", "c.jpg"),
        _frame("sean-doherty", "d.jpg"),
    ]
    verdict = cluster_verdict(frames, min_agree=2)
    assert verdict is not None
    assert verdict[0] == "jason-wilson"


@pytest.mark.parametrize("bad", ["", "   "])
def test_match_roster_whitespace_only(bad):
    assert match_roster(bad, WILSON_ROSTER) is None


# --- looks_like_gallery layout guard ----------------------------------------


@pytest.mark.parametrize("name", GALLERY_FIXTURES)
def test_looks_like_gallery_true_on_galleries(name):
    assert looks_like_gallery(_load(name)) is True


@pytest.mark.parametrize("name", NON_GALLERY_FIXTURES)
def test_looks_like_gallery_false_on_fullframe_and_roomcam(name):
    # Full-frame active-speaker views and the room camera have no crossing tile seams.
    assert looks_like_gallery(_load(name)) is False


# --- full-frame active-speaker label reading --------------------------------


@pytest.mark.parametrize(
    "name, expected_slug",
    [
        ("aUBryPz7PZs_1130.jpg", "carolyn-gaidis"),
        ("aUBryPz7PZs_1536.jpg", "helen-difate"),
    ],
)
def test_read_fullframe_label_matches_roster(name, expected_slug):
    img = _load(name)
    # These are full-frame views: no green tile, and the layout guard says not-gallery.
    assert detect_active_tile(img) is None
    assert looks_like_gallery(img) is False
    raw = read_fullframe_label(img)
    match = match_roster(raw, PC_ROSTER)
    assert match is not None
    slug, score = match
    assert slug == expected_slug
    assert score >= 85


def test_room_camera_fullframe_yields_no_match():
    # The fallback runs on the room camera (not a gallery), but its OCR must not
    # match a roster member — empty/garbage stays below the floor.
    img = _load("9qLqGr9XYb8_20.jpg")
    assert detect_active_tile(img) is None
    assert looks_like_gallery(img) is False
    raw = read_fullframe_label(img)
    assert match_roster(raw, PC_ROSTER) is None


# --- FrameEvidence mode field -----------------------------------------------


def test_frame_evidence_mode_defaults_to_none():
    evidence = FrameEvidence(0.0, "f.jpg", None, "", None, 0)
    assert evidence.mode == "none"


def test_frame_evidence_mode_explicit():
    tile = ActiveTile(left=0, top=0, right=1, bottom=1, green_score=1.0)
    evidence = FrameEvidence(1.0, "f.jpg", tile, "raw", "slug", 90, mode="tile")
    assert evidence.mode == "tile"


# --- feed_label_slugs (room-feed / streaming-account guard) ------------------


def test_feed_label_slugs_flags_dominant_slug():
    # doc 2359 shape: one account name won 9 of 13 clusters (a room camera).
    verdicts = {f"SPEAKER_{i:02d}": "ryan-helle" for i in range(9)}
    verdicts.update({"SPEAKER_09": "carolyn-gaidis", "SPEAKER_10": None})
    verdicts.update({"SPEAKER_11": "helen-difate", "SPEAKER_12": None})
    assert feed_label_slugs(verdicts) == {"ryan-helle"}


def test_feed_label_slugs_two_clusters_not_flagged_at_default():
    # A voice may legitimately over-split into two clusters — not a feed label.
    verdicts = {"SPEAKER_00": "helen-difate", "SPEAKER_01": "helen-difate"}
    assert feed_label_slugs(verdicts) == set()


def test_feed_label_slugs_knob_flags_two_clusters():
    verdicts = {"SPEAKER_00": "helen-difate", "SPEAKER_01": "helen-difate"}
    assert feed_label_slugs(verdicts, max_clusters_per_slug=1) == {"helen-difate"}


def test_feed_label_slugs_ignores_none_and_empty():
    assert feed_label_slugs({}) == set()
    assert feed_label_slugs({"SPEAKER_00": None, "SPEAKER_01": None}) == set()
