"""Tests for the LLM discourse labeler (identity/discourse.py).

The LLM itself is faked (patching ``actalux.identity.discourse.OpenAI``, the summarize-test
idiom); these exercise the containment logic that makes a model's output safe — hard claim
validation, corroboration aggregation, windowing/overlap, and best-effort degradation.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import patch

from actalux.identity.discourse import (
    SIGNAL_CHAIR,
    SIGNAL_GRATITUDE,
    SIGNAL_QUESTION,
    SIGNAL_REFERENCE,
    SIGNAL_ROLE,
    SIGNAL_SELF,
    DiscourseClaim,
    _aggregate,
    _completion_kwargs,
    _parse_claims,
    _person_accepted,
    _render_turns,
    _validate_claim,
    _windows,
    label_discourse,
)
from actalux.identity.resolve import ResolverTurn, RosterMember


def _member(subject_id: int, slug: str, name: str, title: str = "") -> RosterMember:
    return RosterMember(subject_id, slug, name, frozenset(), title)


def _turn(cluster: str, text: str) -> ResolverTurn:
    return ResolverTurn(cluster, text)


ROSTER = [
    _member(1, "jane-harris", "Jane Harris", "Mayor"),
    _member(2, "bob-stevens", "Bob Stevens", "City Manager"),
    _member(3, "carol-diaz", "Carol Diaz"),
]
ROSTER_SLUGS = frozenset(m.slug for m in ROSTER)


# --- claim validation: the three hard gates that make LLM output inert ----------------


class TestValidateClaim:
    turns = [
        _turn("SPEAKER_00", "I recognize the city manager to present the budget."),
        _turn("SPEAKER_01", "Thank you, Mayor. As city manager I recommend approval."),
    ]
    valid_clusters = frozenset({"SPEAKER_00", "SPEAKER_01"})

    def _claim(self, **over) -> dict:
        base = {
            "cluster_label": "SPEAKER_01",
            "person_slug": "bob-stevens",
            "signal": SIGNAL_ROLE,
            "polarity": "self",
            "quote": "As city manager I recommend",
            "turn_idx": 1,
            "confidence": "high",
            "rationale": "role claim",
        }
        base.update(over)
        return base

    def test_valid_claim_passes(self) -> None:
        out = _validate_claim(self._claim(), self.turns, self.valid_clusters, ROSTER_SLUGS)
        assert out is not None and out.person_slug == "bob-stevens" and out.turn_idx == 1

    def test_non_roster_slug_dropped(self) -> None:
        # The model can never mint a name: a slug outside the closed roster enum is dropped.
        c = self._claim(person_slug="mystery-person")
        assert _validate_claim(c, self.turns, self.valid_clusters, ROSTER_SLUGS) is None

    def test_quote_not_substring_dropped(self) -> None:
        # A paraphrased / hallucinated quote not verbatim in the cited turn is dropped.
        c = self._claim(quote="I hereby appoint myself treasurer")
        assert _validate_claim(c, self.turns, self.valid_clusters, ROSTER_SLUGS) is None

    def test_quote_substring_case_and_space_insensitive(self) -> None:
        c = self._claim(quote="as   CITY manager i recommend")
        assert _validate_claim(c, self.turns, self.valid_clusters, ROSTER_SLUGS) is not None

    def test_unknown_cluster_dropped(self) -> None:
        c = self._claim(cluster_label="SPEAKER_99")
        assert _validate_claim(c, self.turns, self.valid_clusters, ROSTER_SLUGS) is None

    def test_turn_idx_out_of_range_dropped(self) -> None:
        c = self._claim(turn_idx=7)
        assert _validate_claim(c, self.turns, self.valid_clusters, ROSTER_SLUGS) is None

    def test_bad_signal_dropped(self) -> None:
        c = self._claim(signal="Z")
        assert _validate_claim(c, self.turns, self.valid_clusters, ROSTER_SLUGS) is None

    def test_short_quote_dropped(self) -> None:
        c = self._claim(quote="I", turn_idx=0)
        assert _validate_claim(c, self.turns, self.valid_clusters, ROSTER_SLUGS) is None

    def test_non_int_turn_idx_dropped(self) -> None:
        c = self._claim(turn_idx="one")
        assert _validate_claim(c, self.turns, self.valid_clusters, ROSTER_SLUGS) is None


# --- per-signal directional grounding (the cue must point at the attributed cluster) --------


class TestDirectional:
    """A claim's quote must be grounded AND its cue must point at the attributed cluster.

    This is what stops crafted speech from attaching a real roster name to an arbitrary
    far-away cluster: even a real slug + real cluster + real substring quote is dropped when
    the signal's geometry (next / previous / self) doesn't place the cluster at the cue.
    """

    turns = [
        _turn("SPEAKER_00", "I recognize the city manager to present."),
        _turn("SPEAKER_01", "As city manager I recommend approval of the budget."),
        _turn("SPEAKER_01", "It balances."),
        _turn("SPEAKER_01", "Thank you."),
        _turn("SPEAKER_05", "Thank you, city manager, for that report."),
    ]
    vc = frozenset({"SPEAKER_00", "SPEAKER_01", "SPEAKER_05"})

    def _mk(self, **over) -> dict:
        base = {
            "cluster_label": "SPEAKER_01",
            "person_slug": "bob-stevens",
            "signal": SIGNAL_CHAIR,
            "polarity": "next",
            "quote": "I recognize the city manager",
            "turn_idx": 0,
            "confidence": "high",
            "rationale": "x",
        }
        base.update(over)
        return base

    def test_chair_next_cluster_within_window_ok(self) -> None:
        # cue at turn 0, SPEAKER_01 speaks at turn 1 (within the adjacency window).
        assert _validate_claim(self._mk(), self.turns, self.vc, ROSTER_SLUGS) is not None

    def test_chair_far_cluster_dropped(self) -> None:
        # THE injection shape: real cue quote, but the attributed cluster speaks far away
        # (SPEAKER_05 only at turn 4, cue at turn 0 -> outside the window) -> dropped.
        c = self._mk(cluster_label="SPEAKER_05")
        assert _validate_claim(c, self.turns, self.vc, ROSTER_SLUGS) is None

    def test_question_next_cluster_ok(self) -> None:
        c = self._mk(signal=SIGNAL_QUESTION)
        assert _validate_claim(c, self.turns, self.vc, ROSTER_SLUGS) is not None

    def test_gratitude_previous_cluster_ok(self) -> None:
        # cue "Thank you, city manager" at turn 4 -> the PREVIOUS speaker SPEAKER_01 (turns 1-3).
        c = self._mk(signal=SIGNAL_GRATITUDE, quote="Thank you, city manager", turn_idx=4)
        assert _validate_claim(c, self.turns, self.vc, ROSTER_SLUGS) is not None

    def test_gratitude_non_previous_cluster_dropped(self) -> None:
        # SPEAKER_00 (turn 0) is not within the window BEFORE the turn-4 cue -> dropped.
        c = self._mk(
            cluster_label="SPEAKER_00",
            person_slug="jane-harris",
            signal=SIGNAL_GRATITUDE,
            quote="Thank you, city manager",
            turn_idx=4,
        )
        assert _validate_claim(c, self.turns, self.vc, ROSTER_SLUGS) is None

    def test_self_signal_requires_own_turn(self) -> None:
        ok = self._mk(signal=SIGNAL_ROLE, quote="As city manager I recommend", turn_idx=1)
        assert _validate_claim(ok, self.turns, self.vc, ROSTER_SLUGS) is not None
        # same claim but citing SPEAKER_00's turn -> not the speaker's own turn -> dropped.
        bad = self._mk(signal=SIGNAL_SELF, quote="I recognize the city manager", turn_idx=0)
        assert _validate_claim(bad, self.turns, self.vc, ROSTER_SLUGS) is None

    def test_reference_signal_is_unconstrained(self) -> None:
        # F is corroborative-only: it carries no direction, so position is not checked (it can
        # never satisfy the acceptance bar alone, so leaving it unconstrained is safe).
        c = self._mk(cluster_label="SPEAKER_05", signal=SIGNAL_REFERENCE)
        assert _validate_claim(c, self.turns, self.vc, ROSTER_SLUGS) is not None


# --- aggregation: corroboration + contested-silence -----------------------------------


def _c(cluster: str, slug: str, signal: str, turn_idx: int) -> DiscourseClaim:
    return DiscourseClaim(cluster, slug, signal, "", "q", turn_idx, "", "")


class TestPersonAccepted:
    def test_single_chair_recognition_accepts(self) -> None:
        assert _person_accepted([_c("SPEAKER_01", "bob-stevens", SIGNAL_CHAIR, 0)])

    def test_two_independent_claims_accept(self) -> None:
        claims = [
            _c("SPEAKER_01", "bob-stevens", SIGNAL_ROLE, 1),
            _c("SPEAKER_01", "bob-stevens", SIGNAL_GRATITUDE, 3),
        ]
        assert _person_accepted(claims)

    def test_single_non_chair_claim_rejected(self) -> None:
        assert not _person_accepted([_c("SPEAKER_01", "bob-stevens", SIGNAL_ROLE, 1)])

    def test_duplicate_signal_turn_not_independent(self) -> None:
        # Two reads of the same (signal, turn) count once -> still below the 2-claim bar.
        claims = [
            _c("SPEAKER_01", "bob-stevens", SIGNAL_ROLE, 1),
            _c("SPEAKER_01", "bob-stevens", SIGNAL_ROLE, 1),
        ]
        assert not _person_accepted(claims)

    def test_same_signal_distinct_turns_independent(self) -> None:
        claims = [
            _c("SPEAKER_01", "bob-stevens", SIGNAL_REFERENCE, 1),
            _c("SPEAKER_01", "bob-stevens", SIGNAL_REFERENCE, 5),
        ]
        assert _person_accepted(claims)


class TestAggregate:
    members_by_slug = {m.slug: m for m in ROSTER}

    def test_single_accepted_person_yields_proposal(self) -> None:
        claims = [_c("SPEAKER_01", "bob-stevens", SIGNAL_CHAIR, 0)]
        out = _aggregate(claims, self.members_by_slug)
        assert len(out) == 1
        assert out[0].cluster_label == "SPEAKER_01"
        assert out[0].slug == "bob-stevens"
        assert out[0].subject_id == 2
        assert out[0].basis == "discourse"
        assert out[0].confidence == "inferred_medium"

    def test_contested_cluster_emits_nothing(self) -> None:
        # Two different people each clear the bar for the same cluster -> silence.
        claims = [
            _c("SPEAKER_01", "bob-stevens", SIGNAL_CHAIR, 0),
            _c("SPEAKER_01", "jane-harris", SIGNAL_CHAIR, 4),
        ]
        assert _aggregate(claims, self.members_by_slug) == []

    def test_below_bar_person_does_not_veto_accepted_one(self) -> None:
        # One person clears the bar (chair), another has a lone weak claim (below bar) ->
        # not contested; the accepted label still emits.
        claims = [
            _c("SPEAKER_01", "bob-stevens", SIGNAL_CHAIR, 0),
            _c("SPEAKER_01", "jane-harris", SIGNAL_REFERENCE, 2),
        ]
        out = _aggregate(claims, self.members_by_slug)
        assert [(p.cluster_label, p.slug) for p in out] == [("SPEAKER_01", "bob-stevens")]

    def test_no_claims_no_proposals(self) -> None:
        assert _aggregate([], self.members_by_slug) == []


# --- windowing + rendering ------------------------------------------------------------


class TestWindows:
    def test_single_window_when_short(self) -> None:
        assert _windows(150) == [(0, 150)]

    def test_overlapping_windows_when_long(self) -> None:
        spans = _windows(400)
        assert spans[0] == (0, 150)
        # each later window steps back by the overlap so a boundary exchange stays whole
        assert spans[1][0] == 150 - 10
        assert spans[-1][1] == 400
        # windows cover every turn (no gap)
        covered: set[int] = set()
        for s, e in spans:
            covered.update(range(s, e))
        assert covered == set(range(400))

    def test_render_uses_global_indices_and_caps_text(self) -> None:
        turns = [_turn("SPEAKER_00", "a"), _turn("SPEAKER_01", "b" * 1000)]
        rendered = _render_turns(turns, 1, 2)
        assert rendered.startswith("[1][SPEAKER_01] ")
        # the long turn is capped so one turn can't blow the token budget
        assert len(rendered) < 700


# --- JSON parsing ---------------------------------------------------------------------


class TestParseClaims:
    def test_object_form(self) -> None:
        raw = '{"claims": [{"cluster_label": "SPEAKER_00", "person_slug": "x"}]}'
        assert _parse_claims(raw) == [{"cluster_label": "SPEAKER_00", "person_slug": "x"}]

    def test_bare_array_form(self) -> None:
        assert _parse_claims('[{"a": 1}]') == [{"a": 1}]

    def test_code_fenced(self) -> None:
        assert _parse_claims('```json\n{"claims": [{"a": 1}]}\n```') == [{"a": 1}]

    def test_malformed_returns_empty(self) -> None:
        assert _parse_claims("not json") == []
        assert _parse_claims("") == []

    def test_non_object_items_dropped(self) -> None:
        assert _parse_claims('{"claims": [{"a": 1}, "junk", 5]}') == [{"a": 1}]


# --- provider kwargs (family normalization, mirrors summarize) -------------------------


class TestCompletionKwargs:
    def test_reasoning_model_uses_completion_tokens_no_temperature(self) -> None:
        k = _completion_kwargs("openai/gpt-5-mini", [])
        assert "max_completion_tokens" in k and "reasoning_effort" in k
        assert "temperature" not in k and "max_tokens" not in k

    def test_plain_model_uses_max_tokens_and_temperature_zero(self) -> None:
        k = _completion_kwargs("openai/gpt-4o-mini", [])
        assert k["max_tokens"] > 0 and k["temperature"] == 0
        assert "max_completion_tokens" not in k


# --- end-to-end with a faked provider -------------------------------------------------


def _fake_response(content: str, prompt_tokens: int = 100, completion_tokens: int = 20):
    return SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=content))],
        usage=SimpleNamespace(prompt_tokens=prompt_tokens, completion_tokens=completion_tokens),
    )


def _run_label(turns, members, content, **kw):
    with patch("actalux.identity.discourse.OpenAI") as mock_openai:
        mock_openai.return_value.chat.completions.create.return_value = _fake_response(content)
        return label_discourse(turns, members, "fake-key", model="openai/gpt-5-mini", **kw)


class TestLabelDiscourseEndToEnd:
    turns = [
        _turn("SPEAKER_00", "I recognize the city manager to present the budget."),
        _turn("SPEAKER_01", "Thank you, Mayor. As city manager I recommend approval."),
    ]

    def test_chair_recognition_labels_next_cluster(self) -> None:
        content = json.dumps(
            {
                "claims": [
                    {
                        "cluster_label": "SPEAKER_01",
                        "person_slug": "bob-stevens",
                        "signal": SIGNAL_CHAIR,
                        "polarity": "next",
                        "quote": "I recognize the city manager",
                        "turn_idx": 0,
                        "confidence": "high",
                        "rationale": "chair grants floor",
                    }
                ]
            }
        )
        out = _run_label(self.turns, ROSTER, content)
        assert [(p.cluster_label, p.slug, p.basis) for p in out] == [
            ("SPEAKER_01", "bob-stevens", "discourse")
        ]

    def test_hallucinated_name_never_becomes_proposal(self) -> None:
        # A model told (or tricked) into naming a non-roster person produces nothing.
        content = json.dumps(
            {
                "claims": [
                    {
                        "cluster_label": "SPEAKER_01",
                        "person_slug": "evil-injected-name",
                        "signal": SIGNAL_CHAIR,
                        "polarity": "next",
                        "quote": "I recognize the city manager",
                        "turn_idx": 0,
                        "confidence": "high",
                        "rationale": "x",
                    }
                ]
            }
        )
        assert _run_label(self.turns, ROSTER, content) == []

    def test_ungrounded_quote_never_becomes_proposal(self) -> None:
        content = json.dumps(
            {
                "claims": [
                    {
                        "cluster_label": "SPEAKER_01",
                        "person_slug": "bob-stevens",
                        "signal": SIGNAL_CHAIR,
                        "polarity": "next",
                        "quote": "text that does not appear in any turn",
                        "turn_idx": 0,
                        "confidence": "high",
                        "rationale": "x",
                    }
                ]
            }
        )
        assert _run_label(self.turns, ROSTER, content) == []

    def test_claims_out_and_usage_out_populated(self) -> None:
        content = json.dumps(
            {
                "claims": [
                    {
                        "cluster_label": "SPEAKER_01",
                        "person_slug": "bob-stevens",
                        "signal": SIGNAL_CHAIR,
                        "polarity": "next",
                        "quote": "I recognize the city manager",
                        "turn_idx": 0,
                        "confidence": "high",
                        "rationale": "x",
                    }
                ]
            }
        )
        claims: list = []
        usage: dict = {}
        _run_label(self.turns, ROSTER, content, claims_out=claims, usage_out=usage)
        assert len(claims) == 1 and claims[0].person_slug == "bob-stevens"
        assert usage["prompt_tokens"] == 100 and usage["completion_tokens"] == 20

    def test_llm_failure_yields_no_proposals(self) -> None:
        with patch("actalux.identity.discourse.OpenAI") as mock_openai:
            mock_openai.return_value.chat.completions.create.side_effect = RuntimeError("boom")
            out = label_discourse(self.turns, ROSTER, "k", model="openai/gpt-5-mini")
        assert out == []

    def test_empty_turns_or_members_short_circuits(self) -> None:
        with patch("actalux.identity.discourse.OpenAI") as mock_openai:
            assert label_discourse([], ROSTER, "k", model="m") == []
            assert label_discourse(self.turns, [], "k", model="m") == []
            mock_openai.return_value.chat.completions.create.assert_not_called()

    def test_provider_error_on_any_window_drops_whole_meeting(self) -> None:
        # A multi-window meeting where window 1 succeeds and window 2 errors -> no proposals
        # for the whole meeting (an API failure on a meeting = no proposals for that meeting).
        long_turns = [_turn(f"SPEAKER_{i % 3:02d}", f"turn {i}") for i in range(200)]
        good = _fake_response(
            json.dumps(
                {
                    "claims": [
                        {
                            "cluster_label": "SPEAKER_01",
                            "person_slug": "bob-stevens",
                            "signal": SIGNAL_CHAIR,
                            "polarity": "next",
                            "quote": "turn 1",
                            "turn_idx": 1,
                            "confidence": "high",
                            "rationale": "x",
                        }
                    ]
                }
            )
        )
        with patch("actalux.identity.discourse.OpenAI") as mock_openai:
            mock_openai.return_value.chat.completions.create.side_effect = [
                good,
                RuntimeError("boom"),
            ]
            out = label_discourse(long_turns, ROSTER, "k", model="openai/gpt-5-mini")
        assert out == []  # fail-closed at the meeting level

    def test_window_cap_fails_closed_without_calling_llm(self) -> None:
        # A pathological transcript exceeding the window cap yields no proposals and never
        # calls the model (cost guard).
        huge = [_turn("SPEAKER_00", "x") for _ in range(6000)]
        with patch("actalux.identity.discourse.OpenAI") as mock_openai:
            out = label_discourse(huge, ROSTER, "k", model="openai/gpt-5-mini")
            mock_openai.return_value.chat.completions.create.assert_not_called()
        assert out == []
