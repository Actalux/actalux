"""Unit tests for deterministic speaker-identity resolution (synthetic transcripts)."""

from __future__ import annotations

from collections import Counter
from types import SimpleNamespace
from typing import Any

from actalux.glossary.canonicalize import CorrectionRule
from actalux.identity.resolve import (
    _MIN_SUSTAINED_WORDS,
    RESOLVER_BASES,
    IdentityProposal,
    ResolverTurn,
    RosterMember,
    _rows_to_turns,
    persist_identities,
    resolve_identities,
)

_DISCOURSE_BASES = frozenset({"discourse"})


def _members() -> list[RosterMember]:
    return [
        RosterMember(1, "jane-harris", "Jane Harris", frozenset({"jane harris", "harris"})),
        RosterMember(2, "bob-stevens", "Bob Stevens", frozenset({"bob stevens", "stevens"})),
        RosterMember(3, "carol-diaz", "Carol Diaz", frozenset({"carol diaz", "diaz"})),
    ]


def _titled_members() -> list[RosterMember]:
    """Members carrying their per-body roster title — the input the appositive pattern needs.

    Two share nothing but a title family ("City Clerk" is Frazier's; Dilber's own title is
    "Director of Finance"), which lets the tests prove the appositive binds a name only to
    that member's OWN title.
    """
    return [
        RosterMember(
            1,
            "karen-dilber",
            "Karen Dilber",
            frozenset({"karen dilber", "dilber"}),
            "Director of Finance",
        ),
        RosterMember(
            2, "june-frazier", "June Frazier", frozenset({"june frazier", "frazier"}), "City Clerk"
        ),
        RosterMember(
            3,
            "jane-harris",
            "Jane Harris",
            frozenset({"jane harris", "harris"}),
            "Council President",
        ),
    ]


def _council_members() -> list[RosterMember]:
    """A council-style roster (shared generic title) for the roll-call / vote-call guards."""
    return [
        RosterMember(
            10, "jeffery-yorg", "Jeffery Yorg", frozenset({"jeffery yorg", "yorg"}), "Councilmember"
        ),
        RosterMember(
            11,
            "rick-hummell",
            "Rick Hummell",
            frozenset({"rick hummell", "hummell"}),
            "Councilmember",
        ),
        RosterMember(
            12,
            "karen-dilber",
            "Karen Dilber",
            frozenset({"karen dilber", "dilber"}),
            "Director of Finance",
        ),
    ]


def _t(cluster: str, text: str) -> ResolverTurn:
    return ResolverTurn(cluster, text)


def _long(n_words: int = _MIN_SUSTAINED_WORDS) -> str:
    """A turn of ``n_words`` filler words — only the word count drives the sustained floor."""
    return " ".join(["budget"] * n_words)


def test_rollcall_clean_bijection_is_high_confidence():
    turns = [
        _t("SPEAKER_09", "Jane Harris"),
        _t("SPEAKER_00", "Here"),
        _t("SPEAKER_09", "Bob Stevens"),
        _t("SPEAKER_01", "Present"),
    ]
    props = resolve_identities(turns, _members())
    by_cluster = {p.cluster_label: p for p in props}
    assert by_cluster["SPEAKER_00"].subject_id == 1
    assert by_cluster["SPEAKER_00"].confidence == "inferred_high"
    assert by_cluster["SPEAKER_00"].basis == "rollcall"
    assert by_cluster["SPEAKER_01"].subject_id == 2
    assert "SPEAKER_09" not in by_cluster  # the clerk is never attributed


def test_contested_subject_drops_to_review():
    # Two clusters both answer to "Jane Harris" -> contested -> both inferred_low.
    turns = [
        _t("SPEAKER_09", "Jane Harris"),
        _t("SPEAKER_00", "Here"),
        _t("SPEAKER_09", "Jane Harris"),
        _t("SPEAKER_01", "Here"),
    ]
    props = resolve_identities(turns, _members())
    assert {p.cluster_label for p in props} == {"SPEAKER_00", "SPEAKER_01"}
    assert all(p.subject_id == 1 and p.confidence == "inferred_low" for p in props)


def test_self_introduction_is_high_confidence():
    turns = [_t("SPEAKER_05", "Hi, I'm Bob Stevens, the treasurer.")]
    props = resolve_identities(turns, _members())
    assert len(props) == 1
    assert props[0].cluster_label == "SPEAKER_05"
    assert props[0].subject_id == 2
    assert props[0].confidence == "inferred_high"
    assert props[0].basis == "self_intro"


def test_ambiguous_cluster_two_candidates_is_dropped():
    # SPEAKER_00 is anchored to Harris (roll call) AND opens a turn introducing itself
    # as Stevens -> two candidates -> dropped (stays anonymous).
    turns = [
        _t("SPEAKER_09", "Jane Harris"),
        _t("SPEAKER_00", "Here"),
        _t("SPEAKER_00", "I'm Bob Stevens"),
    ]
    props = resolve_identities(turns, _members())
    assert all(p.cluster_label != "SPEAKER_00" for p in props)  # ambiguous -> anonymous


def test_third_person_this_is_not_a_self_intro():
    # "this is" mid-sentence (third person) must NOT publish an identity.
    turns = [_t("SPEAKER_09", "The applicant for this item is Bob Stevens")]
    assert resolve_identities(turns, _members()) == []


def test_introducing_another_person_is_not_a_self_intro():
    # The name does not immediately follow the lead-in -> not a self-introduction.
    turns = [_t("SPEAKER_05", "I am pleased to introduce Bob Stevens")]
    assert resolve_identities(turns, _members()) == []


def test_negated_rollcall_response_is_rejected():
    for response in ("Not present", "Absent", "No, not here"):
        turns = [_t("SPEAKER_09", "Jane Harris"), _t("SPEAKER_00", response)]
        assert resolve_identities(turns, _members()) == []


def test_surname_only_match_is_review_not_published():
    # "Mr. Harris" anchors via the bare surname -> proposal stays inferred_low (review).
    turns = [_t("SPEAKER_09", "Mr. Harris"), _t("SPEAKER_00", "Here")]
    props = resolve_identities(turns, _members())
    assert len(props) == 1
    assert props[0].subject_id == 1 and props[0].confidence == "inferred_low"


def test_nonroster_same_surname_is_not_anchored():
    # A non-roster "Mark Harris" is not a name-only match for roster Jane Harris, so the
    # roll-call turn anchors nothing (precision over recall).
    turns = [_t("SPEAKER_09", "Mark Harris"), _t("SPEAKER_00", "Here")]
    assert resolve_identities(turns, _members()) == []


def test_full_name_extension_is_not_published():
    # A longer name that strictly extends a roster member's name must NOT publish.
    for naming in ("Jane Harris Smith", "Jane Harris-Smith"):
        rollcall = [_t("SPEAKER_09", naming), _t("SPEAKER_00", "Here")]
        assert resolve_identities(rollcall, _members()) == []
    intro = [_t("SPEAKER_05", "I'm Jane Harris Smith")]
    assert resolve_identities(intro, _members()) == []


def test_rollcall_requires_a_name_only_turn():
    # The name-reading turn must BE the name, not arbitrary speech containing it.
    turns = [_t("SPEAKER_09", "I spoke with Jane Harris yesterday"), _t("SPEAKER_00", "Here")]
    assert resolve_identities(turns, _members()) == []


def test_response_whitelist_rejects_nonresponses():
    rejected = [_t("SPEAKER_09", "Jane Harris"), _t("SPEAKER_00", "She is here today")]
    assert resolve_identities(rejected, _members()) == []
    accepted = [_t("SPEAKER_09", "Jane Harris"), _t("SPEAKER_00", "I am here")]
    props = resolve_identities(accepted, _members())
    assert len(props) == 1 and props[0].confidence == "inferred_high"


def test_self_intro_with_role_tail_is_published():
    turns = [_t("SPEAKER_05", "I'm Jane Harris, councilmember")]
    props = resolve_identities(turns, _members())
    assert len(props) == 1
    assert props[0].subject_id == 1 and props[0].confidence == "inferred_high"


def test_long_present_turn_is_not_a_rollcall_response():
    turns = [
        _t("SPEAKER_09", "Jane Harris"),
        _t("SPEAKER_00", "I am here to present the budget for the upcoming fiscal year"),
    ]
    assert resolve_identities(turns, _members()) == []


def test_response_from_same_cluster_is_ignored():
    turns = [_t("SPEAKER_09", "Jane Harris"), _t("SPEAKER_09", "Here")]
    assert resolve_identities(turns, _members()) == []


def test_name_not_in_roster_is_never_invented():
    turns = [_t("SPEAKER_09", "Walter Unknown"), _t("SPEAKER_00", "Here")]
    assert resolve_identities(turns, _members()) == []


def test_empty_inputs():
    assert resolve_identities([], _members()) == []
    assert resolve_identities([_t("A", "Jane Harris")], []) == []


def test_presenter_intro_anchors_the_introduced_speaker():
    # Chair introduces a member in handoff position; a different cluster then takes the
    # floor with sustained speech -> that speaking cluster is anchored to the member.
    turns = [
        _t("CHAIR", "For the next item I'd like to introduce Jane Harris"),
        _t("SPEAKER_02", _long()),
    ]
    props = resolve_identities(turns, _members())
    by_cluster = {p.cluster_label: p for p in props}
    assert by_cluster["SPEAKER_02"].subject_id == 1
    assert by_cluster["SPEAKER_02"].basis == "presenter_intro"
    # Held below the public-display gate (RLS shows only inferred_high/confirmed): a handoff
    # inferred from free text must not publish a speaker label, only feed review + enrollment.
    assert by_cluster["SPEAKER_02"].confidence == "inferred_medium"
    assert "CHAIR" not in by_cluster  # the introducer is never attributed


def test_presenter_intro_with_role_tail_still_anchors():
    # A trailing role word ("treasurer") is stripped, so the name is still the suffix.
    turns = [
        _t("CHAIR", "I'd like to introduce Jane Harris, our treasurer"),
        _t("SPEAKER_02", _long()),
    ]
    props = resolve_identities(turns, _members())
    assert len(props) == 1
    assert props[0].cluster_label == "SPEAKER_02" and props[0].subject_id == 1


def test_presenter_intro_longer_name_extension_does_not_anchor():
    # A longer name that strictly extends a roster name is a different person -> no anchor.
    turns = [
        _t("CHAIR", "I'll turn it over to Jane Harris Smith"),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _members()) == []


def test_presenter_intro_prefix_extension_does_not_anchor():
    # Roster "Jane Harris" is only a SUFFIX of the spoken "Mary Jane Harris" -> no anchor.
    turns = [
        _t("CHAIR", "I'll turn it over to Mary Jane Harris"),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _members()) == []


def test_presenter_intro_requires_a_different_cluster_to_take_over():
    # Same cluster keeps the floor after naming the member -> no handoff happened.
    turns = [
        _t("CHAIR", "I'll turn it over to Jane Harris"),
        _t("CHAIR", _long()),
    ]
    assert resolve_identities(turns, _members()) == []


def test_presenter_intro_two_names_is_ambiguous():
    # Two distinct members named in the introducer's turn -> cannot tell who takes over.
    turns = [
        _t("CHAIR", "I'll turn it over to Jane Harris and Bob Stevens"),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _members()) == []


def test_presenter_intro_brief_reply_does_not_anchor():
    # The incoming cluster speaks well below the sustained floor -> not a presentation.
    turns = [
        _t("CHAIR", "I'll turn it over to Jane Harris"),
        _t("SPEAKER_02", "Thank you very much"),
    ]
    assert resolve_identities(turns, _members()) == []


def test_presenter_intro_name_early_in_turn_is_not_a_handoff():
    # The member is named at the START of a long turn (incidental reference), not at the
    # handoff position near its end -> no anchor even though a cluster then speaks at length.
    turns = [
        _t("CHAIR", "Jane Harris asked us last week to review the budget before we begin"),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _members()) == []


def test_presenter_intro_requires_the_next_speaker_to_hold_the_floor():
    # Chair hands off, but the immediate next cluster only interjects briefly and a THIRD
    # cluster presents at length -> the introduced voice can't be pinned -> no anchor.
    turns = [
        _t("CHAIR", "I'll turn it over to Jane Harris"),
        _t("SPEAKER_02", "Okay"),
        _t("SPEAKER_03", _long()),
    ]
    assert resolve_identities(turns, _members()) == []


def test_presenter_intro_tie_in_the_window_does_not_anchor():
    # Two clusters speak the same amount after the handoff -> the presenter can't be pinned.
    turns = [
        _t("CHAIR", "I'll turn it over to Jane Harris"),
        _t("SPEAKER_02", _long()),
        _t("SPEAKER_03", _long()),
    ]
    assert resolve_identities(turns, _members()) == []


def test_presenter_intro_trailing_incidental_mention_does_not_anchor():
    # The name ends the turn but as an incidental thank-you (no connector/honorific before
    # it), then a different cluster speaks at length -> not a handoff to that member.
    turns = [
        _t("CHAIR", "That completes the report, thank you Jane Harris"),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _members()) == []


def test_presenter_intro_gratitude_closing_with_connector_does_not_anchor():
    # "...thank you TO Jane Harris" has a connector before the name (passes the boundary),
    # but no handoff cue -> a gratitude closing must not anchor the next speaker.
    turns = [
        _t("CHAIR", "That completes the report, thank you to Jane Harris"),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _members()) == []


def test_presenter_intro_negated_cue_does_not_anchor():
    # A negation directly governing the cue flips it -> no handoff.
    for turn_text in ("I do not recognize Jane Harris", "I did not invite Jane Harris"):
        turns = [_t("CHAIR", turn_text), _t("SPEAKER_02", _long())]
        assert resolve_identities(turns, _members()) == [], turn_text


def test_presenter_intro_earlier_cue_does_not_license_a_gratitude_close():
    # A cue word ("Welcome") earlier in the turn must not license a later gratitude closing
    # that happens to end with a member's name -> the cue must be local to the name.
    turns = [
        _t("CHAIR", "Welcome everyone. That completes the report, thank you to Jane Harris"),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _members()) == []


def test_presenter_intro_noun_and_transfer_lookalikes_do_not_anchor():
    # Candidate cue words that mis-read in non-handoff senses must NOT anchor: object
    # transfers ("over to"), noun uses of "call"/"floor", and the applause "a hand for".
    for turn_text in (
        "I sent the packet over to Jane Harris",
        "Those records were turned over to Jane Harris",
        "The matter is now over to Jane Harris",
        "I had a phone call with Jane Harris",
        "Next item is the roll call for Jane Harris",
        "The next comment is about the floor to Jane Harris",
        "Let's give a hand for Jane Harris",
    ):
        turns = [_t("CHAIR", turn_text), _t("SPEAKER_02", _long())]
        assert resolve_identities(turns, _members()) == [], turn_text


def test_presenter_intro_nonroster_name_does_not_anchor():
    turns = [
        _t("CHAIR", "I'll turn it over to Walter Unknown"),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _members()) == []


def test_presenter_intro_surname_only_does_not_anchor():
    # A bare surname is too collision-prone to launch an anchor (unlike roll call, which
    # keeps a surname hit as review-only); a presenter handoff needs a full-name mention.
    turns = [
        _t("CHAIR", "I'll turn it over to Harris"),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _members()) == []


def test_presenter_intro_does_not_borrow_strength_from_a_surname_rollcall():
    # A surname-only roll call is review-only on its own; pairing it with a strong presenter
    # introduction for the same member must NOT promote it to the public tier. Confidence and
    # basis come from one anchor together -> stays non-public inferred_medium presenter_intro.
    turns = [
        _t("CLERK", "Harris"),
        _t("SPEAKER_02", "Here"),
        _t("CHAIR", "I'd like to introduce Jane Harris"),
        _t("SPEAKER_02", _long()),
    ]
    props = resolve_identities(turns, _members())
    by_cluster = {p.cluster_label: p for p in props}
    assert by_cluster["SPEAKER_02"].subject_id == 1
    assert by_cluster["SPEAKER_02"].confidence == "inferred_medium"
    assert by_cluster["SPEAKER_02"].basis == "presenter_intro"


def test_rollcall_outranks_presenter_intro_for_recorded_basis():
    # One cluster is reached by both a roll call and a presenter handoff for the same
    # member -> the stronger roll-call basis is the one recorded.
    turns = [
        _t("CLERK", "Jane Harris"),
        _t("SPEAKER_02", "Here"),
        _t("CHAIR", "I now recognize Jane Harris"),
        _t("SPEAKER_02", _long()),
    ]
    props = resolve_identities(turns, _members())
    by_cluster = {p.cluster_label: p for p in props}
    assert by_cluster["SPEAKER_02"].subject_id == 1
    assert by_cluster["SPEAKER_02"].basis == "rollcall"
    # Recorded as a roll call, so it publishes (inferred_high), not the presenter tier.
    assert by_cluster["SPEAKER_02"].confidence == "inferred_high"


# --- presenter-introduction: title-appositive + presence families (real corpus + edges) --


def test_presenter_intro_presence_and_appositive_real_sentence():
    # Real council-transcript introduction: no whitelisted cue verb, name mid-turn. Fires via
    # both the presence template ("have with us <name>") and the name+own-title appositive.
    turns = [
        _t(
            "CHAIR",
            "this evening we have with us karen dilber, our director of finance "
            "to go through the budget",
        ),
        _t("SPEAKER_02", _long()),
    ]
    tally: Counter[str] = Counter()
    props = resolve_identities(turns, _titled_members(), tally)
    by_cluster = {p.cluster_label: p for p in props}
    assert by_cluster["SPEAKER_02"].subject_id == 1
    assert by_cluster["SPEAKER_02"].basis == "presenter_intro"
    # Held below the public gate exactly like the cue-verb handoff — inferred from free text.
    assert by_cluster["SPEAKER_02"].confidence == "inferred_medium"
    assert "CHAIR" not in by_cluster  # the introducer is never attributed
    assert tally["title_appositive"] == 1  # appositive is credited first when both fire


def test_presenter_intro_appositive_title_before_name_real_sentence():
    # Real council-transcript introduction with the title BEFORE the name.
    turns = [
        _t("CHAIR", "we do have our director of finance, karen dilber here as well"),
        _t("SPEAKER_02", _long()),
    ]
    props = resolve_identities(turns, _titled_members())
    assert len(props) == 1
    assert props[0].cluster_label == "SPEAKER_02" and props[0].subject_id == 1
    assert props[0].basis == "presenter_intro" and props[0].confidence == "inferred_medium"


def test_presenter_intro_gratitude_appositive_does_not_anchor_real_sentence():
    # "...thank our city clerk june frazier" is name+own-title adjacency identical in surface
    # form to an introduction; the gratitude word must disqualify it even with a valid handoff
    # structure following. This is the load-bearing precision guard for the appositive.
    turns = [
        _t("CHAIR", "i would also like to thank our city clerk june frazier"),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _titled_members()) == []


def test_presenter_intro_appositive_requires_the_members_own_title():
    # Dilber named next to a DIFFERENT member's title ("city clerk" is Frazier's) -> the
    # appositive binds a name only to that member's own title, so this does not anchor.
    turns = [
        _t("CHAIR", "our city clerk karen dilber will now speak"),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _titled_members()) == []


def test_presenter_intro_appositive_gap_too_large_does_not_anchor():
    # The member's title appears only in a later clause, well beyond the appositive window.
    turns = [
        _t("CHAIR", "karen dilber joined the city last spring as the director of finance"),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _titled_members()) == []


def test_presenter_intro_presence_suffix_is_here_anchors():
    turns = [
        _t("CHAIR", "karen dilber is here to walk us through the numbers"),
        _t("SPEAKER_02", _long()),
    ]
    tally: Counter[str] = Counter()
    props = resolve_identities(turns, _titled_members(), tally)
    assert len(props) == 1 and props[0].subject_id == 1
    assert props[0].basis == "presenter_intro"
    assert tally["presence_intro"] == 1


def test_presenter_intro_presence_suffix_is_with_us_anchors():
    turns = [
        _t("CHAIR", "jane harris is with us to present the plan"),
        _t("SPEAKER_02", _long()),
    ]
    props = resolve_identities(turns, _titled_members())
    assert len(props) == 1 and props[0].subject_id == 3


def test_presenter_intro_presence_here_to_present_is_anchors():
    turns = [
        _t("CHAIR", "here to present is jane harris from the finance office"),
        _t("SPEAKER_02", _long()),
    ]
    props = resolve_identities(turns, _titled_members())
    assert len(props) == 1 and props[0].subject_id == 3


def test_presenter_intro_presence_negated_does_not_anchor():
    # A negation shortly before the presence template flips it -> no handoff.
    turns = [
        _t("CHAIR", "we do not have with us karen dilber this evening"),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _titled_members()) == []


def test_presenter_intro_gratitude_blocks_presence_template():
    # A thanks word shortly before a presence template disqualifies it just as for appositive.
    turns = [
        _t("CHAIR", "i want to thank karen dilber is here today"),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _titled_members()) == []


def test_presenter_intro_appositive_requires_a_different_cluster():
    # Same cluster keeps the floor after the appositive introduction -> no handoff happened.
    turns = [
        _t("CHAIR", "we have with us karen dilber, our director of finance"),
        _t("CHAIR", _long()),
    ]
    assert resolve_identities(turns, _titled_members()) == []


def test_presenter_intro_appositive_brief_reply_does_not_anchor():
    # The incoming cluster speaks well below the sustained floor -> not a presentation.
    turns = [
        _t("CHAIR", "we have with us karen dilber, our director of finance"),
        _t("SPEAKER_02", "thank you very much"),
    ]
    assert resolve_identities(turns, _titled_members()) == []


def test_presenter_intro_appositive_two_members_is_ambiguous():
    # Two distinct members named in the introducer's turn -> cannot tell who takes over.
    turns = [
        _t("CHAIR", "we have with us karen dilber and june frazier tonight"),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _titled_members()) == []


def test_presenter_intro_tally_counts_cue_verb_pattern():
    # The cue-verb family still fires and is credited separately in the fire-count report.
    turns = [
        _t("CHAIR", "for the next item i would like to introduce jane harris"),
        _t("SPEAKER_02", _long()),
    ]
    tally: Counter[str] = Counter()
    resolve_identities(turns, _titled_members(), tally)
    assert tally["cue_verb"] == 1
    assert tally["title_appositive"] == 0 and tally["presence_intro"] == 0


def test_presenter_intro_appositive_gap_must_be_filler_words():
    # A name and its own title co-occur but a content word sits between them ("<name> <verb>
    # the <title>") -> an incidental clause, not an appositive -> no anchor.
    turns = [
        _t("CHAIR", "karen dilber reviewed the director of finance report last week"),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _titled_members()) == []


def test_presenter_intro_title_before_name_incidental_reference_does_not_anchor():
    # "our <title> <name> <keeps talking>" is a referential mention (the O'Keefe/Gipson shape),
    # not a handoff -> title-before-name needs the name to end the turn or lead into a handoff.
    turns = [
        _t("CHAIR", "our director of finance karen dilber mentioned the shortfall earlier"),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _titled_members()) == []


def test_presenter_intro_title_before_name_turn_final_with_content_anchors():
    # Title-before-name where the name ends the turn AND the turn carries real introduction
    # content still anchors (the content guard distinguishes this from a bare roll-call address).
    turns = [
        _t("CHAIR", "and now to walk us through the report our director of finance karen dilber"),
        _t("SPEAKER_02", _long()),
    ]
    props = resolve_identities(turns, _titled_members())
    assert len(props) == 1 and props[0].subject_id == 1
    assert props[0].basis == "presenter_intro"


def test_presenter_intro_rollcall_address_does_not_anchor():
    # A bare roll-call / vote prompt "<honorific> <name>[?]" matches the title appositive
    # (title-before-name, name turn-final) but carries no introduction content, so the vote
    # responses that follow it must not be anchored to the addressed member (docs 2517/2515).
    for prompt in ("Councilmember Jeffery Yorg?", "Councilmember Jeffery Yorg"):
        turns = [_t("CHAIR", prompt), _t("SPEAKER_02", _long())]
        assert resolve_identities(turns, _council_members()) == [], prompt


def test_presenter_intro_vote_call_two_names_is_ambiguous():
    # A roll-call turn naming two members is blocked by the two-distinct-members ambiguity
    # guard, which applies to ALL families including title_appositive (grouped vote call).
    turns = [
        _t("CHAIR", "Councilmember Rick Hummell? Aye. Councilmember Jeffery Yorg?"),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _council_members()) == []


def test_presenter_intro_terse_name_before_title_still_anchors():
    # The content gate is scoped to the title-before-name (roll-call-prone) order only; a terse
    # name-before-title appositive is the canonical introducing form and keeps its recall.
    turns = [
        _t("CHAIR", "Karen Dilber, our Director of Finance."),
        _t("SPEAKER_02", _long()),
    ]
    props = resolve_identities(turns, _council_members())
    assert len(props) == 1 and props[0].subject_id == 12
    assert props[0].basis == "presenter_intro"


def test_presenter_intro_procedural_vote_prompt_does_not_anchor():
    # A terse title-before-name vote prompt stays below the content floor because vote-action
    # words are excluded ("will now vote" -> only "will"/"now" count = 2 < 3) -> no anchor.
    turns = [
        _t("CHAIR", "Councilmember Jeffery Yorg will now vote"),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _council_members()) == []


def test_presenter_intro_commendation_does_not_anchor():
    # A commendation ("commend") shortly before a name+title reads as recognition, not a handoff.
    turns = [
        _t("CHAIR", "i want to commend karen dilber, our director of finance, for her work"),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _titled_members()) == []


def test_presenter_intro_applause_recognition_does_not_anchor():
    # "...a round of applause to June Frazier, our city clerk" is recognition of the member,
    # not a handoff of the floor to them -> must not anchor even with a valid handoff after.
    turns = [
        _t("CHAIR", "lets give a round of applause to june frazier, our city clerk tonight"),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _titled_members()) == []


def test_presenter_intro_first_person_recital_does_not_anchor():
    # A first-person recital ("I, <name>, <title>, hereby proclaim...") names the SPEAKER, not
    # a third party to hand the floor to, so it must not anchor the next (different) cluster.
    turns = [
        _t(
            "CHAIR",
            "now therefore be it resolved i, jane harris, council president, hereby proclaim",
        ),
        _t("SPEAKER_02", _long()),
    ]
    assert resolve_identities(turns, _titled_members()) == []


def test_rows_to_turns_canonicalizes_names():
    rows = [
        {
            "cluster_label": "SPEAKER_00",
            "words": [{"word": "Council"}, {"word": "member"}, {"word": "York"}],
        }
    ]
    rules = [CorrectionRule("york", "Jeffery Yorg", "lexicon")]
    assert _rows_to_turns(rows, rules)[0].text == "Council member Jeffery Yorg"
    assert _rows_to_turns(rows)[0].text == "Council member York"  # raw when no rules


def test_resolution_after_canonicalizing_a_mangled_name():
    # The mangled "York" only resolves to roster "Jeffery Yorg" once canonicalized.
    members = [RosterMember(1, "jeffery-yorg", "Jeffery Yorg", frozenset({"jeffery yorg"}))]
    rows = [
        {
            "cluster_label": "CLERK",
            "words": [{"word": "Council"}, {"word": "member"}, {"word": "York"}],
        },
        {"cluster_label": "M1", "words": [{"word": "Here"}]},
    ]
    rules = [CorrectionRule("york", "Jeffery Yorg", "lexicon")]
    props = resolve_identities(_rows_to_turns(rows, rules), members)
    assert len(props) == 1
    assert props[0].subject_id == 1 and props[0].confidence == "inferred_high"
    # Without canonicalization the raw "York" does not match -> no proposal.
    assert resolve_identities(_rows_to_turns(rows), members) == []


def test_to_row_shape():
    row = IdentityProposal("SPEAKER_00", 1, "jane-harris", "inferred_high", "rollcall").to_row(7)
    assert row == {
        "document_id": 7,
        "cluster_label": "SPEAKER_00",
        "subject_id": 1,
        "confidence": "inferred_high",
        "basis": "rollcall",
    }


class _IdentTable:
    def __init__(self, existing: list[dict], log: list[dict]) -> None:
        self._existing = existing
        self._log = log
        self._op: str | None = None
        self._payload: Any = None
        self._on_conflict: str | None = None
        self._filters: list[tuple[str, Any]] = []

    def select(self, _cols: str) -> _IdentTable:
        self._op = "select"
        return self

    def delete(self) -> _IdentTable:
        self._op = "delete"
        return self

    def eq(self, col: str, val: Any) -> _IdentTable:
        self._filters.append((col, val))
        return self

    def upsert(self, payload: Any, on_conflict: str | None = None) -> _IdentTable:
        self._op, self._payload, self._on_conflict = "upsert", payload, on_conflict
        return self

    def execute(self) -> SimpleNamespace:
        if self._op == "select":
            return SimpleNamespace(data=self._existing)
        self._log.append(
            {
                "op": self._op,
                "payload": self._payload,
                "on_conflict": self._on_conflict,
                "filters": self._filters,
            }
        )
        return SimpleNamespace(data=[])


class _IdentClient:
    def __init__(self, existing: list[dict]) -> None:
        self._existing = existing
        self.log: list[dict] = []

    def table(self, _name: str) -> _IdentTable:
        return _IdentTable(self._existing, self.log)


def test_persist_identities_protects_confirmed_clusters():
    proposals = [
        IdentityProposal("SPEAKER_00", 1, "jane-harris", "inferred_high", "rollcall"),
        IdentityProposal("SPEAKER_01", 2, "bob-stevens", "inferred_high", "rollcall"),
    ]
    # SPEAKER_00 was confirmed by a human -> must not be overwritten by the auto pass.
    client = _IdentClient([{"cluster_label": "SPEAKER_00", "confidence": "confirmed"}])
    written = persist_identities(client, 7, proposals)
    assert written == 1
    upserts = [e for e in client.log if e["op"] == "upsert"]
    assert [r["cluster_label"] for r in upserts[0]["payload"]] == ["SPEAKER_01"]
    assert upserts[0]["on_conflict"] == "document_id,cluster_label"
    assert not [e for e in client.log if e["op"] == "delete"]  # nothing stale to retract


def test_persist_identities_retracts_stale_auto_rows():
    # SPEAKER_00 was auto-published but is no longer proposed -> retract it.
    # SPEAKER_01 is confirmed (manual) -> keep it even though not proposed.
    existing = [
        {"cluster_label": "SPEAKER_00", "confidence": "inferred_high"},
        {"cluster_label": "SPEAKER_01", "confidence": "confirmed"},
    ]
    proposals = [IdentityProposal("SPEAKER_02", 3, "carol-diaz", "inferred_high", "rollcall")]
    client = _IdentClient(existing)
    written = persist_identities(client, 7, proposals)
    deletes = [e for e in client.log if e["op"] == "delete"]
    deleted_clusters = {dict(e["filters"]).get("cluster_label") for e in deletes}
    assert deleted_clusters == {"SPEAKER_00"}  # stale auto retracted; confirmed kept
    assert written == 1


def test_persist_identities_no_proposals_no_existing_is_noop():
    client = _IdentClient([])
    assert persist_identities(client, 7, []) == 0
    assert client.log == []


def test_persist_identities_protects_rejected_clusters():
    # A human DENIED SPEAKER_00 -> the resolver must never re-propose it (no upsert on it), and it
    # is not treated as a stale auto row to retract either.
    proposals = [
        IdentityProposal("SPEAKER_00", 1, "jane-harris", "inferred_high", "rollcall"),
        IdentityProposal("SPEAKER_01", 2, "bob-stevens", "inferred_high", "rollcall"),
    ]
    client = _IdentClient([{"cluster_label": "SPEAKER_00", "confidence": "rejected"}])
    written = persist_identities(client, 7, proposals)
    assert written == 1
    upserts = [e for e in client.log if e["op"] == "upsert"]
    assert [r["cluster_label"] for r in upserts[0]["payload"]] == ["SPEAKER_01"]
    assert not [e for e in client.log if e["op"] == "delete"]  # rejected row is not retracted


def test_persist_identities_keeps_rejected_when_no_longer_proposed():
    # SPEAKER_00 denied and no longer proposed -> kept (not deleted), exactly like a confirmed row.
    existing = [{"cluster_label": "SPEAKER_00", "confidence": "rejected"}]
    proposals = [IdentityProposal("SPEAKER_01", 2, "bob-stevens", "inferred_high", "rollcall")]
    client = _IdentClient(existing)
    persist_identities(client, 7, proposals)
    assert not [e for e in client.log if e["op"] == "delete"]


# --- family-scoped coexistence of the resolver and the discourse labeler ---------------
# Two independent evidence families write speaker_identities on one document. The invariant:
# a discourse row survives resolver re-passes and vice versa, and a (document, cluster)
# conflict resolves by tier (higher wins; equal keeps existing) — see persist_identities.


def _deletes(client: _IdentClient) -> set:
    return {dict(e["filters"]).get("cluster_label") for e in client.log if e["op"] == "delete"}


def _upserted(client: _IdentClient) -> set:
    return {r["cluster_label"] for e in client.log if e["op"] == "upsert" for r in e["payload"]}


def test_resolver_repass_never_retracts_a_foreign_discourse_row():
    # A discourse row on SPEAKER_05 must survive a resolver re-pass that doesn't propose it.
    existing = [
        {"cluster_label": "SPEAKER_05", "confidence": "inferred_medium", "basis": "discourse"},
    ]
    proposals = [IdentityProposal("SPEAKER_00", 1, "jane-harris", "inferred_high", "rollcall")]
    client = _IdentClient(existing)
    persist_identities(client, 7, proposals, managed_bases=RESOLVER_BASES)
    assert _deletes(client) == set()  # foreign discourse row not retracted
    assert _upserted(client) == {"SPEAKER_00"}


def test_discourse_repass_never_retracts_a_foreign_resolver_row():
    existing = [
        {"cluster_label": "SPEAKER_00", "confidence": "inferred_high", "basis": "rollcall"},
    ]
    proposals = [IdentityProposal("SPEAKER_05", 3, "carol-diaz", "inferred_medium", "discourse")]
    client = _IdentClient(existing)
    persist_identities(client, 7, proposals, managed_bases=_DISCOURSE_BASES)
    assert _deletes(client) == set()  # foreign resolver row not retracted
    assert _upserted(client) == {"SPEAKER_05"}


def test_discourse_yields_to_higher_tier_resolver_row_on_same_cluster():
    # Discourse (medium) must NOT clobber a foreign resolver inferred_high on the same cluster.
    existing = [
        {"cluster_label": "SPEAKER_00", "confidence": "inferred_high", "basis": "rollcall"},
    ]
    proposals = [IdentityProposal("SPEAKER_00", 1, "jane-harris", "inferred_medium", "discourse")]
    client = _IdentClient(existing)
    written = persist_identities(client, 7, proposals, managed_bases=_DISCOURSE_BASES)
    assert written == 0 and _upserted(client) == set()  # kept the resolver row
    assert _deletes(client) == set()


def test_higher_tier_resolver_takes_over_foreign_discourse_row():
    # A resolver inferred_high overwrites a foreign discourse medium on the same cluster.
    existing = [
        {"cluster_label": "SPEAKER_00", "confidence": "inferred_medium", "basis": "discourse"},
    ]
    proposals = [IdentityProposal("SPEAKER_00", 1, "jane-harris", "inferred_high", "rollcall")]
    client = _IdentClient(existing)
    written = persist_identities(client, 7, proposals, managed_bases=RESOLVER_BASES)
    assert written == 1 and _upserted(client) == {"SPEAKER_00"}  # higher tier wins


def test_equal_tier_conflict_keeps_existing_foreign_row():
    # presenter_intro (medium) already holds the cluster; a discourse (medium) re-pass ties
    # -> keep existing, so the outcome is stable across passes.
    existing = [
        {
            "cluster_label": "SPEAKER_00",
            "confidence": "inferred_medium",
            "basis": "presenter_intro",
        },
    ]
    proposals = [IdentityProposal("SPEAKER_00", 1, "jane-harris", "inferred_medium", "discourse")]
    client = _IdentClient(existing)
    written = persist_identities(client, 7, proposals, managed_bases=_DISCOURSE_BASES)
    assert written == 0 and _upserted(client) == set()


def test_discourse_takes_over_lower_tier_resolver_contested_row():
    # A resolver contested cluster (inferred_low) yields to a corroborated discourse medium.
    existing = [
        {"cluster_label": "SPEAKER_00", "confidence": "inferred_low", "basis": "rollcall"},
    ]
    proposals = [IdentityProposal("SPEAKER_00", 3, "carol-diaz", "inferred_medium", "discourse")]
    client = _IdentClient(existing)
    written = persist_identities(client, 7, proposals, managed_bases=_DISCOURSE_BASES)
    assert written == 1 and _upserted(client) == {"SPEAKER_00"}  # medium > low


def test_discourse_retracts_only_its_own_stale_rows():
    # SPEAKER_05 (discourse, no longer proposed) is retracted; SPEAKER_00 (foreign resolver) is not.
    existing = [
        {"cluster_label": "SPEAKER_05", "confidence": "inferred_medium", "basis": "discourse"},
        {"cluster_label": "SPEAKER_00", "confidence": "inferred_high", "basis": "rollcall"},
    ]
    proposals = [IdentityProposal("SPEAKER_09", 2, "bob-stevens", "inferred_medium", "discourse")]
    client = _IdentClient(existing)
    persist_identities(client, 7, proposals, managed_bases=_DISCOURSE_BASES)
    assert _deletes(client) == {"SPEAKER_05"}  # own stale retracted; foreign resolver kept


def test_discourse_replaces_its_own_prior_row_regardless_of_tier():
    # An OWNED discourse row is always replaced by the family's current proposal (no arbitration
    # against your own row) — here re-proposing the same cluster for a different member.
    existing = [
        {"cluster_label": "SPEAKER_00", "confidence": "inferred_medium", "basis": "discourse"},
    ]
    proposals = [IdentityProposal("SPEAKER_00", 2, "bob-stevens", "inferred_medium", "discourse")]
    client = _IdentClient(existing)
    written = persist_identities(client, 7, proposals, managed_bases=_DISCOURSE_BASES)
    assert written == 1 and _upserted(client) == {"SPEAKER_00"}
