"""Tests for positional vote linkage (actalux.landuse.votes_link, G4).

The corpus fact these pin: different items are decided by byte-identical
sentences (doc 1250 has "made a motion to approve as submitted" three times, as
three distinct votes), so linkage must be positional — k-th occurrence to k-th
vote in parse order — never text-match alone.
"""

from __future__ import annotations

from actalux.landuse.segment import BusinessItem
from actalux.landuse.votes_link import link_votes, position_votes


def _item(start: int, end: int) -> BusinessItem:
    return BusinessItem(
        address_raw="42 Elm",
        type_raw="Variance",
        subtype_raw=None,
        video_timestamp=None,
        application_type="variance",
        body="",
        start=start,
        end=end,
    )


DOC = (
    "1. First item text here.\n"
    "Helen DiFate made a motion to approve as submitted. Carried.\n"
    "2. Second item text here.\n"
    "Helen DiFate made a motion to approve as submitted. Carried.\n"
    "3. Third item text here.\n"
    "Helen DiFate made a motion to deny the request. Carried.\n"
)
Q_APPROVE = "Helen DiFate made a motion to approve as submitted."
Q_DENY = "Helen DiFate made a motion to deny the request."


class TestPositionVotes:
    def test_identical_quotes_pair_kth_occurrence_to_kth_vote(self) -> None:
        votes = [
            {"id": 11, "motion": Q_APPROVE},
            {"id": 12, "motion": Q_APPROVE},
            {"id": 13, "motion": Q_DENY},
        ]
        pos = position_votes(DOC, votes)
        assert pos[11] < pos[12] < pos[13]
        assert DOC[pos[11] : pos[11] + 5] == "Helen"

    def test_parse_order_is_id_order_even_when_rows_arrive_shuffled(self) -> None:
        votes = [
            {"id": 12, "motion": Q_APPROVE},
            {"id": 11, "motion": Q_APPROVE},
        ]
        pos = position_votes(DOC, votes)
        assert pos[11] < pos[12]

    def test_unlocatable_quote_leaves_vote_unpositioned(self) -> None:
        pos = position_votes(DOC, [{"id": 9, "motion": "a fabricated passage"}])
        assert pos == {}

    def test_more_votes_than_occurrences_drops_the_excess_not_misassigns(self) -> None:
        votes = [{"id": i, "motion": Q_DENY} for i in (21, 22)]
        pos = position_votes(DOC, votes)
        assert 21 in pos and 22 not in pos


class TestLinkVotes:
    def test_each_item_gets_its_own_identically_worded_vote(self) -> None:
        i1 = _item(0, DOC.index("2. Second"))
        i2 = _item(DOC.index("2. Second"), DOC.index("3. Third"))
        i3 = _item(DOC.index("3. Third"), len(DOC))
        votes = [
            {"id": 11, "motion": Q_APPROVE},
            {"id": 12, "motion": Q_APPROVE},
            {"id": 13, "motion": Q_DENY},
        ]
        links = link_votes(DOC, [i1, i2, i3], votes)
        assert links == {i1.start: 11, i2.start: 12, i3.start: 13}

    def test_multiple_votes_in_one_span_link_the_last(self) -> None:
        # A procedural motion (open the hearing) precedes the decision; the
        # item's decisive vote is the final one recorded in its span.
        doc = (
            "1. Item.\nA motion to open the public hearing. Carried.\n"
            "A motion to approve the variance. Carried.\n"
        )
        votes = [
            {"id": 1, "motion": "A motion to open the public hearing."},
            {"id": 2, "motion": "A motion to approve the variance."},
        ]
        links = link_votes(doc, [_item(0, len(doc))], votes)
        assert links == {0: 2}

    def test_item_without_a_positioned_vote_is_absent_never_guessed(self) -> None:
        links = link_votes(DOC, [_item(0, 10)], [{"id": 5, "motion": Q_DENY}])
        assert links == {}
