"""
Tests for value_matches() — how a rule's search string is compared to a field.

The three modes exist because one size genuinely does not fit: "membership" must
be found inside a long free-text answer, while "No" must NOT be found inside
"Not attending".

Run: pytest tests/test_value_matches.py -s -v
"""

import pytest


@pytest.fixture
def vm():
    from pco_webhook.main import value_matches
    return value_matches


class TestContainsIsTheDefault:
    """Every rule written before `match` existed must behave exactly as before."""

    def test_no_rule_at_all_is_contains(self, vm):
        assert vm("membership", "I want membership and community") is True

    def test_empty_match_is_contains(self, vm):
        assert vm("membership", "looking for membership", {"match": ""}) is True

    def test_missing_key_is_contains(self, vm):
        assert vm("membership", "looking for membership", {}) is True

    def test_the_live_rule_still_matches(self, vm):
        """The real 'Relationship contains membership' rule, unchanged."""
        long_answer = "Interested in joining!  Please contact me about membership."
        assert vm("membership", long_answer, {"match": "contains"}) is True


class TestWholeWord:
    """'No' must not fire on 'Not attending' — the reason this was added."""

    @pytest.mark.parametrize("value,expected", [
        ("No", True),
        ("no", True),
        ("No way", True),        # still a whole word, just not alone
        ("None", False),
        ("Not attending", False),
        ("Not Needed", False),
        ("Yes", False),
    ])
    def test_no_against_lookalikes(self, vm, value, expected):
        assert vm("No", value, {"match": "whole word"}) is expected

    def test_contains_would_have_been_wrong(self, vm):
        """Documents the trap: the default mode does fire on these."""
        assert vm("No", "Not attending", {"match": "contains"}) is True
        assert vm("No", "Not attending", {"match": "whole word"}) is False

    def test_multi_word_search(self, vm):
        assert vm("not needed", "Not Needed", {"match": "whole word"}) is True
        assert vm("not needed", "Not Needed Yet", {"match": "whole word"}) is True


class TestExact:

    @pytest.mark.parametrize("value,expected", [
        ("No", True),
        ("  No  ", True),        # stored values get stripped
        ("No way", False),
        ("None", False),
    ])
    def test_exact(self, vm, value, expected):
        assert vm("No", value, {"match": "exact"}) is expected

    def test_long_value_needs_the_whole_thing(self, vm):
        """Why exact is not the default — you would have to type all of this."""
        long_answer = "looking for community with like minded people"
        assert vm("community", long_answer, {"match": "exact"}) is False
        assert vm(long_answer, long_answer, {"match": "exact"}) is True


class TestEdges:

    def test_blank_search_never_matches(self, vm):
        for mode in ("contains", "whole word", "exact"):
            assert vm("", "anything", {"match": mode}) is False

    def test_case_insensitive_in_every_mode(self, vm):
        assert vm("NO", "no", {"match": "exact"}) is True
        assert vm("no", "NO WAY", {"match": "whole word"}) is True
        assert vm("MeMbEr", "membership", {"match": "contains"}) is True

    @pytest.mark.parametrize("mode", ["", "fuzzy", "anything", "whol"])
    def test_unrecognised_mode_falls_back_to_contains(self, vm, mode):
        """A typo must not silently stop rules matching — contains is the safe default."""
        assert vm("No", "Not attending", {"match": mode}) is True

    @pytest.mark.parametrize("mode", ["whole word", "wholeword", "Whole-Word", "WHOLE WORD"])
    def test_whole_word_spelling_is_lenient(self, vm, mode):
        """Anything starting with 'whole' counts, so hand-edited rules.json is forgiving."""
        assert vm("No", "Not attending", {"match": mode}) is False

    @pytest.mark.parametrize("mode", ["exact", "Exact", "exact match"])
    def test_exact_spelling_is_lenient(self, vm, mode):
        assert vm("No", "No way", {"match": mode}) is False
