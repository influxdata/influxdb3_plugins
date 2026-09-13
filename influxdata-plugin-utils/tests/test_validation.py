"""Tests for influxdata_plugin_utils.validation."""

from datetime import timedelta

import pytest

from influxdata_plugin_utils.parsing import parse_timedelta
from influxdata_plugin_utils.validation import Validator, validate


class TestDefaultsAndCasting:
    def test_default_fills_a_missing_key(self):
        assert validate({}, [Validator("window", default="1h")]) == {"window": "1h"}

    def test_cast_applies_to_the_default_as_well(self):
        rule = Validator("window", default="1h", cast=parse_timedelta)
        assert validate({}, [rule]) == {"window": timedelta(hours=1)}
        assert validate({"window": "5min"}, [rule]) == {"window": timedelta(minutes=5)}

    def test_checks_run_on_the_cast_value(self):
        rule = Validator("rows", cast=int, gte=1)
        assert validate({"rows": "5"}, [rule]) == {"rows": 5}
        with pytest.raises(ValueError, match=r"rows must be at least 1, got 0"):
            validate({"rows": "0"}, [rule])

    def test_a_failing_cast_keeps_its_own_message(self):
        with pytest.raises(ValueError, match="window: Unknown duration unit 'y'"):
            validate({"window": "5y"}, [Validator("window", cast=parse_timedelta)])

    def test_a_cast_that_cannot_take_the_value_is_still_a_valueerror(self):
        with pytest.raises(ValueError, match="rows: cannot read"):
            validate({"rows": None}, [Validator("rows", cast=len)])


class TestPresence:
    def test_required_key_missing(self):
        with pytest.raises(ValueError, match="measurement is required"):
            validate({}, [Validator("measurement", required=True)])

    def test_must_exist_is_the_same_rule(self):
        with pytest.raises(ValueError, match="measurement is required"):
            validate({}, [Validator("measurement", must_exist=True)])

    def test_a_default_satisfies_a_required_key(self):
        rule = Validator("measurement", required=True, default="cpu")
        assert validate({}, [rule]) == {"measurement": "cpu"}

    @pytest.mark.parametrize("blank", [None, "", "   "])
    def test_a_blank_value_does_not_satisfy_a_required_key(self, blank):
        with pytest.raises(ValueError, match="measurement is required"):
            validate({"measurement": blank}, [Validator("measurement", required=True)])

    def test_a_falsy_value_is_a_value(self):
        values = {"rows": 0, "dry_run": False, "excluded": []}
        rule = Validator("rows", "dry_run", "excluded", required=True)
        assert validate(values, [rule]) == values

    def test_an_unvalidated_key_passes_through(self):
        assert validate({"extra": 1}, [Validator("window", default="1h")]) == {
            "extra": 1,
            "window": "1h",
        }

    def test_none_is_a_value_unless_the_rule_says_otherwise(self):
        assert validate({"k": None}, [Validator("k", default="x")]) == {"k": None}
        rule = Validator("k", default="x", apply_default_on_none=True)
        assert validate({"k": None}, [rule]) == {"k": "x"}

    def test_one_rule_can_cover_several_keys(self):
        rule = Validator("start", "end", required=True)
        assert validate({"start": 1, "end": 2}, [rule]) == {"start": 1, "end": 2}
        with pytest.raises(ValueError, match="end is required"):
            validate({"start": 1}, [rule])


class TestChecks:
    @pytest.mark.parametrize(
        "rule, value, complaint",
        [
            (Validator("k", gt=0), 0, "must be greater than 0"),
            (Validator("k", lte=10), 11, "must be at most 10"),
            (Validator("k", ne="x"), "x", "must not equal 'x'"),
            (Validator("k", is_in=("mean", "max")), "median", "must be one of"),
            (Validator("k", is_not_in=("admin",)), "admin", "must not be one of"),
            (Validator("k", is_type_of=int), "5", "must be of type"),
            (Validator("k", len_min=2), "a", "must be at least 2 long"),
            (Validator("k", len_max=2), "abc", "must be at most 2 long"),
            (Validator("k", contains="cpu"), "mem_usage", "must contain 'cpu'"),
            (Validator("k", startswith="cpu"), "mem", "must start with 'cpu'"),
            (Validator("k", endswith="_raw"), "cpu", "must end with '_raw'"),
            (Validator("k", regex=r"^[a-z_]+$"), "CPU!", "must match"),
            (Validator("k", not_regex=r"\s"), "two words", "must not match"),
            (Validator("k", ge=1), 0, "must be at least 1"),
            (Validator("k", lt=10), 10, "must be less than 10"),
            (Validator("k", le=10), 11, "must be at most 10"),
            (Validator("k", identity=None), 0, "must be None itself"),
            (Validator("k", cont="a"), "xyz", "must contain 'a'"),
            (Validator("k", not_contains="a"), "abc", "must not contain 'a'"),
            (Validator("k", len_eq=2), "abc", "must have length 2"),
            (Validator("k", len_ne=3), "abc", "must not have length 3"),
            (Validator("k", not_startswith="a"), "abc", "must not start with 'a'"),
            (Validator("k", not_endswith="c"), "abc", "must not end with 'c'"),
        ],
    )
    def test_a_failing_check_names_the_key_and_the_value(self, rule, value, complaint):
        with pytest.raises(ValueError, match=f"k {complaint}"):
            validate({"k": value}, [rule])

    def test_passing_values_are_returned_unchanged(self):
        rule = Validator("aggregate", is_in=("mean", "max"), len_min=3)
        assert validate({"aggregate": "mean"}, [rule]) == {"aggregate": "mean"}

    @pytest.mark.parametrize(
        "rule, value, check",
        [
            (Validator("k", len_min=2), 5, "len_min"),
            (Validator("k", is_in={"a"}), ["a"], "is_in"),
            (Validator("k", regex="["), "x", "regex"),
            (Validator("k", startswith="a"), 5, "startswith"),
            (Validator("k", condition=len), 5, "condition"),
        ],
    )
    def test_a_predicate_that_cannot_answer_is_a_valueerror(self, rule, value, check):
        """A plugin answers a bad configuration from one ``except`` clause."""
        with pytest.raises(ValueError, match=f"k cannot be checked with {check}"):
            validate({"k": value}, [rule])

    def test_condition_takes_an_arbitrary_predicate(self):
        rule = Validator("port", cast=int, condition=lambda value: value % 2 == 0)
        assert validate({"port": "8086"}, [rule]) == {"port": 8086}
        with pytest.raises(ValueError, match="port is not allowed: 8087"):
            validate({"port": "8087"}, [rule])

    def test_a_misspelled_check_is_refused_where_it_is_written(self):
        with pytest.raises(TypeError):
            Validator("rows", gtee=1)


class TestWhen:
    """A rule that only applies while another one holds."""

    RIPPLE = Validator(
        "ripple",
        required=True,
        gte=0.01,
        lte=80,
        when=Validator("prototype", eq="cheby1"),
    )

    def test_the_rule_applies_once_the_condition_holds(self):
        with pytest.raises(ValueError, match="ripple is required"):
            validate({"prototype": "cheby1"}, [self.RIPPLE])

    def test_the_rule_is_skipped_otherwise(self):
        assert validate({"prototype": "butter"}, [self.RIPPLE]) == {
            "prototype": "butter"
        }

    def test_the_condition_does_not_change_the_values(self):
        rule = Validator("b", default=2, when=Validator("a", default=1, eq=1))
        assert validate({}, [rule]) == {"b": 2}

    def test_a_key_nobody_set_does_not_hold(self):
        assert validate({}, [self.RIPPLE]) == {}

    def test_a_condition_that_cannot_judge_the_value_answers_no(self):
        rule = Validator("ripple", required=True, when=Validator("prototype", cast=len))
        assert validate({"prototype": 5}, [rule]) == {"prototype": 5}


def test_two_rules_on_one_key_apply_in_order():
    """The second rule sees the value the first one cast."""
    rules = [Validator("rows", default="10", cast=int), Validator("rows", lte=5)]
    with pytest.raises(ValueError, match="rows must be at most 5, got 10"):
        validate({}, rules)


def test_a_mutable_default_is_not_shared_between_loads():
    rules = [Validator("excluded", default=[])]
    first = validate({}, rules)
    first["excluded"].append("time")
    assert validate({}, rules) == {"excluded": []}


def test_validate_returns_a_copy():
    values = {"rows": "5"}
    assert validate(values, [Validator("rows", cast=int)]) == {"rows": 5}
    assert values == {"rows": "5"}


def test_no_validators_is_a_plain_copy():
    assert validate({"a": 1}, None) == {"a": 1}