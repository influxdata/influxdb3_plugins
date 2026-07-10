"""Tests for influxdata_plugin_utils.parsing."""

from datetime import datetime, timedelta, timezone

import pytest

from influxdata_plugin_utils.parsing import (
    parse_bool,
    parse_delimited_list,
    parse_int,
    parse_key_value,
    parse_timedelta,
    parse_timestamp_ns,
)


class TestParseTimedelta:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("30s", timedelta(seconds=30)),
            ("5min", timedelta(minutes=5)),
            ("1h", timedelta(hours=1)),
            ("2d", timedelta(days=2)),
            ("1w", timedelta(weeks=1)),
            (" 10 S ", timedelta(seconds=10)),
        ],
    )
    def test_valid_durations(self, raw, expected):
        assert parse_timedelta(raw) == expected

    def test_timedelta_passthrough(self):
        value = timedelta(seconds=1)
        assert parse_timedelta(value) is value

    @pytest.mark.parametrize("raw", ["", "abc", "10", "10 fortnights", "1.5h"])
    def test_invalid_durations(self, raw):
        with pytest.raises(ValueError):
            parse_timedelta(raw)


class TestParseTimestampNs:
    @pytest.mark.parametrize(
        ("value", "fmt", "expected"),
        [
            (1, "s", 1_000_000_000),
            ("1700000000", "s", 1_700_000_000_000_000_000),
            (1_700_000_000_000, "ms", 1_700_000_000_000_000_000),
            (1_700_000_000_000_000, "us", 1_700_000_000_000_000_000),
            (1_700_000_000_123_456_789, "ns", 1_700_000_000_123_456_789),
        ],
    )
    def test_epoch_units(self, value, fmt, expected):
        assert parse_timestamp_ns(value, fmt) == expected

    def test_large_ns_epoch_is_exact(self):
        # would lose precision if routed through float64
        value = 1_700_000_000_123_456_789
        assert parse_timestamp_ns(str(value), "ns") == value

    def test_datetime_string_utc(self):
        assert (
            parse_timestamp_ns("2026-01-01T00:00:00Z", "datetime")
            == int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp()) * 1_000_000_000
        )

    def test_naive_datetime_treated_as_utc(self):
        naive = datetime(2026, 1, 1)
        aware = datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert parse_timestamp_ns(naive, "datetime") == int(aware.timestamp()) * 1_000_000_000

    def test_unknown_format_raises(self):
        with pytest.raises(ValueError):
            parse_timestamp_ns(1, "minutes")

    def test_datetime_format_rejects_non_datetime(self):
        with pytest.raises(ValueError):
            parse_timestamp_ns(12345, "datetime")


class TestParseInt:
    def test_parses_and_strips(self):
        assert parse_int(" 42 ") == 42

    def test_bounds(self):
        assert parse_int("5", minimum=5, maximum=5) == 5
        with pytest.raises(ValueError):
            parse_int("4", minimum=5)
        with pytest.raises(ValueError):
            parse_int("6", maximum=5)

    @pytest.mark.parametrize("raw", ["", "abc", "1.5", None])
    def test_invalid(self, raw):
        with pytest.raises(ValueError):
            parse_int(raw)


class TestParseBool:
    @pytest.mark.parametrize("raw", [True, "true", "T", "1", "yes", "ON"])
    def test_truthy(self, raw):
        assert parse_bool(raw) is True

    @pytest.mark.parametrize("raw", [False, "false", "F", "0", "no", "OFF"])
    def test_falsy(self, raw):
        assert parse_bool(raw) is False

    @pytest.mark.parametrize("raw", ["", "maybe", "2"])
    def test_invalid(self, raw):
        with pytest.raises(ValueError):
            parse_bool(raw)


class TestParseDelimitedList:
    def test_default_space_separator(self):
        assert parse_delimited_list("a  b c ") == ["a", "b", "c"]

    def test_custom_separator(self):
        assert parse_delimited_list("a, b,,c", sep=",") == ["a", "b", "c"]

    def test_list_passthrough_trims_and_drops_empty(self):
        assert parse_delimited_list(["a ", "", 1]) == ["a", "1"]


class TestParseKeyValue:
    def test_parses_pairs(self):
        assert parse_key_value("a=1  b=2") == {"a": "1", "b": "2"}

    def test_custom_separators(self):
        assert parse_key_value("a:1;b:2", pair_sep=";", kv_sep=":") == {"a": "1", "b": "2"}

    def test_value_may_contain_kv_sep(self):
        assert parse_key_value("url=http://x?a=b") == {"url": "http://x?a=b"}

    def test_dict_passthrough_stringifies(self):
        assert parse_key_value({"a": 1}) == {"a": "1"}

    def test_missing_separator_raises(self):
        with pytest.raises(ValueError):
            parse_key_value("a=1 b")
