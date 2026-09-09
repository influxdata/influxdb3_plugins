"""Tests for influxdata_plugin_utils.request.

The layers parsed here carry caller-supplied data, so the security-critical
behavior is that a layer contributes only what the plugin named.
"""

import json

import pytest

from influxdata_plugin_utils.request import (
    parse_json_body,
    parse_query_parameters,
    parse_request_headers,
)


class TestParseJsonBody:
    def test_decodes_bytes_str_and_dict_alike(self):
        expected = {"measurement": "cpu"}
        payload = json.dumps(expected)
        assert parse_json_body(payload) == expected
        assert parse_json_body(payload.encode()) == expected
        assert parse_json_body(dict(expected)) == expected

    def test_keeps_json_types_and_drops_empty_values(self):
        body = json.dumps(
            {"rows": 5, "fields": ["a"], "dry_run": False, "window": None, "table": " "}
        )
        assert parse_json_body(body) == {
            "rows": 5,
            "fields": ["a"],
            "dry_run": False,
        }

    def test_nested_values_are_passed_through_untouched(self):
        body = json.dumps({"opts": {"x": "", "y": None}})
        assert parse_json_body(body) == {"opts": {"x": "", "y": None}}

    def test_empty_body_is_empty_config(self):
        assert parse_json_body(None) == {}
        assert parse_json_body(b"") == {}
        assert parse_json_body("  ") == {}

    def test_names_restricts_and_renames(self):
        body = json.dumps({"measurement": "cpu"})
        assert parse_json_body(body, "measurement") == {"measurement": "cpu"}
        assert parse_json_body(body, {"measurement": "table"}) == {"table": "cpu"}

    def test_names_are_kept_exactly_as_written(self):
        """Only header names are normalized; the author writes these ones."""
        body = json.dumps({"Measurement": "cpu"})
        assert parse_json_body(body, ["Measurement"]) == {"Measurement": "cpu"}

    def test_unknown_key_is_dropped_by_default(self):
        body = json.dumps({"target_database": "x", "measurement": "cpu"})
        assert parse_json_body(body, ["measurement"]) == {"measurement": "cpu"}

    def test_unknown_key_can_be_named_in_an_error_instead(self):
        with pytest.raises(ValueError, match="target_database"):
            parse_json_body(
                json.dumps({"target_database": "x"}),
                ["measurement"],
                unknown="reject",
            )

    def test_the_rejection_error_stays_bounded(self):
        """The message is built from the caller's own keys and goes back to them."""
        body = json.dumps({f"key{index}" * 40: 1 for index in range(500)})
        with pytest.raises(ValueError) as raised:
            parse_json_body(body, ["measurement"], unknown="reject")

        message = str(raised.value)
        assert "and 490 more" in message
        assert len(message) < 1000

    def test_deeply_nested_body_stays_within_the_valueerror_contract(self):
        """The contract is ValueError; RecursionError would escape the plugin."""
        with pytest.raises(ValueError, match="nested too deeply"):
            parse_json_body(b'{"a": ' + b"[" * 20000 + b"]" * 20000 + b"}")

    def test_oversized_body_is_refused_before_parsing(self):
        """Unparsable on purpose: parsing first would report the JSON error."""
        with pytest.raises(ValueError, match="over the 10 byte limit"):
            parse_json_body(b"{not json" + b" " * 50, max_bytes=10)

    def test_body_with_a_byte_order_mark_is_accepted(self):
        """.NET and PowerShell clients prefix a BOM."""
        assert parse_json_body('\ufeff{"measurement": "cpu"}') == {"measurement": "cpu"}
        assert parse_json_body(b"\xef\xbb\xbf" + b'{"measurement": "cpu"}') == {
            "measurement": "cpu"
        }

    @pytest.mark.parametrize(
        "body, message",
        [
            ("{not json", "not valid JSON"),
            ("[1, 2]", "must be a JSON object"),
            (b"\xff\xfe", "not valid UTF-8"),
            (42, "must be JSON text"),
            ([], "must be JSON text"),
            (False, "must be JSON text"),
        ],
    )
    def test_undecodable_bodies_are_rejected(self, body, message):
        with pytest.raises(ValueError, match=message):
            parse_json_body(body)


class TestParseRequestHeaders:
    def test_matches_regardless_of_casing_and_hyphenation(self):
        headers = {"X-Api-Key": "secret"}
        assert parse_request_headers(headers, "X-Api-Key") == {"x_api_key": "secret"}
        assert parse_request_headers(headers, ["x_api_key"]) == {"x_api_key": "secret"}

    def test_renames_hyphenated_header_to_a_config_key(self):
        assert parse_request_headers({"source-token": "t"}, {"source-token": "source_token"}) == {
            "source_token": "t"
        }

    def test_unnamed_headers_are_dropped(self):
        headers = {"user-agent": "curl/8.5.0", "host": "localhost", "X-Api-Key": "secret"}
        assert parse_request_headers(headers, ["x-api-key"]) == {"x_api_key": "secret"}

    def test_names_none_reads_every_header(self):
        """Including the ones a client sends on its own, which is why you name them."""
        headers = {"host": "localhost", "user-agent": "curl/8.5.0", "X-Api-Key": "s"}
        assert parse_request_headers(headers) == {
            "host": "localhost",
            "user_agent": "curl/8.5.0",
            "x_api_key": "s",
        }

    def test_unknown_reject_names_the_unnamed_headers(self):
        """Which is every ordinary request, so it only suits a closed set."""
        headers = {"host": "localhost", "X-Api-Key": "s"}
        with pytest.raises(ValueError, match="Request headers may not set 'host'"):
            parse_request_headers(headers, ["x-api-key"], unknown="reject")

        assert parse_request_headers(headers, ["host", "x-api-key"], unknown="reject") == {
            "host": "localhost",
            "x_api_key": "s",
        }

    def test_invalid_unknown_policy_is_rejected(self):
        with pytest.raises(ValueError, match="Invalid unknown"):
            parse_request_headers({}, ["x-api-key"], unknown="bogus")

    def test_empty_value_counts_as_not_provided(self):
        assert parse_request_headers({"X-Api-Key": "  "}, ["x-api-key"]) == {}

    def test_repeated_header_takes_the_first_or_all(self):
        headers = {"Accept": ["a", "b"]}
        assert parse_request_headers(headers, ["accept"]) == {"accept": "a"}
        assert parse_request_headers(headers, ["accept"], multi=True) == {
            "accept": ["a", "b"]
        }

    def test_a_sequence_of_pairs_is_read_like_a_mapping(self):
        """ASGI and byte-level runtimes deliver headers as pairs, repeats included."""
        headers = [(b"accept", b"a"), (b"x-api-key", b"secret"), (b"accept", b"b")]
        assert parse_request_headers(headers, ["x-api-key"]) == {"x_api_key": "secret"}
        assert parse_request_headers(headers, ["accept"], multi=True) == {
            "accept": ["a", "b"]
        }

    @pytest.mark.parametrize(
        "headers", ["x-api-key: secret", 42, ["x-api-key"], [("a", "b", "c")]]
    )
    def test_another_shape_stays_within_the_valueerror_contract(self, headers):
        """The contract is ValueError; AttributeError would escape the plugin."""
        with pytest.raises(ValueError, match="must be a mapping or a sequence"):
            parse_request_headers(headers, ["x-api-key"])

    def test_an_error_from_the_runtime_is_not_reported_as_a_shape_error(self):
        def headers():
            yield ("x-api-key", "secret")
            raise ValueError("decode failed halfway")

        with pytest.raises(ValueError, match="decode failed halfway"):
            parse_request_headers(headers(), ["x-api-key"])

    def test_two_spellings_of_one_header_are_refused(self):
        """Both reach the plugin; the winner would be the runtime's dict order."""
        headers = {"x-api-key": "from-gateway", "x_api_key": "from-client"}
        with pytest.raises(ValueError, match="same config key 'x_api_key'"):
            parse_request_headers(headers, ["x-api-key"])

    def test_a_repeated_header_is_not_a_collision(self):
        headers = [("accept", "a"), ("accept", "b")]
        assert parse_request_headers(headers, ["accept"], multi=True) == {
            "accept": ["a", "b"]
        }

    def test_a_bytearray_name_is_one_name(self):
        assert parse_request_headers({"X-Api-Key": "s"}, bytearray(b"x-api-key")) == {
            "x_api_key": "s"
        }

    def test_names_colliding_on_one_config_key_is_rejected(self):
        """Two different headers cannot both land on ``api_key``."""
        with pytest.raises(ValueError, match="more than one source onto 'api_key'"):
            parse_request_headers({}, {"x-api-key": "api_key", "x-key": "api_key"})

    def test_names_looking_up_one_header_twice_is_rejected(self):
        """Otherwise the second spelling wins and the first config key never appears."""
        with pytest.raises(ValueError, match="more than once"):
            parse_request_headers({}, {"X-Api-Key": "gateway_key", "x_api_key": "fallback_key"})

    def test_byte_names_and_values_are_decoded(self):
        assert parse_request_headers({b"X-Api-Key": b"secret"}, ["x-api-key"]) == {
            "x_api_key": "secret"
        }


class TestParseQueryParameters:
    def test_routing_parameters_do_not_reach_the_config(self):
        params = {"action": "start", "measurement": "cpu"}
        assert parse_query_parameters(params, ["measurement"]) == {"measurement": "cpu"}

    def test_names_are_kept_exactly_as_written(self):
        assert parse_query_parameters({"max-rows": "5"}, ["max-rows"]) == {"max-rows": "5"}
        assert parse_query_parameters({"max-rows": "5"}, {"max-rows": "max_rows"}) == {
            "max_rows": "5"
        }

    def test_empty_value_counts_as_not_provided(self):
        assert parse_query_parameters({"window": ""}, ["window"]) == {}

    def test_unknown_parameter_can_be_rejected(self):
        with pytest.raises(ValueError, match="action"):
            parse_query_parameters(
                {"action": "start"}, ["measurement"], unknown="reject"
            )

    def test_a_repeated_parameter_is_collected_from_pairs(self):
        params = [("table", "cpu"), ("table", "mem")]
        assert parse_query_parameters(params, ["table"]) == {"table": "cpu"}
        assert parse_query_parameters(params, ["table"], multi=True) == {
            "table": ["cpu", "mem"]
        }
