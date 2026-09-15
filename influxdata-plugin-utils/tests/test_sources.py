"""Tests for influxdata_plugin_utils.sources.

Several of these sources carry caller-supplied data, so the behavior that
matters is that a layer contributes only what the plugin named.
"""

import json

import pytest

from influxdata_plugin_utils.sources import (
    KeySpec,
    parse_env,
    parse_json_body,
    parse_query_parameters,
    parse_request_headers,
    parse_toml,
    parse_trigger_args,
)


class TestKeySpec:
    def test_one_name_is_not_read_as_letters(self):
        assert KeySpec(allowlist="measurement").allowlist == ("measurement",)

    def test_a_key_cannot_be_allowed_and_denied(self):
        with pytest.raises(ValueError, match="both allowed and denied"):
            KeySpec(allowlist=["a", "b"], denylist=["b"])

    def test_two_keys_cannot_be_renamed_onto_one(self):
        with pytest.raises(ValueError, match="maps several keys onto"):
            KeySpec(rename={"x-api-key": "api_key", "x-key": "api_key"})

    def test_renaming_a_key_the_allowlist_refuses_is_a_mistake(self):
        with pytest.raises(ValueError, match="allowlist does not let through"):
            KeySpec(allowlist=["window"], rename={"max-rows": "max_rows"})

    @pytest.mark.parametrize(
        "spec, complaint",
        [
            (dict(denylist=["secret"], rename={"x": "secret"}),
             "lands on \\['secret'\\], which the denylist keeps out"),
            (dict(denylist=["secret"], rename={"secret": "safe"}),
             "names \\['secret'\\], which the denylist keeps out"),
            (dict(allowlist=["a", "b"], rename={"a": "b"}),
             "lands on \\['b'\\], which the allowlist already lets through"),
        ],
    )
    def test_a_rename_cannot_reach_around_the_lists(self, spec, complaint):
        """A refused key must stay refused, whichever end of the rename names it."""
        with pytest.raises(ValueError, match=complaint):
            KeySpec(**spec)

    @pytest.mark.parametrize(
        "spec",
        [
            dict(allowlist=["a", "b"], rename={"a": "b", "b": "c"}),
            dict(allowlist=["a", "b"], rename={"a": "b", "b": "a"}),
            dict(denylist=["other"], rename={"x": "y"}),
        ],
    )
    def test_a_rename_that_frees_its_target_is_allowed(self, spec):
        """The target is only taken while some key keeps that name of its own."""
        assert KeySpec(**spec).rename is not None

    def test_unknown_policy_is_checked(self):
        with pytest.raises(ValueError, match="Invalid unknown"):
            KeySpec(unknown="bogus")


class TestTriggerArgs:
    def test_no_arguments_is_an_empty_layer(self):
        assert parse_trigger_args(None) == {}

    def test_values_keep_their_types_and_blanks_are_dropped(self):
        args = {"measurement": "cpu", "rows": 5, "dry_run": False, "window": "  "}
        assert parse_trigger_args(args) == {
            "measurement": "cpu",
            "rows": 5,
            "dry_run": False,
        }

    def test_a_spec_selects_and_renames(self):
        args = {"measurement": "cpu", "config_file_path": "c.toml"}
        spec = KeySpec(allowlist=["measurement"], rename={"measurement": "table"})
        assert parse_trigger_args(args, spec) == {"table": "cpu"}

    def test_a_misspelled_argument_can_be_refused(self):
        """The operator sets these, so a typo is worth naming rather than dropping."""
        args = {"measurement": "cpu", "meausrement": "cpu"}
        spec = KeySpec(allowlist=["measurement"], unknown="reject")
        with pytest.raises(ValueError, match="Trigger arguments may not set 'meausrement'"):
            parse_trigger_args(args, spec)

    def test_a_denylist_keeps_one_argument_out(self):
        args = {"measurement": "cpu", "config_file_path": "c.toml"}
        assert parse_trigger_args(args, KeySpec(denylist=["config_file_path"])) == {
            "measurement": "cpu"
        }


class TestToml:
    def test_no_path_is_an_empty_layer(self):
        assert parse_toml(None) == {}

    def test_types_are_native_and_blanks_are_dropped(self, tmp_path, monkeypatch):
        config_file = tmp_path / "c.toml"
        config_file.write_text('rows = 5\nfields = ["a"]\nmeasurement = ""\n')
        monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))

        assert parse_toml("c.toml") == {"rows": 5, "fields": ["a"]}

    def test_a_relative_path_resolves_against_the_plugin_directory(self, tmp_path, monkeypatch):
        (tmp_path / "c.toml").write_text('k = "v"\n')
        monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
        assert parse_toml("c.toml") == {"k": "v"}

    def test_a_missing_file_is_reported_by_the_name_the_caller_used(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
        with pytest.raises(ValueError, match="Cannot read config file 'nope.toml'"):
            parse_toml("nope.toml")

    def test_a_spec_selects_from_the_file_as_well(self, tmp_path, monkeypatch):
        (tmp_path / "c.toml").write_text('rows = 5\nsecret = "s"\n')
        monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))

        spec = KeySpec(allowlist=["rows"], rename={"rows": "batch_size"})
        assert parse_toml("c.toml", spec) == {"batch_size": 5}
        with pytest.raises(ValueError, match="Config file may not set 'secret'"):
            parse_toml("c.toml", KeySpec(allowlist=["rows"], unknown="reject"))

    def test_a_broken_file_is_reported_as_such(self, tmp_path, monkeypatch):
        (tmp_path / "broken.toml").write_text("not = valid = toml\n")
        monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
        with pytest.raises(ValueError, match="is not valid TOML"):
            parse_toml("broken.toml")

    def test_a_file_that_is_not_utf8_is_reported_by_name(self, tmp_path, monkeypatch):
        (tmp_path / "binary.toml").write_bytes(b"\xd8\xff\x00binary")
        monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
        with pytest.raises(ValueError, match="Config file 'binary.toml' is not valid TOML"):
            parse_toml("binary.toml")


class TestEnv:
    def test_nothing_is_read_without_an_allowlist(self):
        with pytest.raises(ValueError, match="needs a KeySpec with an allowlist"):
            parse_env(KeySpec())

    def test_only_the_named_variables_are_read(self, monkeypatch):
        monkeypatch.setenv("PLUGIN_TOKEN", "secret")
        monkeypatch.setenv("PLUGIN_OTHER", "ignored")

        assert parse_env(KeySpec(allowlist=["PLUGIN_TOKEN"])) == {
            "PLUGIN_TOKEN": "secret"
        }

    def test_a_variable_becomes_the_config_key_it_is_renamed_to(self, monkeypatch):
        monkeypatch.setenv("AGGREGATE_PLUGIN_API_KEY", "secret")
        spec = KeySpec(
            allowlist=["AGGREGATE_PLUGIN_API_KEY"],
            rename={"AGGREGATE_PLUGIN_API_KEY": "api_key"},
        )
        assert parse_env(spec) == {"api_key": "secret"}

    def test_unset_and_blank_variables_contribute_nothing(self, monkeypatch):
        monkeypatch.delenv("PLUGIN_TOKEN", raising=False)
        monkeypatch.setenv("PLUGIN_BLANK", "  ")
        spec = KeySpec(allowlist=["PLUGIN_TOKEN", "PLUGIN_BLANK"])
        assert parse_env(spec) == {}


class TestJsonBody:
    def test_bytes_text_and_dict_read_alike(self):
        expected = {"measurement": "cpu"}
        payload = json.dumps(expected)
        assert parse_json_body(payload) == expected
        assert parse_json_body(payload.encode()) == expected
        assert parse_json_body(dict(expected)) == expected

    def test_json_types_survive_and_empty_values_do_not(self):
        body = json.dumps(
            {"rows": 5, "fields": ["a"], "dry_run": False, "window": None, "table": " "}
        )
        assert parse_json_body(body) == {"rows": 5, "fields": ["a"], "dry_run": False}

    def test_nested_values_are_passed_through_untouched(self):
        body = json.dumps({"opts": {"x": "", "y": None}})
        assert parse_json_body(body) == {"opts": {"x": "", "y": None}}

    def test_an_empty_body_is_an_empty_layer(self):
        assert parse_json_body(None) == {}
        assert parse_json_body(b"") == {}
        assert parse_json_body("  ") == {}

    def test_keys_are_matched_and_kept_as_written(self):
        body = json.dumps({"Measurement": "cpu", "target_database": "x"})
        spec = KeySpec(allowlist=["Measurement"])
        assert parse_json_body(body, spec) == {"Measurement": "cpu"}

    def test_a_refused_key_is_dropped_or_named(self):
        body = json.dumps({"target_database": "x", "measurement": "cpu"})
        assert parse_json_body(body, KeySpec(allowlist=["measurement"])) == {
            "measurement": "cpu"
        }
        with pytest.raises(ValueError, match="Request body may not set 'target_database'"):
            parse_json_body(
                body, KeySpec(allowlist=["measurement"], unknown="reject")
            )

    def test_renaming_onto_a_key_the_body_already_carries_is_refused(self):
        """Without an allowlist the clash is only visible once the body arrives."""
        body = json.dumps({"table": "renamed", "measurement": "sent"})
        spec = KeySpec(rename={"table": "measurement"})
        with pytest.raises(ValueError, match="same config key 'measurement'"):
            parse_json_body(body, spec)

    def test_the_rejection_error_stays_bounded(self):
        """The message is built from the caller's own keys and goes back to them."""
        body = json.dumps({f"key{index}" * 40: 1 for index in range(500)})
        spec = KeySpec(allowlist=["measurement"], unknown="reject")
        with pytest.raises(ValueError) as raised:
            parse_json_body(body, spec)

        message = str(raised.value)
        assert "and 490 more" in message
        assert len(message) < 1000

    def test_a_body_with_a_byte_order_mark_is_accepted(self):
        """.NET and PowerShell clients prefix a BOM."""
        assert parse_json_body('\ufeff{"measurement": "cpu"}') == {"measurement": "cpu"}
        assert parse_json_body(b"\xef\xbb\xbf" + b'{"measurement": "cpu"}') == {
            "measurement": "cpu"
        }

    def test_an_oversized_body_is_refused_before_parsing(self):
        """Unparsable on purpose: parsing first would report the JSON error."""
        with pytest.raises(ValueError, match="over the 10 byte limit"):
            parse_json_body(b"{not json" + b" " * 50, max_bytes=10)

    def test_a_deeply_nested_body_stays_within_the_valueerror_contract(self):
        with pytest.raises(ValueError, match="nested too deeply"):
            parse_json_body(b'{"a": ' + b"[" * 20000 + b"]" * 20000 + b"}")

    @pytest.mark.parametrize(
        "body, complaint",
        [
            ("{not json", "not valid JSON"),
            ("[1, 2]", "must be a JSON object"),
            (b"\xff\xfe", "not valid UTF-8"),
            (42, "must be JSON text"),
            ([], "must be JSON text"),
            (False, "must be JSON text"),
        ],
    )
    def test_undecodable_bodies_are_refused(self, body, complaint):
        with pytest.raises(ValueError, match=complaint):
            parse_json_body(body)


class TestRequestHeaders:
    def test_only_the_casing_of_a_name_is_folded(self):
        """RFC 9110 makes casing meaningless; an underscore is a different header."""
        headers = {"X-Api-Key": "secret"}
        for spelling in ("X-Api-Key", "x-api-key", "X-API-KEY"):
            assert parse_request_headers(headers, KeySpec(allowlist=[spelling])) == {
                "x-api-key": "secret"
            }
        assert parse_request_headers(headers, KeySpec(allowlist=["x_api_key"])) == {}

    def test_a_hyphen_and_an_underscore_are_separate_headers(self):
        headers = [("x-api-key", "from-gateway"), ("x_api_key", "from-client")]
        assert parse_request_headers(headers) == {
            "x-api-key": "from-gateway",
            "x_api_key": "from-client",
        }

    def test_every_header_is_read_without_a_spec(self):
        """Including the ones a client sends on its own, which is why you name them."""
        headers = {"host": "localhost", "User-Agent": "curl/8.5.0", "X-Api-Key": "s"}
        assert parse_request_headers(headers) == {
            "host": "localhost",
            "user-agent": "curl/8.5.0",
            "x-api-key": "s",
        }

    def test_a_spec_selects_and_renames(self):
        headers = {"source-token": "t", "user-agent": "curl/8.5.0"}
        spec = KeySpec(allowlist=["source-token"], rename={"source-token": "source_token"})
        assert parse_request_headers(headers, spec) == {"source_token": "t"}

    def test_rejecting_the_rest_turns_away_an_ordinary_request(self):
        headers = {"host": "localhost", "X-Api-Key": "s"}
        spec = KeySpec(allowlist=["x-api-key"], unknown="reject")
        with pytest.raises(ValueError, match="Request headers may not set 'host'"):
            parse_request_headers(headers, spec)

    def test_an_empty_value_counts_as_not_provided(self):
        assert parse_request_headers({"X-Api-Key": "  "}) == {}

    def test_a_repeated_header_is_refused_unless_every_value_is_asked_for(self):
        """Which value the plugin would get is otherwise the runtime's dict order."""
        headers = [("accept", "a"), ("x-api-key", "secret"), ("accept", "b")]
        spec = KeySpec(allowlist=["accept"])

        with pytest.raises(ValueError, match="'accept' is set more than once"):
            parse_request_headers(headers, spec)
        assert parse_request_headers(headers, spec, multi=True) == {
            "accept": ["a", "b"]
        }

    def test_two_spellings_of_one_name_are_one_repeated_header(self):
        """Casing carries no meaning, so these are one header sent twice."""
        headers = [("X-Api-Key", "from-gateway"), ("X-API-KEY", "from-client")]
        spec = KeySpec(allowlist=["x-api-key"])

        with pytest.raises(ValueError, match="'x-api-key' is set more than once"):
            parse_request_headers(headers, spec)
        assert parse_request_headers(headers, spec, multi=True) == {
            "x-api-key": ["from-gateway", "from-client"]
        }

    def test_two_names_renamed_onto_one_config_key_are_refused(self):
        headers = {"X-Api-Key": "from-header", "api_key": "already-there"}
        spec = KeySpec(rename={"x-api-key": "api_key"})
        with pytest.raises(ValueError, match="same config key 'api_key'"):
            parse_request_headers(headers, spec)

    @pytest.mark.parametrize(
        "headers", ["x-api-key: secret", 42, ["x-api-key"], [("a", "b", "c")]]
    )
    def test_another_shape_stays_within_the_valueerror_contract(self, headers):
        with pytest.raises(ValueError, match="must be a mapping or a sequence"):
            parse_request_headers(headers)

    def test_an_error_from_the_runtime_is_not_reported_as_a_shape_error(self):
        def headers():
            yield ("x-api-key", "secret")
            raise ValueError("decode failed halfway")

        with pytest.raises(ValueError, match="decode failed halfway"):
            parse_request_headers(headers())


class TestQueryParameters:
    def test_names_are_matched_and_kept_as_written(self):
        params = {"max-rows": "5", "action": "start"}
        assert parse_query_parameters(params, KeySpec(allowlist=["max-rows"])) == {
            "max-rows": "5"
        }
        spec = KeySpec(allowlist=["max-rows"], rename={"max-rows": "max_rows"})
        assert parse_query_parameters(params, spec) == {"max_rows": "5"}

    def test_an_empty_value_counts_as_not_provided(self):
        assert parse_query_parameters({"window": "", "table": "cpu"}) == {"table": "cpu"}

    def test_a_routing_parameter_can_be_refused_or_ignored(self):
        params = {"action": "start", "table": "cpu"}
        assert parse_query_parameters(params, KeySpec(allowlist=["table"])) == {
            "table": "cpu"
        }
        spec = KeySpec(allowlist=["table"], unknown="reject")
        with pytest.raises(ValueError, match="Query parameters may not set 'action'"):
            parse_query_parameters(params, spec)

    def test_a_repeated_parameter_is_refused_unless_every_value_is_asked_for(self):
        """Which value the plugin would get is otherwise the runtime's dict order."""
        params = [("table", "cpu"), ("table", "mem")]

        with pytest.raises(ValueError, match="'table' is set more than once"):
            parse_query_parameters(params)
        assert parse_query_parameters(params, multi=True) == {"table": ["cpu", "mem"]}