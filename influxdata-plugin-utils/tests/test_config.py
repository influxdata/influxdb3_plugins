"""Tests for influxdata_plugin_utils.config."""

import pytest

from influxdata_plugin_utils.config import (
    Config,
    load_config,
    load_plugin_config,
    merge_config_layers,
)
from influxdata_plugin_utils.sources import KeySpec, parse_json_body, parse_trigger_args
from influxdata_plugin_utils.validation import Validator


class TestConfig:
    def test_values_are_reachable_as_keys_and_as_attributes(self):
        cfg = Config({"measurement": "cpu", "rows": 5})
        assert cfg["measurement"] == "cpu"
        assert cfg.rows == 5
        assert "rows" in cfg
        assert cfg.get("missing", "fallback") == "fallback"
        assert cfg.as_dict() == {"measurement": "cpu", "rows": 5}

    def test_a_missing_attribute_says_so(self):
        with pytest.raises(AttributeError, match="window"):
            Config({}).window


class TestMergeConfigLayers:
    def test_later_layers_win(self):
        merged = merge_config_layers({"a": 1, "b": 1}, {"b": 2}, {"b": 3, "c": 4})
        assert merged == {"a": 1, "b": 3, "c": 4}

    def test_absent_layers_need_no_branching(self):
        assert merge_config_layers(None, {"a": 1}, {}) == {"a": 1}

    def test_empty_values_are_left_out_and_real_ones_are_kept(self):
        merged = merge_config_layers(
            {"measurement": "", "window": None, "rows": 0},
            {"window": "  ", "dry_run": False, "excluded": []},
        )
        assert merged == {"rows": 0, "dry_run": False, "excluded": []}

    def test_a_pinned_key_resists_the_layers_above_it(self):
        with pytest.raises(ValueError, match=r"Cannot override pinned keys \['measurement'\]"):
            merge_config_layers(
                {"measurement": "cpu"}, {"measurement": "mem"}, pinned=["measurement"]
            )

    def test_a_pinned_key_nobody_set_stays_open(self):
        merged = merge_config_layers({}, {"measurement": "cpu"}, pinned=["measurement"])
        assert merged == {"measurement": "cpu"}

    def test_a_conflict_can_be_ignored_instead_of_raising(self):
        merged = merge_config_layers(
            {"measurement": "cpu"},
            {"measurement": "mem", "window": "1h"},
            pinned=["measurement"],
            on_conflict="ignore",
        )
        assert merged == {"measurement": "cpu", "window": "1h"}

    def test_pinning_is_expressible_from_what_an_earlier_layer_set(self):
        """The recipe for "the caller may fill in what the operator left open"."""
        args = {"measurement": "cpu"}
        body = {"measurement": "mem", "window": "1h"}
        spec = KeySpec(denylist=[key for key in ("measurement", "window") if key in args])
        merged = merge_config_layers(args, parse_json_body(body, spec))
        assert merged == {"measurement": "cpu", "window": "1h"}

    def test_pinned_as_a_bare_string_is_refused(self):
        with pytest.raises(ValueError, match="must be a list"):
            merge_config_layers({"a": 1}, pinned="a")

    def test_an_unknown_conflict_policy_is_refused(self):
        with pytest.raises(ValueError, match="on_conflict"):
            merge_config_layers({}, on_conflict="bogus")


class TestLoadConfig:
    def test_layers_merge_in_order_and_validators_apply(self):
        cfg = load_config(
            {"window": "1h", "rows": "10"},
            {"rows": "20"},
            validators=[
                Validator("rows", cast=int, lte=100),
                Validator("aggregate", default="mean"),
            ],
        )
        assert cfg == {"window": "1h", "rows": 20, "aggregate": "mean"}

    def test_a_rejected_value_names_the_key(self):
        with pytest.raises(ValueError, match="rows must be at least 1"):
            load_config({"rows": "0"}, validators=[Validator("rows", cast=int, gte=1)])

    def test_values_are_stored_as_they_arrive(self):
        """Nothing in a layer is interpreted: a value is data, whatever it spells."""
        cfg = load_config({"motd": "@read_file /etc/passwd", "path": "${HOME}"})
        assert cfg["motd"] == "@read_file /etc/passwd"
        assert cfg["path"] == "${HOME}"

    def test_no_layers_is_an_empty_configuration(self):
        assert load_config() == {}


class TestLoadPluginConfig:
    def test_the_file_wins_over_arguments_and_the_environment(self, tmp_path, monkeypatch):
        config_file = tmp_path / "config.toml"
        config_file.write_text('k = "from_toml"\n')
        monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
        monkeypatch.setenv("K", "from_env")

        cfg = load_plugin_config(
            {"k": "from_args", "config_file_path": "config.toml"},
            env_keys=["K"],
            source="merge",
        )
        assert cfg["k"] == "from_toml"

    def test_source_args_ignores_the_file(self, tmp_path, monkeypatch):
        (tmp_path / "config.toml").write_text('k = "from_toml"\n')
        monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))

        cfg = load_plugin_config(
            {"k": "from_args", "config_file_path": "config.toml"}, source="args"
        )
        assert cfg["k"] == "from_args"

    def test_source_toml_ignores_the_arguments(self, tmp_path, monkeypatch):
        (tmp_path / "config.toml").write_text('k = "from_toml"\n')
        monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))

        cfg = load_plugin_config(
            {"k": "from_args", "config_file_path": "config.toml"}, source="toml"
        )
        assert cfg.as_dict() == {"k": "from_toml"}

    def test_an_environment_variable_becomes_a_lower_case_key(self, monkeypatch):
        monkeypatch.setenv("INFLUXDB3_AUTH_TOKEN", "secret")
        cfg = load_plugin_config({}, env_keys=["INFLUXDB3_AUTH_TOKEN"], source="args")
        assert cfg["influxdb3_auth_token"] == "secret"

    def test_nothing_is_read_from_the_environment_by_default(self, monkeypatch):
        monkeypatch.setenv("INFLUXDB3_AUTH_TOKEN", "secret")
        assert load_plugin_config({"k": "v"}, source="args").as_dict() == {"k": "v"}

    def test_the_file_path_argument_stays_out_of_the_configuration(self, tmp_path, monkeypatch):
        (tmp_path / "config.toml").write_text('k = "v"\n')
        monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))

        cfg = load_plugin_config({"config_file_path": "config.toml"}, source="merge")
        assert cfg.as_dict() == {"k": "v"}

    def test_an_empty_argument_leaves_the_layer_below_alone(self, monkeypatch):
        monkeypatch.setenv("MY_TOKEN", "real-token")
        cfg = load_plugin_config({"my_token": ""}, env_keys=["MY_TOKEN"], source="args")
        assert cfg["my_token"] == "real-token"

    def test_an_empty_file_value_leaves_the_argument_alone(self, tmp_path, monkeypatch):
        (tmp_path / "config.toml").write_text('k = ""\n')
        monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))

        cfg = load_plugin_config(
            {"k": "from_args", "config_file_path": "config.toml"}, source="merge"
        )
        assert cfg["k"] == "from_args"

    def test_validators_apply_to_the_merged_values(self):
        cfg = load_plugin_config(
            {"port": "8086"},
            validators=[Validator("port", cast=int), Validator("window", default="1h")],
            source="args",
        )
        assert cfg["port"] == 8086 and cfg["window"] == "1h"

    def test_a_missing_file_is_reported_as_a_value_error(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
        with pytest.raises(ValueError, match="Cannot read config file 'nope.toml'"):
            load_plugin_config({"config_file_path": "nope.toml"}, source="toml")

    def test_an_unknown_source_is_refused(self):
        with pytest.raises(ValueError, match="Invalid source"):
            load_plugin_config({}, source="bogus")

    def test_a_trigger_without_arguments_loads(self):
        assert load_plugin_config(None, source="args").as_dict() == {}


def test_a_plugin_composes_the_layers_it_needs():
    """The shape a request plugin uses: arguments under the request."""
    cfg = load_config(
        parse_trigger_args({"measurement": "cpu", "window": ""}),
        parse_json_body('{"window": "15min"}', KeySpec(allowlist=["window"])),
        validators=[Validator("measurement", required=True), Validator("window", required=True)],
    )
    assert cfg.as_dict() == {"measurement": "cpu", "window": "15min"}