"""Tests for influxdata_plugin_utils.config.

The security-critical behavior is that ``load_plugin_config`` never evaluates
dynaconf's ``@`` substitution tokens, so a value beginning with ``@`` cannot
read the server's filesystem or environment. See
https://github.com/influxdata/influxdb3_plugins/issues/134.
"""

import pytest

from influxdata_plugin_utils.config import (
    _DYNACONF_GUARDS,
    _is_dynaconf_option,
    Validator,
    load_plugin_config,
    merge_config_layers,
)


class TestTokenSubstitutionDisabled:
    """A value that begins with ``@`` must be stored verbatim, not evaluated."""

    def test_read_file_token_is_literal(self, tmp_path, monkeypatch):
        secret = tmp_path / "secret.txt"
        secret.write_text("TOP-SECRET-FILE-CONTENTS")
        monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))

        cfg = load_plugin_config(
            {"source_measurement": f"@read_file {secret}"}, source="args"
        )
        assert cfg.get("source_measurement") == f"@read_file {secret}"

    def test_format_env_token_is_literal(self, monkeypatch):
        monkeypatch.setenv("LEAK_ME", "TOP-SECRET-ENV-VALUE")

        cfg = load_plugin_config(
            {"source_measurement": "@format {env[LEAK_ME]}"}, source="args"
        )
        assert cfg.get("source_measurement") == "@format {env[LEAK_ME]}"

    def test_nested_token_is_literal(self):
        cfg = load_plugin_config(
            {"opts": {"inner": "@format {env[HOME]}"}}, source="args"
        )
        assert cfg.get("opts")["inner"] == "@format {env[HOME]}"

    def test_token_in_toml_file_is_literal(self, tmp_path, monkeypatch):
        secret = tmp_path / "secret.txt"
        secret.write_text("TOP-SECRET-FILE-CONTENTS")
        config_file = tmp_path / "config.toml"
        config_file.write_text(f'motd = "@read_file {secret}"\n')
        monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))

        cfg = load_plugin_config(
            {"config_file_path": str(config_file)}, source="toml"
        )
        assert cfg.get("motd") == f"@read_file {secret}"

    def test_token_from_env_layer_is_literal(self, monkeypatch):
        monkeypatch.setenv("MY_SETTING", "@format {env[HOME]}")
        cfg = load_plugin_config({}, env_keys=["MY_SETTING"], source="args")
        assert cfg.get("my_setting") == "@format {env[HOME]}"

    @pytest.mark.parametrize(
        "value",
        ["cpu", "host@example", "a@b", "@", "email@host.com"],
    )
    def test_ordinary_values_unchanged(self, value):
        cfg = load_plugin_config({"k": value}, source="args")
        assert cfg.get("k") == value


class TestValidationStillWorks:
    """Casting, defaults and required keys work, and a rejection is a ValueError."""

    def test_cast_is_applied(self):
        cfg = load_plugin_config(
            {"port": "8086", "ratio": "0.5"},
            validators=[
                Validator("port", cast=int),
                Validator("ratio", cast=float),
            ],
            source="args",
        )
        assert cfg.get("port") == 8086 and isinstance(cfg.get("port"), int)
        assert cfg.get("ratio") == 0.5 and isinstance(cfg.get("ratio"), float)

    def test_default_is_applied(self):
        cfg = load_plugin_config(
            {}, validators=[Validator("window", default="30d")], source="args"
        )
        assert cfg.get("window") == "30d"

    def test_required_missing_raises(self):
        """ValueError, not dynaconf's ValidationError."""
        with pytest.raises(ValueError, match="must is required"):
            load_plugin_config(
                {}, validators=[Validator("must", must_exist=True)], source="args"
            )

    def test_bound_rejection_raises(self):
        with pytest.raises(ValueError, match="rows must gte 1"):
            load_plugin_config(
                {"rows": 0}, validators=[Validator("rows", gte=1)], source="args"
            )

    def test_unreadable_config_file_raises(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
        with pytest.raises(ValueError, match="Cannot read config file 'missing.toml'"):
            load_plugin_config({"config_file_path": "missing.toml"}, source="toml")


class TestLayerMerge:
    """Sanity: precedence env < args < TOML is preserved."""

    def test_toml_overrides_args_and_env(self, tmp_path, monkeypatch):
        config_file = tmp_path / "config.toml"
        config_file.write_text('k = "from_toml"\n')
        monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
        monkeypatch.setenv("K", "from_env")

        cfg = load_plugin_config(
            {"k": "from_args", "config_file_path": str(config_file)},
            env_keys=["K"],
            source="merge",
        )
        assert cfg.get("k") == "from_toml"

    def test_layer_order_holds_when_layers_spell_a_key_differently(self, tmp_path, monkeypatch):
        """The store folds both spellings into one key, so TOML must still win."""
        config_file = tmp_path / "config.toml"
        config_file.write_text('my_token = "from_toml"\n')
        monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
        monkeypatch.setenv("MY_TOKEN", "from_env")

        cfg = load_plugin_config(
            {"MY_TOKEN": "from_args", "config_file_path": str(config_file)},
            env_keys=["MY_TOKEN"],
            source="merge",
        )
        assert cfg.get("my_token") == "from_toml"

    def test_invalid_source_rejected(self):
        with pytest.raises(ValueError):
            load_plugin_config({}, source="bogus")

    def test_blank_layer_value_lets_the_validator_default_apply(self):
        """Same rule as merge_config_layers, so both entry points agree."""
        cfg = load_plugin_config(
            {"window": ""}, validators=[Validator("window", default="5min")], source="args"
        )
        assert cfg.get("window") == "5min"

    def test_blank_arg_does_not_erase_the_env_layer(self, monkeypatch):
        """Blanks are dropped per layer, so a blank arg sets nothing."""
        monkeypatch.setenv("MY_TOKEN", "real-token")

        cfg = load_plugin_config(
            {"my_token": ""}, env_keys=["MY_TOKEN"], source="args"
        )
        assert cfg.get("my_token") == "real-token"

    def test_blank_toml_value_does_not_erase_an_arg(self, tmp_path, monkeypatch):
        config_file = tmp_path / "config.toml"
        config_file.write_text('k = ""\n')
        monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))

        cfg = load_plugin_config(
            {"k": "from_args", "config_file_path": str(config_file)}, source="merge"
        )
        assert cfg.get("k") == "from_args"


class TestDynaconfOptionsCannotBeSetByALayer:
    """A layer must not reach dynaconf's own options, however it spells them."""

    @pytest.mark.parametrize(
        "key",
        [
            "AUTO_CAST_FOR_DYNACONF",
            "auto_cast_for_dynaconf",
            "AUTO_CAST_FOR_DYNACONF ",
            " AUTO_CAST_FOR_DYNACONF",
        ],
    )
    def test_layer_key_does_not_re_enable_token_substitution(self, key, monkeypatch):
        """The settings store trims and folds the key, so the filter must too."""
        monkeypatch.setenv("LEAK_ME", "TOP-SECRET-ENV-VALUE")

        cfg = load_plugin_config({key: True, "k": "@format {env[LEAK_ME]}"}, source="args")
        assert cfg.get("k") == "@format {env[LEAK_ME]}"

    def test_process_environment_does_not_re_enable_token_substitution(self, monkeypatch):
        """dynaconf reads its options from the environment, winning over the kwarg."""
        monkeypatch.setenv("AUTO_CAST_FOR_DYNACONF", "true")
        monkeypatch.setenv("LEAK_ME", "TOP-SECRET-ENV-VALUE")

        cfg = load_plugin_config({"k": "@format {env[LEAK_ME]}"}, source="args")
        assert cfg.get("k") == "@format {env[LEAK_ME]}"

    @pytest.mark.parametrize("guard, value", _DYNACONF_GUARDS.items())
    def test_process_environment_does_not_flip_a_guard(self, guard, value, monkeypatch):
        monkeypatch.setenv(guard, "true")
        assert load_plugin_config({}, source="args").get(guard) == value

    @pytest.mark.parametrize("key", ["dynaconf_include", "default_settings_paths"])
    def test_option_key_without_the_suffix_does_not_load_a_foreign_file(
        self, key, tmp_path
    ):
        """These reach the loaders on a fresh read and on a reload."""
        foreign = tmp_path / "foreign.toml"
        foreign.write_text('injected = "yes"\n')

        cfg = load_plugin_config({key: [str(foreign)]}, source="args")
        cfg.reload()
        assert cfg.get("injected", fresh=True) is None

    def test_option_key_without_the_suffix_cannot_break_nested_access(self):
        """dynaboxify=False would turn every nested table into a plain dict."""
        cfg = load_plugin_config({"dynaboxify": False, "opts": {"key": "v"}}, source="args")
        assert cfg.opts.key == "v"

    def test_the_option_filter_agrees_with_dynaconf_itself(self):
        """A dynaconf upgrade adding an option must fail here, not in production."""
        from dynaconf import default_settings
        from dynaconf.utils import RENAMED_VARS

        known = {name for name in dir(default_settings) if name.isupper()}
        known |= set(RENAMED_VARS)
        # plain names dynaconf reads back from the store
        known |= {"DEFAULT_SETTINGS_PATHS", "DYNABOXIFY", "DYNACONF_INCLUDE"}
        assert sorted(name for name in known if not _is_dynaconf_option(name)) == []

    def test_dotted_key_does_not_write_through_another_key(self):
        cfg = load_plugin_config(
            {"measurement": "safe", "measurement.sub": "evil"}, source="args"
        )
        assert cfg.get("measurement") == "safe"
        assert cfg.get("measurement.sub") == "evil"


class TestMergeConfigLayers:
    """Later layers win, and only an explicitly pinned key resists them."""

    def test_later_overlay_wins_and_base_is_kept(self):
        merged = merge_config_layers({"a": 1, "b": 1}, {"b": 2}, {"b": 3, "c": 4})
        assert merged == {"a": 1, "b": 3, "c": 4}

    def test_missing_layers_are_treated_as_empty(self):
        assert merge_config_layers(None, {"a": 1}, None) == {"a": 1}

    def test_empty_values_are_dropped_so_a_default_can_apply(self):
        merged = merge_config_layers(
            {"measurement": "", "window": None}, {"window": "  ", "rows": 0}
        )
        assert merged == {"rows": 0}

    def test_falsy_but_real_values_are_kept(self):
        merged = merge_config_layers({"dry_run": False, "excluded": []})
        assert merged == {"dry_run": False, "excluded": []}

    def test_pinned_key_set_by_base_cannot_be_overridden(self):
        with pytest.raises(ValueError, match="measurement"):
            merge_config_layers(
                {"measurement": "safe"}, {"measurement": "evil"}, pinned=["measurement"]
            )

    @pytest.mark.parametrize(
        "respelled", ["MEASUREMENT", " measurement", "Measurement ", b"measurement"]
    )
    def test_pinned_key_resists_a_respelled_overlay_key(self, respelled):
        """An overlay built from a request is spelled by its sender."""
        with pytest.raises(ValueError, match="Cannot override pinned"):
            merge_config_layers(
                {"measurement": "safe"}, {respelled: "evil"}, pinned=["measurement"]
            )

    def test_pinned_dotted_key_resists_the_nested_separator_spelling(self):
        """The settings store reads ``a__b`` as the same key as ``a.b``."""
        with pytest.raises(ValueError, match="Cannot override pinned"):
            merge_config_layers({"a.b": "safe"}, {"a__b": "evil"}, pinned=["a.b"])

    @pytest.mark.parametrize("pinned", ["measurement", bytearray(b"measurement")])
    def test_pinned_as_a_bare_string_is_rejected(self, pinned):
        """It would otherwise iterate into single letters and pin none of them."""
        with pytest.raises(ValueError, match="must be a list"):
            merge_config_layers({"measurement": "safe"}, {}, pinned=pinned)

    def test_pinned_key_base_never_set_stays_open(self):
        merged = merge_config_layers({}, {"measurement": "cpu"}, pinned=["measurement"])
        assert merged == {"measurement": "cpu"}

    def test_conflict_can_be_ignored_instead_of_raising(self):
        merged = merge_config_layers(
            {"measurement": "safe"},
            {"measurement": "evil", "window": "1h"},
            pinned=["measurement"],
            on_conflict="ignore",
        )
        assert merged == {"measurement": "safe", "window": "1h"}

    def test_invalid_on_conflict_rejected(self):
        with pytest.raises(ValueError, match="on_conflict"):
            merge_config_layers({}, on_conflict="bogus")
