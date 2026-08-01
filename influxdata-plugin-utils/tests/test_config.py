"""Tests for influxdata_plugin_utils.config.

The security-critical behavior is that ``load_plugin_config`` never evaluates
dynaconf's ``@`` substitution tokens, so a value beginning with ``@`` cannot
read the server's filesystem or environment. See
https://github.com/influxdata/influxdb3_plugins/issues/134.
"""

import tomllib

import pytest

from influxdata_plugin_utils.config import Validator, load_plugin_config


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
        ["cpu", "host@example", "a@b", "", "@", "email@host.com"],
    )
    def test_ordinary_values_unchanged(self, value):
        cfg = load_plugin_config({"k": value}, source="args")
        assert cfg.get("k") == value


class TestValidationStillWorks:
    """Disabling tokens must not disturb Validator casting/defaults/required."""

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
        with pytest.raises(Exception):
            load_plugin_config(
                {}, validators=[Validator("must", must_exist=True)], source="args"
            )


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

    def test_invalid_source_rejected(self):
        with pytest.raises(ValueError):
            load_plugin_config({}, source="bogus")
