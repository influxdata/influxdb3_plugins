"""Plugin configuration loading backed by dynaconf.

Loads a plugin's TOML config (resolved via the plugin directory), merges
environment variables and engine-supplied ``args``, and validates the result.
dynaconf is an implementation detail and must not leak into plugin code beyond
the re-exported ``Validator``.

All values are treated as literal data. dynaconf's ``@`` token substitution
(``@read_file``, ``@format``, ``@get``, ...) is disabled so that a value
beginning with ``@`` is never evaluated against the server's filesystem or
environment; see https://github.com/influxdata/influxdb3_plugins/issues/134.
"""

import os
import tomllib
from pathlib import Path

from dynaconf import Dynaconf, ValidationError, Validator

from ._utils import as_text, is_blank, require_choice

__all__ = [
    "resolve_plugin_dir",
    "resolve_path",
    "load_plugin_config",
    "merge_config_layers",
    "Validator",
]

# dynaconf options that decide how a value is interpreted: no "@" token
# substitution (issue #134), no dotted write-through, and a nested value
# replaced rather than merged into. Set twice: dynaconf also reads them from
# the process environment, which beats a constructor argument.
_DYNACONF_GUARDS = {
    "AUTO_CAST_FOR_DYNACONF": False,
    "DOTTED_LOOKUP_FOR_DYNACONF": False,
    "MERGE_ENABLED_FOR_DYNACONF": False,
}

# dynaconf keeps its own options in the settings store, so a layer key naming
# one is a control knob, not config. Most match by suffix or prefix; the rest
# are listed here (test_config.py checks the list against dynaconf's own).
_DYNACONF_OPTION_SUFFIX = "_FOR_DYNACONF"
_DYNACONF_OPTION_PREFIX = "DYNACONF"
_DYNACONF_RESERVED_KEYS = frozenset(
    {
        "DEFAULT_SETTINGS_PATHS",
        "DYNABOXIFY",
        "PROJECT_ROOT",
        "RENAMED_VARS",
        "SETTINGS_MODULE",
    }
)


def _config_key(key) -> str:
    """Normalize a key the way the settings store does, so comparisons agree."""
    return as_text(key).replace("__", ".").strip().upper()


def _is_dynaconf_option(key: str) -> bool:
    """Is this normalized key one of dynaconf's own options?"""
    return (
        key.endswith(_DYNACONF_OPTION_SUFFIX)
        or key.startswith(_DYNACONF_OPTION_PREFIX)
        or key in _DYNACONF_RESERVED_KEYS
    )


def _without_blanks(layer: dict | None) -> dict:
    """Drop values that count as not provided, so a validator default applies.

    ``0``, ``False`` and an empty list are kept: they are real config values.
    """
    return {key: value for key, value in (layer or {}).items() if not is_blank(value)}


def resolve_plugin_dir() -> Path:
    """Resolve the plugin directory from the environment.

    Order: ``PLUGIN_DIR`` -> ``INFLUXDB3_PLUGIN_DIR`` -> parent of ``VIRTUAL_ENV``.
    """
    for env_var in ("PLUGIN_DIR", "INFLUXDB3_PLUGIN_DIR"):
        value = os.environ.get(env_var)
        if value:
            return Path(value)
    virtual_env = os.environ.get("VIRTUAL_ENV")
    if virtual_env:
        return Path(virtual_env).parent
    raise ValueError(
        "Cannot resolve plugin directory: set PLUGIN_DIR, INFLUXDB3_PLUGIN_DIR, "
        "or run inside the processing engine venv (VIRTUAL_ENV)."
    )


def resolve_path(path: str) -> Path:
    """Resolve a possibly relative path against the plugin directory.

    Absolute paths are returned unchanged.
    """
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return resolve_plugin_dir() / candidate


def merge_config_layers(
    base: dict | None,
    *overlays: dict | None,
    pinned: list[str] | None = None,
    on_conflict: str = "reject",
) -> dict:
    """Merge config layers into one dict, dropping values that arrive empty.

    Args:
        base: The lowest layer.
        *overlays: Layers in increasing precedence; the last one wins.
        pinned: Keys an overlay may not change once ``base`` sets them. Compared
            as the settings store keeps them, so a respelled key cannot reach a
            pinned one. Must be a list: a bare string would pin its letters.
        on_conflict: What to do when an overlay sets a pinned key --
            ``"reject"`` raises, ``"ignore"`` keeps the ``base`` value.

    Returns:
        A new dict without ``None`` values and blank strings.

    Raises:
        ValueError: ``pinned`` is a string, or an overlay sets a pinned key
            while ``on_conflict="reject"``.
    """
    require_choice(on_conflict, ("reject", "ignore"), "on_conflict")
    if isinstance(pinned, (str, bytes, bytearray)):
        raise ValueError("pinned must be a list of keys, not a single string")

    merged = _without_blanks(base)
    requested: dict = {}
    for overlay in overlays:
        requested.update(_without_blanks(overlay))

    if pinned:
        held = {_config_key(key) for key in pinned} & {
            _config_key(key) for key in merged
        }
        # sorted by config key: a layer may spell one key as text and as bytes
        conflicting = sorted(
            (key for key in requested if _config_key(key) in held), key=_config_key
        )
        if conflicting and on_conflict == "reject":
            raise ValueError(f"Cannot override pinned keys {conflicting}")
        for key in conflicting:
            del requested[key]

    merged.update(requested)
    return merged


def load_plugin_config(
    args: dict,
    validators: list[Validator] | None = None,
    *,
    env_keys: list[str] | None = None,
    config_file_path_arg: str = "config_file_path",
    source: str = "merge",
) -> Dynaconf:
    """Load and validate plugin configuration.

    Layers are merged per key (low -> high): env vars -> ``args`` -> TOML file.
    Within a layer, later keys override earlier ones; layers do not replace each
    other wholesale. dynaconf is used for casting and validation only.

    Each layer drops its own empty values, so a blank means "not set here": a
    lower layer or a validator default applies instead. A key naming a dynaconf
    option is dropped too; it would configure the loader, not the plugin.

    Args:
        args: The dict passed to the plugin entry point (``None`` is treated as
            empty). The TOML file path is read from ``args[config_file_path_arg]``
            when present.
        validators: Optional dynaconf ``Validator`` objects for required keys,
            type casting, and bounds. A name is one key, dots included: validate
            a nested table as a whole, not by ``"section.key"``.
        env_keys: Explicit environment variable names to read. Nothing is read
            from the environment when omitted; the variable name is the config
            key, which reads back in any casing.
        config_file_path_arg: Name of the ``args`` key holding the TOML path.
        source: Which non-env layers to apply. ``"merge"`` uses both ``args``
            and TOML (TOML highest); ``"args"`` uses only ``args``; ``"toml"``
            uses only the TOML file. The env layer always applies underneath.

    Returns:
        A ``Dynaconf`` settings object; access values as attributes or items.

    Raises:
        ValueError: ``source`` is unknown, the TOML file cannot be read or
            parsed, or a validator rejects the result.
    """
    require_choice(source, ("merge", "args", "toml"), "source")

    # the engine passes None when a trigger has no arguments
    args = args or {}

    layers: dict = {}

    # 1. env vars (lowest): only the explicitly requested names
    for env_var in env_keys or []:
        value = os.environ.get(env_var)
        if not is_blank(value):
            layers[_config_key(env_var)] = value

    # 2. engine args (middle)
    if source in ("merge", "args"):
        for key, value in args.items():
            if key != config_file_path_arg and not is_blank(value):
                layers[_config_key(key)] = value

    # 3. TOML file (highest)
    if source in ("merge", "toml"):
        config_file_path = args.get(config_file_path_arg)
        if config_file_path:
            try:
                with open(resolve_path(config_file_path), "rb") as config_file:
                    table = tomllib.load(config_file)
            except OSError as exc:
                raise ValueError(
                    f"Cannot read config file {config_file_path!r}: "
                    f"{exc.strerror or exc}"
                ) from exc
            for key, value in _without_blanks(table).items():
                layers[_config_key(key)] = value

    layers = {
        key: value for key, value in layers.items() if not _is_dynaconf_option(key)
    }

    # loaders=[] disables the DYNACONF_* env loader: env is read only via env_keys
    settings = Dynaconf(loaders=[], **_DYNACONF_GUARDS)
    settings.update(dict(_DYNACONF_GUARDS))
    settings.update(layers)
    if validators:
        settings.validators.register(*validators)
    try:
        settings.validators.validate()
    except ValidationError as exc:
        raise ValueError(str(exc)) from exc
    return settings
