"""Plugin configuration loading backed by dynaconf.

Loads a plugin's TOML config (resolved via the plugin directory), merges
environment variables and engine-supplied ``args``, and validates the result.
dynaconf is an implementation detail and must not leak into plugin code beyond
the re-exported ``Validator``.
"""

import os
import tomllib
from pathlib import Path

from dynaconf import Dynaconf, Validator

__all__ = ["resolve_plugin_dir", "resolve_path", "load_plugin_config", "Validator"]


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

    Args:
        args: The dict passed to the plugin entry point (``None`` is treated as
            empty). The TOML file path is read from ``args[config_file_path_arg]``
            when present.
        validators: Optional dynaconf ``Validator`` objects for required keys,
            type casting, and bounds.
        env_keys: Explicit environment variable names to read. Nothing is read
            from the environment when omitted; each name is lowercased to form
            the config key.
        config_file_path_arg: Name of the ``args`` key holding the TOML path.
        source: Which non-env layers to apply. ``"merge"`` uses both ``args``
            and TOML (TOML highest); ``"args"`` uses only ``args``; ``"toml"``
            uses only the TOML file. The env layer always applies underneath.

    Returns:
        A ``Dynaconf`` settings object; access values as attributes or items.
    """
    if source not in ("merge", "args", "toml"):
        raise ValueError(
            f"Invalid source {source!r}. Supported: merge, args, toml"
        )

    # the engine passes None when a trigger has no arguments
    args = args or {}

    layers: dict = {}

    # 1. env vars (lowest): only the explicitly requested names
    for env_var in env_keys or []:
        value = os.environ.get(env_var)
        if value is not None:
            layers[env_var.lower()] = value

    # 2. engine args (middle)
    if source in ("merge", "args"):
        for key, value in args.items():
            if key != config_file_path_arg:
                layers[key] = value

    # 3. TOML file (highest)
    if source in ("merge", "toml"):
        config_file_path = args.get(config_file_path_arg)
        if config_file_path:
            with open(resolve_path(config_file_path), "rb") as config_file:
                layers.update(tomllib.load(config_file))

    # loaders=[] disables the DYNACONF_* env loader; env is read only via env_keys
    settings = Dynaconf(loaders=[])
    settings.update(layers)
    if validators:
        settings.validators.register(*validators)
    settings.validators.validate()
    return settings