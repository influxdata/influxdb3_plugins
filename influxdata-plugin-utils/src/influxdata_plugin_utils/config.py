"""Configuration loading for InfluxDB 3 plugins.

``load_config`` takes the layers a plugin chooses -- trigger arguments, a TOML
file, environment variables, the parts of an HTTP request -- merges them in the
order given and validates the result. Later layers win, so the argument order
is the precedence, lowest first.

``merge_config_layers`` does the merging on its own, for a plugin that wants the
dict without validation, and can hold chosen keys against the layers above.
"""

from ._utils import is_blank, require_choice
from .sources import (
    KeySpec,
    parse_env,
    parse_toml,
    parse_trigger_args,
    resolve_path,
    resolve_plugin_dir,
)
from .validation import Validator, validate

__all__ = [
    "Config",
    "load_config",
    "load_plugin_config",
    "merge_config_layers",
    "resolve_plugin_dir",
    "resolve_path",
    "Validator",
]


class Config(dict):
    """Validated configuration: a dict that also answers to attribute access.

    A key that shares a name with a dict method is reachable as ``cfg["items"]``.
    """

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None

    def as_dict(self) -> dict:
        """A plain dict copy, for code that would rather not have the extras."""
        return dict(self)


def merge_config_layers(*layers, pinned=None, on_conflict: str = "reject") -> dict:
    """Merge layers into one dict, lowest precedence first.

    Args:
        *layers: Dicts in increasing precedence; the last one wins. ``None`` and
            empty layers are allowed, so an absent source needs no branching.
        pinned: Keys that a later layer may not change once an earlier one has
            set them. Must be a list: a bare string would pin its letters.
        on_conflict: What to do when a later layer sets a pinned key --
            ``"reject"`` raises, ``"ignore"`` keeps the value already set.

    Returns:
        A new dict. A value that arrives empty is left out, so a validator
        default applies instead; ``0``, ``False`` and ``[]`` are kept.

    Raises:
        ValueError: ``pinned`` is a string, or a later layer sets a pinned key
            while ``on_conflict="reject"``.
    """
    require_choice(on_conflict, ("reject", "ignore"), "on_conflict")
    if isinstance(pinned, (str, bytes, bytearray)):
        raise ValueError("pinned must be a list of keys, not a single string")
    fixed = frozenset(pinned or ())

    merged: dict = {}
    held: set = set()
    conflicts: list = []
    for layer in layers:
        for key, value in (layer or {}).items():
            if is_blank(value):
                continue
            if key in held:
                conflicts.append(key)
                continue
            merged[key] = value
            if key in fixed:
                held.add(key)

    if conflicts and on_conflict == "reject":
        # sorted by repr: a layer may spell one key as text and another as bytes
        raise ValueError(f"Cannot override pinned keys {sorted(set(conflicts), key=repr)}")
    return merged


def load_config(*layers, validators=None) -> Config:
    """Merge the given layers and validate the result.

    Args:
        *layers: Dicts in increasing precedence, as produced by the ``sources``
            parsers or built by the plugin itself.
        validators: ``Validator`` rules applied to the merged values.

    Returns:
        The validated configuration.

    Raises:
        ValueError: A validator rejects the configuration.
    """
    return Config(validate(merge_config_layers(*layers), validators))


def load_plugin_config(
    args: dict,
    validators=None,
    *,
    env_keys: list[str] | None = None,
    config_file_path_arg: str = "config_file_path",
    source: str = "merge",
) -> Config:
    """Load configuration from the environment, the trigger and a TOML file.

    A plugin that needs other layers, or another order, composes them itself
    with ``load_config``.

    Args:
        args: The dict passed to the plugin entry point (``None`` is treated as
            empty). The TOML path is read from ``args[config_file_path_arg]``.
        validators: ``Validator`` rules applied to the merged values.
        env_keys: Environment variables to read. Nothing is read from the
            environment when omitted; each name becomes a lower-case config key.
        config_file_path_arg: Name of the ``args`` key holding the TOML path.
        source: Which layers besides the environment apply. ``"merge"`` uses
            both ``args`` and the TOML file (the file wins); ``"args"`` uses the
            arguments alone; ``"toml"`` the file alone.

    Returns:
        The validated configuration.

    Raises:
        ValueError: ``source`` is unknown, the TOML file cannot be read or
            parsed, or a validator rejects the configuration.
    """
    require_choice(source, ("merge", "args", "toml"), "source")
    args = args or {}

    layers = []
    if env_keys:
        values = parse_env(KeySpec(allowlist=env_keys))
        layers.append({name.lower(): value for name, value in values.items()})
    if source in ("merge", "args"):
        layers.append(
            parse_trigger_args(args, KeySpec(denylist=[config_file_path_arg]))
        )
    if source in ("merge", "toml"):
        layers.append(parse_toml(args.get(config_file_path_arg)))

    merged = merge_config_layers(*layers)
    return Config(validate(merged, validators))
