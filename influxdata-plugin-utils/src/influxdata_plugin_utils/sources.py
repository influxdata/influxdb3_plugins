"""Configuration sources: one parser per place a plugin's settings come from.

Each parser reads one raw input -- trigger arguments, a TOML file, environment
variables, the request body, headers, the query string -- and returns a plain
dict. A ``KeySpec`` says which keys of that source may contribute and under what
names, so a layer carries only what the plugin asked for.

A value that arrives empty is left out, so a validator default applies instead:
a blank string, a JSON ``null``, an unset variable. Nested values are passed
through untouched.

Every rejection is a ``ValueError`` naming the source.
"""

import json
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

from ._utils import as_text, is_blank, require_choice

__all__ = [
    "KeySpec",
    "is_toml_path",
    "parse_trigger_args",
    "parse_json_body",
    "parse_request_headers",
    "parse_query_parameters",
    "parse_env",
    "parse_toml",
    "resolve_plugin_dir",
    "resolve_path",
]

DEFAULT_MAX_BODY_BYTES = 10 * 1024 * 1024

_UNKNOWN_POLICIES = ("ignore", "reject")

# refused keys are spelled by whoever sent them, so an error names a sample
_MAX_REPORTED_KEYS = 10
_MAX_REPORTED_KEY_CHARS = 64


def _as_names(value) -> tuple[str, ...] | None:
    """One name or a sequence of them, as a tuple."""
    if value is None:
        return None
    if isinstance(value, (str, bytes, bytearray)):
        return (as_text(value),)
    return tuple(as_text(name) for name in value)


@dataclass(frozen=True)
class KeySpec:
    """Which keys of a source contribute, and under what names.

    ``allowlist`` names the keys that pass and ``denylist`` the ones that do
    not; ``rename`` maps a source key onto the config key it becomes. One
    config key comes from one source key, so renaming onto a key the source
    already carries is refused. ``unknown`` decides what happens to a refused
    key: ``"ignore"`` drops it, ``"reject"`` names it in the error.

    On a layer the caller controls, prefer ``allowlist``: a key added to the
    plugin later stays unreachable until it is listed, where a ``denylist``
    would let it through unnoticed.
    """

    allowlist: tuple[str, ...] | None = None
    denylist: tuple[str, ...] | None = None
    rename: dict[str, str] | None = None
    unknown: str = "ignore"

    def __post_init__(self):
        object.__setattr__(self, "allowlist", _as_names(self.allowlist))
        object.__setattr__(self, "denylist", _as_names(self.denylist))
        object.__setattr__(self, "rename", dict(self.rename) if self.rename else None)
        require_choice(self.unknown, _UNKNOWN_POLICIES, "unknown")

        both = set(self.allowlist or ()) & set(self.denylist or ())
        if both:
            raise ValueError(f"{sorted(both)} are both allowed and denied")

        if self.rename:
            targets = list(self.rename.values())
            duplicates = {name for name in targets if targets.count(name) > 1}
            if duplicates:
                raise ValueError(f"rename maps several keys onto {sorted(duplicates)}")
            if self.allowlist is not None:
                unreachable = set(self.rename) - set(self.allowlist)
                if unreachable:
                    raise ValueError(
                        f"rename names {sorted(unreachable)}, which the allowlist "
                        f"does not let through"
                    )

    def folded(self) -> "KeySpec":
        """The same spec with protocol names folded into config keys."""
        return KeySpec(
            allowlist=None if self.allowlist is None else
            tuple(header_key(name) for name in self.allowlist),
            denylist=None if self.denylist is None else
            tuple(header_key(name) for name in self.denylist),
            rename=self._folded_rename(),
            unknown=self.unknown,
        )

    def _folded_rename(self) -> dict[str, str] | None:
        """``rename`` keyed by config key, refusing two spellings of one name."""
        if self.rename is None:
            return None
        folded: dict[str, str] = {}
        for name, target in self.rename.items():
            key = header_key(name)
            if key in folded:
                raise ValueError(f"rename looks up {key!r} more than once")
            folded[key] = target
        return folded


def is_toml_path(path) -> bool:
    """Does this path name a TOML file? What to do when it does not is the caller's call."""
    return as_text(path).strip().lower().endswith(".toml")


def header_key(name) -> str:
    """Fold a header name to its config key: ``X-Api-Key`` -> ``x-api-key``.

    Only the casing is folded, because only the casing is meaningless: RFC 9110
    makes field names case-insensitive. ``rename`` gives the key another name.
    """
    return as_text(name).strip().lower()


def _values_of(raw) -> list[str]:
    """One protocol value as a list of non-empty strings.

    A repeated name arrives as a list from some runtimes.
    """
    values = raw if isinstance(raw, (list, tuple)) else [raw]
    return [
        text
        for text in (as_text(value).strip() for value in values if value is not None)
        if text
    ]


def _claim(spelled_by: dict, target: str, key: str, source: str) -> None:
    """One config key comes from one source key; two spellings are ambiguous."""
    first = spelled_by.setdefault(target, key)
    if first != key:
        raise ValueError(
            f"{source}: {first!r} and {key!r} are the same config key {target!r}"
        )


def _wrong_shape(source: str, value) -> ValueError:
    return ValueError(
        f"{source} must be a mapping or a sequence of name/value pairs, "
        f"got {type(value).__name__}"
    )


def _pairs(raw, source: str):
    """Name/value pairs from a mapping or from a sequence of pairs."""
    if raw is None:
        return ()
    if hasattr(raw, "items"):
        return raw.items()
    if isinstance(raw, (str, bytes, bytearray)) or not hasattr(raw, "__iter__"):
        raise _wrong_shape(source, raw)
    pairs = []
    for item in raw:
        if isinstance(item, (str, bytes, bytearray)):
            raise _wrong_shape(source, item)
        try:
            name, value = item
        except (TypeError, ValueError) as exc:
            raise _wrong_shape(source, item) from exc
        pairs.append((name, value))
    return pairs


def _select(
    pairs,
    spec: KeySpec | None,
    *,
    source: str,
    fold_case: bool = False,
    coerce: bool = True,
    multi: bool = False,
    reject_repeats: bool = False,
) -> dict:
    """Apply a spec to the pairs of one source.

    ``coerce`` turns protocol values into strings; body and file values keep
    their own types. ``reject_repeats`` refuses a key the source carries more
    than once, for a source where a repeat is ambiguous rather than a list.
    """
    spec = spec.folded() if spec is not None and fold_case else spec
    allowed = (
        None if spec is None or spec.allowlist is None else frozenset(spec.allowlist)
    )
    denied = frozenset(spec.denylist) if spec is not None and spec.denylist else ()
    rename = (spec.rename or {}) if spec is not None else {}
    report = spec is not None and spec.unknown == "reject"

    selected: dict = {}
    spelled_by: dict[str, str] = {}
    refused_sample: list[str] = []
    refused_count = 0

    for raw_key, raw_value in pairs:
        key = as_text(raw_key)
        match = header_key(key) if fold_case else key
        if match in denied or (allowed is not None and match not in allowed):
            if report:
                refused_count += 1
                if len(refused_sample) < _MAX_REPORTED_KEYS:
                    refused_sample.append(key[:_MAX_REPORTED_KEY_CHARS])
            continue

        target = rename.get(match, match if fold_case else key)
        if coerce:
            values = _values_of(raw_value)
            if not values:
                continue
            _claim(spelled_by, target, match, source)
            if multi:
                selected.setdefault(target, []).extend(values)
            else:
                if reject_repeats and target in selected:
                    raise ValueError(f"{source}: {match!r} is set more than once")
                selected.setdefault(target, values[0])
        elif not is_blank(raw_value):
            _claim(spelled_by, target, match, source)
            selected[target] = raw_value

    if refused_count:
        refused = ", ".join(repr(key) for key in sorted(refused_sample))
        hidden = refused_count - len(refused_sample)
        if hidden:
            refused += f" and {hidden} more"
        accepted = sorted(allowed) if allowed is not None else "everything else"
        raise ValueError(f"{source} may not set {refused}; accepted keys: {accepted}")
    return selected


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


def parse_trigger_args(args, spec: KeySpec | None = None) -> dict:
    """Read the dict the engine hands to the plugin entry point.

    Args:
        args: Trigger arguments; ``None`` when the trigger has none.
        spec: Which of them become config values.

    Returns:
        Config values, with the argument names kept as written.
    """
    return _select(_pairs(args, "Trigger arguments"), spec,
                   source="Trigger arguments", coerce=False)


def parse_toml(
    config_file_path,
    spec: KeySpec | None = None,
    *,
    require_suffix: bool = True,
) -> dict:
    """Read a TOML file, resolving a relative path against the plugin directory.

    Args:
        config_file_path: Path to the file; ``None`` or empty yields ``{}``.
        spec: Which of its keys become config values.
        require_suffix: Refuse a path that does not name a ``.toml`` file,
            before opening it.

    Returns:
        Config values, with TOML's own types preserved.

    Raises:
        ValueError: The path is refused, or the file cannot be read or is not
            valid TOML.
    """
    if not config_file_path:
        return {}
    if require_suffix and not is_toml_path(config_file_path):
        raise ValueError(
            f"Invalid config file format: expected a .toml file, "
            f"got {config_file_path!r}"
        )
    try:
        with open(resolve_path(config_file_path), "rb") as config_file:
            table = tomllib.load(config_file)
    except OSError as exc:
        raise ValueError(
            f"Cannot read config file {config_file_path!r}: {exc.strerror or exc}"
        ) from exc
    except (tomllib.TOMLDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"Config file {config_file_path!r} is not valid TOML: {exc}") from exc
    return _select(table.items(), spec, source="Config file", coerce=False)


def parse_env(spec: KeySpec) -> dict:
    """Read named environment variables.

    Args:
        spec: The variables to read. An ``allowlist`` is required: the process
            environment belongs to the host and holds credentials, so nothing
            is read without being named.

    Returns:
        Config values keyed by variable name, or by the name ``rename`` gives.

    Raises:
        ValueError: The spec names no variables.
    """
    if spec is None or not spec.allowlist:
        raise ValueError("parse_env needs a KeySpec with an allowlist of variables")
    present = [(name, os.environ[name]) for name in spec.allowlist if name in os.environ]
    return _select(present, spec, source="Environment variables")


def _decode_body(request_body, max_bytes: int | None) -> dict:
    """Decode a raw request body into a JSON object."""
    if request_body is None:
        return {}
    if isinstance(request_body, dict):
        return request_body
    # the size is checked before decoding, so an oversized body is refused unparsed
    if isinstance(request_body, str):
        # ascii is one byte per character; only other text needs encoding
        size = (
            len(request_body)
            if request_body.isascii()
            else len(request_body.encode("utf-8"))
        )
        _reject_oversized(size, max_bytes)
        text = request_body.lstrip("\ufeff")  # a BOM, if the caller pre-decoded
    elif isinstance(request_body, (bytes, bytearray)):
        _reject_oversized(len(request_body), max_bytes)
        try:
            # utf-8-sig drops the BOM that .NET and PowerShell clients send
            text = bytes(request_body).decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise ValueError(f"Request body is not valid UTF-8: {exc}") from exc
    else:
        raise ValueError(
            "Request body must be JSON text, bytes or a dict, got "
            f"{type(request_body).__name__}"
        )
    if not text.strip():
        return {}
    try:
        body = json.loads(text)
    except RecursionError as exc:
        # deep nesting exhausts the stack long before the byte limit is reached
        raise ValueError("Request body is nested too deeply") from exc
    except ValueError as exc:
        raise ValueError(f"Request body is not valid JSON: {exc}") from exc
    if not isinstance(body, dict):
        raise ValueError("Request body must be a JSON object")
    return body


def _reject_oversized(size: int, max_bytes: int | None) -> None:
    if max_bytes is not None and size > max_bytes:
        raise ValueError(
            f"Request body is {size} bytes, over the {max_bytes} byte limit"
        )


def parse_json_body(
    request_body,
    spec: KeySpec | None = None,
    *,
    max_bytes: int | None = DEFAULT_MAX_BODY_BYTES,
) -> dict:
    """Read a JSON request body.

    Args:
        request_body: The body as delivered to ``process_request`` (``bytes``,
            ``str`` or an already-decoded ``dict``). ``None`` and blank text
            yield ``{}``; any other type is refused.
        spec: Which of its keys become config values.
        max_bytes: Refuse a body larger than this before parsing it. ``None``
            lifts the limit; a ``dict`` body is never measured.

    Returns:
        Config values, with the JSON types preserved, so a validator ``cast``
        sees a real list or number rather than its string form.

    Raises:
        ValueError: The body is oversized, undecodable, not a JSON object, or
            carries a key the spec refuses under ``unknown="reject"``.
    """
    body = _decode_body(request_body, max_bytes)
    return _select(body.items(), spec, source="Request body", coerce=False)


def parse_request_headers(
    request_headers,
    spec: KeySpec | None = None,
    *,
    multi: bool = False,
) -> dict:
    """Read request headers.

    Names are matched regardless of casing, which RFC 9110 makes meaningless,
    and become config keys spelled in lower case (``X-Api-Key`` ->
    ``x-api-key``); ``rename`` gives a key another name. Two casings of one
    name are therefore one header: ``X-Api-Key`` and ``X-API-KEY`` in one
    request are that header sent twice. A header the plugin asked for that
    arrives more than once is refused rather than resolved by the order the
    runtime delivers it in -- ``multi`` reads every value instead. InfluxDB 3
    hands the plugin a plain dict, which holds one value per name, so neither
    the refusal nor ``multi`` fires there; both are for a runtime that delivers
    name/value pairs. ``Authorization`` never arrives: the engine authenticates
    with it and drops it, so a token needs a header of your own.

    Args:
        request_headers: Headers as delivered to ``process_request`` -- a
            mapping, or a sequence of name/value pairs.
        spec: Which of them become config values. Worth naming: a client sends
            headers of its own on every request (``host``, ``user-agent``, ...),
            and ``unknown="reject"`` turns such a request away.
        multi: Read a header the request carries more than once as a list of
            every value, instead of refusing it.

    Returns:
        Config values keyed by config key; empty header values are omitted.

    Raises:
        ValueError: The headers are of another shape, one the plugin asked for
            arrives more than once while ``multi`` is off, two of them land on
            one config key, or one is refused under ``unknown="reject"``.
    """
    return _select(
        _pairs(request_headers, "Request headers"),
        spec,
        source="Request headers",
        fold_case=True,
        multi=multi,
        reject_repeats=True,
    )


def parse_query_parameters(
    query_parameters,
    spec: KeySpec | None = None,
    *,
    multi: bool = False,
) -> dict:
    """Read query-string parameters.

    Names are compared exactly, so ``Window`` and ``window`` are two
    parameters; only the same spelling twice is a repeat. A parameter the
    plugin asked for that arrives more than once is refused rather than
    resolved by the order the runtime delivers it in -- ``multi`` reads every
    value instead. InfluxDB 3 hands the plugin a plain dict, which holds one
    value per name, so neither the refusal nor ``multi`` fires there; both are
    for a runtime that delivers name/value pairs.

    Args:
        query_parameters: Parameters as delivered to ``process_request`` -- a
            mapping, or a sequence of name/value pairs.
        spec: Which of them become config values. Names are matched and kept
            exactly as written, so rename what needs a different config key.
        multi: Read a parameter the query string carries more than once as a
            list of every value, instead of refusing it.

    Returns:
        Config values as strings; leave the typing to a validator ``cast``.

    Raises:
        ValueError: The parameters are of another shape, one the plugin asked
            for arrives more than once while ``multi`` is off, or one is
            refused under ``unknown="reject"``.
    """
    return _select(
        _pairs(query_parameters, "Query parameters"),
        spec,
        source="Query parameters",
        multi=multi,
        reject_repeats=True,
    )