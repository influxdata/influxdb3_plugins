"""Primary processing of HTTP request inputs for ``process_request`` plugins.

Each parser turns one raw runtime input -- JSON body, headers, query string --
into a plain dict ready for ``config.load_plugin_config``: ``names`` selects the
keys a layer may contribute, ``unknown`` decides what happens to the rest.

A top-level value that arrives empty -- a blank string, a JSON ``null`` -- is
left out, so a validator default applies. Nested values pass through untouched.

``unknown`` needs ``names`` to act on, since without it every key is accepted.
It defaults to ``"ignore"``; ``"reject"`` names the refused keys back to them.
"""

import json

from ._utils import as_text, is_blank, require_choice

__all__ = [
    "parse_json_body",
    "parse_request_headers",
    "parse_query_parameters",
]

DEFAULT_MAX_BODY_BYTES = 10 * 1024 * 1024

_UNKNOWN_POLICIES = ("ignore", "reject")

# the caller spells the refused keys, so an error names a bounded sample of them
_MAX_REPORTED_KEYS = 10
_MAX_REPORTED_KEY_CHARS = 64


def _normalize_key(name) -> str:
    """Turn a protocol-level name into a config key: ``X-Api-Key`` -> ``x_api_key``."""
    return as_text(name).strip().lower().replace("-", "_")


def _build_selection(names, *, fold_case: bool) -> dict[str, str] | None:
    """Map each accepted source name to the config key it produces.

    ``names`` is one name, a sequence, or a ``{source: config_key}`` dict;
    ``None`` accepts everything. ``fold_case`` normalizes header names.
    """
    if names is None:
        return None
    if isinstance(names, (str, bytes, bytearray)):
        names = [names]
    pairs = (
        names.items()
        if isinstance(names, dict)
        else ((name, _normalize_key(name) if fold_case else name) for name in names)
    )
    selection: dict[str, str] = {}
    targets: set[str] = set()
    for source, target in pairs:
        match = _normalize_key(source) if fold_case else as_text(source)
        target = as_text(target)
        if match in selection:
            raise ValueError(f"names looks up {match!r} more than once")
        if target in targets:
            raise ValueError(f"names maps more than one source onto {target!r}")
        selection[match] = target
        targets.add(target)
    return selection


def _wrong_shape(layer: str, value) -> ValueError:
    return ValueError(
        f"{layer} must be a mapping or a sequence of name/value pairs, "
        f"got {type(value).__name__}"
    )


def _pairs(source, layer: str):
    """Read name/value pairs from a mapping or from a sequence of pairs."""
    if source is None:
        return ()
    if hasattr(source, "items"):
        return source.items()
    if isinstance(source, (str, bytes, bytearray)) or not hasattr(source, "__iter__"):
        raise _wrong_shape(layer, source)
    pairs = []
    for item in source:
        if isinstance(item, (str, bytes, bytearray)):
            raise _wrong_shape(layer, item)
        try:
            name, value = item
        except (TypeError, ValueError) as exc:
            raise _wrong_shape(layer, item) from exc
        pairs.append((name, value))
    return pairs


def _clean_values(raw) -> list[str]:
    """Normalize one protocol value to a list of non-empty strings.

    A repeated header or query parameter arrives as a list in some runtimes.
    """
    values = raw if isinstance(raw, (list, tuple)) else [raw]
    return [
        text
        for text in (as_text(value).strip() for value in values if value is not None)
        if text
    ]


def _select(
    items,
    selection,
    unknown: str,
    *,
    fold_case: bool = False,
    multi: bool = False,
    coerce: bool = True,
    layer: str,
) -> dict:
    """Apply a selection to key/value pairs from one request layer.

    ``coerce`` normalizes protocol values into strings; JSON body values keep
    their own types.
    """
    result: dict = {}
    spelled_by: dict[str, str] = {}
    report_unknown = unknown == "reject"
    unknown_sample: list[str] = []
    unknown_count = 0
    for raw_key, raw_value in items:
        key = as_text(raw_key)
        if selection is None:
            target = _normalize_key(key) if fold_case else key
        else:
            match = _normalize_key(key) if fold_case else key
            if match not in selection:
                if report_unknown:
                    unknown_count += 1
                    if len(unknown_sample) < _MAX_REPORTED_KEYS:
                        unknown_sample.append(key[:_MAX_REPORTED_KEY_CHARS])
                continue
            target = selection[match]
        if coerce:
            values = _clean_values(raw_value)
            if not values:
                continue
            if fold_case:
                # two spellings of one name fold onto one config key, and the
                # order they arrive in is the runtime's, not the sender's
                first = spelled_by.setdefault(target, key)
                if first != key:
                    raise ValueError(
                        f"{layer} carry both {first!r} and {key!r}, which are the "
                        f"same config key {target!r}"
                    )
            if multi:
                result.setdefault(target, []).extend(values)
            else:
                result.setdefault(target, values[0])
        elif not is_blank(raw_value):
            result[target] = raw_value
    if unknown_count:
        refused = ", ".join(repr(key) for key in sorted(unknown_sample))
        hidden = unknown_count - len(unknown_sample)
        if hidden:
            refused += f" and {hidden} more"
        raise ValueError(
            f"{layer} may not set {refused}; accepted keys: {sorted(selection)}"
        )
    return result


def _reject_oversized(size: int, max_bytes: int | None) -> None:
    if max_bytes is not None and size > max_bytes:
        raise ValueError(
            f"Request body is {size} bytes, over the {max_bytes} byte limit"
        )


def _decode_body(request_body, max_bytes: int | None) -> dict:
    """Decode a raw request body into a JSON object."""
    if request_body is None:
        return {}
    if isinstance(request_body, dict):
        return request_body
    # size is checked before decoding, so an oversized body is refused unparsed
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


def parse_json_body(
    request_body,
    names=None,
    *,
    max_bytes: int | None = DEFAULT_MAX_BODY_BYTES,
    unknown: str = "ignore",
) -> dict:
    """Parse a JSON request body into config values.

    Body values keep their JSON types, so a validator ``cast`` sees a real list
    or number rather than its string form.

    Args:
        request_body: Raw body as delivered to ``process_request`` (``bytes``,
            ``str`` or an already-decoded ``dict``). ``None`` and blank text
            yield ``{}``; any other type is refused.
        names: Keys the body may set -- one name, a sequence, or a
            ``{body_key: config_key}`` dict to rename. ``None`` accepts every
            key. Names are matched and kept exactly as written.
        max_bytes: Reject a body larger than this before parsing it. ``None``
            disables the limit; a ``dict`` body is never measured.
        unknown: ``"reject"`` names the keys outside ``names`` in the error;
            ``"ignore"`` drops them.

    Returns:
        Config values; a top-level ``null`` or blank string is omitted.

    Raises:
        ValueError: The body is oversized, undecodable, not a JSON object, or
            sets a key outside ``names``.
    """
    require_choice(unknown, _UNKNOWN_POLICIES, "unknown")
    body = _decode_body(request_body, max_bytes)
    selection = _build_selection(names, fold_case=False)
    return _select(
        body.items(), selection, unknown, coerce=False, layer="Request body"
    )


def parse_request_headers(
    request_headers,
    names=None,
    *,
    multi: bool = False,
    unknown: str = "ignore",
) -> dict:
    """Read request headers into config values.

    Names are matched regardless of casing and hyphenation, and become config
    keys (``X-Api-Key`` -> ``x_api_key``). Two spellings of one name --
    ``X-Api-Key`` and ``x_api_key`` are separate headers on the wire -- are
    refused rather than resolved by the order the runtime delivers them in.
    ``Authorization`` never arrives: the engine authenticates with it and drops
    it, so a token needs a header of your own.

    Args:
        request_headers: Headers as delivered to ``process_request`` -- a
            mapping, or a sequence of name/value pairs.
        names: Headers to read -- one name, a sequence, or a
            ``{header: config_key}`` dict to rename. ``None`` reads every
            header, including the ones a client sends on its own (``host``,
            ``user-agent``, ...), so name the ones you want.
        multi: Return every value of a repeated header as a list instead of
            taking the first.
        unknown: ``"ignore"`` drops headers outside ``names``; ``"reject"``
            names them in the error, which turns away any request carrying a
            header you did not name.

    Returns:
        Config values keyed by config key; empty header values are omitted.

    Raises:
        ValueError: The headers are of another shape, two of them fold onto one
            config key, or one outside ``names`` arrived and
            ``unknown="reject"``.
    """
    require_choice(unknown, _UNKNOWN_POLICIES, "unknown")
    selection = _build_selection(names, fold_case=True)
    return _select(
        _pairs(request_headers, "Request headers"),
        selection,
        unknown,
        fold_case=True,
        multi=multi,
        layer="Request headers",
    )


def parse_query_parameters(
    query_parameters,
    names=None,
    *,
    multi: bool = False,
    unknown: str = "ignore",
) -> dict:
    """Read query-string parameters into config values.

    Values are always strings; leave the typing to a validator ``cast``.

    Args:
        query_parameters: Parameters as delivered to ``process_request`` -- a
            mapping, or a sequence of name/value pairs.
        names: Parameters to read -- one name, a sequence, or a
            ``{parameter: config_key}`` dict to rename. ``None`` reads every
            parameter. Names are matched and kept exactly as written.
        multi: Return every value of a repeated parameter as a list instead of
            taking the first.
        unknown: ``"ignore"`` drops parameters outside ``names``; ``"reject"``
            raises. The default keeps routing parameters such as ``?action=``
            from failing a config read.

    Returns:
        Config values keyed by config key; empty values are omitted.

    Raises:
        ValueError: The parameters are of another shape, or one outside
            ``names`` arrived and ``unknown="reject"``.
    """
    require_choice(unknown, _UNKNOWN_POLICIES, "unknown")
    selection = _build_selection(names, fold_case=False)
    return _select(
        _pairs(query_parameters, "Query parameters"),
        selection,
        unknown,
        fold_case=False,
        multi=multi,
        layer="Query parameters",
    )
