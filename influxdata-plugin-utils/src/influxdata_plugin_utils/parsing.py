"""Parsing helpers for plugin configuration values.

Each parser accepts a raw string (as delivered via CLI/trigger args) and can be
used directly or as a dynaconf ``cast=`` callable. Only simple formats are
supported here; plugin-specific nested formats stay in their plugins.
"""

import re
from datetime import datetime, timedelta, timezone

__all__ = [
    "parse_timedelta",
    "parse_timestamp_ns",
    "parse_int",
    "parse_bool",
    "parse_delimited_list",
    "parse_key_value",
]

_DURATION_RE = re.compile(r"^\s*(\d+)\s*([a-zA-Z]+)\s*$")
_DURATION_UNITS = {
    "us": "microseconds",
    "ms": "milliseconds",
    "s": "seconds",
    "min": "minutes",
    "h": "hours",
    "d": "days",
    "w": "weeks",
}

_TIMESTAMP_UNIT_NS = {"ns": 1, "us": 1_000, "ms": 1_000_000, "s": 1_000_000_000}

_TRUE = {"true", "t", "1", "yes", "on"}
_FALSE = {"false", "f", "0", "no", "off"}


def parse_timedelta(raw) -> timedelta:
    """Parse a duration like ``500us``, ``100ms``, ``30s``, ``5min``, ``1h``, ``2d``, ``1w``."""
    if isinstance(raw, timedelta):
        return raw
    match = _DURATION_RE.match(str(raw))
    if not match:
        raise ValueError(f"Invalid duration: {raw!r}")
    magnitude, unit = int(match.group(1)), match.group(2).lower()
    if unit not in _DURATION_UNITS:
        raise ValueError(f"Unknown duration unit {unit!r} in {raw!r}")
    return timedelta(**{_DURATION_UNITS[unit]: magnitude})


def parse_timestamp_ns(value, time_format: str) -> int:
    """Convert a timestamp to integer nanoseconds.

    Numeric epoch units: ``ns``, ``us``, ``ms``, ``s``.
    ``datetime``: ISO-8601 string or ``datetime`` object; naive values are UTC.
    """
    if time_format in _TIMESTAMP_UNIT_NS:
        unit = _TIMESTAMP_UNIT_NS[time_format]
        if isinstance(value, float):
            return int(value * unit)
        try:
            # exact integer path: no float64 rounding of ns/us epochs
            return int(value) * unit
        except (TypeError, ValueError):
            return int(float(value) * unit)
    if time_format == "datetime":
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, str):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            raise ValueError(
                f"datetime format requires str or datetime, got {type(value)}"
            )
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1_000_000_000)
    raise ValueError(
        f"Unknown time format: {time_format!r}. Supported: ns, us, ms, s, datetime"
    )


def parse_int(raw, *, minimum: int | None = None, maximum: int | None = None) -> int:
    """Parse an integer with optional inclusive bounds."""
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        raise ValueError(f"Invalid integer: {raw!r}")
    if minimum is not None and value < minimum:
        raise ValueError(f"Value {value} below minimum {minimum}")
    if maximum is not None and value > maximum:
        raise ValueError(f"Value {value} above maximum {maximum}")
    return value


def parse_bool(raw) -> bool:
    """Parse a boolean from common truthy/falsy strings."""
    if isinstance(raw, bool):
        return raw
    text = str(raw).strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    raise ValueError(f"Invalid boolean: {raw!r}")


def parse_delimited_list(raw, *, sep: str = " ") -> list[str]:
    """Split a delimited string into a list of non-empty trimmed items."""
    if isinstance(raw, (list, tuple)):
        return [str(item).strip() for item in raw if str(item).strip()]
    return [item.strip() for item in str(raw).split(sep) if item.strip()]


def parse_key_value(
    raw, *, pair_sep: str = " ", kv_sep: str = "="
) -> dict[str, str]:
    """Parse a flat ``key=value`` map from a delimited string."""
    if isinstance(raw, dict):
        return {str(key): str(value) for key, value in raw.items()}
    result: dict[str, str] = {}
    for pair in str(raw).split(pair_sep):
        pair = pair.strip()
        if not pair:
            continue
        if kv_sep not in pair:
            raise ValueError(f"Invalid pair {pair!r} (missing {kv_sep!r})")
        key, value = pair.split(kv_sep, 1)
        result[key.strip()] = value.strip()
    return result