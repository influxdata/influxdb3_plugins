"""Write helpers: LineBuilder construction and resilient writes.

The processing engine injects ``LineBuilder`` as a runtime global inside the
plugin, so it is not importable here. Builder helpers therefore receive the
``LineBuilder`` class as their first argument (pass the plugin's global), while
write helpers operate on already-built line objects and need no class.
"""

import math
import random
import time

from .parsing import parse_bool

__all__ = [
    "add_field_with_type",
    "build_line",
    "build_line_typed",
    "write_data",
    "BatchLines",
]

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_UINT64_MAX = 2**64 - 1


class BatchLines:
    """Join multiple LineBuilder outputs into a single write payload."""

    def __init__(self, line_builders):
        self._line_builders = list(line_builders)
        self._built: str | None = None

    def build(self) -> str:
        if self._built is None:
            lines = [str(builder.build()) for builder in self._line_builders]
            if not lines:
                raise ValueError("write received no lines to build")
            self._built = "\n".join(lines)
        return self._built


def _infer_type(value) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    return "string"


def add_field_with_type(line, field_key: str, value, field_type: str):
    """Add a field to a LineBuilder with explicit type conversion.

    Supported types: ``int``, ``uint``, ``float``, ``bool``, ``string``.
    """
    if field_type == "int":
        ival = int(value)
        if not (_INT64_MIN <= ival <= _INT64_MAX):
            raise ValueError(
                f"int field {field_key!r} out of int64 range: {ival}"
            )
        line.int64_field(field_key, ival)
    elif field_type == "uint":
        uval = int(value)
        if not (0 <= uval <= _UINT64_MAX):
            raise ValueError(
                f"uint field {field_key!r} out of uint64 range: {uval}"
            )
        line.uint64_field(field_key, uval)
    elif field_type == "float":
        fval = float(value)
        if not math.isfinite(fval):
            raise ValueError(f"float field {field_key!r} is not finite: {value!r}")
        line.float64_field(field_key, fval)
    elif field_type == "bool":
        # strings are parsed strictly; everything else via Python truthiness
        bval = parse_bool(value) if isinstance(value, str) else bool(value)
        line.bool_field(field_key, bval)
    elif field_type == "string":
        line.string_field(field_key, str(value))
    else:
        raise ValueError(
            f"Unsupported field type {field_type!r} for {field_key!r}"
        )
    return line


def build_line(
    line_builder_cls,
    measurement: str,
    *,
    tags: dict | None = None,
    fields: dict | None = None,
    time_ns: int | None = None,
):
    """Build a LineBuilder, inferring each field's type from its value.

    Variant for the common case where Python types map cleanly to line types.
    Raises ``ValueError`` when no non-``None`` fields remain.
    """
    line = line_builder_cls(measurement)
    for key, value in (tags or {}).items():
        if value is None:
            continue
        line.tag(key, str(value))
    field_count = 0
    for key, value in (fields or {}).items():
        if value is None:
            continue
        add_field_with_type(line, key, value, _infer_type(value))
        field_count += 1
    if not field_count:
        # fail early: a field-less line would poison the whole batched write
        raise ValueError(f"line for {measurement!r} has no fields")
    if time_ns is not None:
        line.time_ns(time_ns)
    return line


def build_line_typed(
    line_builder_cls,
    measurement: str,
    *,
    tags: dict | None = None,
    typed_fields: dict | None = None,
    time_ns: int | None = None,
):
    """Build a LineBuilder with explicit field types.

    Variant for when types must be forced. ``typed_fields`` maps each field to a
    ``(value, type)`` tuple, where ``type`` is one of the
    :func:`add_field_with_type` types. Raises ``ValueError`` when no
    non-``None`` fields remain.
    """
    line = line_builder_cls(measurement)
    for key, value in (tags or {}).items():
        if value is None:
            continue
        line.tag(key, str(value))
    field_count = 0
    for key, (value, field_type) in (typed_fields or {}).items():
        if value is None:
            continue
        add_field_with_type(line, key, value, field_type)
        field_count += 1
    if not field_count:
        raise ValueError(f"line for {measurement!r} has no fields")
    if time_ns is not None:
        line.time_ns(time_ns)
    return line


def write_data(
    influxdb3_local,
    line_builders,
    *,
    batch: bool = True,
    retries: int = 3,
    base_delay: float = 1.0,
    no_sync: bool = True,
) -> None:
    """Write LineBuilder objects with optional batching and retry.

    Args:
        influxdb3_local: InfluxDB client instance.
        line_builders: Iterable of LineBuilder objects (``None`` items skipped).
        batch: Combine all lines into one payload via :class:`BatchLines`.
            Set ``False`` to write each line individually.
        retries: Number of extra attempts on failure (``0`` disables retry).
        base_delay: Base seconds for exponential backoff with jitter.
        no_sync: Passed through to ``write_sync``.
    """
    builders = [builder for builder in line_builders if builder is not None]
    if not builders:
        return

    payloads = [BatchLines(builders)] if batch else builders
    attempts = max(retries, 0) + 1

    for payload in payloads:
        for attempt in range(attempts):
            try:
                influxdb3_local.write_sync(payload, no_sync=no_sync)
                break
            except Exception:
                if attempt == attempts - 1:
                    raise
                delay = (2**attempt) * base_delay + random.uniform(0, base_delay)
                time.sleep(delay)