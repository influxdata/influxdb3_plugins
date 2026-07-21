"""
{
    "plugin_type": ["scheduled"],
    "scheduled_args_config": [
        {
            "name": "config_file_path",
            "example": "sagemaker_config.toml",
            "description": "Path to TOML configuration file (absolute or relative to the plugin directory from INFLUXDB3_PLUGIN_DIR or PLUGIN_DIR). When provided, all other arguments are loaded from the file.",
            "required": false
        },
        {
            "name": "endpoint_name",
            "example": "canvas-new-deployment-06-04-2025-10-35-AM",
            "description": "SageMaker endpoint to invoke.",
            "required": true
        },
        {
            "name": "source_measurement",
            "example": "data",
            "description": "InfluxDB measurement (table) to read from.",
            "required": true
        },
        {
            "name": "feature_order",
            "example": "{time}|0.0|0|{motor_speed:speed}|{ambient_temperature:temp}|0.0",
            "description": "Pipe-separated tokens used to build the SageMaker request body. Token forms: '{col}' = value of column 'col'; '{col:alias}' = value of 'col' under JSON key 'alias' (object shapes); 'literal' = padding/constant value; 'literal:alias' = constant value under JSON key 'alias' (object shapes). For instances_object/raw_object every literal MUST carry an alias and {col} tokens may use aliases to rename keys; for array shapes and text/csv aliases are accepted but ignored (one warning at init).",
            "required": true
        },
        {
            "name": "output_fields",
            "example": "forecasting=predictions[*].score|class=predictions[*].label",
            "description": "Pipe-separated 'name=path' pairs describing how to extract output columns from the SageMaker response. Path syntax depends on 'accept': JMESPath for application/json, integer column index for text/csv, empty for text/plain (path must be empty, response body is the value, exactly one entry allowed). In batch mode each path must yield an array; M (number of written rows) = min length across all paths and timestamp_path.",
            "required": true
        },
        {
            "name": "region",
            "example": "eu-central-1",
            "description": "AWS region of the SageMaker endpoint. Default: eu-central-1.",
            "required": false
        },
        {
            "name": "interval",
            "example": "5min",
            "description": "Lookback window for the source query (passed to SQL INTERVAL). Supported units: s, min, h, d, w, m (months≈30d), q (quarters≈91d), y (years=365d). Default: 60min.",
            "required": false
        },
        {
            "name": "limit",
            "example": "1",
            "description": "Max rows to read per scheduled call. In batch mode all rows are sent in one inference request; in per-row mode one request per row. Default: 1.",
            "required": false
        },
        {
            "name": "content_type",
            "example": "application/json",
            "description": "SageMaker request Content-Type: 'text/csv' or 'application/json'. Default: application/json.",
            "required": false
        },
        {
            "name": "accept",
            "example": "application/json",
            "description": "Expected response Content-Type (Accept header). Supported: application/json, application/jsonlines, text/csv, text/plain. Default: same as content_type.",
            "required": false
        },
        {
            "name": "json_shape",
            "example": "instances_array",
            "description": "JSON body shape (only when content_type=application/json). 'instances_array' -> {'instances':[[v1,v2,...], ...]}; 'instances_object' -> {'instances':[{'col':v,...}, ...]}; 'instances_features' -> {'instances':[{'features':[v1,v2,...]}, ...]} (built-in algos: KMeans, k-NN, RCF, Linear Learner, NTM, PCA); 'inputs' -> {'inputs':[v1,v2,...]} (single row only); 'inputs_array' -> {'inputs':[[v1,v2],...]} (TF Serving / PyTorch batch); 'inputs_flat' -> {'inputs':[v,v,...]} (HF NLP batch; requires exactly one {col} token in feature_order); 'inputs_timeseries' -> {'inputs':[{'target':[v1,v2,...,vN]}]} (time-series forecasting; collects all rows into one target array; requires exactly one {col} token; use with forecast_output=true); 'raw_array' -> [[v1,v2,...], ...]; 'raw_object' -> {'col':v,...} (single row only). Object forms require literals to use 'literal:alias' syntax. 'inputs' and 'raw_object' are not batch-compatible. Default: instances_array.",
            "required": false
        },
        {
            "name": "extra_body",
            "example": "parameters.max_new_tokens=50|parameters.top_p=0.95|parameters.do_sample=true",
            "description": "Optional pipe-separated 'path=value' pairs merged into the JSON body at the specified dotted paths. Values type-coerced (numbers, true/false/null, quoted strings, arrays with semicolons: [v1;v2;v3]). Used for nested LLM parameters and metadata. Conflicts with feature_order keys, raw_array (top-level list), or content_type=text/csv -> fail config.",
            "required": false
        },
        {
            "name": "target_model",
            "example": "model-a.tar.gz",
            "description": "Optional model identifier for multi-model endpoints (sets X-Amzn-SageMaker-Target-Model header). When set, also writes a 'sagemaker_model' tag to every output line.",
            "required": false
        },
        {
            "name": "batch_inference",
            "example": "true",
            "description": "If true, send all selected rows in one SageMaker request and parse a batch response. If false, one HTTP request per row. Default: true.",
            "required": false
        },
        {
            "name": "timestamp_path",
            "example": "predictions[*].t",
            "description": "Optional path to extract per-row timestamps from the response. Same syntax as output_fields paths (JMESPath / int column / ignored for text/plain). Value autodetected: ISO string -> parsed as UTC; int >1e15 -> nanoseconds; int 1e9..1e15 -> seconds; float -> seconds. If empty, time.time_ns() is used at the moment of LineBuilder creation. If set but a row's timestamp is missing or unparseable, that prediction is skipped with an error (no wall-clock fallback).",
            "required": false
        },
        {
            "name": "tag_values",
            "example": "sensor_id:A1@A2.env:prod",
            "description": "Optional tag filter applied to the source query. Syntax: 'tag1:val1@val2.tag2:val3'. Dot separates tag pairs; colon separates tag from values; @ separates multiple values for the same tag; values may be quoted with double or single quotes to include special characters. Tags with a single value are also written to the output line; tags with multiple values are filtered in the query but not written.",
            "required": false
        },
        {
            "name": "target_measurement",
            "example": "data_predictions",
            "description": "Measurement to write predictions into. Default: <source_measurement>_predictions.",
            "required": false
        },
        {
            "name": "target_database",
            "example": "forecasts",
            "description": "Database to write predictions into. When set, write_sync_to_db is used; otherwise predictions are written to the trigger's database via write_sync.",
            "required": false
        },
        {
            "name": "forecast_output",
            "example": "true",
            "description": "If true, output_fields paths may return arrays. Each array position becomes a separate output row; all arrays must have equal length. Scalar values are broadcast to every row. Only supported with accept=application/json. Useful with json_shape=inputs_timeseries for time-series forecasting models. Default: false.",
            "required": false
        }
    ]
}
"""

import csv
import io
import json
import math
import os
import re
import time
import tomllib
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import boto3
import jmespath
import pandas as pd

# Internal constants
_DEFAULT_REGION = "eu-central-1"
_DEFAULT_INTERVAL = "60min"
_DEFAULT_LIMIT = 1
_DEFAULT_CONTENT_TYPE = "application/json"
_DEFAULT_JSON_SHAPE = "instances_array"
_CONFIG_CACHE_KEY = "sagemaker_config"
_CONFIG_CACHE_TTL_SEC = 60 * 60
_COLUMNS_CACHE_TTL_SEC = 60 * 60

_VALID_CONTENT_TYPES = {"text/csv", "application/json"}
_VALID_ACCEPT_TYPES = {
    "application/json",
    "application/jsonlines",
    "text/csv",
    "text/plain",
}
_VALID_JSON_SHAPES = {
    "instances_array",
    "instances_object",
    "instances_features",
    "inputs",
    "inputs_array",
    "inputs_flat",
    "inputs_timeseries",
    "raw_array",
    "raw_object",
}
_TIMESERIES_JSON_SHAPES = {"inputs_timeseries"}
_OBJECT_JSON_SHAPES = {"instances_object", "raw_object"}
_NON_BATCH_JSON_SHAPES = {"inputs", "raw_object"}
_TOP_LEVEL_LIST_JSON_SHAPES = {"raw_array"}

# InfluxDB int64 field / timestamp range
_INT64_MIN: int = -9223372036854775808
_INT64_MAX: int = 9223372036854775807


"""
Helper for batching multiple line protocol builders into a single write.
"""


@runtime_checkable
class _LineBuilderInterface(Protocol):
    def build(self) -> str: ...


class _BatchLines:
    def __init__(self, line_builders: list[_LineBuilderInterface]):
        self._line_builders = list(line_builders)
        self._built: str | None = None

    def build(self) -> str:
        if self._built is None:
            lines = [str(b.build()) for b in self._line_builders]
            if not lines:
                raise ValueError("batch_write received no lines to build")
            self._built = "\n".join(lines)
        return self._built


def _parse_field_token(inner: str) -> tuple[str, str, str | None]:
    """Parse the body of a {...} token. Supports '{col}' and '{col:alias}'."""
    s = inner.strip()
    if ":" in s:
        col, alias = s.split(":", 1)
        col = col.strip()
        alias = alias.strip()
        if not col:
            raise ValueError(f"empty column name in '{{{inner}}}'")
        if not alias:
            raise ValueError(f"empty alias in '{{{inner}}}'")
        return ("field", col, alias)
    if not s:
        raise ValueError("empty {} token in feature_order")
    return ("field", s, None)


def _parse_literal_token(t: str) -> tuple[str, str, str | None]:
    """Parse a bare literal token, possibly with ':alias' suffix.

    Quoted literals: the alias (if any) appears after the closing quote and a ':'.
    Unquoted literals: split at the first ':' if present. To put a literal
    containing ':' use quotes (e.g. '"1:30":time').
    """
    s = t.strip()
    if not s:
        raise ValueError("empty literal token in feature_order")
    if s[0] in ('"', "'"):
        q = s[0]
        end = s.find(q, 1)
        if end == -1:
            raise ValueError(f"unclosed quote in literal: {s!r}")
        literal_text = s[: end + 1]
        rest = s[end + 1 :].strip()
        if not rest:
            return ("literal", literal_text, None)
        if rest.startswith(":"):
            alias = rest[1:].strip()
            if not alias:
                raise ValueError(f"empty alias in literal: {s!r}")
            return ("literal", literal_text, alias)
        raise ValueError(f"unexpected text after quoted literal: {s!r}")
    if ":" in s:
        literal_text, alias = s.split(":", 1)
        literal_text = literal_text.strip()
        alias = alias.strip()
        if not literal_text:
            raise ValueError(f"empty literal value in '{s}'")
        if not alias:
            raise ValueError(f"empty alias in '{s}'")
        return ("literal", literal_text, alias)
    return ("literal", s, None)


def parse_feature_order(spec: str) -> list[tuple[str, str, str | None]]:
    """Parse a pipe-separated feature_order spec into (kind, value, alias) tokens.

    kind is 'field' (value = column name) or 'literal' (value = literal text).
    alias is None unless the token used the ':alias' suffix; for object shapes
    aliases become JSON keys.
    """
    tokens: list[tuple[str, str, str | None]] = []
    for raw in spec.split("|"):
        t = raw.strip()
        if not t:
            continue
        if t.startswith("{") and t.endswith("}"):
            tokens.append(_parse_field_token(t[1:-1]))
        else:
            tokens.append(_parse_literal_token(t))
    return tokens


def coerce_literal(token: str) -> Any:
    """Coerce a literal token to a JSON-appropriate type.

    Supports: true/false/null, quoted strings, int, float, arrays ([v1;v2;v3]).
    """
    s = token.strip()
    if s == "true":
        return True
    if s == "false":
        return False
    if s == "null":
        return None
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ('"', "'"):
        return s[1:-1]
    if s.startswith("[") and s.endswith("]"):
        inner = s[1:-1]
        return [coerce_literal(elem) for elem in inner.split(";") if elem.strip()]
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        return s


def jsonify_cell(value: Any) -> Any:
    """Convert a query-result cell to a JSON-serialisable primitive."""
    if value is None or isinstance(value, (int, float, bool, str)):
        return value
    return str(value)


def get_measurement_columns(
    influxdb3_local, measurement: str, task_id: str
) -> list[str]:
    """Return all column names of a measurement (fields + tags + time), with 1h cache."""
    cache_key = f"sagemaker_columns::{measurement}"
    cached: list | None = influxdb3_local.cache.get(cache_key)
    if cached:
        return cached

    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $measurement
    """
    rows: list[dict] = influxdb3_local.query(query, {"measurement": measurement})
    if not rows:
        raise ValueError(
            f"[{task_id}] No columns found for measurement '{measurement}' "
            f"(does it exist?)"
        )
    columns = [r["column_name"] for r in rows]
    influxdb3_local.cache.put(cache_key, columns, _COLUMNS_CACHE_TTL_SEC)
    return columns


def parse_tag_values(
    spec: str, available_columns: list[str], task_id: str
) -> dict[str, list[str]]:
    """Parse a tag_values spec ('tag1:val1@val2.tag2:val3') into {tag: [values]}.

    Mirrors the downsampler syntax. Tags not present as columns of the source
    measurement raise a ValueError so the user fixes typos at config time.
    """
    result: dict[str, list[str]] = {}
    for pair in spec.split("."):
        pair = pair.strip()
        if not pair:
            continue
        parts = pair.split(":")
        if len(parts) != 2:
            raise ValueError(
                f"[{task_id}] Invalid tag_values pair: '{pair}' "
                f"(must contain exactly one ':')"
            )
        tag_name, value_str = parts[0].strip(), parts[1]
        if tag_name not in available_columns:
            raise ValueError(
                f"[{task_id}] Tag '{tag_name}' is not a column of the source "
                f"measurement"
            )
        values: list[str] = []
        for raw in value_str.split("@"):
            v = raw.strip()
            if not v:
                continue
            if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
                v = v[1:-1]
            values.append(v)
        if not values:
            raise ValueError(
                f"[{task_id}] Tag '{tag_name}' has no values in tag_values"
            )
        if tag_name in result:
            result[tag_name].extend(values)
        else:
            result[tag_name] = values
    return result


def generate_tag_filter_clause(tag_values: dict[str, list[str]]) -> str:
    """Build the AND-prefixed WHERE clause for tag filters (downsampler-compatible)."""
    if not tag_values:
        return ""
    parts: list[str] = []
    for key, values in tag_values.items():
        escaped = [v.replace("'", "''") for v in values]
        if len(escaped) == 1:
            parts.append(f"AND \"{key}\" = '{escaped[0]}'")
        else:
            quoted = ", ".join(f"'{v}'" for v in escaped)
            parts.append(f'AND "{key}" IN ({quoted})')
    return " " + " ".join(parts)


def _parse_output_field_entry(entry: str, task_id: str) -> tuple[str, str]:
    """Split a single 'name=path' entry into (name, path), validating name."""
    if "=" not in entry:
        raise ValueError(
            f"[{task_id}] Invalid output_fields entry '{entry}': expected 'name=path'"
        )
    name, path = entry.split("=", 1)
    name = name.strip()
    path = path.strip()
    return name, path


class _CompiledOutput:
    """Compiled output_fields spec carrying per-format extractors.

    'kind' is one of:
      - 'jmespath'  (JSON / JSONLines)  -> compiled jmespath.Expression in 'extractor'
      - 'csv_index' (text/csv)          -> int column index in 'extractor'
      - 'plain'     (text/plain)        -> extractor is None, value = body
    """

    __slots__ = ("name", "kind", "extractor", "raw_path")

    def __init__(self, name: str, kind: str, extractor: Any, raw_path: str):
        self.name = name
        self.kind = kind
        self.extractor = extractor
        self.raw_path = raw_path


def parse_output_fields(spec: str, accept: str, task_id: str) -> list[_CompiledOutput]:
    """Parse and validate output_fields against the chosen accept format."""
    entries = [e.strip() for e in spec.split("|") if e.strip()]
    if not entries:
        raise ValueError(f"[{task_id}] output_fields is empty")

    compiled: list[_CompiledOutput] = []
    seen_names: set[str] = set()
    for entry in entries:
        name, path = _parse_output_field_entry(entry, task_id)
        if name in seen_names:
            raise ValueError(
                f"[{task_id}] Duplicate output field name '{name}' in output_fields"
            )
        seen_names.add(name)

        if accept in ("application/json", "application/jsonlines"):
            if not path:
                raise ValueError(
                    f"[{task_id}] Output field '{name}' has empty JMESPath; "
                    f"specify a path (e.g. 'predictions[*].score')"
                )
            try:
                expr = jmespath.compile(path)
            except Exception as e:
                raise ValueError(
                    f"[{task_id}] Invalid JMESPath for '{name}': {path!r} ({e})"
                )
            compiled.append(_CompiledOutput(name, "jmespath", expr, path))
        elif accept == "text/csv":
            try:
                idx = int(path)
            except ValueError:
                raise ValueError(
                    f"[{task_id}] Output field '{name}' must be an integer "
                    f"column index for text/csv (got {path!r})"
                )
            if idx < 0:
                raise ValueError(
                    f"[{task_id}] Output field '{name}' index must be >= 0 (got {idx})"
                )
            compiled.append(_CompiledOutput(name, "csv_index", idx, path))
        elif accept == "text/plain":
            if path:
                raise ValueError(
                    f"[{task_id}] Output field '{name}' must have an empty path "
                    f"for text/plain (got {path!r})"
                )
            compiled.append(_CompiledOutput(name, "plain", None, path))
        else:
            raise ValueError(
                f"[{task_id}] Unsupported accept type for output_fields: {accept!r}"
            )

    if accept == "text/plain" and len(compiled) != 1:
        raise ValueError(
            f"[{task_id}] text/plain accepts exactly one output field "
            f"(got {len(compiled)})"
        )

    return compiled


def _compile_timestamp_path(
    path: str, accept: str, task_id: str
) -> _CompiledOutput | None:
    """Compile timestamp_path with the same per-format rules as output_fields.

    text/plain ignores timestamp_path entirely (always None).
    """
    p = (path or "").strip()
    if not p or accept == "text/plain":
        return None
    if accept in ("application/json", "application/jsonlines"):
        try:
            expr = jmespath.compile(p)
        except Exception as e:
            raise ValueError(
                f"[{task_id}] Invalid JMESPath for timestamp_path: {p!r} ({e})"
            )
        return _CompiledOutput("__ts__", "jmespath", expr, p)
    if accept == "text/csv":
        try:
            idx = int(p)
        except ValueError:
            raise ValueError(
                f"[{task_id}] timestamp_path must be an integer column index "
                f"for text/csv (got {p!r})"
            )
        if idx < 0:
            raise ValueError(
                f"[{task_id}] timestamp_path index must be >= 0 (got {idx})"
            )
        return _CompiledOutput("__ts__", "csv_index", idx, p)
    raise ValueError(
        f"[{task_id}] Unsupported accept type for timestamp_path: {accept!r}"
    )


def autodetect_timestamp_ns(value: Any) -> int:
    """Autodetect a timestamp value's format and return nanoseconds since epoch."""
    if isinstance(value, bool):
        raise ValueError(f"timestamp value cannot be a boolean: {value!r}")
    if isinstance(value, int):
        if value > 10**15:
            ns = value
        elif value >= 10**9:
            ns = value * 1_000_000_000
        else:
            raise ValueError(
                f"integer timestamp out of supported range (need ns or seconds): {value}"
            )
    elif isinstance(value, float):
        ns = int(value * 1_000_000_000)
    elif isinstance(value, str):
        try:
            ns = pd.to_datetime(value, utc=True).value
        except Exception as e:
            raise ValueError(f"cannot parse timestamp string {value!r}: {e}")
    else:
        raise ValueError(f"unsupported timestamp type: {type(value).__name__}")
    if not (_INT64_MIN <= ns <= _INT64_MAX):
        raise ValueError(
            f"timestamp out of int64 range [{_INT64_MIN}, {_INT64_MAX}]: {ns}"
        )
    return ns


def to_nanoseconds(value: Any) -> int:
    """Convert a query 'time' value to nanoseconds since epoch (UTC)."""
    if isinstance(value, int):
        return value
    return pd.to_datetime(value, utc=True).value


def add_typed_field(line, name: str, value: Any) -> None:
    """Add a field to LineBuilder picking the right type based on the value."""
    if isinstance(value, bool):
        # bool is a subclass of int; check it first
        line.bool_field(name, value)
    elif isinstance(value, int):
        if not (_INT64_MIN <= value <= _INT64_MAX):
            raise ValueError(
                f"int field '{name}' out of int64 range "
                f"[{_INT64_MIN}, {_INT64_MAX}]: {value}"
            )
        line.int64_field(name, value)
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"float field '{name}' is not finite: {value}")
        line.float64_field(name, value)
    elif isinstance(value, str):
        line.string_field(name, value)
    else:
        raise ValueError(
            f"unsupported value type for field '{name}': {type(value).__name__}"
        )


def build_query(
    measurement: str,
    fields: list[str],
    interval: str,
    limit: int,
    tag_filter_clause: str = "",
) -> str:
    """Build the SQL query that pulls source rows for inference."""
    seen: dict[str, None] = {}
    for f in ["time"] + list(fields):
        seen[f] = None
    cols = ", ".join('"' + c.replace('"', '""') + '"' for c in seen.keys())
    safe_measurement = measurement.replace("'", "''")
    return (
        f"SELECT {cols} FROM '{safe_measurement}' "
        f"WHERE time > now() - INTERVAL '{interval}'"
        f"{tag_filter_clause} "
        f"ORDER BY time DESC LIMIT {int(limit)}"
    )


def build_request_body(
    rows: list[dict[str, Any]],
    tokens: list[tuple[str, str, str | None]],
    content_type: str,
    json_shape: str,
    extra_body: dict[str, Any] | None = None,
) -> str:
    """Build a SageMaker request body for one or more rows.

    For JSON shapes the body is built as a Python object first, then optionally
    deep-merged with extra_body, then serialised. CSV bypasses extra_body
    (config layer rejects this combination).
    """
    if not rows:
        raise ValueError("build_request_body received no rows")

    if content_type == "text/csv":
        lines: list[str] = []
        for row in rows:
            parts = [str(row[v]) if k == "field" else v for k, v, _ in tokens]
            lines.append(",".join(parts))
        return "\n".join(lines)

    per_row_arrays: list[list[Any]] = []
    per_row_objects: list[dict[str, Any]] = []
    is_object_shape = json_shape in _OBJECT_JSON_SHAPES
    for row in rows:
        if is_object_shape:
            obj: dict[str, Any] = {}
            for kind, value, alias in tokens:
                if kind == "field":
                    key = alias if alias else value
                    obj[key] = jsonify_cell(row[value])
                else:
                    if alias is None:
                        raise ValueError(
                            f"json_shape '{json_shape}' requires a key alias "
                            f"for literal token {value!r} (use 'literal:alias')"
                        )
                    obj[alias] = coerce_literal(value)
            per_row_objects.append(obj)
        else:
            values = [
                jsonify_cell(row[v]) if k == "field" else coerce_literal(v)
                for k, v, _ in tokens
            ]
            per_row_arrays.append(values)

    body_obj: Any
    if json_shape == "instances_array":
        body_obj = {"instances": per_row_arrays}
    elif json_shape == "instances_object":
        body_obj = {"instances": per_row_objects}
    elif json_shape == "instances_features":
        body_obj = {"instances": [{"features": arr} for arr in per_row_arrays]}
    elif json_shape == "raw_array":
        body_obj = per_row_arrays
    elif json_shape == "inputs":
        body_obj = {"inputs": per_row_arrays[0]}
    elif json_shape == "inputs_array":
        body_obj = {"inputs": per_row_arrays}
    elif json_shape == "inputs_flat":
        # Validated at config: tokens has exactly one ('field', col, _)
        col = tokens[0][1]
        body_obj = {"inputs": [jsonify_cell(row[col]) for row in rows]}
    elif json_shape == "inputs_timeseries":
        # Validated at config: tokens has exactly one ('field', col, _)
        col = tokens[0][1]
        body_obj = {"inputs": [{"target": [jsonify_cell(row[col]) for row in rows]}]}
    elif json_shape == "raw_object":
        body_obj = per_row_objects[0]
    else:
        raise ValueError(f"unsupported json_shape: {json_shape}")

    if extra_body:
        if not isinstance(body_obj, dict):
            raise ValueError(
                f"extra_body cannot be merged into a top-level list "
                f"(json_shape '{json_shape}')"
            )
        deep_merge(body_obj, extra_body, [])

    return json.dumps(body_obj)


def parse_extra_body(spec: str, task_id: str) -> dict[str, Any]:
    """Parse a pipe-separated 'path=value' spec into a nested dict.

    Path uses dots for nesting. Value is coerced via coerce_literal.
    Duplicate paths or path/leaf conflicts raise ValueError at config time.
    """
    out: dict[str, Any] = {}
    for raw in spec.split("|"):
        pair = raw.strip()
        if not pair:
            continue
        if "=" not in pair:
            raise ValueError(
                f"[{task_id}] Invalid extra_body entry '{pair}': expected 'path=value'"
            )
        path, value = pair.split("=", 1)
        path = path.strip()
        value = value.strip()
        if not path:
            raise ValueError(f"[{task_id}] empty path in extra_body entry: {pair!r}")
        keys = path.split(".")
        for k in keys:
            if not k:
                raise ValueError(
                    f"[{task_id}] empty key segment in extra_body path '{path}'"
                )
        coerced = coerce_literal(value)
        cur = out
        for k in keys[:-1]:
            if k in cur:
                if not isinstance(cur[k], dict):
                    raise ValueError(
                        f"[{task_id}] extra_body path conflict: '{path}' "
                        f"(intermediate '{k}' is not a dict)"
                    )
                cur = cur[k]
            else:
                cur[k] = {}
                cur = cur[k]
        last = keys[-1]
        if last in cur:
            raise ValueError(f"[{task_id}] duplicate extra_body path: '{path}'")
        cur[last] = coerced
    return out


def deep_merge(base: dict, extra: dict, path: list[str]) -> None:
    """Merge extra into base in place; raises on any leaf conflict."""
    for k, v in extra.items():
        cur_path = path + [k]
        if k not in base:
            base[k] = v
        elif isinstance(base[k], dict) and isinstance(v, dict):
            deep_merge(base[k], v, cur_path)
        else:
            raise ValueError(
                f"extra_body conflicts with main body at '{'.'.join(cur_path)}'"
            )


def _coerce_csv_cell(raw: str) -> Any:
    """Best-effort type coerce for a CSV cell."""
    s = raw.strip()
    if s == "":
        return None
    if s.lower() == "true":
        return True
    if s.lower() == "false":
        return False
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        return s


def _normalize_extracted(value: Any, name: str, accept: str, task_id: str) -> Any:
    """Reject list/dict for output values; coerce CSV strings to typed values."""
    if isinstance(value, (list, dict)):
        raise ValueError(
            f"[{task_id}] Output field '{name}' resolved to a {type(value).__name__}; "
            f"enumerate indices instead (e.g. 'p0=probs[0]|p1=probs[1]')"
        )
    if accept == "text/csv" and isinstance(value, str):
        return _coerce_csv_cell(value)
    return value


def _extract_from_json_forecast(
    body: str,
    output_fields: list[_CompiledOutput],
    timestamp_compiled: _CompiledOutput | None,
    task_id: str,
    influxdb3_local,
) -> list[tuple[int | None, dict[str, Any]]]:
    """Extract rows in forecast mode: array fields → indexed rows, scalars → broadcast."""
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as e:
        raise ValueError(f"[{task_id}] invalid JSON in response: {e}")

    name_to_value: dict[str, Any] = {}
    for cf in output_fields:
        result = cf.extractor.search(parsed)
        if result is None:
            raise ValueError(
                f"[{task_id}] output_fields path '{cf.raw_path}' for '{cf.name}' "
                f"returned None; check your JMESPath or the response structure"
            )
        name_to_value[cf.name] = result

    array_lengths = [len(v) for v in name_to_value.values() if isinstance(v, list)]
    if not array_lengths:
        n = 1
    else:
        n = min(array_lengths)
        if max(array_lengths) != n:
            influxdb3_local.warn(
                f"[{task_id}] forecast output paths returned different array lengths "
                f"(min={n}, max={max(array_lengths)}); truncating to {n}"
            )

    ts_array: list[Any] | None = None
    if timestamp_compiled is not None:
        ts_result = timestamp_compiled.extractor.search(parsed)
        if ts_result is None:
            influxdb3_local.error(
                f"[{task_id}] timestamp_path '{timestamp_compiled.raw_path}' "
                f"returned None; skipping all predictions in this batch"
            )
            return []
        ts_array = ts_result if isinstance(ts_result, list) else [ts_result] * n

    out: list[tuple[int | None, dict[str, Any]]] = []
    for i in range(n):
        fields: dict[str, Any] = {}
        skip = False
        for cf in output_fields:
            v = name_to_value[cf.name]
            value = v[i] if isinstance(v, list) else v
            if value is None:
                influxdb3_local.warn(
                    f"[{task_id}] forecast row {i}: '{cf.name}' is None; skipping row"
                )
                skip = True
                break
            if isinstance(value, (list, dict)):
                raise ValueError(
                    f"[{task_id}] forecast row {i}: '{cf.name}' resolved to a nested "
                    f"{type(value).__name__}; use a more specific JMESPath"
                )
            fields[cf.name] = value
        if skip:
            continue

        ts_ns: int | None = None
        if timestamp_compiled is not None:
            ts_val = ts_array[i] if ts_array and i < len(ts_array) else None
            if ts_val is None:
                influxdb3_local.error(
                    f"[{task_id}] forecast row {i}: timestamp resolved to None; "
                    f"skipping prediction"
                )
                continue
            try:
                ts_ns = autodetect_timestamp_ns(ts_val)
            except ValueError as e:
                influxdb3_local.error(
                    f"[{task_id}] forecast row {i}: bad timestamp {ts_val!r}: "
                    f"{e}; skipping prediction"
                )
                continue
        out.append((ts_ns, fields))
    return out


def extract_outputs(
    response_body: str,
    response_ct: str | None,
    accept: str,
    output_fields: list[_CompiledOutput],
    timestamp_compiled: _CompiledOutput | None,
    task_id: str,
    influxdb3_local,
    forecast_output: bool = False,
) -> list[tuple[int | None, dict[str, Any]]]:
    """Extract M (timestamp_or_None, {field_name: value}) tuples from the response.

    None timestamp means "use time.time_ns() at write time".
    """
    body = response_body.strip() if response_body else ""
    if not body:
        raise ValueError(f"[{task_id}] empty response body from SageMaker")

    effective = accept
    if response_ct:
        ct_main = response_ct.split(";", 1)[0].strip().lower()
        if ct_main in _VALID_ACCEPT_TYPES and ct_main != accept:
            influxdb3_local.warn(
                f"[{task_id}] response Content-Type '{ct_main}' differs from "
                f"requested accept '{accept}'; parsing as '{ct_main}'"
            )
            effective = ct_main

    if effective == "application/json":
        if forecast_output:
            return _extract_from_json_forecast(
                body, output_fields, timestamp_compiled, task_id, influxdb3_local
            )
        return _extract_from_json(
            body, output_fields, timestamp_compiled, task_id, influxdb3_local
        )
    if effective == "application/jsonlines":
        return _extract_from_jsonlines(
            body, output_fields, timestamp_compiled, task_id, influxdb3_local
        )
    if effective == "text/csv":
        return _extract_from_csv(
            body, output_fields, timestamp_compiled, task_id, influxdb3_local
        )
    if effective == "text/plain":
        return _extract_from_plain(body, output_fields, task_id)
    raise ValueError(
        f"[{task_id}] cannot parse response with Content-Type '{effective}'"
    )


def _extract_from_json(
    body: str,
    output_fields: list[_CompiledOutput],
    timestamp_compiled: _CompiledOutput | None,
    task_id: str,
    influxdb3_local,
) -> list[tuple[int | None, dict[str, Any]]]:
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as e:
        raise ValueError(f"[{task_id}] invalid JSON in response: {e}")

    name_to_array: dict[str, list[Any]] = {}
    for cf in output_fields:
        result = cf.extractor.search(parsed)
        if result is None:
            raise ValueError(
                f"[{task_id}] output_fields path '{cf.raw_path}' for '{cf.name}' "
                f"returned None; check your JMESPath or the response structure"
            )
        if not isinstance(result, list):
            result = [result]
        name_to_array[cf.name] = result

    ts_array: list[Any] | None = None
    if timestamp_compiled is not None:
        ts_result = timestamp_compiled.extractor.search(parsed)
        if ts_result is None:
            influxdb3_local.error(
                f"[{task_id}] timestamp_path '{timestamp_compiled.raw_path}' "
                f"returned None; skipping all predictions in this batch"
            )
            return []
        if not isinstance(ts_result, list):
            ts_result = [ts_result]
        ts_array = ts_result

    return _zip_arrays(
        name_to_array,
        ts_array,
        output_fields,
        "application/json",
        task_id,
        influxdb3_local,
        timestamp_required=timestamp_compiled is not None,
    )


def _extract_from_jsonlines(
    body: str,
    output_fields: list[_CompiledOutput],
    timestamp_compiled: _CompiledOutput | None,
    task_id: str,
    influxdb3_local,
) -> list[tuple[int | None, dict[str, Any]]]:
    out: list[tuple[int | None, dict[str, Any]]] = []
    for idx, raw_line in enumerate(body.splitlines()):
        line = raw_line.strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"[{task_id}] invalid JSON on line {idx} of jsonlines response: {e}"
            )
        fields: dict[str, Any] = {}
        skip = False
        for cf in output_fields:
            value = cf.extractor.search(parsed)
            if value is None:
                influxdb3_local.warn(
                    f"[{task_id}] jsonlines line {idx}: path '{cf.raw_path}' "
                    f"for '{cf.name}' resolved to None; skipping row"
                )
                skip = True
                break
            value = _normalize_extracted(
                value, cf.name, "application/jsonlines", task_id
            )
            fields[cf.name] = value
        if skip:
            continue

        ts_ns: int | None = None
        if timestamp_compiled is not None:
            ts_value = timestamp_compiled.extractor.search(parsed)
            if ts_value is None:
                influxdb3_local.error(
                    f"[{task_id}] jsonlines line {idx}: timestamp_path resolved "
                    f"to None; skipping prediction"
                )
                continue
            try:
                ts_ns = autodetect_timestamp_ns(ts_value)
            except ValueError as e:
                influxdb3_local.error(
                    f"[{task_id}] jsonlines line {idx}: bad timestamp "
                    f"{ts_value!r}: {e}; skipping prediction"
                )
                continue
        out.append((ts_ns, fields))
    return out


def _extract_from_csv(
    body: str,
    output_fields: list[_CompiledOutput],
    timestamp_compiled: _CompiledOutput | None,
    task_id: str,
    influxdb3_local,
) -> list[tuple[int | None, dict[str, Any]]]:
    reader = csv.reader(io.StringIO(body))
    out: list[tuple[int | None, dict[str, Any]]] = []
    for idx, row in enumerate(reader):
        if not row:
            continue
        fields: dict[str, Any] = {}
        skip = False
        for cf in output_fields:
            if cf.extractor >= len(row):
                influxdb3_local.warn(
                    f"[{task_id}] csv row {idx}: column index {cf.extractor} "
                    f"out of range for '{cf.name}' (row has {len(row)} cols); "
                    f"skipping row"
                )
                skip = True
                break
            value = _coerce_csv_cell(row[cf.extractor])
            if value is None:
                influxdb3_local.warn(
                    f"[{task_id}] csv row {idx}: '{cf.name}' is empty; skipping row"
                )
                skip = True
                break
            fields[cf.name] = value
        if skip:
            continue

        ts_ns: int | None = None
        if timestamp_compiled is not None:
            if timestamp_compiled.extractor >= len(row):
                influxdb3_local.error(
                    f"[{task_id}] csv row {idx}: timestamp column index "
                    f"{timestamp_compiled.extractor} out of range; skipping prediction"
                )
                continue
            ts_value = _coerce_csv_cell(row[timestamp_compiled.extractor])
            if ts_value is None:
                influxdb3_local.error(
                    f"[{task_id}] csv row {idx}: timestamp cell is empty; "
                    f"skipping prediction"
                )
                continue
            try:
                ts_ns = autodetect_timestamp_ns(ts_value)
            except ValueError as e:
                influxdb3_local.error(
                    f"[{task_id}] csv row {idx}: bad timestamp "
                    f"{ts_value!r}: {e}; skipping prediction"
                )
                continue
        out.append((ts_ns, fields))
    return out


def _extract_from_plain(
    body: str,
    output_fields: list[_CompiledOutput],
    task_id: str,
) -> list[tuple[int | None, dict[str, Any]]]:
    cf = output_fields[0]
    coerced = _coerce_csv_cell(body)
    if coerced is None:
        raise ValueError(f"[{task_id}] text/plain response is empty after stripping")
    return [(None, {cf.name: coerced})]


def _zip_arrays(
    name_to_array: dict[str, list[Any]],
    ts_array: list[Any] | None,
    output_fields: list[_CompiledOutput],
    accept: str,
    task_id: str,
    influxdb3_local,
    timestamp_required: bool = False,
) -> list[tuple[int | None, dict[str, Any]]]:
    """Zip per-field arrays into per-row tuples using the min-length rule."""
    lengths = [len(name_to_array[cf.name]) for cf in output_fields]
    if ts_array is not None:
        lengths.append(len(ts_array))
    if not lengths:
        return []
    m = min(lengths)
    if max(lengths) != m:
        influxdb3_local.warn(
            f"[{task_id}] output paths returned different lengths "
            f"(min={m}, max={max(lengths)}); truncating to {m}"
        )

    out: list[tuple[int | None, dict[str, Any]]] = []
    for i in range(m):
        fields: dict[str, Any] = {}
        skip = False
        for cf in output_fields:
            value = name_to_array[cf.name][i]
            if value is None:
                influxdb3_local.warn(
                    f"[{task_id}] row {i}: '{cf.name}' is None at index {i}; "
                    f"skipping row"
                )
                skip = True
                break
            value = _normalize_extracted(value, cf.name, accept, task_id)
            fields[cf.name] = value
        if skip:
            continue

        ts_ns: int | None = None
        if timestamp_required:
            ts_value = (
                ts_array[i] if ts_array is not None and i < len(ts_array) else None
            )
            if ts_value is None:
                influxdb3_local.error(
                    f"[{task_id}] row {i}: timestamp_path resolved to None; "
                    f"skipping prediction"
                )
                continue
            try:
                ts_ns = autodetect_timestamp_ns(ts_value)
            except ValueError as e:
                influxdb3_local.error(
                    f"[{task_id}] row {i}: bad timestamp {ts_value!r}: "
                    f"{e}; skipping prediction"
                )
                continue
        out.append((ts_ns, fields))
    return out


def _parse_interval(interval: str, task_id: str) -> tuple[int, str]:
    """Parse interval string into (magnitude, unit_word) for SQL INTERVAL injection.

    Same unit mapping as downsampler/threshold plugins:
    s→seconds, min→minutes, h→hours, d→days, w→weeks,
    m/q/y → converted to days (1m≈30.42d, 1q≈91.25d, 1y=365d).
    """
    unit_mapping: dict = {
        "s": "seconds",
        "min": "minutes",
        "h": "hours",
        "d": "days",
        "w": "weeks",
        "m": "days",
        "q": "days",
        "y": "days",
    }
    day_conversions: dict = {
        "m": 30.42,
        "q": 91.25,
        "y": 365.0,
    }
    match = re.fullmatch(r"(\d+)([a-zA-Z]+)", interval)
    if match:
        number_part, unit = match.groups()
        magnitude: int = int(number_part)
        if unit in unit_mapping and magnitude >= 1:
            if unit in day_conversions:
                days: int = int(magnitude * day_conversions[unit])
                return days, "days"
            return magnitude, unit_mapping[unit]
    raise ValueError(f"[{task_id}] Invalid interval format: {interval!r}.")


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    if s in ("true", "1", "yes", "y", "on"):
        return True
    if s in ("false", "0", "no", "n", "off"):
        return False
    raise ValueError(f"cannot parse boolean from {value!r}")


class SageMakerConfig:
    """Configuration loader and validator for the SageMaker inference plugin."""

    def __init__(self, influxdb3_local, args: dict[str, str] | None, task_id: str):
        self.influxdb3_local = influxdb3_local
        self.args: dict = args or {}
        self.task_id: str = task_id
        self.config: dict[str, Any] = self._build_config()

    @staticmethod
    def _resolve_path(path: str, description: str) -> str:
        """Resolve path — absolute as-is, relative resolved from the plugin dir
        (INFLUXDB3_PLUGIN_DIR or PLUGIN_DIR), falling back to server-derivable
        locations."""
        if os.path.isabs(path):
            return path
        plugin_dir: str | None = os.environ.get(
            "INFLUXDB3_PLUGIN_DIR"
        ) or os.environ.get("PLUGIN_DIR")
        if plugin_dir:
            return os.path.join(plugin_dir, path)

        # Fallback for servers where neither variable is present in the
        # process environment: VIRTUAL_ENV is exported by the processing
        # engine and its default venv location is <plugin-dir>/.venv.
        if virtual_env := os.environ.get("VIRTUAL_ENV"):
            candidate = os.path.join(str(Path(virtual_env).parent), path)
            if os.path.exists(candidate):
                return candidate

        raise ValueError(
            f"Neither INFLUXDB3_PLUGIN_DIR nor PLUGIN_DIR environment "
            f"variable is set and {description} path '{path}' was not "
            f"found via the VIRTUAL_ENV fallback. Required for relative "
            f"{description} path."
        )

    def _normalize_from_args(self, args: dict) -> dict[str, Any]:
        """Convert trigger args (all strings) to the internal normalized format."""
        feature_order_raw = str(args.get("feature_order", ""))
        feature_order = [t.strip() for t in feature_order_raw.split("|") if t.strip()]

        output_fields_raw = str(args.get("output_fields", ""))
        output_fields = [t.strip() for t in output_fields_raw.split("|") if t.strip()]

        extra_body_raw = str(args.get("extra_body", ""))
        extra_body = [t.strip() for t in extra_body_raw.split("|") if t.strip()]

        limit_raw = args.get("limit", _DEFAULT_LIMIT)
        try:
            limit = int(limit_raw)
        except (ValueError, TypeError):
            raise ValueError(
                f"[{self.task_id}] Invalid limit: '{limit_raw}'. Expected integer."
            )

        batch_inference = _parse_bool(args.get("batch_inference", "true"))
        forecast_output = _parse_bool(args.get("forecast_output", "false"))

        return {
            "endpoint_name": str(args.get("endpoint_name", "")),
            "source_measurement": str(args.get("source_measurement", "")),
            "feature_order": feature_order,
            "output_fields": output_fields,
            "extra_body": extra_body,
            "tag_values": str(args.get("tag_values", "") or ""),
            "batch_inference": batch_inference,
            "forecast_output": forecast_output,
            "limit": limit,
            "accept": args.get("accept"),
            "content_type": args.get("content_type"),
            "json_shape": args.get("json_shape"),
            "region": args.get("region"),
            "interval": args.get("interval"),
            "target_measurement": args.get("target_measurement"),
            "target_database": args.get("target_database"),
            "target_model": args.get("target_model"),
            "timestamp_path": str(args.get("timestamp_path", "") or ""),
        }

    def _normalize_from_toml(self, config_file: str) -> dict[str, Any]:
        """Load TOML config file and normalize to the internal format."""
        if not config_file.endswith(".toml"):
            raise ValueError("Invalid config file format: expected a .toml file")

        config_path = self._resolve_path(config_file, "configuration file")
        try:
            with open(config_path, "rb") as f:
                raw: dict[str, Any] = tomllib.load(f)
        except Exception:
            raise ValueError("Failed to read config file") from None

        feature_order = raw.get("feature_order", [])
        if not isinstance(feature_order, list):
            raise ValueError(
                f"[{self.task_id}] TOML 'feature_order' must be a list of strings"
            )

        output_fields = raw.get("output_fields", [])
        if not isinstance(output_fields, list):
            raise ValueError(
                f"[{self.task_id}] TOML 'output_fields' must be a list of strings"
            )

        extra_body = raw.get("extra_body", [])
        if not isinstance(extra_body, list):
            raise ValueError(
                f"[{self.task_id}] TOML 'extra_body' must be a list of strings"
            )

        limit_raw = raw.get("limit", _DEFAULT_LIMIT)
        try:
            limit = int(limit_raw)
        except (ValueError, TypeError):
            raise ValueError(
                f"[{self.task_id}] Invalid limit: '{limit_raw}'. Expected integer."
            )

        batch_inference = _parse_bool(raw.get("batch_inference", True))
        forecast_output = _parse_bool(raw.get("forecast_output", False))

        tag_values = raw.get("tag_values", {})
        if not isinstance(tag_values, dict):
            raise ValueError(
                f"[{self.task_id}] TOML 'tag_values' must be a table [tag_values] "
                f"mapping tag keys to lists of values"
            )

        return {
            "endpoint_name": str(raw.get("endpoint_name", "")),
            "source_measurement": str(raw.get("source_measurement", "")),
            "feature_order": [str(t) for t in feature_order],
            "output_fields": [str(t) for t in output_fields],
            "extra_body": [str(t) for t in extra_body],
            "tag_values": tag_values,
            "batch_inference": batch_inference,
            "forecast_output": forecast_output,
            "limit": limit,
            "accept": raw.get("accept"),
            "content_type": raw.get("content_type"),
            "json_shape": raw.get("json_shape"),
            "region": raw.get("region"),
            "interval": raw.get("interval"),
            "target_measurement": raw.get("target_measurement"),
            "target_database": raw.get("target_database"),
            "target_model": raw.get("target_model"),
            "timestamp_path": str(raw.get("timestamp_path", "") or ""),
        }

    def _build_config(self) -> dict[str, Any]:
        """Validate and compile normalized config into the final plugin config dict."""
        config_file: str | None = self.args.get("config_file_path")
        n: dict[str, Any] = (
            self._normalize_from_toml(config_file)
            if config_file
            else self._normalize_from_args(self.args)
        )

        required = ["endpoint_name", "source_measurement"]
        missing = [k for k in required if not n.get(k)]
        if not n["feature_order"]:
            missing.append("feature_order")
        if not n["output_fields"]:
            missing.append("output_fields")
        if missing:
            raise ValueError(
                f"[{self.task_id}] Missing required arguments: {', '.join(missing)}"
            )

        content_type: str = str(n["content_type"] or _DEFAULT_CONTENT_TYPE)
        if content_type not in _VALID_CONTENT_TYPES:
            raise ValueError(
                f"[{self.task_id}] Invalid content_type: '{content_type}'. "
                f"Supported: {', '.join(sorted(_VALID_CONTENT_TYPES))}"
            )

        accept: str = str(n["accept"] or content_type)
        if accept not in _VALID_ACCEPT_TYPES:
            raise ValueError(
                f"[{self.task_id}] Invalid or unsupported accept: '{accept}'. "
                f"Supported: {', '.join(sorted(_VALID_ACCEPT_TYPES))} "
                f"(application/x-recordio-protobuf is intentionally not supported)"
            )

        json_shape: str = str(n["json_shape"] or _DEFAULT_JSON_SHAPE)
        if content_type == "application/json" and json_shape not in _VALID_JSON_SHAPES:
            raise ValueError(
                f"[{self.task_id}] Invalid json_shape: '{json_shape}'. "
                f"Supported: {', '.join(sorted(_VALID_JSON_SHAPES))}"
            )

        limit: int = n["limit"]
        if limit < 1:
            raise ValueError(f"[{self.task_id}] limit must be a positive integer")

        batch_inference: bool = n["batch_inference"]
        forecast_output: bool = n["forecast_output"]

        if forecast_output and accept != "application/json":
            raise ValueError(
                f"[{self.task_id}] forecast_output=true requires accept=application/json "
                f"(got '{accept}')"
            )

        if json_shape in _TIMESERIES_JSON_SHAPES and not batch_inference:
            self.influxdb3_local.warn(
                f"[{self.task_id}] json_shape 'inputs_timeseries' always sends all rows "
                f"in one request; batch_inference=false is ignored for this shape"
            )

        if (
            batch_inference
            and limit > 1
            and content_type == "application/json"
            and json_shape in _NON_BATCH_JSON_SHAPES
        ):
            raise ValueError(
                f"[{self.task_id}] json_shape '{json_shape}' is not "
                f"batch-compatible. Either set batch_inference=false, set limit=1, "
                f"or use one of: instances_array, instances_object, raw_array."
            )
        if accept == "text/plain" and batch_inference and limit > 1:
            raise ValueError(
                f"[{self.task_id}] accept=text/plain requires limit=1 or "
                f"batch_inference=false"
            )

        tokens: list[tuple[str, str, str | None]] = parse_feature_order(
            "|".join(n["feature_order"])
        )
        if not tokens:
            raise ValueError(f"[{self.task_id}] feature_order is empty after parsing")

        is_object_shape = (
            content_type == "application/json" and json_shape in _OBJECT_JSON_SHAPES
        )
        if is_object_shape:
            for kind, value, alias in tokens:
                if kind == "literal" and alias is None:
                    raise ValueError(
                        f"[{self.task_id}] json_shape '{json_shape}' requires "
                        f"every literal token to carry an alias key "
                        f"(use 'literal:alias'); offending token: {value!r}"
                    )
        else:
            if any(alias is not None for _, _, alias in tokens):
                self.influxdb3_local.warn(
                    f"[{self.task_id}] feature_order aliases are ignored when "
                    f"content_type={content_type} / json_shape={json_shape} "
                    f"(aliases only apply to instances_object and raw_object)"
                )

        if content_type == "application/json" and json_shape in ("inputs_flat", "inputs_timeseries"):
            if len(tokens) != 1 or tokens[0][0] != "field":
                raise ValueError(
                    f"[{self.task_id}] json_shape '{json_shape}' requires exactly "
                    f"one {{col}} token in feature_order (no literals/padding)"
                )

        source_measurement: str = n["source_measurement"]
        columns = get_measurement_columns(
            self.influxdb3_local, source_measurement, self.task_id
        )
        feature_cols = [v for k, v, _ in tokens if k == "field"]
        unknown = [c for c in feature_cols if c not in columns]
        if unknown:
            raise ValueError(
                f"[{self.task_id}] feature_order references columns that don't "
                f"exist in '{source_measurement}': {', '.join(unknown)}. "
                f"Available columns: {', '.join(sorted(columns))}"
            )

        # tag_values: str from args path, dict from TOML path
        tag_values_raw = n["tag_values"]
        if isinstance(tag_values_raw, dict):
            tag_values: dict[str, list[str]] = {}
            for tag_name, values in tag_values_raw.items():
                if tag_name not in columns:
                    raise ValueError(
                        f"[{self.task_id}] Tag '{tag_name}' in [tag_values] is not a "
                        f"column of '{source_measurement}'"
                    )
                if not isinstance(values, list) or not values:
                    raise ValueError(
                        f"[{self.task_id}] [tag_values].{tag_name} must be a "
                        f"non-empty list of strings"
                    )
                tag_values[tag_name] = [str(v) for v in values]
        else:
            tag_values_spec = str(tag_values_raw or "")
            tag_values = (
                parse_tag_values(tag_values_spec, columns, self.task_id)
                if tag_values_spec.strip()
                else {}
            )

        output_fields = parse_output_fields(
            "|".join(n["output_fields"]), accept, self.task_id
        )
        timestamp_compiled = _compile_timestamp_path(
            n["timestamp_path"], accept, self.task_id
        )

        extra_body_spec = "|".join(n["extra_body"])
        if extra_body_spec.strip():
            if content_type == "text/csv":
                raise ValueError(
                    f"[{self.task_id}] extra_body cannot be used with "
                    f"content_type=text/csv (CSV body has no JSON keys)"
                )
            if json_shape in _TOP_LEVEL_LIST_JSON_SHAPES:
                raise ValueError(
                    f"[{self.task_id}] extra_body cannot be merged into "
                    f"json_shape '{json_shape}' (top-level is a list)"
                )
            extra_body = parse_extra_body(extra_body_spec, self.task_id)
        else:
            extra_body = {}

        target_model: str | None = (str(n["target_model"] or "")).strip() or None
        target_measurement: str = str(
            n["target_measurement"] or f"{source_measurement}_predictions"
        )

        return {
            "endpoint_name": n["endpoint_name"],
            "region": str(n["region"] or _DEFAULT_REGION),
            "source_measurement": source_measurement,
            "target_measurement": target_measurement,
            "target_database": n["target_database"] or None,
            "interval": "{} {}".format(
                *_parse_interval(str(n["interval"] or _DEFAULT_INTERVAL), self.task_id)
            ),
            "limit": limit,
            "content_type": content_type,
            "accept": accept,
            "json_shape": json_shape,
            "batch_inference": batch_inference,
            "forecast_output": forecast_output,
            "feature_order_tokens": tokens,
            "feature_cols": feature_cols,
            "tag_values": tag_values,
            "output_fields": output_fields,
            "timestamp_compiled": timestamp_compiled,
            "extra_body": extra_body,
            "target_model": target_model,
        }

    def get(self, key: str, default: Any = None) -> Any:
        return self.config.get(key, default)


class SageMakerInvoker:
    """Thin wrapper around boto3 sagemaker-runtime for endpoint invocation."""

    def __init__(self, endpoint_name: str, region: str, task_id: str, influxdb3_local):
        self.endpoint_name: str = endpoint_name
        self.region: str = region
        self.task_id: str = task_id
        self.influxdb3_local = influxdb3_local
        self._client = None

    def _get_client(self):
        if self._client is None:
            self._client = boto3.client("sagemaker-runtime", region_name=self.region)
        return self._client

    def invoke(
        self,
        body: str,
        content_type: str,
        accept: str,
        target_model: str | None = None,
    ) -> tuple[str, str | None]:
        client = self._get_client()
        kwargs: dict[str, Any] = {
            "EndpointName": self.endpoint_name,
            "ContentType": content_type,
            "Accept": accept,
            "Body": body,
        }
        if target_model:
            kwargs["TargetModel"] = target_model
        response = client.invoke_endpoint(**kwargs)
        response_body: str = response["Body"].read().decode("utf-8")
        response_ct: str | None = response.get("ContentType")
        return response_body, response_ct


def _write_lines(
    influxdb3_local,
    payload: _LineBuilderInterface,
    target_database: str | None,
):
    """Write a LineBuilder/_BatchLines to the trigger DB or a target DB."""
    if target_database:
        influxdb3_local.write_sync_to_db(target_database, payload, no_sync=True)
    else:
        influxdb3_local.write_sync(payload, no_sync=True)


def _build_line(
    config: dict[str, Any],
    fields: dict[str, Any],
    ts_ns: int,
    single_value_tags: dict[str, str],
    task_id: str,
) -> Any:
    """Build a LineBuilder for one prediction row."""
    line = LineBuilder(config["target_measurement"])

    line.tag("sagemaker_endpoint", str(config["endpoint_name"]))
    line.tag("sagemaker_source_measurement", str(config["source_measurement"]))
    line.tag("sagemaker_region", str(config["region"]))
    if config.get("target_model"):
        line.tag("sagemaker_model", str(config["target_model"]))

    for tag_name, tag_value in single_value_tags.items():
        line.tag(tag_name, tag_value)

    for field_name, value in fields.items():
        try:
            add_typed_field(line, field_name, value)
        except ValueError as e:
            raise ValueError(f"[{task_id}] {e}")

    line.time_ns(ts_ns)
    return line


def _next_ns(prev: int) -> int:
    """Return time.time_ns(), bumped by 1 if it duplicates the previous timestamp."""
    t = time.time_ns()
    if t <= prev:
        t = prev + 1
    return t


def process_scheduled_call(
    influxdb3_local, call_time: datetime, args: dict | None = None
):
    """Main plugin entry point - called on schedule by InfluxDB 3 Processing Engine.

    Args:
        influxdb3_local: Shared API for InfluxDB operations
        call_time: Timestamp when trigger was called
        args: Trigger arguments
    """
    task_id: str = str(uuid.uuid4())

    if not args:
        influxdb3_local.error(f"[{task_id}] No arguments provided")
        return

    cached_config: dict | None = None
    try:
        cached_config = influxdb3_local.cache.get(_CONFIG_CACHE_KEY)
        if cached_config is None:
            config_loader = SageMakerConfig(influxdb3_local, args, task_id)
            cached_config = config_loader.config
            influxdb3_local.cache.put(
                _CONFIG_CACHE_KEY, cached_config, _CONFIG_CACHE_TTL_SEC
            )
            influxdb3_local.info(
                f"[{task_id}] SageMaker plugin initialized for endpoint "
                f"'{cached_config['endpoint_name']}' "
                f"(content_type={cached_config['content_type']}, "
                f"accept={cached_config['accept']}, "
                f"json_shape={cached_config['json_shape']}, "
                f"batch_inference={cached_config['batch_inference']}, "
                f"forecast_output={cached_config['forecast_output']})"
            )

        config: dict[str, Any] = cached_config
        target_database: str | None = config.get("target_database")

        feature_cols: list[str] = config["feature_cols"]
        tag_filter_clause: str = generate_tag_filter_clause(config["tag_values"])
        query: str = build_query(
            config["source_measurement"],
            feature_cols,
            config["interval"],
            config["limit"],
            tag_filter_clause,
        )
        influxdb3_local.info(f"[{task_id}] Source query: {query}")

        rows: list[dict[str, Any]] = influxdb3_local.query(query=query)
        if not rows:
            influxdb3_local.info(
                f"[{task_id}] No source rows returned, nothing to score"
            )
            return

        # Query selects the most recent N rows (ORDER BY time DESC); reverse to
        # chronological order (oldest->newest) so batch requests preserve the
        # series order models expect (e.g. time-series forecasting).
        rows.reverse()

        n_rows = len(rows)
        influxdb3_local.info(f"[{task_id}] Fetched {n_rows} source row(s)")

        invoker = SageMakerInvoker(
            config["endpoint_name"], config["region"], task_id, influxdb3_local
        )

        single_value_tags: dict[str, str] = {
            k: v[0] for k, v in config["tag_values"].items() if len(v) == 1
        }

        predictions: list[tuple[int | None, dict[str, Any]]] = []

        target_model: str | None = config.get("target_model")
        extra_body: dict[str, Any] = config.get("extra_body") or {}
        forecast_output: bool = config.get("forecast_output", False)

        use_batch = (config["batch_inference"] and n_rows > 1) or config["json_shape"] in _TIMESERIES_JSON_SHAPES
        if use_batch:
            try:
                body = build_request_body(
                    rows,
                    config["feature_order_tokens"],
                    config["content_type"],
                    config["json_shape"],
                    extra_body,
                )
                response_body, response_ct = invoker.invoke(
                    body, config["content_type"], config["accept"], target_model
                )
                predictions = extract_outputs(
                    response_body,
                    response_ct,
                    config["accept"],
                    config["output_fields"],
                    config["timestamp_compiled"],
                    task_id,
                    influxdb3_local,
                    forecast_output,
                )
            except Exception as e:
                influxdb3_local.error(
                    f"[{task_id}] Batch inference failed ({type(e).__name__}): {e}"
                )
                return
        else:
            for i, row in enumerate(rows):
                try:
                    body = build_request_body(
                        [row],
                        config["feature_order_tokens"],
                        config["content_type"],
                        config["json_shape"],
                        extra_body,
                    )
                    response_body, response_ct = invoker.invoke(
                        body, config["content_type"], config["accept"], target_model
                    )
                    row_preds = extract_outputs(
                        response_body,
                        response_ct,
                        config["accept"],
                        config["output_fields"],
                        config["timestamp_compiled"],
                        task_id,
                        influxdb3_local,
                        forecast_output,
                    )
                    predictions.extend(row_preds)
                except Exception as e:
                    influxdb3_local.error(
                        f"[{task_id}] Error scoring row {i} ({type(e).__name__}): {e}"
                    )

        m = len(predictions)
        if use_batch and not forecast_output and config["json_shape"] not in _TIMESERIES_JSON_SHAPES:
            if m < n_rows:
                influxdb3_local.warn(
                    f"[{task_id}] Got {m} prediction(s) for {n_rows} input row(s); "
                    f"{n_rows - m} input row(s) had no matching prediction"
                )
            elif m > n_rows:
                influxdb3_local.warn(
                    f"[{task_id}] Got {m} prediction(s) but only sent {n_rows} "
                    f"input row(s); writing all {m} predictions"
                )

        if not predictions:
            influxdb3_local.info(f"[{task_id}] No predictions to write")
            return

        line_builders: list = []
        prev_ns = 0
        for ts_ns, fields in predictions:
            try:
                if ts_ns is None:
                    ts_ns = _next_ns(prev_ns)
                prev_ns = ts_ns
                line = _build_line(config, fields, ts_ns, single_value_tags, task_id)
                line_builders.append(line)
            except Exception as e:
                influxdb3_local.error(
                    f"[{task_id}] Failed to build line for prediction "
                    f"({type(e).__name__}): {e}"
                )

        if not line_builders:
            influxdb3_local.info(
                f"[{task_id}] No line builders produced after type checks"
            )
            return

        try:
            _write_lines(influxdb3_local, _BatchLines(line_builders), target_database)
            influxdb3_local.info(
                f"[{task_id}] Write complete: {len(line_builders)} prediction(s)"
            )
        except Exception as e:
            influxdb3_local.error(
                f"[{task_id}] Batch write failed for "
                f"{len(line_builders)} prediction(s): {e}"
            )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Error in SageMaker plugin: {e}")
        if cached_config:
            source_measurement = cached_config.get("source_measurement", "")
            if source_measurement:
                influxdb3_local.cache.delete(f"sagemaker_columns::{source_measurement}")
        influxdb3_local.cache.delete(_CONFIG_CACHE_KEY)
