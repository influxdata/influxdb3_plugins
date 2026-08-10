"""
{
    "plugin_type": ["http"],
    "http_args_config": [
        {
            "name": "measurement",
            "example": "temperature",
            "description": "InfluxDB measurement name to read from",
            "required": true
        },
        {
            "name": "field",
            "example": "value",
            "description": "Field name containing the time series values",
            "required": false
        },
        {
            "name": "tags",
            "example": "room:Bedroom@Kitchen.location:Hall",
            "description": "Tag filters in dot-separated 'key:val1@val2' format. In request body, may also be a JSON object mapping tag name to value or list of values.",
            "required": false
        },
        {
            "name": "time_range",
            "example": "30d",
            "description": "Historical data window. Format: '<number><unit>' where unit is one of us, ms, s, min, h, d, w, m, q, y.",
            "required": false
        },
        {
            "name": "forecast_horizon",
            "example": "7d",
            "description": "Forecast duration. Format: '<number><unit>' (units: us, ms, s, min, h, d, w, m, q, y) or '<number> points'.",
            "required": false
        },
        {
            "name": "model",
            "example": "sfm-tabular",
            "description": "Synthefy model to use (e.g., 'sfm-tabular', 'Migas-latest'). See README for supported models.",
            "required": false
        },
        {
            "name": "output_measurement",
            "example": "temperature_forecast",
            "description": "Output measurement name (default: '{measurement}_forecast')",
            "required": false
        },
        {
            "name": "metadata_fields",
            "example": "humidity pressure",
            "description": "Space-separated list of metadata field names to use as covariates. In request body, may also be a JSON list of strings.",
            "required": false
        },
        {
            "name": "max_forecast_points",
            "example": "10000",
            "description": "Maximum number of forecast points one request may produce (default: 10000). The horizon is converted to points using the series' own step, so a dense series with a long horizon would otherwise build a very large payload.",
            "required": false
        },
        {
            "name": "database",
            "example": "mydb",
            "description": "Optional override database for writing forecasts. Reads always go to the trigger's database.",
            "required": false
        }
    ],
    "http_body_config": [
        {
            "name": "measurement",
            "example": "temperature",
            "description": "InfluxDB measurement name to read from. Required unless set in the trigger arguments.",
            "required": false
        },
        {
            "name": "field",
            "example": "value",
            "description": "Field name containing the time series values",
            "required": false
        },
        {
            "name": "tags",
            "example": "{'room': ['Bedroom', 'Kitchen'], 'location': 'Hall'}",
            "description": "Tag filters as a JSON object mapping tag name to a value or list of values. The dot-separated string form of the trigger arguments is also accepted. Send {} to clear the filters configured on the trigger; null means 'not set', so the trigger argument applies.",
            "required": false
        },
        {
            "name": "time_range",
            "example": "30d",
            "description": "Historical data window. Format: '<number><unit>' where unit is one of us, ms, s, min, h, d, w, m, q, y.",
            "required": false
        },
        {
            "name": "forecast_horizon",
            "example": "7d",
            "description": "Forecast duration. Format: '<number><unit>' (units: us, ms, s, min, h, d, w, m, q, y) or '<number> points'.",
            "required": false
        },
        {
            "name": "model",
            "example": "sfm-tabular",
            "description": "Synthefy model to use (e.g., 'sfm-tabular', 'Migas-latest'). See README for supported models.",
            "required": false
        },
        {
            "name": "output_measurement",
            "example": "temperature_forecast",
            "description": "Output measurement name (default: '{measurement}_forecast')",
            "required": false
        },
        {
            "name": "metadata_fields",
            "example": "['humidity', 'pressure']",
            "description": "JSON list of metadata field names to use as covariates. A space-separated string is also accepted. Send [] to clear the list configured on the trigger; null means 'not set', so the trigger argument applies.",
            "required": false
        },
        {
            "name": "max_forecast_points",
            "example": "10000",
            "description": "Maximum number of forecast points one request may produce (default: 10000). Accepts a JSON number or a string.",
            "required": false
        },
        {
            "name": "database",
            "example": "mydb",
            "description": "Optional override database for writing forecasts. Reads always go to the trigger's database.",
            "required": false
        }
    ],
    "http_headers_config": [
        {
            "name": "X-Synthefy-Api-Key",
            "example": "<your-synthefy-api-key>",
            "description": "Synthefy API key. Required unless the SYNTHEFY_API_KEY environment variable is set on the InfluxDB process; the header wins when both are present.",
            "required": false
        }
    ]
}
"""

import json
import math
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from json import JSONDecodeError
from typing import Any

import pandas as pd
import requests
from influxdata_plugin_utils.config import Validator, load_plugin_config
from influxdata_plugin_utils.introspection import get_field_names, get_tag_names
from influxdata_plugin_utils.parsing import (
    parse_delimited_list,
    parse_int,
    parse_timedelta,
)
from influxdata_plugin_utils.write import build_line, write_data

# Note: LineBuilder is provided by the InfluxDB 3 plugin framework at runtime.

SYNTHEFY_API_BASE_URL = "https://forecast.synthefy.com"
API_KEY_HEADER = "X-Synthefy-Api-Key"
API_KEY_ENV_VAR = "SYNTHEFY_API_KEY"

DEFAULT_MAX_FORECAST_POINTS = 10000

# Calendar units have no fixed length, so they are approximated in days.
CALENDAR_UNIT_DAYS = {"m": 30.42, "q": 91.25, "y": 365.0}

QUOTE_CHARS = ("'", '"')
# Separators after which a quoted tag value may start.
VALUE_START_CHARS = ":@"

# Synthefy accepts sub-second timestamps.
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

VALIDATORS: list = [
    Validator("measurement", default="", cast=str),
    Validator("field", default="value", cast=str),
    Validator("tags", default=None),
    Validator("metadata_fields", default=None),
    Validator("time_range", default="30d", cast=str),
    Validator("forecast_horizon", default="7d", cast=str),
    Validator("model", default="sfm-tabular", cast=str),
    Validator("output_measurement", default="", cast=str),
    Validator("database", default="", cast=str),
    Validator(
        "max_forecast_points",
        default=DEFAULT_MAX_FORECAST_POINTS,
        cast=lambda raw: parse_int(raw, minimum=1),
    ),
]


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def escape_string_literal(value: str) -> str:
    return value.replace("'", "''")


def _load_config(args: dict | None, body: dict | None) -> dict:
    """
    Merge trigger arguments with the request body and validate the result.

    Body values override trigger arguments. An explicit JSON null means "not set",
    so the validator default applies.
    """
    args = args or {}
    body = body or {}
    merged = {
        **args,
        **{key: value for key, value in body.items() if value is not None},
    }
    try:
        settings = load_plugin_config(merged, validators=VALIDATORS, source="args")
    except Exception as e:
        raise Exception(f"Invalid configuration: {e}") from e
    return {key.lower(): value for key, value in settings.as_dict().items()}


def parse_time_interval(raw: str, task_id: str) -> timedelta:
    """
    Parse an interval string ('10min', '2d', '1y', ...) into a timedelta.

    Supported units: us, ms, s, min, h, d, w, plus the approximate calendar units
    m (30.42d), q (91.25d) and y (365d).
    """
    if not isinstance(raw, str):
        raise Exception(
            f"[{task_id}] Invalid interval type: expected string like '10min', got {type(raw).__name__}"
        )

    match = re.fullmatch(r"\s*(\d+)\s*([a-zA-Z]+)\s*", raw)
    if match and match.group(2).lower() in CALENDAR_UNIT_DAYS:
        magnitude = int(match.group(1))
        unit = match.group(2).lower()
        days = int(magnitude * CALENDAR_UNIT_DAYS[unit])
        if days < 1:
            raise Exception(
                f"[{task_id}] Computed days < 1 for {magnitude}{unit} in '{raw}'."
            )
        return timedelta(days=days)

    try:
        return parse_timedelta(raw)
    except ValueError as e:
        raise Exception(
            f"[{task_id}] Invalid interval format: '{raw}' ({e}). "
            f"Expected '<number><unit>', e.g. '10min', '2d', '1y'."
        ) from e


def split_unquoted(text: str, separator: str) -> list[str]:
    """
    Split on `separator`, ignoring separators inside '...' or "..." quotes.

    A quote is only special where a value may start: at the beginning of a part
    or right after ':' or '@'. Elsewhere it is data, so "Bob's" stays intact.

    Raises:
        ValueError: if a quote is never closed.
    """
    parts: list[str] = []
    current: list[str] = []
    quote = ""
    for char in text:
        if quote:
            current.append(char)
            if char == quote:
                quote = ""
        elif char in QUOTE_CHARS and (not current or current[-1] in VALUE_START_CHARS):
            quote = char
            current.append(char)
        elif char == separator:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    if quote:
        raise ValueError(f"unterminated {quote} quote in '{text}'")
    parts.append("".join(current))
    return parts


def strip_quotes(value: str) -> str:
    """Remove one matching pair of surrounding single or double quotes."""
    if len(value) >= 2 and value[0] == value[-1] and value[0] in QUOTE_CHARS:
        return value[1:-1]
    return value


def parse_tags_from_args(
    influxdb3_local, raw: Any, measurement: str, tag_names: list[str], task_id: str
) -> dict[str, list[str]]:
    """
    Parse a downsampler-style tag string from trigger args.

    Format: 'room:Bedroom@Kitchen.location:Hall'.
        - '.' separates pairs
        - ':' separates the tag key from its value(s)
        - '@' separates multiple values for one key
        - a value wrapped in '...' or "..." is stripped of its quotes, and any
          separator inside the quotes is treated as part of the value
        - a quote inside a value, as in "Bob's", needs no escaping
    """
    if raw is None or raw == "":
        return {}
    if not isinstance(raw, str):
        raise Exception(
            f"[{task_id}] Invalid 'tags' format in trigger args: expected string, got {type(raw).__name__}."
        )

    result: dict[str, list[str]] = {}
    try:
        pairs = split_unquoted(raw, ".")
    except ValueError as e:
        raise Exception(
            f"[{task_id}] Invalid 'tags' string in trigger args: {e}."
        ) from e

    for pair in pairs:
        if not pair:
            continue
        parts = split_unquoted(pair, ":")
        if len(parts) != 2:
            raise Exception(
                f"[{task_id}] Invalid tag-value pair: '{pair}' (must contain exactly one ':'; quote values containing ':')"
            )
        tag_name, value_str = parts
        values = [strip_quotes(value) for value in split_unquoted(value_str, "@")]

        if tag_name not in tag_names:
            influxdb3_local.warn(
                f"[{task_id}] Tag '{tag_name}' does not exist in '{measurement}'; ignoring."
            )
            continue

        if tag_name in result:
            result[tag_name].extend(values)
        else:
            result[tag_name] = values
    return result


def parse_tags_from_body(
    influxdb3_local, raw: Any, measurement: str, tag_names: list[str], task_id: str
) -> dict[str, list[str]]:
    """
    Parse tag filters from the HTTP request body.

    Accepts a JSON object mapping tag name to a string or list of strings:
        {"room": "Bedroom"}            -> {"room": ["Bedroom"]}
        {"room": ["Bedroom", "Hall"]}  -> {"room": ["Bedroom", "Hall"]}
    """
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise Exception(
            f"[{task_id}] Invalid 'tags' format in request body: expected JSON object, got {type(raw).__name__}."
        )

    result: dict[str, list[str]] = {}
    for tag_name, values in raw.items():
        if tag_name not in tag_names:
            influxdb3_local.warn(
                f"[{task_id}] Tag '{tag_name}' does not exist in '{measurement}'; ignoring."
            )
            continue
        if isinstance(values, str):
            parsed_values = [values]
        elif isinstance(values, list):
            parsed_values = [str(v) for v in values]
        else:
            raise Exception(
                f"[{task_id}] Invalid tag values for '{tag_name}': expected string or list of strings."
            )
        if not parsed_values:
            influxdb3_local.warn(
                f"[{task_id}] Empty value list for tag '{tag_name}'; ignoring."
            )
            continue
        result[tag_name] = parsed_values
    return result


def parse_tags(
    influxdb3_local, raw: Any, measurement: str, tag_names: list[str], task_id: str
) -> dict[str, list[str]]:
    """Dispatch to the JSON-object form (request body) or the string form (trigger args)."""
    if isinstance(raw, dict):
        return parse_tags_from_body(
            influxdb3_local, raw, measurement, tag_names, task_id
        )
    if raw is None or isinstance(raw, str):
        return parse_tags_from_args(
            influxdb3_local, raw, measurement, tag_names, task_id
        )
    raise Exception(
        f"[{task_id}] Invalid 'tags' format: expected a string or JSON object, got {type(raw).__name__}."
    )


def parse_metadata_fields(
    influxdb3_local,
    raw: Any,
    measurement: str,
    field_names: list[str],
    task_id: str,
) -> list[str]:
    """
    Parse metadata_fields from trigger args (space-separated string) or
    request body (list of strings). Unknown fields are dropped with a warning.
    """
    if raw is None or raw == "":
        return []
    if not isinstance(raw, (str, list)):
        raise Exception(
            f"[{task_id}] Invalid 'metadata_fields' format: expected string or list, got {type(raw).__name__}."
        )

    result: list[str] = []
    for item in parse_delimited_list(raw):
        if item not in field_names:
            influxdb3_local.warn(
                f"[{task_id}] Metadata field '{item}' does not exist in '{measurement}'; ignoring."
            )
            continue
        result.append(item)
    return result


def build_history_query(
    measurement: str,
    field: str,
    metadata_fields: list[str],
    tag_filters: dict[str, list[str]],
    start_time: datetime,
) -> tuple[str, dict]:
    """
    Build a parameterized SQL query to fetch historical data from InfluxDB.

    Returns (query, params) for use with influxdb3_local.query(query, params).
    All identifiers are quoted via quote_identifier(); values are bound.
    """
    select_columns = ["time", quote_identifier(field)]
    for mf in metadata_fields:
        select_columns.append(quote_identifier(mf))
    select_clause = ", ".join(select_columns)

    start_iso = start_time.astimezone(timezone.utc).strftime(TIMESTAMP_FORMAT)
    params: dict[str, Any] = {}
    where_parts = [f"time >= '{start_iso}'"]

    param_idx = 0
    for tag_key, values in tag_filters.items():
        if not values:
            continue
        quoted_key = quote_identifier(tag_key)
        if len(values) == 1:
            param_name = f"tag_val_{param_idx}"
            where_parts.append(f"{quoted_key} = ${param_name}")
            params[param_name] = values[0]
            param_idx += 1
        else:
            placeholders = []
            for v in values:
                param_name = f"tag_val_{param_idx}"
                placeholders.append(f"${param_name}")
                params[param_name] = v
                param_idx += 1
            where_parts.append(f"{quoted_key} IN ({', '.join(placeholders)})")

    where_clause = " AND ".join(where_parts)
    escaped_measurement = escape_string_literal(measurement)
    query = f"""
        SELECT {select_clause}
        FROM '{escaped_measurement}'
        WHERE {where_clause}
        ORDER BY time
    """
    return query, params


def dataframe_to_synthefy_request(
    influxdb3_local,
    df: pd.DataFrame,
    field: str,
    forecast_horizon: str,
    metadata_fields: list[str],
    model: str,
    max_forecast_points: int,
    task_id: str,
) -> dict[str, Any]:
    """
    Convert a DataFrame of historical data into a Synthefy ForecastV2Request payload.
    """
    if df.empty:
        raise ValueError("No data found in query result")
    if "time" not in df.columns:
        raise ValueError("Query result must include 'time' column")

    df = df.copy()
    df["time"] = pd.to_datetime(df["time"])
    df = df.sort_values("time").reset_index(drop=True)

    repeated_timestamps = int(df["time"].duplicated().sum())
    if repeated_timestamps:
        influxdb3_local.warn(
            f"[{task_id}] History holds {repeated_timestamps} repeated timestamps, so the "
            f"window covers more than one series and their values are interleaved. "
            f"Set 'tags' to select a single series."
        )

    history_timestamps = df["time"].dt.strftime(TIMESTAMP_FORMAT).tolist()
    history_values = [None if pd.isna(v) else v for v in df[field].tolist()]

    collapsed = (
        len(history_timestamps) - len(set(history_timestamps)) - repeated_timestamps
    )
    if collapsed > 0:
        raise Exception(
            f"[{task_id}] {collapsed} history timestamps differ by less than a microsecond "
            f"and collapse onto each other. Resample the series to a coarser step."
        )

    if len(df) >= 2:
        time_step = df["time"].iloc[-1] - df["time"].iloc[-2]
        if time_step <= timedelta(0):
            time_step = timedelta(hours=1)
    else:
        time_step = timedelta(hours=1)

    fh = forecast_horizon.strip()
    if fh.endswith(" points"):
        try:
            num_points = parse_int(fh.removesuffix(" points"), minimum=1)
        except ValueError as e:
            raise Exception(
                f"[{task_id}] Invalid forecast_horizon: '{forecast_horizon}' ({e})."
            ) from e
    else:
        forecast_td = parse_time_interval(fh, task_id)
        num_points = max(1, int(forecast_td / time_step))

    if num_points > max_forecast_points:
        raise Exception(
            f"[{task_id}] forecast_horizon '{forecast_horizon}' resolves to {num_points} points "
            f"at the series' step of {time_step}, above the max_forecast_points limit of "
            f"{max_forecast_points}. Shorten the horizon or raise max_forecast_points."
        )

    target_timestamps: list[str] = []
    current_time = df["time"].iloc[-1] + time_step
    for _ in range(num_points):
        target_timestamps.append(current_time.strftime(TIMESTAMP_FORMAT))
        current_time += time_step

    if len(set(target_timestamps)) != len(target_timestamps):
        raise Exception(
            f"[{task_id}] The series' step of {time_step} is finer than a microsecond, so "
            f"forecast timestamps collapse onto each other. Resample to a coarser step."
        )

    target_values = [None] * len(target_timestamps)

    forecast_sample = {
        "sample_id": field,
        "history_timestamps": history_timestamps,
        "history_values": history_values,
        "target_timestamps": target_timestamps,
        "target_values": target_values,
        "forecast": True,
        "metadata": False,
        "leak_target": False,
        "column_name": field,
    }

    metadata_samples: list[dict] = []
    for mf in metadata_fields:
        if mf not in df.columns:
            continue
        metadata_samples.append(
            {
                "sample_id": mf,
                "history_timestamps": history_timestamps,
                "history_values": [None if pd.isna(v) else v for v in df[mf].tolist()],
                "target_timestamps": target_timestamps,
                "target_values": [None] * len(target_timestamps),
                "forecast": False,
                "metadata": True,
                "leak_target": False,
                "column_name": mf,
            }
        )

    return {
        "samples": [[forecast_sample] + metadata_samples],
        "model": model,
    }


def call_synthefy_api(
    influxdb3_local,
    request_data: dict[str, Any],
    api_key: str,
    task_id: str,
) -> dict[str, Any]:
    """
    POST a forecast request to the Synthefy v2/forecast endpoint and return the JSON response.
    """
    endpoint = f"{SYNTHEFY_API_BASE_URL.rstrip('/')}/v2/forecast"
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": api_key,
    }

    influxdb3_local.info(f"[{task_id}] Calling Synthefy API: {endpoint}")
    try:
        response = requests.post(
            endpoint, json=request_data, headers=headers, timeout=300
        )
        response.raise_for_status()
        result = response.json()
        influxdb3_local.info(f"[{task_id}] Synthefy API call successful.")
        return result
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Synthefy API call failed: {e}")
        raise


def _timestamp_ns(raw: Any) -> int:
    """Convert a forecast timestamp to integer nanoseconds; naive values are UTC."""
    ts = pd.Timestamp(raw)
    if ts.tz is None:
        ts = ts.tz_localize("UTC")
    return int(ts.value)


def _is_non_finite(value: Any) -> bool:
    """True for NaN/inf floats, which would make InfluxDB reject the whole batch."""
    return isinstance(value, float) and not math.isfinite(value)


def forecast_response_to_line_builders(
    influxdb3_local,
    forecast_response: dict[str, Any],
    output_measurement: str,
    tag_filters: dict[str, list[str]],
    model: str,
    field_name: str,
    task_id: str,
) -> list[Any]:
    """
    Convert the Synthefy forecast response into LineBuilder objects.

    Tag filters with a single value are written as static tags on every output
    point. Tag filters with multiple values are not written (the response mixes
    rows from several tag values, so no single value applies).
    """
    if "forecasts" not in forecast_response:
        raise ValueError("Invalid forecast response: missing 'forecasts' field")

    forecasts = forecast_response["forecasts"]
    if not forecasts or not forecasts[0]:
        raise ValueError("No forecasts in response")

    forecast_row = forecasts[0]

    forecast_payload: dict | None = None
    for f in forecast_row:
        if isinstance(f, dict) and "timestamps" in f and "values" in f:
            forecast_payload = f
            break

    if forecast_payload is None:
        raise ValueError(
            "No forecast payload (with 'timestamps' and 'values') found in response"
        )

    timestamps = forecast_payload.get("timestamps", [])
    values = forecast_payload.get("values", [])
    quantiles = forecast_payload.get("quantiles") or {}
    output_field_name = field_name or forecast_payload.get("sample_id", "value")

    static_tags = {
        tag_key: tag_values[0]
        for tag_key, tag_values in tag_filters.items()
        if len(tag_values) == 1
    }
    static_tags["model"] = model

    builders: list[Any] = []
    for i, (ts_str, value) in enumerate(zip(timestamps, values)):
        if value is None:
            continue
        if _is_non_finite(value):
            influxdb3_local.warn(
                f"[{task_id}] Non-finite forecast value at '{ts_str}'; skipping point."
            )
            continue
        try:
            ts_ns = _timestamp_ns(ts_str)
        except Exception:
            influxdb3_local.warn(
                f"[{task_id}] Could not parse timestamp '{ts_str}'; skipping point."
            )
            continue

        fields: dict[str, Any] = {output_field_name: value}
        for q_level, q_values in quantiles.items():
            if i >= len(q_values):
                continue
            q_value = q_values[i]
            if q_value is None or _is_non_finite(q_value):
                continue
            fields[f"value_{q_level}"] = q_value

        builders.append(
            build_line(
                LineBuilder,
                output_measurement,
                tags=static_tags,
                fields=fields,
                time_ns=ts_ns,
            )
        )
    return builders


def write_forecasts_to_influxdb(
    influxdb3_local,
    builders: list[Any],
    database: str | None,
    task_id: str,
    max_retries: int = 3,
) -> None:
    """
    Write forecast points as a single batched, synchronous payload, retrying with
    exponential backoff. Writes go to `database` when set, otherwise to the
    trigger's own database.
    """
    if not builders:
        influxdb3_local.warn(f"[{task_id}] No forecast points to write.")
        return

    target = f"database {database}" if database else "trigger database"
    influxdb3_local.info(
        f"[{task_id}] Writing {len(builders)} forecast points to {target}."
    )
    try:
        write_data(
            influxdb3_local,
            builders,
            batch=True,
            retries=max_retries - 1,
            no_sync=True,
            database=database,
        )
    except Exception as e:
        influxdb3_local.error(
            f"[{task_id}] Failed to write forecasts after {max_retries} attempts: {e}"
        )
        raise
    influxdb3_local.info(f"[{task_id}] Wrote {len(builders)} forecast points.")


def _decode_request_body(request_body: Any, task_id: str) -> dict:
    """Decode the HTTP request body into a dict. Empty body -> {}. Invalid JSON -> raise."""
    if request_body is None or request_body == "" or request_body == b"":
        return {}
    if isinstance(request_body, dict):
        return request_body
    if not isinstance(request_body, (bytes, str)):
        raise Exception(
            f"[{task_id}] Unsupported request_body type: {type(request_body).__name__}"
        )
    body_str = (
        request_body.decode("utf-8")
        if isinstance(request_body, bytes)
        else request_body
    )
    return json.loads(body_str)


def _get_api_key(request_headers: dict | None) -> str | None:
    """Return the API key from the request header or env var, or None."""
    if request_headers:
        for key, value in request_headers.items():
            if isinstance(key, str) and key.lower() == API_KEY_HEADER.lower():
                return value
    return os.getenv(API_KEY_ENV_VAR)


def process_request(
    influxdb3_local: Any,
    query_parameters: dict,
    request_headers: dict,
    request_body: Any,
    args: dict | None = None,
) -> dict:
    """
    HTTP entry point. Reads historical data, calls Synthefy, writes the forecast back.

    Authentication for the Synthefy API is taken from the
    'X-Synthefy-Api-Key' header or the SYNTHEFY_API_KEY environment variable.
    """
    task_id: str = str(uuid.uuid4())
    influxdb3_local.info(f"[{task_id}] Starting Synthefy forecast request.")

    if args is None:
        args = {}

    try:
        body_dict = _decode_request_body(request_body, task_id)
    except (JSONDecodeError, UnicodeDecodeError) as e:
        influxdb3_local.error(f"[{task_id}] Invalid JSON in request body: {e}")
        return {"message": "Invalid JSON in request body"}
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to decode request body: {e}")
        return {"message": f"Error: {e}"}

    if not isinstance(body_dict, dict):
        influxdb3_local.error(f"[{task_id}] Request body must be a JSON object.")
        return {"message": "Request body must be a JSON object"}

    api_key = _get_api_key(request_headers)
    if not api_key:
        influxdb3_local.error(
            f"[{task_id}] Missing API key: set the '{API_KEY_HEADER}' "
            f"header or '{API_KEY_ENV_VAR}' environment variable."
        )
        return {"message": "Missing API key"}

    try:
        config = _load_config(args, body_dict)

        measurement = config["measurement"]
        if not measurement:
            influxdb3_local.error(f"[{task_id}] 'measurement' argument is required.")
            return {"message": "'measurement' argument is required"}

        field = config["field"]
        model = config["model"]
        output_measurement = config["output_measurement"] or f"{measurement}_forecast"
        database = config["database"] or None
        max_forecast_points = config["max_forecast_points"]

        field_names = get_field_names(influxdb3_local, measurement, use_cache=False)
        tag_names = get_tag_names(influxdb3_local, measurement, use_cache=False)

        if not field_names and not tag_names:
            influxdb3_local.error(
                f"[{task_id}] Measurement '{measurement}' not found or has no schema."
            )
            return {"message": f"Measurement '{measurement}' not found"}

        if field not in field_names:
            influxdb3_local.error(
                f"[{task_id}] Field '{field}' does not exist in measurement '{measurement}'."
            )
            return {"message": f"Field '{field}' does not exist in '{measurement}'"}

        tag_filters = parse_tags(
            influxdb3_local, config["tags"], measurement, tag_names, task_id
        )
        metadata_fields = parse_metadata_fields(
            influxdb3_local, config["metadata_fields"], measurement, field_names, task_id
        )

        time_range_td = parse_time_interval(config["time_range"], task_id)
        start_time = datetime.now(timezone.utc) - time_range_td

        query, params = build_history_query(
            measurement, field, metadata_fields, tag_filters, start_time
        )
        influxdb3_local.info(
            f"[{task_id}] Executing history query for measurement '{measurement}'."
        )
        result_rows = influxdb3_local.query(query, params)

        if not result_rows:
            influxdb3_local.info(f"[{task_id}] No data found for query.")
            return {"message": "No data found"}

        df = pd.DataFrame(result_rows)
        if df.empty:
            return {"message": "No data found"}

        synthefy_request = dataframe_to_synthefy_request(
            influxdb3_local,
            df,
            field,
            config["forecast_horizon"],
            metadata_fields,
            model,
            max_forecast_points,
            task_id,
        )
        forecast_response = call_synthefy_api(
            influxdb3_local, synthefy_request, api_key, task_id
        )
        builders = forecast_response_to_line_builders(
            influxdb3_local,
            forecast_response,
            output_measurement,
            tag_filters,
            model,
            field,
            task_id,
        )

        write_forecasts_to_influxdb(influxdb3_local, builders, database, task_id)

        influxdb3_local.info(
            f"[{task_id}] Forecast complete: {len(builders)} points written."
        )
        return {
            "message": (
                f"Forecast generated and written to InfluxDB. "
                f"{len(builders)} forecast points written."
            )
        }
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] HTTP request forecast failed: {e}")
        return {"message": f"Error: {e}"}
