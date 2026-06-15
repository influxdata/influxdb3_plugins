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
            "description": "Historical data window. Format: '<number><unit>' where unit is one of s, min, h, d, w, m, q, y.",
            "required": false
        },
        {
            "name": "forecast_horizon",
            "example": "7d",
            "description": "Forecast duration. Format: '<number><unit>' (units: s, min, h, d, w, m, q, y) or '<number> points'.",
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
            "name": "database",
            "example": "mydb",
            "description": "Optional override database for writing forecasts. Reads always go to the trigger's database.",
            "required": false
        }
    ]
}
"""

import json
import os
import random
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from json import JSONDecodeError
from typing import Any, Iterable, Optional, Protocol

import pandas as pd
import requests

# Note: LineBuilder is provided by the InfluxDB 3 plugin framework at runtime.

SYNTHEFY_API_BASE_URL = "https://forecast.synthefy.com"
API_KEY_HEADER = "X-Synthefy-Api-Key"
API_KEY_ENV_VAR = "SYNTHEFY_API_KEY"


class _LineBuilderInterface(Protocol):
    def build(self) -> str: ...


class _BatchLines:
    """
    Wraps multiple LineBuilder objects into a single object with a build()
    method that returns a newline-separated string. Allows batched writes
    through the write_sync / write_sync_to_db APIs.
    """

    def __init__(self, line_builders: Iterable[_LineBuilderInterface]):
        self._line_builders = list(line_builders)
        self._built: Optional[str] = None

    def _coerce_builder(self, builder: _LineBuilderInterface) -> str:
        build_fn = getattr(builder, "build", None)
        if not callable(build_fn):
            raise TypeError("line_builder is missing a callable build()")
        return str(build_fn())

    def build(self) -> str:
        if self._built is None:
            lines = [self._coerce_builder(b) for b in self._line_builders]
            if not lines:
                raise ValueError("batch_write received no lines to build")
            self._built = "\n".join(lines)
        return self._built


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def escape_string_literal(value: str) -> str:
    return value.replace("'", "''")


def parse_time_interval(raw: str, task_id: str) -> timedelta:
    """
    Parse an interval string ('10min', '2d', '1y', ...) into a timedelta.

    Supported units: s, min, h, d, w, m (≈30.42d), q (≈91.25d), y (365d).
    """
    unit_mapping = {
        "s": "seconds",
        "min": "minutes",
        "h": "hours",
        "d": "days",
        "w": "weeks",
        "m": "days",
        "q": "days",
        "y": "days",
    }
    day_conversions = {
        "m": 30.42,
        "q": 91.25,
        "y": 365.0,
    }

    if not isinstance(raw, str):
        raise Exception(
            f"[{task_id}] Invalid interval type: expected string like '10min', got {type(raw).__name__}"
        )

    match = re.fullmatch(r"(\d+)([a-zA-Z]+)", raw.strip())
    if not match:
        raise Exception(
            f"[{task_id}] Invalid interval format: '{raw}'. Expected '<number><unit>', e.g. '10min', '2d'."
        )

    number_part, unit = match.groups()
    magnitude = int(number_part)
    unit = unit.lower()
    if unit not in unit_mapping:
        raise Exception(f"[{task_id}] Unsupported interval unit '{unit}' in '{raw}'.")

    if unit in day_conversions:
        days_approx = int(magnitude * day_conversions[unit])
        if days_approx < 1:
            raise Exception(
                f"[{task_id}] Computed days < 1 for {magnitude}{unit} in '{raw}'."
            )
        return timedelta(days=days_approx)

    if unit == "s":
        return timedelta(seconds=magnitude)
    if unit == "min":
        return timedelta(minutes=magnitude)
    if unit == "h":
        return timedelta(hours=magnitude)
    if unit == "d":
        return timedelta(days=magnitude)
    if unit == "w":
        return timedelta(weeks=magnitude)
    raise Exception(f"[{task_id}] Unsupported interval unit '{unit}' in '{raw}'.")


def get_tag_names(influxdb3_local, measurement: str, task_id: str) -> list[str]:
    """Return tag column names for `measurement`, or an empty list if none/no schema."""
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $measurement
        AND data_type = 'Dictionary(Int32, Utf8)'
    """
    res = influxdb3_local.query(query, {"measurement": measurement})
    if not res:
        influxdb3_local.info(
            f"[{task_id}] No tags found for measurement '{measurement}'."
        )
        return []
    return [row["column_name"] for row in res]


def get_field_names(influxdb3_local, measurement: str, task_id: str) -> list[str]:
    """Return non-tag, non-time field column names for `measurement`."""
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $measurement
        AND data_type != 'Dictionary(Int32, Utf8)'
        AND column_name != 'time'
    """
    res = influxdb3_local.query(query, {"measurement": measurement})
    if not res:
        return []
    return [row["column_name"] for row in res]


def parse_tags_from_args(
    influxdb3_local, raw: Any, measurement: str, tag_names: list[str], task_id: str
) -> dict[str, list[str]]:
    """
    Parse a downsampler-style tag string from trigger args.

    Format: 'room:Bedroom@Kitchen.location:Hall'.
        - '.' separates pairs
        - ':' separates the tag key from its value(s)
        - '@' separates multiple values for one key
        - quoted values ('...' or "...") are stripped of their quotes
    """
    if raw is None or raw == "":
        return {}
    if not isinstance(raw, str):
        raise Exception(
            f"[{task_id}] Invalid 'tags' format in trigger args: expected string, got {type(raw).__name__}."
        )

    result: dict[str, list[str]] = {}
    for pair in raw.split("."):
        if not pair:
            continue
        parts = pair.split(":")
        if len(parts) != 2:
            raise Exception(
                f"[{task_id}] Invalid tag-value pair: '{pair}' (must contain exactly one ':'; quote values containing ':')"
            )
        tag_name, value_str = parts
        values: list[str] = []
        for v in value_str.split("@"):
            if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
                values.append(v[1:-1])
            else:
                values.append(v)

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
    if isinstance(raw, str):
        items = raw.split()
    elif isinstance(raw, list):
        items = [str(x) for x in raw]
    else:
        raise Exception(
            f"[{task_id}] Invalid 'metadata_fields' format: expected string or list, got {type(raw).__name__}."
        )

    result: list[str] = []
    for item in items:
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

    start_iso = start_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
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
    df: pd.DataFrame,
    field: str,
    forecast_horizon: str,
    metadata_fields: list[str],
    model: str,
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

    history_timestamps = df["time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ").tolist()
    history_values = [None if pd.isna(v) else v for v in df[field].tolist()]

    if len(df) >= 2:
        time_step = df["time"].iloc[-1] - df["time"].iloc[-2]
        if time_step <= timedelta(0):
            time_step = timedelta(hours=1)
    else:
        time_step = timedelta(hours=1)

    fh = forecast_horizon.strip()
    if fh.endswith(" points"):
        try:
            num_points = int(fh.replace(" points", "").strip())
        except ValueError:
            raise Exception(
                f"[{task_id}] Invalid forecast_horizon: '{forecast_horizon}'."
            )
        if num_points < 1:
            raise Exception(f"[{task_id}] forecast_horizon must be >= 1 point.")
    else:
        forecast_td = parse_time_interval(fh, task_id)
        num_points = max(1, int(forecast_td / time_step))

    target_timestamps: list[str] = []
    current_time = df["time"].iloc[-1] + time_step
    for _ in range(num_points):
        target_timestamps.append(current_time.strftime("%Y-%m-%dT%H:%M:%SZ"))
        current_time += time_step

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

    forecast_payload: Optional[dict] = None
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

    builders: list[Any] = []
    for i, (ts_str, value) in enumerate(zip(timestamps, values)):
        if value is None:
            continue
        try:
            ts = pd.to_datetime(ts_str)
            ts_ns = int(ts.timestamp() * 1e9)
        except Exception:
            influxdb3_local.warn(
                f"[{task_id}] Could not parse timestamp '{ts_str}'; skipping point."
            )
            continue

        builder = LineBuilder(output_measurement)
        builder.time_ns(ts_ns)

        for tag_key, tag_values in tag_filters.items():
            if len(tag_values) == 1:
                builder.tag(tag_key, tag_values[0])
        builder.tag("model", model)

        _set_field(builder, output_field_name, value)
        for q_level, q_values in quantiles.items():
            if i < len(q_values) and q_values[i] is not None:
                _set_field(builder, f"value_{q_level}", q_values[i])

        builders.append(builder)
    return builders


def _set_field(builder: Any, name: str, value: Any) -> None:
    if isinstance(value, bool):
        builder.string_field(name, str(value))
    elif isinstance(value, int):
        builder.int64_field(name, value)
    elif isinstance(value, float):
        builder.float64_field(name, value)
    else:
        builder.string_field(name, str(value))


def write_forecasts_to_influxdb(
    influxdb3_local,
    builders: list[Any],
    database: Optional[str],
    task_id: str,
    max_retries: int = 3,
) -> None:
    """
    Write forecast points using write_sync (or write_sync_to_db when `database`
    is set), batched into a single line-protocol payload, with exponential backoff retries.
    """
    if not builders:
        influxdb3_local.warn(f"[{task_id}] No forecast points to write.")
        return

    influxdb3_local.info(
        f"[{task_id}] Writing {len(builders)} forecast points to "
        f"{'database ' + database if database else 'trigger database'}."
    )

    batch = _BatchLines(builders)
    for attempt in range(max_retries):
        try:
            if database:
                influxdb3_local.write_sync_to_db(database, batch, no_sync=True)
            else:
                influxdb3_local.write_sync(batch, no_sync=True)
            influxdb3_local.info(
                f"[{task_id}] Wrote {len(builders)} forecast points (attempt {attempt + 1})."
            )
            return
        except Exception as e:
            influxdb3_local.warn(
                f"[{task_id}] Error writing forecasts attempt {attempt + 1}/{max_retries}: {e}"
            )
            if attempt < max_retries - 1:
                wait_time = (2**attempt) + random.random()
                time.sleep(wait_time)
            else:
                influxdb3_local.error(
                    f"[{task_id}] Failed to write forecasts after {max_retries} attempts: {e}"
                )
                raise


def _decode_request_body(request_body: Any, task_id: str) -> dict:
    """Decode the HTTP request body into a dict. Empty body -> {}. Invalid JSON -> raise."""
    if request_body is None or request_body == "" or request_body == b"":
        return {}
    if isinstance(request_body, dict):
        return request_body
    if isinstance(request_body, bytes):
        body_str = request_body.decode("utf-8")
    elif isinstance(request_body, str):
        body_str = request_body
    else:
        raise Exception(
            f"[{task_id}] Unsupported request_body type: {type(request_body).__name__}"
        )
    return json.loads(body_str)


def _get_api_key(request_headers: Optional[dict]) -> Optional[str]:
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
    args: Optional[dict] = None,
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
        merged_args = {**args, **body_dict}

        measurement = merged_args.get("measurement")
        if not measurement:
            influxdb3_local.error(f"[{task_id}] 'measurement' argument is required.")
            return {"message": "'measurement' argument is required"}

        field = merged_args.get("field", "value")
        time_range_str = merged_args.get("time_range", "30d")
        forecast_horizon_str = merged_args.get("forecast_horizon", "7d")
        model = merged_args.get("model", "sfm-tabular")
        output_measurement = (
            merged_args.get("output_measurement") or f"{measurement}_forecast"
        )
        database = merged_args.get("database") or None

        field_names = get_field_names(influxdb3_local, measurement, task_id)
        tag_names = get_tag_names(influxdb3_local, measurement, task_id)

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

        if "tags" in body_dict:
            tag_filters = parse_tags_from_body(
                influxdb3_local,
                body_dict.get("tags"),
                measurement,
                tag_names,
                task_id,
            )
        else:
            tag_filters = parse_tags_from_args(
                influxdb3_local,
                args.get("tags"),
                measurement,
                tag_names,
                task_id,
            )

        if "metadata_fields" in body_dict:
            metadata_fields = parse_metadata_fields(
                influxdb3_local,
                body_dict.get("metadata_fields"),
                measurement,
                field_names,
                task_id,
            )
        else:
            metadata_fields = parse_metadata_fields(
                influxdb3_local,
                args.get("metadata_fields"),
                measurement,
                field_names,
                task_id,
            )

        time_range_td = parse_time_interval(time_range_str, task_id)
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
            df, field, forecast_horizon_str, metadata_fields, model, task_id
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
