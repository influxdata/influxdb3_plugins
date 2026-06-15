"""
{
    "plugin_type": ["scheduled", "http"],
    "scheduled_args_config": [
        {
            "name": "measurement",
            "example": "sensor_data",
            "description": "Source table containing historical time-series data.",
            "required": true
        },
        {
            "name": "field",
            "example": "temperature",
            "description": "Numeric field name to forecast.",
            "required": true
        },
        {
            "name": "window",
            "example": "6h",
            "description": "Historical lookback window for context data. Format: <number><unit> (s, min, h, d).",
            "required": true
        },
        {
            "name": "horizon",
            "example": "64",
            "description": "Number of forecast steps to generate.",
            "required": true
        },
        {
            "name": "target_measurement",
            "example": "_forecasts.sensor_data",
            "description": "Destination table for writing forecast results. Default: _forecasts.{measurement}.",
            "required": false
        },
        {
            "name": "model_id",
            "example": "amazon/chronos-bolt-tiny",
            "description": "HuggingFace model ID. Supports Chronos-2, Chronos-Bolt, and original Chronos models.",
            "required": false
        },
        {
            "name": "context_limit",
            "example": "512",
            "description": "Maximum number of data points fed to the model. Default: 512.",
            "required": false
        },
        {
            "name": "agg_interval",
            "example": "30s",
            "description": "Aggregation interval for date_bin query. Default: 30s.",
            "required": false
        },
        {
            "name": "tag_values",
            "example": "host:server1@server2.region:us-west",
            "description": "Dot-separated tag filters; values joined by '@' (e.g., 'tag:value1@value2.tag2:value3').",
            "required": false
        },
        {
            "name": "covariate_fields",
            "example": "humidity pressure",
            "description": "Space-separated covariate field names. Setting this enables Chronos-2 multivariate forecasting (requires a Chronos-2 model).",
            "required": false
        },
        {
            "name": "covariate_mode",
            "example": "covariate",
            "description": "How covariate_fields are used (Chronos-2 only): 'covariate' (auxiliary past covariates, default) or 'target' (jointly forecast all series). Default: covariate.",
            "required": false
        },
        {
            "name": "target_database",
            "example": "forecast_db",
            "description": "Write forecasts to a different database. Default: current database.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "chronos_forecasting_scheduler.toml",
            "description": "Path to a TOML config file: absolute, or relative to the plugin directory (INFLUXDB3_PLUGIN_DIR or PLUGIN_DIR).",
            "required": false
        }
    ],
    "http_args_config": [
        {
            "name": "table",
            "example": "sensor_data",
            "description": "Source table name containing historical data.",
            "required": true
        },
        {
            "name": "field",
            "example": "temperature",
            "description": "Numeric field name to forecast.",
            "required": true
        },
        {
            "name": "horizon",
            "example": "64",
            "description": "Number of forecast steps to generate. Default: 64.",
            "required": false
        },
        {
            "name": "context_limit",
            "example": "512",
            "description": "Maximum context window size (data points). Default: 512.",
            "required": false
        },
        {
            "name": "model_id",
            "example": "amazon/chronos-bolt-tiny",
            "description": "HuggingFace model ID. Supports Chronos-2, Chronos-Bolt, and original Chronos models.",
            "required": false
        },
        {
            "name": "covariate_fields",
            "example": "humidity pressure",
            "description": "Space-separated covariate field names. Setting this enables Chronos-2 multivariate forecasting (requires a Chronos-2 model).",
            "required": false
        },
        {
            "name": "covariate_mode",
            "example": "covariate",
            "description": "How covariate_fields are used (Chronos-2 only): 'covariate' (auxiliary past covariates, default) or 'target' (jointly forecast all series). Default: covariate.",
            "required": false
        },
        {
            "name": "write_results",
            "example": "true",
            "description": "Write forecast results to the database. Default: false.",
            "required": false
        },
        {
            "name": "target_measurement",
            "example": "temperature_forecast",
            "description": "Destination table for written forecast results (required if write_results is true).",
            "required": false
        },
        {
            "name": "target_database",
            "example": "forecast_db",
            "description": "Write forecasts to a different database. Default: current database.",
            "required": false
        }
    ]
}
"""

import os

# Fix for Docker containers running as unmapped UIDs:
# HuggingFace Hub needs a username and writable cache directory.
if "USER" not in os.environ:
    os.environ["USER"] = "influxdb3"
if "HF_HOME" not in os.environ:
    _cache_dir = "/tmp/hf_cache"
    os.makedirs(_cache_dir, exist_ok=True)
    os.environ["HF_HOME"] = _cache_dir
if "HOME" not in os.environ or os.environ["HOME"] == "/":
    os.environ["HOME"] = "/tmp"

# Disable torch.compile / dynamo — not needed for CPU inference
# and avoids "mega-cache artifact factory" conflicts in embedded Python.
os.environ["TORCHDYNAMO_DISABLE"] = "1"

# Disable tqdm/HF progress bars.
os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
os.environ["TQDM_DISABLE"] = "1"

import json
import re
import time as _time
import uuid
from datetime import datetime, timezone
from typing import Protocol, runtime_checkable

import torch
from chronos import BaseChronosPipeline


@runtime_checkable
class _LineBuilderInterface(Protocol):
    def build(self) -> str: ...


class _BatchLines:
    """Combine many LineBuilder objects into one line-protocol payload.

    Passing this to write_sync/write_sync_to_db sends all lines in a single
    write call instead of one call per line.
    """

    def __init__(self, line_builders: list[_LineBuilderInterface]):
        self._line_builders = list(line_builders)
        self._built: str | None = None

    def build(self) -> str:
        if self._built is None:
            lines = [str(b.build()) for b in self._line_builders]
            if not lines:
                raise ValueError("batch write received no lines to build")
            self._built = "\n".join(lines)
        return self._built


# Quantile levels used for the prediction intervals. 0.1/0.5/0.9 are native
# levels the Chronos models are trained on; 0.25/0.75 fall inside the trained
# range and are linearly interpolated by the library (no extrapolation, no
# quality warning). We keep within [0.1, 0.9] to avoid extrapolation entirely.
_QUANTILE_LEVELS = [0.1, 0.25, 0.5, 0.75, 0.9]


# ---------------------------------------------------------------------------
# Parameter handling
# ---------------------------------------------------------------------------


def _parse_int(value, name, min_value=None):
    """Parse an integer parameter, raising ValueError with a clear message.

    If min_value is set, the parsed value must be >= min_value.
    """
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid {name}: expected an integer, got '{value}'")
    if min_value is not None and parsed < min_value:
        raise ValueError(f"Invalid {name}: expected an integer >= {min_value}, got {parsed}")
    return parsed


_TAG_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")


def _parse_tag_values(tag_input):
    """Parse tag filters into a dict of {tag_name: [values]}.

    Accepts:
        - dict (from a TOML config), e.g. {"host": "server1"} or
          {"host": ["server1", "server2"]};
        - a dot-separated string (from trigger args), e.g.
          'host:server1@server2.region:us-west', where pairs are separated
          by '.', tag and values by ':', and multiple values by '@'.

    Raises ValueError on malformed input rather than silently dropping it.
    """
    if tag_input is None:
        return {}

    if isinstance(tag_input, dict):
        result = {}
        for k, v in tag_input.items():
            result[k] = [str(x) for x in v] if isinstance(v, list) else [str(v)]
        return result

    if not isinstance(tag_input, str):
        raise ValueError(
            f"Invalid tag_values type: expected dict or string, got {type(tag_input).__name__}"
        )

    result = {}
    for pair in tag_input.split("."):
        pair = pair.strip()
        if not pair:
            continue
        parts = pair.split(":")
        if len(parts) != 2:
            raise ValueError(
                f"Invalid tag-value pair: '{pair}' (must contain exactly one ':')"
            )
        tag_name, value_str = parts[0].strip(), parts[1]
        if not _TAG_NAME_PATTERN.match(tag_name):
            raise ValueError(
                f"Invalid tag name: '{tag_name}' (allowed: letters, digits, '-', '_')"
            )
        values = []
        for value in value_str.split("@"):
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                values.append(value[1:-1])
            else:
                values.append(value)
        result.setdefault(tag_name, []).extend(values)
    return result


def _quote_ident(name):
    """Quote a SQL identifier (table/field/tag), escaping embedded double quotes.

    Prevents identifier-based SQL injection from table/field/covariate names
    that originate from untrusted query parameters.
    """
    return '"' + str(name).replace('"', '""') + '"'


def _generate_tag_filter_sql(tag_values):
    """Build a SQL AND-clause fragment from a {tag: [values]} dict.

    Multiple values for a tag are OR-ed together. Returns empty string if no tags.
    """
    if not tag_values:
        return ""
    clauses = []
    for k, values in tag_values.items():
        ors = [
            f"{_quote_ident(k)} = '{str(v).replace(chr(39), chr(39) * 2)}'"
            for v in values
        ]
        if len(ors) == 1:
            clauses.append(ors[0])
        else:
            clauses.append("(" + " OR ".join(ors) + ")")
    return " AND ".join(clauses)


def _resolve_config_path(config_path):
    """Resolve a config file path: absolute as-is, relative from the plugin dir.

    Relative paths resolve from INFLUXDB3_PLUGIN_DIR or PLUGIN_DIR, falling back
    to the VIRTUAL_ENV parent (the processing engine's default venv lives at
    <plugin-dir>/.venv) for servers where neither var is exported.
    """
    if os.path.isabs(config_path):
        return config_path
    plugin_dir = os.environ.get("INFLUXDB3_PLUGIN_DIR") or os.environ.get("PLUGIN_DIR")
    if plugin_dir:
        return os.path.join(plugin_dir, config_path)
    virtual_env = os.environ.get("VIRTUAL_ENV")
    if virtual_env:
        candidate = os.path.join(os.path.dirname(virtual_env), config_path)
        if os.path.exists(candidate):
            return candidate
    raise ValueError(
        f"Neither INFLUXDB3_PLUGIN_DIR nor PLUGIN_DIR environment variable is set "
        f"and config file path '{config_path}' was not found via the VIRTUAL_ENV "
        f"fallback. Required for a relative config file path."
    )


def _load_toml_config(config_path, influxdb3_local, task_id):
    """Load and return the TOML config file referenced by config_file_path."""
    import tomllib

    if not config_path.endswith(".toml"):
        raise ValueError(
            f"Invalid config file format: expected a .toml file, got '{config_path}'"
        )
    full_path = _resolve_config_path(config_path)
    with open(full_path, "rb") as f:
        config = tomllib.load(f)
    influxdb3_local.info(f"[{task_id}] Loaded TOML config from {full_path}")
    return config


# ---------------------------------------------------------------------------
# Time / interval utilities
# ---------------------------------------------------------------------------

# Interval units: short form -> (DataFusion word, seconds). 'min' (not 'm')
# denotes minutes, matching other plugins where 'm' would mean month.
_INTERVAL_UNITS = {
    "s": ("seconds", 1),
    "min": ("minutes", 60),
    "h": ("hours", 3600),
    "d": ("days", 86400),
}


def _parse_interval(interval_str):
    """Parse an interval string (e.g. '30s', '5min') into (step_ns, sql_interval)."""
    s = str(interval_str).strip()
    match = re.fullmatch(r"(\d+)\s*(s|min|h|d)", s)
    if not match:
        raise ValueError(
            f"Invalid interval '{interval_str}': expected <number><unit> with unit s, min, h, or d"
        )
    num = int(match.group(1))
    word, mult = _INTERVAL_UNITS[match.group(2)]
    return num * mult * 1_000_000_000, f"{num} {word}"


def _get_now_ns(influxdb3_local):
    """Get current time in nanoseconds from InfluxDB."""
    time_rows = influxdb3_local.query("SELECT now() AS t")
    now_str = str(time_rows[0]["t"])
    try:
        now_dt = datetime.fromisoformat(now_str.replace("Z", "+00:00"))
        # Treat naive timestamps as UTC so .timestamp() doesn't use local time.
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=timezone.utc)
    except Exception:
        now_dt = datetime.now(timezone.utc)
    return int(now_dt.timestamp() * 1_000_000_000)


def _parse_ts_to_ms(raw_timestamp):
    """Parse a raw timestamp value (int ns, datetime, or string) to epoch milliseconds."""
    if raw_timestamp is None:
        return None
    if isinstance(raw_timestamp, (int, float)):
        v = int(raw_timestamp)
        if v > 1e15:
            return v // 1_000_000
        elif v > 1e12:
            return v // 1_000
        else:
            return v
    if isinstance(raw_timestamp, datetime):
        if raw_timestamp.tzinfo is None:
            raw_timestamp = raw_timestamp.replace(tzinfo=timezone.utc)
        return int(raw_timestamp.timestamp() * 1000)
    try:
        dt = datetime.fromisoformat(str(raw_timestamp).replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        pass
    try:
        return int(float(str(raw_timestamp)) // 1_000_000)
    except Exception:
        return None


def _compute_step_ms(timestamps):
    """Compute step size in ms as the median of consecutive gaps. Default 30s.

    Using the median makes the step robust to outliers and gaps in the source data.
    """
    if len(timestamps) < 2:
        return 30000
    ms = [_parse_ts_to_ms(t) for t in timestamps]
    diffs = sorted(
        abs(b - a)
        for a, b in zip(ms, ms[1:])
        if a is not None and b is not None and abs(b - a) > 0
    )
    if not diffs:
        return 30000
    n = len(diffs)
    if n % 2:
        return diffs[n // 2]
    return (diffs[n // 2 - 1] + diffs[n // 2]) // 2


# ---------------------------------------------------------------------------
# Model loading
# ---------------------------------------------------------------------------


def _load_model(model_id, influxdb3_local, task_id):
    """Load a Chronos model from HuggingFace.

    The module is reloaded on every trigger run, so there is no in-process
    model cache; subsequent loads are served from the on-disk HuggingFace
    cache (HF_HOME), which keeps reloads fast.
    """
    influxdb3_local.info(f"[{task_id}] Loading model {model_id}...")
    t0 = _time.time()
    pipeline = BaseChronosPipeline.from_pretrained(
        model_id,
        device_map="cpu",
        dtype=torch.float32,
    )
    load_secs = _time.time() - t0
    influxdb3_local.info(f"[{task_id}] Model loaded in {load_secs:.1f}s")
    return pipeline, load_secs


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


def _build_query_simple(table, fields, where_clause, context_limit):
    """Build an ORDER BY time DESC LIMIT query selecting one or more fields.

    fields: list of field names; the first is the forecast target, the rest are covariates.
    Returns (sql, aliases) where aliases[i] is the column name for fields[i].
    """
    aliases = [f"f{i}" for i in range(len(fields))]
    cols = ", ".join(f"{_quote_ident(f)} AS {a}" for f, a in zip(fields, aliases))
    where_sql = f"WHERE {where_clause}" if where_clause else ""
    sql = (
        f"SELECT time, {cols} FROM {_quote_ident(table)} "
        f"{where_sql} ORDER BY time DESC LIMIT {context_limit}"
    )
    return sql, aliases


def _build_query_aggregated(
    table, fields, agg_sql_interval, lookback_sql_interval, tag_sql
):
    """Build a date_bin aggregated query selecting one or more fields.

    fields: list of field names; the first is the forecast target, the rest are
    covariates. Aggregating them in one query puts every field on the same
    date_bin grid, so rows stay time-aligned. Returns (sql, aliases).
    """
    aliases = [f"f{i}" for i in range(len(fields))]
    cols = ", ".join(f"avg({_quote_ident(f)}) AS {a}" for f, a in zip(fields, aliases))
    where_parts = [f"time > now() - INTERVAL '{lookback_sql_interval}'"]
    if tag_sql:
        where_parts.append(tag_sql)
    where_clause = " AND ".join(where_parts)
    sql = (
        f"SELECT date_bin(INTERVAL '{agg_sql_interval}', time) AS t, {cols} "
        f"FROM {_quote_ident(table)} "
        f"WHERE {where_clause} "
        f"GROUP BY t ORDER BY t"
    )
    return sql, aliases


def _query_aligned_series(influxdb3_local, sql, aliases, time_col, reverse=False):
    """Execute a query and return time-aligned columns.

    Returns (timestamps, columns) with columns[i] aligned to aliases[i].
    """
    rows = influxdb3_local.query(sql)
    if reverse:
        rows = list(reversed(rows))

    timestamps = []
    columns = [[] for _ in aliases]
    for row in rows:
        tv = row.get(aliases[0])
        if tv is None:
            continue
        timestamps.append(row[time_col])
        columns[0].append(float(tv))
        for j in range(1, len(aliases)):
            cv = row.get(aliases[j])
            columns[j].append(float(cv) if cv is not None else None)
    return timestamps, columns


def _fill_nulls(seq):
    """Forward-then-back-fill None gaps in a numeric column.

    Returns None if every entry is None (covariate has no data at all).
    """
    if all(x is None for x in seq):
        return None
    result = list(seq)
    last = None
    for i in range(len(result)):
        if result[i] is None:
            result[i] = last
        else:
            last = result[i]
    # Back-fill any leading Nones with the first observed value.
    first_valid = next(x for x in result if x is not None)
    for i in range(len(result)):
        if result[i] is None:
            result[i] = first_valid
        else:
            break
    return result


# ---------------------------------------------------------------------------
# Inference
# ---------------------------------------------------------------------------


def _is_chronos2(model_id):
    """True if model_id refers to a Chronos-2 model (supports multivariate)."""
    return "chronos-2" in model_id or "chronos_2" in model_id


def _build_model_input(target_vals, covariates, model_id, covariate_mode):
    """Build the model input for Chronos inference.

    target_vals: 1D list of target values.
    covariates: list of (name, values) pairs, each aligned to target length.
    covariate_mode (Chronos-2 only):
        - "covariate": covariates are passed as `past_covariates` so the model
          treats them as auxiliary signals (the proper covariate API);
        - "target": covariates are stacked as extra target variates and jointly
          forecast via group attention ([1, channels, length]).

    Without covariates, or for non-Chronos-2 models, returns a list with a
    single 1D tensor (univariate inference).
    """
    target_tensor = torch.tensor(target_vals, dtype=torch.float32)

    if not covariates or not _is_chronos2(model_id):
        return [target_tensor]

    if covariate_mode == "target":
        # Joint multivariate: [1, channels, length]
        tensors = [target_tensor] + [
            torch.tensor(v, dtype=torch.float32) for _, v in covariates
        ]
        return torch.stack(tensors).unsqueeze(0)

    # covariate_mode == "covariate": list-of-dict with past-only covariates.
    return [
        {
            "target": target_tensor,
            "past_covariates": {
                name: torch.tensor(v, dtype=torch.float32) for name, v in covariates
            },
        }
    ]


def _run_inference(pipeline, tensor, horizon, target_channel=0):
    """Run predict_quantiles and return (quantiles_transposed, inference_ms).

    Requests the quantile levels in _QUANTILE_LEVELS, which stay within the
    trained [0.1, 0.9] range (0.25/0.75 are interpolated, never extrapolated).
    quantiles_transposed has shape [len(_QUANTILE_LEVELS), horizon] — rows are
    quantile levels in _QUANTILE_LEVELS order.
    """
    t0 = _time.time()
    quantiles, _ = pipeline.predict_quantiles(
        inputs=tensor, prediction_length=horizon, quantile_levels=_QUANTILE_LEVELS
    )
    inference_ms = (_time.time() - t0) * 1000

    # Extract quantile matrix for the target channel
    if isinstance(quantiles, list):
        q = torch.stack(quantiles)[target_channel]  # [horizon, n_quantiles]
    else:
        q = quantiles[
            target_channel
        ]  # [horizon, n_quantiles] or [1, horizon, n_quantiles]

    if q.dim() == 3:
        q = q[0]  # [horizon, n_quantiles]

    return q.T, inference_ms  # [n_quantiles, horizon]


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def _build_forecast_lines(
    fc_t,
    target_measurement,
    field,
    model_id,
    source_table,
    tags,
    mode,
    step_ns,
    base_ns,
    horizon,
):
    """Build LineBuilder objects for each forecast step.

    fc_t: quantile tensor of shape [len(_QUANTILE_LEVELS), horizon], rows in
    _QUANTILE_LEVELS order [0.1, 0.25, 0.5, 0.75, 0.9].
    Returns list of LineBuilder objects.
    """
    model_label = model_id.split("/")[-1]
    lines = []

    for i in range(horizon):
        ts_ns = base_ns + (i + 1) * step_ns
        line = (
            LineBuilder(target_measurement)
            .tag("field", field)
            .tag("model", model_label)
            .tag("source_table", source_table)
            .tag("mode", mode)
        )
        # Add user-specified tags. tags maps {name: [values]}; a single value is
        # written as a tag, while multi-value filters are skipped (they can't be
        # represented as one tag without ambiguity).
        for k, v in tags.items():
            tag_vals = v if isinstance(v, list) else [v]
            if len(tag_vals) == 1:
                line = line.tag(k, str(tag_vals[0]))

        line = (
            line.float64_field("forecast", fc_t[2, i].item())
            .float64_field("lower_80", fc_t[0, i].item())
            .float64_field("upper_80", fc_t[4, i].item())
            .float64_field("lower_50", fc_t[1, i].item())
            .float64_field("upper_50", fc_t[3, i].item())
            .int64_field("step", i + 1)
            .time_ns(ts_ns)
        )
        lines.append(line)

    return lines


def _write_lines(influxdb3_local, lines, target_database, task_id):
    """Write all forecast lines in a single synchronous batch write.

    Returns (success: bool, error: str or None).
    """
    if not lines:
        return True, None
    try:
        batch = _BatchLines(lines)
        if target_database:
            influxdb3_local.write_sync_to_db(target_database, batch, no_sync=True)
        else:
            influxdb3_local.write_sync(batch, no_sync=True)
        influxdb3_local.info(f"[{task_id}] Wrote {len(lines)} forecast lines")
        return True, None
    except Exception as e:
        error_msg = str(e)
        influxdb3_local.error(f"[{task_id}] Write failed: {error_msg}")
        return False, error_msg


def _build_http_response(
    model_id,
    table,
    field,
    values,
    timestamps,
    fc_t,
    step_ms,
    load_secs,
    inference_ms,
    horizon,
):
    """Build the JSON response dict matching the UI's ForecastResponse contract."""
    last_ts_ms = _parse_ts_to_ms(timestamps[-1]) if timestamps else None

    # Forecast points
    forecast_points = []
    for step in range(horizon):
        ts = None
        if last_ts_ms is not None:
            ts = last_ts_ms + (step + 1) * step_ms
        forecast_points.append(
            {
                "step": step + 1,
                "timestamp": ts,
                "median": round(fc_t[2, step].item(), 4),
                "lower_80": round(fc_t[0, step].item(), 4),
                "upper_80": round(fc_t[4, step].item(), 4),
                "lower_50": round(fc_t[1, step].item(), 4),
                "upper_50": round(fc_t[3, step].item(), 4),
            }
        )

    # Historical points (last 10 for chart context). Use the real per-point
    # timestamps (aligned 1:1 with values) rather than reconstructing them from
    # a uniform step, so gaps in the source data are reflected accurately.
    n_hist = min(10, len(values))
    historical_points = []
    for i in range(len(values) - n_hist, len(values)):
        ts = _parse_ts_to_ms(timestamps[i]) if i < len(timestamps) else None
        historical_points.append(
            {
                "timestamp": ts,
                "value": round(values[i], 4),
            }
        )

    return {
        "status": "ok",
        "model_id": model_id,
        "table": table,
        "field": field,
        "context_length": len(values),
        "horizon": horizon,
        "load_seconds": round(load_secs, 2),
        "inference_ms": round(inference_ms, 1),
        "step_ms": step_ms,
        "historical": historical_points,
        "forecast": forecast_points,
    }


# ===========================================================================
# Entry point: HTTP trigger
# ===========================================================================


def process_request(
    influxdb3_local, query_parameters, request_headers, request_body, args=None
):
    """HTTP entry point — on-demand forecasting for any table/field.

    Args:
        influxdb3_local: Processing engine API handle for queries, writes, and logging.
        query_parameters: Dict of URL query parameters (not used; parameters are
            read from the JSON request body).
        request_headers: Dict of HTTP request headers.
        request_body: JSON object with the request parameters (table, field, and
            optional settings such as where_clause, horizon, covariate_fields).
            Values here override any trigger args of the same name.
        args: Optional trigger arguments set at trigger creation time; used as
            defaults when a key is absent from the request body.

    Returns:
        Dict with status, model metadata, historical points, and forecast points
        including median and 50%/80% prediction intervals.
    """
    args = args or {}
    task_id = str(uuid.uuid4())

    try:
        # Parameters come from the JSON request body, falling back to trigger
        # args; body values override args.
        body = {}
        if request_body:
            try:
                body = json.loads(request_body)
            except (json.JSONDecodeError, ValueError):
                return {"status": "error", "error": "Invalid JSON in request body"}
            if not isinstance(body, dict):
                return {
                    "status": "error",
                    "error": "Request body must be a JSON object",
                }
        cfg = {**args, **body}

        # --- Extract parameters ---
        table = cfg.get("table")
        field = cfg.get("field")
        horizon = _parse_int(cfg.get("horizon", 64), "horizon", min_value=1)
        context_limit = _parse_int(cfg.get("context_limit", 512), "context_limit", min_value=1)
        model_id = cfg.get("model_id", "amazon/chronos-bolt-tiny")
        where_clause = cfg.get("where_clause", "")
        covariate_fields_raw = cfg.get("covariate_fields", "")
        covariate_mode = str(cfg.get("covariate_mode", "covariate")).lower()
        write_results = str(cfg.get("write_results", "false")).lower() == "true"
        target_measurement = cfg.get("target_measurement")
        target_database = cfg.get("target_database")

        if isinstance(covariate_fields_raw, list):
            cov_names = [str(c) for c in covariate_fields_raw]
        elif covariate_fields_raw:
            cov_names = str(covariate_fields_raw).split()
        else:
            cov_names = []

        # --- Validate required params ---
        if not table or not field:
            return {
                "status": "error",
                "error": "Missing required parameters: 'table' and 'field'",
            }

        # --- Covariates are only supported by Chronos-2 models ---
        if cov_names and not _is_chronos2(model_id):
            return {
                "status": "error",
                "error": (
                    f"covariate_fields requires a Chronos-2 model, but model_id is "
                    f"'{model_id}'. Use a model with 'chronos-2' in its name."
                ),
            }
        if covariate_mode not in ("covariate", "target"):
            return {
                "status": "error",
                "error": (
                    f"Invalid covariate_mode '{covariate_mode}': expected "
                    f"'covariate' or 'target'."
                ),
            }

        influxdb3_local.info(
            f"[{task_id}] forecast_series: table={table}, field={field}, "
            f"model={model_id}, horizon={horizon}, ctx={context_limit}, "
            f"covariates={cov_names}"
        )

        # --- Load model ---
        try:
            pipeline, load_secs = _load_model(model_id, influxdb3_local, task_id)
        except Exception as e:
            return {
                "status": "error",
                "error": f"Failed to load model '{model_id}': {e}",
            }

        # --- Query target + covariates together (time-aligned by row) ---
        fields = [field] + cov_names
        sql, aliases = _build_query_simple(table, fields, where_clause, context_limit)
        try:
            # Rows come back DESC; reverse=True restores chronological order.
            times, columns = _query_aligned_series(
                influxdb3_local, sql, aliases, "time", reverse=True
            )
        except Exception as e:
            return {"status": "error", "error": f"Query failed: {e}"}

        vals = columns[0]
        if len(vals) < 10:
            return {
                "status": "error",
                "error": f"Only {len(vals)} data points — need at least 10",
            }

        influxdb3_local.info(
            f"[{task_id}] Got {len(vals)} values, forecasting {horizon} steps"
        )

        # --- Assemble covariates (already aligned to the target rows) ---
        covariates = []
        for idx in range(1, len(fields)):
            filled = _fill_nulls(columns[idx])
            if filled is None:
                influxdb3_local.warn(
                    f"[{task_id}] Covariate '{fields[idx]}' has no data, skipping"
                )
                continue
            covariates.append((fields[idx], filled))

        tensor = _build_model_input(vals, covariates, model_id, covariate_mode)

        # --- Run inference ---
        try:
            fc_t, inference_ms = _run_inference(
                pipeline, tensor, horizon, target_channel=0
            )
        except Exception as e:
            return {"status": "error", "error": f"Inference failed: {e}"}

        step_ms = _compute_step_ms(times)

        # --- Optionally write results to DB ---
        if write_results and not target_measurement:
            influxdb3_local.warn(
                f"[{task_id}] write_results=true but target_measurement is not set; "
                "skipping write and returning forecast JSON only"
            )
        if write_results and target_measurement:
            step_ns = step_ms * 1_000_000
            last_ts_ms = _parse_ts_to_ms(times[-1])
            base_ns = (
                last_ts_ms * 1_000_000 if last_ts_ms else _get_now_ns(influxdb3_local)
            )

            mode = "multivariate" if covariates else "univariate"
            lines = _build_forecast_lines(
                fc_t,
                target_measurement,
                field,
                model_id,
                table,
                {},
                mode,
                step_ns,
                base_ns,
                horizon,
            )
            success, err = _write_lines(
                influxdb3_local, lines, target_database, task_id
            )
            if not success:
                influxdb3_local.warn(
                    f"[{task_id}] Write failed but returning forecast JSON: {err}"
                )

        # --- Build and return response ---
        result = _build_http_response(
            model_id,
            table,
            field,
            vals,
            times,
            fc_t,
            step_ms,
            load_secs,
            inference_ms,
            horizon,
        )

        influxdb3_local.info(
            f"[{task_id}] forecast_series complete: {len(vals)} ctx -> {horizon} steps "
            f"in {inference_ms:.0f}ms, load={load_secs:.2f}s"
        )
        return result
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Unhandled error: {e}")
        return {"status": "error", "error": str(e)}


# ===========================================================================
# Entry point: Scheduled trigger
# ===========================================================================


def process_scheduled_call(
    influxdb3_local, call_time: datetime, args: dict | None = None
):
    """Scheduled entry point — recurring forecasts written to InfluxDB.

    Args:
        influxdb3_local: Processing engine API handle for queries, writes, and logging.
        call_time: Timestamp when the scheduled trigger fired.
        args: Trigger arguments. Config is taken from EITHER the TOML file
            (when config_file_path is set) OR the trigger args, never merged.
            Required keys: measurement, field, window, horizon. Optional:
            target_measurement (defaults to _forecasts.{measurement}).

    Returns:
        None. Forecast results are written to the target measurement via LineBuilder.
    """
    args = args or {}
    task_id = str(uuid.uuid4())

    try:
        # Config comes from EITHER the TOML file OR the trigger args, never merged.
        config_path = args.get("config_file_path")
        if config_path:
            try:
                cfg = _load_toml_config(config_path, influxdb3_local, task_id)
            except Exception as e:
                influxdb3_local.error(f"[{task_id}] Failed to load config file: {e}")
                return None
        else:
            cfg = args

        # --- Extract parameters ---
        measurement = cfg.get("measurement")
        field = cfg.get("field")
        window = cfg.get("window")
        horizon_str = cfg.get("horizon")
        target_measurement = cfg.get("target_measurement")
        model_id = cfg.get("model_id", "amazon/chronos-bolt-tiny")
        context_limit = _parse_int(cfg.get("context_limit", "512"), "context_limit", min_value=1)
        agg_interval = cfg.get("agg_interval", "30s")
        tag_values_raw = cfg.get("tag_values")
        covariate_fields_raw = cfg.get("covariate_fields", "")
        covariate_mode = str(cfg.get("covariate_mode", "covariate")).lower()
        target_database = cfg.get("target_database")

        if isinstance(covariate_fields_raw, list):
            cov_names = [str(c) for c in covariate_fields_raw]
        elif covariate_fields_raw:
            cov_names = str(covariate_fields_raw).split()
        else:
            cov_names = []

        # --- Validate ---
        missing = []
        if not measurement:
            missing.append("measurement")
        if not field:
            missing.append("field")
        if not window:
            missing.append("window")
        if not horizon_str:
            missing.append("horizon")
        if missing:
            influxdb3_local.error(
                f"[{task_id}] Missing required config: {', '.join(missing)}"
            )
            return None

        # Covariates are only supported by Chronos-2 models.
        if cov_names and not _is_chronos2(model_id):
            influxdb3_local.error(
                f"[{task_id}] covariate_fields requires a Chronos-2 model, but "
                f"model_id is '{model_id}'. Use a model with 'chronos-2' in its name."
            )
            return None
        if covariate_mode not in ("covariate", "target"):
            influxdb3_local.error(
                f"[{task_id}] Invalid covariate_mode '{covariate_mode}': expected "
                f"'covariate' or 'target'."
            )
            return None

        # agg_interval drives both the date_bin aggregation and the spacing of the
        # written forecast timestamps, so step_ns and the SQL interval come from
        # the same parse. Invalid values raise and are caught by the outer handler.
        horizon = _parse_int(horizon_str, "horizon", min_value=1)
        step_ns, agg_sql_interval = _parse_interval(agg_interval)
        _, window_sql_interval = _parse_interval(window)

        if not target_measurement:
            target_measurement = f"_forecasts.{measurement}"

        influxdb3_local.info(
            f"[{task_id}] Scheduled forecast: {measurement}.{field}, "
            f"window={window}, horizon={horizon}, agg={agg_interval}, "
            f"target={target_measurement}, model={model_id}"
        )

        # --- Parse tags ---
        try:
            tag_values = _parse_tag_values(tag_values_raw)
        except ValueError as e:
            influxdb3_local.error(f"[{task_id}] Invalid tag_values: {e}")
            return None
        tag_sql = _generate_tag_filter_sql(tag_values)

        # --- Load model ---
        try:
            pipeline, load_secs = _load_model(model_id, influxdb3_local, task_id)
        except Exception as e:
            influxdb3_local.error(f"[{task_id}] Failed to load model: {e}")
            return None

        # --- Query target + covariates together (time-aligned on the date_bin grid) ---
        fields = [field] + cov_names
        sql, aliases = _build_query_aggregated(
            measurement, fields, agg_sql_interval, window_sql_interval, tag_sql
        )
        try:
            timestamps, columns = _query_aligned_series(
                influxdb3_local, sql, aliases, "t"
            )
        except Exception as e:
            influxdb3_local.error(f"[{task_id}] Query failed: {e}")
            return None

        # Trim every column to the context limit consistently (rows stay aligned).
        if len(columns[0]) > context_limit:
            timestamps = timestamps[-context_limit:]
            columns = [c[-context_limit:] for c in columns]
        values = columns[0]

        if len(values) < 10:
            influxdb3_local.error(
                f"[{task_id}] Only {len(values)} data points — need at least 10"
            )
            return None

        influxdb3_local.info(f"[{task_id}] Got {len(values)} values")

        # --- Assemble covariates (already aligned to the target rows) ---
        covariates = []
        for idx in range(1, len(fields)):
            filled = _fill_nulls(columns[idx])
            if filled is None:
                influxdb3_local.warn(
                    f"[{task_id}] Covariate '{fields[idx]}' has no data, skipping"
                )
                continue
            covariates.append((fields[idx], filled))

        tensor = _build_model_input(values, covariates, model_id, covariate_mode)

        # --- Run inference ---
        try:
            fc_t, inference_ms = _run_inference(
                pipeline, tensor, horizon, target_channel=0
            )
        except Exception as e:
            influxdb3_local.error(f"[{task_id}] Inference failed: {e}")
            return None

        influxdb3_local.info(f"[{task_id}] Inference completed in {inference_ms:.0f}ms")

        # --- Write forecast to DB ---
        # Anchor forecast timestamps to the last observed data point (not now()),
        # so forecasts line up with the source series even if the data lags.
        last_ts_ms = _parse_ts_to_ms(timestamps[-1]) if timestamps else None
        base_ns = last_ts_ms * 1_000_000 if last_ts_ms else _get_now_ns(influxdb3_local)

        mode = "multivariate" if covariates else "univariate"
        lines = _build_forecast_lines(
            fc_t,
            target_measurement,
            field,
            model_id,
            measurement,
            tag_values,
            mode,
            step_ns,
            base_ns,
            horizon,
        )

        success, err = _write_lines(influxdb3_local, lines, target_database, task_id)

        if success:
            influxdb3_local.info(
                f"[{task_id}] Scheduled forecast complete: {len(values)} ctx -> "
                f"{horizon} steps, inference={inference_ms:.0f}ms, "
                f"load={load_secs:.2f}s, wrote {len(lines)} lines"
            )
        else:
            influxdb3_local.error(
                f"[{task_id}] Scheduled forecast failed to write: {err}"
            )

        return None
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Unhandled error: {e}")
        return None
