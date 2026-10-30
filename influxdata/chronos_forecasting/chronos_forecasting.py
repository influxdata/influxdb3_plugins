"""
{
    "plugin_type": ["scheduled", "http"],
    "scheduled_args_config": [
        {
            "name": "source_measurement",
            "example": "cpu",
            "description": "Measurement to read. Required with field unless query is set.",
            "required": false
        },
        {
            "name": "field",
            "example": "usage",
            "description": "Numeric field to forecast. Required with source_measurement unless query is set.",
            "required": false
        },
        {
            "name": "query",
            "example": "SELECT date_bin(INTERVAL '5 minutes', time) AS time, host, avg(usage) AS usage FROM cpu GROUP BY 1, host",
            "description": "SQL whose result is forecast instead of source_measurement and field. The timestamp column must be named time; text columns are series keys and each numeric column is forecast on its own. The whole result is read on every run, so bound its time range in the query. The CLI splits --trigger-arguments at commas, so set a query with commas in the TOML config.",
            "required": false
        },
        {
            "name": "source_database",
            "example": "demo",
            "description": "Database to read from. Default: the trigger's database.",
            "required": false
        },
        {
            "name": "tag_filter",
            "example": "host=server1 region=eu",
            "description": "Space-separated tag=value pairs the rows must match, one value per tag; several values per tag only through the TOML config. Not with query.",
            "required": false
        },
        {
            "name": "sql_filter",
            "example": "usage > 0",
            "description": "SQL condition without WHERE, AND-ed with tag_filter and inserted into the query as is. Not with query. The CLI splits --trigger-arguments at commas, so set a condition with commas in the TOML config.",
            "required": false
        },
        {
            "name": "group_by",
            "example": "host",
            "description": "Space-separated tags to split by; each distinct combination is its own series, otherwise all rows form one. Without sampling_interval, a series mixing tag sets that write at the same time is refused. A tag named like a stored row column (field, model, forecast, step, ...) is refused. Not with query.",
            "required": false
        },
        {
            "name": "series_limit",
            "example": "5",
            "description": "Forecast only the first N forecasts (series x fields), ordered by the series' group_by tags or query key columns as text, a missing value last, then by field name; the rest are skipped. An integer >= 1, at most max_series. Unset: every series.",
            "required": false
        },
        {
            "name": "window",
            "example": "6h",
            "description": "History as a time window ending now: a whole number and s, min, h, d or w. With point_limit, the last point_limit points of the window; never more points than the model's context. Default: point_limit buckets of sampling_interval (at most 365d), or 1d of raw points; none with query.",
            "required": false
        },
        {
            "name": "point_limit",
            "example": "512",
            "description": "History as the most recent cells of each series' regular grid (its buckets with sampling_interval), an integer >= 10, at most the model's context. Default 512 when window is not set either.",
            "required": false
        },
        {
            "name": "sampling_interval",
            "example": "30s",
            "description": "Resamples history to the mean per bucket of this duration (s, min, h, d, w); empty buckets are passed as gaps and the bucket still filling is left out. Default: a grid at the median spacing of the points.",
            "required": false
        },
        {
            "name": "horizon_steps",
            "example": "64",
            "description": "Forecast length in points at the data spacing, an integer >= 1. Exactly one of horizon_steps and horizon_duration is required.",
            "required": false
        },
        {
            "name": "horizon_duration",
            "example": "1h",
            "description": "Forecast length as a duration (s, min, h, d, w), converted to points by the data spacing. Exactly one of horizon_steps and horizon_duration is required.",
            "required": false
        },
        {
            "name": "levels",
            "example": "0.5 0.8",
            "description": "Space-separated interval levels, each between 0 and 1, written as lower_<percent> and upper_<percent>. A level the model cannot produce is not written. Default: 0.5 0.8.",
            "required": false
        },
        {
            "name": "model",
            "example": "amazon/chronos-bolt-tiny",
            "description": "HuggingFace model ID: Chronos-Bolt, Chronos-2 or original Chronos. Default: amazon/chronos-bolt-tiny.",
            "required": false
        },
        {
            "name": "target_measurement",
            "example": "_forecasts.cpu",
            "description": "Measurement the forecast is written to. Default: _forecasts.<source_measurement>; required when query reads several measurements.",
            "required": false
        },
        {
            "name": "target_database",
            "example": "forecasts",
            "description": "Database the forecast is written to. Default: the trigger's database.",
            "required": false
        },
        {
            "name": "forecast_id",
            "example": "ops-1h",
            "description": "Written to every row as the tag forecast_id, so forecasts of one series with different settings do not overwrite each other. Unset: no tag.",
            "required": false
        },
        {
            "name": "covariate_fields",
            "example": "humidity pressure",
            "description": "Space-separated numeric fields of source_measurement, other than field, passed to a Chronos-2 model as past covariates. Requires a Chronos-2 model; not with query.",
            "required": false
        },
        {
            "name": "max_series",
            "example": "100",
            "description": "Most forecasts one run may produce (series x fields), an integer >= 1; more is refused unless series_limit keeps fewer. Default 100.",
            "required": false
        },
        {
            "name": "max_forecast_points",
            "example": "1024",
            "description": "Most forecast points per series, an integer >= 1; a longer horizon is refused. Default 1024.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "chronos_forecasting_scheduler.toml",
            "description": "TOML config file, absolute or relative to the plugin directory. Its keys override trigger arguments.",
            "required": false
        }
    ],
    "http_args_config": [
        {
            "name": "source_measurement",
            "example": "cpu",
            "description": "Default measurement to read; a request may set it. Required with field unless query is set.",
            "required": false
        },
        {
            "name": "field",
            "example": "usage",
            "description": "Default numeric field to forecast; a request may set it.",
            "required": false
        },
        {
            "name": "query",
            "example": "SELECT date_bin(INTERVAL '5 minutes', time) AS time, host, avg(usage) AS usage FROM cpu GROUP BY 1, host",
            "description": "Default SQL whose result is forecast instead of source_measurement and field; a request may set either. The timestamp column must be named time; text columns are series keys and each numeric column is forecast on its own. The CLI splits --trigger-arguments at commas, so set a query with commas in the TOML config or the request.",
            "required": false
        },
        {
            "name": "source_database",
            "example": "demo",
            "description": "Default database to read from. Default: the trigger's database.",
            "required": false
        },
        {
            "name": "tag_filter",
            "example": "host=server1 region=eu",
            "description": "Default space-separated tag=value pairs the rows must match, one value per tag; several values per tag only through the TOML config. Not with query.",
            "required": false
        },
        {
            "name": "sql_filter",
            "example": "usage > 0",
            "description": "Default SQL condition without WHERE, AND-ed with tag_filter and inserted into the query as is. Not with query. The CLI splits --trigger-arguments at commas, so set a condition with commas in the TOML config or the request.",
            "required": false
        },
        {
            "name": "group_by",
            "example": "host",
            "description": "Default space-separated tags to split by; each distinct combination is its own series, otherwise all rows form one. Without sampling_interval, a series mixing tag sets that write at the same time is refused; when the forecast is written, a tag named like a stored row column (field, model, forecast, step, ...) is refused. Not with query.",
            "required": false
        },
        {
            "name": "series_limit",
            "example": "5",
            "description": "Default count of forecasts (series x fields) to keep: the first N, ordered by the series' group_by tags or query key columns as text, a missing value last, then by field name; the rest are skipped and series_total in the response counts them all. An integer >= 1, at most max_series. Unset: every series.",
            "required": false
        },
        {
            "name": "window",
            "example": "6h",
            "description": "Default history as a time window ending now: a whole number and s, min, h, d or w. With point_limit, the last point_limit points of the window; never more points than the model's context. Default: point_limit buckets of sampling_interval (at most 365d), or 1d of raw points; none with query.",
            "required": false
        },
        {
            "name": "point_limit",
            "example": "512",
            "description": "Default history in most recent cells of each series' regular grid (its buckets with sampling_interval), an integer >= 10, at most the model's context. Default 512 when window is not set either.",
            "required": false
        },
        {
            "name": "sampling_interval",
            "example": "30s",
            "description": "Default resampling to the mean per bucket of this duration (s, min, h, d, w); empty buckets are passed as gaps and the bucket still filling is left out. Default: a grid at the median spacing of the points.",
            "required": false
        },
        {
            "name": "horizon_steps",
            "example": "64",
            "description": "Default forecast length in points at the data spacing, an integer >= 1; a request may set it or horizon_duration instead. Without a default here, a request sets one of them.",
            "required": false
        },
        {
            "name": "horizon_duration",
            "example": "1h",
            "description": "Default forecast length as a duration (s, min, h, d, w), converted to points by the data spacing; a request may set it or horizon_steps instead.",
            "required": false
        },
        {
            "name": "levels",
            "example": "0.5 0.8",
            "description": "Default space-separated interval levels, each between 0 and 1. Default: 0.5 0.8.",
            "required": false
        },
        {
            "name": "model",
            "example": "amazon/chronos-bolt-tiny",
            "description": "Default HuggingFace model ID. Default: amazon/chronos-bolt-tiny.",
            "required": false
        },
        {
            "name": "write_results",
            "example": "false",
            "description": "Also write each forecast to target_measurement. Default: false.",
            "required": false
        },
        {
            "name": "target_measurement",
            "example": "_forecasts.cpu",
            "description": "Measurement a written forecast goes to. Default: _forecasts.<source_measurement>; required when query reads several measurements.",
            "required": false
        },
        {
            "name": "target_database",
            "example": "forecasts",
            "description": "Database a written forecast goes to. Default: the trigger's database.",
            "required": false
        },
        {
            "name": "forecast_id",
            "example": "ops-1h",
            "description": "Written to every row of a written forecast as the tag forecast_id, so forecasts of one series with different settings do not overwrite each other. Unset: no tag.",
            "required": false
        },
        {
            "name": "covariate_fields",
            "example": "humidity pressure",
            "description": "Default space-separated numeric fields of source_measurement, other than field, passed to a Chronos-2 model as past covariates. Requires a Chronos-2 model; not with query.",
            "required": false
        },
        {
            "name": "max_series",
            "example": "100",
            "description": "Most forecasts one request may produce (series x fields), an integer >= 1; more is refused unless series_limit keeps fewer. Default 100.",
            "required": false
        },
        {
            "name": "max_forecast_points",
            "example": "1024",
            "description": "Most forecast points per series, an integer >= 1; a longer horizon is refused. Default 1024.",
            "required": false
        },
        {
            "name": "backtest_points",
            "example": "12",
            "description": "Default number of latest history points to hold out: they are forecast from the points before them, and each series gets the share inside each interval and the mean absolute error. An integer >= 1, at most max_forecast_points. Unset: no backtest.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "chronos_forecasting_http.toml",
            "description": "TOML config file with defaults, absolute or relative to the plugin directory. Every request key may also be set here.",
            "required": false
        }
    ],
    "http_body_config": [
        {
            "name": "source_measurement",
            "example": "cpu",
            "description": "Measurement to read. Required with field unless query is set, here or in the trigger's settings.",
            "required": false
        },
        {
            "name": "field",
            "example": "usage",
            "description": "Numeric field to forecast.",
            "required": false
        },
        {
            "name": "query",
            "example": "SELECT date_bin(INTERVAL '5 minutes', time) AS time, host, avg(usage) AS usage FROM cpu WHERE time > now() - INTERVAL '6 hours' GROUP BY 1, host",
            "description": "SQL whose result is forecast instead of source_measurement and field; sent here, it replaces those set on the trigger. The timestamp column must be named time; text columns are series keys and each numeric column is forecast on its own. The whole result is read, so bound its time range in the query.",
            "required": false
        },
        {
            "name": "source_database",
            "example": "demo",
            "description": "Database to read from. Default: the trigger's database.",
            "required": false
        },
        {
            "name": "tag_filter",
            "example": "{'host': ['server1', 'server2'], 'region': 'eu'}",
            "description": "JSON object of tag to a value or a list of values the rows must match; a host=a region=eu string is also accepted. Not with query.",
            "required": false
        },
        {
            "name": "sql_filter",
            "example": "usage > 0",
            "description": "SQL condition without WHERE, AND-ed with tag_filter and inserted into the query as is. Not with query.",
            "required": false
        },
        {
            "name": "group_by",
            "example": "['host']",
            "description": "JSON array of tags to split by; each distinct combination is its own series, otherwise all rows form one. Without sampling_interval, a series mixing tag sets that write at the same time is refused; when the forecast is written, a tag named like a stored row column (field, model, forecast, step, ...) is refused. Not with query.",
            "required": false
        },
        {
            "name": "series_limit",
            "example": "5",
            "description": "Integer >= 1, at most max_series: forecast only the first N forecasts (series x fields), ordered by the series' group_by tags or query key columns as text, a missing value last, then by field name; series_total in the response counts them all. Unset: every series.",
            "required": false
        },
        {
            "name": "window",
            "example": "6h",
            "description": "History as a time window ending now: a whole number and s, min, h, d or w. With point_limit, the last point_limit points of the window; never more points than the model's context. Default: point_limit buckets of sampling_interval (at most 365d), or 1d of raw points; none with query.",
            "required": false
        },
        {
            "name": "point_limit",
            "example": "512",
            "description": "Integer >= 10: history in the most recent cells of each series' regular grid (its buckets with sampling_interval), at most the model's context. Default 512 when window is not set either.",
            "required": false
        },
        {
            "name": "sampling_interval",
            "example": "30s",
            "description": "Resample history to the mean per bucket of this duration (s, min, h, d, w); empty buckets are passed as gaps and the bucket still filling is left out. Default: a grid at the median spacing of the points.",
            "required": false
        },
        {
            "name": "horizon_steps",
            "example": "64",
            "description": "Integer >= 1: forecast length in points at the data spacing. Exactly one of horizon_steps and horizon_duration, here or on the trigger; one sent here replaces the other set on the trigger.",
            "required": false
        },
        {
            "name": "horizon_duration",
            "example": "1h",
            "description": "Forecast length as a duration (s, min, h, d, w), converted to points by the data spacing. Exactly one of horizon_steps and horizon_duration.",
            "required": false
        },
        {
            "name": "levels",
            "example": "[0.5, 0.8]",
            "description": "JSON array of interval levels, each between 0 and 1; a level the model cannot produce is null in the response. Default: [0.5, 0.8].",
            "required": false
        },
        {
            "name": "model",
            "example": "amazon/chronos-bolt-tiny",
            "description": "HuggingFace model ID or a local path. Default: amazon/chronos-bolt-tiny.",
            "required": false
        },
        {
            "name": "write_results",
            "example": "true",
            "description": "Boolean: also write the forecast to target_measurement. Default: false.",
            "required": false
        },
        {
            "name": "target_measurement",
            "example": "_forecasts.cpu",
            "description": "Measurement a written forecast goes to. Default: _forecasts.<source_measurement>; required when query reads several measurements.",
            "required": false
        },
        {
            "name": "target_database",
            "example": "forecasts",
            "description": "Database a written forecast goes to. Default: the trigger's database.",
            "required": false
        },
        {
            "name": "forecast_id",
            "example": "ops-1h",
            "description": "Written to every row of a written forecast as the tag forecast_id, so forecasts of one series with different settings do not overwrite each other. Unset: no tag.",
            "required": false
        },
        {
            "name": "covariate_fields",
            "example": "['humidity', 'pressure']",
            "description": "JSON array of numeric fields of source_measurement, other than field, passed to a Chronos-2 model as past covariates. Requires a Chronos-2 model; not with query.",
            "required": false
        },
        {
            "name": "max_series",
            "example": "100",
            "description": "Integer >= 1: most forecasts the request may produce (series x fields); more is refused unless series_limit keeps fewer. Default 100.",
            "required": false
        },
        {
            "name": "max_forecast_points",
            "example": "1024",
            "description": "Integer >= 1: most forecast points per series; a longer horizon is refused. Default 1024.",
            "required": false
        },
        {
            "name": "backtest_points",
            "example": "12",
            "description": "Integer from 1 to max_forecast_points: hold out the latest N history points, forecast them from the points before them, and return each series' backtest (the share inside each interval and the mean absolute error). Unset: no backtest.",
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

import dataclasses
import math
import re
import sys
import threading
import time as _time
import types
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import torch
from chronos import BaseChronosPipeline, Chronos2Pipeline
from influxdata_plugin_utils.config import load_config
from influxdata_plugin_utils.introspection import NUMERIC_TYPES, TAG_DATA_TYPE, get_schema
from influxdata_plugin_utils.parsing import parse_bool, parse_int
from influxdata_plugin_utils.sources import (
    KeySpec,
    parse_env,
    parse_json_body,
    parse_query_parameters,
    parse_request_headers,
    parse_toml,
    parse_trigger_args,
)
from influxdata_plugin_utils.validation import Validator
from influxdata_plugin_utils.write import write_data

DEFAULT_MODEL = "amazon/chronos-bolt-tiny"
DEFAULT_LEVELS = (0.5, 0.8)
DEFAULT_POINT_LIMIT = 512
# Without window, measurement mode reads point_limit buckets of sampling_interval
# (at most a year) or a day of raw points, so a read never scans the whole table.
DEFAULT_WINDOW_NS = 24 * 3600 * 10**9
MAX_DEFAULT_WINDOW_NS = 365 * 24 * 3600 * 10**9
DEFAULT_MAX_SERIES = 100
DEFAULT_MAX_FORECAST_POINTS = 1024
# Fewest observed points a series needs; the manifest's min_points.
MIN_POINTS = 10
# History read per series when a model does not report its context length.
FALLBACK_CONTEXT = 8192
# Spacing assumed when a series has no two distinct timestamps.
FALLBACK_STEP_NS = 30 * 10**9

# Columns of a stored forecast row, besides lower_<level>/upper_<level>.
FORECAST_ROW_TAGS = ("field", "model", "source_measurement", "forecast_id")
FORECAST_ROW_FIELDS = ("forecast", "step", "points_read", "run_time")

HTTP_STATUS = {
    "bad_request": 400,
    "unauthorized": 401,
    "model_unavailable": 503,
    "timeout": 504,
    "internal": 500,
    "other": 400,
}


class ForecastError(Exception):
    """A failure answered with one of the contract's error codes."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

CONTRACT_KEYS = (
    "source_measurement",
    "field",
    "query",
    "source_database",
    "tag_filter",
    "sql_filter",
    "group_by",
    "series_limit",
    "window",
    "point_limit",
    "sampling_interval",
    "horizon_steps",
    "horizon_duration",
    "levels",
    "model",
    "write_results",
    "target_measurement",
    "target_database",
    "forecast_id",
    "backtest_points",
)
PLUGIN_KEYS = ("covariate_fields", "max_series", "max_forecast_points")
SETTING_NAMES = CONTRACT_KEYS + PLUGIN_KEYS
# A scheduled run always writes and returns no response: no write_results, no backtest.
SCHEDULED_NAMES = tuple(
    name for name in SETTING_NAMES if name not in ("write_results", "backtest_points")
)

ENV_PREFIX = "INFLUXDB3_CHRONOS_FORECASTING_"
HEADER_PREFIX = "X-Influxdb3-Chronos-Forecasting-"

_DURATION_PATTERN = re.compile(r"(\d+)(s|min|h|d|w)")
_UNIT_NS = {
    "s": 10**9,
    "min": 60 * 10**9,
    "h": 3600 * 10**9,
    "d": 86400 * 10**9,
    "w": 604800 * 10**9,
}


def _text(raw) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"expected a non-empty string, got {raw!r}")
    return raw.strip()


def _names(raw) -> list[str]:
    """A list of names, from a list or a space-separated string."""
    if isinstance(raw, str):
        items = raw.split()
    elif isinstance(raw, list) and all(isinstance(item, str) for item in raw):
        items = [item.strip() for item in raw]
    else:
        raise ValueError(f"expected a list of names or a space-separated string, got {raw!r}")
    if not all(items):
        raise ValueError(f"names must not be empty, got {raw!r}")
    return list(dict.fromkeys(items))


def _duration_ns(raw) -> int:
    match = _DURATION_PATTERN.fullmatch(raw.strip()) if isinstance(raw, str) else None
    if not match or int(match.group(1)) == 0:
        raise ValueError(
            f"expected a whole number above 0 and a unit (s, min, h, d, w), got {raw!r}"
        )
    return int(match.group(1)) * _UNIT_NS[match.group(2)]


def _duration_text(ns: int) -> str:
    """A duration in the largest unit that divides it: 86400 s -> '1d'."""
    for unit in reversed(_UNIT_NS):
        if ns % _UNIT_NS[unit] == 0:
            return f"{ns // _UNIT_NS[unit]}{unit}"
    return f"{ns}ns"


def _levels(raw) -> list[float]:
    items = raw.split() if isinstance(raw, str) else raw
    if not isinstance(items, (list, tuple)):
        raise ValueError(f"expected a list of levels, got {raw!r}")
    levels = []
    for item in items:
        if isinstance(item, bool):
            raise ValueError(f"expected a number between 0 and 1, got {item!r}")
        try:
            level = float(item)
        except (TypeError, ValueError):
            raise ValueError(f"expected a number between 0 and 1, got {item!r}") from None
        if not 0 < level < 1:
            raise ValueError(f"each level must be between 0 and 1, got {item!r}")
        levels.append(level)
    return sorted(set(levels))


def _tag_value(raw) -> str:
    if isinstance(raw, bool) or not isinstance(raw, (str, int, float)) or raw == "":
        raise ValueError(f"expected a tag value, got {raw!r}")
    return str(raw)


def _tag_filter(raw) -> dict[str, list[str]]:
    """{tag: [values]} from 'host=a region=b' or from a JSON/TOML object."""
    result: dict[str, list[str]] = {}
    if isinstance(raw, str):
        for pair in raw.split():
            tag, sep, value = pair.partition("=")
            if not sep or not tag or not value:
                raise ValueError(f"expected space-separated tag=value pairs, got {pair!r}")
            if tag in result:
                raise ValueError(
                    f"tag {tag!r} is set twice; several values per tag need the TOML "
                    f"config or the JSON body"
                )
            result[tag] = [value]
        return result
    if isinstance(raw, dict):
        for tag, value in raw.items():
            values = value if isinstance(value, list) else [value]
            if not tag or not values:
                raise ValueError(f"expected a tag and at least one value, got {tag!r}")
            result[str(tag)] = list(dict.fromkeys(_tag_value(v) for v in values))
        return result
    raise ValueError(f"expected tag=value pairs or an object, got {raw!r}")


SETTING_VALIDATORS = [
    Validator(
        "source_measurement",
        "field",
        "query",
        "source_database",
        "sql_filter",
        "target_measurement",
        "target_database",
        "forecast_id",
        cast=_text,
    ),
    Validator("tag_filter", cast=_tag_filter),
    Validator("group_by", "covariate_fields", default=[], cast=_names),
    Validator("window", "sampling_interval", "horizon_duration", cast=_duration_ns),
    # A series needs MIN_POINTS points, so a shorter history is refused up front.
    Validator("point_limit", cast=lambda raw: parse_int(raw, minimum=MIN_POINTS)),
    Validator("horizon_steps", "series_limit", cast=lambda raw: parse_int(raw, minimum=1)),
    Validator("backtest_points", cast=lambda raw: parse_int(raw, minimum=1)),
    Validator(
        "max_series", default=DEFAULT_MAX_SERIES, cast=lambda raw: parse_int(raw, minimum=1)
    ),
    Validator(
        "max_forecast_points",
        default=DEFAULT_MAX_FORECAST_POINTS,
        cast=lambda raw: parse_int(raw, minimum=1),
    ),
    Validator("levels", default=list(DEFAULT_LEVELS), cast=_levels),
    Validator("model", default=DEFAULT_MODEL, cast=_text),
    Validator("write_results", default=False, cast=parse_bool),
]


def _env_spec(names) -> KeySpec:
    """Read settings from INFLUXDB3_CHRONOS_FORECASTING_<KEY>."""
    rename = {f"{ENV_PREFIX}{name.upper()}": name for name in names}
    return KeySpec(allowlist=tuple(rename), rename=rename)


def _header_name(name: str) -> str:
    return HEADER_PREFIX + name.replace("_", "-")


HEADER_KEYS = KeySpec(
    allowlist=tuple(_header_name(name) for name in SETTING_NAMES),
    rename={_header_name(name): name for name in SETTING_NAMES},
)
REQUEST_KEYS = KeySpec(allowlist=SETTING_NAMES, unknown="reject")


def _refuse_unknown_headers(request_headers) -> None:
    """A header with the plugin's prefix must name a setting, or a typo would be lost."""
    known = {_header_name(name).lower() for name in SETTING_NAMES}
    prefix = HEADER_PREFIX.lower()
    for name in request_headers or {}:
        if str(name).lower().startswith(prefix) and str(name).lower() not in known:
            raise ForecastError(
                "bad_request",
                f"unknown header {name!r}; a setting goes in {HEADER_PREFIX}<key>, "
                f"with hyphens for underscores",
            )

# Choices between keys: the side the highest source sets wins, and the other
# side from lower sources is dropped, so a request overrides a trigger default.
EXCLUSIVE_KEYS = (
    (
        ("query",),
        ("source_measurement", "field", "tag_filter", "sql_filter", "group_by", "covariate_fields"),
    ),
    (("horizon_steps",), ("horizon_duration",)),
)


def _given(value) -> bool:
    """Whether a raw value counts as set; empty text, lists and objects do not."""
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, dict)):
        return bool(value)
    return value is not None


def _drop_overridden(layers: list) -> list:
    """Keep, for each choice in EXCLUSIVE_KEYS, only the side set highest."""
    layers = [dict(layer) for layer in layers]
    for first, second in EXCLUSIVE_KEYS:
        tops = [
            max(
                (i for i, layer in enumerate(layers) if any(_given(layer.get(k)) for k in side)),
                default=-1,
            )
            for side in (first, second)
        ]
        if tops[0] == tops[1]:
            continue  # neither side, or both in one source: _resolve decides
        loser = second if tops[0] > tops[1] else first
        for layer in layers:
            for key in loser:
                layer.pop(key, None)
    return layers


def _load_settings(args, names, request_layers=()) -> dict:
    """Merge env, trigger args, TOML and, on demand, the request; later wins."""
    args = args or {}
    config_file_path = args.get("config_file_path") or parse_env(
        _env_spec(("config_file_path",))
    ).get("config_file_path")
    layers = [
        parse_env(_env_spec(names)),
        parse_trigger_args(
            args, KeySpec(allowlist=names + ("config_file_path",), unknown="reject")
        ),
        parse_toml(config_file_path, KeySpec(allowlist=names, unknown="reject")),
        *request_layers,
    ]
    return load_config(*_drop_overridden(layers), validators=SETTING_VALIDATORS)


@dataclasses.dataclass
class ForecastRequest:
    """A validated request: the contract keys after every cross-key check."""

    source_measurement: str | None
    field: str | None
    query: str | None
    source_database: str | None
    tag_filter: dict
    sql_filter: str | None
    group_by: list
    series_limit: int | None
    window_ns: int | None
    default_window: bool
    point_limit: int | None
    sampling_ns: int | None
    horizon_steps: int | None
    horizon_ns: int | None
    backtest_points: int | None
    levels: list
    model: str
    write_results: bool
    target_measurement: str | None
    target_database: str | None
    forecast_id: str | None
    covariate_fields: list
    max_series: int
    max_forecast_points: int


def _forecast_row_columns(levels: list) -> set:
    """Tag and field names of a stored forecast row."""
    names = {*FORECAST_ROW_TAGS, *FORECAST_ROW_FIELDS}
    for level in levels:
        key = _level_key(level)
        names.update((f"lower_{key}", f"upper_{key}"))
    return names


def _refuse_clashing_keys(keys: list, levels: list, kind: str) -> None:
    """Series keys are written as tags, so one named like a row column is refused."""
    columns = _forecast_row_columns(levels)
    for key in keys:
        if key in columns:
            raise ForecastError(
                "bad_request",
                f"{kind} {key!r} clashes with a column of the stored forecast rows; "
                f"rename it with an alias in a query",
            )


def _horizon_points(req: ForecastRequest, step_ns: int | None) -> int:
    """Forecast points of the horizon at a data spacing of step_ns."""
    # Integer ceiling: a float quotient past 2**53 ns can round up to one point more.
    return req.horizon_steps or max(1, -(-req.horizon_ns // step_ns))


def _check_horizon(req: ForecastRequest, points: int) -> None:
    if points > req.max_forecast_points:
        raise ForecastError(
            "bad_request",
            f"the horizon is {points} points, more than max_forecast_points="
            f"{req.max_forecast_points}; shorten it or raise max_forecast_points",
        )


def _resolve(cfg: dict, scheduled: bool) -> ForecastRequest:
    """Check the keys against each other and build the request."""
    query_mode = "query" in cfg
    if query_mode:
        # These first: set with query in one source, they keep lower sources'
        # source_measurement and field, which would be named instead.
        for key in ("tag_filter", "sql_filter", "group_by", "covariate_fields"):
            if cfg.get(key):
                raise ForecastError("bad_request", f"{key} does not apply to query")
        if "source_measurement" in cfg or "field" in cfg:
            raise ForecastError(
                "bad_request", "set either query or source_measurement and field, not both"
            )
    elif "source_measurement" not in cfg or "field" not in cfg:
        raise ForecastError(
            "bad_request", "source_measurement and field are required, or query"
        )

    if ("horizon_steps" in cfg) == ("horizon_duration" in cfg):
        raise ForecastError(
            "bad_request", "set exactly one of horizon_steps and horizon_duration"
        )
    if cfg.get("field") in cfg["covariate_fields"]:
        raise ForecastError(
            "bad_request", f"covariate_fields must not include the field {cfg['field']!r}"
        )
    write_results = True if scheduled else cfg["write_results"]
    if write_results:
        _refuse_clashing_keys(cfg["group_by"], cfg["levels"], "group_by tag")

    window_ns = cfg.get("window")
    point_limit = cfg.get("point_limit")
    if point_limit is None and window_ns is None:
        point_limit = DEFAULT_POINT_LIMIT
    sampling_ns = cfg.get("sampling_interval")
    backtest_points = cfg.get("backtest_points")
    default_window = window_ns is None and not query_mode
    if default_window:
        # The backtest's held-out buckets come on top, and one more: the bucket still
        # filling is dropped.
        buckets = point_limit + (backtest_points or 0) + 1
        window_ns = (
            min(buckets * sampling_ns, MAX_DEFAULT_WINDOW_NS) if sampling_ns else DEFAULT_WINDOW_NS
        )

    req = ForecastRequest(
        source_measurement=cfg.get("source_measurement"),
        field=cfg.get("field"),
        query=cfg.get("query"),
        source_database=cfg.get("source_database"),
        tag_filter=cfg.get("tag_filter") or {},
        sql_filter=cfg.get("sql_filter"),
        group_by=cfg["group_by"],
        series_limit=cfg.get("series_limit"),
        window_ns=window_ns,
        default_window=default_window,
        point_limit=point_limit,
        sampling_ns=sampling_ns,
        horizon_steps=cfg.get("horizon_steps"),
        horizon_ns=cfg.get("horizon_duration"),
        backtest_points=backtest_points,
        levels=cfg["levels"],
        model=cfg["model"],
        write_results=write_results,
        target_measurement=cfg.get("target_measurement"),
        target_database=cfg.get("target_database"),
        forecast_id=cfg.get("forecast_id"),
        covariate_fields=cfg["covariate_fields"],
        max_series=cfg["max_series"],
        max_forecast_points=cfg["max_forecast_points"],
    )
    # The horizon in points is known now unless the spacing comes from the data.
    if req.horizon_steps or req.sampling_ns:
        _check_horizon(req, _horizon_points(req, req.sampling_ns))
    if backtest_points and backtest_points > req.max_forecast_points:
        raise ForecastError(
            "bad_request",
            f"backtest_points is {backtest_points}, more than max_forecast_points="
            f"{req.max_forecast_points}; lower it or raise max_forecast_points",
        )
    # Within the cap, the limit always holds, so the cap never refuses a limited request.
    if req.series_limit and req.series_limit > req.max_series:
        raise ForecastError(
            "bad_request",
            f"series_limit is {req.series_limit}, more than max_series={req.max_series}; "
            f"lower it or raise max_series",
        )
    return req


# ---------------------------------------------------------------------------
# Model loading
# ---------------------------------------------------------------------------


# The engine reloads this module on every run, but the interpreter lives on, so what
# must outlive a run (the locks and loaded models) is kept in a module in sys.modules.
_STATE_MODULE = "_influxdb3_chronos_forecasting_state"
# Loaded models kept for later runs; the least recently used is dropped past this many.
_MAX_LOADED_MODELS = 2


def _shared_state() -> types.ModuleType:
    """The state every run shares: locks, loaded models and when each was last used."""
    state = sys.modules.get(_STATE_MODULE)
    if state is None:
        candidate = types.ModuleType(_STATE_MODULE)
        candidate.load_lock = threading.Lock()
        candidate.pipelines = {}
        candidate.last_used = {}
        candidate.inference_locks = {}
        # setdefault is atomic, so two first runs agree on one state.
        state = sys.modules.setdefault(_STATE_MODULE, candidate)
    if not hasattr(state, "last_used"):
        # The shared state may have been created without last_used.
        state.last_used = {}
    return state


def _load_model(model_id: str, influxdb3_local, task_id: str):
    """Load a model once per server and reuse it in later runs.

    `from_pretrained` is not thread-safe: two overlapping loads can switch off
    transformers' weight tying for good, and every later model then loads with
    weights on the meta device until InfluxDB restarts. So loads share one lock.
    """
    state = _shared_state()
    pipeline = state.pipelines.get(model_id)
    if pipeline is None:
        with state.load_lock:
            pipeline = state.pipelines.get(model_id)
            if pipeline is None:
                influxdb3_local.info(f"[{task_id}] Loading model {model_id}...")
                t0 = _time.time()
                pipeline = BaseChronosPipeline.from_pretrained(
                    model_id,
                    device_map="cpu",
                    dtype=torch.float32,
                )
                load_secs = _time.time() - t0
                # Another plugin loading a transformers model at the same time can still break it.
                if any(t.is_meta for t in pipeline.model.parameters()):
                    raise RuntimeError(
                        "The model loaded with weights missing. Another model loading "
                        "at the same time may have broken the transformers library; "
                        "restart InfluxDB."
                    )
                while len(state.pipelines) >= _MAX_LOADED_MODELS:
                    unused = min(state.pipelines, key=lambda name: state.last_used.get(name, 0.0))
                    state.pipelines.pop(unused)
                    state.last_used.pop(unused, None)
                state.pipelines[model_id] = pipeline
                influxdb3_local.info(f"[{task_id}] Model loaded in {load_secs:.1f}s")
    # Only a loaded model is recorded, so a failed name does not stay behind.
    state.last_used[model_id] = _time.monotonic()
    return pipeline


def _inference_lock(pipeline) -> threading.Lock:
    """The lock that keeps forecasts on one shared model one at a time."""
    locks = _shared_state().inference_locks
    return locks.setdefault(id(pipeline), threading.Lock())


# ---------------------------------------------------------------------------
# Reading history
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class Series:
    """One forecast input: a field of one series, oldest point first."""

    field: str
    tags: dict
    times: list = dataclasses.field(default_factory=list)  # int ns
    values: list = dataclasses.field(default_factory=list)  # float, NaN = gap
    covariates: list = dataclasses.field(default_factory=list)  # [(name, [float|None])]
    step_ns: int = 0
    observed: int = 0  # cells of the grid that hold data
    error: str | None = None


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _run_query(influxdb3_local, sql: str, params: dict | None, database: str | None):
    kwargs = {"database": database} if database else {}
    if params:
        return influxdb3_local.query(sql, params, **kwargs)
    return influxdb3_local.query(sql, **kwargs)


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _to_ns(value) -> int | None:
    """A query timestamp as integer nanoseconds."""
    if value is None or isinstance(value, int):
        return value
    if not isinstance(value, datetime):
        try:
            value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    # Integer arithmetic: a float timestamp is off by up to a few hundred ns.
    return (value - _EPOCH) // timedelta(microseconds=1) * 1000


def _numeric(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _too_many(cap: int) -> ForecastError:
    return ForecastError(
        "bad_request",
        f"the request produces more forecasts than max_series={cap}; "
        f"narrow it, set series_limit, or raise max_series",
    )


def _measurement_tags(influxdb3_local, req: ForecastRequest) -> list[str]:
    """Check the named columns against the schema and return the tag names."""
    try:
        schema = get_schema(
            influxdb3_local,
            req.source_measurement,
            database=req.source_database,
            use_cache=False,
        )
    except Exception as exc:
        raise ForecastError(
            "bad_request", f"cannot read measurement {req.source_measurement!r}: {exc}"
        ) from exc
    tags = [name for name, data_type in schema.items() if data_type == TAG_DATA_TYPE]
    numeric = {name for name, data_type in schema.items() if data_type in NUMERIC_TYPES}
    for name in [req.field, *req.covariate_fields]:
        if name not in numeric:
            raise ForecastError(
                "bad_request",
                f"{name!r} is not a numeric field of {req.source_measurement!r}",
            )
    for name in [*req.group_by, *req.tag_filter]:
        if name not in tags:
            raise ForecastError(
                "bad_request", f"{name!r} is not a tag of {req.source_measurement!r}"
            )
    return tags


def _where(req: ForecastRequest, params: dict) -> str:
    """The WHERE clause shared by the series and the data queries."""
    clauses = [f"{_quote_ident(req.field)} IS NOT NULL"]
    if req.window_ns:
        clauses.append(f"time > now() - INTERVAL '{req.window_ns // 10**9} seconds'")
    for i, (tag, values) in enumerate(req.tag_filter.items()):
        placeholders = []
        for j, value in enumerate(values):
            params[f"tf{i}_{j}"] = value
            placeholders.append(f"$tf{i}_{j}")
        if len(placeholders) == 1:
            clauses.append(f"{_quote_ident(tag)} = {placeholders[0]}")
        else:
            clauses.append(f"{_quote_ident(tag)} IN ({', '.join(placeholders)})")
    if req.sql_filter:
        # Inserted as is, by design: the README states what it can read.
        clauses.append(f"({req.sql_filter})")
    return " AND ".join(clauses)


def _series_keys(influxdb3_local, req, tags, where, params) -> tuple[list, int]:
    """Find the series, their single-valued tags and row counts, and enforce the cap.

    Returns:
        [(group values, tags, rows)] in the contract's order (group_by values, a missing
        one last), at most series_limit of them, and how many series there are in all.
        Without group_by there is always one series, empty when no row matches; with
        it, no row means no series.
    """
    others = [tag for tag in tags if tag not in req.group_by]
    columns = [f"{_quote_ident(g)} AS __g{i}" for i, g in enumerate(req.group_by)]
    for i, tag in enumerate(others):
        text = f"CAST({_quote_ident(tag)} AS VARCHAR)"
        columns.append(f"count({text}) AS __c{i}")
        columns.append(f"count(DISTINCT {text}) AS __n{i}")
        columns.append(f"min({text}) AS __v{i}")
    columns.append("count(*) AS __rows")
    limited = bool(req.group_by and req.series_limit)
    if limited:
        # Counts every group before LIMIT, so only series_limit rows come back.
        columns.append("count(*) OVER () AS __total")
    sql = f"SELECT {', '.join(columns)} FROM {_quote_ident(req.source_measurement)} WHERE {where}"
    if req.group_by:
        positions = ", ".join(str(i + 1) for i in range(len(req.group_by)))
        order = ", ".join(f"{i + 1} ASC NULLS LAST" for i in range(len(req.group_by)))
        limit = req.series_limit or req.max_series + 1
        sql += f" GROUP BY {positions} ORDER BY {order} LIMIT {limit}"
    try:
        rows = _run_query(influxdb3_local, sql, params, req.source_database)
    except Exception as exc:
        # The first query to run the filters, so a bad sql_filter fails here.
        raise ForecastError("bad_request", f"cannot read the series: {exc}") from exc
    rows = [row for row in rows if row.get("__rows")]
    if not req.group_by and not rows:
        return [((), {}, 0)], 1
    if len(rows) > req.max_series:
        raise _too_many(req.max_series)
    total = rows[0]["__total"] if limited and rows else len(rows)

    keys = []
    for row in rows:
        group = tuple(row.get(f"__g{i}") for i in range(len(req.group_by)))
        series_tags = {
            g: str(value) for g, value in zip(req.group_by, group) if value not in (None, "")
        }
        for i, tag in enumerate(others):
            # A series tag is on every row with one value; partial tags are left out.
            if (
                row.get(f"__c{i}") == row.get("__rows")
                and row.get(f"__n{i}") == 1
                and row.get(f"__v{i}") not in (None, "")
            ):
                series_tags[tag] = str(row[f"__v{i}"])
        keys.append((group, series_tags, row["__rows"]))
    return keys, total


def _chosen_series(req: ForecastRequest, keys: list, params: dict) -> str:
    """A condition matching only the rows of these series, the ones series_limit keeps."""
    alternatives = []
    for i, (group, _, _) in enumerate(keys):
        parts = []
        for j, (tag, value) in enumerate(zip(req.group_by, group)):
            if value is None:
                parts.append(f"{_quote_ident(tag)} IS NULL")
            else:
                params[f"sl{i}_{j}"] = value
                parts.append(f"{_quote_ident(tag)} = $sl{i}_{j}")
        alternatives.append(f"({' AND '.join(parts)})")
    return f"({' OR '.join(alternatives)})"


# Tag sets read to check one request for mixed series; more than this counts as mixed.
_MAX_TAG_SETS = 10_000


def _refuse_mixed_series(influxdb3_local, req, tags, where, params) -> None:
    """Refuse a series whose rows come from tag sets writing at the same time.

    Interleaved, they form a saw-tooth; a tag set that follows another in time
    (a version after an upgrade) is fine. Only the last point_limit timestamps of
    each series count, as the model reads no more; with window alone, all of it.
    """
    others = [tag for tag in tags if tag not in req.group_by]
    if not others:
        return
    groups = [f"__g{i}" for i in range(len(req.group_by))]
    keys = ", ".join(groups + [f"__o{i}" for i in range(len(others))])
    columns = [f"{_quote_ident(g)} AS __g{i}" for i, g in enumerate(req.group_by)]
    columns += [f"{_quote_ident(tag)} AS __o{i}" for i, tag in enumerate(others)]
    rows_sql = (
        f"SELECT {', '.join(columns)}, time AS __time "
        f"FROM {_quote_ident(req.source_measurement)} WHERE {where}"
    )
    if req.point_limit:
        partition = f"PARTITION BY {', '.join(groups)} " if groups else ""
        rows_sql = (
            f"SELECT * FROM (SELECT *, DENSE_RANK() OVER ({partition}ORDER BY __time DESC) "
            f"AS __rank FROM ({rows_sql}) AS __rows) AS __ranked "
            f"WHERE __rank <= {req.point_limit}"
        )
    sql = (
        f"SELECT {keys}, min(__time) AS __first, max(__time) AS __last "
        f"FROM ({rows_sql}) AS __recent GROUP BY {keys} LIMIT {_MAX_TAG_SETS + 1}"
    )
    try:
        rows = _run_query(influxdb3_local, sql, params, req.source_database)
    except Exception as exc:
        raise ForecastError("bad_request", f"cannot read the series: {exc}") from exc
    if len(rows) > _MAX_TAG_SETS:
        raise ForecastError(
            "bad_request",
            f"the rows come from more than {_MAX_TAG_SETS} tag sets; narrow them with "
            f"group_by or tag_filter, or set sampling_interval",
        )

    spans: dict[tuple, list] = {}  # group -> [(first, last, tag values)]
    for row in rows:
        group = tuple(row.get(f"__g{i}") for i in range(len(req.group_by)))
        tag_values = tuple(row.get(f"__o{i}") for i in range(len(others)))
        spans.setdefault(group, []).append(
            (_to_ns(row["__first"]) or 0, _to_ns(row["__last"]) or 0, tag_values)
        )
    for group, tag_sets in spans.items():
        latest = None  # the tag set that ends last so far
        for span in sorted(tag_sets, key=lambda span: span[0]):
            if latest and span[0] < latest[1]:
                differ = [tag for tag, a, b in zip(others, span[2], latest[2]) if a != b]
                series = f"the series {dict(zip(req.group_by, group))}" if req.group_by else "the series"
                raise ForecastError(
                    "bad_request",
                    f"rows of {series} come from several tag sets writing at the same time "
                    f"(they differ in {', '.join(differ)}); add those tags to group_by, pin "
                    f"them with tag_filter, or set sampling_interval to forecast their mean",
                )
            if latest is None or span[1] > latest[1]:
                latest = span


def _data_sql(req: ForecastRequest, where: str, limit: int) -> tuple[str, bool]:
    """The history query, at most limit points per series; the flag says rows come newest first.

    Rows of a series sharing a timestamp, or a sampling bucket, are averaged.
    """
    fields = [req.field, *req.covariate_fields]
    groups = [f"{_quote_ident(g)} AS __g{i}" for i, g in enumerate(req.group_by)]
    table = _quote_ident(req.source_measurement)
    moment = "time"
    if req.sampling_ns:
        moment = f"date_bin(INTERVAL '{req.sampling_ns // 10**9} seconds', time)"
    values = [f"avg({_quote_ident(f)}) AS __f{i}" for i, f in enumerate(fields)]
    select = [f"{moment} AS __t", *groups, *values]
    positions = ", ".join(str(i + 1) for i in range(1 + len(groups)))
    sql = f"SELECT {', '.join(select)} FROM {table} WHERE {where} GROUP BY {positions}"

    if not req.group_by:
        return f"{sql} ORDER BY __t DESC LIMIT {limit}", True
    partition = ", ".join(f"__g{i}" for i in range(len(req.group_by)))
    ranked = (
        f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY __t DESC) "
        f"AS __rn FROM ({sql}) AS __history"
    )
    return (
        f"SELECT * FROM ({ranked}) AS __ranked WHERE __rn <= {limit} ORDER BY __t",
        False,
    )


def _measurement_plan(influxdb3_local, req: ForecastRequest) -> tuple:
    """Mode A checks that need no model: the schema, the filters, the series cap, mixed series.

    Returns:
        (WHERE clause, its parameters, series keys from _series_keys, series in all).
    """
    tags = _measurement_tags(influxdb3_local, req)
    params: dict = {}
    where = _where(req, params)
    keys, total = _series_keys(influxdb3_local, req, tags, where, params)
    if len(keys) < total:
        # series_limit left series out: they are neither checked nor read.
        where = f"{where} AND {_chosen_series(req, keys, params)}"
    if not req.sampling_ns and any(rows for _, _, rows in keys):
        # With sampling_interval, a mixed series is the mean per bucket the request asks for.
        _refuse_mixed_series(influxdb3_local, req, tags, where, params)
    return where, params, keys, total


def _read_measurement(influxdb3_local, req: ForecastRequest, plan: tuple, limit: int) -> list[Series]:
    """Mode A: one series per group_by combination, or one in all."""
    where, params, keys, _ = plan
    sql, newest_first = _data_sql(req, where, limit)
    try:
        rows = _run_query(influxdb3_local, sql, params, req.source_database)
    except Exception as exc:
        raise ForecastError("internal", f"history query failed: {exc}") from exc
    if newest_first:
        rows = list(reversed(rows))

    by_key = {group: Series(field=req.field, tags=series_tags) for group, series_tags, _ in keys}
    covariate_values = {group: [[] for _ in req.covariate_fields] for group in by_key}
    for row in rows:
        group = tuple(row.get(f"__g{i}") for i in range(len(req.group_by)))
        series = by_key.get(group)
        value = row.get("__f0")
        moment = _to_ns(row.get("__t"))
        if series is None or value is None or moment is None:
            continue
        series.times.append(moment)
        series.values.append(float(value))
        for j, column in enumerate(covariate_values[group]):
            raw = row.get(f"__f{j + 1}")
            column.append(float(raw) if raw is not None else None)

    for group, series in by_key.items():
        series.covariates = list(zip(req.covariate_fields, covariate_values[group]))
    return list(by_key.values())


# EXPLAIN names each scanned table unquoted, then its attributes:
# "TableScan: cpu load projection=[...], full_filters=[...]".
_TABLE_SCAN = re.compile(
    r"TableScan: (.+?)(?= projection=\[|, (?:partial_filters|full_filters|unsupported_filters|fetch)=|$)",
    re.MULTILINE,
)


def _query_measurements(influxdb3_local, req: ForecastRequest) -> list[str]:
    """The measurements a query reads, from its logical plan."""
    try:
        rows = _run_query(influxdb3_local, f"EXPLAIN {req.query}", None, req.source_database)
    except Exception as exc:
        raise ForecastError("bad_request", f"the query does not plan: {exc}") from exc
    names = set()
    for row in rows:
        if row.get("plan_type") != "logical_plan":
            continue
        for name in _TABLE_SCAN.findall(str(row.get("plan", ""))):
            for prefix in ("public.iox.", "iox."):
                if name.startswith(prefix):
                    name = name[len(prefix):]
                    break
            names.add(name)
    return sorted(names)


def _windowed_query(query: str, window_ns: int) -> str:
    """The query wrapped in a window filter the server applies.

    UTC compares a time zone aware time (tz()) by instant; the line breaks keep a
    trailing -- comment from swallowing the closing parenthesis.
    """
    body = query.strip().rstrip(";").rstrip()
    return (
        f"SELECT * FROM (\n{body}\n) AS __history WHERE time > "
        f"(now() AT TIME ZONE 'UTC') - INTERVAL '{window_ns // 10**9} seconds'"
    )


def _query_rows(influxdb3_local, req: ForecastRequest, task_id: str) -> list:
    """The query's rows; with window, filtered by the server when the query can be wrapped."""
    if req.window_ns:
        try:
            return _run_query(
                influxdb3_local,
                _windowed_query(req.query, req.window_ns),
                None,
                req.source_database,
            )
        except Exception as exc:
            if "Resources exhausted" in str(exc):
                # Unwrapped, the query would read more and fail the same way.
                raise ForecastError("internal", f"the query failed: {exc}") from exc
            # For example an integer time column; _read_query applies the window then.
            influxdb3_local.warn(
                f"[{task_id}] Window applied in the plugin, the wrapped query failed: {exc}"
            )
    try:
        return _run_query(influxdb3_local, req.query, None, req.source_database)
    except Exception as exc:
        raise ForecastError("internal", f"the query failed: {exc}") from exc


def _read_query(influxdb3_local, req: ForecastRequest, task_id: str) -> tuple[list[Series], int]:
    """Mode B: each key combination × each numeric column is a series.

    Returns:
        The series in the contract's order, at most series_limit of them, and how many
        there are in all.
    """
    rows = _query_rows(influxdb3_local, req, task_id)
    if rows and not any("time" in row for row in rows):
        raise ForecastError(
            "bad_request", "no time column in query result; alias the timestamp AS time"
        )
    if req.window_ns:
        # Repeats the server's filter, or applies it when the query could not be wrapped;
        # before the split, so a series with no rows in the window does not exist.
        cutoff = _time.time_ns() - req.window_ns
        rows = [row for row in rows if (_to_ns(row.get("time")) or 0) > cutoff]
    if not rows:
        return [], 0
    columns = list(dict.fromkeys(column for row in rows for column in row))

    keys, values = [], []
    for column in columns:
        if column == "time":
            continue
        present = [row.get(column) for row in rows if row.get(column) is not None]
        if not present:
            continue
        (values if all(_numeric(v) for v in present) else keys).append(column)
    if not values:
        raise ForecastError("bad_request", "no numeric column to forecast in query result")
    if req.write_results:
        _refuse_clashing_keys(keys, req.levels, "query column")

    groups: dict[tuple, list] = {}
    for row in rows:
        groups.setdefault(tuple(row.get(k) for k in keys), []).append(row)
    # The contract's order: key values as text, a missing value last, then field name.
    forecasts = sorted(
        ((group, column) for group in groups for column in values),
        key=lambda pair: (tuple((v is None, str(v)) for v in pair[0]), pair[1]),
    )
    total = len(forecasts)
    if req.series_limit:
        forecasts = forecasts[: req.series_limit]
    elif total > req.max_series:
        raise _too_many(req.max_series)

    series_list = []
    for group, column in forecasts:
        series_tags = {k: str(v) for k, v in zip(keys, group) if v not in (None, "")}
        points = sorted(
            (moment, float(row[column]))
            for row in groups[group]
            if row.get(column) is not None
            and (moment := _to_ns(row.get("time"))) is not None
        )
        series = Series(field=column, tags=series_tags)
        series.times = [moment for moment, _ in points]
        series.values = [value for _, value in points]
        if req.sampling_ns:
            _bucket(series, req.sampling_ns)
        series_list.append(series)
    return series_list, total


def _bucket(series: Series, step_ns: int) -> None:
    """Mean per bucket of step_ns, aligned to the epoch like date_bin."""
    sums: dict[int, list] = {}
    for moment, value in zip(series.times, series.values):
        bucket = moment - moment % step_ns
        total = sums.setdefault(bucket, [0.0, 0])
        total[0] += value
        total[1] += 1
    series.times = sorted(sums)
    series.values = [sums[t][0] / sums[t][1] for t in series.times]


def _regularize(series: Series, req: ForecastRequest, limit: int) -> None:
    """Lay the series on a regular grid, as Chronos reads points as equally spaced.

    With sampling_interval the cells are its buckets, without the one still filling;
    raw points go to the nearest cell of a grid at their spacing (_grid_anchor).
    Points in one cell are averaged, an empty cell is a gap (NaN), and the last limit
    cells are kept.
    """
    step = req.sampling_ns
    if step and series.times and series.times[-1] + step > _time.time_ns():
        series.times, series.values = series.times[:-1], series.values[:-1]
        series.covariates = [(name, column[:-1]) for name, column in series.covariates]
    if not series.times:
        series.step_ns = step or FALLBACK_STEP_NS
        return
    cells: dict[int, list] = {}  # cell time -> indices of its points
    if step:
        cells = {moment: [i] for i, moment in enumerate(series.times)}
    else:
        step = _round_step(_median_step_ns(series.times))
        anchor = _grid_anchor(series.times, step)
        for i, moment in enumerate(series.times):
            cell = anchor - (anchor - moment + step // 2) // step * step
            cells.setdefault(cell, []).append(i)
    last = max(cells)
    # Only the last limit cells: sparse data on a fine grid stays bounded.
    grid = range(max(min(cells), last - (limit - 1) * step), last + step, step)
    series.values = [_mean(series.values, cells.get(t, ())) for t in grid]
    series.covariates = [
        (name, [_mean(column, cells.get(t, ())) for t in grid])
        for name, column in series.covariates
    ]
    series.times = list(grid)
    series.step_ns = step


def _mean(column: list, indices) -> float:
    """The mean of a column's values at indices; NaN, a gap, when none is there."""
    present = [column[i] for i in indices if column[i] is not None and not math.isnan(column[i])]
    return sum(present) / len(present) if present else math.nan


def _median_step_ns(times: list) -> int:
    """The median gap between consecutive distinct timestamps."""
    gaps = sorted(b - a for a, b in zip(times, times[1:]) if b > a)
    if not gaps:
        return FALLBACK_STEP_NS
    middle = len(gaps) // 2
    return gaps[middle] if len(gaps) % 2 else (gaps[middle - 1] + gaps[middle]) // 2


# Round units for a raw-point grid: its step is rounded to one about 1% of it,
# its phase to one about 10% (10 ms and 1 s for a 10 s step, 1 h for a week).
_ROUND_UNITS_NS = tuple(
    n * 10**6 for n in (1, 10, 100, 1_000, 10_000, 60_000, 600_000, 3_600_000)
)


def _round_unit(step: int, share: int) -> int:
    """The largest round unit at most step / share, or 1 ns."""
    return max((unit for unit in _ROUND_UNITS_NS if share * unit <= step), default=1)


def _round_step(gap_ns: int) -> int:
    """The median gap rounded to a unit about 1% of it, so jitter does not change it."""
    unit = _round_unit(gap_ns, 100)
    return (gap_ns + unit // 2) // unit * unit


def _grid_anchor(times: list, step: int) -> int:
    """A cell of the raw-point grid, on the rhythm most points keep.

    The last point moved by the median offset of all points, so a stray timestamp
    does not shift the grid; its phase is rounded to about a tenth of the step, so
    jitter does not move the grid between runs.
    """
    last = times[-1]
    half = step // 2
    offsets = sorted((moment - last + half) % step - half for moment in times)
    anchor = last + offsets[len(offsets) // 2]
    unit = _round_unit(step, 10)
    phase = anchor % step
    return anchor - phase + (phase + unit // 2) // unit * unit


# ---------------------------------------------------------------------------
# Inference
# ---------------------------------------------------------------------------


def _level_key(level: float) -> str:
    """0.8 -> '80', 0.975 -> '97.5'."""
    return format((Decimal(str(level)) * 100).normalize(), "f")


def _quantile_plan(pipeline, levels: list) -> tuple[list, dict]:
    """The quantiles to request and the bounds of each level.

    A level outside the quantiles the model was trained on is left out, not
    extrapolated; Chronos-T5 samples, so any level passes.
    """
    trained = getattr(pipeline, "quantiles", None)
    trained = [float(q) for q in trained] if trained is not None else None
    bounds = {}
    for level in levels:
        # Rounded so 0.8 asks for 0.1 rather than 0.09999999999999998.
        low, high = round((1 - level) / 2, 6), round((1 + level) / 2, 6)
        if trained and (low < min(trained) - 1e-9 or high > max(trained) + 1e-9):
            continue
        bounds[level] = (low, high)
    quantiles = sorted({0.5, *(q for pair in bounds.values() for q in pair)})
    return quantiles, bounds


def _model_input(series: Series, req: ForecastRequest):
    """The model's input for a series: its values, with its covariates for Chronos-2."""
    target = torch.tensor(series.values, dtype=torch.float32)
    if not req.covariate_fields:
        return target
    # A gap stays NaN, which Chronos-2 masks; an all-missing covariate is kept, as
    # Chronos-2 needs the same covariates in every input.
    covariates = {
        name: torch.tensor(column, dtype=torch.float32) for name, column in series.covariates
    }
    return {"target": target, "past_covariates": covariates}


def _predict(pipeline, inputs: list, horizon: int, quantiles: list) -> tuple[list, float]:
    """One predict_quantiles call for every series.

    Returns:
        Per input, a [horizon][len(quantiles)] list, and the milliseconds spent.
    """
    with _inference_lock(pipeline):
        t0 = _time.time()
        output, _ = pipeline.predict_quantiles(
            inputs=inputs, prediction_length=horizon, quantile_levels=quantiles
        )
        inference_ms = (_time.time() - t0) * 1000
    if isinstance(output, list):
        # Chronos-2: one (variates, horizon, quantiles) tensor per input; the
        # forecast field is the first variate.
        matrices = [tensor[0] if tensor.dim() == 3 else tensor for tensor in output]
    else:
        matrices = [output[i] for i in range(output.shape[0])]
    # Quantile heads can cross; sorted per step, a lower bound never exceeds the forecast.
    return [matrix.sort(dim=-1).values.tolist() for matrix in matrices], inference_ms


def _finite(value) -> float | None:
    return value if value is not None and math.isfinite(value) else None


# ---------------------------------------------------------------------------
# A forecast run
# ---------------------------------------------------------------------------


def _split_backtest(series: Series, limit: int, held_out: int) -> tuple[Series, list]:
    """Hold out the last cells of a series for the backtest, and keep limit cells for the forecast.

    Returns:
        The backtest's input, the limit cells before the held-out ones, and the
        held-out values (NaN for a gap).
    """
    cut = slice(-limit - held_out, -held_out)
    check = Series(
        field=series.field,
        tags=series.tags,
        times=series.times[cut],
        values=series.values[cut],
        covariates=[(name, column[cut]) for name, column in series.covariates],
        step_ns=series.step_ns,
    )
    check.observed = sum(1 for value in check.values if not math.isnan(value))
    actual = series.values[-held_out:]
    series.times, series.values = series.times[-limit:], series.values[-limit:]
    series.covariates = [(name, column[-limit:]) for name, column in series.covariates]
    return check, actual


def _backtest_result(matrix: list, actual: list, level_columns: list, median: int) -> dict:
    """The backtest forecast against the held-out values: the share inside each level, and the MAE."""
    pairs = [(row, value) for row, value in zip(matrix, actual) if not math.isnan(value)]
    coverage = {
        key: sum(1 for row, value in pairs if row[indices[0]] <= value <= row[indices[1]])
        / len(pairs)
        if indices
        else None
        for key, _, _, indices in level_columns
    }
    mae = sum(abs(row[median] - value) for row, value in pairs) / len(pairs)
    return {"points": len(pairs), "coverage": coverage, "mae": _finite(mae)}


def _forecast_lines(req, series, points, source, run_time, points_read, row_columns):
    """The stored rows of one forecast, one per point."""
    target = req.target_measurement or f"_forecasts.{source}"
    last = series.times[-1]
    lines = []
    for step, point in enumerate(points, start=1):
        line = LineBuilder(target).tag("field", series.field).tag("model", req.model)
        if source:
            line = line.tag("source_measurement", source)
        if req.forecast_id:
            line = line.tag("forecast_id", req.forecast_id)
        for tag, value in series.tags.items():
            # A series tag named like a row column is left out; such series keys were refused.
            if tag not in row_columns and value:
                line = line.tag(tag, value)
        if point["forecast"] is not None:
            line = line.float64_field("forecast", point["forecast"])
        for key, value in point.items():
            if key.startswith(("lower_", "upper_")) and value is not None:
                line = line.float64_field(key, value)
        line = (
            line.int64_field("step", step)
            .int64_field("points_read", points_read)
            .string_field("run_time", run_time)
            .time_ns(last + step * series.step_ns)
        )
        lines.append(line)
    return lines


def _write(influxdb3_local, req: ForecastRequest, lines: list, task_id: str) -> None:
    if not lines:
        return
    try:
        write_data(influxdb3_local, lines, no_sync=True, database=req.target_database, retries=0)
    except Exception as exc:
        raise ForecastError("internal", f"forecast computed but the write failed: {exc}") from exc
    influxdb3_local.info(f"[{task_id}] Wrote {len(lines)} forecast rows")


def _history_limit(pipeline, req: ForecastRequest) -> int:
    """Points read per series: point_limit, never more than the model's context."""
    context = getattr(pipeline, "model_context_length", None) or FALLBACK_CONTEXT
    return min(req.point_limit, context) if req.point_limit else context


def _pipeline_for(influxdb3_local, req: ForecastRequest, task_id: str):
    """The loaded model, checked for the request's covariates."""
    try:
        pipeline = _load_model(req.model, influxdb3_local, task_id)
    except Exception as exc:
        raise ForecastError(
            "model_unavailable", f"cannot load model {req.model!r}: {exc}"
        ) from exc
    # Only Chronos-2 takes covariates; the loaded class tells, whatever the model's name.
    if req.covariate_fields and not isinstance(pipeline, Chronos2Pipeline):
        raise ForecastError(
            "bad_request", f"covariate_fields requires a Chronos-2 model, got {req.model!r}"
        )
    return pipeline


def _too_few_message(req: ForecastRequest, observed: int) -> str:
    message = f"need >= {MIN_POINTS} points, got {observed}"
    if req.window_ns:
        message += f" in the last {_duration_text(req.window_ns)}"
        if req.default_window and req.sampling_ns:
            message += "; set window, raise point_limit or shorten sampling_interval"
        elif req.default_window:
            message += "; set window or sampling_interval"
    return message


def _run_forecast(influxdb3_local, req: ForecastRequest, task_id: str) -> dict:
    """Read, forecast, optionally write, and return the response of §4."""
    response = {
        "status": "ok",
        "model": req.model,
        "levels": req.levels,
        "series_total": 0,
        "results": [],
    }
    # Checks, and reading a query, need no model and come first, so a bad request loads nothing.
    plan = None
    if req.query:
        # EXPLAIN also without a write: a query that does not plan is a bad request,
        # while one that fails while running is an internal error.
        sources = _query_measurements(influxdb3_local, req)
        source = sources[0] if len(sources) == 1 else None
        if req.write_results and source is None and not req.target_measurement:
            raise ForecastError(
                "bad_request",
                f"the query reads {len(sources)} measurements {sources}; set "
                f"target_measurement",
            )
        series_list, total = _read_query(influxdb3_local, req, task_id)
        kept = len(series_list)
        enough = any(len(series.times) >= MIN_POINTS for series in series_list)
    else:
        source = req.source_measurement
        plan = _measurement_plan(influxdb3_local, req)
        kept, total = len(plan[2]), plan[3]
        enough = any(rows >= MIN_POINTS for _, _, rows in plan[2])
    response["series_total"] = total
    if not kept:
        influxdb3_local.info(f"[{task_id}] No history matches; nothing to forecast")
        return response
    if kept < total:
        influxdb3_local.info(
            f"[{task_id}] series_limit={req.series_limit} keeps {kept} of {total} forecasts"
        )

    # The model loads only when some series can be forecast; the row counts above
    # usually tell, else the read history does (rows may arrive between the two).
    pipeline = _pipeline_for(influxdb3_local, req, task_id) if enough else None
    limit = _history_limit(pipeline, req) if pipeline else req.point_limit or FALLBACK_CONTEXT
    held_out = req.backtest_points or 0
    if not req.query:
        # At most limit points per series, plus the backtest's held-out ones; with
        # sampling_interval one bucket more, as the one still filling is dropped.
        read = limit + held_out + (1 if req.sampling_ns else 0)
        series_list = _read_measurement(influxdb3_local, req, plan, read)

    for series in series_list:
        _regularize(series, req, limit + held_out)
    if pipeline is None and any(
        sum(1 for value in series.values if not math.isnan(value)) >= MIN_POINTS
        for series in series_list
    ):
        pipeline = _pipeline_for(influxdb3_local, req, task_id)
        limit = _history_limit(pipeline, req)
        for series in series_list:
            # Keep what the model reads: its context may be shorter than the fallback.
            _regularize(series, req, limit + held_out)

    ready, checks = [], {}
    for series in series_list:
        check = _split_backtest(series, limit, held_out) if held_out else None
        series.observed = sum(1 for value in series.values if not math.isnan(value))
        if series.observed < MIN_POINTS:
            series.error = _too_few_message(req, series.observed)
            continue
        ready.append(series)
        # The backtest needs MIN_POINTS before the cut and a measured point after it.
        if check and check[0].observed >= MIN_POINTS and any(not math.isnan(v) for v in check[1]):
            checks[id(series)] = check
        for name, column in series.covariates:
            if all(math.isnan(value) for value in column):
                influxdb3_local.warn(
                    f"[{task_id}] Covariate {name!r} has no data for {series.tags}; "
                    f"passed as missing"
                )

    outputs, backtests = {}, {}
    if ready:
        quantiles, bounds = _quantile_plan(pipeline, req.levels)
        horizons = {id(series): _horizon_points(req, series.step_ns) for series in ready}
        longest = max(horizons.values())
        _check_horizon(req, longest)
        checked = [series for series in ready if id(series) in checks]
        length = max(longest, held_out) if checked else longest
        native = getattr(pipeline, "model_prediction_length", None)
        if native and length > native:
            influxdb3_local.warn(
                f"[{task_id}] {length} forecast points are past the model's native "
                f"{native}; they are extended autoregressively, at lower quality"
            )
        # One model call: the forecasts first, then the backtests.
        inputs = [_model_input(series, req) for series in ready]
        inputs += [_model_input(checks[id(series)][0], req) for series in checked]
        try:
            matrices, inference_ms = _predict(pipeline, inputs, length, quantiles)
        except Exception as exc:
            raise ForecastError("internal", f"inference failed: {exc}") from exc
        influxdb3_local.info(
            f"[{task_id}] Forecast {len(ready)} series"
            f"{f', {len(checked)} with a backtest,' if checked else ''} in {inference_ms:.0f}ms"
        )
        median = quantiles.index(0.5)
        # Per level, once per request: its keys and the indices of its bounds.
        level_columns = []
        for level in req.levels:
            key, pair = _level_key(level), bounds.get(level)
            indices = (quantiles.index(pair[0]), quantiles.index(pair[1])) if pair else None
            level_columns.append((key, f"lower_{key}", f"upper_{key}", indices))
        now_ns = _time.time_ns()
        for series, matrix in zip(ready, matrices):
            last = series.times[-1]
            if last + horizons[id(series)] * series.step_ns < now_ns:
                influxdb3_local.warn(
                    f"[{task_id}] {series.field} {series.tags}: the history ends at "
                    f"{datetime.fromtimestamp(last // 10**9, timezone.utc):%Y-%m-%dT%H:%M:%SZ}, "
                    f"so the whole forecast lies in the past"
                )
            points = []
            for step in range(horizons[id(series)]):
                row = matrix[step]
                point = {
                    "timestamp": (last + (step + 1) * series.step_ns) // 10**6,
                    "forecast": _finite(row[median]),
                }
                for _, lower, upper, indices in level_columns:
                    point[lower] = _finite(row[indices[0]]) if indices else None
                    point[upper] = _finite(row[indices[1]]) if indices else None
                points.append(point)
            outputs[id(series)] = points
        for series, matrix in zip(checked, matrices[len(ready):]):
            backtests[id(series)] = _backtest_result(
                matrix, checks[id(series)][1], level_columns, median
            )

    run_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    row_columns = _forecast_row_columns(req.levels)
    lines = []
    for series in series_list:
        result = {"field": series.field, "tags": series.tags}
        if id(series) not in outputs:
            result.update(
                {"status": "error", "code": "too_few_points", "message": series.error}
            )
            response["results"].append(result)
            continue
        points = outputs[id(series)]
        result.update(
            {
                "points_read": series.observed,
                "step_ms": series.step_ns // 10**6,
                "forecast": points,
            }
        )
        if req.backtest_points:
            # None when the series is too short for a backtest.
            result["backtest"] = backtests.get(id(series))
        response["results"].append(result)
        if req.write_results:
            try:
                lines.extend(
                    _forecast_lines(
                        req, series, points, source, run_time, series.observed, row_columns
                    )
                )
            except Exception as exc:
                # LineBuilder refuses some names, e.g. one with a space.
                raise ForecastError(
                    "internal", f"forecast computed but its rows cannot be built: {exc}"
                ) from exc

    if req.write_results:
        _write(influxdb3_local, req, lines, task_id)
    return response


def _error_body(exc: ForecastError) -> dict:
    return {"status": "error", "code": exc.code, "message": exc.message}


# ===========================================================================
# Entry point: HTTP trigger
# ===========================================================================


def process_request(
    influxdb3_local, query_parameters, request_headers, request_body, args=None
):
    """On-demand forecast of a measurement field or of a query result.

    Args:
        influxdb3_local: Processing engine API handle for queries, writes, and logging.
        query_parameters: URL query parameters; any request key may be set here.
        request_headers: HTTP headers; a key is also read from
            X-Influxdb3-Chronos-Forecasting-<key>.
        request_body: JSON object with the request keys.
        args: Trigger arguments, used as defaults for the request.

    Returns:
        The response body and its HTTP status: 200 with the forecast, or the
        status of the error code.
    """
    task_id = str(uuid.uuid4())[:8]
    try:
        _refuse_unknown_headers(request_headers)
        try:
            cfg = _load_settings(
                args,
                SETTING_NAMES,
                (
                    parse_json_body(request_body, REQUEST_KEYS),
                    parse_request_headers(request_headers, HEADER_KEYS),
                    parse_query_parameters(query_parameters, REQUEST_KEYS),
                ),
            )
        except ValueError as exc:
            raise ForecastError("bad_request", str(exc)) from exc
        req = _resolve(cfg, scheduled=False)
        influxdb3_local.info(
            f"[{task_id}] Forecast request: "
            f"{'query' if req.query else f'{req.source_measurement}.{req.field}'}, "
            f"model={req.model}"
        )
        return _run_forecast(influxdb3_local, req, task_id), 200
    except ForecastError as exc:
        log = influxdb3_local.warn if exc.code == "bad_request" else influxdb3_local.error
        log(f"[{task_id}] {exc.code}: {exc.message}")
        return _error_body(exc), HTTP_STATUS[exc.code]
    except Exception as exc:
        influxdb3_local.error(f"[{task_id}] internal: {exc}")
        return {"status": "error", "code": "internal", "message": str(exc)}, 500


# ===========================================================================
# Entry point: Scheduled trigger
# ===========================================================================


def process_scheduled_call(
    influxdb3_local, call_time: datetime, args: dict | None = None
):
    """Scheduled forecast, written to target_measurement on every run.

    Args:
        influxdb3_local: Processing engine API handle for queries, writes, and logging.
        call_time: When the trigger fired.
        args: Trigger arguments; config_file_path points at a TOML file whose
            keys override them.

    Returns:
        None. Errors go to the plugin log with their code.
    """
    task_id = str(uuid.uuid4())[:8]
    try:
        try:
            cfg = _load_settings(args, SCHEDULED_NAMES)
        except ValueError as exc:
            raise ForecastError("bad_request", str(exc)) from exc
        req = _resolve(cfg, scheduled=True)
        response = _run_forecast(influxdb3_local, req, task_id)
        for result in response["results"]:
            if result.get("status") == "error":
                influxdb3_local.warn(
                    f"[{task_id}] {result['code']} for {result['field']} "
                    f"{result['tags']}: {result['message']}"
                )
    except ForecastError as exc:
        influxdb3_local.error(f"[{task_id}] {exc.code}: {exc.message}")
    except Exception as exc:
        influxdb3_local.error(f"[{task_id}] internal: {exc}")
    return None
