"""
{
    "plugin_type": ["scheduled", "http"],
    "scheduled_args_config": [
        {
            "name": "measurement",
            "example": "temperature",
            "description": "The InfluxDB measurement to query for historical data.",
            "required": true
        },
        {
            "name": "field",
            "example": "value",
            "description": "The field name within the measurement to forecast.",
            "required": true
        },
        {
            "name": "window",
            "example": "30d",
            "description": "Historical window duration for training data. Format: <number><unit> where unit is us, ms, s, min, h, d, w, m (30.42d), q (91.25d), y (365d).",
            "required": true
        },
        {
            "name": "forecast_horizont",
            "example": "2d",
            "description": "Future duration to forecast. Format: <number><unit> where unit is us, ms, s, min, h, d, w, m, q, y.",
            "required": true
        },
        {
            "name": "tag_values",
            "example": "region:us-west.device:sensor1",
            "description": "Tag filters for the source query, as dot-separated 'tag:value' pairs. A TOML table such as { region = 'us-west' } is also accepted.",
            "required": true
        },
        {
            "name": "target_measurement",
            "example": "temperature_forecast",
            "description": "Destination measurement for storing forecast results.",
            "required": true
        },
        {
            "name": "model_mode",
            "example": "train",
            "description": "Mode of operation: 'train' to train a new in-memory model on every run, 'predict' to load the saved model or train and save it when no file exists.",
            "required": true
        },
        {
            "name": "unique_suffix",
            "example": "20250619_v1",
            "description": "Model version identifier, also used as the file name suffix. Up to 64 characters from letters, digits, '.', '_' and '-'.",
            "required": true
        },
        {
            "name": "seasonality_mode",
            "example": "additive",
            "description": "Prophet seasonality mode ('additive' or 'multiplicative'). Defaults to 'additive'.",
            "required": false
        },
        {
            "name": "changepoint_prior_scale",
            "example": "0.05",
            "description": "Flexibility of trend changepoints, must be greater than 0. Defaults to 0.05.",
            "required": false
        },
        {
            "name": "changepoints",
            "example": "2025-01-01 2025-06-01",
            "description": "Space-separated list of changepoint dates (ISO format). A TOML or JSON list is also accepted.",
            "required": false
        },
        {
            "name": "holiday_date_list",
            "example": "2025-01-01 2025-12-25",
            "description": "Space-separated list of custom holiday dates (ISO format). A TOML or JSON list is also accepted.",
            "required": false
        },
        {
            "name": "holiday_names",
            "example": "New Year.Christmas",
            "description": "Dot-separated list of names matching holiday_date_list. A TOML or JSON list is also accepted.",
            "required": false
        },
        {
            "name": "holiday_country_names",
            "example": "US",
            "description": "Country code for built-in holidays, as a dot-separated string or a TOML list. Prophet supports one country, so only the first entry is used.",
            "required": false
        },
        {
            "name": "inferred_freq",
            "example": "1D",
            "description": "Manually specified pandas frequency alias, fixed ('30min', '1h', '1s') or calendar ('D', 'W-SUN', 'MS', 'QS'). If not provided, frequency is inferred from data.",
            "required": false
        },
        {
            "name": "validation_window",
            "example": "3d",
            "description": "Duration held back from training and used to validate the forecast. Defaults to '0s' (no validation).",
            "required": false
        },
        {
            "name": "validation_alignment",
            "example": "nearest",
            "description": "How actual and forecasted values are paired during validation: 'position' (default) pairs them in time order, 'nearest' pairs each actual value with the closest forecast point within half a frequency step.",
            "required": false
        },
        {
            "name": "msre_threshold",
            "example": "0.05",
            "description": "Maximum acceptable Mean Squared Relative Error (MSRE) for validation. Defaults to infinity (no threshold).",
            "required": false
        },
        {
            "name": "max_forecast_points",
            "example": "10000",
            "description": "Maximum number of forecast points per run, counting the validation window. Defaults to 10000.",
            "required": false
        },
        {
            "name": "target_database",
            "example": "forecast_db",
            "description": "Database for forecast results. Defaults to a database named 'default', which is created on first write.",
            "required": false
        },
        {
            "name": "is_sending_alert",
            "example": "true",
            "description": "Whether to send alerts on validation failure ('true' or 'false'). Defaults to 'false'.",
            "required": false
        },
        {
            "name": "notification_text",
            "example": "Validation failed for prophet model:$version on table:$measurement, field:$field for period from $start_time to $end_time, forecast not written to table:$output_measurement",
            "description": "Templated text for the alert message. Supported variables: $version, $measurement, $field, $start_time, $end_time, $output_measurement.",
            "required": false
        },
        {
            "name": "senders",
            "example": "slack",
            "description": "Dot-separated list of sender types (e.g., 'slack.sms'). Required when is_sending_alert is true.",
            "required": false
        },
        {
            "name": "notification_path",
            "example": "notify",
            "description": "URL path of the notification sender plugin. Defaults to 'notify'.",
            "required": false
        },
        {
            "name": "influxdb3_auth_token",
            "example": "your_token",
            "description": "Authentication token for sending notifications. If not provided, uses the INFLUXDB3_AUTH_TOKEN environment variable.",
            "required": false
        },
        {
            "name": "port_override",
            "example": "8182",
            "description": "Custom port for notification dispatch (1-65535). Defaults to 8181.",
            "required": false
        },
        {
            "name": "slack_webhook_url",
            "example": "https://hooks.slack.com/...",
            "description": "Webhook URL for Slack notifications. Required if using slack sender.",
            "required": false
        },
        {
            "name": "slack_headers",
            "example": "eyJDb250ZW50LVR5cGUiOiAiYXBwbGljYXRpb24vanNvbiJ9",
            "description": "Optional base64-encoded headers for the Slack webhook.",
            "required": false
        },
        {
            "name": "discord_webhook_url",
            "example": "https://discord.com/api/webhooks/...",
            "description": "Webhook URL for Discord notifications. Required if using discord sender.",
            "required": false
        },
        {
            "name": "discord_headers",
            "example": "eyJDb250ZW50LVR5cGUiOiAiYXBwbGljYXRpb24vanNvbiJ9",
            "description": "Optional base64-encoded headers for the Discord webhook.",
            "required": false
        },
        {
            "name": "http_webhook_url",
            "example": "https://example.com/webhook",
            "description": "Webhook URL for HTTP POST notifications. Required if using http sender.",
            "required": false
        },
        {
            "name": "http_headers",
            "example": "eyJDb250ZW50LVR5cGUiOiAiYXBwbGljYXRpb24vanNvbiJ9",
            "description": "Optional base64-encoded headers for the HTTP webhook.",
            "required": false
        },
        {
            "name": "twilio_sid",
            "example": "ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "description": "Twilio Account SID. Required if using sms or whatsapp sender.",
            "required": false
        },
        {
            "name": "twilio_token",
            "example": "your_auth_token",
            "description": "Twilio Auth Token. Required if using sms or whatsapp sender.",
            "required": false
        },
        {
            "name": "twilio_to_number",
            "example": "+1234567890",
            "description": "Recipient phone number. Required if using sms or whatsapp sender.",
            "required": false
        },
        {
            "name": "twilio_from_number",
            "example": "+19876543210",
            "description": "Twilio sender phone number (verified). Required if using sms or whatsapp sender.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "prophet_forecasting_scheduler.toml",
            "description": "Path to a TOML file supplying all parameters, relative to PLUGIN_DIR. When set, the file replaces the inline trigger arguments.",
            "required": false
        }
    ],
    "http_body_config": [
        {
            "name": "measurement",
            "example": "temperature",
            "description": "The InfluxDB measurement to query for historical data.",
            "required": true
        },
        {
            "name": "field",
            "example": "value",
            "description": "The field name within the measurement to forecast.",
            "required": true
        },
        {
            "name": "forecast_horizont",
            "example": "7d",
            "description": "Future duration to forecast. Format: <number><unit> where unit is us, ms, s, min, h, d, w, m, q, y.",
            "required": true
        },
        {
            "name": "tag_values",
            "example": "{'region': 'us-west', 'device': 'sensor1'}",
            "description": "Tag filters for the source query, as a JSON object. The dot-separated 'tag:value' string form is also accepted.",
            "required": true
        },
        {
            "name": "target_measurement",
            "example": "temperature_forecast",
            "description": "Destination measurement for storing forecast results.",
            "required": true
        },
        {
            "name": "unique_suffix",
            "example": "20250619_v1",
            "description": "Model version identifier, also used as the file name suffix. Up to 64 characters from letters, digits, '.', '_' and '-'.",
            "required": true
        },
        {
            "name": "start_time",
            "example": "2025-05-20T00:00:00Z",
            "description": "Start of the historical window, ISO 8601 with timezone.",
            "required": true
        },
        {
            "name": "end_time",
            "example": "2025-06-19T00:00:00Z",
            "description": "End of the historical window, ISO 8601 with timezone. Forecast points are written from this moment onward.",
            "required": true
        },
        {
            "name": "save_mode",
            "example": "true",
            "description": "When true, load the saved model for unique_suffix or train and save it when no file exists. Defaults to false, which trains an in-memory model per request.",
            "required": false
        },
        {
            "name": "seasonality_mode",
            "example": "additive",
            "description": "Prophet seasonality mode ('additive' or 'multiplicative'). Defaults to 'additive'.",
            "required": false
        },
        {
            "name": "changepoint_prior_scale",
            "example": "0.05",
            "description": "Flexibility of trend changepoints, must be greater than 0. Defaults to 0.05.",
            "required": false
        },
        {
            "name": "changepoints",
            "example": "['2025-01-01', '2025-06-01']",
            "description": "Changepoint dates (ISO format) as a JSON list or a space-separated string.",
            "required": false
        },
        {
            "name": "holiday_date_list",
            "example": "['2025-07-04']",
            "description": "Custom holiday dates (ISO format) as a JSON list or a space-separated string.",
            "required": false
        },
        {
            "name": "holiday_names",
            "example": "['Independence Day']",
            "description": "Names matching holiday_date_list, as a JSON list or a dot-separated string.",
            "required": false
        },
        {
            "name": "holiday_country_names",
            "example": "['US']",
            "description": "Country code for built-in holidays, as a JSON list or a dot-separated string. Prophet supports one country, so only the first entry is used.",
            "required": false
        },
        {
            "name": "inferred_freq",
            "example": "1D",
            "description": "Manually specified pandas frequency alias, fixed ('30min', '1h', '1s') or calendar ('D', 'W-SUN', 'MS', 'QS'). If not provided, frequency is inferred from data.",
            "required": false
        },
        {
            "name": "validation_window",
            "example": "3d",
            "description": "Duration held back from training and used to validate the forecast. Defaults to '0s' (no validation).",
            "required": false
        },
        {
            "name": "validation_alignment",
            "example": "nearest",
            "description": "How actual and forecasted values are paired during validation: 'position' (default) pairs them in time order, 'nearest' pairs each actual value with the closest forecast point within half a frequency step.",
            "required": false
        },
        {
            "name": "msre_threshold",
            "example": "0.05",
            "description": "Maximum acceptable Mean Squared Relative Error (MSRE) for validation. Defaults to infinity (no threshold).",
            "required": false
        },
        {
            "name": "max_forecast_points",
            "example": "10000",
            "description": "Maximum number of forecast points per run, counting the validation window. Defaults to 10000.",
            "required": false
        },
        {
            "name": "target_database",
            "example": "forecast_db",
            "description": "Database for forecast results. Defaults to a database named 'default', which is created on first write.",
            "required": false
        }
    ]
}
"""

import json
import math
import os
import random
import re
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from string import Template
from urllib.parse import urlparse

import pandas as pd
import requests
from influxdata_plugin_utils.config import (
    Validator,
    load_plugin_config,
    resolve_plugin_dir,
)
from influxdata_plugin_utils.parsing import (
    parse_bool,
    parse_delimited_list,
    parse_int,
    parse_timedelta,
)
from influxdata_plugin_utils.write import build_line, write_data
from prophet import Prophet
from prophet.serialize import model_from_json, model_to_json

AVAILABLE_SENDERS = {
    "slack": ["slack_webhook_url", "slack_headers"],
    "discord": ["discord_webhook_url", "discord_headers"],
    "http": ["http_webhook_url", "http_headers"],
    "whatsapp": [
        "twilio_sid",
        "twilio_token",
        "twilio_to_number",
        "twilio_from_number",
    ],
    "sms": ["twilio_sid", "twilio_token", "twilio_to_number", "twilio_from_number"],
}

# Keywords to skip when validating sender args
EXCLUDED_KEYWORDS = ["headers", "token", "sid"]

# Calendar units have no fixed length, so they are approximated in days.
CALENDAR_UNIT_DAYS = {"m": 30.42, "q": 91.25, "y": 365.0}

MODEL_DIR_NAME = "prophet_models"
# unique_suffix becomes part of a file name, so its character set is restricted
SAFE_SUFFIX_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")

SEASONALITY_MODES = ("additive", "multiplicative")
MODEL_MODES = ("train", "predict")
VALIDATION_ALIGNMENTS = ("position", "nearest")

DEFAULT_MAX_FORECAST_POINTS = 10000

# Database used when target_database is not configured
DEFAULT_TARGET_DATABASE = "default"

DEFAULT_NOTIFICATION_TEXT = (
    "Validation failed for prophet model:$version on table:$measurement, field:$field "
    "for period from $start_time to $end_time, forecast not written to "
    "table:$output_measurement"
)

# run_forecast outcomes
FORECAST_WRITTEN = "written"
FORECAST_VALIDATION_FAILED = "validation_failed"
FORECAST_FAILED = "failed"


class ForecastError(Exception):
    """Raised for a condition that stops the run with a message meant for the user."""


def parse_interval(raw) -> timedelta:
    """
    Parse an interval string ('30s', '10min', '2d', '1m', '1q', '1y') into a timedelta.

    Supported units: us, ms, s, min, h, d, w, plus the approximate calendar units
    m (30.42d), q (91.25d) and y (365d).
    """
    if isinstance(raw, timedelta):
        return raw

    match = re.fullmatch(r"\s*(\d+)\s*([a-zA-Z]+)\s*", str(raw))
    if match and match.group(2).lower() in CALENDAR_UNIT_DAYS:
        magnitude: int = int(match.group(1))
        unit: str = match.group(2).lower()
        days: int = int(magnitude * CALENDAR_UNIT_DAYS[unit])
        if days < 1:
            raise ValueError(f"Duration {raw!r} rounds down to less than one day")
        return timedelta(days=days)

    try:
        return parse_timedelta(raw)
    except ValueError as e:
        raise ValueError(
            f"Invalid duration {raw!r} ({e}). Expected '<number><unit>', e.g. '10min', '2d', '1y'"
        ) from e


def parse_positive_interval(raw) -> timedelta:
    """Parse an interval and require it to be greater than zero."""
    interval: timedelta = parse_interval(raw)
    if interval <= timedelta(0):
        raise ValueError(f"Duration must be positive, got {raw!r}")
    return interval


def parse_unique_suffix(raw) -> str:
    """Validate the model version suffix, which becomes part of a file name."""
    suffix: str = str(raw).strip()
    if not SAFE_SUFFIX_PATTERN.fullmatch(suffix) or ".." in suffix:
        raise ValueError(
            f"Invalid unique_suffix {raw!r}: use up to 64 characters from letters, "
            "digits, '.', '_' and '-'"
        )
    return suffix


def parse_choice(raw, name: str, allowed: tuple[str, ...]) -> str:
    """Validate a lower-cased value against a fixed set of options."""
    value: str = str(raw).strip().lower()
    if value not in allowed:
        raise ValueError(
            f"Invalid {name} {raw!r}: expected one of {', '.join(allowed)}"
        )
    return value


def parse_prior_scale(raw) -> float:
    """Parse the changepoint prior scale, which Prophet requires to be finite and positive."""
    value: float = float(raw)
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"changepoint_prior_scale must be greater than 0, got {raw!r}")
    return value


def parse_msre_threshold(raw) -> float:
    """Parse the MSRE threshold, which cannot be negative."""
    value: float = float(raw)
    if value < 0:
        raise ValueError(f"msre_threshold cannot be negative, got {raw!r}")
    return value


def parse_freq(raw) -> str:
    """Validate a pandas frequency alias; an empty value means 'infer from data'."""
    freq: str = str(raw).strip()
    if not freq:
        return ""
    pd.tseries.frequencies.to_offset(freq)
    return freq


COMMON_VALIDATORS: list = [
    Validator("measurement", required=True, cast=str),
    Validator("field", required=True, cast=str),
    Validator("forecast_horizont", required=True, cast=parse_positive_interval),
    Validator("tag_values", required=True),
    Validator("target_measurement", required=True, cast=str),
    Validator("unique_suffix", required=True, cast=parse_unique_suffix),
    Validator(
        "seasonality_mode",
        default="additive",
        cast=lambda raw: parse_choice(raw, "seasonality_mode", SEASONALITY_MODES),
    ),
    Validator("changepoint_prior_scale", default=0.05, cast=parse_prior_scale),
    Validator("changepoints", default=""),
    Validator("holiday_date_list", default=""),
    Validator(
        "holiday_names",
        default="",
        cast=lambda raw: parse_delimited_list(raw, sep="."),
    ),
    Validator(
        "holiday_country_names",
        default="",
        cast=lambda raw: parse_delimited_list(raw, sep="."),
    ),
    Validator("inferred_freq", default="", cast=parse_freq),
    Validator("validation_window", default="0s", cast=parse_interval),
    Validator(
        "validation_alignment",
        default="position",
        cast=lambda raw: parse_choice(
            raw, "validation_alignment", VALIDATION_ALIGNMENTS
        ),
    ),
    Validator("msre_threshold", default=float("inf"), cast=parse_msre_threshold),
    Validator(
        "max_forecast_points",
        default=DEFAULT_MAX_FORECAST_POINTS,
        cast=lambda raw: parse_int(raw, minimum=1),
    ),
    Validator("target_database", default="", cast=str),
]

SCHEDULED_VALIDATORS: list = COMMON_VALIDATORS + [
    Validator("window", required=True, cast=parse_positive_interval),
    Validator(
        "model_mode",
        required=True,
        cast=lambda raw: parse_choice(raw, "model_mode", MODEL_MODES),
    ),
    Validator("is_sending_alert", default=False, cast=parse_bool),
    Validator("notification_text", default=DEFAULT_NOTIFICATION_TEXT, cast=str),
    Validator("notification_path", default="notify", cast=str),
    Validator(
        "port_override",
        default=8181,
        cast=lambda raw: parse_int(raw, minimum=1, maximum=65535),
    ),
]

HTTP_VALIDATORS: list = COMMON_VALIDATORS + [
    Validator("start_time", required=True, cast=str),
    Validator("end_time", required=True, cast=str),
    Validator("save_mode", default=False, cast=parse_bool),
]


def load_config(
    args: dict | None, validators: list, *, source: str, env_keys=None
) -> dict:
    """
    Load and validate the plugin configuration.

    Args:
        args (dict | None): Trigger arguments or the parsed HTTP request body.
        validators (list): Validators for the entry point in use.
        source (str): "toml" to read the file named by config_file_path, "args" otherwise.
        env_keys (list[str] | None): Environment variables merged below the other layers.

    Returns:
        dict: Config values keyed by lower-case name.

    Raises:
        ForecastError: If a required value is missing or a value fails to cast.
    """
    try:
        loaded = load_plugin_config(
            args, validators=validators, env_keys=env_keys, source=source
        )
    except Exception as e:
        raise ForecastError(f"Failed to load configuration: {e}") from e

    return {key.lower(): value for key, value in loaded.as_dict().items()}


def quote_identifier(name: str) -> str:
    """Quote a SQL identifier, escaping embedded double quotes."""
    return '"' + str(name).replace('"', '""') + '"'


def parse_tag_values(influxdb3_local, raw, task_id: str) -> dict[str, str]:
    """
    Parse tag filters from a 'tag:value.tag2:value2' string or a mapping.

    Malformed pairs are skipped with a warning so one bad pair does not stop the run.

    Returns:
        dict[str, str]: Tag names mapped to the values to filter on.

    Example:
        >>> parse_tag_values(client, "host:server1.region:us-west", "t")
        {'host': 'server1', 'region': 'us-west'}
    """
    if isinstance(raw, dict):
        tag_values = {}
        for tag, value in raw.items():
            if isinstance(value, (list, tuple, set, dict)):
                influxdb3_local.warn(
                    f"[{task_id}] Skipping tag filter '{tag}': one value per tag is supported"
                )
                continue
            tag_values[str(tag)] = str(value)
        return tag_values

    tag_values: dict = {}
    for pair in parse_delimited_list(raw, sep="."):
        tag, separator, value = pair.partition(":")
        if not separator:
            influxdb3_local.warn(
                f"[{task_id}] Skipping malformed tag filter '{pair}': expected <tag>:<value>"
            )
            continue
        tag_values[tag.strip()] = value.strip()
    return tag_values


def parse_date_list(influxdb3_local, raw, name: str, task_id: str) -> list[str] | None:
    """
    Parse ISO dates from a space-separated string or a list, skipping invalid entries.

    Returns:
        list[str] | None: Valid dates, or None when none remain.
    """
    dates: list = []
    for item in parse_delimited_list(raw, sep=" "):
        try:
            datetime.fromisoformat(item)
        except ValueError:
            influxdb3_local.warn(f"[{task_id}] Skipping invalid {name} value '{item}'")
            continue
        dates.append(item)

    return dates or None


def build_where(
    tag_values: dict, start_time: datetime, end_time: datetime
) -> tuple[str, dict]:
    """
    Build the WHERE clause and bound parameters for a time window and tag filters.

    Values are passed as query parameters and identifiers are quote-escaped, so a
    quote in a tag value or column name can neither break nor inject the query.

    Returns:
        tuple[str, dict]: The clause and the parameters it references.
    """
    params: dict = {
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
    }
    clause: str = "time >= $start_time AND time < $end_time"
    for index, (tag, value) in enumerate(tag_values.items()):
        name: str = f"tag{index}"
        clause += f" AND {quote_identifier(tag)} = ${name}"
        params[name] = value
    return clause, params


def query_series(
    influxdb3_local,
    config: dict,
    tag_values: dict,
    start_time: datetime,
    end_time: datetime,
    task_id: str,
) -> pd.DataFrame | None:
    """
    Query one time window and return it as a Prophet-shaped frame.

    Returns:
        pd.DataFrame | None: Columns 'ds' (tz-naive UTC) and 'y' (numeric), or None
        when the window holds no usable rows.

    Raises:
        ForecastError: If the results lack the time or field column.
    """
    measurement: str = config["measurement"]
    field: str = config["field"]
    where, params = build_where(tag_values, start_time, end_time)
    query: str = (
        f"SELECT time, {quote_identifier(field)} "
        f"FROM {quote_identifier(measurement)} WHERE {where} ORDER BY time"
    )

    rows: list = influxdb3_local.query(query, params)
    if not rows:
        return None

    df: pd.DataFrame = pd.DataFrame(rows)
    if "time" not in df.columns or field not in df.columns:
        raise ForecastError(
            f"Query results for '{measurement}' are missing 'time' or '{field}'"
        )

    df = df.rename(columns={"time": "ds", field: "y"})
    df["ds"] = pd.to_datetime(df["ds"], unit="ns")
    df["y"] = pd.to_numeric(df["y"], errors="coerce")
    dropped: int = int(df["y"].isna().sum())
    if dropped:
        influxdb3_local.warn(
            f"[{task_id}] Dropping {dropped} rows where '{field}' is missing or not numeric"
        )
        df = df.dropna(subset=["y"])
    if df.empty:
        return None

    influxdb3_local.info(
        f"[{task_id}] Retrieved {len(df)} rows from {measurement} "
        f"({start_time.isoformat()} to {end_time.isoformat()})"
    )
    return df.reset_index(drop=True)


def get_model_storage_path(unique_suffix: str) -> Path:
    """
    Return the model file path for a version suffix, creating the directory if needed.

    Args:
        unique_suffix (str): Validated model version suffix.

    Returns:
        Path: Path to the model JSON file under <plugin dir>/prophet_models.
    """
    model_dir: Path = resolve_plugin_dir() / MODEL_DIR_NAME
    model_dir.mkdir(parents=True, exist_ok=True)
    return model_dir / f"prophet_model_{unique_suffix}.json"


def save_model(model: Prophet, file_path: Path) -> None:
    """Serialize a model through a temporary file so readers never see partial JSON."""
    temp_path: Path = file_path.with_name(f"{file_path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temp_path.write_text(model_to_json(model))
        os.replace(temp_path, file_path)
    finally:
        temp_path.unlink(missing_ok=True)


def create_prophet_model(
    influxdb3_local,
    config: dict,
    changepoints: list | None,
    holiday_dates: list | None,
    task_id: str,
) -> Prophet:
    """
    Build a Prophet model from the configured seasonality, changepoints and holidays.

    Returns:
        Prophet: An unfitted model.
    """
    model: Prophet = Prophet(
        seasonality_mode=config["seasonality_mode"],
        changepoint_prior_scale=config["changepoint_prior_scale"],
        changepoints=changepoints,
    )

    holiday_names: list = config["holiday_names"]
    if bool(holiday_dates) != bool(holiday_names):
        missing: str = "holiday_names" if holiday_dates else "holiday_date_list"
        influxdb3_local.warn(
            f"[{task_id}] {missing} is not set, skipping custom holidays"
        )
    elif holiday_dates and holiday_names:
        if len(holiday_dates) != len(holiday_names):
            influxdb3_local.warn(
                f"[{task_id}] holiday_date_list ({len(holiday_dates)}) and holiday_names "
                f"({len(holiday_names)}) differ in length, skipping holidays"
            )
        else:
            model.holidays = pd.DataFrame(
                {"ds": pd.to_datetime(holiday_dates), "holiday": holiday_names}
            )

    country_names: list = config["holiday_country_names"]
    if country_names:
        if len(country_names) > 1:
            influxdb3_local.warn(
                f"[{task_id}] Prophet supports built-in holidays for one country, "
                f"using '{country_names[0]}'"
            )
        model.add_country_holidays(country_name=country_names[0])

    return model


def load_or_train_model(
    influxdb3_local,
    config: dict,
    history: pd.DataFrame,
    changepoints: list | None,
    holiday_dates: list | None,
    use_saved_model: bool,
    task_id: str,
) -> Prophet:
    """
    Load the saved model for this version, or train one and save it when missing.

    With use_saved_model set to False the model is trained in memory and not stored.

    Returns:
        Prophet: A fitted model.
    """
    if not use_saved_model:
        model: Prophet = create_prophet_model(
            influxdb3_local, config, changepoints, holiday_dates, task_id
        )
        model.fit(history)
        influxdb3_local.info(f"[{task_id}] Model trained")
        return model

    file_path: Path = get_model_storage_path(config["unique_suffix"])
    if file_path.exists():
        model = model_from_json(file_path.read_text())
        influxdb3_local.info(f"[{task_id}] Model loaded from {file_path}")
        return model

    influxdb3_local.warn(
        f"[{task_id}] Model file not found at {file_path}, training a new model now"
    )
    model = create_prophet_model(
        influxdb3_local, config, changepoints, holiday_dates, task_id
    )
    model.fit(history)
    save_model(model, file_path)
    influxdb3_local.info(f"[{task_id}] Newly trained model saved to {file_path}")
    return model


def fixed_step(freq: str) -> timedelta | None:
    """Duration of one step, or None for calendar frequencies (day, week, month, ...)."""
    try:
        return pd.to_timedelta(pd.tseries.frequencies.to_offset(freq)).to_pytimedelta()
    except (ValueError, TypeError):
        return None


def resolve_frequency(
    influxdb3_local, config: dict, history: pd.DataFrame, task_id: str
) -> tuple[str, timedelta | None]:
    """
    Return the forecast frequency alias and its duration.

    Calendar frequencies such as ``D``, ``W-SUN`` or ``MS`` have no fixed duration;
    ``None`` is returned for them and the grid is stepped by the offset itself.

    Raises:
        ForecastError: If the frequency is neither configured nor inferable, or if
        one step does not move time forward.
    """
    freq: str = config["inferred_freq"]
    if not freq:
        if len(history) < 3:
            raise ForecastError(
                f"Only {len(history)} points retrieved, at least 3 are needed to infer the "
                "frequency; provide it with the 'inferred_freq' argument"
            )
        freq = pd.infer_freq(history["ds"])
        if freq is None:
            raise ForecastError(
                "Unable to infer frequency, please provide it manually with the "
                "'inferred_freq' argument"
            )

    if pd.tseries.frequencies.to_offset(freq).n <= 0:
        raise ForecastError(f"Frequency '{freq}' does not move time forward")

    step: timedelta | None = fixed_step(freq)

    influxdb3_local.info(f"[{task_id}] Using frequency: {freq}")
    return freq, step


def forecast_tolerance(freq: str, grid: pd.DatetimeIndex) -> timedelta:
    """Half of one forecast step: the widest gap that still maps an actual value to one point."""
    if len(grid) > 1:
        spacing: pd.Timedelta = pd.Series(grid).diff().median()
    else:
        offset = pd.tseries.frequencies.to_offset(freq)
        spacing = (grid[0] + offset) - grid[0]
    return (spacing / 2).to_pytimedelta()


def validate_forecast(
    influxdb3_local,
    actual: pd.DataFrame,
    forecast: pd.DataFrame,
    msre_threshold: float,
    alignment: str,
    tolerance: timedelta,
    task_id: str,
) -> bool:
    """
    Compare forecasted values with actual ones over the validation window.

    With alignment "position" the two series are paired in time order; with
    "nearest" each actual value is paired with the closest forecast point within
    `tolerance`.

    Returns:
        bool: True when MSRE stays within the threshold, False when MSRE cannot be
        computed or exceeds it.
    """
    actual_sorted: pd.DataFrame = (
        actual.dropna(subset=["y"]).sort_values("ds").reset_index(drop=True)
    )
    predicted: pd.DataFrame = (
        forecast[["ds", "yhat"]].sort_values("ds").reset_index(drop=True)
    )

    if alignment == "nearest":
        # half a step keeps the mapping unambiguous: the windows of neighbouring
        # forecast points do not overlap
        matched: pd.DataFrame = pd.merge_asof(
            actual_sorted,
            predicted,
            on="ds",
            direction="nearest",
            tolerance=pd.Timedelta(tolerance),
        ).dropna(subset=["yhat"])
        if matched.empty:
            influxdb3_local.warn(
                f"[{task_id}] No actual value falls within {tolerance} of a forecast "
                f"point, treating validation as failed"
            )
            return False
        influxdb3_local.info(
            f"[{task_id}] Validating {len(matched)} of {len(actual_sorted)} actual "
            f"points matched within {tolerance}"
        )
        y_true, y_pred = matched["y"], matched["yhat"]
    else:
        length: int = min(len(actual_sorted), len(predicted))
        y_true = actual_sorted["y"].iloc[:length]
        y_pred = predicted["yhat"].iloc[:length]

    # zero actuals would divide by zero in the relative error
    nonzero = y_true != 0
    y_true, y_pred = y_true[nonzero], y_pred[nonzero]
    if y_true.empty:
        influxdb3_local.warn(
            f"[{task_id}] All actual values in the validation window are zero, MSRE has "
            f"nothing to compare, treating validation as failed"
        )
        return False

    msre: float = float(((y_true - y_pred) ** 2 / y_true**2).mean())
    influxdb3_local.info(f"[{task_id}] MSRE: {msre}")

    if math.isnan(msre):
        influxdb3_local.warn(
            f"[{task_id}] MSRE is not a number, treating validation as failed"
        )
        return False

    if msre > msre_threshold:
        influxdb3_local.warn(
            f"[{task_id}] MSRE {msre} exceeds threshold {msre_threshold}, consider retraining."
        )
        return False

    return True


def to_naive_utc(moment: datetime) -> pd.Timestamp:
    """Convert a datetime to a tz-naive UTC timestamp, matching the queried data."""
    timestamp: pd.Timestamp = pd.Timestamp(moment)
    if timestamp.tz is None:
        return timestamp
    return timestamp.tz_convert("UTC").tz_localize(None)


def build_forecast_lines(
    influxdb3_local,
    forecast: pd.DataFrame,
    config: dict,
    tag_values: dict,
    run_time: datetime,
    forecast_start: datetime,
    task_id: str,
) -> list:
    """
    Turn forecast rows at or after forecast_start into LineBuilder objects.

    Points with a non-finite value are skipped, because one of them would fail the
    whole batched write.

    Returns:
        list: LineBuilder objects ready to write.
    """
    cutoff: pd.Timestamp = to_naive_utc(forecast_start)
    future: pd.DataFrame = forecast.loc[
        forecast["ds"] >= cutoff, ["ds", "yhat", "yhat_lower", "yhat_upper"]
    ].copy()
    future["time_ns"] = future["ds"].astype("datetime64[ns]").astype("int64")

    tags: dict = {"model_version": config["unique_suffix"], **tag_values}
    run_time_text: str = run_time.isoformat()

    lines: list = []
    skipped: int = 0
    for row in future.itertuples(index=False):
        values: tuple = (float(row.yhat), float(row.yhat_lower), float(row.yhat_upper))
        if not all(math.isfinite(value) for value in values):
            skipped += 1
            continue
        forecast_value, lower, upper = values
        lines.append(
            build_line(
                LineBuilder,
                config["target_measurement"],
                tags=tags,
                fields={
                    "forecast": forecast_value,
                    "yhat_lower": lower,
                    "yhat_upper": upper,
                    "run_time": run_time_text,
                },
                time_ns=int(row.time_ns),
            )
        )

    if skipped:
        influxdb3_local.warn(
            f"[{task_id}] Skipped {skipped} forecast points with non-finite values"
        )
    return lines


def write_forecast(influxdb3_local, lines: list, database: str, task_id: str) -> None:
    """Queue forecast points as one batched payload."""
    influxdb3_local.info(
        f"[{task_id}] Writing {len(lines)} forecast points to database {database}"
    )
    write_data(influxdb3_local, lines, database=database, retries=0)


def run_forecast(
    influxdb3_local,
    config: dict,
    tag_values: dict,
    *,
    history_start: datetime,
    history_end: datetime,
    forecast_start: datetime,
    use_saved_model: bool,
    run_time: datetime,
    task_id: str,
) -> tuple[str, str]:
    """
    Train or load a model, forecast, optionally validate, and write the results.

    Args:
        influxdb3_local: InfluxDB client instance.
        config (dict): Loaded configuration.
        tag_values (dict): Tag filters for the source query.
        history_start (datetime): Start of the training window.
        history_end (datetime): End of the training window and start of validation.
        forecast_start (datetime): End of validation and first timestamp written.
        use_saved_model (bool): Load or persist the model for this version.
        run_time (datetime): Value stored in the run_time field.
        task_id (str): Unique task identifier.

    Returns:
        tuple[str, str]: Outcome (FORECAST_WRITTEN, FORECAST_VALIDATION_FAILED or
        FORECAST_FAILED) and a message describing it.
    """
    target_measurement: str = config["target_measurement"]
    validation_window: timedelta = config["validation_window"]

    try:
        history: pd.DataFrame | None = query_series(
            influxdb3_local, config, tag_values, history_start, history_end, task_id
        )
        if history is None:
            return (
                FORECAST_FAILED,
                f"No data found from {history_start.isoformat()} to {history_end.isoformat()}",
            )

        changepoints: list | None = parse_date_list(
            influxdb3_local, config["changepoints"], "changepoints", task_id
        )
        holiday_dates: list | None = parse_date_list(
            influxdb3_local, config["holiday_date_list"], "holiday_date_list", task_id
        )
        model: Prophet = load_or_train_model(
            influxdb3_local,
            config,
            history,
            changepoints,
            holiday_dates,
            use_saved_model,
            task_id,
        )

        freq, step = resolve_frequency(influxdb3_local, config, history, task_id)
        forecast_horizont: timedelta = config["forecast_horizont"]
        if step is not None and forecast_horizont < step:
            return (
                FORECAST_FAILED,
                f"Forecast horizon {forecast_horizont} is shorter than one '{freq}' step",
            )

        # the timestamps follow the queried data, so a model loaded from disk forecasts
        # the requested range instead of the dates it happens to be trained up to
        anchor: pd.Timestamp = history["ds"].max() + pd.tseries.frequencies.to_offset(
            freq
        )
        target_end: pd.Timestamp = to_naive_utc(forecast_start) + forecast_horizont
        if anchor >= target_end:
            return (
                FORECAST_FAILED,
                f"The first '{freq}' point after the data is {anchor}, "
                f"not before the end of the horizon {target_end}",
            )

        max_points: int = config["max_forecast_points"]
        span: pd.Timedelta = target_end - anchor
        if step is not None:
            periods: int = math.ceil(span / step)
            if periods > max_points:
                return (
                    FORECAST_FAILED,
                    f"Forecast needs {periods} points at the '{freq}' step, above "
                    f"max_forecast_points ({max_points})",
                )
            grid: pd.DatetimeIndex = pd.date_range(
                start=anchor, periods=periods, freq=freq
            )
        else:
            # a calendar step has no fixed length, so the grid is bounded by the
            # requested range and counted once it exists
            grid = pd.date_range(
                start=anchor, end=target_end, freq=freq, inclusive="left"
            )
            if len(grid) > max_points:
                return (
                    FORECAST_FAILED,
                    f"Forecast needs {len(grid)} points at the '{freq}' step, above "
                    f"max_forecast_points ({max_points})",
                )

        if grid.empty:
            return (
                FORECAST_FAILED,
                f"No '{freq}' point falls between {anchor} and {target_end}",
            )
        influxdb3_local.info(
            f"[{task_id}] Forecast horizon: {forecast_horizont}, frequency: {freq}, "
            f"periods: {len(grid)} from {grid[0]}"
        )

        forecast: pd.DataFrame = model.predict(pd.DataFrame({"ds": grid}))

        if validation_window > timedelta(0):
            actual: pd.DataFrame | None = query_series(
                influxdb3_local,
                config,
                tag_values,
                history_end,
                forecast_start,
                task_id,
            )
            if actual is None:
                return (
                    FORECAST_VALIDATION_FAILED,
                    f"No data found for validation window "
                    f"{history_end.isoformat()} to {forecast_start.isoformat()}, "
                    f"forecast not written to {target_measurement}",
                )
            if not validate_forecast(
                influxdb3_local,
                actual,
                forecast,
                config["msre_threshold"],
                config["validation_alignment"],
                forecast_tolerance(freq, grid),
                task_id,
            ):
                return (
                    FORECAST_VALIDATION_FAILED,
                    f"Validation failed, forecast not written to {target_measurement}",
                )

        lines: list = build_forecast_lines(
            influxdb3_local,
            forecast,
            config,
            tag_values,
            run_time,
            forecast_start,
            task_id,
        )
        if not lines:
            return (
                FORECAST_FAILED,
                f"Forecast holds no points at or after {forecast_start.isoformat()}",
            )

        database: str = config["target_database"] or DEFAULT_TARGET_DATABASE
        write_forecast(influxdb3_local, lines, database, task_id)
        return FORECAST_WRITTEN, f"Forecast written to {target_measurement}"

    except ForecastError as e:
        return FORECAST_FAILED, str(e)


def parse_senders(influxdb3_local, config: dict, task_id: str) -> dict:
    """
    Parse and validate sender configurations from the loaded config.

    Args:
        influxdb3_local: InfluxDB client instance.
        config (dict): Loaded configuration containing "senders" (dot-separated string
            or list) and the keys each sender requires (see AVAILABLE_SENDERS).
        task_id (str): Unique task identifier used for logging context.

    Returns:
        dict: A mapping `{sender_type: {key: value}}` for each valid sender.

    Raises:
        Exception: If no valid senders are found after parsing.
    """
    senders_config: defaultdict = defaultdict(dict)
    senders: list = parse_delimited_list(config.get("senders", ""), sep=".")
    if not senders:
        raise Exception("No senders provided")

    for sender in senders:
        if sender not in AVAILABLE_SENDERS:
            influxdb3_local.warn(f"[{task_id}] Invalid sender type: {sender}")
            continue
        for key in AVAILABLE_SENDERS[sender]:
            if key not in config and not any(ex in key for ex in EXCLUDED_KEYWORDS):
                influxdb3_local.warn(
                    f"[{task_id}] Required key '{key}' missing for sender '{sender}'"
                )
                senders_config.pop(sender, None)
                break
            if "url" in key and not validate_webhook_url(
                influxdb3_local, sender, config[key], task_id
            ):
                senders_config.pop(sender, None)
                break

            if key not in config:
                continue
            senders_config[sender][key] = config[key]

    if not senders_config:
        raise Exception("No valid senders configured")
    return senders_config


def validate_webhook_url(influxdb3_local, service: str, url: str, task_id: str) -> bool:
    """
    Validate webhook URL format.

    Returns:
        bool: True if URL is valid, False otherwise.
    """
    try:
        result = urlparse(url)
        if result.scheme not in ("http", "https"):
            influxdb3_local.error(
                f"[{task_id}] {service} webhook URL must start with 'https://' or 'http://'"
            )
            return False
        return True
    except Exception as e:
        influxdb3_local.error(
            f"[{task_id}] Unable to parse {service} webhook URL: {str(e)}"
        )
        return False


def interpolate_notification_text(text: str, row_data: dict) -> str:
    """Replace $variables in the notification template with actual values."""
    return Template(text).safe_substitute(row_data)


def send_notification(
    influxdb3_local, port: int, path: str, token: str, payload: dict, task_id: str
) -> None:
    """
    Send a JSON POST to the given InfluxDB 3 webhook endpoint, with up to
    3 retry attempts and randomized backoff delays between attempts.

    Args:
        influxdb3_local: InfluxDB client instance.
        port (int): Port number on which the HTTP API is listening (e.g. 8181).
        path (str): Path to the webhook handler (e.g. "notify" or "custom/path").
        token (str): API v3 token string (without the "Bearer " prefix).
        payload (dict): Dict to serialize as JSON in the POST body.
        task_id (str): Unique task identifier.
    """
    url: str = f"http://localhost:{port}/api/v3/engine/{path}"
    headers: dict = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    data: str = json.dumps(payload)

    max_retries: int = 3
    timeout: float = 5.0

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(url, headers=headers, data=data, timeout=timeout)
            resp.raise_for_status()  # raises on 4xx/5xx
            influxdb3_local.info(
                f"[{task_id}] Alert sent to notification plugin with results: {resp.json()['results']}"
            )
            return
        except requests.RequestException as e:
            influxdb3_local.warn(
                f"[{task_id}] [Attempt {attempt}/{max_retries}] Error sending alert to notification plugin: {e}"
            )
            if attempt < max_retries:
                wait = random.uniform(1, 4)
                influxdb3_local.info(
                    f"[{task_id}] Retrying sending alert to notification plugin in {wait:.1f} seconds."
                )
                time.sleep(wait)
            else:
                influxdb3_local.error(
                    f"[{task_id}] Failed to send alert to notification plugin after {max_retries} attempts: {e}"
                )


def send_validation_alert(
    influxdb3_local,
    config: dict,
    validation_start: datetime,
    validation_end: datetime,
    task_id: str,
) -> None:
    """Send the validation-failure alert for the window that was actually validated."""
    try:
        senders_config: dict = parse_senders(influxdb3_local, config, task_id)
        token: str = config.get("influxdb3_auth_token", "")
        if not token:
            raise Exception("INFLUXDB3_AUTH_TOKEN not found")

        payload: dict = {
            "notification_text": interpolate_notification_text(
                config["notification_text"],
                {
                    "version": config["unique_suffix"],
                    "measurement": config["measurement"],
                    "field": config["field"],
                    "start_time": validation_start.isoformat(),
                    "end_time": validation_end.isoformat(),
                    "output_measurement": config["target_measurement"],
                },
            ),
            "senders_config": senders_config,
        }
        send_notification(
            influxdb3_local,
            config["port_override"],
            config["notification_path"],
            token,
            payload,
            task_id,
        )
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to send notification: {e}")


def process_scheduled_call(
    influxdb3_local, call_time: datetime, args: dict | None = None
):
    """
    Run a scheduled Prophet forecast: query the historical window, train or load a
    model, forecast the configured horizon, validate the result when a validation
    window is set, write the forecast, and alert on validation failure.

    Args:
        influxdb3_local: InfluxDB client instance.
        call_time (datetime): Time the trigger fired; the windows hang off it.
        args (dict | None): Trigger arguments, or a TOML file named by
            config_file_path. See the plugin metadata for the supported keys.

    All exceptions are caught and logged; nothing propagates to the engine.
    """
    task_id: str = str(uuid.uuid4())
    influxdb3_local.info(f"[{task_id}] Starting scheduled forecast at {call_time}")

    try:
        config_file_path = (args or {}).get("config_file_path")
        if config_file_path and not str(config_file_path).endswith(".toml"):
            raise ForecastError("Invalid config file format: expected a .toml file")

        config: dict = load_config(
            args,
            SCHEDULED_VALIDATORS,
            source="toml" if config_file_path else "args",
            env_keys=["INFLUXDB3_AUTH_TOKEN"],
        )
        tag_values: dict = parse_tag_values(
            influxdb3_local, config["tag_values"], task_id
        )

        # the engine passes a naive UTC timestamp
        run_time: datetime = (
            call_time if call_time.tzinfo else call_time.replace(tzinfo=timezone.utc)
        )
        if config["is_sending_alert"] and config["validation_window"] <= timedelta(0):
            influxdb3_local.warn(
                f"[{task_id}] is_sending_alert has no effect without validation_window: "
                f"alerts are only sent when validation fails"
            )

        history_start: datetime = run_time - config["window"]
        history_end: datetime = run_time - config["validation_window"]
        if history_start >= history_end:
            raise ForecastError(
                f"Empty training window: 'window' ({config['window']}) must exceed "
                f"'validation_window' ({config['validation_window']})"
            )

        status, message = run_forecast(
            influxdb3_local,
            config,
            tag_values,
            history_start=history_start,
            history_end=history_end,
            forecast_start=run_time,
            use_saved_model=config["model_mode"] == "predict",
            run_time=run_time,
            task_id=task_id,
        )

        if status == FORECAST_WRITTEN:
            influxdb3_local.info(f"[{task_id}] {message}")
            return

        influxdb3_local.error(f"[{task_id}] {message}")
        if status == FORECAST_VALIDATION_FAILED and config["is_sending_alert"]:
            send_validation_alert(
                influxdb3_local, config, history_end, run_time, task_id
            )

    except ForecastError as e:
        influxdb3_local.error(f"[{task_id}] {e}")
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Unexpected error: {e}")


def parse_time_window(config: dict) -> tuple[datetime, datetime]:
    """
    Parse the historical window bounds, which must be timezone-aware ISO 8601
    strings (e.g. '2025-05-01T00:00:00+03:00').

    Returns:
        tuple[datetime, datetime]: Start and end of the window in UTC.

    Raises:
        ForecastError: If a value is not ISO 8601, lacks a timezone, or start >= end.
    """

    def parse_iso_datetime(name: str, value: str) -> datetime:
        try:
            moment: datetime = datetime.fromisoformat(str(value))
        except ValueError:
            raise ForecastError(f"Invalid ISO 8601 datetime for {name}: '{value}'.")
        if moment.tzinfo is None:
            raise ForecastError(f"{name} must include timezone info (e.g., '+00:00').")
        return moment.astimezone(timezone.utc)

    start_time: datetime = parse_iso_datetime("start_time", config["start_time"])
    end_time: datetime = parse_iso_datetime("end_time", config["end_time"])

    if start_time >= end_time:
        raise ForecastError(
            f"start_time {start_time} must be earlier than end_time {end_time}."
        )

    return start_time, end_time


def process_request(
    influxdb3_local, query_parameters, request_headers, request_body, args=None
):
    """
    Run a one-off Prophet forecast over the window given in the request body.

    Reads the historical window, trains a model or loads the saved one for
    unique_suffix when save_mode is set, forecasts the configured horizon,
    validates the result when a validation window is set, and writes the forecast.

    Args:
        influxdb3_local: InfluxDB client instance.
        query_parameters: HTTP query parameters (unused).
        request_headers: HTTP request headers (unused).
        request_body: JSON body holding the forecast configuration. See the
            http_body_config section of the plugin metadata for the supported keys.
        args: Trigger arguments (unused; the body carries the configuration).

    Returns:
        dict: {"message": <outcome>}.
    """
    task_id: str = str(uuid.uuid4())
    influxdb3_local.info(f"[{task_id}] Received forecasting request")

    if not request_body:
        influxdb3_local.error(f"[{task_id}] No request body provided.")
        return {"message": f"[{task_id}] Error: No request body provided."}

    try:
        data = json.loads(request_body)
        if not isinstance(data, dict):
            raise ForecastError("Request body must be a JSON object")

        # an explicit JSON null means "not set", so the validator default applies
        config: dict = load_config(
            {key: value for key, value in data.items() if value is not None},
            HTTP_VALIDATORS,
            source="args",
        )
        tag_values: dict = parse_tag_values(
            influxdb3_local, config["tag_values"], task_id
        )

        start_time, end_time = parse_time_window(config)
        history_end: datetime = end_time - config["validation_window"]
        if start_time >= history_end:
            raise ForecastError(
                f"Empty training window: 'validation_window' "
                f"({config['validation_window']}) covers the whole requested range"
            )

        status, message = run_forecast(
            influxdb3_local,
            config,
            tag_values,
            history_start=start_time,
            history_end=history_end,
            forecast_start=end_time,
            use_saved_model=config["save_mode"],
            run_time=datetime.now(timezone.utc),
            task_id=task_id,
        )

        if status == FORECAST_WRITTEN:
            influxdb3_local.info(f"[{task_id}] {message}")
        else:
            influxdb3_local.error(f"[{task_id}] {message}")
        return {"message": f"[{task_id}] {message}"}

    except (ForecastError, json.JSONDecodeError) as e:
        influxdb3_local.error(f"[{task_id}] {e}")
        return {"message": f"[{task_id}] {e}"}
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Unexpected error: {e}")
        return {"message": f"[{task_id}] Unexpected error: {e}"}
