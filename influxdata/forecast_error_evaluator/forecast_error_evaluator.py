"""
{
    "plugin_type": ["scheduled"],
    "scheduled_args_config": [
        {
            "name": "forecast_measurement",
            "example": "forecast_data",
            "description": "The InfluxDB measurement containing forecasted values.",
            "required": true
        },
        {
            "name": "actual_measurement",
            "example": "actual_data",
            "description": "The InfluxDB measurement containing actual (ground truth) values.",
            "required": true
        },
        {
            "name": "forecast_field",
            "example": "predicted_temp",
            "description": "The field name in forecast_measurement for forecasted values.",
            "required": true
        },
        {
            "name": "actual_field",
            "example": "temp",
            "description": "The field name in actual_measurement for actual values.",
            "required": true
        },
        {
            "name": "error_metric",
            "example": "rmse",
            "description": "The error metric to use (mse, mae, rmse, mape, smape). Computed per timestamp, so rmse equals mae.",
            "required": true
        },
        {
            "name": "error_thresholds",
            "example": "INFO-'0.5':WARN-'0.9':ERROR-'1.2':CRITICAL-'1.5'",
            "description": "Colon-separated <level>-<threshold> pairs (a TOML config may use a table instead). Levels: INFO, WARN, ERROR, CRITICAL. Thresholds must be above 0. Every level is evaluated on its own, so a point above several thresholds alerts once per level.",
            "required": true
        },
        {
            "name": "window",
            "example": "1h",
            "description": "Time window for data analysis (e.g., `1h` for 1 hour). Must be a positive duration. Units: `us`, `ms`, `s`, `min`, `h`, `d`, `w`.",
            "required": true
        },
        {
            "name": "senders",
            "example": "slack",
            "description": "Dot-separated list of notification channels (a TOML config may use a list instead). Supported channels: slack, discord, http, sms, whatsapp.",
            "required": true
        },
        {
            "name": "influxdb3_auth_token",
            "example": "YOUR_API_TOKEN",
            "description": "API token for InfluxDB 3. Can be set via `INFLUXDB3_AUTH_TOKEN` environment variable.",
            "required": false
        },
        {
            "name": "min_condition_duration",
            "example": "5min",
            "description": "Minimum duration for an anomaly condition to persist before triggering a notification (e.g., `5min`). Units: `us`, `ms`, `s`, `min`, `h`, `d`, `w`. Default: `0s` (alert on the first point above the threshold).",
            "required": false
        },
        {
            "name": "rounding_freq",
            "example": "1s",
            "description": "Fixed pandas frequency used to round timestamps before matching forecast to actual rows (e.g., `1s`, `500ms`, `5min`, `1h`). Default: no rounding.",
            "required": false
        },
        {
            "name": "max_notifications_per_run",
            "example": "20",
            "description": "Maximum number of notifications sent by a single run. Levels are processed from the highest threshold down, and alerts beyond the limit are counted in a warning and not resent later. Default: 20.",
            "required": false
        },
        {
            "name": "notification_text",
            "example": "[$level] Forecast error alert in $measurement.$field: $metric=$error. Tags: $tags",
            "description": "Template for notification message with variables `$measurement`, `$level`, `$field`, `$error`, `$metric`, `$tags`, `$timestamp`.",
            "required": false
        },
        {
            "name": "notification_path",
            "example": "some/path",
            "description": "URL path for the notification sending plugin. Default: 'notify'.",
            "required": false
        },
        {
            "name": "port_override",
            "example": "8182",
            "description": "Port number where InfluxDB accepts requests. Default: 8181.",
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
            "description": "Optional headers as base64-encoded string for Slack notifications.",
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
            "description": "Optional headers as base64-encoded string for Discord notifications.",
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
            "description": "Optional headers as base64-encoded string for HTTP notifications.",
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
            "example": "config.toml",
            "description": "Path to config file to override args. Format: 'config.toml'.",
            "required": false
        }
    ]
}
"""

import json
import os
import random
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from string import Template
from urllib.parse import urlparse

import pandas as pd
import requests
from influxdata_plugin_utils.config import Validator, load_plugin_config
from influxdata_plugin_utils.introspection import (
    get_table_names,
    get_tag_names,
    query_window,
)
from influxdata_plugin_utils.parsing import (
    parse_delimited_list,
    parse_int,
    parse_timedelta,
)

# Supported sender types with their required arguments
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

# Error metrics computed per timestamp
AVAILABLE_ERROR_METRICS = ("mse", "mae", "rmse", "mape", "smape")

# Severity levels accepted in error_thresholds
THRESHOLD_LEVELS = ("INFO", "WARN", "ERROR", "CRITICAL")

_DEFAULT_NOTIFICATION_TEXT = (
    "[$level] Forecast error alert in $measurement.$field: $metric=$error. Tags: $tags"
)


def parse_window(raw) -> timedelta:
    """Parse the analysis window, rejecting non-positive durations."""
    window: timedelta = parse_timedelta(raw)
    if window <= timedelta(0):
        raise ValueError(f"Invalid window: {raw!r} (must be a positive duration)")
    return window


def parse_error_metric(raw) -> str:
    """Normalize the error metric name and reject unsupported ones."""
    metric: str = str(raw).strip().lower()
    if metric not in AVAILABLE_ERROR_METRICS:
        raise ValueError(
            f"Unsupported error_metric {raw!r}; use {'|'.join(AVAILABLE_ERROR_METRICS)}"
        )
    return metric


def parse_rounding_freq(raw) -> str:
    """Validate a pandas rounding frequency, treating an empty value as no rounding."""
    freq: str = str(raw).strip()
    if not freq:
        return ""
    try:
        pd.Timestamp(0).round(freq)
    except Exception:
        raise ValueError(
            f"Invalid rounding_freq {raw!r}; use a fixed pandas frequency "
            f"such as 1s, 500ms, 5min or 1h"
        )
    return freq


_VALIDATORS = [
    Validator("forecast_measurement", required=True, cast=str),
    Validator("actual_measurement", required=True, cast=str),
    Validator("forecast_field", required=True, cast=str),
    Validator("actual_field", required=True, cast=str),
    Validator("error_metric", required=True, cast=parse_error_metric),
    Validator("error_thresholds", required=True),
    Validator("window", required=True, cast=parse_window),
    Validator("senders", required=True),
    Validator("min_condition_duration", default="0s", cast=parse_timedelta),
    Validator("rounding_freq", default="", cast=parse_rounding_freq),
    Validator(
        "max_notifications_per_run",
        default=20,
        cast=lambda raw: parse_int(raw, minimum=1),
    ),
    Validator("notification_text", default=_DEFAULT_NOTIFICATION_TEXT, cast=str),
    Validator("notification_path", default="notify", cast=str),
    Validator(
        "port_override",
        default=8181,
        cast=lambda raw: parse_int(raw, minimum=1, maximum=65535),
    ),
]


def _load_config(influxdb3_local, args: dict | None, task_id: str) -> dict | None:
    """
    Load the plugin configuration, applying defaults and type casts.

    Args:
        influxdb3_local: InfluxDB client instance.
        args (dict | None): Runtime arguments of the trigger.
        task_id (str): Unique task identifier.

    Returns:
        dict | None: Config values keyed by lower-case name, or None if loading failed.
    """
    config_file_path = (args or {}).get("config_file_path")
    if config_file_path and not str(config_file_path).endswith(".toml"):
        influxdb3_local.error(
            f"[{task_id}] Invalid config file format: expected a .toml file"
        )
        return None

    try:
        loaded = load_plugin_config(
            args,
            validators=_VALIDATORS,
            env_keys=["INFLUXDB3_AUTH_TOKEN"],
            source="toml" if config_file_path else "args",
        )
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to load configuration: {e}")
        return None

    return {key.lower(): value for key, value in loaded.as_dict().items()}


def parse_error_thresholds(
    influxdb3_local, config: dict, task_id: str
) -> dict[str, float]:
    """
    Parse the error thresholds into a mapping of severity level to threshold value.

    Accepts the inline ``<level>-<value>`` form separated by colons and a mapping
    coming from a TOML table.

    Returns:
        dict[str, float]: Thresholds keyed by level, empty when nothing is valid.

    Example:
        >>> parse_error_thresholds(client, {"error_thresholds": "INFO-10:WARN-'20.5'"}, "t")
        {'INFO': 10.0, 'WARN': 20.5}
    """
    raw: str | dict = config["error_thresholds"]
    if isinstance(raw, dict):
        pairs: list = list(raw.items())
    else:
        pairs = []
        for part in str(raw).split(":"):
            part = part.strip()
            if not part:
                continue
            level, separator, value = part.partition("-")
            if not separator:
                influxdb3_local.warn(
                    f"[{task_id}] Skipping threshold '{part}': expected <level>-<value>"
                )
                continue
            pairs.append((level, value))

    thresholds: dict = {}
    for raw_level, raw_value in pairs:
        level: str = str(raw_level).strip().upper()
        if level not in THRESHOLD_LEVELS:
            influxdb3_local.warn(
                f"[{task_id}] Skipping threshold '{raw_level}': level must be one of "
                f"{', '.join(THRESHOLD_LEVELS)}"
            )
            continue

        text: str = str(raw_value).strip().strip("'\"")
        try:
            threshold: float = float(text)
        except ValueError:
            influxdb3_local.warn(
                f"[{task_id}] Skipping threshold '{level}': '{text}' is not a number"
            )
            continue

        if threshold <= 0:
            # every supported metric is non-negative, so such a threshold flags every point
            influxdb3_local.warn(
                f"[{task_id}] Skipping threshold '{level}': {threshold} would flag every "
                f"point, use a value above 0"
            )
            continue

        if level in thresholds:
            influxdb3_local.warn(
                f"[{task_id}] Skipping duplicate threshold '{level}': keeping "
                f"{thresholds[level]}"
            )
            continue
        thresholds[level] = threshold

    return thresholds


def parse_senders(influxdb3_local, config: dict, task_id: str) -> dict:
    """
    Parse and validate sender configurations from the loaded config.

    Args:
        influxdb3_local: InfluxDB client instance.
        config (dict): Loaded config containing "senders" and the sender-specific
            keys listed in AVAILABLE_SENDERS.
        task_id (str): Unique task identifier used for logging context.

    Returns:
        dict: A mapping `{sender_type: {key: value}}` for each valid sender.
              For example:
                {
                  "slack": {
                    "slack_webhook_url": "https://hooks.slack.com/...",
                    "slack_headers": "..."
                  },
                  "sms": { ... }
                }

    Raises:
        Exception: If no valid senders are found after parsing.
    """
    senders_config: defaultdict = defaultdict(dict)
    senders: list = parse_delimited_list(config["senders"], sep=".")

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
        bool: True if URL is valid, False otherwise
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
    """
    Replace variables in notification text with actual values from row data.

    Args:
        text (str): Template string with variables
        row_data (dict): Dictionary containing values to interpolate

    Returns:
        str: Interpolated text with variables replaced
    """
    return Template(text).safe_substitute(row_data)


def send_notification(
    influxdb3_local, port: int, path: str, token: str, payload: dict, task_id: str
) -> bool:
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

    Returns:
        bool: True when the alert was accepted, False when every attempt failed.
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
            return True
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

    return False


def resolve_shared_tags(
    influxdb3_local, forecast_measurement: str, actual_measurement: str, task_id: str
) -> list[str]:
    """
    Return the tags present in both measurements, sorted for stable cache keys.

    Args:
        influxdb3_local: InfluxDB client instance.
        forecast_measurement (str): Measurement holding forecasted values.
        actual_measurement (str): Measurement holding actual values.
        task_id (str): Unique task identifier.

    Returns:
        list[str]: Tag names usable for matching both series.
    """
    forecast_tags: set = set(get_tag_names(influxdb3_local, forecast_measurement))
    actual_tags: set = set(get_tag_names(influxdb3_local, actual_measurement))

    only_in_one: set = forecast_tags ^ actual_tags
    if only_in_one:
        influxdb3_local.warn(
            f"[{task_id}] Ignoring tags missing from one of the measurements: "
            f"{', '.join(sorted(only_in_one))}"
        )

    shared: list = sorted(forecast_tags & actual_tags)
    if not shared:
        influxdb3_local.info(
            f"[{task_id}] No tags shared by '{forecast_measurement}' and "
            f"'{actual_measurement}', matching on time only"
        )
    return shared


def format_tags(row: pd.Series, tags: list[str]) -> str:
    """Render the tag values of a row as a comma-separated list."""
    return ", ".join(f"{tag}={row.get(tag, 'None')}" for tag in tags)


def generate_cache_key(
    measurement: str, field: str, threshold_level: str, tags: list[str], row: pd.Series
) -> str:
    """
    Build a stable cache key string, ignoring timestamps, for debounce logic.

    Args:
        measurement (str): Measurement name.
        field (str): Field name under test.
        threshold_level (str): One of THRESHOLD_LEVELS.
        tags (list[str]): Tag column names to include.
        row (pd.Series): A row of data (from pandas DataFrame), used to pull tag values.

    Returns:
        str: Key like "cpu:temp:WARN:host=server1:region=us-west"
    """
    key: str = f"{measurement}:{field}:{threshold_level}"
    for tag in sorted(tags):
        tag_val: str = row.get(tag, "None")
        key += f":{tag}={tag_val}"
    return key


def query_series(
    influxdb3_local,
    measurement: str,
    field: str,
    tags: list[str],
    start_time: datetime,
    end_time: datetime,
    task_id: str,
) -> pd.DataFrame | None:
    """
    Query one measurement window and return it as a frame.

    Returns:
        pd.DataFrame | None: The queried rows, or None when there is nothing usable.
    """
    rows: list = query_window(
        influxdb3_local,
        measurement,
        start=start_time.isoformat(),
        end=end_time.isoformat(),
        columns=["time", field, *tags],
    )
    if not rows:
        influxdb3_local.info(
            f"[{task_id}] No data in {measurement}.{field} from {start_time} to {end_time}"
        )
        return None

    df: pd.DataFrame = pd.DataFrame(rows)
    if "time" not in df.columns or field not in df.columns:
        influxdb3_local.error(
            f"[{task_id}] Query results for '{measurement}' are missing 'time' or '{field}'"
        )
        return None

    influxdb3_local.info(f"[{task_id}] Retrieved {len(df)} rows from '{measurement}'")
    return df


def align_frames(
    influxdb3_local,
    df_forecast: pd.DataFrame,
    df_actual: pd.DataFrame,
    tags: list[str],
    rounding_freq: str,
    task_id: str,
) -> pd.DataFrame | None:
    """
    Round timestamps and inner-join the forecast frame with the actual frame.

    Rows sharing a key are collapsed to the earliest one, because duplicate keys
    would join into a cross product of unrelated points.

    Returns:
        pd.DataFrame | None: Matched rows, or None when nothing overlaps.
    """
    for df in (df_forecast, df_actual):
        df["time"] = pd.to_datetime(df["time"], unit="ns")
        if rounding_freq:
            df["time"] = df["time"].dt.round(rounding_freq)

    keys: list = ["time", *tags]
    forecast_rows, actual_rows = len(df_forecast), len(df_actual)
    df_forecast = df_forecast.sort_values("time").drop_duplicates(
        subset=keys, keep="first"
    )
    df_actual = df_actual.sort_values("time").drop_duplicates(subset=keys, keep="first")
    collapsed_forecast: int = forecast_rows - len(df_forecast)
    collapsed_actual: int = actual_rows - len(df_actual)
    if collapsed_forecast or collapsed_actual:
        influxdb3_local.info(
            f"[{task_id}] Collapsed {collapsed_forecast} forecast and {collapsed_actual} "
            f"actual rows sharing a rounded timestamp"
        )

    merged: pd.DataFrame = pd.merge(
        df_forecast[[*keys, "forecast"]],
        df_actual[[*keys, "actual"]],
        on=keys,
        how="inner",
    )

    matched_rows: int = len(merged)
    merged = merged.dropna(subset=["forecast", "actual"])
    incomplete_rows: int = matched_rows - len(merged)
    if incomplete_rows:
        influxdb3_local.info(
            f"[{task_id}] Skipped {incomplete_rows} matched rows without both values"
        )

    if merged.empty:
        influxdb3_local.error(f"[{task_id}] No overlapping timestamps after merge")
        return None

    influxdb3_local.info(f"[{task_id}] Merged dataset has {len(merged)} rows")
    return merged


def compute_error(
    influxdb3_local, merged: pd.DataFrame, error_metric: str, task_id: str
) -> pd.DataFrame | None:
    """
    Add a per-timestamp "error" column for the configured metric.

    Returns:
        pd.DataFrame | None: The frame with an "error" column, or None when the
            metric is undefined for every row.
    """
    if error_metric == "mape":
        usable: pd.Series = merged["actual"] != 0
    elif error_metric == "smape":
        usable = (merged["forecast"].abs() + merged["actual"].abs()) != 0
    else:
        usable = pd.Series(True, index=merged.index)

    undefined_rows: int = int((~usable).sum())
    if undefined_rows:
        influxdb3_local.warn(
            f"[{task_id}] Skipping {undefined_rows} rows where {error_metric.upper()} "
            f"is undefined because its denominator is zero"
        )
        merged = merged[usable]
    if merged.empty:
        influxdb3_local.error(
            f"[{task_id}] No rows left to evaluate {error_metric.upper()}"
        )
        return None

    merged = merged.copy()
    difference: pd.Series = merged["forecast"] - merged["actual"]
    if error_metric == "mse":
        merged["error"] = difference**2
    elif error_metric == "mape":
        merged["error"] = difference.abs() / merged["actual"].abs() * 100
    elif error_metric == "smape":
        merged["error"] = (
            200 * difference.abs() / (merged["forecast"].abs() + merged["actual"].abs())
        )
    else:
        # per timestamp the root of the squared difference is the absolute difference
        merged["error"] = difference.abs()
    return merged


def process_scheduled_call(
    influxdb3_local, call_time: datetime, args: dict | None = None
):
    """
    Scheduler trigger to evaluate forecast-error metrics and alert on elevated model error.

    Matches forecast and actual values over a rolling window, computes a per-timestamp
    error metric, and notifies for every point that reaches a configured threshold.

    Args:
        influxdb3_local: InfluxDB client for queries, caching, and logging.
        call_time (datetime): UTC timestamp at which this scheduled function runs.
        args (dict):
            Required:
              - "forecast_measurement" (str): Measurement holding forecast data.
              - "actual_measurement" (str): Measurement holding actual data.
              - "forecast_field" (str): Numeric field with forecasted values.
              - "actual_field" (str): Numeric field with actual values.
              - "error_metric" (str): One of mse, mae, rmse, mape, smape.
              - "error_thresholds" (str): Colon-separated <level>-<threshold> pairs.
              - "window" (str): Positive duration for the lookback (e.g., "1h").
              - "senders" (str): Dot-separated list of notification channels.
            Optional:
              - "min_condition_duration" (str): Time an anomaly must persist before
                   alerting (default "0s").
              - "rounding_freq" (str): Pandas frequency for timestamp rounding.
              - "max_notifications_per_run" (int): Notification cap per run (default 20).
              - "notification_text" (str): Template with variables $measurement, $level,
                   $field, $error, $metric, $tags, $timestamp.
              - "notification_path" (str): Notification plugin path (default "notify").
              - "port_override" (int): HTTP port for notification plugin (default 8181).
              - "influxdb3_auth_token" (str): API v3 token (or via INFLUXDB3_AUTH_TOKEN).
              - "config_file_path" (str): Path to a TOML config replacing the args.

    Exceptions:
        All exceptions are caught and logged via influxdb3_local.error.
    """
    task_id: str = str(uuid.uuid4())
    influxdb3_local.info(f"[{task_id}] Forecast error check started at {call_time}")

    config: dict | None = _load_config(influxdb3_local, args, task_id)
    if config is None:
        return

    try:
        forecast_measurement: str = config["forecast_measurement"]
        actual_measurement: str = config["actual_measurement"]
        tables: list = get_table_names(influxdb3_local)
        for measurement in (forecast_measurement, actual_measurement):
            if measurement not in tables:
                influxdb3_local.error(
                    f"[{task_id}] Measurement '{measurement}' not found"
                )
                return

        forecast_field: str = config["forecast_field"]
        actual_field: str = config["actual_field"]
        error_metric: str = config["error_metric"]

        error_thresholds: dict = parse_error_thresholds(
            influxdb3_local, config, task_id
        )
        if not error_thresholds:
            influxdb3_local.error(f"[{task_id}] No valid error thresholds configured")
            return

        window: timedelta = config["window"]
        rounding_freq: str = config["rounding_freq"]
        min_condition_duration: timedelta = config["min_condition_duration"]
        if min_condition_duration >= window:
            influxdb3_local.warn(
                f"[{task_id}] min_condition_duration={min_condition_duration} is not shorter "
                f"than window={window}, an anomaly can never persist long enough to alert"
            )

        max_notifications_per_run: int = config["max_notifications_per_run"]
        senders_config: dict = parse_senders(influxdb3_local, config, task_id)
        notification_path: str = config["notification_path"]
        notification_text: str = config["notification_text"]
        port_override: int = config["port_override"]
        influxdb3_auth_token: str = (
            config.get("influxdb3_auth_token")
            or os.getenv("INFLUXDB3_AUTH_TOKEN")
            or ""
        )
        if not influxdb3_auth_token:
            influxdb3_local.error(f"[{task_id}] Missing influxdb3_auth_token")
            return

        tags: list = resolve_shared_tags(
            influxdb3_local, forecast_measurement, actual_measurement, task_id
        )
        influxdb3_local.info(
            f"[{task_id}] Configuration completed: {forecast_measurement}.{forecast_field} "
            f"vs {actual_measurement}.{actual_field}, metric={error_metric}, window={window}, "
            f"thresholds={error_thresholds}, tags={tags}, senders={list(senders_config)}"
        )

        end_time: datetime = (
            call_time if call_time.tzinfo else call_time.replace(tzinfo=timezone.utc)
        )
        start_time: datetime = end_time - window
        df_forecast: pd.DataFrame | None = query_series(
            influxdb3_local,
            forecast_measurement,
            forecast_field,
            tags,
            start_time,
            end_time,
            task_id,
        )
        if df_forecast is None:
            return
        df_actual: pd.DataFrame | None = query_series(
            influxdb3_local,
            actual_measurement,
            actual_field,
            tags,
            start_time,
            end_time,
            task_id,
        )
        if df_actual is None:
            return

        merged: pd.DataFrame | None = align_frames(
            influxdb3_local,
            df_forecast.rename(columns={forecast_field: "forecast"}),
            df_actual.rename(columns={actual_field: "actual"}),
            tags,
            rounding_freq,
            task_id,
        )
        if merged is None:
            return

        merged = compute_error(influxdb3_local, merged, error_metric, task_id)
        if merged is None:
            return

        errors: pd.Series = merged["error"]
        influxdb3_local.info(
            f"[{task_id}] {error_metric.upper()} over {len(merged)} points - "
            f"mean: {errors.mean():.4f}, median: {errors.median():.4f}, "
            f"min: {errors.min():.4f}, max: {errors.max():.4f}"
        )

        merged = merged.sort_values("time")
        window_start: pd.Timestamp = (
            pd.Timestamp(start_time).tz_convert("UTC").tz_localize(None)
        )
        sent_notifications: int = 0
        failed_notifications: int = 0
        suppressed_notifications: int = 0

        # severe levels first so the notification cap is not spent on the lowest one
        for threshold_level, error_threshold in sorted(
            error_thresholds.items(), key=lambda item: item[1], reverse=True
        ):
            merged["is_outlier"] = merged["error"] >= error_threshold
            influxdb3_local.info(
                f"[{task_id}] {threshold_level} threshold {error_threshold}: "
                f"{int(merged['is_outlier'].sum())}/{len(merged)} points reach it"
            )

            for _, row in merged.iterrows():
                row_time: pd.Timestamp = row["time"]
                cache_key: str = generate_cache_key(
                    actual_measurement, actual_field, threshold_level, tags, row
                )
                alert_key: str = f"{cache_key}:last_alert"
                tag_str: str = format_tags(row, tags)
                series_label: str = (
                    f"{actual_measurement}.{actual_field} (tags: {tag_str})"
                )

                last_alert: str = influxdb3_local.cache.get(alert_key, default="")
                if last_alert and row_time <= pd.Timestamp(last_alert):
                    continue

                pending_since: str = influxdb3_local.cache.get(cache_key, default="")
                if pending_since and pd.Timestamp(pending_since) < window_start:
                    influxdb3_local.cache.delete(cache_key)
                    pending_since = ""
                alert_reason: str | None = None

                if row["is_outlier"]:
                    if not pending_since:
                        if min_condition_duration > timedelta(0):
                            influxdb3_local.cache.put(cache_key, row_time.isoformat())
                            influxdb3_local.info(
                                f"[{task_id}] {threshold_level} error started in {series_label}, "
                                f"waiting for {min_condition_duration}"
                            )
                            continue
                        alert_reason = (
                            f"{threshold_level} alert triggered - {error_metric.upper()}: "
                            f"{row['error']:.4f} (threshold: {error_threshold}) for {series_label}"
                        )
                    else:
                        elapsed: timedelta = (
                            row_time - pd.Timestamp(pending_since)
                        ).to_pytimedelta()
                        if elapsed < min_condition_duration:
                            influxdb3_local.info(
                                f"[{task_id}] {threshold_level} error in {series_label} lasted "
                                f"{elapsed} < {min_condition_duration}, deferring alert"
                            )
                            continue
                        alert_reason = (
                            f"{threshold_level} alert triggered after {elapsed} - "
                            f"{error_metric.upper()}: {row['error']:.4f} "
                            f"(threshold: {error_threshold}) for {series_label}"
                        )
                elif pending_since:
                    influxdb3_local.cache.delete(cache_key)
                    influxdb3_local.info(
                        f"[{task_id}] {threshold_level} error cleared in {series_label}"
                    )

                if alert_reason is None:
                    continue

                if (
                    sent_notifications + failed_notifications
                    >= max_notifications_per_run
                ):
                    suppressed_notifications += 1
                else:
                    payload: dict = {
                        "notification_text": interpolate_notification_text(
                            notification_text,
                            {
                                "level": threshold_level,
                                "measurement": actual_measurement,
                                "field": actual_field,
                                "error": row["error"],
                                "metric": error_metric,
                                "tags": tag_str,
                                "timestamp": row_time.isoformat(),
                            },
                        ),
                        "senders_config": senders_config,
                    }
                    influxdb3_local.error(f"[{task_id}] {alert_reason}")
                    delivered: bool = send_notification(
                        influxdb3_local,
                        port_override,
                        notification_path,
                        influxdb3_auth_token,
                        payload,
                        task_id,
                    )
                    if not delivered:
                        # leave the state untouched so a later run can alert again
                        failed_notifications += 1
                        continue
                    sent_notifications += 1

                influxdb3_local.cache.delete(cache_key)
                influxdb3_local.cache.put(alert_key, row_time.isoformat())

        if failed_notifications:
            influxdb3_local.warn(
                f"[{task_id}] {failed_notifications} notifications could not be delivered, "
                f"the next run will alert on them again"
            )
        if suppressed_notifications:
            influxdb3_local.warn(
                f"[{task_id}] Suppressed {suppressed_notifications} notifications after "
                f"reaching max_notifications_per_run={max_notifications_per_run}"
            )
        influxdb3_local.info(
            f"[{task_id}] Forecast error check completed: {sent_notifications} notifications sent"
        )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Unexpected error: {e}")
