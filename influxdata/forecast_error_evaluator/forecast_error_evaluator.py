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
            "description": "The error metric to use (mse, mae, rmse).",
            "required": true
        },
        {
            "name": "error_thresholds",
            "example": "INFO-'0.5':WARN-'0.9':ERROR-'1.2':CRITICAL-'1.5'",
            "description": "The threshold value for the error metric to trigger an anomaly with levels.",
            "required": true
        },
        {
            "name": "window",
            "example": "1h",
            "description": "Time window for data analysis (e.g., '1h' for 1 hour). Units: 's', 'min', 'h', 'd', 'w'.",
            "required": true
        },
        {
            "name": "senders",
            "example": "slack",
            "description": "Dot-separated list of notification channels (e.g., slack.discord).",
            "required": true
        },
        {
            "name": "influxdb3_auth_token",
            "example": "YOUR_API_TOKEN",
            "description": "API token for InfluxDB 3. Can be set via INFLUXDB3_AUTH_TOKEN environment variable.",
            "required": false
        },
        {
            "name": "min_condition_duration",
            "example": "5min",
            "description": "Minimum duration for an anomaly condition to persist before triggering a notification (e.g., '5m'). Units: 's', 'min', 'h', 'd', 'w'.",
            "required": false
        },
        {
            "name": "rounding_freq",
            "example": "1s",
            "description": "Frequency to round timestamps for alignment (e.g., '1s').",
            "required": false
        },
        {
            "name": "notification_text",
            "example": "[$level] Forecast error alert in $measurement.$field: $metric=$error. Tags: $tags",
            "description": "Template for notification message with variables $measurement, $level, $field, $error, $metric, $tags.",
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
            "description": "Optional headers as base64-encoded string for HTTP notifications.",
            "required": false
        },
        {
            "name": "discord_webhook_url",
            "example": "https://discord.com/api/webhooks/...",
            "description": "Webhook URL for Discord notifications. Required if using discord sender.",
            "required": false
        },
        {
            "name": "slack_headers",
            "example": "eyJDb250ZW50LVR5cGUiOiAiYXBwbGljYXRpb24vanNvbiJ9",
            "description": "Optional headers as base64-encoded string for HTTP notifications.",
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
import tomllib
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from string import Template
from urllib.parse import urlparse

import pandas as pd
import requests

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


def validate_webhook_url(influxdb3_local, service: str, url: str, task_id: str) -> bool:
    """
    Validate webhook URL format.

    Args:
        influxdb3_local: InfluxDB client instance.
        service (str): Type of service (e.g., "slack", "telegram", etc.).
        url (str): Webhook URL to validate.
        task_id (str): Unique task identifier.

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


def parse_senders(influxdb3_local, args: dict, task_id: str) -> dict:
    """
    Parse and validate sender configurations from input arguments.

    Args:
        influxdb3_local: InfluxDB client instance.
        args (dict): Input arguments containing:
            - "senders": dot-separated list of sender types (e.g., "slack.http").
            - For each sender, its own required keys (see AVAILABLE_SENDERS).
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

    senders: str | list = args.get("senders")
    if args["use_config_file"]:
        if not isinstance(senders, list):
            raise Exception(
                f"[{task_id}] 'senders' must be a list when using config file"
            )
    else:
        senders = senders.split(".")

    for sender in senders:
        if sender not in AVAILABLE_SENDERS:
            influxdb3_local.warn(f"[{task_id}] Invalid sender type: {sender}")
            continue
        for key in AVAILABLE_SENDERS[sender]:
            if key not in args and not any(ex in key for ex in EXCLUDED_KEYWORDS):
                influxdb3_local.warn(
                    f"[{task_id}] Required key '{key}' missing for sender '{sender}'"
                )
                senders_config.pop(sender, None)
                break
            if "url" in key and not validate_webhook_url(
                influxdb3_local, sender, args[key], task_id
            ):
                senders_config.pop(sender, None)
                break

            if key not in args:
                continue
            senders_config[sender][key] = args[key]

    if not senders_config:
        raise Exception(f"[{task_id}] No valid senders configured")
    return senders_config


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

    Raises:
        requests.RequestException: If all retries fail or a non-2xx response is received.
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
            break
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


def parse_port_override(args: dict, task_id: str) -> int:
    """
    Parse and validate 'port_override' from args (default 8181).

    Args:
        args (dict): Runtime argument's dict.
        task_id (str): Unique task identifier.

    Returns:
        int: Validated port number between 1 and 65535.

    Raises:
        Exception if invalid or out of range.
    """
    raw: str | int = args.get("port_override", 8181)
    try:
        port = int(raw)
    except (ValueError, TypeError):
        raise Exception(f"[{task_id}] 'port_override' must be an integer, got '{raw}'")
    if not (1 <= port <= 65535):
        raise Exception(
            f"[{task_id}] 'port_override' {port} is out of valid range 1–65535"
        )
    return port


def parse_time_duration(raw: str, task_id: str) -> timedelta:
    """
    Convert a duration string (e.g., "5m", "2h") into a timedelta.

    Args:
        raw (str): Duration with unit suffix.
        task_id (str): Unique task identifier (for errors).

    Returns:
        timedelta.

    Raises:
        Exception if format is invalid or number conversion fails.
    """
    units: dict = {
        "s": "seconds",
        "min": "minutes",
        "h": "hours",
        "d": "days",
        "w": "weeks",
    }
    num_part, unit_part = "", ""
    for u in sorted(units.keys(), key=len, reverse=True):
        if raw.endswith(u):
            num_part = raw[: -len(u)]
            unit_part = u
            break
    if not num_part or unit_part not in units:
        raise Exception(f"[{task_id}] Invalid duration '{raw}'")
    try:
        val = int(num_part)
    except ValueError:
        raise Exception(f"[{task_id}] Invalid number in duration '{raw}'")
    return timedelta(**{units[unit_part]: val})


def get_all_measurements(influxdb3_local) -> list[str]:
    """
    Retrieves a list of all tables of type 'BASE TABLE' from the current InfluxDB database.

    Args:
        influxdb3_local: InfluxDB client instance.

    Returns:
        list[str]: List of table names (e.g., ["cpu", "memory", "disk"]).
    """
    result: list = influxdb3_local.query("SHOW TABLES")
    return [
        row["table_name"] for row in result if row.get("table_type") == "BASE TABLE"
    ]


def parse_float(float_str_val: str, task_id: str) -> float:
    if float_str_val[0] == float_str_val[-1] and float_str_val[0] in ("'", '"'):
        float_val: str = float_str_val[1:-1]
    else:
        float_val = float_str_val

    try:
        return float(float_val)
    except ValueError:
        raise Exception(f"[{task_id}] Invalid float value: '{float_val}'")


def get_tag_names(influxdb3_local, measurement: str, task_id: str) -> list[str]:
    """
    Retrieve all tag column names (type Dictionary(Int32, Utf8)) for a measurement.

    Args:
        influxdb3_local: InfluxDB client instance.
        measurement (str): Name of the measurement to inspect.
        task_id (str): Unique task identifier.

    Returns:
        List of tag names (strings). Empty list if none found.
    """
    query: str = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $measurement
          AND data_type = 'Dictionary(Int32, Utf8)'
    """
    res: list = influxdb3_local.query(query, {"measurement": measurement})
    if not res:
        influxdb3_local.info(f"[{task_id}] No tags found for '{measurement}'")
        return []
    return [row["column_name"] for row in res]


def generate_cache_key(
    measurement: str, field: str, threshold_level: str, tags: list[str], row: pd.Series
) -> str:
    """
    Build a stable cache key string, ignoring timestamps, for debounce logic.

    Args:
        measurement (str): Measurement name.
        field (str): Field name under test.
        threshold_level (str): One of "INFO", "WARN", "ERROR".
        tags (list[str]): Tag column names to include.
        row (pd.Series): A row of data (from pandas DataFrame), used to pull tag values.

    Returns:
        str: Key like "cpu:temp:host=server1:region=us-west"
    """
    key: str = f"{measurement}:{field}:{threshold_level}"
    for tag in sorted(tags):
        tag_val: str = row.get(tag, "None")
        key += f":{tag}={tag_val}"
    return key


def parse_error_thresholds(
    influxdb3_local, args: dict, task_id: str
) -> dict[str, float]:
    """
    Parses a string of error thresholds into a dictionary mapping severity
        levels to threshold values or uses values from a config file.

    Args:
        influxdb3_local: An InfluxDB 3 logger instance for logging warnings.
        args (dict): A dictionary containing the `error_thresholds` key with a string value
            specifying thresholds in the format "level-threshold:level-threshold" (e.g., "INFO-10:WARN-20.5").
        task_id (str): A unique identifier for the task, included in warning and error messages.

    Returns:
        dict[str, float]: A dictionary mapping severity levels (e.g., "INFO", "WARN") to their
            corresponding threshold values as floats.

    Raises:
        Exception: If no valid thresholds are parsed from the input string, with a message
            including the `task_id` and a description of the error.

    Example:
        args = {"error_thresholds": "INFO-10:WARN-'20.5':ERROR-30"}
        parse_error_thresholds(influxdb3_local, args, "task123")
        {'INFO': 10.0, 'WARN': 20.5, 'ERROR': 30.0}
    """
    threshold_input: str | dict = args["error_thresholds"]
    thresholds: dict = {}

    if args["use_config_file"]:
        if isinstance(threshold_input, dict):
            return threshold_input
        else:
            raise Exception(
                f"[{task_id}] Invalid format of error_threshold, expected a dictionary, got '{type(threshold_input)}'"
            )

    parts = threshold_input.split(":")
    for part in parts:
        if not "-" in part:
            influxdb3_local.warn(
                f"[{task_id}] Invalid format of error_threshold, part {part} should be of form <level>-<threshold>"
            )
            continue
        level, thresh_str = part.split("-")
        if level not in ["INFO", "WARN", "ERROR", "CRITICAL"]:
            influxdb3_local.warn(
                f"[{task_id}] Invalid format of error_threshold, {level} is not a valid level"
            )
            continue
        if thresh_str[0] == thresh_str[-1] and thresh_str[0] in ("'", '"'):
            thresh_str = thresh_str[1:-1]
        try:
            thresholds[level] = float(thresh_str)
        except ValueError:
            influxdb3_local.warn(
                f"[{task_id}] Invalid format of error_threshold, part {part} should be of form <level>-<int/float>"
            )
            continue

    if not thresholds:
        raise Exception(
            f"[{task_id}] Invalid format of error_threshold, no valid thresholds found"
        )

    return thresholds


def generate_query(
    measurement: str,
    field: str,
    tags: list[str],
    start_time: datetime,
    end_time: datetime,
) -> str:
    """
    Generate an InfluxDB query to select time, a specified field, and optional tags from a measurement within a time range.

    Args:
        measurement (str): Name of the InfluxDB measurement (table).
        field (str): Name of the field to query.
        start_time (datetime): Start of the time range (inclusive).
        end_time (datetime): End of the time range (exclusive).
        tags (list[str], optional): List of tag names to include in the SELECT clause. Defaults to None.

    Returns:
        str: Formatted InfluxDB query string.
    """
    # If tags is None or empty, default to selecting only time and field
    select_clause = f'time, "{field}"'
    if tags:
        # Add tags to the SELECT clause, ensuring proper escaping
        select_clause += ", " + ", ".join([f"{tag}" for tag in tags])

    return f"SELECT {select_clause} FROM '{measurement}' WHERE time >= '{start_time.isoformat()}' AND time < '{end_time.isoformat()}' ORDER BY time"


def process_scheduled_call(
    influxdb3_local, call_time: datetime, args: dict | None = None
):
    """
    Scheduler trigger to evaluate forecast‐error metrics and alert on elevated model error.

    Compares two time series (forecast vs. actual) over a rolling window, computes a per‐timestamp
    error metric (MSE/MAE/RMSE), and triggers notifications if any computed error exceeds a threshold.

    Args:
        influxdb3_local: InfluxDB client for queries, caching, and logging.
        call_time (datetime): UTC timestamp at which this scheduled function runs.
        args (dict):
            Required:
              - "forecast_measurement" (str): Measurement name where to get forecast data.
              - "actual_measurement" (str): Measurement name where to get actual data.
              - "forecast_field" (str): Column name in forecast results (numeric).
              - "actual_field" (str): Column name in actual results (numeric).
              - "error_metric" (str): One of ["mse", "mae", "rmse"].
              - "error_thresholds" (str): Colon-separated list of <level>-<threshold> pairs.
              - "window" (str): Duration string for lookback (e.g., "1h", "30m").
              - "senders" (str): Dot-separated list of notification channels.
            Optional:
              - "min_condition_duration" (str): If provided (e.g., "5m"), anomaly must persist
                   for at least that duration before alerting.
              - "rounding_freq" (str): If provided (e.g., "1s"), timestamps are rounded to nearest
                   multiple of this duration.
              - "notification_text" (str): Template for alert, with variables:
                   $measurement, $field, $timestamp, $error, $metric, $tags.
                   Defaults to "Forecast error alert in $measurement.$field at $timestamp: $metric=$error. Tags: $tags".
              - "notification_path" (str): Notification plugin path (default "notify").
              - "port_override" (int): HTTP port for notification plugin (default 8181).
              - "influxdb3_auth_token" (str): API v3 token (or via ENV var INFLUXDB3_AUTH_TOKEN).
              - "config_file_path": (str), path to config file to override args.

    Exceptions:
        All exceptions are caught and logged via influxdb3_local.error.
    """
    task_id: str = str(uuid.uuid4())
    influxdb3_local.info(f"[{task_id}] Forecast error check started at {call_time} with args: {args}")

    # Override args with config file if specified
    if args:
        if path := args.get("config_file_path", None):
            try:
                plugin_dir_var: str | None = os.getenv("PLUGIN_DIR", None)
                if not plugin_dir_var:
                    influxdb3_local.error(
                        f"[{task_id}] Failed to get PLUGIN_DIR env var"
                    )
                    return
                plugin_dir: Path = Path(plugin_dir_var)
                file_path = plugin_dir / path
                influxdb3_local.info(f"[{task_id}] Reading config file {file_path}")
                with open(file_path, "rb") as f:
                    args = tomllib.load(f)
                    args["use_config_file"] = True
                influxdb3_local.info(f"[{task_id}] New args content: {args}")
            except Exception:
                influxdb3_local.error(f"[{task_id}] Failed to read config file")
                return
        else:
            args["use_config_file"] = False

    # Validate required arguments
    required_keys: list = [
        "forecast_measurement",
        "actual_measurement",
        "forecast_field",
        "actual_field",
        "error_metric",
        "error_thresholds",
        "window",
        "senders",
    ]
    if not args or any(key not in args for key in required_keys):
        influxdb3_local.error(
            f"[{task_id}] Missing some of the required arguments: {', '.join(required_keys)}"
        )
        return

    try:
        # Parse common config
        actual_measurement: str = args.get("actual_measurement")
        forecast_measurement: str = args.get("forecast_measurement")
        forecast_field: str = args["forecast_field"]
        actual_field: str = args["actual_field"]
        error_metric: str = args["error_metric"].lower()
        if error_metric not in ("mse", "mae", "rmse"):
            influxdb3_local.error(
                f"[{task_id}] Unsupported error_metric '{error_metric}'; use mse|mae|rmse"
            )
            return

        error_thresholds: dict = parse_error_thresholds(influxdb3_local, args, task_id)
        window_td: timedelta = parse_time_duration(args["window"], task_id)
        rounding_freq: str | None = args.get("rounding_freq", None)

        # Notification & sender config
        senders_config: dict = parse_senders(influxdb3_local, args, task_id)
        tags: list = get_tag_names(influxdb3_local, actual_measurement, task_id)
        port_override: int = parse_port_override(args, task_id)
        notification_path: str = args.get("notification_path", "notify")
        notification_tpl: str = args.get(
            "notification_text",
            "[$level] Forecast error alert in $measurement.$field: $metric=$error. Tags: $tags",
        )
        min_condition_duration: timedelta = parse_time_duration(
            args.get("min_condition_duration", "0s"), task_id
        )
        influxdb3_auth_token: str | None = args.get("influxdb3_auth_token") or os.getenv(
            "INFLUXDB3_AUTH_TOKEN"
        )
        if not influxdb3_auth_token:
            influxdb3_local.error(f"[{task_id}] Missing influxdb3_auth_token")
            return

        # Determine time window
        end_time: datetime = call_time.replace(tzinfo=timezone.utc)
        start_time: datetime = end_time - window_td
        influxdb3_local.info(f"[{task_id}] Querying data from {start_time} to {end_time}")

        # Execute forecast query
        forecast_query: str = generate_query(
            forecast_measurement, forecast_field, tags, start_time, end_time
        )
        forecast_results: list = influxdb3_local.query(forecast_query)
        if not forecast_results:
            influxdb3_local.info(
                f"[{task_id}] No forecast data returned from {start_time} to {end_time}"
            )
            return

        # Execute actual query
        actual_query: str = generate_query(
            actual_measurement, actual_field, tags, start_time, end_time
        )
        actual_results: list = influxdb3_local.query(actual_query)
        if not actual_results:
            influxdb3_local.info(
                f"[{task_id}] No actual data returned from {start_time} to {end_time}"
            )
            return

        # Load into DataFrames
        df_fore: pd.DataFrame = pd.DataFrame(forecast_results)
        df_act: pd.DataFrame = pd.DataFrame(actual_results)

        if "time" not in df_fore.columns or forecast_field not in df_fore.columns:
            influxdb3_local.error(
                f"[{task_id}] forecast_query results missing 'time' or '{forecast_field}'"
            )
            return
        if "time" not in df_act.columns or actual_field not in df_act.columns:
            influxdb3_local.error(
                f"[{task_id}] actual_query results missing 'time' or '{actual_field}'"
            )
            return

        # Extract tags columns to save their values
        tag_columns: list = [
            col for col in df_act.columns if col not in {"time", actual_field}
        ]

        # Rename fields
        df_fore = df_fore.rename(columns={forecast_field: "forecast"})
        df_act = df_act.rename(columns={actual_field: "actual"})

        # Parse timestamps and round to nearest second for alignment
        if rounding_freq is None:
            df_fore["time"] = pd.to_datetime(df_fore["time"], unit="ns")
            df_act["time"] = pd.to_datetime(df_act["time"], unit="ns")
        else:
            influxdb3_local.info(f"[{task_id}] Converting timestamps with rounding={rounding_freq}")
            try:
                df_fore["time"] = pd.to_datetime(df_fore["time"], unit="ns").dt.round(
                    rounding_freq
                )
                df_act["time"] = pd.to_datetime(df_act["time"], unit="ns").dt.round(
                    rounding_freq
                )
            except Exception as e:
                influxdb3_local.error(
                    f"[{task_id}] Error rounding timestamps: {str(e)}"
                )
                return

        merge_columns: list = ["time"] + tag_columns
        fore_columns: list = merge_columns + ["forecast"]
        act_columns: list = merge_columns + ["actual"]

        # Merge on time (inner join)
        merged: pd.DataFrame = pd.merge(
            df_fore[fore_columns],
            df_act[act_columns],
            on=merge_columns,
            how="inner",
        )
        if merged.empty:
            influxdb3_local.error(f"[{task_id}] No overlapping timestamps after merge")
            return
        influxdb3_local.info(f"[{task_id}] Merged dataset has {len(merged)} rows")

        # Compute error per row
        if error_metric == "mse":
            merged["error"] = (merged["forecast"] - merged["actual"]) ** 2
        elif error_metric == "mae":
            merged["error"] = (merged["forecast"] - merged["actual"]).abs()
        else:  # rmse
            merged["error"] = ((merged["forecast"] - merged["actual"]) ** 2) ** 0.5

        for threshold_level, error_threshold in error_thresholds.items():
            merged["is_outlier"] = merged["error"] >= error_threshold
            merged = merged.sort_values("time")  # Sort by time
            for _, row in merged.iterrows():
                row_time: datetime = row["time"]
                is_outlier: bool = row["is_outlier"]
                cache_key: str = generate_cache_key(
                    actual_measurement, actual_field, threshold_level, tags, row
                )
                tag_str: str = ", ".join(f"{t}={row.get(t, 'None')}" for t in tags)

                if is_outlier:
                    start_iso: str = influxdb3_local.cache.get(cache_key, default="")
                    if not start_iso:
                        influxdb3_local.cache.put(cache_key, row_time.isoformat())
                        if min_condition_duration > timedelta(0):
                            influxdb3_local.info(
                                f"[{task_id}] Error threshold exceeded in {actual_measurement}.{actual_field} for row {cache_key}, but waiting for {min_condition_duration}"
                            )
                            continue
                    else:
                        start_dt: datetime = datetime.fromisoformat(start_iso)
                        elapsed: timedelta = row_time - start_dt
                        if elapsed >= min_condition_duration:
                            payload: dict = {
                                "notification_text": interpolate_notification_text(
                                    notification_tpl,
                                    {
                                        "level": threshold_level,
                                        "measurement": actual_measurement,
                                        "field": actual_field,
                                        "error": row["error"],
                                        "metric": error_metric,
                                        "tags": tag_str,
                                    },
                                ),
                                "senders_config": senders_config,
                            }
                            influxdb3_local.error(
                                f"[{task_id}] Error threshold exceeded ({error_metric}: {row['error']}) in {actual_measurement}.{actual_field} for row {cache_key}"
                            )
                            send_notification(
                                influxdb3_local,
                                port_override,
                                notification_path,
                                influxdb3_auth_token,
                                payload,
                                task_id,
                            )
                            influxdb3_local.cache.put(cache_key, "")
                        else:
                            influxdb3_local.info(
                                f"[{task_id}] Error above threshold in {actual_measurement}.{actual_field} for row {cache_key}, duration {elapsed} < {min_condition_duration}, deferring alert"
                            )
                else:
                    influxdb3_local.cache.put(cache_key, "")

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Unexpected error: {e}")
