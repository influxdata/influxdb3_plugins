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
            "description": "Historical window duration for training data. Format: <number><unit> where unit is s, min, h, d, w, m, q, y.",
            "required": true
        },
        {
            "name": "forecast_horizont",
            "example": "2d",
            "description": "Future duration to forecast. Format: <number><unit> where unit is s, min, h, d, w, m, q, y.",
            "required": true
        },
        {
            "name": "tag_values",
            "example": "region:us-west.device:sensor1",
            "description": "Dot-separated tag filter string for querying specific tag values.",
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
            "description": "Mode of operation: 'train' to train a new model, 'predict' to use an existing one or train if not found.",
            "required": true
        },
        {
            "name": "unique_suffix",
            "example": "20250619_v1",
            "description": "Unique identifier for model versioning and storage.",
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
            "description": "Flexibility of trend changepoints. Defaults to 0.05.",
            "required": false
        },
        {
            "name": "changepoints",
            "example": "2025-01-01 2025-06-01",
            "description": "Space-separated list of changepoint dates (ISO format).",
            "required": false
        },
        {
            "name": "holiday_date_list",
            "example": "2025-01-01 2025-12-25",
            "description": "Space-separated list of custom holiday dates (ISO format).",
            "required": false
        },
        {
            "name": "holiday_names",
            "example": "New Year.Christmas",
            "description": "Dot-separated list of names corresponding to the holiday dates.",
            "required": false
        },
        {
            "name": "holiday_country_names",
            "example": "US.UK",
            "description": "Dot-separated list of country codes for built-in holidays.",
            "required": false
        },
        {
            "name": "inferred_freq",
            "example": "1D",
            "description": "Manually specified frequency (e.g., '1D', '1H'). If not provided, frequency is inferred from data.",
            "required": false
        },
        {
            "name": "validation_window",
            "example": "3d",
            "description": "Duration for validation window. Defaults to '0s' (no validation). Format: <number><unit>.",
            "required": false
        },
        {
            "name": "msre_threshold",
            "example": "0.05",
            "description": "Maximum acceptable Mean Squared Relative Error (MSRE) for validation. Defaults to infinity (no threshold).",
            "required": false
        },
        {
            "name": "target_database",
            "example": "forecast_db",
            "description": "Optional InfluxDB database name for writing forecast results.",
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
            "description": "Templated text for alert message. Variables like $version, $measurement can be used.",
            "required": false
        },
        {
            "name": "senders",
            "example": "slack",
            "description": "Dot-separated list of sender types (e.g., 'slack.sms').",
            "required": false
        },
        {
            "name": "notification_path",
            "example": "notify",
            "description": "URL path for posting the alert. Defaults to 'notify'.",
            "required": false
        },
        {
            "name": "influxdb3_auth_token",
            "example": "your_token",
            "description": "Authentication token for sending notifications. If not provided, uses INFLUXDB3_AUTH_TOKEN environment variable.",
            "required": false
        },
        {
            "name": "port_override",
            "example": "8182",
            "description": "Optional custom port for notification dispatch (1–65535). Defaults to 8181.",
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
import re
import time
import tomllib
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from string import Template
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import requests
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


def parse_time_interval(raw: str, task_id: str) -> timedelta:
    """
    Parses the interval string from raw into a datetime.timedelta.

    Supports time units:
      - seconds: "s"
      - minutes: "min"
      - hours: "h"
      - days: "d"
      - weeks: "w"
      - months: "m"      (approximate: 1 month ≈ 30.42 days)
      - quarters: "q"    (approximate: 1 quarter ≈ 91.25 days)
      - years: "y"       (approximate: 1 year = 365 days)

    Args:
        raw (str): The interval string to be parsed.
        task_id (str): Task identifier for logging context.

    Returns:
        timedelta: The parsed duration as a datetime.timedelta.

    Raises:
        Exception: If the format is invalid, unit unsupported.
    """
    unit_mapping: dict = {
        "s": "seconds",
        "min": "minutes",
        "h": "hours",
        "d": "days",
        "w": "weeks",
        "m": "days",  # months -> days
        "q": "days",  # quarters -> days
        "y": "days",  # years -> days
    }
    # Approximate conversions to days for month, quarter, year
    day_conversions: dict = {
        "m": 30.42,  # average days in a month
        "q": 91.25,  # average days in a quarter
        "y": 365.0,  # days in a year
    }
    valid_units = unit_mapping.keys()

    if not isinstance(raw, str):
        raise Exception(
            f"[{task_id}] Invalid {raw} type: expected string like '10min', got {type(raw)}"
        )

    match = re.fullmatch(r"(\d+)([a-zA-Z]+)", raw.strip())
    if not match:
        raise Exception(
            f"[{task_id}] Invalid raw format: '{raw}'. Expected format '<number><unit>', e.g. '10min', '2d'."
        )

    number_part, unit = match.groups()
    try:
        magnitude = int(number_part)
    except ValueError:
        raise Exception(f"[{task_id}] Invalid number in {raw}: '{number_part}'")

    unit = unit.lower()
    if unit not in valid_units:
        raise Exception(f"[{task_id}] Unsupported unit '{unit}' in raw: '{raw}'")

    # Build timedelta
    if unit in day_conversions:
        # months, quarters, years -> approximate days
        days_approx = int(magnitude * day_conversions[unit])
        if days_approx < 1:
            raise Exception(
                f"[{task_id}] Computed days < 1 for {magnitude}{unit} in raw"
            )
        return timedelta(days=days_approx)

    # For other units, use direct timedelta arguments
    if unit == "s":
        return timedelta(seconds=magnitude)
    elif unit == "min":
        return timedelta(minutes=magnitude)
    elif unit == "h":
        return timedelta(hours=magnitude)
    elif unit == "d":
        return timedelta(days=magnitude)
    elif unit == "w":
        return timedelta(weeks=magnitude)


def generate_tag_filter_clause(tag_values: dict):
    """
    Generates the WHERE clause for filtering by tag values.

    Args:
        tag_values (dict): Dictionary mapping tag names to values, or None.

    Returns:
        str: SQL WHERE clause string for tag filters, or empty string if tag_values is None.
    """
    if not tag_values:
        return ""

    sql_clause: str = ""
    for key, value in tag_values.items():
        sql_clause += f"AND\n\t\"{key}\" = '{value}'\n"
    return sql_clause


def generate_query(
    measurement: str,
    field: str,
    tag_values: dict,
    start_time: datetime,
    end_time: datetime,
) -> str:
    """Generate an SQL query to fetch data from InfluxDB."""
    tag_filter_clause: str = generate_tag_filter_clause(tag_values)

    return f"""
        SELECT time AS ds, "{field}" AS y
        FROM {measurement}
        WHERE time >= '{start_time.isoformat()}'
          AND time < '{end_time.isoformat()}'
          {tag_filter_clause}
        ORDER BY time
    """


def transform_to_influx_line(
    data: list[dict],
    measurement: str,
    fields_list: list[tuple[str, str]],
    tag_values: dict,
) -> list[LineBuilder]:
    """
    Transforms data into LineBuilder objects for writing to InfluxDB.

    Args:
        data (list[dict]): List of data rows as dictionaries.
        measurement (str): Name of the target measurement.
        fields_list (list[tuple[str, str]]): List of tuples containing field names and aggregation functions.
        tag_values (dict): Dictionary mapping tag names to values.

    Returns:
        list[LineBuilder]: List of LineBuilder objects ready for writing to InfluxDB.
    """
    builders: list = []

    for row in data:
        builder = LineBuilder(measurement)
        timestamp: int = row["time"]
        builder.time_ns(timestamp)
        builder.tag("model_version", row["model_version"])
        for tag, value in tag_values.items():
            builder.tag(tag, value)

        for field_name in fields_list:
            value = row[field_name]
            if isinstance(value, int):
                builder.int64_field(field_name, value)
            elif isinstance(value, float):
                builder.float64_field(field_name, value)
            else:
                builder.string_field(field_name, str(value))

        builders.append(builder)

    return builders


def write_downsampled_data(
    influxdb3_local,
    data: list,
    max_retries: int,
    target_measurement: str,
    target_database: str | None,
    task_id: str,
):
    """
    Writes downsampled data to the target measurement with retry logic.

    Args:
        influxdb3_local: InfluxDB client instance.
        data (list): List of LineBuilder objects to write.
        max_retries (int): Maximum number of retry attempts for write operations.
        target_measurement (str): Name of the target measurement.
        target_database (str | None): Target database name, or None to use the default database.
        task_id (str): The task ID.

    Returns:
        tuple[bool, str | None, int]: Tuple containing success status, error message (if any), and number of retries.
    """
    retry_count: int = 0
    record_count: int = len(data)
    db_name: str = target_database if target_database else "default"
    log_data: dict = {
        "records": record_count,
        "database": db_name,
        "measurement": target_measurement,
        "max_retries": max_retries,
    }
    influxdb3_local.info(f"[{task_id}] Preparing to write data", log_data)
    try:
        for tries in range(max_retries):
            try:
                for row in data:
                    influxdb3_local.write_to_db(db_name, row)
                success_log: dict = {
                    "records_written": record_count,
                    "database": db_name,
                    "measurement": target_measurement,
                    "retries": retry_count,
                }
                influxdb3_local.info(
                    f"[{task_id}] Successful write to {target_measurement}", success_log
                )
                return True, None, retry_count
            except Exception as e:
                retry_count += 1
                retry_log: dict = {
                    "attempt": tries + 1,
                    "max_retries": max_retries,
                    "records": record_count,
                    "database": db_name,
                    "error": str(e),
                }
                influxdb3_local.warn(
                    f"[{task_id}] Error write attempt {tries + 1}", retry_log
                )
                wait_time: float = (2**tries) + random.random()
                time.sleep(wait_time)
                if tries == max_retries - 1:
                    raise
    except Exception as e:
        failure_log: dict = {
            "records": record_count,
            "database": db_name,
            "measurement": target_measurement,
            "retries": retry_count,
            "error": str(e),
        }
        influxdb3_local.error(f"[{task_id}] Write failed with exception, {failure_log}")
        return False, str(e), retry_count


def parse_tag_values(
    influxdb3_local, tag_input: str | dict, args: dict, task_id: str
) -> dict[str, str]:
    """
    Parse a string of the form 'tag:value.tag2:value...' into a dictionary.

    Args:
        influxdb3_local: InfluxDB client instance.
        tag_input (str or dict): Input string in the format 'tag:value.tag:value2.tag2:value...' or a dictionary.
        args (dict): Dictionary of runtime arguments.
        task_id (str): Unique task identifier.

    Returns:
        dict[str, str]: Dictionary where keys are tag names and values are tag values.

    Example:
        parse_tag_values("host:server1.region:us-west")
        {'host': 'server1', 'region': 'us-west'}
    """
    if not tag_input:
        return {}

    if args["use_config_file"]:
        if isinstance(tag_input, dict):
            return tag_input
        else:
            influxdb3_local.warn(
                f"[{task_id}] Skipping malformed tag-value pair: '{tag_input}' (expected dict)"
            )
            return {}

    result: dict = {}
    # Split the string by '.' to get individual tag:value pairs
    pairs: list = tag_input.split(".")

    for pair in pairs:
        # Split each pair by ':' to separate tag and value
        if ":" not in pair:
            influxdb3_local.warn(
                f"[{task_id}] Skipping malformed tag-value pair: '{pair}' (missing ':')"
            )
            continue  # Skip malformed pairs
        tag, value = pair.split(":", 1)  # Split on first ':'
        result[tag] = value

    return result


def parse_string_of_dates(
    influxdb3_local, input_value: str | list | None, args: dict, task_id: str
) -> list[str] | None:
    """
    Parses a space-separated string of changepoint dates and validates their format or use values from config file.

    Args:
        influxdb3_local: Logger for reporting errors.
        input_value (str | None): Space-separated string of date strings.
        args (dict): Dictionary of runtime arguments.
        task_id (str): Task identifier for logging.

    Returns:
        list[str] | None: List of validated date strings, or None if input is None or invalid.
    """
    if input_value is None:
        return None
    result: list = []

    if args["use_config_file"]:
        if isinstance(input_value, list):
            for point in input_value:
                try:
                    datetime.fromisoformat(point)
                    result.append(point)
                except ValueError:
                    influxdb3_local.warn(
                        f"[{task_id}] Skipping invalid point '{point}'"
                    )
            return result
        else:
            influxdb3_local.warn(
                f"[{task_id}] Skipping malformed date string: '{input_value}' (expected list)"
            )
            return None

    raw_points: list = input_value.strip().split(" ")
    for point in raw_points:
        if not point.strip():
            continue  # skip empty parts
        # Validate the format — allow ISO-like date strings
        try:
            datetime.fromisoformat(point)
            result.append(point)
        except ValueError:
            influxdb3_local.warn(f"[{task_id}] Skipping invalid point '{point}'")

    if not result:
        return None

    return result


def get_model_storage_path(unique_file_suffix: str) -> Path:
    """
    Generate a unique model storage path based on plugin directory and unique suffix.

    Args:
        unique_file_suffix (str): Unique suffix for model storage.

    Returns:
        Path: Path object pointing to the model JSON file.

    Raises:
        Exception: If unique_file_suffix is missing or invalid.
    """
    try:
        plugin_dir = Path(__file__).parent / "prophet_models"
    except NameError:
        plugin_dir = Path(os.path.expanduser("~/.plugins/prophet_models"))

    # Ensure the directory exists
    plugin_dir.mkdir(parents=True, exist_ok=True)

    # Create the model file path
    model_path = plugin_dir / f"prophet_model_{unique_file_suffix}.json"
    return model_path


def validate_forecast(
    influxdb3_local,
    val_results: list[dict],
    forecast: pd.DataFrame,
    msre_threshold: float,
    task_id: str,
) -> bool:
    """
    Validate forecast against actual values over a validation window.

    Args:
        val_results: List of dicts from InfluxDB query for validation period; each dict must contain:
            - 'ds': timestamp in nanoseconds or convertible to datetime
            - 'y': actual value for the target field
        forecast: DataFrame from model.predict; must have columns 'ds' (datetime64[ns], possibly tz-aware)
            and 'yhat'.
        msre_threshold: Threshold for MSRE; if computed MSRE > threshold, validation fails.
        influxdb3_local: Logger-like interface with methods .info(), .warn(), .error().
        task_id: String identifier for logging context.

    Returns:
        bool: True if validation passes (sufficient matches and MSRE ≤ threshold), False otherwise.
    """
    val_df: pd.DataFrame = pd.DataFrame(val_results)

    # Convert 'ds' in val_df from nanoseconds to datetime64[ns], unify to UTC-naive
    val_df["ds"] = pd.to_datetime(val_df["ds"], unit="ns", utc=True)
    # Drop tzinfo to get UTC-naive timestamps
    val_df["ds"] = val_df["ds"].dt.tz_convert("UTC").dt.tz_localize(None)
    fc = forecast[["ds", "yhat"]].copy()

    # Convert forecast['ds'] to UTC-naive if tz-aware
    try:
        if pd.api.types.is_datetime64_any_dtype(fc["ds"]):
            if fc["ds"].dt.tz is not None:
                fc["ds"] = fc["ds"].dt.tz_convert("UTC").dt.tz_localize(None)
        else:
            # If forecast['ds'] is not datetime dtype, try parsing
            fc["ds"] = (
                pd.to_datetime(fc["ds"], utc=True)
                .dt.tz_convert("UTC")
                .dt.tz_localize(None)
            )
    except Exception as e:
        influxdb3_local.error(
            f"[{task_id}] Failed to convert forecast['ds'] to datetime: {e}"
        )
        return False

    val_sorted: pd.DataFrame = (
        val_df.sort_values("ds").dropna(subset=["y"]).reset_index(drop=True)
    )
    fc_sorted: pd.DataFrame = fc.sort_values("ds").reset_index(drop=True)

    # Get minimum length to avoid IndexError
    min_len: int = min(len(val_sorted), len(fc_sorted))

    # Trim both DataFrames to the same length
    val_trimmed: pd.DataFrame = val_sorted.iloc[:min_len]
    fc_trimmed: pd.DataFrame = fc_sorted.iloc[:min_len]

    # Extract the actual and predicted values
    y_true: pd.Series = val_trimmed["y"]
    y_pred: pd.Series = fc_trimmed["yhat"]

    # Filter out zero actuals to avoid division by zero in MSRE
    nonzero_mask: pd.Series = y_true != 0
    y_true = y_true[nonzero_mask]
    y_pred = y_pred[nonzero_mask]

    if y_true.empty:
        influxdb3_local.warn(
            f"[{task_id}] All actual 'y' values are zero after filtering; cannot compute MSRE."
        )
        return False

    # Compute MSRE
    try:
        msre = ((y_true - y_pred) ** 2 / y_true**2).mean()
        influxdb3_local.info(f"[{task_id}] MSRE: {msre}")
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to compute MSRE: {e}")
        return False

    # Compare to threshold
    if msre > msre_threshold:
        influxdb3_local.warn(
            f"[{task_id}] MSRE {msre} exceeds threshold {msre_threshold}, consider retraining."
        )
        return False

    return True


def create_prophet_model(
    influxdb3_local,
    seasonality_mode: str,
    changepoint_prior_scale: float,
    changepoints: list | None,
    holiday_date_list: list | None,
    holiday_names_list: list | None,
    holiday_country_names: list | None,
    task_id: str,
) -> Prophet:
    model: Prophet = Prophet(
        seasonality_mode=seasonality_mode,
        changepoint_prior_scale=changepoint_prior_scale,
        changepoints=changepoints,
    )

    # Add holidays if provided
    if holiday_date_list and holiday_names_list:
        if not len(holiday_date_list) == len(holiday_names_list):
            influxdb3_local.warn(
                f"[{task_id}] Number of holiday dates and names must be equal. Scipping adding holidays."
            )
        else:
            holidays: pd.DataFrame = pd.DataFrame(
                {"ds": pd.to_datetime(holiday_date_list), "holiday": holiday_names_list}
            )
            model.holidays = holidays
    else:
        influxdb3_local.info(
            f"[{task_id}] No holidays date or names provided. Skipping adding holidays."
        )

    if holiday_country_names:
        for country_name in holiday_country_names:
            model.add_country_holidays(country_name=country_name)

    return model


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

    senders_input: str | list | None = args.get("senders", None)
    if not senders_input:
        raise Exception(f"[{task_id}] No senders provided. Skipping sending alerts.")

    if args["use_config_file"]:
        if not isinstance(senders_input, list):
            raise Exception(
                f"[{task_id}] 'senders' must be a list when using config file"
            )
    else:
        senders_input = senders_input.split(".")

    for sender in senders_input:
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


def process_scheduled_call(
    influxdb3_local, call_time: datetime, args: dict | None = None
):
    """
    Executes a scheduled forecasting task using the Prophet time series model. Supports both model
    training and prediction. Validates forecasts (if configured), writes results to InfluxDB, and optionally
    sends alerts via configured senders on validation failure.

    This function is typically invoked by a scheduler with pre-defined configuration arguments.

    Args:
        influxdb3_local: Logger and InfluxDB client instance.
        call_time (datetime): The time at which the function is invoked. Used to define window boundaries.
        args (dict | None): Configuration dictionary for forecasting task.

            Required keys:
                - measurement (str): Source InfluxDB measurement name.
                - field (str): Target field to forecast.
                - window (str): Historical window duration (e.g., "30d").
                - forecast_horizont (str): Future duration to forecast (e.g., "2d").
                - tag_values (str): Comma-separated tag filter string.
                - target_measurement (str): Destination measurement for storing forecast.
                - model_mode (str): Mode of operation - "train" or "predict".
                - unique_suffix (str): Unique version suffix for model saving/loading.

            Optional keys:
                - config_file_path: path to config file to override args (str).
                - seasonality_mode (str): Prophet seasonality mode ("additive" or "multiplicative").
                - changepoint_prior_scale (float): Changepoint flexibility parameter.
                - changepoints (str): Comma-separated list of changepoint timestamps (ISO format).
                - holiday_date_list (str): List of holiday timestamps to inject.
                - holiday_names (str): Name to associate with the custom holiday list.
                - holiday_country_names (str): Built-in country holidays to include.
                - inferred_freq (str): Manually specified frequency string (e.g., "1H").
                - validation_window (str): Duration to validate forecast against recent true values.
                - msre_threshold (float): Maximum MSRE allowed; above this triggers validation failure.
                - target_database (str): Optional InfluxDB database override.
                - is_sending_alert (bool): Whether to send alerts on validation failure.
                - notification_text (str): Templated text for alert message.
                - senders (str): Dot-separated list of sender types (e.g., "slack.sms").
                - [sender-specific keys]: Each sender requires its own credentials and config.
                - notification_path (str): URL path for posting the alert (e.g., "notify").
                - influxdb3_auth_token (str): Auth token used for sending notifications.
                - port_override (int): Optional custom port for notification dispatch.

    Behavior:
        - Validates presence of all required configuration.
        - Loads or trains a Prophet model.
        - Forecasts future values and optionally validates them.
        - Writes forecast to InfluxDB if valid.
        - On validation failure, sends alert if `is_sending_alert` is True and `senders` are configured.

    Raises:
        All exceptions are caught and logged. No exceptions are propagated upward.
    """
    task_id: str = str(uuid.uuid4())
    influxdb3_local.info(f"[{task_id}] Starting scheduled forecast process at {call_time} with args: {args}")

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

    required_keys: list = [
        "measurement",
        "field",
        "window",
        "forecast_horizont",
        "tag_values",
        "target_measurement",
        "model_mode",
        "unique_suffix",
    ]

    if not args or any(key not in args for key in required_keys):
        influxdb3_local.error(
            f"[{task_id}] Missing some of the required arguments: {', '.join(required_keys)}"
        )
        return

    try:
        # Set up configuration
        measurement: str = args["measurement"]
        field: str = args["field"]
        tag_values: dict = parse_tag_values(
            influxdb3_local, args.get("tag_values", ""), args, task_id
        )
        is_sending_alert: bool = str(args.get("is_sending_alert", "")).lower() == "true"
        window: timedelta = parse_time_interval(args["window"], task_id)
        forecast_horizont: timedelta = parse_time_interval(
            args["forecast_horizont"], task_id
        )
        output_measurement: str = args["target_measurement"]
        model_mode: str = args["model_mode"].lower()
        unique_suffix: str = args["unique_suffix"]
        seasonality_mode: str = args.get("seasonality_mode", "additive")
        if seasonality_mode not in ["additive", "multiplicative"]:
            raise Exception(f"[{task_id}] Wrong seasonality mode: {seasonality_mode}")
        changepoint_prior_scale: float = float(
            args.get("changepoint_prior_scale", 0.05)
        )
        changepoints: list | None = parse_string_of_dates(
            influxdb3_local, args.get("changepoints", None), args, task_id
        )
        validation_window: timedelta = parse_time_interval(
            args.get("validation_window", "0s"), task_id
        )
        msre_threshold: float = float(args.get("msre_threshold", float("inf")))
        target_database: str | None = args.get("target_database", None)
        holiday_date_list: list[str] | None = parse_string_of_dates(
            influxdb3_local, args.get("holiday_date_list", None), args, task_id
        )

        if args["use_config_file"]:
            holiday_names_list: list | None = args.get("holiday_names")
            if not isinstance(holiday_names_list, list) and not None:
                influxdb3_local.warn(
                    f"[{task_id}] Expecting holiday_names to be a list, got {type(holiday_names_list)}. Skipping adding holidays."
                )
                holiday_names_list = None
        else:
            holiday_names_list: list[str] | None = (
                args.get("holiday_names").split(".")
                if args.get("holiday_names")
                else None
            )

        if args["use_config_file"]:
            holiday_country_names: list | None = args.get("holiday_country_names")
            if not isinstance(holiday_country_names, list) and not None:
                influxdb3_local.warn(
                    f"[{task_id}] Expecting holiday_country_names to be a list, got {type(holiday_country_names)}. Skipping adding holidays."
                )
                holiday_country_names = None
        else:
            holiday_country_names: list | None = (
                args.get("holiday_country_names").split(".")
                if args.get("holiday_country_names")
                else None
            )

        inferred_freq: str | None = args.get("inferred_freq", None)

        # Fetch historical data
        if validation_window > timedelta(0):
            end_time: datetime = call_time - validation_window
        else:
            end_time: datetime = call_time
        start_time: datetime = call_time - window
        if start_time == end_time:
            raise Exception(
                f"[{task_id}] Time window for data query is zero — no time range specified for data collection."
            )
        influxdb3_local.info(f"[{task_id}] Fetching data from {start_time} to {end_time}")
        query: str = generate_query(
            measurement, field, tag_values, start_time, end_time
        )
        results: list = influxdb3_local.query(query)

        if not results:
            influxdb3_local.error(
                f"[{task_id}] No data found from {start_time} to {end_time}"
            )
            return
        influxdb3_local.info(f"[{task_id}] Retrieved {len(results)} records from {measurement}")

        df: pd.DataFrame = pd.DataFrame(results)
        df["ds"] = pd.to_datetime(df["ds"], unit="ns", utc=True)
        df["ds"] = df["ds"].dt.tz_convert("UTC").dt.tz_localize(None)

        influxdb3_local.info(
            f"[{task_id}] Starting Prophet model with {model_mode} mode"
        )
        # Train or load model
        if model_mode == "train":
            model: Prophet = create_prophet_model(
                influxdb3_local,
                seasonality_mode,
                changepoint_prior_scale,
                changepoints,
                holiday_date_list,
                holiday_names_list,
                holiday_country_names,
                task_id,
            )
            model.fit(df)
            influxdb3_local.info(f"[{task_id}] Model trained")
        elif model_mode == "predict":
            file_path: Path = get_model_storage_path(unique_suffix)
            if not file_path.exists():
                # Model file not found: train a new model now, then save
                influxdb3_local.warn(
                    f"[{task_id}] Model file not found at {file_path}. Training a new model now."
                )
                try:
                    model = create_prophet_model(
                        influxdb3_local,
                        seasonality_mode,
                        changepoint_prior_scale,
                        changepoints,
                        holiday_date_list,
                        holiday_names_list,
                        holiday_country_names,
                        task_id,
                    )
                    # Train on the full historical df
                    model.fit(df)
                    influxdb3_local.info(
                        f"[{task_id}] New model trained because no existing file was found."
                    )

                    # Save the newly trained model
                    file_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(file_path, "w") as file:
                        file.write(model_to_json(model))
                    influxdb3_local.info(
                        f"[{task_id}] Newly trained model saved to {file_path}"
                    )
                except Exception as e:
                    influxdb3_local.error(
                        f"[{task_id}] Failed to train and save new model: {e}"
                    )
                    return
            else:
                with open(file_path, "r") as fin:
                    model = model_from_json(fin.read())
                influxdb3_local.info(f"[{task_id}] Model loaded from {file_path}")
        else:
            influxdb3_local.error(f"[{task_id}] Invalid model_mode: {model_mode}")
            return

        # Generate forecast
        if not inferred_freq:
            inferred_freq: str = pd.infer_freq(df["ds"])

            if inferred_freq is None:
                influxdb3_local.error(
                    f"[{task_id}] Unable to infer frequency, please provide it manually with the 'inferred_freq' argument"
                )
                return
        influxdb3_local.info(f"[{task_id}] Inferred frequency: {inferred_freq}")

        try:
            freq_timedelta: timedelta = pd.to_timedelta(
                pd.tseries.frequencies.to_offset(inferred_freq)
            )
        except Exception:
            influxdb3_local.error(
                f"[{task_id}] Unable to transform {inferred_freq} to timedelta"
            )
            return

        periods: int = int((forecast_horizont + validation_window) / freq_timedelta)
        influxdb3_local.info(
            f"[{task_id}] Forecast horizon: {forecast_horizont}, inferred frequency: {inferred_freq}, periods: {periods}"
        )

        future: pd.DataFrame = model.make_future_dataframe(
            periods=periods, freq=inferred_freq, include_history=False
        )
        forecast: pd.DataFrame = model.predict(future)

        # Model evaluation (if validation window is set)
        is_valid: bool = True
        if validation_window > timedelta(0):
            val_start_time: datetime = end_time
            val_query: str = generate_query(
                measurement, field, tag_values, val_start_time, call_time
            )
            val_results: list = influxdb3_local.query(val_query)
            if val_results:
                is_valid = validate_forecast(
                    influxdb3_local=influxdb3_local,
                    val_results=val_results,
                    forecast=forecast,
                    msre_threshold=msre_threshold,
                    task_id=task_id,
                )
            else:
                influxdb3_local.warn(
                    f"[{task_id}] No data found for validation window: {val_start_time} to {end_time}, skipping validation"
                )

        if is_valid:
            # Prepare forecast data
            forecast_df: pd.DataFrame = forecast[
                forecast["ds"] >= np.datetime64(call_time)
            ][["ds", "yhat", "yhat_lower", "yhat_upper"]].rename(
                columns={"ds": "time", "yhat": "forecast"}
            )
            forecast_df["model_version"] = unique_suffix
            forecast_df["run_time"] = call_time.isoformat()
            forecast_df["time"] = forecast_df["time"].astype("int64")
            forecast_data: list = forecast_df.to_dict("records")

            # Define fields for forecast data (no aggregation needed)
            fields_list: list = ["forecast", "yhat_lower", "yhat_upper", "run_time"]

            # Transform data to LineBuilder objects
            builders: list = transform_to_influx_line(
                forecast_data, output_measurement, fields_list, tag_values
            )
            influxdb3_local.info(f"[{task_id}] Writing {len(builders)} lines to InfluxDB")
            # Write forecast data to InfluxDB
            max_retries: int = 3
            success, error, retries = write_downsampled_data(
                influxdb3_local,
                builders,
                max_retries=max_retries,
                target_measurement=output_measurement,
                target_database=target_database,
                task_id=task_id,
            )
            if success:
                influxdb3_local.info(
                    f"[{task_id}] Forecast written to {output_measurement}"
                )
            else:
                influxdb3_local.error(f"[{task_id}] Failed to write forecast: {error}")

        else:
            influxdb3_local.error(
                f"[{task_id}] Validation failed, forecast not written to {output_measurement}"
            )
            if is_sending_alert:
                try:
                    senders_config: dict = parse_senders(influxdb3_local, args, task_id)
                    notification_tpl: str = args.get(
                        "notification_text",
                        "Validation failed for prophet model:$version on table:$measurement, field:$field for period from $start_time to $end_time, forecast not written to table:$output_measurement",
                    )
                    val_start_time: datetime = end_time - validation_window
                    port_override: int = parse_port_override(args, task_id)
                    notification_path: str = args.get("notification_path", "notify")
                    influxdb3_auth_token: str = args.get(
                        "influxdb3_auth_token"
                    ) or os.getenv("INFLUXDB3_AUTH_TOKEN")
                    if not influxdb3_auth_token:
                        raise Exception(f"[{task_id}] INFLUXDB3_AUTH_TOKEN not found")

                    payload: dict = {
                        "notification_text": interpolate_notification_text(
                            notification_tpl,
                            {
                                "version": unique_suffix,
                                "measurement": measurement,
                                "field": field,
                                "start_time": val_start_time,
                                "end_time": end_time,
                                "output_measurement": output_measurement,
                            },
                        ),
                        "senders_config": senders_config,
                    }
                    send_notification(
                        influxdb3_local,
                        port_override,
                        notification_path,
                        influxdb3_auth_token,
                        payload,
                        task_id,
                    )
                except Exception as e:
                    influxdb3_local.error(
                        f"[{task_id}] Failed to send notification: {e}"
                    )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Unexpected error: {e}")


def parse_time_window(
    data: dict, start_key: str, end_key: str, task_id: str
) -> tuple[datetime | None, datetime]:
    """
    Parses time window. Requires timezone-aware datetime strings
    in ISO 8601 format (e.g., '2025-05-01T00:00:00+03:00').

    Args:
        data (dict): Dictionary containing 'start_time' and 'end_time' keys.
        start_key (str): The key for the start time.
        end_key (str): The key for the end time.
        task_id (str): The task ID.

    Returns:
        tuple[datetime | None, datetime]: Tuple of start and end datetimes in UTC.

    Raises:
        Exception: If the datetime format is invalid, lacks timezone info, or if start ≥ end.
    """

    def parse_iso_datetime(name: str, value: str) -> datetime:
        try:
            dt: datetime = datetime.fromisoformat(value)
        except ValueError:
            raise Exception(
                f"[{task_id}] Invalid ISO 8601 datetime for {name}: '{value}'."
            )
        if dt.tzinfo is None:
            raise Exception(
                f"[{task_id}] {name} must include timezone info (e.g., '+00:00')."
            )
        return dt.astimezone(timezone.utc)

    start_str: str = data.get(start_key)
    end_str: str = data.get(end_key)

    start_time: datetime = parse_iso_datetime(start_key, start_str)
    end_time: datetime = parse_iso_datetime(end_key, end_str)

    if start_time >= end_time:
        raise Exception(
            f"[{task_id}] start_time {start_time} must be earlier than end_time {end_time}."
        )

    return start_time, end_time


def process_request(
    influxdb3_local, query_parameters, request_headers, request_body, args=None
):
    """
        Handle an HTTP request to perform a one-off Prophet forecast over a specified historical window
        and horizon, optionally saving/loading the model, validating the forecast, and writing results to InfluxDB.

        This function is intended to be invoked by an HTTP endpoint. It:
          1. Parses the JSON body for required forecasting parameters.
          2. Queries InfluxDB for historical data in [start_time, end_time].
          3. Creates or loads a Prophet model (depending on save_mode).
          4. Infers or uses provided frequency, computes the forecast horizon, and predicts future values.
          5. Optionally validates the forecast against a recent validation window.
          6. Writes forecast points to the specified InfluxDB measurement.
          7. Returns a dictionary containing a "message" summarizing success or error.

        Args:
            influxdb3_local:
                An object providing:
                  - Logging methods: .info(), .warn(), .error().
                  - A .query(query_str) method to execute InfluxDB queries and return results as list[dict].
                  - Other utilities as needed by helper functions (e.g., write_downsampled_data).
            query_parameters:
                A dict of HTTP query parameters (currently not used by this implementation but provided for extensibility).
            request_headers:
                A dict of HTTP request headers (currently not used here but available for future checks, e.g. auth).
            request_body:
                A JSON-encoded string containing the forecasting configuration. Must include the following keys:
                    - "measurement" (str): Source InfluxDB measurement name to query historical data from.
                    - "field" (str): Field name within the measurement to forecast.
                    - "forecast_horizont" (str): Forecast horizon duration (e.g., "7d", "24h"). Parsed via parse_time_duration.
                    - "tag_values" (dict): Tag filters for the InfluxDB query, e.g. {"region":"us-west","device":"sensor1"}.
                    - "target_measurement" (str): Destination measurement name to write forecast points.
                    - "unique_suffix" (str): Unique identifier for model versioning and storage.
                    - "start_time" (str): Start of historical window, in a format supported by parse_time_window (e.g., ISO 8601).
                    - "end_time" (str): End of historical window, in a format supported by parse_time_window.
                Optional keys in the JSON body:
                    - "seasonality_mode" (str): Prophet seasonality mode, "additive" or "multiplicative". Defaults to "additive".
                    - "changepoint_prior_scale" (float): Flexibility of trend changepoints. Defaults to 0.05.
                    - "changepoints" (list[str]): List of changepoint dates/strings in ISO format.
                    - "save_mode" (str or bool-like): If present and equals "true" (case-insensitive), the function will attempt to load/save a persisted model from disk using unique_suffix. Otherwise, the model is always trained from scratch for this request.
                    - "validation_window" (str): Duration for validation window (e.g., "3d"). Defaults to "0s" (no validation).
                    - "msre_threshold" (float): Maximum acceptable MSRE for validation. Defaults to infinity (no threshold).
                    - "target_database" (str): Optional InfluxDB database name to write forecast.
                    - "holiday_date_list" (list[str]): List of custom holiday dates (ISO strings).
                    - "holiday_names" (list[str]): List of names corresponding to holiday_date_list.
                    - "holiday_country_names" (list[str]): List of country codes/names for built-in Prophet holidays.
                    - "inferred_freq" (str): Manually specified frequency (e.g., "1D", "1H"). If absent, code attempts pd.infer_freq.
            args:
                A dict of additional arguments.

        Side Effects:
            - Queries InfluxDB for historical data via influxdb3_local.query().
            - Trains or loads a Prophet model:
                - If save_mode is true and a model file exists at the path determined by unique_suffix, it loads it.
                - If save_mode is true but no file exists (or loading fails), it trains a new model on the full historical df and saves it.
                - If save_mode is false or absent, always trains a new model in-memory.
            - Infers or uses provided frequency for forecasting; computes periods = int(forecast_horizont / freq_timedelta).
            - Generates forecast DataFrame via model.make_future_dataframe(...) and model.predict(...).
            - Optionally validates forecast against recent actuals if validation_window > 0:
                - If validation fails, does not write forecast and returns an error message.
            - On successful validation (or if validation not requested), prepares forecast points:
                - Writes to InfluxDB.
            - Logs all steps and errors via influxdb3_local.info()/warn()/error().

        Example Usage (curl):
            # Minimal required fields:
            curl -X POST https://your-server/api/forecast \
                 -H "Content-Type: application/json" \
                 -d '{
                       "measurement": "temperature",
                       "field": "value",
                       "forecast_horizont": "7d",
                       "tag_values": {"region":"us-west"},
                       "target_measurement": "temperature_forecast",
                       "unique_suffix": "20250619_v1",
                       "start_time": "2025-05-20T00:00:00Z",
                       "end_time": "2025-06-19T00:00:00Z"
                     }'
        """
    task_id: str = str(uuid.uuid4())
    influxdb3_local.info(f"[{task_id}] Received forecasting request.")
    run_time: datetime = datetime.now(timezone.utc)

    if request_body:
        data: dict = json.loads(request_body)
        influxdb3_local.info(f"[{task_id}] Request body: {data}")
    else:
        influxdb3_local.error(f"[{task_id}] No request body provided.")
        return {"message": f"[{task_id}] Error: No request body provided."}

    required_keys: list = [
        "measurement",
        "field",
        "forecast_horizont",
        "tag_values",
        "target_measurement",
        "unique_suffix",
        "start_time",
        "end_time",
    ]

    if not data or any(key not in data for key in required_keys):
        influxdb3_local.error(
            f"[{task_id}] Missing some of the required arguments: {', '.join(required_keys)}"
        )
        return {
            "message": f"[{task_id}] Missing some of the required arguments: {', '.join(required_keys)}"
        }

    try:
        # Set up configuration
        measurement: str = data["measurement"]
        field: str = data["field"]
        tag_values: dict = data["tag_values"]
        forecast_horizont: timedelta = parse_time_interval(
            data["forecast_horizont"], task_id
        )
        output_measurement: str = data["target_measurement"]
        save_mode: bool = str(data.get("save_mode", "")).lower() == "true"
        unique_suffix: str = data["unique_suffix"]
        seasonality_mode: str = data.get("seasonality_mode", "additive")
        changepoint_prior_scale: float = float(
            data.get("changepoint_prior_scale", 0.05)
        )
        changepoints: list | None = data.get("changepoints", None)
        validation_window: timedelta = parse_time_interval(
            data.get("validation_window", "0s"), task_id
        )
        msre_threshold: float = float(data.get("msre_threshold", float("inf")))
        target_database: str | None = data.get("target_database", None)
        holiday_date_list: list[str] | None = data.get("holiday_date_list", None)
        holiday_names: list | None = data.get("holiday_names", None)
        holiday_country_names: list | None = data.get("holiday_country_names", None)
        inferred_freq: str | None = data.get("inferred_freq", None)

        start_time, end_time = parse_time_window(
            data, "start_time", "end_time", task_id
        )
        validation_start_time: datetime = end_time - validation_window

        influxdb3_local.info(f"[{task_id}] Fetching historical data from {start_time} to {validation_start_time}.")
        query: str = generate_query(
            measurement, field, tag_values, start_time, validation_start_time
        )
        results: list = influxdb3_local.query(query)

        if not results:
            influxdb3_local.error(
                f"[{task_id}] No data found from {start_time} to {end_time}"
            )
            return {
                "message": f"[{task_id}] No data found from {start_time} to {end_time}"
            }
        influxdb3_local.info(f"[{task_id}] Retrieved {len(results)} records from {measurement}")

        df: pd.DataFrame = pd.DataFrame(results)
        df["ds"] = pd.to_datetime(df["ds"], unit="ns", utc=True)
        df["ds"] = df["ds"].dt.tz_convert("UTC").dt.tz_localize(None)

        influxdb3_local.info(
            f"[{task_id}] Starting Prophet model with safe mode: {save_mode}"
        )
        # Train or load model
        if save_mode:
            file_path: Path = get_model_storage_path(unique_suffix)
            if not file_path.exists():
                # Model file not found: train a new model now, then save
                influxdb3_local.warn(
                    f"[{task_id}] Model file not found at {file_path}. Training a new model now."
                )
                try:
                    model: Prophet = create_prophet_model(
                        influxdb3_local,
                        seasonality_mode,
                        changepoint_prior_scale,
                        changepoints,
                        holiday_date_list,
                        holiday_names,
                        holiday_country_names,
                        task_id,
                    )
                    # Train on the full historical df
                    model.fit(df)
                    influxdb3_local.info(
                        f"[{task_id}] New model trained because no existing file was found."
                    )

                    # Save the newly trained model
                    file_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(file_path, "w") as file:
                        file.write(model_to_json(model))
                    influxdb3_local.info(
                        f"[{task_id}] Newly trained model saved to {file_path}"
                    )
                except Exception as e:
                    influxdb3_local.error(
                        f"[{task_id}] Failed to train and save new model: {e}"
                    )
                    return {
                        "message": f"[{task_id}] Failed to train and save new model: {e}"
                    }
            else:
                with open(file_path, "r") as fin:
                    model = model_from_json(fin.read())
                influxdb3_local.info(f"[{task_id}] Model loaded from {file_path}")

        else:
            model = create_prophet_model(
                influxdb3_local,
                seasonality_mode,
                changepoint_prior_scale,
                changepoints,
                holiday_date_list,
                holiday_names,
                holiday_country_names,
                task_id,
            )
            model.fit(df)
            influxdb3_local.info(f"[{task_id}] Model trained")

        # Generate forecast
        if not inferred_freq:
            inferred_freq: str = pd.infer_freq(df["ds"])

            if inferred_freq is None:
                influxdb3_local.error(
                    f"[{task_id}] Unable to infer frequency, please provide it manually with the 'inferred_freq' argument"
                )
                return {
                    "message": f"[{task_id}] Unable to infer frequency, please provide it manually with the 'inferred_freq' argument"
                }
        influxdb3_local.info(f"[{task_id}] Inferred frequency: {inferred_freq}")

        try:
            freq_timedelta: timedelta = pd.to_timedelta(
                pd.tseries.frequencies.to_offset(inferred_freq)
            )
        except Exception:
            influxdb3_local.error(
                f"[{task_id}] Unable to transform {inferred_freq} to timedelta"
            )
            return {
                "message": f"[{task_id}] Unable to transform {inferred_freq} to timedelta"
            }

        periods: int = int((forecast_horizont + validation_window) / freq_timedelta)
        influxdb3_local.info(
            f"[{task_id}] Forecast horizon: {forecast_horizont}, inferred frequency: {inferred_freq}, periods: {periods}"
        )

        future: pd.DataFrame = model.make_future_dataframe(
            periods=periods, freq=inferred_freq, include_history=False
        )
        forecast: pd.DataFrame = model.predict(future)

        # Model evaluation (if validation window is set)
        is_valid: bool = True
        if validation_window > timedelta(0):
            val_start_time: datetime = validation_start_time
            val_query: str = generate_query(
                measurement, field, tag_values, val_start_time, end_time
            )
            val_results: list = influxdb3_local.query(val_query)
            if val_results:
                is_valid = validate_forecast(
                    influxdb3_local=influxdb3_local,
                    val_results=val_results,
                    forecast=forecast,
                    msre_threshold=msre_threshold,
                    task_id=task_id,
                )
            else:
                influxdb3_local.warn(
                    f"[{task_id}] No data found for validation window: {val_start_time} to {end_time}, skipping validation"
                )

        if is_valid:
            # Prepare forecast data
            forecast_df: pd.DataFrame = forecast[
                forecast["ds"] >= np.datetime64(end_time)
            ][["ds", "yhat", "yhat_lower", "yhat_upper"]].rename(
                columns={"ds": "time", "yhat": "forecast"}
            )
            forecast_df["model_version"] = unique_suffix
            forecast_df["run_time"] = run_time.isoformat()
            forecast_df["time"] = forecast_df["time"].astype("int64")
            forecast_data: list = forecast_df.to_dict("records")

            # Define fields for forecast data (no aggregation needed)
            fields_list: list = ["forecast", "yhat_lower", "yhat_upper", "run_time"]

            # Transform data to LineBuilder objects
            builders: list = transform_to_influx_line(
                forecast_data, output_measurement, fields_list, tag_values
            )
            influxdb3_local.info(f"[{task_id}] Writing {len(builders)} lines to InfluxDB")

            # Write forecast data to InfluxDB
            max_retries: int = 3
            success, error, retries = write_downsampled_data(
                influxdb3_local,
                builders,
                max_retries=max_retries,
                target_measurement=output_measurement,
                target_database=target_database,
                task_id=task_id,
            )
            if success:
                influxdb3_local.info(
                    f"[{task_id}] Forecast written to {output_measurement}. Forecast generation completed."
                )
                return {
                    "message": f"[{task_id}] Forecast written to {output_measurement}. Forecast generation completed."
                }
            else:
                influxdb3_local.error(f"[{task_id}] Failed to write forecast: {error}")
                return {"message": f"[{task_id}] Failed to write forecast: {error}"}

        else:
            influxdb3_local.error(
                f"[{task_id}] Validation failed, forecast not written to {output_measurement}"
            )
            return {
                "message": f"[{task_id}] Validation failed, forecast not written to {output_measurement}"
            }

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Unexpected error: {e}")
        return {"message": f"[{task_id}] Unexpected error: {e}"}
