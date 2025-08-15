"""
{
    "plugin_type": ["scheduled"],
    "scheduled_args_config": [
        {
            "name": "measurement",
            "example": "cpu",
            "description": "The InfluxDB measurement (table) to query.",
            "required": true
        },
        {
            "name": "field",
            "example": "usage",
            "description": "The numeric field to evaluate for anomalies.",
            "required": true
        },
        {
            "name": "detectors",
            "example": "QuantileAD.LevelShiftAD",
            "description": "Dot-separated list of ADTK detectors. Supported: GeneralizedESDTestAD, InterQuartileRangeAD, ThresholdAD, QuantileAD, LevelShiftAD, VolatilityShiftAD, PersistAD, SeasonalAD.",
            "required": true
        },
        {
            "name": "detector_params",
            "example": "eyJRdWFudGlsZUFKIjogeyJsb3dfcXVhbnRpbGUiOiA...",
            "description": "Base64-encoded JSON string specifying parameters for each detector.",
            "required": true
        },
        {
            "name": "min_consensus",
            "example": "2",
            "description": "Minimum number of detectors that must agree to flag a point as anomalous. Default: 1.",
            "required": false
        },
        {
            "name": "window",
            "example": "1h",
            "description": "Time window for data analysis (e.g., `1h` for 1 hour). Units: `s`, `min`, `h`, `d`, `w`.",
            "required": true
        },
        {
            "name": "senders",
            "example": "slack.discord",
            "description": "Dot-separated list of notification channels. Supported channels: slack, discord, http, sms, whatsapp.",
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
            "example": "5m",
            "description": "Minimum duration for an anomaly condition to persist before triggering a notification (e.g., `5m`). Units: `s`, `min`, `h`, `d`, `w`. Default: `0s`.",
            "required": false
        },
        {
            "name": "notification_text",
            "example": "Anomaly detected in $table.$field with value $value by $detectors. Tags: $tags",
            "description": "Template for notification message with variables `$table`, `$field`, `$value`, `$detectors`, `$tags`.",
            "required": false
        },
        {
            "name": "notification_path",
            "example": "some/path",
            "description": "URL path for the notification sending plugin. Default: `notify`.",
            "required": false
        },
        {
            "name": "port_override",
            "example": "8182",
            "description": "Port number where InfluxDB accepts requests. Default: `8181`.",
            "required": false
        },
        {
            "name": "slack_webhook_url",
            "example": "https://hooks.slack.com/services/...",
            "description": "Incoming webhook URL for Slack notifications. Required if using the slack sender.",
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
            "description": "Incoming webhook URL for Discord notifications. Required if using the discord sender.",
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
            "description": "Webhook URL for HTTP notifications. Required if using the http sender.",
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
            "description": "Twilio Account SID. Required if using the sms or whatsapp sender.",
            "required": false
        },
        {
            "name": "twilio_token",
            "example": "your_auth_token",
            "description": "Twilio Auth Token. Required if using the sms or whatsapp sender.",
            "required": false
        },
        {
            "name": "twilio_to_number",
            "example": "+1234567890",
            "description": "Recipient phone number. Required if using the sms or whatsapp sender.",
            "required": false
        },
        {
            "name": "twilio_from_number",
            "example": "+19876543210",
            "description": "Twilio sender phone number (verified). Required if using the sms or whatsapp sender.",
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

import base64
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
from adtk.data import validate_series
from adtk.detector import (
    GeneralizedESDTestAD,
    InterQuartileRangeAD,
    LevelShiftAD,
    PersistAD,
    QuantileAD,
    SeasonalAD,
    ThresholdAD,
    VolatilityShiftAD,
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

# List of keywords to exclude from argument validation in AVAILABLE_SENDERS
EXCLUDED_KEYWORDS = ["headers", "token", "sid"]

# Supported ADTK detectors
AVAILABLE_DETECTORS = {
    "GeneralizedESDTestAD": GeneralizedESDTestAD,
    "InterQuartileRangeAD": InterQuartileRangeAD,
    "ThresholdAD": ThresholdAD,
    "QuantileAD": QuantileAD,
    "LevelShiftAD": LevelShiftAD,
    "VolatilityShiftAD": VolatilityShiftAD,
    "PersistAD": PersistAD,
    "SeasonalAD": SeasonalAD,
}


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


def get_tag_names(influxdb3_local, measurement: str, task_id: str) -> list[str]:
    """
    Retrieves the list of tag names for a measurement.

    Args:
        influxdb3_local: InfluxDB client instance.
        measurement (str): Name of the measurement to query.
        task_id (str): The task ID.

    Returns:
        list[str]: List of tag names with 'Dictionary(Int32, Utf8)' data type.
    """
    query: str = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $measurement
        AND data_type = 'Dictionary(Int32, Utf8)'
    """
    res: list[dict] = influxdb3_local.query(query, {"measurement": measurement})

    if not res:
        influxdb3_local.info(
            f"[{task_id}] No tags found for measurement '{measurement}'."
        )
        return []

    tag_names: list[str] = [tag["column_name"] for tag in res]
    return tag_names


def generate_cache_key(
    measurement: str, field: str, tags: list[str], row: pd.Series
) -> str:
    """
    Generate a consistent cache key string for tracking anomaly durations without timestamp.

    Args:
        measurement (str): Measurement (table) name.
        field (str): Field name being checked.
        tags (list[str]): List of tag column names to include.
        row (pd.Series): Current row data; used to extract tag values.

    Returns:
        str: Formatted key, e.g., "cpu:usage:host=server1:region=us-west".
    """
    base: str = f"{measurement}:{field}"
    for tag in sorted(tags):
        tag_val = row.get(tag, "None")
        base += f":{tag}={tag_val}"
    return base


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
    Parse and validate the 'port_override' argument, converting it from string to int.

    Args:
        args (dict): Runtime arguments containing 'port_override'.
        task_id (str): Unique task identifier for logging context.

    Returns:
        int: Parsed port number (1–65535), or 8181 if not provided.

    Raises:
        Exception: If 'port_override' is provided but is not a valid integer in the range 1–65535.
    """
    raw: str | int = args.get("port_override", 8181)

    try:
        port = int(raw)
    except (TypeError, ValueError):
        raise Exception(f"[{task_id}] Invalid port_override, not an integer: {raw!r}")

    # Validate port range
    if not (1 <= port <= 65535):
        raise Exception(
            f"[{task_id}] Invalid port_override, must be between 1 and 65535: {port}"
        )

    return port


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


def parse_time_duration(raw: str, task_id: str) -> timedelta:
    """
    Parse a time duration string into a timedelta.

    Args:
        raw (str): Duration string (e.g., "5m", "1h").
        task_id (str): Unique task identifier for logging.

    Returns:
        timedelta: Parsed duration.

    Raises:
        Exception: If the duration format is invalid.
    """
    valid_units: dict = {
        "s": "seconds",
        "min": "minutes",
        "h": "hours",
        "d": "days",
        "w": "weeks",
    }
    num_part, unit_part = "", ""
    for unit in sorted(valid_units.keys(), key=len, reverse=True):
        if raw.endswith(unit):
            num_part = raw[: -len(unit)]
            unit_part = unit
            break
    if not num_part or unit_part not in valid_units:
        raise Exception(f"[{task_id}] Invalid duration format: {raw}")
    try:
        num = int(num_part)
    except ValueError:
        raise Exception(f"[{task_id}] Invalid number in duration: {raw}")
    return timedelta(**{valid_units[unit_part]: num})


def parse_detector_params(
    influxdb3_local, args: dict, detectors: list, task_id: str
) -> dict:
    """
    Parse and validate detector parameters from args, expecting detector_params as a base64-encoded JSON string or use values from config file.

    Args:
        influxdb3_local: InfluxDB client instance.
        args (dict): Must include "detector_params" as a base64-encoded JSON string.
        detectors (list): List of detector names.
        task_id (str): Unique task identifier.

    Returns:
        dict: Mapping of detector names to their parameter dictionaries.

    Raises:
        Exception: If detector_params is not valid base64, contains invalid JSON, or is missing required parameters.
    """
    input_params: str | dict = args["detector_params"]

    if args["use_config_file"]:
        if isinstance(input_params, dict):
            params: dict = input_params
        else:
            raise Exception(
                f"[{task_id}] detector_params must be a dict when using config file"
            )
    else:
        try:
            # Decode base64-encoded string
            decoded_bytes: bytes = base64.b64decode(input_params)
            decoded_str: str = decoded_bytes.decode("utf-8")
        except Exception:
            raise Exception(
                f"[{task_id}] Invalid base64 encoding in detector_params: {input_params}"
            )

        try:
            # Parse JSON from decoded string
            params: dict = json.loads(decoded_str)
        except json.JSONDecodeError:
            raise Exception(
                f"[{task_id}] Invalid JSON in decoded detector_params: {decoded_str}"
            )

    valid_params: dict = {}
    for detector in detectors[:]:
        if detector not in params:
            influxdb3_local.warn(
                f"[{task_id}] Missing parameters for detector: {detector}"
            )
            continue
        valid_params[detector] = params[detector]

        # Validate required parameters
        if detector == "LevelShiftAD":
            if "window" not in valid_params[detector]:
                influxdb3_local.warn(
                    f"[{task_id}] LevelShiftAD requires 'window' parameter"
                )
                del valid_params[detector]
                detectors.remove(detector)
                continue
        elif detector == "VolatilityShiftAD":
            if "window" not in valid_params[detector]:
                influxdb3_local.warn(
                    f"[{task_id}] VolatilityShiftAD requires 'window' parameter"
                )
                del valid_params[detector]
                detectors.remove(detector)
                continue

    if not valid_params:
        raise Exception(
            f"[{task_id}] No valid detector parameters found in detector_params: {params}"
        )

    return valid_params


def parse_min_consensus(min_consensus: str | int, task_id: str) -> int:
    """Validate and convert min_consensus to an integer."""
    try:
        return int(min_consensus)
    except (TypeError, ValueError):
        raise Exception(
            f"[{task_id}] Invalid min_consensus, not an integer: {min_consensus!r}"
        )


def process_scheduled_call(
    influxdb3_local, call_time: datetime, args: dict | None = None
):
    """
    Scheduler trigger for anomaly detection using ADTK stateless detectors.

    Queries a specified measurement and field within a time window, applies one or more
    ADTK detectors, and sends notifications for anomalies. Supports consensus-based detection
    (all detectors must agree) and optional debounce logic.

    Args:
        influxdb3_local: InfluxDB client for querying, caching, and logging.
        call_time (datetime): UTC timestamp at which the scheduler triggers this function.
        args (dict):
            Required:
                - table (str): Measurement name to query.
                - field (str): Numeric field to evaluate.
                - detectors (str): Dot-separated list of ADTK detectors.
                - detector_params (str): JSON string of detector parameters.
                - min_consensus (int): Minimum number of detectors required to flag anomaly.
                - window (str): Time window for data query (e.g., "1h").
                - senders (str): Dot-separated notification channels.
            Optional:
                - config_file_path (str): path to config file to override args.
                - min_condition_duration (str): Minimum anomaly duration (e.g., "5m").
                - notification_text (str): Message template.
                - notification_path (str): Path for notification plugin (default: "notify").
                - port_override (int): HTTP port (default: 8181).
                - influxdb3_auth_token (str): API v3 token (or via ENV var).

    Exceptions:
        All exceptions are caught and logged via influxdb3_local.error.
    """
    task_id: str = str(uuid.uuid4())
    influxdb3_local.info(f"[{task_id}] Starting anomaly detection scheduled call at {call_time} with args: {args}")

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

    if (
        not args
        or "measurement" not in args
        or "field" not in args
        or "detectors" not in args
        or "detector_params" not in args
        or "window" not in args
        or "senders" not in args
    ):
        influxdb3_local.error(
            f"[{task_id}] Missing required arguments: measurement, field, detectors, detector_params, window, or senders"
        )
        return

    try:
        # Parse configuration
        influxdb3_local.info(f"[{task_id}] Starting configuration parsing")
        measurement: str = args["measurement"]
        # Validate measurement
        all_measurements: list = get_all_measurements(influxdb3_local)
        if measurement not in all_measurements:
            influxdb3_local.error(f"[{task_id}] Measurement '{measurement}' not found")
            return
        influxdb3_local.info(f"[{task_id}] Validated measurement: {measurement}")

        field: str = args["field"]
        detectors: list = (
            args["detectors"].split(".")
            if not args.get("use_config_file")
            else args["detectors"]
        )
        detector_params: dict = parse_detector_params(
            influxdb3_local, args, detectors, task_id
        )
        influxdb3_local.info(f"[{task_id}] Retrieved detector_params: {detector_params}")

        min_consensus: int = parse_min_consensus(args.get("min_consensus", 1), task_id)
        window: timedelta = parse_time_duration(args["window"], task_id)
        senders_config: dict = parse_senders(influxdb3_local, args, task_id)
        port_override: int = parse_port_override(args, task_id)
        min_condition_duration: timedelta = parse_time_duration(
            args.get("min_condition_duration", "0s"), task_id
        )
        notification_path: str = args.get("notification_path", "notify")
        notification_text: str = args.get(
            "notification_text",
            "Anomaly detected in $table.$field with value $value by $detectors. Tags: $tags",
        )
        influxdb3_auth_token: str = args.get("influxdb3_auth_token") or os.getenv(
            "INFLUXDB3_AUTH_TOKEN"
        )
        if not influxdb3_auth_token:
            influxdb3_local.error(f"[{task_id}] Missing influxdb3_auth_token")
            return

        influxdb3_local.info(f"[{task_id}] Configuration completed: field={field}, detectors={len(detectors)}, min_consensus={min_consensus}, window={window}")

        # Query data
        tags: list = get_tag_names(influxdb3_local, measurement, task_id)
        tags_clause: str = ", ".join([f'"{tag}"' for tag in tags])
        end_time: datetime = call_time
        start_time: datetime = end_time - window
        influxdb3_local.info(f"[{task_id}] Querying {measurement}.{field} from {start_time} to {end_time}")
        query: str = f"""
                SELECT "{field}", "time", {tags_clause}
                FROM "{measurement}"
                WHERE time >= $start_time AND time < $end_time
                ORDER BY time
            """
        result: list = influxdb3_local.query(
            query,
            {"start_time": start_time.isoformat(), "end_time": end_time.isoformat()},
        )
        if not result:
            influxdb3_local.info(
                f"[{task_id}] No data found for {measurement}.{field} from {start_time} to {end_time}"
            )
            return
        influxdb3_local.info(f"[{task_id}] Retrieved {len(result)} records from {measurement}")

        # Convert to pandas Series
        df: pd.DataFrame = pd.DataFrame(result)
        if field not in df.columns or "time" not in df.columns:
            influxdb3_local.error(
                f"[{task_id}] Field '{field}' or 'time' not found in query results"
            )
            return
        series: pd.Series = pd.Series(
            df[field].values, index=pd.to_datetime(df["time"], unit="ns")
        )
        series = validate_series(series)  # Ensure regular sampling and time order
        influxdb3_local.info(f"[{task_id}] Prepared time series data with {len(series)} points")

        # Apply detectors
        influxdb3_local.info(f"[{task_id}] Starting anomaly detection with {len(detectors)} detectors")
        anomaly_results: list = []
        for detector_name in detectors:
            try:
                params: dict = detector_params[detector_name]
                influxdb3_local.info(
                    f"[{task_id}] Applying detector {detector_name} with params {params}"
                )
                detector_class = AVAILABLE_DETECTORS[detector_name]
                detector = detector_class(**params)
                if detector_name not in ("ThresholdAD",):
                    detector.fit(series)
                anomalies: pd.Series = detector.detect(series)
                anomaly_results.append(anomalies)
                anomaly_count = anomalies.sum()
                influxdb3_local.info(f"[{task_id}] Detector {detector_name} found {anomaly_count} anomalies")
            except Exception as e:
                influxdb3_local.warn(
                    f"[{task_id}] Failed to apply detector {detector_name}: {e}"
                )

        if not anomaly_results:
            influxdb3_local.error(
                f"[{task_id}] No valid detectors applied to {measurement}.{field}"
            )
            return

        # Consensus: point is anomaly if >= min_consensus detectors agree
        anomaly_df = pd.concat(anomaly_results, axis=1).fillna(False)
        anomaly_count = anomaly_df.sum(axis=1)
        consensus_anomalies = anomaly_count >= min_consensus
        consensus_anomalies = consensus_anomalies.astype(bool)
        total_consensus_anomalies = consensus_anomalies.sum()
        influxdb3_local.info(f"[{task_id}] Consensus analysis: {total_consensus_anomalies} anomalies detected with min_consensus={min_consensus}")

        # Process anomalies with debounce logic
        influxdb3_local.info(f"[{task_id}] Processing anomalies with debounce logic (min_condition_duration={min_condition_duration})")
        processed_anomalies = 0
        sent_notifications = 0
        for idx, is_anomaly in consensus_anomalies.items():
            row: pd.Series = df[df["time"] == pd.Timestamp(idx.isoformat()).value].iloc[
                0
            ]
            time_datetime: datetime = pd.to_datetime(row["time"], unit="ns")
            cache_key: str = generate_cache_key(measurement, field, tags, row)
            tag_str: str = ", ".join(f"{t}={row.get(t, 'None')}" for t in tags)
            start_time_str: str = influxdb3_local.cache.get(cache_key, default="")

            if is_anomaly:
                if not start_time_str:
                    if min_condition_duration > timedelta(0):
                        # Start of a new anomaly
                        influxdb3_local.cache.put(cache_key, time_datetime.isoformat())
                        influxdb3_local.info(
                            f"[{task_id}] Anomaly started for {measurement}.{field} (tags: {tag_str}), waiting for duration {min_condition_duration}"
                        )
                        continue

                    # Send notification
                    payload: dict = {
                        "notification_text": interpolate_notification_text(
                            notification_text,
                            {
                                "table": measurement,
                                "field": field,
                                "value": row[field],
                                "detectors": ".".join(detectors),
                                "tags": tag_str,
                            },
                        ),
                        "senders_config": senders_config,
                    }
                    influxdb3_local.error(
                        f"[{task_id}] Anomaly detected for {measurement}.{field} (tags: {tag_str}), sending alert"
                    )
                    send_notification(
                        influxdb3_local,
                        port_override,
                        notification_path,
                        influxdb3_auth_token,
                        payload,
                        task_id,
                    )
                    sent_notifications += 1
                else:
                    # Check duration
                    duration_start_time: datetime = datetime.fromisoformat(
                        start_time_str
                    )
                    elapsed: timedelta = time_datetime - duration_start_time
                    if elapsed >= min_condition_duration:
                        # Send notification
                        payload: dict = {
                            "notification_text": interpolate_notification_text(
                                notification_text,
                                {
                                    "table": measurement,
                                    "field": field,
                                    "value": row[field],
                                    "detectors": ".".join(detectors),
                                    "tags": tag_str,
                                },
                            ),
                            "senders_config": senders_config,
                        }
                        influxdb3_local.error(
                            f"[{task_id}] Anomaly persisted for {elapsed} for {measurement}.{field} (tags: {tag_str}), sending alert"
                        )
                        send_notification(
                            influxdb3_local,
                            port_override,
                            notification_path,
                            influxdb3_auth_token,
                            payload,
                            task_id,
                        )
                        influxdb3_local.cache.put(
                            cache_key, ""
                        )  # Reset cache after sending
                        sent_notifications += 1
                    else:
                        influxdb3_local.info(
                            f"[{task_id}] Anomaly ongoing for {elapsed} < {min_condition_duration} for {measurement}.{field} (tags: {tag_str})"
                        )
            else:
                # Reset cache if anomaly stops
                if start_time_str:
                    influxdb3_local.cache.put(cache_key, "")
                    influxdb3_local.info(
                        f"[{task_id}] Anomaly cleared for {measurement}.{field} (tags: {tag_str})"
                    )
            processed_anomalies += 1

        influxdb3_local.info(f"[{task_id}] Anomaly processing completed: {processed_anomalies} points processed, {sent_notifications} notifications sent")

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Error: {e}")
