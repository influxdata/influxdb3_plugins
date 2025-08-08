"""
{
    "plugin_type": ["scheduled", "onwrite"],
    "scheduled_args_config": [
        {
            "name": "measurement",
            "example": "cpu",
            "description": "The InfluxDB table (measurement) to monitor.",
            "required": true
        },
        {
            "name": "field_change_count",
            "example": "temp:3.load:2",
            "description": "Dot-separated list of field thresholds (e.g., field:count).",
            "required": true
        },
        {
            "name": "senders",
            "example": "slack.discord",
            "description": "Dot-separated list of notification channels (e.g., slack.discord).",
            "required": true
        },
        {
            "name": "window",
            "example": "1h",
            "description": "Time window for data analysis (e.g., '1h' for 1 hour). Units: 's', 'min', 'h', 'd', 'w'.",
            "required": true
        },
        {
            "name": "influxdb3_auth_token",
            "example": "YOUR_API_TOKEN",
            "description": "API token for InfluxDB 3. Can be set via INFLUXDB3_AUTH_TOKEN environment variable.",
            "required": false
        },
        {
            "name": "notification_text",
            "example": "Field $field in table $table changed $changes times in window $window for tags $tags",
            "description": "Template for notification message with variables $table, $field, $changes, $window, $tags.",
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
            "example": "https://hooks.slack.com/services/...",
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
            "description": "Twilio Service ID. Required if using sms or whatsapp sender.",
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
    ],
    "onwrite_args_config": [
        {
            "name": "measurement",
            "example": "cpu",
            "description": "The InfluxDB table (measurement) to monitor.",
            "required": true
        },
        {
            "name": "field_thresholds",
            "example": "temp:'30.1':10@humidity:'true':2h",
            "description": "Threshold conditions (e.g., field:value:count or field:value:time). Multiple conditions separated by '@'.",
            "required": true
        },
        {
            "name": "senders",
            "example": "slack.discord",
            "description": "Dot-separated list of notification channels.",
            "required": true
        },
        {
            "name": "influxdb3_auth_token",
            "example": "YOUR_API_TOKEN",
            "description": "API token for InfluxDB 3. Can be set via INFLUXDB3_AUTH_TOKEN environment variable.",
            "required": false
        },
        {
            "name": "state_change_window",
            "example": "5",
            "description": "Number of recent values to check for stability. Default: 1.",
            "required": false
        },
        {
            "name": "state_change_count",
            "example": "2",
            "description": "Maximum allowed changes within state_change_window to allow notifications. Default: 1.",
            "required": false
        },
        {
            "name": "notification_count_text",
            "example": "State change detected: Field $field in table $table changed to $value during last $duration times. Row: $row",
            "description": "Template for notification message (when condition with count) with variables $table, $field, $value, $duration, $row.",
            "required": false
        },
        {
            "name": "notification_time_text",
            "example": "State change detected: Field $field in table $table changed to $value during $duration. Row: $row",
            "description": "Template for notification message (when condition with time) with variables $table, $field, $value, $duration, $row.",
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
            "example": "https://hooks.slack.com/services/...",
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
            "description": "Twilio Service ID. Required if using sms or whatsapp sender.",
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
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from string import Template
from urllib.parse import urlparse

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

# List of keywords to exclude from argument validation in AVAILABLE_SENDERS
EXCLUDED_KEYWORDS = ["headers", "token", "sid"]


def get_all_measurements(influxdb3_local) -> list[str]:
    """
    Retrieves a list of all tables of type 'BASE TABLE' from cache or the current InfluxDB database.

    Args:
        influxdb3_local: InfluxDB client instance.

    Returns:
        list[str]: List of table names (e.g., ["cpu", "memory", "disk"]).
    """
    # check cache first
    measurements: list = influxdb3_local.cache.get("measurements")
    if measurements:
        return measurements

    # if not in cache, query the database
    result: list = influxdb3_local.query("SHOW TABLES")
    measurements = [
        row["table_name"] for row in result if row.get("table_type") == "BASE TABLE"
    ]

    # cache the result for 1 hour
    influxdb3_local.cache.put(f"measurements", measurements, 60 * 60)

    return measurements


def get_tag_names(influxdb3_local, measurement: str, task_id: str) -> list[str]:
    """
    Retrieves the list of tag names for a measurement from cache or the database.

    Args:
        influxdb3_local: InfluxDB client instance.
        measurement (str): Name of the measurement to query.
        task_id (str): The task ID.

    Returns:
        list[str]: List of tag names with 'Dictionary(Int32, Utf8)' data type.
    """
    # check cache first
    tags: list = influxdb3_local.cache.get(f"{measurement}_tags")
    if tags:
        return tags

    # if not in cache, query the database
    query = """
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

    # cache the result for 1 hour
    influxdb3_local.cache.put(f"{measurement}_tags", tag_names, 60 * 60)

    return tag_names


def generate_cache_key(
    measurement: str,
    field: str,
    value: int | float | str,
    suffix: str,
    tags: list,
    row: dict,
) -> str:
    """Generate cache key based on input parameters."""
    cache_key: str = f"{measurement}:{field}:{value}:{suffix}"

    for tag in sorted(tags):
        tag_value = row.get(tag, "None")
        cache_key += f":{tag}={tag_value}"

    return cache_key


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


def _coerce_value(raw: str) -> str | int | float | bool:
    """
    Convert a raw string value into int, float, bool, or str.
    """
    raw = raw.strip()
    # Quoted string
    if (raw.startswith('"') and raw.endswith('"')) or (
        raw.startswith("'") and raw.endswith("'")
    ):
        raw = raw[1:-1]
    # Boolean
    if raw.lower() in ("true", "false"):
        return raw.lower() == "true"
    # Integer
    if re.fullmatch(r"-?\d+", raw):
        return int(raw)
    # Float
    if re.fullmatch(r"-?\d+\.\d*", raw):
        return float(raw)
    # Plain string
    return raw


def parse_field_thresholds(
    influxdb3_local,
    args: dict,
    task_id: str,
) -> list:
    """
    Extracts and parses field threshold definitions from args or use values from config file.

    Args:
        influxdb3_local: InfluxDB client instance.
        args (dict): Dictionary with the 'field_thresholds' key.
        task_id (str): Unique identifier for the current task, used for logging.

    The args dict must contain the key "field_thresholds" with a value like:
        'field_name-1:"value 2":10@field_name-2:"value 1":1s'

    Each '@'-separated segment must contain exactly two ':' characters,
    producing three parts: field_name, raw_value, and raw_third.

    - raw_value is coerced into int, float, bool, or str.
    - raw_third is either:
        • A plain integer (e.g., "10")
        • A duration with unit suffix: <number><unit>, where unit ∈ {s, min, h, d, w}

    Valid units and their corresponding timedelta keyword:
        "s"   → "seconds"
        "min" → "minutes"
        "h"   → "hours"
        "d"   → "days"
        "w"   → "weeks"

    Returns:
        A list of lists [field_name, coerced_value, converted_third]:
          - coerced_value is int, float, bool, or str
          - converted_third is int or timedelta

    Example:
        args = {
        ...     "field_thresholds": 'temp:"30":60@humidity:"true":2h'
        ... }
        parse_field_thresholds(args)
        [
            ["temp", 30, 60],
            ["humidity", True, datetime.timedelta(hours=2)]
        ]
    """
    valid_units: dict[str, str] = {
        "s": "seconds",
        "min": "minutes",
        "h": "hours",
        "d": "days",
        "w": "weeks",
    }

    raw_input: str | list = args.get("field_thresholds")
    results: list = []

    if args["use_config_file"]:
        if not isinstance(raw_input, list):
            raise Exception(
                "[{task_id}] field_thresholds must be a list while using config file"
            )
        for threshold in raw_input:
            try:
                field_name = str(threshold[0])
                value = threshold[1]
                duration: str | int = threshold[2]
                if isinstance(duration, str):
                    num_part, unit_part = "", ""
                    for unit in sorted(valid_units.keys(), key=len, reverse=True):
                        if duration.endswith(unit):
                            num_part = duration[: -len(unit)]
                            unit_part = unit
                            break
                    if not num_part or unit_part not in valid_units:
                        influxdb3_local.warn(
                            f"[{task_id}] Invalid duration format '{duration}'"
                        )
                        continue
                    try:
                        num = int(num_part)
                    except ValueError:
                        influxdb3_local.warn(
                            f"[{task_id}] Invalid number in duration '{duration}'"
                        )
                        continue
                    threshold_param: timedelta | int = timedelta(
                        **{valid_units[unit_part]: num}
                    )
                elif isinstance(duration, int):
                    threshold_param = duration
                else:
                    influxdb3_local.warn(
                        f"[{task_id}] Invalid duration format '{duration}'"
                    )
                    continue
                results.append([field_name, value, threshold_param])
            except Exception:
                influxdb3_local.warn(
                    f"[{task_id}] Invalid duration definition: {threshold}, skipping"
                )
        if not results:
            raise Exception(
                f"[{task_id}] No valid field threshold segments found in {raw_input}"
            )
        return results

    segments: list = [seg.strip() for seg in raw_input.split("@") if seg.strip()]
    for segment in segments:
        # Each segment must contain exactly two ':' characters
        if segment.count(":") != 2:
            influxdb3_local.warn(
                f"[{task_id}] Skipping invalid threshold: '{segment}' – must have exactly 2 colons (':')"
            )
            continue

        # Split into three parts: field_name, raw_value, raw_third
        field_name, raw_value, raw_third = segment.split(":", 2)
        field_name = field_name.strip()
        raw_value = raw_value.strip()
        raw_third = raw_third.strip()

        # Coerce raw_value into int, float, bool, or str
        value = _coerce_value(raw_value)

        # Parse raw_third: integer or duration
        if re.fullmatch(r"-?\d+", raw_third):
            third_converted: int | timedelta = int(raw_third)
        else:
            # Attempt duration parsing: <number><unit>
            num_part: str = ""
            unit_part: str = ""
            for unit in sorted(valid_units.keys(), key=len, reverse=True):
                if raw_third.endswith(unit):
                    num_part = raw_third[: -len(unit)]
                    unit_part = unit
                    break

            if not num_part or unit_part not in valid_units:
                influxdb3_local.warn(
                    f"[{task_id}] Invalid duration format: {raw_third}"
                )
                continue

            try:
                num = int(num_part)
            except ValueError:
                influxdb3_local.warn(f"[{task_id}] Invalid duration number: {num_part}")
                continue

            kw: str = valid_units[unit_part]
            third_converted = timedelta(**{kw: num})

        results.append((field_name, value, third_converted))

    if not results:
        raise Exception(
            f"[{task_id}] No valid field threshold segments found in {raw_input}"
        )

    return results


def check_state_changes(cached_values: deque, state_change_count: int) -> bool:
    """
    Checks how many times the value changes in the given deque.

    Args:
        cached_values (deque): A deque of recent field values (size = state_change_window).
        state_change_count (int): Maximum allowed number of changes within the window.

    Returns:
        bool:
            True if the number of value changes in cached_values is <= state_change_count,
            False if it exceeds state_change_count.
    """
    # If fewer than 2 values, there can be no change
    if len(cached_values) < 2:
        return True

    changes: int = 0
    prev = None
    first = True

    for val in cached_values:
        if first:
            prev = val
            first = False
            continue

        if val != prev:
            changes += 1
            if changes >= state_change_count:
                return False
        prev = val

    return True


def process_writes(influxdb3_local, table_batches: list, args: dict | None = None):
    """
    Data write trigger entry point implementing field‐level thresholds with “count” and “duration” logic,
    while also suppressing notifications if the field value has flipped too many times recently.

    When you create a Data Write trigger, point to this file and the function name must be `process_writes`.
    Other names are not supported.

    The trigger fires on each WAL flush. All newly written rows within that flush—optionally filtered by
    a configured measurement—are grouped into `table_batches`.

    Args:
        influxdb3_local:
            InfluxDB client instance (for logging, SQL queries, writing, and cache).
        table_batches (list):
            A list of dicts, each with:
              - "table_name": str
              - "rows": list[dict]  # Each dict is one row of data, containing fields and tags.
        args (dict, optional):
            Must include:
              - "measurement": measurement (table) name to monitor (str).
              - "field_thresholds": string defining thresholds, parsed by `parse_field_thresholds`.
              - "senders": dot-separated list of notification channels (e.g., "slack.sms").
            May also include:
              - "config_file_path": path to config file to override args (str).
              - "state_change_window": integer count of last values to consider for flip detection.
              - "state_change_count": integer threshold of flips to suppress notifications.
              - "port_override": HTTP port for notification plugin (default 8181).
              - "influxdb3_auth_token": API v3 token (or provided via ENV var INFLUXDB3_AUTH_TOKEN).
              - "notification_path": path on engine (default "notify").
              - "notification_text": template for alert text, with placeholders:
                    $table, $field, $value, $duration, $row.

    Raises:
        Exception: Captures and logs any unexpected error (with `influxdb3_local.error`).
    """
    task_id: str = str(uuid.uuid4())
    influxdb3_local.info(f"[{task_id}] Starting writes process with args: {args}")

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
        or "field_thresholds" not in args
        or "senders" not in args
    ):
        influxdb3_local.error(
            f"[{task_id}] Missing required arguments: measurement, field_thresholds, or senders"
        )
        return

    # Parse configuration
    measurement: str = args["measurement"]
    all_measurements: list = get_all_measurements(influxdb3_local)
    if measurement not in all_measurements:
        influxdb3_local.error(
            f"[{task_id}] Measurement '{measurement}' not found in database"
        )
        return

    try:
        field_thresholds: list = parse_field_thresholds(influxdb3_local, args, task_id)
        influxdb3_local.info(f"[{task_id}] Field thresholds: {field_thresholds}")

        senders_config: dict = parse_senders(influxdb3_local, args, task_id)
        tags: list = get_tag_names(influxdb3_local, measurement, task_id)
        port_override: int = parse_port_override(args, task_id)
        state_change_window: int = int(args.get("state_change_window", 1))
        state_change_count: int = int(args.get("state_change_count", 1))
        notification_path: str = args.get("notification_path", "notify")
        influxdb3_auth_token: str = args.get("influxdb3_auth_token") or os.getenv(
            "INFLUXDB3_AUTH_TOKEN"
        )
        if influxdb3_auth_token is None:
            influxdb3_local.error(
                f"[{task_id}] Missing required argument: influxdb3_auth_token"
            )
            return
        notification_count_tpl = args.get(
            "notification_count_text",
            "State change detected: Field $field in table $table changed to $value during last $duration times. Row: $row",
        )
        notification_time_tpl = args.get(
            "notification_time_text",
            "State change detected: Field $field in table $table changed to $value during $duration. Row: $row",
        )

        # Process incoming data
        for table_batch in table_batches:
            # Skip non-matching tables
            if table_batch["table_name"] != measurement:
                continue

            # Process rows in this batch
            for row in table_batch["rows"]:
                for field_name, target_value, threshold_param in field_thresholds:
                    # Get cache keys
                    if isinstance(threshold_param, timedelta):
                        duration_suffix: str = "time"
                    else:
                        duration_suffix = "count"

                    duration_cache_key: str = generate_cache_key(
                        measurement=measurement,
                        field=field_name,
                        value=target_value,
                        suffix=duration_suffix,
                        tags=tags,
                        row=row,
                    )

                    current_val = row.get(field_name)

                    # Only proceed if the field is present
                    if current_val is None:
                        # If field missing, treat as condition failure and reset cache
                        influxdb3_local.info(
                            f"[{task_id}] Field '{field_name}' not present in row. Cache key: {duration_cache_key}. Resetting state."
                        )
                        influxdb3_local.cache.put(duration_cache_key, "")
                        continue

                    # Check if the condition is satisfied: row[field_name] == target_value
                    condition_met: bool = current_val == target_value

                    # Get cache keys
                    values_cache_key: str = generate_cache_key(
                        measurement=measurement,
                        field=field_name,
                        value=target_value,
                        suffix="values",
                        tags=tags,
                        row=row,
                    )
                    # Get cached values
                    cached_values = influxdb3_local.cache.get(
                        values_cache_key, default=deque(maxlen=state_change_window)
                    )
                    # Ensure cached values has correct type and size
                    if (
                        not isinstance(cached_values, deque)
                        or cached_values.maxlen != state_change_window
                    ):
                        cached_values = deque(maxlen=state_change_window)

                    is_sending: bool = check_state_changes(
                        cached_values, state_change_count
                    )
                    cached_values.append(current_val)

                    if duration_suffix == "count":
                        cached_state: int = int(
                            influxdb3_local.cache.get(duration_cache_key, default=0)
                        )

                        if condition_met:
                            cached_state += 1
                            if cached_state >= threshold_param:
                                # Condition met for N consecutive points → trigger alert
                                influxdb3_local.error(
                                    f"[{task_id}] State change detected: {field_name} in table {measurement} changed to {target_value} during last {threshold_param} values. Row: {duration_cache_key}, sending alert"
                                )
                                # Send notification
                                payload: dict = {
                                    "notification_text": interpolate_notification_text(
                                        notification_count_tpl,
                                        {
                                            "table": measurement,
                                            "field": field_name,
                                            "value": target_value,
                                            "duration": threshold_param,
                                            "row": duration_cache_key,
                                        },
                                    ),
                                    "senders_config": senders_config,
                                }

                                if is_sending:
                                    send_notification(
                                        influxdb3_local,
                                        port_override,
                                        notification_path,
                                        influxdb3_auth_token,
                                        payload,
                                        task_id,
                                    )
                                else:
                                    influxdb3_local.warn(
                                        f"[{task_id}] Skipping notification due to unstable data state"
                                    )

                                # Reset count
                                influxdb3_local.cache.put(duration_cache_key, "0")
                            else:
                                # Update count in cache
                                influxdb3_local.cache.put(
                                    duration_cache_key, str(cached_state)
                                )
                                influxdb3_local.warn(
                                    f"[{task_id}] State change detected: {field_name} in table {measurement} changed to {target_value} for {cached_state}/{threshold_param}. Row: {duration_cache_key}, skipping alert"
                                )
                        else:
                            # Condition failed → reset count
                            influxdb3_local.cache.put(duration_cache_key, "0")

                    else:  # duration_suffix == "time"
                        required_duration: timedelta = threshold_param
                        cached_state: str = influxdb3_local.cache.get(
                            duration_cache_key, default=""
                        )

                        if condition_met:
                            # Parse cached start time, if any
                            prev_start_iso: str = cached_state
                            if prev_start_iso:
                                try:
                                    start_time = datetime.fromisoformat(prev_start_iso)
                                except Exception:
                                    start_time = None
                            else:
                                start_time = None

                            # Use current UTC time rather than row's "time" field
                            now = datetime.now(timezone.utc)

                            if not start_time:
                                # First time condition met, store start
                                influxdb3_local.cache.put(
                                    duration_cache_key, now.isoformat()
                                )
                                influxdb3_local.info(
                                    f"[{task_id}] Condition started for row: {duration_cache_key} at {now.isoformat()}"
                                )
                            else:
                                elapsed = now - start_time
                                if elapsed >= required_duration:
                                    influxdb3_local.error(
                                        f"[{task_id}] Threshold duration reached for row: {duration_cache_key}, target_value={target_value} (required {required_duration})"
                                    )
                                    # Send notification
                                    payload: dict = {
                                        "notification_text": interpolate_notification_text(
                                            notification_time_tpl,
                                            {
                                                "table": measurement,
                                                "field": field_name,
                                                "value": target_value,
                                                "duration": threshold_param,
                                                "row": duration_cache_key,
                                            },
                                        ),
                                        "senders_config": senders_config,
                                    }

                                    if is_sending:
                                        send_notification(
                                            influxdb3_local,
                                            port_override,
                                            notification_path,
                                            influxdb3_auth_token,
                                            payload,
                                            task_id,
                                        )
                                    else:
                                        influxdb3_local.warn(
                                            f"[{task_id}] Skipping notification due to unstable data state"
                                        )

                                    # Reset duration cache
                                    influxdb3_local.cache.put(duration_cache_key, "")

                                else:
                                    # Update elapsed (keep original start in cache)
                                    influxdb3_local.warn(
                                        f"[{task_id}] Threshold duration reached for row: {row}, target_value={target_value} with elapsed={elapsed} (required {required_duration})"
                                    )
                        else:
                            # Condition failed → reset any stored start time
                            if cached_state:
                                influxdb3_local.info(
                                    f"[{task_id}] Condition failed for row: {row}, clearing duration cache"
                                )
                            influxdb3_local.cache.put(duration_cache_key, "")

                    influxdb3_local.cache.put(values_cache_key, cached_values)

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Error: {str(e)}")


def parse_field_change_count(
    influxdb3_local, args: dict, task_id: str
) -> dict[str, int]:
    """
    Parses the 'field_change_count' parameter into a dictionary of field names and their change thresholds or use values from config file.

    Args:
        influxdb3_local: InfluxDB client instance.
        args (dict): Dictionary with the 'field_change_count' key.
        task_id (str): Unique task identifier for logging.

    Returns:
        dict[str, int]: Dictionary mapping field names to change counts.

    Raises:
        Exception: If the format is invalid or no valid fields are found.
    """
    raw_input: str | dict = args.get("field_change_count")
    field_counts: dict = {}

    if args["use_config_file"]:
        if not isinstance(raw_input, dict):
            raise Exception(
                f"[{task_id}] field_change_count must be a dictionary when using config file"
            )
        return raw_input

    pairs: list = raw_input.split(".")
    for pair in pairs:
        if ":" not in pair:
            influxdb3_local.warn(
                f"[{task_id}] Invalid format of field_change_count, missing ':' in pair: {pair}"
            )
            continue
        field, count_str = pair.split(":", 1)
        try:
            count: int = int(count_str)
            field_counts[field.strip()] = count
        except ValueError:
            influxdb3_local.warn(
                f"[{task_id}] Invalid format of field_change_count, invalid count: {count_str} in pair: {pair}"
            )
            continue

    if not field_counts:
        raise Exception(f"[{task_id}] No valid entries found in field_change_count.")

    return field_counts


def parse_window(args: dict, task_id: str) -> timedelta:
    """
    Parses the 'window' argument from args and converts it into a timedelta object.

    Args:
        args (dict): Dictionary with the 'window' key (e.g., {"window": "2h"}).
        task_id (str): Unique task identifier.

    Returns:
        timedelta: Parsed time interval.

    Raises:
        Exception: If window is missing or has an invalid format.
    """
    valid_units: dict = {
        "s": "seconds",
        "min": "minutes",
        "h": "hours",
        "d": "days",
        "w": "weeks",
    }

    window: str | None = args.get("window")

    match = re.fullmatch(r"(\d+)([a-zA-Z]+)", window)
    if match:
        number, unit = match.groups()
        number = int(number)
        if number >= 1 and unit in valid_units:
            return timedelta(**{valid_units[unit]: number})

    raise Exception(f"[{task_id}] Invalid interval format: {window}.")


def build_query(measurement: str, start_time: datetime, end_time: datetime) -> str:
    """
    Builds an SQL query to select all data from a measurement within a time range.

    Args:
        measurement (str): Name of the measurement/table.
        start_time (datetime): Start time (inclusive).
        end_time (datetime): End time (exclusive).

    Returns:
        str: SQL query string selecting all data between start_time and end_time.
    """
    start_iso: str = start_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso: str = end_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    query = f"""
        SELECT *
        FROM '{measurement}'
        WHERE time >= '{start_iso}'
        AND time < '{end_iso}'
        ORDER BY time ASC
    """
    return query


def process_scheduled_call(
    influxdb3_local, call_time: datetime, args: dict | None = None
) -> None:
    """
    Entry point for the InfluxDB scheduler plugin that monitors field changes
    in a measurement and triggers notifications if a threshold is exceeded.

    It queries a specified measurement within a time window, detects how many times certain
    fields have changed, and sends notifications if those changes exceed
    predefined thresholds.

    Args:
        influxdb3_local: Instance of the InfluxDB client used for querying
            and logging.
        call_time (datetime): The UTC timestamp at which the scheduler triggers
            this function. This defines the end of the time window.
        args (dict, optional): Dictionary containing the following required keys:
            - config_file_path (str): path to config file to override args.
            - measurement (str): The name of the measurement to query.
            - field_change_count (dict): Mapping of field names to change thresholds.
            - senders (dict): Configuration for notification senders.
            - window (str | int): Duration to look back from `call_time`.
            - influxdb3_auth_token (str, optional): Token for authentication.
            - notification_path (str, optional): Endpoint path for notifications.
            - notification_text (str, optional): Template for the alert message.
            - port_override (int, optional): Custom port for notification endpoint.

    Raises:
        No exceptions are raised directly; all errors are caught and logged.
    """
    task_id: str = str(uuid.uuid4())
    influxdb3_local.info(f"[{task_id}] Starting scheduled field change check at {call_time} with args: {args}.")

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

    # Check for required arguments
    if (
        not args
        or "measurement" not in args
        or "field_change_count" not in args
        or "senders" not in args
        or "window" not in args
    ):
        influxdb3_local.error(
            f"[{task_id}] Missing required arguments: measurement, field_change_count, senders, or window"
        )
        return

    measurement: str = args["measurement"]
    all_measurements: list = get_all_measurements(influxdb3_local)
    if measurement not in all_measurements:
        influxdb3_local.error(
            f"[{task_id}] Measurement '{measurement}' not found in database"
        )
        return

    try:
        # Extract and validate parameters
        field_counts: dict = parse_field_change_count(influxdb3_local, args, task_id)
        influxdb3_local.info(f"[{task_id}] Field change counts: {field_counts}")

        senders_config: dict = parse_senders(influxdb3_local, args, task_id)
        tags: list = get_tag_names(influxdb3_local, measurement, task_id)
        window: timedelta = parse_window(args, task_id)
        notification_path: str = args.get("notification_path", "notify")
        port_override: int = parse_port_override(args, task_id)
        influxdb3_auth_token: str = args.get("influxdb3_auth_token") or os.getenv(
            "INFLUXDB3_AUTH_TOKEN"
        )
        if influxdb3_auth_token is None:
            influxdb3_local.error(
                f"[{task_id}] Missing required argument: influxdb3_auth_token"
            )
            return
        notification_tpl: str = args.get(
            "notification_text",
            "Field $field in table $table changed $changes times in window $window for tags $tags",
        )

        # Calculate time range
        end_time: datetime = call_time.replace(tzinfo=timezone.utc)
        start_time: datetime = end_time - window
        influxdb3_local.info(f"[{task_id}] Querying '{measurement}' from {start_time} to {end_time}")

        # Build query to get data
        query: str = build_query(measurement, start_time, end_time)
        results: list = influxdb3_local.query(query)
        if not results:
            influxdb3_local.info(
                f"[{task_id}] No data found in '{measurement}' from {start_time} to {end_time}."
            )
            return
        influxdb3_local.info(f"[{task_id}] Retrieved {len(results)} records from {measurement}")

        # Group data by unique tag combinations
        tag_combinations = defaultdict(list)
        for row in results:
            tag_values = tuple(row[tag] if tag in row else "None" for tag in tags)
            tag_combinations[tag_values].append(row)

        # Process each tag combination
        for tag_values, rows in tag_combinations.items():
            for field, count_threshold in field_counts.items():
                # Count changes
                changes: int = 0
                prev_value = None
                for row in rows:
                    current_value = row.get(field)
                    if current_value is None:
                        continue
                    if prev_value is not None and current_value != prev_value:
                        changes += 1
                    prev_value = current_value

                if changes >= count_threshold:
                    influxdb3_local.error(
                        f"[{task_id}] Found {count_threshold} changes in field '{field}' for tags {tag_values}, sending alert..."
                    )
                    # Send notification
                    tag_str = ", ".join(
                        f"{tag}={value}" for tag, value in zip(tags, tag_values)
                    )
                    payload = {
                        "notification_text": interpolate_notification_text(
                            notification_tpl,
                            {
                                "table": measurement,
                                "field": field,
                                "changes": changes,
                                "window": window,
                                "tags": tag_str,
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
        influxdb3_local.error(f"[{task_id}] Error: {str(e)}")
