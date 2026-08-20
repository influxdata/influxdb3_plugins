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
            "example": "temp:3.disk.used:2",
            "description": "Dot-separated list of field thresholds (e.g., field:count). Each count must be 1 or greater.",
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
            "description": "Time window for data analysis (e.g., '1h' for 1 hour). Units: 'us', 'ms', 's', 'min', 'h', 'd', 'w'. Must be a positive duration.",
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
            "description": "Path to a TOML config file that replaces the trigger arguments entirely. Format: 'config.toml'.",
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
            "description": "Threshold conditions (e.g., field:value:count or field:value:time). Multiple conditions separated by '@'. Count must be 1 or greater; duration units: 'us', 'ms', 's', 'min', 'h', 'd', 'w'.",
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
            "description": "Number of recent values to check for stability. The stability check applies only when this is 2 or greater. Default: 1.",
            "required": false
        },
        {
            "name": "state_change_count",
            "example": "2",
            "description": "Number of changes within state_change_window at which notifications start being suppressed. Default: 1.",
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
            "description": "Path to a TOML config file that replaces the trigger arguments entirely. Format: 'config.toml'.",
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
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from string import Template
from urllib.parse import urlparse

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

# List of keywords to exclude from argument validation in AVAILABLE_SENDERS
EXCLUDED_KEYWORDS = ["headers", "token", "sid"]

_DEFAULT_NOTIFICATION_TEXT = (
    "Field $field in table $table changed $changes times in window $window "
    "for tags $tags"
)
_DEFAULT_COUNT_TEXT = (
    "State change detected: Field $field in table $table changed to $value "
    "during last $duration times. Row: $row"
)
_DEFAULT_TIME_TEXT = (
    "State change detected: Field $field in table $table changed to $value "
    "during $duration. Row: $row"
)

_CHANGE_COUNT_PAIR_RE = re.compile(r"(?P<field>[^:]+):\s*(?P<count>-?\d+)\s*(?:\.|$)")


def parse_window(raw) -> timedelta:
    """Parse the analysis window, rejecting non-positive durations."""
    window: timedelta = parse_timedelta(raw)
    if window <= timedelta(0):
        raise ValueError(f"Invalid window: {raw!r} (must be a positive duration)")
    return window


_COMMON_VALIDATORS = [
    Validator("measurement", required=True, cast=str),
    Validator("senders", required=True),
    Validator(
        "port_override",
        default=8181,
        cast=lambda raw: parse_int(raw, minimum=1, maximum=65535),
    ),
    Validator("notification_path", default="notify", cast=str),
]

_WRITES_VALIDATORS = _COMMON_VALIDATORS + [
    Validator("field_thresholds", required=True),
    Validator(
        "state_change_window", default=1, cast=lambda raw: parse_int(raw, minimum=0)
    ),
    Validator(
        "state_change_count", default=1, cast=lambda raw: parse_int(raw, minimum=0)
    ),
    Validator("notification_count_text", default=_DEFAULT_COUNT_TEXT, cast=str),
    Validator("notification_time_text", default=_DEFAULT_TIME_TEXT, cast=str),
]

_SCHEDULED_VALIDATORS = _COMMON_VALIDATORS + [
    Validator("field_change_count", required=True),
    Validator("window", required=True, cast=parse_window),
    Validator("notification_text", default=_DEFAULT_NOTIFICATION_TEXT, cast=str),
]

_WRITES_CONFIG_CACHE_KEY = "state_change:writes_config"
_WRITES_CONFIG_TTL_SECONDS = 10 * 60


def _load_config(
    influxdb3_local, args: dict, validators: list, task_id: str
) -> dict | None:
    """
    Load the plugin configuration, applying defaults and type casts.

    Args:
        influxdb3_local: InfluxDB client instance.
        args (dict): Runtime arguments of the trigger.
        validators (list): Validators providing defaults and casts for the mode.
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
            validators=validators,
            env_keys=["INFLUXDB3_AUTH_TOKEN"],
            source="toml" if config_file_path else "args",
        )
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to load configuration: {e}")
        return None

    return {key.lower(): value for key, value in loaded.as_dict().items()}


def get_measurement_tags(influxdb3_local, measurement: str, task_id: str) -> list[str]:
    """Return the cached tag names of a measurement, logging when it has none."""
    tags: list[str] = get_tag_names(influxdb3_local, measurement)
    if not tags:
        tags = get_tag_names(influxdb3_local, measurement, use_cache=False)
    if not tags:
        influxdb3_local.info(
            f"[{task_id}] No tags found for measurement '{measurement}'."
        )
    return tags


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


def read_counter(influxdb3_local, cache_key: str) -> int:
    """Read a breach counter, treating a missing or non-numeric entry as zero."""
    try:
        return int(influxdb3_local.cache.get(cache_key))
    except (TypeError, ValueError):
        return 0


def parse_senders(influxdb3_local, config: dict, task_id: str) -> dict:
    """
    Parse and validate sender configurations from the loaded config.

    Args:
        influxdb3_local: InfluxDB client instance.
        config (dict): Loaded config containing "senders" and related settings.
        task_id (str): Unique task identifier.

    Returns:
        dict: A mapping `{sender_type: {key: value}}` for each valid sender.

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

    Request failures and non-2xx responses are retried; after the final attempt
    the error is logged and the alert is dropped.
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


def _parse_threshold_param(
    influxdb3_local, raw, task_id: str
) -> int | timedelta | None:
    """
    Parse the third part of a threshold into a consecutive count or a duration.

    A bare integer is a count of consecutive matching points; anything else is a
    duration such as '10s' or '2h'.

    Returns:
        int | timedelta | None: The parsed threshold, or None when it is invalid.
    """
    if isinstance(raw, bool):
        influxdb3_local.warn(f"[{task_id}] Invalid threshold parameter: {raw!r}")
        return None

    if isinstance(raw, int) or re.fullmatch(r"-?\d+", str(raw).strip()):
        count = int(raw)
        if count < 1:
            influxdb3_local.warn(
                f"[{task_id}] Invalid threshold count {count}, must be 1 or greater"
            )
            return None
        return count

    try:
        duration: timedelta = parse_timedelta(raw)
    except ValueError as e:
        influxdb3_local.warn(f"[{task_id}] Invalid threshold duration {raw!r}: {e}")
        return None

    if duration <= timedelta(0):
        influxdb3_local.warn(
            f"[{task_id}] Invalid threshold duration {raw!r}, must be positive"
        )
        return None
    return duration


def _thresholds_from_entries(influxdb3_local, entries: list, task_id: str) -> list:
    """Parse thresholds given as [field, value, count_or_duration] entries."""
    thresholds: list = []

    for entry in entries:
        if not isinstance(entry, (list, tuple)) or len(entry) != 3:
            influxdb3_local.warn(
                f"[{task_id}] Invalid threshold '{entry}', expected [field, value, count_or_duration]"
            )
            continue
        threshold_param = _parse_threshold_param(influxdb3_local, entry[2], task_id)
        if threshold_param is None:
            continue
        thresholds.append((str(entry[0]), entry[1], threshold_param))

    return thresholds


def _thresholds_from_string(influxdb3_local, raw: str, task_id: str) -> list:
    """Parse thresholds given as '<field>:<value>:<count_or_duration>' joined by '@'."""
    thresholds: list = []

    for segment in parse_delimited_list(raw, sep="@"):
        # Each segment must contain exactly two ':' characters
        if segment.count(":") != 2:
            influxdb3_local.warn(
                f"[{task_id}] Skipping invalid threshold: '{segment}' – must have exactly 2 colons (':')"
            )
            continue

        field_name, raw_value, raw_param = segment.split(":", 2)
        threshold_param = _parse_threshold_param(
            influxdb3_local, raw_param.strip(), task_id
        )
        if threshold_param is None:
            continue
        thresholds.append(
            (field_name.strip(), _coerce_value(raw_value), threshold_param)
        )

    return thresholds


def parse_field_thresholds(influxdb3_local, config: dict, task_id: str) -> list:
    """
    Parse the field thresholds used by the data write trigger.

    Thresholds come either as entries of [field, value, count_or_duration] (TOML) or
    as a string of '<field>:<value>:<count_or_duration>' expressions separated by '@'.

    Args:
        influxdb3_local: InfluxDB client instance.
        config (dict): Loaded config containing "field_thresholds".
        task_id (str): Unique task identifier.

    Returns:
        list[tuple]: Tuples of (field_name, target_value, count_or_duration).

    Example:
        'temp:"30":60@humidity:"true":2h'
        [
            ("temp", 30, 60),
            ("humidity", True, datetime.timedelta(hours=2)),
        ]
    """
    raw: str | list = config["field_thresholds"]

    if isinstance(raw, (list, tuple)):
        thresholds = _thresholds_from_entries(influxdb3_local, raw, task_id)
    elif isinstance(raw, str):
        thresholds = _thresholds_from_string(influxdb3_local, raw, task_id)
    else:
        raise Exception(
            "'field_thresholds' must be a list of entries or a string, "
            f"got {type(raw).__name__}"
        )

    if not thresholds:
        raise Exception("No valid field thresholds provided.")
    return thresholds


def _change_counts_from_string(influxdb3_local, raw: str, task_id: str) -> list:
    """
    Split a string of 'field:count' pairs joined by '.' into (field, count) tuples.

    Example:
        'temp:3.disk.used:2' -> [('temp', '3'), ('disk.used', '2')]
    """
    pairs: list = []
    text: str = raw.strip()
    position: int = 0

    while position < len(text):
        match = _CHANGE_COUNT_PAIR_RE.match(text, position)
        if match:
            pairs.append((match.group("field"), match.group("count")))
            position = match.end()
            continue

        next_dot: int = text.find(".", position)
        skipped: str = text[position:] if next_dot == -1 else text[position:next_dot]
        influxdb3_local.warn(
            f"[{task_id}] Invalid format of field_change_count, expected 'field:count' in: {skipped}"
        )
        if next_dot == -1:
            break
        position = next_dot + 1

    return pairs


def parse_field_change_count(
    influxdb3_local, config: dict, task_id: str
) -> dict[str, int]:
    """
    Parse the per-field change thresholds used by the scheduled trigger.

    Thresholds come either as a mapping of {field: count} (TOML) or as a string of
    'field:count' pairs separated by '.'.

    Args:
        influxdb3_local: InfluxDB client instance.
        config (dict): Loaded config containing "field_change_count".
        task_id (str): Unique task identifier.

    Returns:
        dict[str, int]: Field names mapped to their change count thresholds.

    Raises:
        Exception: If the value has an unsupported type or no valid fields are found.
    """
    raw: str | dict = config["field_change_count"]

    if isinstance(raw, dict):
        pairs: list = list(raw.items())
    elif isinstance(raw, str):
        pairs = _change_counts_from_string(influxdb3_local, raw, task_id)
    else:
        raise Exception(
            "'field_change_count' must be a mapping or a string, "
            f"got {type(raw).__name__}"
        )

    field_counts: dict = {}
    for field, raw_count in pairs:
        try:
            field_counts[str(field).strip()] = parse_int(raw_count, minimum=1)
        except ValueError as e:
            influxdb3_local.warn(
                f"[{task_id}] Invalid change count for field '{field}': {e}"
            )

    if not field_counts:
        raise Exception("No valid entries found in field_change_count.")
    return field_counts


def check_state_changes(cached_values: deque, state_change_count: int) -> bool:
    """
    Checks how many times the value changes in the given deque.

    Args:
        cached_values (deque): A deque of recent field values (size = state_change_window).
        state_change_count (int): Number of changes at which notifications are suppressed.

    Returns:
        bool:
            True while the number of value changes in cached_values stays below
            state_change_count, False once it reaches it.
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


def process_writes(influxdb3_local, table_batches: list, args: dict):
    """
    Data write trigger entry point implementing field-level thresholds with "count" and
    "duration" logic, while also suppressing notifications if the field value has flipped
    too many times recently.

    When you create a Data Write trigger, point to this file and the function name must be
    `process_writes`. Other names are not supported.

    The trigger fires on each WAL flush. All newly written rows within that flush are grouped
    into `table_batches`; only batches of the configured measurement are processed.

    Args:
        influxdb3_local: InfluxDB client instance (for logging, SQL queries, and cache).
        table_batches (list): Dicts with "table_name" (str) and "rows" (list[dict]).
        args (dict): Runtime arguments of the trigger.
    """
    if not table_batches:
        return

    task_id: str = str(uuid.uuid4())
    config: dict | None = influxdb3_local.cache.get(_WRITES_CONFIG_CACHE_KEY)
    if config is None:
        config = _load_config(influxdb3_local, args, _WRITES_VALIDATORS, task_id)
        if config is None:
            return
        influxdb3_local.cache.put(
            _WRITES_CONFIG_CACHE_KEY, config, _WRITES_CONFIG_TTL_SECONDS
        )

    measurement: str = config["measurement"]
    all_measurements: list = get_table_names(influxdb3_local)
    if measurement not in all_measurements:
        influxdb3_local.error(
            f"[{task_id}] Measurement '{measurement}' not found in database"
        )
        return

    monitored_batches: list = [
        table_batch
        for table_batch in table_batches
        if table_batch["table_name"] == measurement
    ]
    if not monitored_batches:
        return

    influxdb3_local.info(f"[{task_id}] Starting writes process")

    try:
        field_thresholds: list = parse_field_thresholds(
            influxdb3_local, config, task_id
        )
        influxdb3_local.info(f"[{task_id}] Field thresholds: {field_thresholds}")

        senders_config: dict = parse_senders(influxdb3_local, config, task_id)
        port_override: int = config["port_override"]
        state_change_window: int = config["state_change_window"]
        state_change_count: int = config["state_change_count"]
        notification_path: str = config["notification_path"]
        influxdb3_auth_token: str = (
            config.get("influxdb3_auth_token")
            or os.getenv("INFLUXDB3_AUTH_TOKEN")
            or ""
        )
        if not influxdb3_auth_token:
            influxdb3_local.error(
                f"[{task_id}] Missing required argument: influxdb3_auth_token"
            )
            return
        notification_count_tpl: str = config["notification_count_text"]
        notification_time_tpl: str = config["notification_time_text"]

        tags: list = get_measurement_tags(influxdb3_local, measurement, task_id)

        for table_batch in monitored_batches:
            for row in table_batch["rows"]:
                for field_name, target_value, threshold_param in field_thresholds:
                    is_duration: bool = isinstance(threshold_param, timedelta)
                    duration_suffix: str = "time" if is_duration else "count"
                    reset_value: str = "" if is_duration else "0"

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
                        influxdb3_local.cache.put(duration_cache_key, reset_value)
                        continue

                    # Check if the condition is satisfied: row[field_name] == target_value
                    condition_met: bool = current_val == target_value

                    values_cache_key: str = generate_cache_key(
                        measurement=measurement,
                        field=field_name,
                        value=target_value,
                        suffix="values",
                        tags=tags,
                        row=row,
                    )
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

                    if not is_duration:
                        cached_state: int = read_counter(
                            influxdb3_local, duration_cache_key
                        )

                        if condition_met:
                            cached_state += 1
                            if cached_state >= threshold_param:
                                # Condition met for N consecutive points → trigger alert
                                influxdb3_local.error(
                                    f"[{task_id}] State change detected: {field_name} in table {measurement} changed to {target_value} during last {threshold_param} values. Row: {duration_cache_key}, sending alert"
                                )
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

                    else:
                        required_duration: timedelta = threshold_param
                        prev_start_iso: str = influxdb3_local.cache.get(
                            duration_cache_key, default=""
                        )

                        if condition_met:
                            start_time = None
                            if prev_start_iso:
                                try:
                                    start_time = datetime.fromisoformat(prev_start_iso)
                                except Exception:
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
                                    # Keep the original start in cache and wait
                                    influxdb3_local.warn(
                                        f"[{task_id}] Condition still holding for row: {duration_cache_key}, target_value={target_value} with elapsed={elapsed} (required {required_duration})"
                                    )
                        else:
                            # Condition failed → reset any stored start time
                            if prev_start_iso:
                                influxdb3_local.info(
                                    f"[{task_id}] Condition failed for row: {duration_cache_key}, clearing duration cache"
                                )
                            influxdb3_local.cache.put(duration_cache_key, "")

                    influxdb3_local.cache.put(values_cache_key, cached_values)

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Error: {str(e)}")


def process_scheduled_call(influxdb3_local, call_time: datetime, args: dict) -> None:
    """
    Scheduled trigger entry point that counts how often fields change within a time window
    and sends a notification when a field exceeds its configured change threshold.

    Args:
        influxdb3_local: InfluxDB client instance used for querying and logging.
        call_time (datetime): UTC timestamp of the scheduled run; the end of the window.
        args (dict): Runtime arguments of the trigger.
    """
    task_id: str = str(uuid.uuid4())
    influxdb3_local.info(
        f"[{task_id}] Starting scheduled field change check at {call_time}"
    )

    config: dict | None = _load_config(
        influxdb3_local, args, _SCHEDULED_VALIDATORS, task_id
    )
    if config is None:
        return

    measurement: str = config["measurement"]
    all_measurements: list = get_table_names(influxdb3_local)
    if measurement not in all_measurements:
        influxdb3_local.error(
            f"[{task_id}] Measurement '{measurement}' not found in database"
        )
        return

    try:
        field_counts: dict = parse_field_change_count(influxdb3_local, config, task_id)
        influxdb3_local.info(f"[{task_id}] Field change counts: {field_counts}")

        senders_config: dict = parse_senders(influxdb3_local, config, task_id)
        port_override: int = config["port_override"]
        notification_path: str = config["notification_path"]
        influxdb3_auth_token: str = (
            config.get("influxdb3_auth_token")
            or os.getenv("INFLUXDB3_AUTH_TOKEN")
            or ""
        )
        if not influxdb3_auth_token:
            influxdb3_local.error(
                f"[{task_id}] Missing required argument: influxdb3_auth_token"
            )
            return
        notification_tpl: str = config["notification_text"]

        tags: list = get_measurement_tags(influxdb3_local, measurement, task_id)
        window: timedelta = config["window"]
        end_time: datetime = call_time.replace(tzinfo=timezone.utc)
        start_time: datetime = end_time - window
        influxdb3_local.info(
            f"[{task_id}] Querying '{measurement}' from {start_time} to {end_time}"
        )

        results: list = query_window(
            influxdb3_local,
            measurement,
            start=start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end=end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        if not results:
            influxdb3_local.info(
                f"[{task_id}] No data found in '{measurement}' from {start_time} to {end_time}."
            )
            return
        influxdb3_local.info(
            f"[{task_id}] Retrieved {len(results)} records from {measurement}"
        )

        # Group data by unique tag combinations
        tag_combinations = defaultdict(list)
        for row in results:
            tag_values = tuple(row.get(tag, "None") for tag in tags)
            tag_combinations[tag_values].append(row)

        for tag_values, rows in tag_combinations.items():
            for field, count_threshold in field_counts.items():
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
                        f"[{task_id}] Found {changes} changes (threshold {count_threshold}) in field '{field}' for tags {tag_values}, sending alert..."
                    )
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