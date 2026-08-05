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
            "name": "senders",
            "example": "slack.discord",
            "description": "Dot-separated list of notification channels (e.g., slack.discord). Supported channels: slack, discord, sms, whatsapp, http.",
            "required": true
        },
        {
            "name": "influxdb3_auth_token",
            "example": "YOUR_API_TOKEN",
            "description": "API token for InfluxDB 3. Can be set via INFLUXDB3_AUTH_TOKEN environment variable.",
            "required": false
        },
        {
            "name": "window",
            "example": "5min",
            "description": "Time window to check for data (e.g., '5min' for 5 minutes). Valid units: s, min, h, d, w. Must be a positive duration.",
            "required": true
        },
        {
            "name": "interval",
            "example": "10min",
            "description": "Time interval for aggregation (e.g., '10min'). Used in DATE_BIN for aggregation-based checks. Default: '5min'.",
            "required": false
        },
        {
            "name": "trigger_count",
            "example": "3",
            "description": "Number of condition breaches before sending an alert. Threshold checks count consecutive breaches per row identifier, including across the time bins of a single run; deadman checks count consecutive runs without data. Default: 1.",
            "required": false
        },
        {
            "name": "notification_deadman_text",
            "example": "Deadman Alert: No data received from $table from $time_from to $time_to.",
            "description": "Template for deadman notification message with variables $table, $time_from, $time_to.",
            "required": false
        },
        {
            "name": "notification_threshold_text",
            "example": "[$level] Threshold Alert on table $table: $aggregation of $field $op_sym $compare_val (actual: $actual) — matched in row $row.",
            "description": "Template for threshold notification message with variables $level, $table, $field, $aggregation, $op_sym, $compare_val, $actual, $row.",
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
            "name": "deadman_check",
            "example": "True",
            "description": "Boolean flag to enable deadman checks. If True, checks for absence of data. Default: False.",
            "required": false
        },
        {
            "name": "field_aggregation_values",
            "example": "temp:avg@>=30-ERROR field2:min@<5.0-INFO cpu:stddev@>10-WARN",
            "description": "Aggregation conditions for threshold checks (e.g., field:aggregation@operator value-level). Supported aggregations: avg, count, sum, min, max, median, stddev, first_value, last_value, var, approx_median. Multiple conditions separated by spaces.",
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
    ],
    "onwrite_args_config": [
        {
            "name": "measurement",
            "example": "cpu",
            "description": "The InfluxDB table (measurement) to monitor.",
            "required": true
        },
        {
            "name": "field_conditions",
            "example": "temp>'30.0'-WARN:status=='ok'-INFO",
            "description": "Conditions for triggering alerts (e.g., field operator value-level).",
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
            "name": "trigger_count",
            "example": "2",
            "description": "Number of times the condition must be met before sending an alert. Default: 1.",
            "required": false
        },
        {
            "name": "notification_text",
            "example": "[$level] InfluxDB 3 alert triggered. Condition $field $op_sym $compare_val matched ($actual)",
            "description": "Template for the notification message with variables $level, $field, $op_sym, $compare_val, $actual.",
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
import operator
import os
import random
import re
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from string import Template
from urllib.parse import urlparse

import requests
from influxdata_plugin_utils.config import Validator, load_plugin_config
from influxdata_plugin_utils.introspection import get_table_names, get_tag_names
from influxdata_plugin_utils.parsing import (
    parse_bool,
    parse_delimited_list,
    parse_int,
    parse_timedelta,
)

# Supported comparison operators
_OP_FUNCS = {
    ">": operator.gt,
    "<": operator.lt,
    ">=": operator.ge,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
}

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

# Alert severity levels accepted in conditions
ALLOWED_MESSAGE_LEVELS = ("INFO", "WARN", "ERROR", "CRITICAL")

# Aggregations supported in field_aggregation_values
AVAILABLE_AGGREGATIONS = (
    "avg",
    "count",
    "sum",
    "min",
    "max",
    "median",
    "stddev",
    "first_value",
    "last_value",
    "var",
    "approx_median",
)

_DEFAULT_NOTIFICATION_TEXT = (
    "[$level] InfluxDB 3 alert triggered. Condition $field $op_sym $compare_val "
    "matched $trigger_count times($actual) — matched in row $row."
)
_DEFAULT_DEADMAN_TEXT = (
    "Deadman Alert: No data received from $table from $time_from to $time_to."
)
_DEFAULT_THRESHOLD_TEXT = (
    "[$level] Threshold Alert on table $table: $aggregation of $field $op_sym "
    "$compare_val (actual: $actual) — matched in row $row."
)


def parse_window(raw) -> timedelta:
    """Parse a check window, rejecting non-positive durations."""
    window: timedelta = parse_timedelta(raw)
    if window <= timedelta(0):
        raise ValueError(f"Invalid window: {raw!r} (must be a positive duration)")
    return window


_COMMON_VALIDATORS = [
    Validator("measurement", required=True, cast=str),
    Validator("senders", required=True),
    Validator("trigger_count", default=1, cast=lambda raw: parse_int(raw, minimum=1)),
    Validator(
        "port_override",
        default=8181,
        cast=lambda raw: parse_int(raw, minimum=1, maximum=65535),
    ),
    Validator("notification_path", default="notify", cast=str),
]

_WRITES_VALIDATORS = _COMMON_VALIDATORS + [
    Validator("field_conditions", required=True),
    Validator("notification_text", default=_DEFAULT_NOTIFICATION_TEXT, cast=str),
]

_SCHEDULED_VALIDATORS = _COMMON_VALIDATORS + [
    Validator("deadman_check", default=False, cast=parse_bool),
    Validator("window", required=True, cast=parse_window),
    Validator("interval", default="5min", cast=parse_timedelta),
    Validator("notification_deadman_text", default=_DEFAULT_DEADMAN_TEXT, cast=str),
    Validator("notification_threshold_text", default=_DEFAULT_THRESHOLD_TEXT, cast=str),
]

_WRITES_CONFIG_CACHE_KEY = "thresholds:writes_config"
_WRITES_CONFIG_TTL_SECONDS = 10 * 60


def _load_config(
    influxdb3_local, args: dict, validators: list, task_id: str
) -> dict | None:
    """
    Load the plugin configuration, applying defaults and type casts.

    A TOML file referenced by 'config_file_path' replaces the inline arguments;
    INFLUXDB3_AUTH_TOKEN from the environment is used when the token is not
    configured explicitly.

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
        # an empty list stays cached for an hour and would hide tags added later
        tags = get_tag_names(influxdb3_local, measurement, use_cache=False)
    if not tags:
        influxdb3_local.info(
            f"[{task_id}] No tags found for measurement '{measurement}'."
        )
    return tags


def parse_senders(influxdb3_local, config: dict, task_id: str) -> dict:
    """
    Parse and validate sender configurations from the loaded config.

    Args:
        influxdb3_local: InfluxDB client instance.
        config (dict): Loaded config containing "senders" and related settings.
        task_id (str): Unique task identifier.

    Returns:
        dict: A dictionary of validated sender configurations.

    Raises:
        Exception: If no valid senders are found.
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
                    f"[{task_id}] Missing required argument for {sender}: {key}"
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
    # String
    return raw


def _conditions_from_entries(influxdb3_local, entries: list, task_id: str) -> list:
    """Parse field conditions given as [field, operator, value, level] entries."""
    conditions: list = []

    for part in entries:
        if not isinstance(part, (list, tuple)) or len(part) != 4:
            influxdb3_local.warn(
                f"[{task_id}] Invalid condition '{part}', expected [field, operator, value, level]"
            )
            continue
        field: str = str(part[0])
        op: str = str(part[1])
        if op not in _OP_FUNCS:
            influxdb3_local.warn(
                f"[{task_id}] Unsupported operator '{op}' in condition '{part}'"
            )
            continue
        value = part[2]
        level: str = str(part[3]).strip().upper()
        if level not in ALLOWED_MESSAGE_LEVELS:
            influxdb3_local.warn(
                f"[{task_id}] Invalid message level '{part[3]}' in condition '{part}'"
            )
            continue
        conditions.append((field, op, _OP_FUNCS[op], value, level))

    return conditions


def _conditions_from_string(influxdb3_local, raw: str, task_id: str) -> list:
    """Parse field conditions given as '<field><op><value>-<level>' joined by ':'."""
    conditions: list = []

    for part in raw.split(":"):
        part = part.strip()
        if not part:
            continue

        # Extract message level (optional)
        if "-" not in part:
            influxdb3_local.warn(
                f"[{task_id}] Invalid field_conditions in condition '{part}', should contain '-'"
            )
            continue

        cond_expr, level = part.rsplit("-", 1)
        level = level.strip().upper()
        if level not in ALLOWED_MESSAGE_LEVELS:
            influxdb3_local.warn(
                f"[{task_id}] Invalid message level '{level}' in condition '{part}'"
            )
            continue

        # Parse field/operator/value
        m = re.match(r"^([A-Za-z0-9_.-]+)\s*(>=|<=|==|!=|>|<)\s*(.+)$", cond_expr)
        if not m:
            influxdb3_local.warn(f"[{task_id}] Invalid condition format: '{part}'")
            continue
        field, op, raw_val = m.groups()

        if op not in _OP_FUNCS:
            influxdb3_local.warn(
                f"[{task_id}] Unsupported operator '{op}' in condition '{part}'"
            )
            continue

        value = _coerce_value(raw_val)
        conditions.append((field, op, _OP_FUNCS[op], value, level))

    return conditions


def parse_field_conditions(influxdb3_local, config: dict, task_id: str) -> list:
    """
    Parse the field conditions used by the data write trigger.

    Conditions come either as entries of [field, operator, value, level] (TOML) or
    as a string of '<field><op><value>-<level>' expressions separated by ':'.

    Args:
        influxdb3_local: InfluxDB client instance.
        config (dict): Loaded config containing "field_conditions".
        task_id (str): Unique task identifier.

    Returns:
        list[tuple]: Tuples of (field_name, operator, operator_fn, compare_value, level).

    Raises:
        Exception: If the value has an unsupported type or no valid conditions are found.

    Example:
        "temp>30-ERROR:status=='ok'-INFO:count<=100-WARN"
        [
            ("temp", ">", operator.gt, 30, "ERROR"),
            ("status", "==", operator.eq, "ok", "INFO"),
            ("count", "<=", operator.le, 100, "WARN"),
        ]
    """
    raw: str | list = config["field_conditions"]

    if isinstance(raw, (list, tuple)):
        conditions = _conditions_from_entries(influxdb3_local, raw, task_id)
    elif isinstance(raw, str):
        conditions = _conditions_from_string(influxdb3_local, raw, task_id)
    else:
        raise Exception(
            "'field_conditions' must be a list of entries or a string, "
            f"got {type(raw).__name__}"
        )

    if not conditions:
        raise Exception("No valid field conditions provided.")
    return conditions


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


def generate_cache_key(
    measurement: str,
    field: str,
    level: str,
    row: dict,
    tags: list,
    aggregation: str | None = None,
) -> str:
    """Generate the row identifier used in alerts ($row). Aggregation is optional."""
    base_parts: list = [measurement, field]
    if aggregation:
        base_parts.append(aggregation)
    base_parts.append(level)

    cache_key: str = ":".join(base_parts)

    for tag in sorted(tags):
        tag_value = row.get(tag)
        # tags without a value are skipped: line protocol has no empty tag values
        if tag_value is not None:
            cache_key += f":{tag}={tag_value}"

    return cache_key


def generate_counter_key(row_identifier: str, op_sym: str, compare_value) -> str:
    """
    Generate the cache key of the breach counter for one condition.

    Conditions that differ only by operator or threshold share a row identifier, so both
    are part of the counter key to keep their counts independent.
    """
    return f"{row_identifier}|{op_sym}|{compare_value!r}"


def record_breach(
    influxdb3_local, cache_key: str, trigger_count: int
) -> tuple[bool, int]:
    """
    Count one consecutive condition breach for the given cache key.

    The counter is reset as soon as the alert is due, so the next alert requires
    another 'trigger_count' consecutive breaches.

    Args:
        influxdb3_local: InfluxDB client instance.
        cache_key (str): Key identifying the condition and row.
        trigger_count (int): Number of consecutive breaches required to alert.

    Returns:
        tuple[bool, int]: Whether an alert is due, and the current breach number.
    """
    cached_value = influxdb3_local.cache.get(cache_key)
    breach_number: int = (int(cached_value) if cached_value is not None else 0) + 1

    if breach_number >= trigger_count:
        influxdb3_local.cache.put(cache_key, "0")
        return True, breach_number

    influxdb3_local.cache.put(cache_key, str(breach_number))
    return False, breach_number


def process_writes(influxdb3_local, table_batches: list, args: dict):
    """
    Process incoming data writes and trigger notifications if field conditions are met
    for a specified number of times.
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

    # an 'all_tables' trigger also receives batches of other tables
    monitored_batches: list = [
        table_batch
        for table_batch in table_batches
        if table_batch["table_name"] == measurement
    ]
    if not monitored_batches:
        return

    influxdb3_local.info(f"[{task_id}] Starting writes process")

    try:
        trigger_count: int = config["trigger_count"]
        senders_config: dict = parse_senders(influxdb3_local, config, task_id)
        field_conditions: list = parse_field_conditions(
            influxdb3_local, config, task_id
        )
        influxdb3_local.info(f"[{task_id}] Field conditions: {field_conditions}")

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
        for table_batch in monitored_batches:
            for row in table_batch["rows"]:
                for field, op_sym, compare_fn, compare_val, level in field_conditions:
                    if field not in row:
                        influxdb3_local.warn(
                            f"[{task_id}] Field '{field}' not found in row: {row}"
                        )
                        continue

                    actual = row[field]
                    cache_key: str = generate_cache_key(
                        measurement, field, level, row, tags
                    )
                    counter_key: str = generate_counter_key(
                        cache_key, op_sym, compare_val
                    )
                    if not compare_fn(actual, compare_val):
                        influxdb3_local.cache.put(counter_key, "0")
                        continue

                    alert_due, breach_number = record_breach(
                        influxdb3_local, counter_key, trigger_count
                    )

                    if not alert_due:
                        influxdb3_local.warn(
                            f"[{task_id}] [{level}] Condition {field} {op_sym} {compare_val!r} matched in row {cache_key} ({actual!r}) for the {breach_number}/{trigger_count} time. Skipping alert."
                        )
                        continue

                    notification_text = interpolate_notification_text(
                        notification_tpl,
                        {
                            "level": level,
                            "row": cache_key,
                            "field": field,
                            "op_sym": op_sym,
                            "compare_val": compare_val,
                            "trigger_count": trigger_count,
                            "actual": actual,
                        },
                    )

                    payload: dict = {
                        "notification_text": notification_text,
                        "senders_config": senders_config,
                    }

                    influxdb3_local.error(
                        f"[{task_id}] [{level}] Condition {field} {op_sym} {compare_val!r} matched in row {cache_key} {trigger_count} times ({actual!r}), sending alert"
                    )
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


def interval_literal(interval: timedelta) -> str:
    """
    Render a DATE_BIN interval literal for the aggregation interval.

    Args:
        interval (timedelta): Aggregation interval.

    Returns:
        str: Interval literal, e.g. "600 seconds".

    Raises:
        ValueError: If the interval is shorter than one second.
    """
    seconds: int = int(interval.total_seconds())
    if seconds < 1:
        raise ValueError(
            f"Invalid interval: {seconds} seconds (must be at least 1 second)"
        )
    return f"{seconds} seconds"


def quote_identifier(identifier: str) -> str:
    """Quote a SQL identifier, escaping embedded double quotes."""
    return '"' + str(identifier).replace('"', '""') + '"'


def generate_fields_string(
    field_aggregation_values: dict,
    interval: str,
    tags_list: list,
):
    """
    Generates the SELECT clause.

    Args:
        field_aggregation_values: dict
        interval (str): DATE_BIN interval literal (e.g., "600 seconds").
        tags_list (list): List of tag names to include in the query.

    Returns:
        str: SQL SELECT clause string including DATE_BIN, aggregations and tags.
    """
    query: str = (
        f"DATE_BIN(INTERVAL '{interval}', time, '1970-01-01T00:00:00Z') AS _time"
    )

    for field_name, aggregation_value_list in field_aggregation_values.items():
        quoted_field: str = quote_identifier(field_name)
        for aggregation, *_ in aggregation_value_list:
            # Dedupe by alias: several conditions may share one aggregation, and
            # duplicate projection names are rejected by the query planner.
            alias: str = quote_identifier(f"{field_name}_{aggregation}")
            if f"as {alias}" in query:
                continue
            query += ",\n"

            # Add ORDER BY time for first_value and last_value to ensure correct temporal ordering
            if aggregation in ("first_value", "last_value"):
                query += f"\t{aggregation}({quoted_field} ORDER BY time) as {alias}"
            else:
                query += f"\t{aggregation}({quoted_field}) as {alias}"

    for tag in tags_list:
        query += f",\n\t{quote_identifier(tag)}"

    return query


def generate_group_by_string(tags_list: list):
    """
    Generates the GROUP BY clause for queries.

    Args:
        tags_list (list): List of tag names to include in the GROUP BY clause.

    Returns:
        str: SQL GROUP BY clause string including '_time' and tags.
    """
    group_by_clause: str = "_time"
    for tag in tags_list:
        group_by_clause += f", {quote_identifier(tag)}"
    return group_by_clause


def build_query(
    field_aggregation_values: dict,
    measurement: str,
    tags_list: list[str],
    interval: str,
    start_time: datetime,
    end_time: datetime,
) -> str:
    """
    Builds an SQL query.

    Args:
        field_aggregation_values: dict for aggregation building
        measurement: source measurement name
        tags_list: list of tag keys to GROUP BY
        interval: DATE_BIN interval literal (e.g., "600 seconds")
        start_time: UTC datetime for WHERE time > ...
        end_time:   UTC datetime for WHERE time < ...

    Returns:
        A complete SQL query string.
    """
    # SELECT clause
    fields_clause: str = generate_fields_string(
        field_aggregation_values, interval, tags_list
    )
    # GROUP BY clause
    group_by: str = generate_group_by_string(tags_list)

    # ISO timestamps, microsecond precision so consecutive windows tile exactly
    start_iso: str = start_time.astimezone(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    end_iso: str = end_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    query: str = f"""
        SELECT
            {fields_clause}
        FROM
            {quote_identifier(measurement)}
        WHERE
            time >= '{start_iso}'
        AND
            time < '{end_iso}'
        GROUP BY
        {group_by}
        ORDER BY
            _time
    """
    return query


def _aggregations_from_mapping(
    influxdb3_local, raw: dict, task_id: str
) -> dict[str, list]:
    """Parse aggregation conditions given as {field: [[aggregation, op, value, level], ...]}."""
    result: dict[str, list] = {}

    for field, conditions in raw.items():
        try:
            for aggregation, op, value, level in conditions:
                if aggregation not in AVAILABLE_AGGREGATIONS:
                    influxdb3_local.warn(
                        f"[{task_id}] Unsupported aggregation '{aggregation}', skipping..."
                    )
                    continue
                message_level: str = str(level).strip().upper()
                if message_level not in ALLOWED_MESSAGE_LEVELS:
                    influxdb3_local.warn(
                        f"[{task_id}] Invalid message level '{level}', skipping..."
                    )
                    continue
                if op not in _OP_FUNCS:
                    influxdb3_local.warn(
                        f"[{task_id}] Invalid operator '{op}', skipping..."
                    )
                    continue
                entry: list = [aggregation, op, _OP_FUNCS[op], value, message_level]
                result.setdefault(field, []).append(entry)
        except Exception as e:
            influxdb3_local.warn(
                f"[{task_id}] Error parsing field aggregation values for field '{field}': {e}"
            )
            continue

    return result


def _aggregations_from_string(
    influxdb3_local, raw: str, task_id: str
) -> dict[str, list]:
    """Parse aggregation conditions given as 'field:aggregation@<op><value>-<level>' pairs."""
    result: dict[str, list] = {}

    # Strip quotes around the string if present
    if len(raw) > 1 and raw[0] == raw[-1] and raw[0] in ('"', "'"):
        raw = raw[1:-1]

    for pair in raw.split(" "):
        if not pair or ":" not in pair:
            influxdb3_local.warn(
                f"[{task_id}] Invalid format in pair '{pair}', skipping..."
            )
            continue

        field_name, agg_expr = pair.split(":", 1)
        if "@" not in agg_expr:
            influxdb3_local.warn(
                f"[{task_id}] Missing '@' in '{agg_expr}', skipping..."
            )
            continue

        aggregation, value_expr = agg_expr.split("@", 1)
        aggregation = aggregation.strip()
        if aggregation not in AVAILABLE_AGGREGATIONS:
            influxdb3_local.warn(
                f"[{task_id}] Unsupported aggregation '{aggregation}', skipping..."
            )
            continue

        # Strip quotes around the value expression if present
        if (
            len(value_expr) > 1
            and value_expr[0] == value_expr[-1]
            and value_expr[0] in ('"', "'")
        ):
            value_expr = value_expr[1:-1]

        # Extract comparison operator
        matched_op = next(
            (
                op
                for op in sorted(_OP_FUNCS, key=len, reverse=True)
                if value_expr.startswith(op)
            ),
            None,
        )
        if not matched_op:
            influxdb3_local.warn(
                f"[{task_id}] No valid comparison operator found in '{value_expr}', skipping..."
            )
            continue

        # Separate value and message level (by '-')
        try:
            value_and_level: str = value_expr[len(matched_op) :].strip()
            value_str, level = value_and_level.rsplit("-", 1)
            level = level.upper()
        except ValueError:
            influxdb3_local.warn(
                f"[{task_id}] Missing or invalid message level in '{value_expr}', skipping..."
            )
            continue

        if level not in ALLOWED_MESSAGE_LEVELS:
            influxdb3_local.warn(
                f"[{task_id}] Invalid message level '{level}', skipping..."
            )
            continue

        try:
            value = float(value_str.strip())
        except ValueError:
            influxdb3_local.warn(
                f"[{task_id}] Value '{value_str}' is not a valid float, skipping..."
            )
            continue

        entry: list = [aggregation, matched_op, _OP_FUNCS[matched_op], value, level]
        result.setdefault(field_name.strip(), []).append(entry)

    return result


def parse_field_aggregation_values(
    influxdb3_local, config: dict, task_id: str
) -> dict[str, list]:
    """
    Parse the aggregation conditions used by the scheduled trigger.

    Conditions come either as {field: [[aggregation, operator, value, level], ...]}
    (TOML) or as a string of 'field:aggregation@<op><value>-<level>' pairs separated
    by spaces, e.g. 'field:avg@>=10-INFO field2:min@<5.0-WARN'.

    Args:
        influxdb3_local: InfluxDB client instance.
        config (dict): Loaded config, optionally containing "field_aggregation_values".
        task_id (str): Unique task identifier.

    Returns:
        dict[str, list]: Field name mapped to a list of
            [aggregation, operator, operator_fn, threshold_value, message_level].

    Raises:
        Exception: If the value has an unsupported type, or if it is provided but no
            valid entries are found.
    """
    raw: str | dict | None = config.get("field_aggregation_values")
    if raw is None:
        return {}

    if isinstance(raw, dict):
        result = _aggregations_from_mapping(influxdb3_local, raw, task_id)
    elif isinstance(raw, str):
        if not raw.strip():
            return {}
        result = _aggregations_from_string(influxdb3_local, raw, task_id)
    else:
        raise Exception(
            "'field_aggregation_values' must be a mapping or a string, "
            f"got {type(raw).__name__}"
        )

    if not result:
        raise Exception("No valid field aggregation values provided.")
    return result


def process_scheduled_call(influxdb3_local, call_time: datetime, args: dict):
    """
    Check for recent data in a specified measurement and send a notification
    if data is missing or matched aggregation conditions for a configured number of checks.

    Args:
        influxdb3_local: InfluxDB client instance used for querying and logging.
        call_time (datetime): The current time of the scheduled check.
        args (dict): Configuration dictionary containing keys like
            "measurement", "senders", "influxdb3_auth_token", "window", and other alert settings.
    """
    task_id: str = str(uuid.uuid4())
    influxdb3_local.info(
        f"[{task_id}] Starting scheduled call with call_time: {call_time}"
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
        trigger_count: int = config["trigger_count"]
        senders_config: dict = parse_senders(influxdb3_local, config, task_id)
        field_aggregation_values: dict = parse_field_aggregation_values(
            influxdb3_local, config, task_id
        )
        influxdb3_local.info(
            f"[{task_id}] Field aggregation conditions: {field_aggregation_values}"
        )

        deadman_check: bool = config["deadman_check"]
        if not field_aggregation_values and not deadman_check:
            influxdb3_local.error(
                "For the plugin to work, you must provide a valid field_aggregation_values parameter or set deadman_check to True"
            )
            return

        port_override: int = config["port_override"]
        notification_path: str = config["notification_path"]
        influxdb3_auth_token: str = (
            config.get("influxdb3_auth_token")
            or os.getenv("INFLUXDB3_AUTH_TOKEN")
            or ""
        )
        if not influxdb3_auth_token:
            influxdb3_local.error(
                f"[{task_id}] Missing required environment variable: INFLUXDB3_AUTH_TOKEN"
            )
            return
        notification_tpl_deadman: str = config["notification_deadman_text"]
        notification_tpl_threshold: str = config["notification_threshold_text"]

        tags: list = get_measurement_tags(influxdb3_local, measurement, task_id)
        window: timedelta = config["window"]
        interval: str = interval_literal(config["interval"])
        time_to: datetime = call_time.replace(tzinfo=timezone.utc)
        time_from: datetime = time_to - window
        influxdb3_local.info(
            f"[{task_id}] Querying data in '{measurement}' from {time_from} to {time_to}"
        )

        query: str = build_query(
            field_aggregation_values, measurement, tags, interval, time_from, time_to
        )
        results: list = influxdb3_local.query(query)
        if not results and deadman_check:
            alert_due, breach_number = record_breach(
                influxdb3_local, measurement, trigger_count
            )

            if alert_due:
                influxdb3_local.error(
                    f"[{task_id}] No data found in '{measurement}' from {time_from} to {time_to} for {trigger_count} times. Sending alert."
                )

                notification_text = interpolate_notification_text(
                    notification_tpl_deadman,
                    {"table": measurement, "time_from": time_from, "time_to": time_to},
                )

                payload: dict = {
                    "notification_text": notification_text,
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
            else:
                influxdb3_local.warn(
                    f"[{task_id}] No data found in '{measurement}' from {time_from} to {time_to} for {breach_number}/{trigger_count} times. Skipping alert."
                )
        else:
            influxdb3_local.cache.put(measurement, "0")

        influxdb3_local.info(
            f"[{task_id}] Query executed, {len(results)} records returned"
        )

        for row in results:
            for field, aggregation_values in field_aggregation_values.items():
                for (
                    aggregation,
                    op_sym,
                    compare_fn,
                    compare_value,
                    level,
                ) in aggregation_values:
                    if f"{field}_{aggregation}" not in row:
                        influxdb3_local.warn(
                            f"[{task_id}] Field '{field}_{aggregation}' not found in results received"
                        )
                        continue

                    actual = row[f"{field}_{aggregation}"]
                    cache_key: str = generate_cache_key(
                        measurement, field, level, row, tags, aggregation
                    )
                    counter_key: str = generate_counter_key(
                        cache_key, op_sym, compare_value
                    )
                    if not compare_fn(actual, compare_value):
                        influxdb3_local.cache.put(counter_key, "0")
                        continue

                    alert_due, breach_number = record_breach(
                        influxdb3_local, counter_key, trigger_count
                    )

                    if not alert_due:
                        influxdb3_local.warn(
                            f"[{task_id}] Condition for row {cache_key} ({aggregation}({field}) {op_sym} {compare_value!r}) matched ({actual!r}) for the {breach_number}/{trigger_count} time. Skipping alert."
                        )
                        continue

                    notification_text = interpolate_notification_text(
                        notification_tpl_threshold,
                        {
                            "level": level,
                            "field": field,
                            "table": measurement,
                            "row": cache_key,
                            "op_sym": op_sym,
                            "aggregation": aggregation,
                            "compare_val": compare_value,
                            "actual": actual,
                        },
                    )

                    payload: dict = {
                        "notification_text": notification_text,
                        "senders_config": senders_config,
                    }

                    influxdb3_local.error(
                        f"[{task_id}] Condition on {measurement}: {aggregation}({field}) {op_sym} {compare_value!r} matched {trigger_count} times in row {cache_key} ({actual!r}), sending alert"
                    )
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
