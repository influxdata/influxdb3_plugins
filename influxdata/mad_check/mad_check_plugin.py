"""
{
    "plugin_type": ["onwrite"],
    "onwrite_args_config": [
        {
            "name": "measurement",
            "example": "cpu",
            "description": "The InfluxDB table (measurement) to monitor.",
            "required": true
        },
        {
            "name": "mad_thresholds",
            "example": "temp:2.5:20:5@load:3:10:2min",
            "description": "Threshold conditions for MAD-based anomaly detection in the form 'field:k:window_count:threshold', separated by '@'. window_count is between 2 and 10000. The threshold is either a count of consecutive outliers or a duration such as 30s, 5min, 2h, 1d.",
            "required": true
        },
        {
            "name": "senders",
            "example": "slack.discord",
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
            "name": "state_change_count",
            "example": "2",
            "description": "Number of transitions between normal and outlier state, within the MAD window, at which notifications are suppressed. Use 2 or greater; 1 would suppress every alert and is treated as 0. Default: 0 (suppression disabled).",
            "required": false
        },
        {
            "name": "notification_count_text",
            "example": "MAD count alert: Field $field in $table outlier for $threshold_count consecutive points. Tags: $tags",
            "description": "Template for count-based notification messages with variables $table, $field, $threshold_count, $tags.",
            "required": false
        },
        {
            "name": "notification_time_text",
            "example": "MAD duration alert: Field $field in $table outlier for $threshold_time. Tags: $tags",
            "description": "Template for duration-based notification messages with variables $table, $field, $threshold_time, $tags.",
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
            "description": "Path to a TOML config file that replaces the trigger arguments entirely. Format: 'config.toml'.",
            "required": false
        }
    ]
}
"""

import json
import os
import re
import uuid
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from statistics import median
from string import Template
from urllib.parse import urlparse

import requests
from influxdata_plugin_utils.config import Validator, load_plugin_config
from influxdata_plugin_utils.introspection import get_table_names, get_tag_names
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

_DEFAULT_COUNT_TEXT = (
    "MAD count alert: Field $field in $table outlier for $threshold_count "
    "consecutive points. Tags: $tags"
)
_DEFAULT_TIME_TEXT = (
    "MAD duration alert: Field $field in $table outlier for $threshold_time. "
    "Tags: $tags"
)


_WRITES_VALIDATORS = [
    Validator("measurement", required=True, cast=str),
    Validator("mad_thresholds", required=True),
    Validator("senders", required=True),
    Validator(
        "port_override",
        default=8181,
        cast=lambda raw: parse_int(raw, minimum=1, maximum=65535),
    ),
    Validator("notification_path", default="notify", cast=str),
    Validator(
        "state_change_count", default=0, cast=lambda raw: parse_int(raw, minimum=0)
    ),
    Validator("notification_count_text", default=_DEFAULT_COUNT_TEXT, cast=str),
    Validator("notification_time_text", default=_DEFAULT_TIME_TEXT, cast=str),
]

_WRITES_CONFIG_CACHE_KEY = "mad_check:writes_config"
_WRITES_CONFIG_TTL_SECONDS = 10 * 60

# window_count bounds: below two points the MAD is always zero, and one deque of
# _MAX_WINDOW_COUNT values is kept per series
_MIN_WINDOW_COUNT = 2
_MAX_WINDOW_COUNT = 10_000


def _load_config(
    influxdb3_local, args: dict | None, validators: list, task_id: str
) -> dict | None:
    """
    Load the plugin configuration, applying defaults and type casts.

    Args:
        influxdb3_local: InfluxDB client instance.
        args (dict | None): Runtime arguments of the trigger.
        validators (list): Validators providing defaults and casts.
        task_id (str): Unique task identifier.

    Returns:
        dict | None: Config values keyed by lower-case name, or None if loading failed.
    """
    args = args or {}
    config_file_path = args.get("config_file_path")
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


def generate_cache_key(
    measurement: str,
    field: str,
    discriminator: float | int | str,
    suffix: str,
    tags: list[str],
    row: dict,
) -> str:
    """
    Generate a consistent cache key from the measurement, field, suffix, and tag values.

    Args:
        measurement (str): Measurement (table) name.
        field (str): Field name being checked.
        discriminator (float|int|str): Value separating keys of different thresholds.
        suffix (str): Identifier (e.g., "count-count", "time-time", "deque", "flips").
        tags (list[str]): List of tag column names to include.
        row (dict): Current row data; used to extract tag values.

    Returns:
        str: Formatted key, e.g. "cpu:temp:2.0-20:count-count:host=server1:region=us-west".
    """
    base = f"{measurement}:{field}:{discriminator}:{suffix}"
    for tag in sorted(tags):
        tag_val = row.get(tag, "None")
        base += f":{tag}={tag_val}"
    return base


def read_counter(influxdb3_local, cache_key: str) -> int:
    """Read an outlier counter, treating a missing or non-numeric entry as zero."""
    try:
        return int(influxdb3_local.cache.get(cache_key))
    except (TypeError, ValueError):
        return 0


def read_window(influxdb3_local, cache_key: str, window_count: int) -> deque:
    """Read a cached deque, replacing it when it is missing or sized differently."""
    window = influxdb3_local.cache.get(cache_key, default=deque(maxlen=window_count))
    if not isinstance(window, deque) or window.maxlen != window_count:
        window = deque(maxlen=window_count)
    return window


def parse_senders(influxdb3_local, config: dict, task_id: str) -> dict:
    """
    Parse and validate sender configurations from the loaded config.

    Args:
        influxdb3_local: InfluxDB client instance.
        config (dict): Loaded config containing "senders" and related settings.
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


def send_notification(
    influxdb3_local, port: int, path: str, token: str, payload: dict, task_id: str
) -> None:
    """
    Send a JSON POST to the given InfluxDB 3 webhook endpoint.

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

    try:
        resp = requests.post(
            url, headers=headers, data=json.dumps(payload), timeout=5.0
        )
        resp.raise_for_status()  # raises on 4xx/5xx
        influxdb3_local.info(
            f"[{task_id}] Alert sent to notification plugin with results: {resp.json()['results']}"
        )
    except requests.RequestException as e:
        influxdb3_local.error(
            f"[{task_id}] Failed to send alert to notification plugin: {e}"
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


def _strip_quotes(raw) -> str:
    """Remove one pair of surrounding quotes from a value."""
    text: str = str(raw).strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in ("'", '"'):
        return text[1:-1]
    return text


def _parse_threshold_param(
    influxdb3_local, raw, task_id: str
) -> int | timedelta | None:
    """
    Parse the fourth part of a threshold into a consecutive count or a duration.

    A bare integer is a count of consecutive outliers; anything else is a duration
    such as '30s' or '2h'.

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


def _parse_mad_entry(influxdb3_local, entry, task_id: str) -> tuple | None:
    """
    Validate one [field, k, window_count, threshold] definition.

    Returns:
        tuple | None: (field_name, k, window_count, threshold_param), or None when any
        part is invalid.
    """
    field_name: str = str(entry[0]).strip()
    if not field_name:
        influxdb3_local.warn(f"[{task_id}] Invalid threshold {entry}: empty field name")
        return None

    try:
        k: float = float(_strip_quotes(entry[1]))
    except (TypeError, ValueError):
        influxdb3_local.warn(f"[{task_id}] Invalid k in threshold {entry}")
        return None
    if k < 0:
        influxdb3_local.warn(
            f"[{task_id}] Invalid k {k} in threshold {entry}, must not be negative"
        )
        return None

    try:
        window_count: int = parse_int(
            entry[2], minimum=_MIN_WINDOW_COUNT, maximum=_MAX_WINDOW_COUNT
        )
    except ValueError as e:
        influxdb3_local.warn(
            f"[{task_id}] Invalid window_count in threshold {entry}: {e}"
        )
        return None

    threshold_param = _parse_threshold_param(influxdb3_local, entry[3], task_id)
    if threshold_param is None:
        return None

    return field_name, k, window_count, threshold_param


def _mad_thresholds_from_entries(influxdb3_local, entries: list, task_id: str) -> list:
    """Parse thresholds given as [field, k, window_count, threshold] entries."""
    thresholds: list = []

    for entry in entries:
        if not isinstance(entry, (list, tuple)) or len(entry) != 4:
            influxdb3_local.warn(
                f"[{task_id}] Invalid threshold '{entry}', expected "
                f"[field, k, window_count, threshold]"
            )
            continue
        parsed = _parse_mad_entry(influxdb3_local, entry, task_id)
        if parsed is not None:
            thresholds.append(parsed)

    return thresholds


def _mad_thresholds_from_string(influxdb3_local, raw: str, task_id: str) -> list:
    """Parse thresholds given as '<field>:<k>:<window_count>:<threshold>' joined by '@'."""
    thresholds: list = []

    for segment in parse_delimited_list(raw, sep="@"):
        parts: list[str] = segment.split(":")
        if len(parts) != 4:
            influxdb3_local.warn(
                f"[{task_id}] Skipping invalid threshold '{segment}' – expected 4 parts "
                f"delimited by ':'"
            )
            continue
        parsed = _parse_mad_entry(influxdb3_local, parts, task_id)
        if parsed is not None:
            thresholds.append(parsed)

    return thresholds


def parse_mad_thresholds(influxdb3_local, config: dict, task_id: str) -> list:
    """
    Parse MAD threshold definitions into structured tuples.

    Thresholds come either as entries of [field, k, window_count, threshold] (TOML) or
    as a string of '<field>:<k>:<window_count>:<threshold>' expressions separated by '@'.

    Args:
        influxdb3_local: InfluxDB client instance.
        config (dict): Loaded config containing "mad_thresholds".
        task_id (str): Unique task identifier.

    Returns:
        list[tuple]: Tuples of (field_name, k, window_count, count_or_duration).

    Example:
        'temp:2.5:20:5@load:3:10:2min'
        [
            ("temp", 2.5, 20, 5),
            ("load", 3.0, 10, datetime.timedelta(minutes=2)),
        ]

    Raises:
        Exception: If no valid thresholds are parsed.
    """
    raw: str | list = config["mad_thresholds"]

    if isinstance(raw, (list, tuple)):
        thresholds = _mad_thresholds_from_entries(influxdb3_local, raw, task_id)
    elif isinstance(raw, str):
        thresholds = _mad_thresholds_from_string(influxdb3_local, raw, task_id)
    else:
        raise Exception(
            "'mad_thresholds' must be a list of entries or a string, "
            f"got {type(raw).__name__}"
        )

    # Repeated definitions share one cache key, so each one would advance the same
    # counter and reach the threshold ahead of time
    unique_thresholds: list = []
    for threshold in thresholds:
        if threshold in unique_thresholds:
            influxdb3_local.warn(
                f"[{task_id}] Skipping duplicate threshold {threshold}"
            )
            continue
        unique_thresholds.append(threshold)

    if not unique_thresholds:
        raise Exception("No valid MAD thresholds provided.")
    return unique_thresholds


def check_state_changes(outlier_flags: deque, state_change_count: int) -> bool:
    """
    Count transitions between normal and outlier state in the window.

    Args:
        outlier_flags (deque): Recent outlier flags of one field.
        state_change_count (int): Number of transitions at which notifications are
            suppressed. 0 disables suppression.

    Returns:
        bool: True while the number of transitions stays below state_change_count.
    """
    if len(outlier_flags) < 2 or state_change_count == 0:
        return True

    flips: int = 0
    previous = outlier_flags[0]
    for flag in list(outlier_flags)[1:]:
        if flag != previous:
            flips += 1
            if flips >= state_change_count:
                return False
        previous = flag
    return True


def normalize_state_change_count(
    influxdb3_local, state_change_count: int, task_id: str
) -> int:
    """Treat 1 as disabled: an alert after normal data always records one transition."""
    if state_change_count != 1:
        return state_change_count

    influxdb3_local.warn(
        f"[{task_id}] state_change_count=1 would suppress every alert, treating it as 0 "
        f"(disabled); use 2 or greater to suppress flapping"
    )
    return 0


def warn_on_inert_suppression(
    influxdb3_local, mad_thresholds: list, state_change_count: int, task_id: str
) -> None:
    """Warn about count thresholds whose window leaves no room for enough transitions."""
    if state_change_count == 0:
        return

    for field_name, _k, window_count, threshold_param in mad_thresholds:
        if isinstance(threshold_param, timedelta):
            continue
        if state_change_count > window_count - threshold_param:
            influxdb3_local.warn(
                f"[{task_id}] Flip suppression never triggers for '{field_name}' with "
                f"count threshold {threshold_param}: set window_count to "
                f"{threshold_param + state_change_count} or more"
            )


def process_writes(influxdb3_local, table_batches: list, args: dict | None = None):
    """
    WAL-Flush trigger applying MAD-based anomaly detection on fields without querying data repeatedly.

    Uses in-memory deques in cache to maintain the last N values per field and series,
    computing median/MAD incrementally. Supports both count- and duration-based
    thresholds, plus suppression of alerts on data that flips in and out of the
    outlier state.

    Args:
        influxdb3_local: InfluxDB client for logging, cache, and minimal queries.
        table_batches (list): Each element is {"table_name": str, "rows": [dict, ...]}.
        args (dict): Runtime arguments of the trigger.

    Exceptions:
        All exceptions are caught and logged via influxdb3_local.error.
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
    if measurement not in get_table_names(influxdb3_local):
        influxdb3_local.error(
            f"[{task_id}] Measurement '{measurement}' not found in database"
        )
        return

    monitored_batches: list = [
        table_batch
        for table_batch in table_batches
        if table_batch.get("table_name") == measurement
    ]
    if not monitored_batches:
        return

    influxdb3_local.info(f"[{task_id}] Starting writes process")

    try:
        mad_thresholds: list = parse_mad_thresholds(influxdb3_local, config, task_id)
        influxdb3_local.info(f"[{task_id}] MAD thresholds: {mad_thresholds}")

        senders_config: dict = parse_senders(influxdb3_local, config, task_id)
        port_override: int = config["port_override"]
        state_change_count: int = normalize_state_change_count(
            influxdb3_local, config["state_change_count"], task_id
        )
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

        warn_on_inert_suppression(
            influxdb3_local, mad_thresholds, state_change_count, task_id
        )

        tags: list = get_tag_names(influxdb3_local, measurement)

        for batch in monitored_batches:
            for row in batch["rows"]:
                tag_str: str = ", ".join(f"{t}={row.get(t, 'None')}" for t in tags)
                # A MAD window depends only on the field and its size, so thresholds
                # sharing one window update it once per row.
                row_windows: dict = {}

                for field_name, k, window_count, threshold_param in mad_thresholds:
                    is_duration: bool = isinstance(threshold_param, timedelta)
                    state_suffix: str = "time-time" if is_duration else "count-count"
                    reset_value: str = "" if is_duration else "0"
                    threshold_label: str = (
                        f"{threshold_param.total_seconds()}s"
                        if is_duration
                        else str(threshold_param)
                    )
                    threshold_id: str = f"{k}-{window_count}-{threshold_label}"
                    state_key: str = generate_cache_key(
                        measurement, field_name, threshold_id, state_suffix, tags, row
                    )

                    current_val = row.get(field_name)
                    if current_val is None or not isinstance(current_val, (int, float)):
                        if (
                            influxdb3_local.cache.get(state_key, default=reset_value)
                            != reset_value
                        ):
                            influxdb3_local.info(
                                f"[{task_id}] Field '{field_name}' missing or non-numeric, resetting state for tags: {tag_str}"
                            )
                        influxdb3_local.cache.put(state_key, reset_value)
                        continue

                    now: datetime = datetime.now(timezone.utc)

                    # Deque of the last window_count values, used for median/MAD
                    window_id: tuple = (field_name, window_count)
                    if window_id not in row_windows:
                        deque_key: str = generate_cache_key(
                            measurement, field_name, window_count, "deque", tags, row
                        )
                        window_deque = read_window(
                            influxdb3_local, deque_key, window_count
                        )
                        window_deque.append(current_val)
                        influxdb3_local.cache.put(deque_key, window_deque)
                        row_windows[window_id] = window_deque
                    window_deque = row_windows[window_id]

                    # Wait until deque is full before computing MAD
                    if len(window_deque) < window_count:
                        influxdb3_local.info(
                            f"[{task_id}] Waiting for {window_count} points for MAD on '{field_name}'. Collected {len(window_deque)} for tags: {tag_str}."
                        )
                        continue

                    med = median(window_deque)
                    abs_devs = [abs(x - med) for x in window_deque]
                    mad = median(abs_devs)

                    lower = med - k * mad
                    upper = med + k * mad

                    is_outlier: bool = (current_val < lower) or (current_val > upper)

                    if is_outlier:
                        influxdb3_local.info(
                            f"[{task_id}] MAD calculation for {field_name}: median={med:.3f}, mad={mad:.3f}, "
                            f"thresholds=({lower:.3f}, {upper:.3f}), current={current_val:.3f}, outlier=True, tags: {tag_str}"
                        )

                    # Suppress alerts when the outlier state flips too often
                    flips_key: str = generate_cache_key(
                        measurement, field_name, threshold_id, "flips", tags, row
                    )
                    outlier_flags = read_window(
                        influxdb3_local, flips_key, window_count
                    )
                    outlier_flags.append(is_outlier)
                    influxdb3_local.cache.put(flips_key, outlier_flags)
                    can_send: bool = check_state_changes(
                        outlier_flags, state_change_count
                    )

                    # Count-based mode
                    if not is_duration:
                        count_so_far: int = read_counter(influxdb3_local, state_key)

                        if is_outlier:
                            count_so_far += 1
                            influxdb3_local.cache.put(state_key, str(count_so_far))
                            influxdb3_local.info(
                                f"[{task_id}] Count-based outlier {count_so_far}/{threshold_param} for {field_name}, tags: {tag_str}"
                            )
                            if count_so_far >= threshold_param:
                                influxdb3_local.error(
                                    f"[{task_id}] MAD count threshold reached for {measurement}.{field_name} (k={k}), tags: {tag_str}, sending alert."
                                )
                                payload: dict = {
                                    "notification_text": interpolate_notification_text(
                                        notification_count_tpl,
                                        {
                                            "table": measurement,
                                            "field": field_name,
                                            "threshold_count": threshold_param,
                                            "tags": tag_str,
                                        },
                                    ),
                                    "senders_config": senders_config,
                                }
                                if can_send:
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
                                        f"[{task_id}] Suppressed count alert for {field_name}: outlier state flipped at least {state_change_count} times in the last {window_count} points"
                                    )
                                influxdb3_local.cache.put(state_key, "0")
                        else:
                            influxdb3_local.cache.put(state_key, "0")

                    # Duration-based mode
                    else:
                        start_iso = influxdb3_local.cache.get(state_key, default="")

                        if start_iso:
                            try:
                                start_dt = datetime.fromisoformat(start_iso)
                            except Exception:
                                start_dt = None
                        else:
                            start_dt = None

                        if is_outlier:
                            if not start_dt:
                                influxdb3_local.cache.put(state_key, now.isoformat())
                                influxdb3_local.warn(
                                    f"[{task_id}] Duration-based outlier started for {field_name} at {now.isoformat()} (k={k}), tags: {tag_str}"
                                )
                            else:
                                elapsed = now - start_dt
                                if elapsed >= threshold_param:
                                    influxdb3_local.error(
                                        f"[{task_id}] MAD duration threshold reached for {measurement}.{field_name} (k={k}). tags: {tag_str}, sending alert."
                                    )
                                    payload: dict = {
                                        "notification_text": interpolate_notification_text(
                                            notification_time_tpl,
                                            {
                                                "table": measurement,
                                                "field": field_name,
                                                "threshold_time": threshold_param,
                                                "tags": tag_str,
                                            },
                                        ),
                                        "senders_config": senders_config,
                                    }
                                    if can_send:
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
                                            f"[{task_id}] Suppressed duration alert for {field_name}: outlier state flipped at least {state_change_count} times in the last {window_count} points"
                                        )
                                    influxdb3_local.cache.put(state_key, "")
                                else:
                                    influxdb3_local.info(
                                        f"[{task_id}] MAD outlier ongoing for {field_name}, elapsed {elapsed}, threshold {threshold_param}, tags: {tag_str}"
                                    )
                        else:
                            if start_dt:
                                influxdb3_local.info(
                                    f"[{task_id}] MAD outlier cleared for {field_name}, tags: {tag_str}; resetting"
                                )
                            influxdb3_local.cache.put(state_key, "")

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Unexpected error: {e}")
