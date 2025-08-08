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
            "example": "temp:'2.5':20:5@load:3:10:2m",
            "description": "Threshold conditions for MAD-based anomaly detection (e.g., field:k:window_count:threshold). Multiple conditions separated by '@'.",
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
            "description": "Maximum allowed flips (changes) in recent values before suppressing notifications. If 0, suppression is disabled. Default: 0.",
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
from statistics import median
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
    k: float | int | str,
    suffix: str,
    tags: list[str],
    row: dict,
) -> str:
    """
    Generate a consistent cache key string combining measurement, field, k, suffix, and tag values.

    Args:
        measurement (str): Measurement (table) name.
        field (str): Field name being checked.
        k (float|int|str): Multiplier or identifier used in key.
        suffix (str): Identifier (e.g., "count-time", "time-time", "deque", "values").
        tags (list[str]): List of tag column names to include.
        row (dict): Current row data; used to extract tag values.

    Returns:
        str: Formatted key, e.g. "cpu:temp:2.0:count-time:host=server1:region=us-west".
    """
    base = f"{measurement}:{field}:{k}:{suffix}"
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


def parse_mad_thresholds(influxdb3_local, args: dict, task_id: str) -> list[tuple]:
    """
    Parse MAD-based threshold definitions from args into structured tuples or use values from config file.

    Args:
        influxdb3_local: InfluxDB client for logging.
        args (dict): Must include "mad_thresholds" key, a string of '@'-separated segments.
        task_id (str): Unique identifier for logging.

    Each segment has the form:
        field_name:k:window_count:threshold
    where:
        - field_name (str): Name of the numeric field.
        - k (float): Multiplier for MAD.
        - window_count (int): Number of recent points to compute median/MAD.
        - threshold:
            • If integer → count-based: trigger after this many consecutive outliers.
            • If duration string (e.g., "2m", "30s") → duration-based.

    Returns:
        list[list[str, float, int, int|timedelta]]:
            Each list: [field_name, k, window_count, threshold_param].

    Raises:
        Exception: If no valid segments are parsed.
    """
    valid_units: dict = {
        "s": "seconds",
        "min": "minutes",
        "h": "hours",
        "d": "days",
        "w": "weeks",
    }
    raw_input: str | list = args.get("mad_thresholds")
    results: list = []

    if args["use_config_file"]:
        if not isinstance(raw_input, list):
            raise Exception(
                f"[{task_id}] 'mad_thresholds' must be a list when using config file"
            )
        for threshold in raw_input:
            try:
                field_name: str = str(threshold[0])
                k: float = float(threshold[1])
                window_count: int = int(threshold[2])
                threshold_input: int | str = threshold[3]
                if isinstance(threshold_input, str):
                    num_part, unit_part = "", ""
                    for unit in sorted(valid_units.keys(), key=len, reverse=True):
                        if threshold_input.endswith(unit):
                            num_part = threshold_input[: -len(unit)]
                            unit_part = unit
                            break
                    if not num_part or unit_part not in valid_units:
                        influxdb3_local.warn(
                            f"[{task_id}] Invalid threshold format '{threshold_input}'"
                        )
                        continue
                    try:
                        num = int(num_part)
                    except ValueError:
                        influxdb3_local.warn(
                            f"[{task_id}] Invalid number in threshold '{threshold_input}'"
                        )
                        continue
                    threshold_param = timedelta(**{valid_units[unit_part]: num})
                elif isinstance(threshold_input, int):
                    threshold_param = threshold_input
                else:
                    influxdb3_local.warn(
                        f"[{task_id}] Invalid threshold format '{threshold_input}'"
                    )
                    continue
                results.append([field_name, k, window_count, threshold_param])
            except Exception:
                influxdb3_local.warn(
                    f"[{task_id}] Invalid threshold definition: {threshold}, skipping"
                )
        return results

    segments: list = [seg.strip() for seg in raw_input.split("@") if seg.strip()]
    for seg in segments:
        parts = seg.split(":")
        if len(parts) != 4:
            influxdb3_local.warn(
                f"[{task_id}] Invalid segment '{seg}'; expected 4 parts delimited by ':'"
            )
            continue

        field_name = parts[0].strip()
        try:
            k: str | float = parts[1].strip()
            if k[0] == k[-1] and k[0] in ("'", '"'):
                k = k[1:-1]
            k = float(k)
        except ValueError:
            influxdb3_local.warn(f"[{task_id}] Invalid k in segment '{seg}'")
            continue

        try:
            window_count: int = int(parts[2].strip())
        except ValueError:
            influxdb3_local.warn(f"[{task_id}] Invalid window_count in '{seg}'")
            continue

        raw_thresh = parts[3].strip()
        if re.fullmatch(r"-?\d+", raw_thresh):
            threshold_param: int | timedelta = int(raw_thresh)
        else:
            num_part, unit_part = "", ""
            for unit in sorted(valid_units.keys(), key=len, reverse=True):
                if raw_thresh.endswith(unit):
                    num_part = raw_thresh[: -len(unit)]
                    unit_part = unit
                    break
            if not num_part or unit_part not in valid_units:
                influxdb3_local.warn(
                    f"[{task_id}] Invalid threshold format '{raw_thresh}'"
                )
                continue
            try:
                num = int(num_part)
            except ValueError:
                influxdb3_local.warn(
                    f"[{task_id}] Invalid number in threshold '{raw_thresh}'"
                )
                continue
            threshold_param = timedelta(**{valid_units[unit_part]: num})

        results.append([field_name, k, window_count, threshold_param])

    if not results:
        raise Exception(f"[{task_id}] No valid MAD threshold segments in '{raw_input}'")
    return results


def check_state_changes(cached_values: deque, max_flips: int) -> bool:
    """
    Count how many times the value changes in a deque; suppress if flips exceed max_flips.

    Args:
        cached_values (deque): Recent field values (size = state_change_window).
        max_flips (int): Maximum allowed flips in that window.

    Returns:
        bool: True if actual flips ≤ max_flips; False otherwise.
    """
    if len(cached_values) < 2 or max_flips == 0:
        return True

    flips: int = 0
    prev = None
    first = True
    for v in cached_values:
        if first:
            prev = v
            first = False
            continue
        if v != prev:
            flips += 1
            if flips >= max_flips:
                return False
        prev = v
    return True


def process_writes(influxdb3_local, table_batches: list, args: dict | None = None):
    """
    WAL-Flush trigger applying MAD-based anomaly detection on fields without querying data repeatedly.

    Uses in-memory deques in cache to maintain the last N values per field+series, computing median/MAD
    incrementally. Supports both count- and duration-based triggers, plus flip-detection suppression.

    Args:
        influxdb3_local: InfluxDB client for logging, cache, and minimal queries.
        table_batches (list): Each element is {"table_name": str, "rows": [dict, ...]}.
        args (dict):
            Required:
              - measurement (str): Measurement name to monitor.
              - mad_thresholds (str): '@'-separated segments "field:k:window:threshold".
              - senders (str): Dot-separated notification channels.
            Optional:
              - config_file_path (str): path to config file to override args.
              - state_change_count (int): Max flips allowed before suppressing.
              - port_override (int): HTTP port for notification plugin (default 8181).
              - influxdb3_auth_token (str): API v3 token (or via ENV var).
              - notification_path (str): Path on engine (default "notify").
              - notification_count_text (str): Template for count-based messages.
              - notification_time_text (str): Template for duration-based messages.

    Exceptions:
        All exceptions are caught and logged via influxdb3_local.error.
    """
    task_id: str = str(uuid.uuid4())
    influxdb3_local.info(f"[{task_id}] Starting writes processing with args: {args}")

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
        or "mad_thresholds" not in args
        or "senders" not in args
    ):
        influxdb3_local.error(
            f"[{task_id}] Missing required arguments: measurement, mad_thresholds, or senders"
        )
        return

    measurement: str = args["measurement"]
    all_measurements: list = get_all_measurements(influxdb3_local)
    if measurement not in all_measurements:
        influxdb3_local.error(f"[{task_id}] Measurement '{measurement}' not found")
        return

    try:
        # Parse configuration
        mad_thresholds: list = parse_mad_thresholds(influxdb3_local, args, task_id)
        senders_config: dict = parse_senders(influxdb3_local, args, task_id)
        tags: list = get_tag_names(influxdb3_local, measurement, task_id)
        port_override: int = parse_port_override(args, task_id)
        state_change_count: int = int(args.get("state_change_count", 0))
        notification_path: str = args.get("notification_path", "notify")
        influxdb3_auth_token: str = args.get("influxdb3_auth_token") or os.getenv(
            "INFLUXDB3_AUTH_TOKEN"
        )
        if not influxdb3_auth_token:
            influxdb3_local.error(f"[{task_id}] Missing influxdb3_auth_token")
            return

        notification_count_tpl: str = args.get(
            "notification_count_text",
            "MAD count alert: Field $field in $table outlier for $threshold_count consecutive points. Tags: $tags",
        )
        notification_time_tpl: str = args.get(
            "notification_time_text",
            "MAD duration alert: Field $field in $table outlier for $threshold_time. Tags: $tags",
        )

        # Process each batch of newly written rows
        for batch in table_batches:
            if batch.get("table_name") != measurement:
                continue

            for row in batch.get("rows", []):
                tag_str: str = ", ".join(f"{t}={row.get(t, 'None')}" for t in tags)
                for field_name, k, window_count, threshold_param in mad_thresholds:
                    # Extract current field value
                    current_val = row.get(field_name)
                    if current_val is None or not isinstance(current_val, (int, float)):
                        influxdb3_local.info(
                            f"[{task_id}] Field '{field_name}' missing or non-numeric → reset"
                        )
                        # Reset any running state
                        count_key = generate_cache_key(
                            measurement, field_name, k, "count-count", tags, row
                        )
                        time_key = generate_cache_key(
                            measurement, field_name, k, "time-time", tags, row
                        )
                        influxdb3_local.cache.put(count_key, "0")
                        influxdb3_local.cache.put(time_key, "")
                        continue

                    now: datetime = datetime.now(timezone.utc)

                    # Manage deque of size window_count for median/MAD
                    deque_key: str = generate_cache_key(
                        measurement, field_name, k, "deque", tags, row
                    )
                    window_deque = influxdb3_local.cache.get(
                        deque_key, default=deque(maxlen=window_count)
                    )
                    if (
                        not isinstance(window_deque, deque)
                        or window_deque.maxlen != window_count
                    ):
                        window_deque = deque(maxlen=window_count)

                    window_deque.append(current_val)
                    influxdb3_local.cache.put(deque_key, window_deque)

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

                    # Flip-detection deque (size = state_change_window)
                    can_send: bool = check_state_changes(
                        window_deque, state_change_count
                    )

                    # Count-based mode
                    if not isinstance(threshold_param, timedelta):
                        count_key: str = generate_cache_key(
                            measurement, field_name, k, "count-count", tags, row
                        )
                        count_so_far: int = int(
                            influxdb3_local.cache.get(count_key, default="0")
                        )

                        if is_outlier:
                            count_so_far += 1
                            influxdb3_local.cache.put(count_key, str(count_so_far))
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
                                        f"[{task_id}] Suppressed count alert due to flips > {state_change_count}"
                                    )
                                influxdb3_local.cache.put(count_key, "0")

                            else:
                                influxdb3_local.warn(
                                    f"[{task_id}] MAD count threshold reached for {measurement}.{field_name} (k={k}) for the {count_so_far}/{threshold_param} time. tags: {tag_str}"
                                )
                        else:
                            influxdb3_local.cache.put(count_key, "0")

                    # Duration-based mode
                    else:
                        time_key: str = generate_cache_key(
                            measurement, field_name, k, "time-time", tags, row
                        )
                        start_iso = influxdb3_local.cache.get(time_key, default="")

                        if start_iso:
                            try:
                                start_dt = datetime.fromisoformat(start_iso)
                            except Exception:
                                start_dt = None
                        else:
                            start_dt = None

                        if is_outlier:
                            if not start_dt:
                                influxdb3_local.cache.put(time_key, now.isoformat())
                                influxdb3_local.warn(
                                    f"[{task_id}] MAD outlier start for {field_name} at {now.isoformat()} (k={k}), tags: {tag_str}"
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
                                            f"[{task_id}] Suppressed time alert due to flips > {state_change_count}"
                                        )
                                    influxdb3_local.cache.put(time_key, "")
                                else:
                                    influxdb3_local.info(
                                        f"[{task_id}] MAD outlier ongoing for {field_name}, elapsed {elapsed}, threshold {threshold_param}, tags: {tag_str}"
                                    )
                        else:
                            if start_dt:
                                influxdb3_local.info(
                                    f"[{task_id}] MAD outlier cleared for {field_name}, tags: {tag_str}; resetting"
                                )
                            influxdb3_local.cache.put(time_key, "")

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Unexpected error: {e}")
