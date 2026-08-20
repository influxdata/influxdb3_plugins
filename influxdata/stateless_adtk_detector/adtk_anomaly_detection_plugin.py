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
            "description": "Dot-separated list of ADTK detectors (a TOML config may use a list instead). Supported: GeneralizedESDTestAD, InterQuartileRangeAD, ThresholdAD, QuantileAD, LevelShiftAD, VolatilityShiftAD, PersistAD, SeasonalAD.",
            "required": true
        },
        {
            "name": "detector_params",
            "example": "eyJRdWFudGlsZUFKIjogeyJsb3dfcXVhbnRpbGUiOiA...",
            "description": "Base64-encoded JSON string specifying parameters for each detector (a TOML config may use a [detector_params] table instead).",
            "required": true
        },
        {
            "name": "min_consensus",
            "example": "2",
            "description": "Minimum number of detectors that must agree to flag a point as anomalous. Must be 1 or greater. Default: 1.",
            "required": false
        },
        {
            "name": "group_by_tags",
            "example": "true",
            "description": "Analyze every tag combination as its own time series. When disabled (default), all rows of the window form a single series and rows sharing a timestamp are collapsed to the first one. Default: false.",
            "required": false
        },
        {
            "name": "window",
            "example": "1h",
            "description": "Time window for data analysis (e.g., `1h` for 1 hour). Must be a positive duration. Units: `us`, `ms`, `s`, `min`, `h`, `d`, `w`.",
            "required": true
        },
        {
            "name": "senders",
            "example": "slack.discord",
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
            "description": "Minimum duration for an anomaly condition to persist before triggering a notification (e.g., `5min`). Units: `us`, `ms`, `s`, `min`, `h`, `d`, `w`. Default: `0s`.",
            "required": false
        },
        {
            "name": "max_notifications_per_run",
            "example": "20",
            "description": "Maximum number of notifications sent by a single run. Anomalies beyond the limit are counted in a warning and not resent later. Default: 20.",
            "required": false
        },
        {
            "name": "notification_text",
            "example": "Anomaly detected in $table.$field with value $value by $detectors. Tags: $tags",
            "description": "Template for notification message with variables `$table`, `$field`, `$value`, `$detectors`, `$tags`, `$timestamp`.",
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
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
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
from influxdata_plugin_utils.config import Validator, load_plugin_config
from influxdata_plugin_utils.introspection import (
    get_table_names,
    get_tag_names,
    query_window,
)
from influxdata_plugin_utils.parsing import (
    parse_bool,
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

# Detectors that cannot be constructed without these parameters
REQUIRED_DETECTOR_PARAMS = {
    "LevelShiftAD": ["window"],
    "VolatilityShiftAD": ["window"],
}

# Detectors that classify points on their own and must not be fitted
UNFITTED_DETECTORS = ("ThresholdAD",)

_DEFAULT_NOTIFICATION_TEXT = (
    "Anomaly detected in $table.$field with value $value by $detectors. Tags: $tags"
)


def parse_window(raw) -> timedelta:
    """Parse the analysis window, rejecting non-positive durations."""
    window: timedelta = parse_timedelta(raw)
    if window <= timedelta(0):
        raise ValueError(f"Invalid window: {raw!r} (must be a positive duration)")
    return window


_VALIDATORS = [
    Validator("measurement", required=True, cast=str),
    Validator("field", required=True, cast=str),
    Validator(
        "detectors",
        required=True,
        cast=lambda raw: parse_delimited_list(raw, sep="."),
    ),
    Validator("detector_params", required=True),
    Validator("senders", required=True),
    Validator("window", required=True, cast=parse_window),
    Validator("min_consensus", default=1, cast=lambda raw: parse_int(raw, minimum=1)),
    Validator("group_by_tags", default=False, cast=parse_bool),
    Validator("min_condition_duration", default="0s", cast=parse_timedelta),
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


def _load_config(influxdb3_local, args: dict, task_id: str) -> dict | None:
    """
    Load the plugin configuration, applying defaults and type casts.

    A TOML file referenced by 'config_file_path' replaces the inline arguments;
    INFLUXDB3_AUTH_TOKEN from the environment is used when the token is not
    configured explicitly.

    Args:
        influxdb3_local: InfluxDB client instance.
        args (dict): Runtime arguments of the trigger.
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


def decode_detector_params(raw: str | dict) -> dict:
    """
    Decode 'detector_params' from a mapping or a base64-encoded JSON string.

    Returns:
        dict: Mapping of detector names to their parameter dictionaries.

    Raises:
        Exception: If the value is not valid base64 or not a JSON object.
    """
    if isinstance(raw, dict):
        return raw

    try:
        decoded: str = base64.b64decode(raw).decode("utf-8")
    except Exception:
        raise Exception("Invalid base64 encoding in detector_params")

    try:
        params = json.loads(decoded)
    except json.JSONDecodeError:
        raise Exception(f"Invalid JSON in decoded detector_params: {decoded}")

    if not isinstance(params, dict):
        raise Exception("detector_params must decode to a JSON object")
    return params


def parse_detectors(influxdb3_local, config: dict, task_id: str) -> tuple[list, dict]:
    """
    Resolve the detectors to apply together with their parameters.

    Returns:
        tuple[list, dict]: Applicable detector names and their parameters.

    Raises:
        Exception: If no detector is applicable.
    """
    params: dict = decode_detector_params(config["detector_params"])

    detectors: list = []
    detector_params: dict = {}
    for detector in config["detectors"]:
        if detector not in AVAILABLE_DETECTORS:
            influxdb3_local.warn(f"[{task_id}] Unknown detector: {detector}")
            continue
        if detector not in params:
            influxdb3_local.warn(
                f"[{task_id}] Missing parameters for detector: {detector}"
            )
            continue
        if not isinstance(params[detector], dict):
            influxdb3_local.warn(
                f"[{task_id}] Parameters for detector {detector} must be a mapping"
            )
            continue

        missing: list = [
            name
            for name in REQUIRED_DETECTOR_PARAMS.get(detector, [])
            if name not in params[detector]
        ]
        if missing:
            influxdb3_local.warn(
                f"[{task_id}] {detector} requires the '{', '.join(missing)}' parameter"
            )
            continue

        detectors.append(detector)
        detector_params[detector] = params[detector]

    if not detectors:
        raise Exception(f"No applicable detectors in {config['detectors']}")

    return detectors, detector_params


def format_tags(row: pd.Series, tags: list) -> str:
    """Render the tag values of a row as 'tag=value' pairs."""
    return ", ".join(f"{tag}={row.get(tag, 'None')}" for tag in tags)


def split_by_tags(df: pd.DataFrame, tags: list, group_by_tags: bool) -> list:
    """
    Split query results into one frame per tag combination.
    """
    if not group_by_tags or not tags:
        return [df]
    return [group for _, group in df.groupby(tags, dropna=False, sort=False)]


def detect_anomalies(
    influxdb3_local,
    series: pd.Series,
    detectors: list,
    detector_params: dict,
    min_consensus: int,
    task_id: str,
) -> pd.Series | None:
    """
    Apply every detector to the series and combine their verdicts by consensus.

    Returns:
        pd.Series | None: True for every point flagged by at least 'min_consensus'
            detectors, or None if no detector could be applied.
    """
    anomaly_results: list = []
    for detector_name in detectors:
        try:
            params: dict = detector_params[detector_name]
            influxdb3_local.info(
                f"[{task_id}] Applying detector {detector_name} with params {params}"
            )
            detector = AVAILABLE_DETECTORS[detector_name](**params)
            if detector_name not in UNFITTED_DETECTORS:
                detector.fit(series)
            anomalies: pd.Series = detector.detect(series)
            anomaly_results.append(anomalies)
            influxdb3_local.info(
                f"[{task_id}] Detector {detector_name} found {anomalies.sum()} anomalies"
            )
        except Exception as e:
            influxdb3_local.warn(
                f"[{task_id}] Failed to apply detector {detector_name}: {e}"
            )

    if not anomaly_results:
        return None

    anomaly_df = pd.concat(anomaly_results, axis=1).fillna(False)
    return (anomaly_df.sum(axis=1) >= min_consensus).astype(bool)


def process_scheduled_call(
    influxdb3_local, call_time: datetime, args: dict | None = None
):
    """
    Scheduler trigger for anomaly detection using ADTK stateless detectors.

    Queries a specified measurement and field within a time window, applies one or more
    ADTK detectors, and sends notifications for anomalies. Supports consensus-based detection
    (a configurable number of detectors must agree) and optional debounce logic.

    Args:
        influxdb3_local: InfluxDB client for querying, caching, and logging.
        call_time (datetime): UTC timestamp at which the scheduler triggers this function.
        args (dict):
            Required:
                - measurement (str): Measurement name to query.
                - field (str): Numeric field to evaluate.
                - detectors (str): Dot-separated list of ADTK detectors.
                - detector_params (str): Base64-encoded JSON of detector parameters.
                - window (str): Time window for data query (e.g., "1h").
                - senders (str): Dot-separated notification channels.
            Optional:
                - config_file_path (str): path to config file to override args.
                - min_consensus (int): Detectors required to flag an anomaly (default: 1).
                - min_condition_duration (str): Minimum anomaly duration (e.g., "5min").
                - max_notifications_per_run (int): Notification cap per run (default: 20).
                - notification_text (str): Message template.
                - notification_path (str): Path for notification plugin (default: "notify").
                - port_override (int): HTTP port (default: 8181).
                - influxdb3_auth_token (str): API v3 token (or via ENV var).

    Exceptions:
        All exceptions are caught and logged via influxdb3_local.error.
    """
    task_id: str = str(uuid.uuid4())
    influxdb3_local.info(
        f"[{task_id}] Starting anomaly detection scheduled call at {call_time}"
    )

    config: dict | None = _load_config(influxdb3_local, args, task_id)
    if config is None:
        return

    try:
        measurement: str = config["measurement"]
        if measurement not in get_table_names(influxdb3_local):
            influxdb3_local.error(f"[{task_id}] Measurement '{measurement}' not found")
            return

        field: str = config["field"]
        detectors, detector_params = parse_detectors(influxdb3_local, config, task_id)
        influxdb3_local.info(
            f"[{task_id}] Retrieved detector_params: {detector_params}"
        )

        min_consensus: int = config["min_consensus"]
        if min_consensus > len(detectors):
            influxdb3_local.warn(
                f"[{task_id}] min_consensus={min_consensus} exceeds the {len(detectors)} applicable detectors, no point can reach consensus"
            )

        group_by_tags: bool = config["group_by_tags"]
        max_notifications_per_run: int = config["max_notifications_per_run"]
        window: timedelta = config["window"]
        senders_config: dict = parse_senders(influxdb3_local, config, task_id)
        port_override: int = config["port_override"]
        min_condition_duration: timedelta = config["min_condition_duration"]
        if min_condition_duration >= window:
            influxdb3_local.warn(
                f"[{task_id}] min_condition_duration={min_condition_duration} is not shorter than window={window}, an anomaly can never persist long enough to alert"
            )
        notification_path: str = config["notification_path"]
        notification_text: str = config["notification_text"]
        influxdb3_auth_token: str = (
            config.get("influxdb3_auth_token")
            or os.getenv("INFLUXDB3_AUTH_TOKEN")
            or ""
        )
        if not influxdb3_auth_token:
            influxdb3_local.error(f"[{task_id}] Missing influxdb3_auth_token")
            return

        influxdb3_local.info(
            f"[{task_id}] Configuration completed: field={field}, detectors={len(detectors)}, min_consensus={min_consensus}, window={window}"
        )

        # Query data
        tags: list = get_measurement_tags(influxdb3_local, measurement, task_id)
        end_time: datetime = call_time
        start_time: datetime = end_time - window
        influxdb3_local.info(
            f"[{task_id}] Querying {measurement}.{field} from {start_time} to {end_time}"
        )
        result: list = query_window(
            influxdb3_local,
            measurement,
            start=start_time.isoformat(),
            end=end_time.isoformat(),
            columns=[field, "time", *tags],
        )
        if not result:
            influxdb3_local.info(
                f"[{task_id}] No data found for {measurement}.{field} from {start_time} to {end_time}"
            )
            return
        influxdb3_local.info(
            f"[{task_id}] Retrieved {len(result)} records from {measurement}"
        )

        # Convert to pandas Series
        df: pd.DataFrame = pd.DataFrame(result)
        if field not in df.columns or "time" not in df.columns:
            influxdb3_local.error(
                f"[{task_id}] Field '{field}' or 'time' not found in query results"
            )
            return
        groups: list = split_by_tags(df, tags, group_by_tags)

        # Apply detectors
        influxdb3_local.info(
            f"[{task_id}] Starting anomaly detection with {len(detectors)} detectors on {len(groups)} series"
        )
        # Process anomalies with debounce logic
        influxdb3_local.info(
            f"[{task_id}] Processing anomalies with debounce logic (min_condition_duration={min_condition_duration})"
        )
        processed_anomalies = 0
        sent_notifications = 0
        failed_notifications = 0
        suppressed_notifications = 0
        for group in groups:
            series_label: str = (
                f" (tags: {format_tags(group.iloc[0], tags)})"
                if group_by_tags and tags
                else ""
            )
            # a time index keeps the per-point row lookup below out of a full scan
            rows: pd.DataFrame = group.drop_duplicates(subset="time")
            rows.index = pd.to_datetime(rows["time"], unit="ns")

            series: pd.Series = rows[field].dropna()
            missing_values: int = len(rows) - len(series)
            if missing_values:
                # detectors raise on NaN, which would drop the whole series
                influxdb3_local.info(
                    f"[{task_id}] Skipped {missing_values} points without a '{field}' value{series_label}"
                )
            if series.empty:
                influxdb3_local.info(
                    f"[{task_id}] No values to analyze for {measurement}.{field}{series_label}"
                )
                continue

            series = validate_series(series)  # Ensure regular sampling and time order
            influxdb3_local.info(
                f"[{task_id}] Prepared time series data with {len(series)} points{series_label}"
            )

            consensus_anomalies = detect_anomalies(
                influxdb3_local,
                series,
                detectors,
                detector_params,
                min_consensus,
                task_id,
            )
            if consensus_anomalies is None:
                influxdb3_local.error(
                    f"[{task_id}] No valid detectors applied to {measurement}.{field}{series_label}"
                )
                continue
            influxdb3_local.info(
                f"[{task_id}] Consensus analysis: {consensus_anomalies.sum()} anomalies detected with min_consensus={min_consensus}{series_label}"
            )

            for timestamp, is_anomaly in consensus_anomalies.items():
                row: pd.Series = rows.loc[timestamp]
                cache_key: str = generate_cache_key(measurement, field, tags, row)
                alert_key: str = f"{cache_key}:last_alert"
                tag_str: str = format_tags(row, tags)

                last_alert_str: str = influxdb3_local.cache.get(alert_key, default="")
                if last_alert_str and timestamp <= pd.Timestamp(last_alert_str):
                    continue

                processed_anomalies += 1
                start_time_str: str = influxdb3_local.cache.get(cache_key, default="")
                alert_reason: str | None = None

                if is_anomaly:
                    if not start_time_str:
                        if min_condition_duration > timedelta(0):
                            # Start of a new anomaly
                            influxdb3_local.cache.put(cache_key, timestamp.isoformat())
                            influxdb3_local.info(
                                f"[{task_id}] Anomaly started for {measurement}.{field} (tags: {tag_str}), waiting for duration {min_condition_duration}"
                            )
                            continue
                        alert_reason = f"Anomaly detected for {measurement}.{field} (tags: {tag_str}), sending alert"
                    else:
                        # Check duration
                        elapsed: timedelta = timestamp - pd.Timestamp(start_time_str)
                        if elapsed < min_condition_duration:
                            influxdb3_local.info(
                                f"[{task_id}] Anomaly ongoing for {elapsed} < {min_condition_duration} for {measurement}.{field} (tags: {tag_str})"
                            )
                            continue
                        alert_reason = f"Anomaly persisted for {elapsed} for {measurement}.{field} (tags: {tag_str}), sending alert"
                elif start_time_str:
                    # Reset cache if anomaly stops
                    influxdb3_local.cache.delete(cache_key)
                    influxdb3_local.info(
                        f"[{task_id}] Anomaly cleared for {measurement}.{field} (tags: {tag_str})"
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
                                "table": measurement,
                                "field": field,
                                "value": row[field],
                                "detectors": ".".join(detectors),
                                "tags": tag_str,
                                "timestamp": timestamp.isoformat(),
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
                influxdb3_local.cache.put(alert_key, timestamp.isoformat())

        if failed_notifications:
            influxdb3_local.warn(
                f"[{task_id}] {failed_notifications} notifications could not be delivered, the next run will alert on them again"
            )
        if suppressed_notifications:
            influxdb3_local.warn(
                f"[{task_id}] Suppressed {suppressed_notifications} notifications after reaching max_notifications_per_run={max_notifications_per_run}"
            )
        influxdb3_local.info(
            f"[{task_id}] Anomaly processing completed: {processed_anomalies} points processed, {sent_notifications} notifications sent"
        )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Error: {e}")
