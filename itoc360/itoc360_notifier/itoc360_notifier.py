"""
{
    "plugin_type": ["scheduled"],
    "scheduled_args_config": [
        {
            "name": "measurement",
            "example": "cpu",
            "description": "Measurement (table) to evaluate.",
            "required": true
        },
        {
            "name": "field",
            "example": "usage_percent",
            "description": "Numeric field to aggregate and compare against the thresholds.",
            "required": true
        },
        {
            "name": "itoc360_url",
            "example": "https://api.itoc360.app/functions/v1/events?token=YOUR_SOURCE_TOKEN",
            "description": "ITOC360 integration endpoint including the source token query parameter.",
            "required": true
        },
        {
            "name": "check_name",
            "example": "CPU Threshold",
            "description": "Human readable check name sent as _check_name and used to build _check_id.",
            "required": true
        },
        {
            "name": "window",
            "example": "5min",
            "description": "Look-back window for the aggregation. Format: <number><unit> where unit is s, min, h, d.",
            "required": true
        },
        {
            "name": "crit_threshold",
            "example": "90",
            "description": "Threshold that raises a crit level alert.",
            "required": true
        },
        {
            "name": "warn_threshold",
            "example": "75",
            "description": "Threshold that raises a warn level alert. Omit to disable the warn level.",
            "required": false
        },
        {
            "name": "operator",
            "example": "gt",
            "description": "Comparison against the thresholds: gt (greater than) or lt (less than). Defaults to gt.",
            "required": false
        },
        {
            "name": "aggregation",
            "example": "avg",
            "description": "Aggregation applied to the field over the window: avg, max, min, sum or count. Defaults to avg.",
            "required": false
        },
        {
            "name": "group_by_tags",
            "example": "host.region",
            "description": "Dot separated tag keys. Each tag combination is evaluated and deduplicated separately in ITOC360.",
            "required": false
        },
        {
            "name": "dry_run",
            "example": "false",
            "description": "When true, evaluate and log the payload without sending it. Defaults to false.",
            "required": false
        },
        {
            "name": "max_retries",
            "example": "3",
            "description": "HTTP delivery attempts before giving up. Defaults to 3.",
            "required": false
        },
        {
            "name": "request_timeout",
            "example": "10",
            "description": "HTTP request timeout in seconds. Defaults to 10.",
            "required": false
        }
    ]
}
"""

import time
import uuid
from datetime import datetime, timezone

import requests

# Levels understood by the ITOC360 InfluxDB provider. The provider treats "ok"
# as a RESOLVE and every other value as an ALERT, and maps the level to a
# priority, so these literals must not be changed or capitalised.
LEVEL_OK = "ok"
LEVEL_INFO = "info"
LEVEL_WARN = "warn"
LEVEL_CRIT = "crit"

CHECK_TYPE = "threshold"
CACHE_PREFIX = "itoc360_notifier"

VALID_AGGREGATIONS = ("avg", "max", "min", "sum", "count")
VALID_OPERATORS = {"gt": ">", "lt": "<"}
UNIT_TO_SECONDS = {"s": 1, "min": 60, "h": 3600, "d": 86400}


def redact_url(url: str) -> str:
    """Strip the query string from a URL so the source token never reaches the logs.

    Plugin logs are queryable through system.processing_engine_logs, so a token
    written there would be readable by anyone with query access.

    Args:
        url: The full endpoint URL.

    Returns:
        The URL without its query string.
    """
    return url.split("?", 1)[0] + "?token=***"


def parse_window(window: str) -> int:
    """Convert a window string such as "5min" into seconds.

    Args:
        window: Window in <number><unit> form, where unit is s, min, h or d.

    Returns:
        The window length in seconds.

    Raises:
        ValueError: If the window cannot be parsed or is not positive.
    """
    for unit in ("min", "s", "h", "d"):
        if window.endswith(unit):
            amount = window[: -len(unit)].strip()
            if not amount.isdigit() or int(amount) <= 0:
                raise ValueError(f"Invalid window amount: {window}")
            return int(amount) * UNIT_TO_SECONDS[unit]
    raise ValueError(f"Invalid window unit: {window}")


def slugify(value: str) -> str:
    """Reduce a display name to a stable lowercase identifier fragment.

    Args:
        value: Arbitrary display text.

    Returns:
        A lowercase string containing only alphanumerics and underscores.
    """
    cleaned = [c.lower() if c.isalnum() else "_" for c in value.strip()]
    return "".join(cleaned).strip("_") or "check"


def build_check_id(check_slug: str, measurement: str, tags: dict) -> str:
    """Build the deterministic identity that ITOC360 fingerprints on.

    ITOC360 derives the alert fingerprint from md5(_check_id), so the ALERT and
    its matching RESOLVE must produce a byte identical string. Tags are sorted
    for that reason, and no timestamp or per-run value is included.

    Args:
        check_slug: Stable slug derived from the check name.
        measurement: Measurement being evaluated.
        tags: Tag key/value pairs identifying the series.

    Returns:
        A string of the form "<check_slug>:<measurement>:<k=v,k=v>".
    """
    tag_part = ",".join(f"{k}={v}" for k, v in sorted(tags.items()))
    return f"{check_slug}:{measurement}:{tag_part}"


def resolve_level(value: float, operator: str, crit: float, warn: float | None) -> str:
    """Classify an aggregated value into an ITOC360 level.

    Args:
        value: Aggregated value for the series.
        operator: Either "gt" or "lt".
        crit: Threshold for the crit level.
        warn: Threshold for the warn level, or None when disabled.

    Returns:
        One of the LEVEL_* constants.
    """
    breached = (lambda a, b: a > b) if operator == "gt" else (lambda a, b: a < b)
    if breached(value, crit):
        return LEVEL_CRIT
    if warn is not None and breached(value, warn):
        return LEVEL_WARN
    return LEVEL_OK


def build_payload(
    check_id: str,
    check_name: str,
    level: str,
    message: str,
    measurement: str,
) -> dict:
    """Assemble the ITOC360 event body.

    Args:
        check_id: Deterministic series identity.
        check_name: Human readable check name.
        level: One of the LEVEL_* constants.
        message: Human readable description of the current state.
        measurement: Source measurement name.

    Returns:
        The request body expected by the ITOC360 InfluxDB provider.
    """
    return {
        "_check_id": check_id,
        "_check_name": check_name,
        "_type": CHECK_TYPE,
        "_level": level,
        "_message": message,
        "_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_source_measurement": measurement,
    }


def send_event(
    influxdb3_local,
    url: str,
    payload: dict,
    max_retries: int,
    timeout: int,
    task_id: str,
) -> bool:
    """POST one event to ITOC360, retrying with exponential backoff.

    Retries are safe: ITOC360 deduplicates on md5(_check_id), so a repeated
    delivery of the same payload collapses into the existing alert.

    Args:
        influxdb3_local: InfluxDB client instance.
        url: ITOC360 endpoint including the source token.
        payload: Event body.
        max_retries: Number of attempts before giving up.
        timeout: Per request timeout in seconds.
        task_id: Unique identifier for this plugin run.

    Returns:
        True when ITOC360 accepted the event, otherwise False.
    """
    safe_url = redact_url(url)
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(url, json=payload, timeout=timeout)
            if 200 <= response.status_code < 300:
                influxdb3_local.info(
                    f"[{task_id}] Sent {payload['_level']} for {payload['_check_id']} "
                    f"to {safe_url} (status={response.status_code})"
                )
                return True
            influxdb3_local.warn(
                f"[{task_id}] ITOC360 rejected event, attempt {attempt}/{max_retries}, "
                f"status={response.status_code}, body={response.text[:200]}"
            )
        except Exception as exc:
            influxdb3_local.error(
                f"[{task_id}] Request error to {safe_url}, "
                f"attempt {attempt}/{max_retries}: {exc}"
            )
        if attempt < max_retries:
            time.sleep(2 ** (attempt - 1))
    influxdb3_local.error(
        f"[{task_id}] Giving up on {payload['_check_id']} after {max_retries} attempts"
    )
    return False


def parse_config(args: dict) -> dict:
    """Validate and normalise trigger arguments.

    Args:
        args: Raw trigger arguments.

    Returns:
        A normalised configuration dictionary.

    Raises:
        ValueError: If a required argument is missing or a value is invalid.
    """
    for required in ("measurement", "field", "itoc360_url", "check_name", "window",
                     "crit_threshold"):
        if not args.get(required):
            raise ValueError(f"Missing required argument: {required}")

    aggregation = args.get("aggregation", "avg").lower()
    if aggregation not in VALID_AGGREGATIONS:
        raise ValueError(f"Invalid aggregation: {aggregation}")

    operator = args.get("operator", "gt").lower()
    if operator not in VALID_OPERATORS:
        raise ValueError(f"Invalid operator: {operator}")

    warn_raw = args.get("warn_threshold")
    tags_raw = args.get("group_by_tags", "")

    return {
        "measurement": args["measurement"],
        "field": args["field"],
        "itoc360_url": args["itoc360_url"],
        "check_name": args["check_name"],
        "check_slug": slugify(args["check_name"]),
        "window_seconds": parse_window(args["window"]),
        "crit_threshold": float(args["crit_threshold"]),
        "warn_threshold": float(warn_raw) if warn_raw not in (None, "") else None,
        "operator": operator,
        "aggregation": aggregation,
        "group_by_tags": [t for t in tags_raw.split(".") if t],
        "dry_run": str(args.get("dry_run", "false")).lower() == "true",
        "max_retries": int(args.get("max_retries", 3)),
        "request_timeout": int(args.get("request_timeout", 10)),
    }


def query_series(influxdb3_local, config: dict, task_id: str) -> list[dict]:
    """Aggregate the configured field over the window, grouped by the tag set.

    Args:
        influxdb3_local: InfluxDB client instance.
        config: Normalised configuration.
        task_id: Unique identifier for this plugin run.

    Returns:
        One row per tag combination, each containing the aggregated value.
    """
    tags = config["group_by_tags"]
    select_tags = "".join(f'"{tag}", ' for tag in tags)
    group_by = ("GROUP BY " + ", ".join(f'"{tag}"' for tag in tags)) if tags else ""
    query = (
        f'SELECT {select_tags}{config["aggregation"]}("{config["field"]}") AS agg_value '
        f'FROM "{config["measurement"]}" '
        f"WHERE time >= now() - INTERVAL '{config['window_seconds']} seconds' "
        f"{group_by}"
    )
    influxdb3_local.info(f"[{task_id}] Query: {query}")
    return influxdb3_local.query(query)


def process_scheduled_call(influxdb3_local, call_time, args: dict) -> None:
    """Evaluate the configured threshold and emit ALERT or RESOLVE events.

    An event is sent only when a series changes level, so a sustained breach
    does not re-notify on every interval. Level state is held in the plugin
    cache; see the README for the restart limitation this implies.

    Args:
        influxdb3_local: InfluxDB client instance.
        call_time: Scheduled invocation time supplied by the Processing Engine.
        args: Trigger arguments.
    """
    task_id = str(uuid.uuid4())[:8]

    try:
        config = parse_config(args or {})
    except (ValueError, TypeError) as exc:
        influxdb3_local.error(f"[{task_id}] Configuration error: {exc}")
        return

    try:
        rows = query_series(influxdb3_local, config, task_id)
    except Exception as exc:
        influxdb3_local.error(f"[{task_id}] Query failed: {exc}")
        return

    if not rows:
        influxdb3_local.info(f"[{task_id}] No data in window, nothing to evaluate")
        return

    for row in rows:
        value = row.get("agg_value")
        if value is None:
            continue

        tags = {tag: str(row.get(tag, "")) for tag in config["group_by_tags"]}
        check_id = build_check_id(config["check_slug"], config["measurement"], tags)
        level = resolve_level(
            float(value),
            config["operator"],
            config["crit_threshold"],
            config["warn_threshold"],
        )

        cache_key = f"{CACHE_PREFIX}:{check_id}"
        previous = influxdb3_local.cache.get(cache_key) or LEVEL_OK

        if level == previous:
            continue

        symbol = VALID_OPERATORS[config["operator"]]
        if level == LEVEL_OK:
            message = (
                f"{config['aggregation']}({config['field']}) returned to normal "
                f"(actual: {value})"
            )
        else:
            threshold = (
                config["crit_threshold"] if level == LEVEL_CRIT
                else config["warn_threshold"]
            )
            message = (
                f"{config['aggregation']}({config['field']}) {symbol} {threshold} "
                f"(actual: {value})"
            )

        payload = build_payload(
            check_id, config["check_name"], level, message, config["measurement"]
        )

        if config["dry_run"]:
            influxdb3_local.info(f"[{task_id}] dry_run, would send: {payload}")
            influxdb3_local.cache.put(cache_key, level)
            continue

        if send_event(
            influxdb3_local,
            config["itoc360_url"],
            payload,
            config["max_retries"],
            config["request_timeout"],
            task_id,
        ):
            influxdb3_local.cache.put(cache_key, level)
