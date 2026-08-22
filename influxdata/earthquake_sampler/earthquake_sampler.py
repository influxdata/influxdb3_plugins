"""
{
    "plugin_type": ["scheduled"],
    "scheduled_args_config": [
        {
            "name": "feed",
            "example": "all_hour",
            "description": "USGS GeoJSON feed key. One of: all_hour, all_day, all_week, all_month, significant_hour, significant_day, significant_week, significant_month, 4.5_hour, 4.5_day, 4.5_week, 4.5_month, 2.5_hour, 2.5_day, 2.5_week, 2.5_month, 1.0_hour, 1.0_day, 1.0_week, 1.0_month.",
            "required": false
        },
        {
            "name": "source_url",
            "example": "https://example.com/earthquakes.json",
            "description": "Optional custom source URL (http or https only). When provided, it overrides `feed` and uses `source_format` parsing.",
            "required": false
        },
        {
            "name": "source_type",
            "example": "influxdb_table",
            "description": "Data source type: `http` (default) fetches JSON from `source_url` or `feed`; `influxdb_table` queries an existing table in the trigger database.",
            "required": false
        },
        {
            "name": "source_format",
            "example": "flat_json",
            "description": "Source parser: `usgs_geojson` (default) or `flat_json`. Used with `source_type=http`. Use `flat_json` for records like {id, latitude, longitude, mag, time, ...}.",
            "required": false
        },
        {
            "name": "source_table",
            "example": "quake",
            "description": "Source table name when `source_type=influxdb_table`. Defaults to quake.",
            "required": false
        },
        {
            "name": "source_query",
            "example": "SELECT * FROM quake WHERE time >= now() - INTERVAL '15 minutes' ORDER BY time DESC LIMIT 500",
            "description": "Optional SQL query override used when `source_type=influxdb_table`. Trigger arguments are comma-separated, so a query containing commas must be supplied through `config_file_path` instead.",
            "required": false
        },
        {
            "name": "lookback_minutes",
            "example": "15",
            "description": "Initial lookback window in minutes for `source_type=influxdb_table` when `source_query` is not provided. Later runs page forward from the cached fetch watermark while `skip_unchanged=true`. Defaults to 15.",
            "required": false
        },
        {
            "name": "measurement",
            "example": "earthquakes",
            "description": "Destination measurement name for earthquake events. Defaults to earthquakes.",
            "required": false
        },
        {
            "name": "write_quake_schema",
            "example": "true",
            "description": "Write USGS events using the existing quake table's column names. All numeric columns are written as float64 (matching CSV-imported quake tables); no tags or extra columns are written. Use with measurement=quake.",
            "required": false
        },

        {
            "name": "min_magnitude",
            "example": "2.5",
            "description": "Optional minimum earthquake magnitude to ingest. When omitted, no magnitude filtering is applied (USGS feeds include negative-magnitude microseisms and events with no magnitude).",
            "required": false
        },
        {
            "name": "max_events",
            "example": "200",
            "description": "Maximum number of events to process per run after filtering and sorting. Defaults to 250.",
            "required": false
        },
        {
            "name": "use_event_timestamp",
            "example": "true",
            "description": "Use event time for point timestamp. If false, use trigger execution time. Defaults to true. Ignored (forced true) when write_quake_schema=true.",
            "required": false
        },
        {
            "name": "skip_unchanged",
            "example": "true",
            "description": "Skip events whose update marker is not newer than the last written copy of the same event (per-event cache). Events without an id are always written. Defaults to true.",
            "required": false
        },
        {
            "name": "user_agent",
            "example": "InfluxDB3-Earthquake-Plugin/1.0",
            "description": "Custom User-Agent header for API requests.",
            "required": false
        },
        {
            "name": "enable_full_logging",
            "example": "false",
            "description": "When true, full exception messages are logged. When false (default), only exception types are logged.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "earthquake_sampler_config_scheduler.toml",
            "description": "Path to a TOML configuration file, relative to the plugin directory. Values in the file override the inline trigger arguments.",
            "required": false
        }
    ]
}
"""

import json
import math
import uuid
import zlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from influxdata_plugin_utils.config import Validator, load_plugin_config
from influxdata_plugin_utils.introspection import get_table_names
from influxdata_plugin_utils.parsing import parse_bool, parse_int
from influxdata_plugin_utils.write import build_line_typed, write_data


# At server runtime LineBuilder is injected as a builtin. In test environments
# pytest patches this module-level name to a vendored copy.
try:
    LineBuilder  # type: ignore  # noqa: F821
except NameError:
    LineBuilder = None  # placeholder for test patching


_ENABLE_FULL_LOGGING: bool = True


FEED_URLS = {
    "all_hour": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
    "all_day": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
    "all_week": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson",
    "all_month": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson",
    "significant_hour": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_hour.geojson",
    "significant_day": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_day.geojson",
    "significant_week": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_week.geojson",
    "significant_month": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_month.geojson",
    "4.5_hour": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_hour.geojson",
    "4.5_day": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson",
    "4.5_week": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_week.geojson",
    "4.5_month": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_month.geojson",
    "2.5_hour": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_hour.geojson",
    "2.5_day": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson",
    "2.5_week": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.geojson",
    "2.5_month": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_month.geojson",
    "1.0_hour": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_hour.geojson",
    "1.0_day": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_day.geojson",
    "1.0_week": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.geojson",
    "1.0_month": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_month.geojson",
}


def _exc(e: BaseException) -> str:
    return str(e) if _ENABLE_FULL_LOGGING else type(e).__name__


DEFAULT_USER_AGENT = "InfluxDB3-Earthquake-Plugin/1.0"


def _optional_text(raw: Any) -> str:
    return "" if raw is None else str(raw).strip()


def _text_or(default: str):
    """Build a cast that falls back to `default` for empty or missing text."""

    def cast(raw: Any) -> str:
        return _optional_text(raw) or default

    return cast


def _choice_or(default: str):
    """Cast for `is_in` arguments: normalize case, fall back to `default` when empty."""

    def cast(raw: Any) -> str:
        return _optional_text(raw).lower() or default

    return cast


def _finite_float(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError(f"must be a finite number, got {raw!r}")
    return value


def _positive_int(raw: Any) -> int:
    return parse_int(raw, minimum=1)


_VALIDATORS = [
    Validator("feed", default="all_hour", cast=_choice_or("all_hour"), is_in=sorted(FEED_URLS)),
    Validator("source_url", default="", cast=_optional_text),
    Validator(
        "source_type",
        default="http",
        cast=_choice_or("http"),
        is_in=["http", "influxdb_table"],
    ),
    Validator(
        "source_format",
        default="usgs_geojson",
        cast=_choice_or("usgs_geojson"),
        is_in=["usgs_geojson", "flat_json"],
    ),
    Validator("source_table", default="quake", cast=_text_or("quake")),
    Validator("source_query", default="", cast=_optional_text),
    Validator("lookback_minutes", default=15, cast=_positive_int),
    Validator("measurement", default="earthquakes", cast=_text_or("earthquakes")),
    Validator("write_quake_schema", default=False, cast=parse_bool),
    Validator("min_magnitude", default=None, cast=_finite_float),
    Validator("max_events", default=250, cast=_positive_int),
    Validator("use_event_timestamp", default=True, cast=parse_bool),
    Validator("skip_unchanged", default=True, cast=parse_bool),
    Validator("user_agent", default=DEFAULT_USER_AGENT, cast=_text_or(DEFAULT_USER_AGENT)),
    Validator("enable_full_logging", default=False, cast=parse_bool),
]


def _load_config(influxdb3_local, args: Optional[Dict[str, Any]], task_id: str) -> Optional[Dict[str, Any]]:
    """Load and validate the trigger configuration.

    Returns:
        Config keyed by lower-case name, or None when the inline arguments
        themselves are invalid.
    """
    args = args or {}
    config_file_path = args.get("config_file_path")
    if config_file_path and not str(config_file_path).endswith(".toml"):
        influxdb3_local.error(f"[{task_id}] Invalid config file format: expected a .toml file")
        config_file_path = None

    try:
        loaded = load_plugin_config(args, validators=_VALIDATORS, source="args")
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Invalid configuration: {e}")
        return None

    if config_file_path:
        try:
            loaded = load_plugin_config(args, validators=_VALIDATORS, source="merge")
            influxdb3_local.info(f"[{task_id}] Loaded configuration from {config_file_path}")
        except Exception as e:
            influxdb3_local.error(
                f"[{task_id}] Failed to apply config file '{config_file_path}': {_exc(e)}. "
                f"Continuing with inline arguments"
            )

    return {key.lower(): value for key, value in loaded.as_dict().items()}


def _redact_url(url: str) -> str:
    """Drop userinfo and query string, which may carry credentials or tokens."""
    parts = urlparse(url)
    host = parts.hostname or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    return f"{parts.scheme}://{host}{parts.path}"


def _truncate(text: str, limit: int = 80) -> str:
    text = " ".join(str(text).split())
    if len(text) <= limit:
        return text
    return text[:limit] + "..."


def _safe_tag(value: Any, fallback: str = "unknown") -> str:
    if value is None:
        return fallback
    out = str(value).strip()
    if not out:
        return fallback
    return out.replace(",", " ").replace("=", " ")


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _to_ns_from_ms(ms: Any) -> Optional[int]:
    """Convert epoch milliseconds to nanoseconds on an exact integer path."""
    if ms is None or isinstance(ms, bool):
        return None
    if not isinstance(ms, (int, float)):
        text = str(ms).strip()
        if not text:
            return None
        try:
            ms = int(text)
        except ValueError:
            try:
                ms = float(text)
            except ValueError:
                return None
    if isinstance(ms, float):
        if not math.isfinite(ms):
            return None
        return int(round(ms * 1_000)) * 1_000
    return ms * 1_000_000


def _to_ns_from_iso(ts: Any) -> Optional[int]:
    if ts is None:
        return None
    s = str(ts).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
    except Exception:
        return None
    delta = dt - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000



def _coerce_time_ns(value: Any) -> Optional[int]:
    """Coerce a timestamp of unknown shape to epoch nanoseconds.

    InfluxDB queries return the time column as an integer (nanoseconds), while
    flat JSON feeds may use ISO strings or epoch seconds/milliseconds, so the
    unit of a bare number is inferred from its magnitude.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str):
        ns = _to_ns_from_iso(value)
        if ns is not None:
            return ns
        try:
            value = int(value)
        except (TypeError, ValueError):
            try:
                value = float(value)
            except (TypeError, ValueError):
                return None
    # Integers stay on an exact path: epoch nanoseconds exceed float64's
    # 53-bit mantissa, and a float round-trip perturbs the low ~8 digits —
    # which also un-aligns ms timestamps and defeats the sub-ms dedup offset.
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        if 0 < value <= 1e11:  # fractional epoch seconds
            return int(value * 1_000_000_000)
        value = int(value)
    elif not isinstance(value, int):
        return None
    if value <= 0:
        return None
    if value > 10**17:  # nanoseconds
        return value
    if value > 10**14:  # microseconds
        return value * 1_000
    if value > 10**11:  # milliseconds
        return value * 1_000_000
    return value * 1_000_000_000  # seconds


# Outlives the longest (monthly) feed window, so an event's marker survives
# for as long as the event can still appear in fetched data.
EVENT_MARKER_TTL_SECONDS = 45 * 24 * 3600


def _event_marker_key(measurement: str, event_id: Any) -> str:
    return f"earthquake_sampler:event_marker:{measurement}:{event_id}"


def _get_cached_int(influxdb3_local, key: str) -> Optional[int]:
    raw = influxdb3_local.cache.get(key)
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def _to_update_marker_ms(event: Dict[str, Any]) -> int:
    """Millisecond marker that detects a revised copy of the same event."""
    marker_ns = _coerce_time_ns(event.get("updated_ms"))
    if marker_ns is None:
        marker_ns = _coerce_time_ns(event.get("event_time_ns"))
    if marker_ns is None:
        return 0
    return marker_ns // 1_000_000


def _fetch_payload(url: str, user_agent: str) -> Dict[str, Any]:
    req = Request(url)
    req.add_header("User-Agent", user_agent)
    req.add_header("Accept", "application/geo+json, application/json")
    with urlopen(req, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def _ns_to_rfc3339(ns: int) -> str:
    seconds, remainder = divmod(int(ns), 1_000_000_000)
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{remainder:09d}Z"


def _table_watermark_key(measurement: str, source_table: str) -> str:
    return f"earthquake_sampler:table_watermark:{measurement}:{source_table}"


def _fetch_table_rows(
    influxdb3_local,
    source_table: str,
    max_events: int,
    lookback_minutes: int,
    source_query: str,
    watermark_ns: Optional[int] = None,
) -> List[Dict[str, Any]]:
    if source_query.strip():
        query = source_query
    else:
        safe_table = source_table.replace('"', '""')
        # Page oldest-first from the fetch watermark: DESC + LIMIT starved
        # rows between the watermark and the newest LIMIT rows under load.
        if watermark_ns is not None:
            time_filter = f"time > '{_ns_to_rfc3339(watermark_ns)}'"
        else:
            safe_lookback = max(1, int(lookback_minutes))
            time_filter = f"time >= now() - INTERVAL '{safe_lookback} minutes'"
        query = (
            f'SELECT * FROM "{safe_table}" '
            f"WHERE {time_filter} "
            f"ORDER BY time ASC "
            f"LIMIT {max_events}"
        )
    rows = influxdb3_local.query(query)
    if not isinstance(rows, list):
        return []
    return [r for r in rows if isinstance(r, dict)]


def _normalize_usgs_feature(feature: Dict[str, Any]) -> Dict[str, Any]:
    properties = feature.get("properties", {}) if isinstance(feature, dict) else {}
    geometry = feature.get("geometry", {}) if isinstance(feature, dict) else {}
    coordinates = geometry.get("coordinates", []) if isinstance(geometry, dict) else []

    return {
        "event_id": feature.get("id"),
        "event_type": properties.get("type"),
        "status": properties.get("status"),
        "alert": properties.get("alert"),
        "net": properties.get("net"),
        "mag_type": properties.get("magType"),
        "magnitude": properties.get("mag"),
        "significance": properties.get("sig"),
        "felt_reports": properties.get("felt"),
        "tsunami": properties.get("tsunami"),
        "mmi": properties.get("mmi"),
        "nst": properties.get("nst"),
        "depth_km": coordinates[2] if len(coordinates) > 2 else None,
        "longitude": coordinates[0] if len(coordinates) > 0 else None,
        "latitude": coordinates[1] if len(coordinates) > 1 else None,
        "gap_degrees": properties.get("gap"),
        "distance_km": properties.get("dmin"),
        "rms": properties.get("rms"),
        "updated_ms": properties.get("updated"),
        "event_time_ns": _to_ns_from_ms(properties.get("time")),
        "place": properties.get("place"),
        "title": properties.get("title"),
        "url": properties.get("url"),
        # depthError/horizontalError/magError/magNst/locationSource/magSource
        # exist only in the USGS CSV feeds, never in GeoJSON properties.
    }


def _normalize_flat_event(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "event_id": item.get("id"),
        "event_type": item.get("type"),
        "status": item.get("status"),
        "alert": item.get("alert"),
        "net": item.get("net"),
        "mag_type": item.get("magType"),
        "magnitude": item.get("mag"),
        "significance": item.get("sig"),
        "felt_reports": item.get("felt"),
        "tsunami": item.get("tsunami"),
        "mmi": item.get("mmi"),
        "nst": item.get("nst"),
        "depth_km": item.get("depth"),
        "longitude": item.get("longitude"),
        "latitude": item.get("latitude"),
        "gap_degrees": item.get("gap"),
        "distance_km": item.get("dmin"),
        "rms": item.get("rms"),
        "updated_ms": item.get("updated") or item.get("updatedMs"),
        "event_time_ns": _coerce_time_ns(item.get("time")) or _to_ns_from_ms(item.get("timeMs")),
        "place": item.get("place"),
        "title": item.get("title") or item.get("place"),
        "url": item.get("url"),
        "depth_error": item.get("depthError"),
        "horizontal_error": item.get("horizontalError"),
        "mag_error": item.get("magError"),
        "mag_nst": item.get("magNst"),
        "location_source": item.get("locationSource"),
        "mag_source": item.get("magSource"),
    }


def _extract_events(payload: Dict[str, Any], source_format: str) -> List[Dict[str, Any]]:
    if source_format == "flat_json":
        if isinstance(payload, list):
            return [e for e in payload if isinstance(e, dict)]
        if isinstance(payload, dict):
            if isinstance(payload.get("events"), list):
                return [e for e in payload.get("events", []) if isinstance(e, dict)]
            return [payload]
        return []

    # usgs_geojson default
    if isinstance(payload, dict) and isinstance(payload.get("features"), list):
        return [e for e in payload.get("features", []) if isinstance(e, dict)]
    return []


def _normalize_event(item: Dict[str, Any], source_format: str) -> Dict[str, Any]:
    if source_format == "flat_json":
        return _normalize_flat_event(item)
    return _normalize_usgs_feature(item)


def _dedup_ns_offset(event_id: Any) -> int:
    if event_id is None:
        return 0
    return zlib.crc32(str(event_id).encode("utf-8")) % 1_000_000


def _event_timestamp_ns(
    event: Dict[str, Any],
    fallback_ts_ns: int,
    use_event_timestamp: bool,
) -> int:
    timestamp_ns = fallback_ts_ns
    event_time_ns = event.get("event_time_ns")
    if use_event_timestamp and event_time_ns is not None:
        try:
            timestamp_ns = int(event_time_ns)
        except (TypeError, ValueError):
            pass

    # USGS event times are millisecond-precision, and the sparse series keys
    # (empty in quake-schema mode) make timestamp collisions between distinct
    # events overwrite each other. Fill the zero sub-ms bits of ms-aligned
    # timestamps with a stable per-event offset; ms-level time is unchanged,
    # and true nanosecond timestamps (e.g. from influxdb_table sources) are
    # left alone.
    if timestamp_ns % 1_000_000 == 0:
        timestamp_ns += _dedup_ns_offset(event.get("event_id"))
    return timestamp_ns


EVENT_FLOAT_FIELDS = (
    "magnitude",
    "depth_km",
    "longitude",
    "latitude",
    "gap_degrees",
    "distance_km",
    "rms",
    "mmi",
    "depth_error",
    "horizontal_error",
    "mag_error",
)

EVENT_INT_FIELDS = (
    "significance",
    "felt_reports",
    "tsunami",
    "nst",
    "mag_nst",
    "updated_ms",
)

EVENT_STRING_FIELDS = (
    "event_id",
    "place",
    "title",
    "url",
    "location_source",
    "mag_source",
)


def _float_or_none(raw: Any) -> Optional[float]:
    """Coerce to a finite float, or None so the field is left out of the line."""
    if raw is None:
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _int_or_none(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _write_line(influxdb3_local, line) -> None:
    """Write one line via write_sync, which raises inline so the caller can count it.

    Retries stay off: a rejected line should be reported at once rather than
    sleeping through a backoff for every event in the run.
    """
    write_data(influxdb3_local, [line], batch=False, retries=0, no_sync=True)


def _write_event(
    influxdb3_local,
    measurement: str,
    event: Dict[str, Any],
    fallback_ts_ns: int,
    use_event_timestamp: bool,
) -> bool:
    timestamp_ns = _event_timestamp_ns(event, fallback_ts_ns, use_event_timestamp)

    typed_fields: Dict[str, Any] = {
        name: (_float_or_none(event.get(name)), "float") for name in EVENT_FLOAT_FIELDS
    }
    typed_fields.update(
        {name: (_int_or_none(event.get(name)), "int") for name in EVENT_INT_FIELDS}
    )
    typed_fields.update(
        {name: (event.get(name), "string") for name in EVENT_STRING_FIELDS}
    )

    line = build_line_typed(
        LineBuilder,
        measurement,
        tags={
            "event_type": _safe_tag(event.get("event_type"), "earthquake"),
            "status": _safe_tag(event.get("status"), "unknown"),
            "alert": _safe_tag(event.get("alert"), "none"),
            "net": _safe_tag(event.get("net"), "unknown"),
            "mag_type": _safe_tag(event.get("mag_type"), "unknown"),
        },
        typed_fields=typed_fields,
        time_ns=timestamp_ns,
    )
    _write_line(influxdb3_local, line)
    return True


QUAKE_FLOAT_COLUMN_MAP = {
    "depth": "depth_km",
    "depthError": "depth_error",
    "dmin": "distance_km",
    "gap": "gap_degrees",
    "horizontalError": "horizontal_error",
    "latitude": "latitude",
    "longitude": "longitude",
    "mag": "magnitude",
    "magError": "mag_error",
    "magNst": "mag_nst",
    "nst": "nst",
    "rms": "rms",
}

QUAKE_STRING_COLUMN_MAP = {
    "id": "event_id",
    "locationSource": "location_source",
    "magSource": "mag_source",
    "magType": "mag_type",
    "net": "net",
    "place": "place",
    "status": "status",
    "type": "event_type",
}


def _write_quake_event(
    influxdb3_local,
    measurement: str,
    event: Dict[str, Any],
    fallback_ts_ns: int,
    use_event_timestamp: bool,
) -> bool:
    """Write a normalized USGS event to a table using only the canonical quake schema."""
    timestamp_ns = _event_timestamp_ns(event, fallback_ts_ns, use_event_timestamp)

    typed_fields: Dict[str, Any] = {
        column: (_float_or_none(event.get(event_key)), "float")
        for column, event_key in QUAKE_FLOAT_COLUMN_MAP.items()
    }
    typed_fields.update(
        {
            column: (event.get(event_key), "string")
            for column, event_key in QUAKE_STRING_COLUMN_MAP.items()
        }
    )

    line = build_line_typed(
        LineBuilder,
        measurement,
        typed_fields=typed_fields,
        time_ns=timestamp_ns,
    )
    _write_line(influxdb3_local, line)
    return True



def process_scheduled_call(
    influxdb3_local,
    call_time: datetime,
    args: Optional[Dict[str, Any]] = None,
) -> None:
    task_id = str(uuid.uuid4())

    global _ENABLE_FULL_LOGGING
    try:
        _ENABLE_FULL_LOGGING = parse_bool((args or {}).get("enable_full_logging", False))
    except ValueError:
        _ENABLE_FULL_LOGGING = False

    config = _load_config(influxdb3_local, args, task_id)
    if config is None:
        return

    _ENABLE_FULL_LOGGING = config["enable_full_logging"]

    source_type: str = config["source_type"]
    source_url: str = config["source_url"]
    source_format: str = config["source_format"]
    feed: str = config["feed"]
    source_table: str = config["source_table"]
    source_query: str = config["source_query"]
    lookback_minutes: int = config["lookback_minutes"]
    measurement: str = config["measurement"]
    write_quake_schema: bool = config["write_quake_schema"]
    min_magnitude: Optional[float] = config["min_magnitude"]
    max_events: int = config["max_events"]
    use_event_timestamp: bool = config["use_event_timestamp"]
    skip_unchanged: bool = config["skip_unchanged"]
    user_agent: str = config["user_agent"]

    if write_quake_schema and not use_event_timestamp:
        # Quake-schema rows have no tags, so a shared trigger timestamp would
        # collapse every event in a run into a single surviving row.
        influxdb3_local.warn(
            f"[{task_id}] use_event_timestamp=false is ignored when write_quake_schema=true; "
            f"using event timestamps to keep events distinct"
        )
        use_event_timestamp = True

    # Display name for logs: URLs are stripped of userinfo/query string and
    # custom SQL is truncated so credentials or literals stay out of the logs.
    if source_type == "influxdb_table":
        source = f"query:{_truncate(source_query)}" if source_query else source_table
    else:
        source = _redact_url(source_url) if source_url else feed

    now_ns = int(call_time.replace(tzinfo=timezone.utc).timestamp() * 1_000_000_000)

    items: List[Dict[str, Any]] = []
    if source_type == "influxdb_table":
        if not source_query:
            # Uncached: a cached list would keep rejecting a table created later.
            try:
                table_names = get_table_names(influxdb3_local, use_cache=False)
            except Exception as e:
                influxdb3_local.error(f"[{task_id}] Failed to list tables: {_exc(e)}")
                return
            if source_table not in table_names:
                influxdb3_local.error(
                    f"[{task_id}] Source table '{source_table}' not found in the trigger database"
                )
                return

        watermark_key = _table_watermark_key(measurement, source_table)
        use_watermark = skip_unchanged and not source_query.strip()
        watermark_ns = _get_cached_int(influxdb3_local, watermark_key) if use_watermark else None
        try:
            items = _fetch_table_rows(
                influxdb3_local=influxdb3_local,
                source_table=source_table,
                max_events=max_events,
                lookback_minutes=lookback_minutes,
                source_query=source_query,
                watermark_ns=watermark_ns,
            )
            source_format = "flat_json"
        except Exception as e:
            influxdb3_local.error(f"[{task_id}] Query error while reading source table '{source_table}': {_exc(e)}")
            return
        if use_watermark and items:
            # Fetch progress, not write progress: dedup is the per-event
            # cache's job; this only keeps paging moving forward.
            max_time_ns = max(
                (t for t in (_coerce_time_ns(r.get("time")) for r in items) if t is not None),
                default=None,
            )
            if max_time_ns is not None:
                influxdb3_local.cache.put(watermark_key, max_time_ns, ttl=EVENT_MARKER_TTL_SECONDS)
    else:
        url = source_url if source_url else FEED_URLS[feed]
        scheme = urlparse(url).scheme.lower()
        if scheme not in ("http", "https"):
            # urlopen would happily fetch file:// or ftp:// URLs.
            influxdb3_local.error(
                f"[{task_id}] Unsupported source_url scheme '{scheme or 'none'}': only http and https are allowed"
            )
            return
        try:
            payload = _fetch_payload(url, user_agent)
        except HTTPError as e:
            influxdb3_local.error(f"[{task_id}] HTTP error while fetching source '{source}': {_exc(e)}")
            return
        except URLError as e:
            influxdb3_local.error(f"[{task_id}] Network error while fetching source '{source}': {_exc(e)}")
            return
        except json.JSONDecodeError as e:
            influxdb3_local.error(f"[{task_id}] Invalid JSON from source '{source}': {_exc(e)}")
            return
        except Exception as e:
            influxdb3_local.error(f"[{task_id}] Unexpected fetch error: {_exc(e)}")
            return

        items = _extract_events(payload, source_format)

    if not items:
        influxdb3_local.warn(f"[{task_id}] No events found for source_type={source_type} source_format={source_format}")
        return

    normalized = [_normalize_event(item, source_format) for item in items]

    fetched = 0
    written = 0
    skipped = 0

    # Newest-first so the max_events cap prioritizes recent events; anything
    # deferred by the cap stays uncached and is picked up on a later run.
    normalized.sort(key=_to_update_marker_ms, reverse=True)

    for event in normalized:
        if written >= max_events:
            break
        fetched += 1

        mag = event.get("magnitude")
        try:
            magnitude = float(mag) if mag is not None else None
        except (TypeError, ValueError):
            magnitude = None

        if min_magnitude is not None and (magnitude is None or magnitude < min_magnitude):
            skipped += 1
            continue

        marker = _to_update_marker_ms(event)
        event_id = event.get("event_id")
        marker_key = _event_marker_key(measurement, event_id) if event_id is not None else None
        if skip_unchanged and marker_key is not None:
            last_marker = _get_cached_int(influxdb3_local, marker_key)
            if last_marker is not None and marker <= last_marker:
                skipped += 1
                continue

        try:
            if write_quake_schema:
                did_write = _write_quake_event(
                    influxdb3_local=influxdb3_local,
                    measurement=measurement,
                    event=event,
                    fallback_ts_ns=now_ns,
                    use_event_timestamp=use_event_timestamp,
                )
            else:
                did_write = _write_event(
                    influxdb3_local=influxdb3_local,
                    measurement=measurement,
                    event=event,
                    fallback_ts_ns=now_ns,
                    use_event_timestamp=use_event_timestamp,
                )
            if did_write:
                written += 1
                if marker_key is not None:
                    influxdb3_local.cache.put(marker_key, marker, ttl=EVENT_MARKER_TTL_SECONDS)
        except Exception as e:
            skipped += 1
            influxdb3_local.error(
                f"[{task_id}] Failed to write earthquake event "
                f"'{event.get('event_id')}': {_exc(e)}"
            )

    influxdb3_local.info(
        f"[{task_id}] Earthquake sampler complete: "
        f"source={source}, format={source_format}, fetched={fetched}, "
        f"written={written}, skipped={skipped}, measurement={measurement}, "
        f"min_magnitude={min_magnitude}"
    )
