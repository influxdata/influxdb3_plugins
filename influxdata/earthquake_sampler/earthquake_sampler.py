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
            "description": "Optional SQL query override used when `source_type=influxdb_table`.",
            "required": false
        },
        {
            "name": "lookback_minutes",
            "example": "15",
            "description": "Lookback window for `source_type=influxdb_table` when `source_query` is not provided. Defaults to 15.",
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


def _parse_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_float(value: Any, default: Optional[float]) -> Optional[float]:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_int(value: Any, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_tag(value: Any, fallback: str = "unknown") -> str:
    if value is None:
        return fallback
    out = str(value).strip()
    if not out:
        return fallback
    return out.replace(",", " ").replace("=", " ")


def _safe_string(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _to_ns_from_ms(ms: Any) -> Optional[int]:
    if ms is None:
        return None
    try:
        return int(float(ms) * 1_000_000)
    except (TypeError, ValueError):
        return None


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
        return int(dt.timestamp() * 1_000_000_000)
    except Exception:
        return None



def _coerce_time_ns(value: Any) -> Optional[int]:
    """Coerce a timestamp of unknown shape to epoch nanoseconds.

    InfluxDB queries return the time column as an integer (nanoseconds), while
    flat JSON feeds may use ISO strings or epoch seconds/milliseconds, so the
    unit of a bare number is inferred from its magnitude.
    """
    if value is None:
        return None
    if isinstance(value, str):
        ns = _to_ns_from_iso(value)
        if ns is not None:
            return ns
        try:
            value = float(value)
        except (TypeError, ValueError):
            return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric) or numeric <= 0:
        return None
    if numeric > 1e17:  # nanoseconds
        return int(numeric)
    if numeric > 1e14:  # microseconds
        return int(numeric * 1_000)
    if numeric > 1e11:  # milliseconds
        return int(numeric * 1_000_000)
    return int(numeric * 1_000_000_000)  # seconds


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
    raw = event.get("updated_ms")
    if raw is not None:
        try:
            return int(raw)
        except (TypeError, ValueError):
            pass
    ts_ns = event.get("event_time_ns")
    if ts_ns is not None:
        try:
            return int(int(ts_ns) / 1_000_000)
        except (TypeError, ValueError):
            pass
    return 0


def _fetch_payload(url: str, user_agent: str) -> Dict[str, Any]:
    req = Request(url)
    req.add_header("User-Agent", user_agent)
    req.add_header("Accept", "application/geo+json, application/json")
    with urlopen(req, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def _fetch_table_rows(
    influxdb3_local,
    source_table: str,
    max_events: int,
    lookback_minutes: int,
    source_query: str,
) -> List[Dict[str, Any]]:
    if source_query.strip():
        query = source_query
    else:
        safe_lookback = max(1, int(lookback_minutes))
        safe_table = source_table.replace('"', '""')
        query = (
            f'SELECT * FROM "{safe_table}" '
            f"WHERE time >= now() - INTERVAL '{safe_lookback} minutes' "
            f"ORDER BY time DESC "
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


def _write_event(
    influxdb3_local,
    measurement: str,
    event: Dict[str, Any],
    fallback_ts_ns: int,
    use_event_timestamp: bool,
) -> bool:
    timestamp_ns = _event_timestamp_ns(event, fallback_ts_ns, use_event_timestamp)

    line = LineBuilder(measurement).time_ns(timestamp_ns)
    line.tag("event_type", _safe_tag(event.get("event_type"), "earthquake"))
    line.tag("status", _safe_tag(event.get("status"), "unknown"))
    line.tag("alert", _safe_tag(event.get("alert"), "none"))
    line.tag("net", _safe_tag(event.get("net"), "unknown"))
    line.tag("mag_type", _safe_tag(event.get("mag_type"), "unknown"))

    for field_name in (
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
    ):
        raw = event.get(field_name)
        if raw is None:
            continue
        try:
            val = float(raw)
            if math.isfinite(val):
                line.float64_field(field_name, val)
        except (TypeError, ValueError):
            continue

    for field_name in (
        "significance",
        "felt_reports",
        "tsunami",
        "nst",
        "mag_nst",
    ):
        raw = event.get(field_name)
        if raw is None:
            continue
        try:
            line.int64_field(field_name, int(raw))
        except (TypeError, ValueError):
            continue

    updated_ms = event.get("updated_ms")
    if updated_ms is not None:
        try:
            line.int64_field("updated_ms", int(updated_ms))
        except (TypeError, ValueError):
            pass

    line.string_field("event_id", _safe_string(event.get("event_id")))
    line.string_field("place", _safe_string(event.get("place")))
    line.string_field("title", _safe_string(event.get("title")))
    line.string_field("url", _safe_string(event.get("url")))
    for field_name in ("location_source", "mag_source"):
        raw = event.get(field_name)
        if raw is not None:
            line.string_field(field_name, _safe_string(raw))

    # Buffered write() only flushes after the trigger returns, so a failed
    # write could never be caught or counted here; write_sync raises inline.
    influxdb3_local.write_sync(line, no_sync=True)
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

    line = LineBuilder(measurement).time_ns(timestamp_ns)
    for column, event_key in QUAKE_FLOAT_COLUMN_MAP.items():
        value = event.get(event_key)
        if value is None:
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(numeric):
            line.float64_field(column, numeric)

    for column, event_key in QUAKE_STRING_COLUMN_MAP.items():
        value = event.get(event_key)
        if value is not None:
            line.string_field(column, _safe_string(value))

    influxdb3_local.write_sync(line, no_sync=True)
    return True



def process_scheduled_call(
    influxdb3_local,
    call_time: datetime,
    args: Optional[Dict[str, Any]] = None,
) -> None:
    task_id = str(uuid.uuid4())

    global _ENABLE_FULL_LOGGING
    _ENABLE_FULL_LOGGING = _parse_bool((args or {}).get("enable_full_logging"), False)

    args = args or {}
    source_type = _safe_string(args.get("source_type", "http")).lower() or "http"
    if source_type not in {"http", "influxdb_table"}:
        source_type = "http"

    source_url = _safe_string(args.get("source_url"))
    source_format = _safe_string(args.get("source_format", "usgs_geojson")).lower() or "usgs_geojson"
    if source_format not in {"usgs_geojson", "flat_json"}:
        source_format = "usgs_geojson"

    feed = _safe_string(args.get("feed", "all_hour"))
    if feed not in FEED_URLS:
        feed = "all_hour"

    source_table = _safe_string(args.get("source_table", "quake")) or "quake"
    source_query = _safe_string(args.get("source_query"))
    lookback_minutes = max(1, _parse_int(args.get("lookback_minutes"), 15))

    source = source_url if source_url else feed
    if source_type == "influxdb_table":
        source = source_query if source_query else source_table

    measurement = _safe_string(args.get("measurement", "earthquakes")) or "earthquakes"
    write_quake_schema = _parse_bool(args.get("write_quake_schema"), False)
    min_magnitude = _parse_float(args.get("min_magnitude"), None)
    max_events = max(1, _parse_int(args.get("max_events"), 250))
    use_event_timestamp = _parse_bool(args.get("use_event_timestamp"), True)
    if write_quake_schema and not use_event_timestamp:
        # Quake-schema rows have no tags, so a shared trigger timestamp would
        # collapse every event in a run into a single surviving row.
        influxdb3_local.warn(
            f"[{task_id}] use_event_timestamp=false is ignored when write_quake_schema=true; "
            f"using event timestamps to keep events distinct"
        )
        use_event_timestamp = True
    skip_unchanged = _parse_bool(args.get("skip_unchanged"), True)
    user_agent = _safe_string(args.get("user_agent", "InfluxDB3-Earthquake-Plugin/1.0")) or "InfluxDB3-Earthquake-Plugin/1.0"

    now_ns = int(call_time.replace(tzinfo=timezone.utc).timestamp() * 1_000_000_000)

    items: List[Dict[str, Any]] = []
    if source_type == "influxdb_table":
        try:
            items = _fetch_table_rows(
                influxdb3_local=influxdb3_local,
                source_table=source_table,
                max_events=max_events,
                lookback_minutes=lookback_minutes,
                source_query=source_query,
            )
            source_format = "flat_json"
        except Exception as e:
            influxdb3_local.error(f"[{task_id}] Query error while reading source table '{source_table}': {_exc(e)}")
            return
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
            influxdb3_local.error(f"[{task_id}] Failed to write earthquake event: {_exc(e)}")

    influxdb3_local.info(
        f"[{task_id}] Earthquake sampler complete: "
        f"source={source}, format={source_format}, fetched={fetched}, "
        f"written={written}, skipped={skipped}, measurement={measurement}, "
        f"min_magnitude={min_magnitude}"
    )
