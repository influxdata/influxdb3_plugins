"""
{
    "plugin_type": ["scheduled", "http"],
    "scheduled_args_config": [
        {
            "name": "source_measurement",
            "example": "signal_resampled",
            "description": "Name of the source measurement to scan for gaps.",
            "required": true
        },
        {
            "name": "target_measurement",
            "example": "signal_filled",
            "description": "Target measurement: the full series (existing points plus fills) is written there. Omit to append only the fill points into the source measurement (in-place repair).",
            "required": false
        },
        {
            "name": "interval",
            "example": "1s",
            "description": "Expected series cadence and fill grid step, at least 1ms. Units: 'us', 'ms', 's', 'min', 'h', 'd', 'w'. Defaults to '1s'.",
            "required": false
        },
        {
            "name": "window",
            "example": "10min",
            "description": "How much history each run processes (e.g., '10min', '1h'), at least one interval. Units: 'us', 'ms', 's', 'min', 'h', 'd', 'w'. Defaults to '10min'.",
            "required": false
        },
        {
            "name": "offset",
            "example": "10s",
            "description": "Processing delay for late-arriving data (e.g., '10s'). Units: 'us', 'ms', 's', 'min', 'h', 'd', 'w'. Defaults to '0s'.",
            "required": false
        },
        {
            "name": "method",
            "example": "linear",
            "description": "Fill method: 'linear', 'previous' (LOCF), 'next', 'nearest', 'cubic', 'pchip', or 'constant'. Defaults to 'linear'.",
            "required": false
        },
        {
            "name": "fill_value",
            "example": "0",
            "description": "Constant used when method='constant'; required for it, invalid otherwise.",
            "required": false
        },
        {
            "name": "gap_threshold",
            "example": "2s",
            "description": "Spacing between consecutive points above which a gap is detected, at least one interval. Units: 'us', 'ms', 's', 'min', 'h', 'd', 'w'. Defaults to 1.5x interval.",
            "required": false
        },
        {
            "name": "max_fill_gap",
            "example": "5min",
            "description": "Gaps longer than this are not filled, only reported, at least one gap_threshold. Units: 'us', 'ms', 's', 'min', 'h', 'd', 'w'. Defaults to unlimited (bounded by the lookback).",
            "required": false
        },
        {
            "name": "fields",
            "example": "temperature pressure",
            "description": "Space-separated numeric field names to fill with the configured method; numeric fields not listed are dropped. Defaults to all numeric fields. Non-numeric fields are carried into fill points by last known value unless excluded.",
            "required": false
        },
        {
            "name": "excluded_fields",
            "example": "status note",
            "description": "Space-separated field names of any type to exclude from the output. Listing a field in both 'fields' and 'excluded_fields' is a configuration error.",
            "required": false
        },
        {
            "name": "mark_filled",
            "example": "true",
            "description": "When 'true', filled points carry a boolean marker field (see filled_field_name). Defaults to 'false'.",
            "required": false
        },
        {
            "name": "filled_field_name",
            "example": "filled",
            "description": "Name of the boolean marker field written when mark_filled is 'true'; it must not collide with an existing source field. Defaults to 'filled'.",
            "required": false
        },
        {
            "name": "report_measurement",
            "example": "gapfill_report",
            "description": "Optional measurement receiving one row per detected gap (series, boundaries, duration, points, status).",
            "required": false
        },
        {
            "name": "target_database",
            "example": "mydb",
            "description": "Target database for writing output; requires target_measurement (chain mode). If not provided, uses the trigger's database.",
            "required": false
        },
        {
            "name": "max_retries",
            "example": "5",
            "description": "Maximum number of write attempts. Defaults to 5.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "config.toml",
            "description": "Path to a TOML config file used instead of trigger args (replaces them entirely). Format: 'config.toml'.",
            "required": false
        }
    ]
}
"""

import bisect
import json
import math
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np
from influxdata_plugin_utils.config import Validator, load_plugin_config
from influxdata_plugin_utils.introspection import query_window
from influxdata_plugin_utils.parsing import (
    parse_bool,
    parse_delimited_list,
    parse_timedelta,
    parse_timestamp_ns,
)
from influxdata_plugin_utils.write import (
    _infer_type,
    build_line,
    build_line_typed,
    write_data,
)
from scipy.interpolate import CubicSpline, PchipInterpolator

# LineBuilder is injected by the InfluxDB 3 Processing Engine runtime.
try:
    LineBuilder = LineBuilder  # noqa: F821 — already in scope when injected
except NameError:
    LineBuilder = None  # type: ignore[assignment,misc]

VALID_METHODS: tuple = (
    "linear",
    "previous",
    "next",
    "nearest",
    "cubic",
    "pchip",
    "constant",
)
# Methods whose output is float64; selection methods keep source types.
BLENDING_METHODS: tuple = ("linear", "cubic", "pchip", "constant")

NS_PER_SECOND: int = 1_000_000_000

# Line-protocol integer ranges for int/uint fills.
INT64_MIN: int = -(2**63)
INT64_MAX: int = 2**63 - 1
UINT64_MAX: int = 2**64 - 1

# Upper bound on fill nodes per run.
MAX_FILL_POINTS: int = 1_000_000

# Upper bound on HTTP backfill batches.
MAX_BATCHES: int = 10_000

TIME_FORMAT: str = "%Y-%m-%dT%H:%M:%S.%fZ"

EPOCH: datetime = datetime(1970, 1, 1, tzinfo=timezone.utc)


def datetime_to_ns(dt: datetime) -> int:
    """Exact integer nanoseconds; float timestamp() drifts by ~100ns."""
    return ((dt - EPOCH) // timedelta(microseconds=1)) * 1000


def parse_bool_arg(value, default: bool = False) -> bool:
    """Parse a config boolean; None means the default."""
    if value is None:
        return default
    return parse_bool(value)


# information_schema data types (mirror influxdata_plugin_utils.introspection).
TAG_DATA_TYPE: str = "Dictionary(Int32, Utf8)"
NUMERIC_TYPES: tuple = ("Int64", "UInt64", "Float64", "Int32", "Float32")
LINE_TYPES: dict = {
    "Int64": "int",
    "Int32": "int",
    "UInt64": "uint",
    "Float64": "float",
    "Float32": "float",
    "Boolean": "bool",
    "Utf8": "string",
}


def resolve_schema(
    influxdb3_local, measurement: str, marker: str | None, task_id: str
) -> dict:
    """Resolve the measurement schema with one information_schema query.

    Returns tags, all_fields, numeric_fields and a field -> line-type map.
    A source field named `marker` (set only with mark_filled) is rejected as
    a collision with the plugin-written marker.
    """
    rows: list[dict] = influxdb3_local.query(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = $table",
        {"table": measurement},
    )
    tags: list[str] = []
    all_fields: list[str] = []
    numeric_fields: list[str] = []
    line_types: dict[str, str] = {}
    for row in rows:
        name, data_type = row["column_name"], row.get("data_type", "")
        if data_type == TAG_DATA_TYPE:
            tags.append(name)
            continue
        if name == "time":
            continue
        if marker is not None and name == marker:
            raise Exception(
                f"[{task_id}] source_measurement '{measurement}' already has a "
                f"field named '{marker}'; rename filled_field_name or disable "
                f"mark_filled."
            )
        all_fields.append(name)
        if data_type in NUMERIC_TYPES:
            numeric_fields.append(name)
        if data_type in LINE_TYPES:
            line_types[name] = LINE_TYPES[data_type]
    if not all_fields:
        raise Exception(f"[{task_id}] No fields found for measurement '{measurement}'.")
    return {
        "tags": tags,
        "all_fields": all_fields,
        "numeric_fields": numeric_fields,
        "line_types": line_types,
    }


def detect_gaps(times_ns: list[int], gap_threshold_ns: int) -> list[tuple[int, int]]:
    """Return (left_ns, right_ns) for consecutive points spaced wider than
    the threshold. times_ns must be sorted and unique."""
    times = np.asarray(times_ns, dtype=np.int64)
    idx = np.flatnonzero(np.diff(times) > gap_threshold_ns)
    return [(int(times[i]), int(times[i + 1])) for i in idx]


def group_by_series(rows: list[dict], tags: list[str]) -> dict[tuple, list[dict]]:
    """Group query rows by unique tag-value combinations."""
    series: dict[tuple, list[dict]] = {}
    for row in rows:
        key: tuple = tuple(row.get(tag) for tag in tags)
        series.setdefault(key, []).append(row)
    return series


def select_fields(
    influxdb3_local,
    requested_raw,
    excluded_raw,
    all_fields: list[str],
    numeric_fields: list[str],
    task_id: str,
) -> tuple[list[str], set[str]]:
    """Resolve output fields; returns (fields, method-filled set).

    Method fill applies to `fields` (default: all numeric); non-numeric
    fields are carried by LOCF. `excluded_fields` drops fields of any type.
    """
    requested: list[str] | None = (
        parse_delimited_list(requested_raw) if requested_raw else None
    )
    excluded: set[str] = set(parse_delimited_list(excluded_raw or ""))

    if requested:
        conflict: set[str] = excluded.intersection(requested)
        if conflict:
            raise Exception(
                f"[{task_id}] Fields listed in both 'fields' and "
                f"'excluded_fields': {', '.join(sorted(conflict))}."
            )

    numeric_set: set[str] = set(numeric_fields)
    if requested is None:
        filled: list[str] = [f for f in numeric_fields if f not in excluded]
    else:
        filled = []
        for field in requested:
            if field in numeric_set:
                filled.append(field)
            else:
                influxdb3_local.info(
                    f"[{task_id}] Field '{field}' is not a numeric field, "
                    f"skipping method fill."
                )
        if not filled:
            raise Exception(f"[{task_id}] None of the requested fields are numeric.")

    if not filled:
        raise Exception(f"[{task_id}] No numeric fields to fill.")
    carried: list[str] = [
        f for f in all_fields if f not in numeric_set and f not in excluded
    ]
    return filled + carried, set(filled)


def normalize_config(cfg, task_id: str) -> dict:
    """Validate cross-field constraints and precompute ns quantities."""
    measurement: str = cfg.source_measurement
    # Empty/whitespace values mean "not set" (e.g. an empty UI form field).
    target: str | None = str(cfg.get("target_measurement") or "").strip() or None
    target_database: str | None = str(cfg.get("target_database") or "").strip() or None
    if target == measurement and target_database is None:
        raise Exception(
            f"[{task_id}] target_measurement equals measurement; "
            f"omit it for in-place fill."
        )
    if target is None and target_database:
        raise Exception(
            f"[{task_id}] target_database requires target_measurement: "
            f"in-place repair writes to the source; use chain mode to route "
            f"output to another database."
        )

    interval: timedelta = cfg.interval
    if interval < timedelta(milliseconds=1):
        raise Exception(f"[{task_id}] interval must be at least 1ms.")

    gap_threshold_raw = cfg.get("gap_threshold")
    gap_threshold: timedelta = (
        parse_timedelta(gap_threshold_raw) if gap_threshold_raw else 1.5 * interval
    )
    if gap_threshold < interval:
        raise Exception(f"[{task_id}] gap_threshold must be at least one interval.")

    max_fill_gap_raw = cfg.get("max_fill_gap")
    max_fill_gap: timedelta | None = (
        parse_timedelta(max_fill_gap_raw) if max_fill_gap_raw else None
    )
    if max_fill_gap is not None and max_fill_gap < gap_threshold:
        raise Exception(f"[{task_id}] max_fill_gap must be at least gap_threshold.")

    method: str = cfg.method
    fill_value_raw = cfg.get("fill_value")
    fill_value: float | None = None
    if method == "constant":
        if fill_value_raw is None:
            raise Exception(f"[{task_id}] method 'constant' requires fill_value.")
        fill_value = float(fill_value_raw)
        if not math.isfinite(fill_value):
            raise Exception(f"[{task_id}] fill_value must be a finite number.")
    elif fill_value_raw is not None:
        raise Exception(f"[{task_id}] fill_value is only valid with method 'constant'.")

    mark_filled: bool = parse_bool_arg(cfg.get("mark_filled"))
    filled_field_name: str = str(cfg.get("filled_field_name") or "filled").strip()
    if not filled_field_name or filled_field_name == "time":
        raise Exception(
            f"[{task_id}] filled_field_name must be a non-empty field name."
        )
    report: str | None = cfg.get("report_measurement")
    if report and report in (measurement, target):
        raise Exception(
            f"[{task_id}] report_measurement must differ from measurement "
            f"and target_measurement."
        )

    def to_ns(td: timedelta) -> int:
        return (td // timedelta(microseconds=1)) * 1000

    return {
        "source_measurement": measurement,
        "target_measurement": target,
        "output_measurement": target or measurement,
        "in_place": target is None,
        "interval": interval,
        "interval_ns": to_ns(interval),
        "gap_threshold_ns": to_ns(gap_threshold),
        "max_fill_gap": max_fill_gap,
        "max_fill_gap_ns": to_ns(max_fill_gap) if max_fill_gap else None,
        "method": method,
        "fill_value": fill_value,
        "mark_filled": mark_filled,
        "filled_field_name": filled_field_name,
        "report_measurement": report,
        "target_database": target_database,
        "max_retries": cfg.max_retries,
        "fields": cfg.get("fields"),
        "excluded_fields": cfg.get("excluded_fields"),
    }


def fill_series(
    influxdb3_local,
    series_rows: list[dict],
    tag_values: dict,
    fields: list[str],
    filled_fields: set[str],
    cfg: dict,
    node_end_ns: int,
    node_start_ns: int,
    field_types: dict[str, str] | None,
    budget: int,
    task_id: str,
) -> tuple[list, list, dict]:
    """Detect and fill gaps of one series.

    Gaps are detected per method-filled field on its non-null timestamps;
    non-numeric fields are only attached to fill points by LOCF. In-place mode
    fills a closed gap back to its start, chain mode clips at node_start_ns;
    nodes past node_end_ns wait for the next run. field_types forces each
    column's stored type (in-place); None infers types from values (chain).
    Raises when the series would push fill points past budget. Returns
    (fill lines, report lines, stats).
    """
    fill_points: dict[int, dict] = {}
    reports: list = []
    stats: dict = {
        "filled": 0,
        "skipped": 0,
        "points": 0,
        "sparse": [],
        "out_of_range": [],
    }
    out_of_range_fields: set[str] = set()
    interval_ns: int = cfg["interval_ns"]

    for field in fields:
        if field not in filled_fields:
            continue
        # Deduplicate timestamps keeping the last value (rows are time-ordered).
        points: dict[int, object] = {}
        for row in series_rows:
            value = row.get(field)
            if value is None:
                continue
            points[int(row["time"])] = value
        if len(points) < 2:
            # A single point cannot bound a gap; absent fields skip silently.
            if points:
                stats["sparse"].append(field)
            continue

        times_ns: list[int] = sorted(points)
        gaps: list[tuple[int, int]] = detect_gaps(times_ns, cfg["gap_threshold_ns"])
        if not gaps:
            continue

        method: str = cfg["method"]
        interpolator = None
        base: int = times_ns[0]
        if method in ("cubic", "pchip"):
            if method == "cubic" and len(times_ns) < 4:
                influxdb3_local.warn(
                    f"[{task_id}] Series {tag_values or '{}'} field '{field}': "
                    f"cubic needs >= 4 points, falling back to linear."
                )
                method = "linear"
            else:
                vals = np.asarray([float(points[t]) for t in times_ns])
                x = (np.asarray(times_ns, dtype=np.int64) - base) / NS_PER_SECOND
                cls = CubicSpline if method == "cubic" else PchipInterpolator
                interpolator = cls(x, vals)

        for start_ns, end_ns in gaps:
            # Nodes sit on the epoch-anchored grid strictly inside the gap, so
            # overlapping runs overwrite the same points idempotently. Chain
            # mode clips at node_start_ns so fills stay inside the copied
            # window.
            first: int = (start_ns // interval_ns + 1) * interval_ns
            if not cfg["in_place"] and first < node_start_ns:
                first = node_start_ns
            too_long: bool = bool(
                cfg["max_fill_gap_ns"] and (end_ns - start_ns) > cfg["max_fill_gap_ns"]
            )
            hi: int = min(end_ns, node_end_ns)
            n_writable: int = (
                (hi - first + interval_ns - 1) // interval_ns if first < hi else 0
            )
            filled_now: bool = not too_long and n_writable > 0
            if filled_now:
                if stats["points"] + n_writable > budget:
                    raise Exception(
                        f"[{task_id}] more than {MAX_FILL_POINTS} fill points "
                        f"in one run; narrow the processing range, increase "
                        f"interval, or set max_fill_gap."
                    )
                stats["filled"] += 1
                left, right = points[start_ns], points[end_ns]
                ftype: str | None = field_types.get(field) if field_types else None
                int_fill: bool = ftype in ("int", "uint")
                for node in np.arange(first, hi, interval_ns, dtype=np.int64):
                    ts = int(node)
                    if method == "constant":
                        value = cfg["fill_value"]
                    elif method == "previous":
                        value = left
                    elif method == "next":
                        value = right
                    elif method == "nearest":
                        # Ties go left, like scipy's 'nearest'.
                        value = left if ts - start_ns <= end_ns - ts else right
                    elif interpolator is not None:
                        value = float(interpolator((ts - base) / NS_PER_SECOND))
                    else:  # linear
                        value = float(left) + (float(right) - float(left)) * (
                            ts - start_ns
                        ) / (end_ns - start_ns)
                    if int_fill:
                        value = int(round(value))
                        lo, hi_v = (
                            (0, UINT64_MAX)
                            if ftype == "uint"
                            else (INT64_MIN, INT64_MAX)
                        )
                        if not (lo <= value <= hi_v):
                            # Outside the column's integer range: skip the
                            # node, not the whole run.
                            out_of_range_fields.add(field)
                            continue
                    fill_points.setdefault(ts, {})[field] = value
                stats["points"] += n_writable
            elif too_long:
                stats["skipped"] += 1

            if cfg["report_measurement"] and (filled_now or too_long):
                # time = gap end, so re-reporting the same gap overwrites
                # in place.
                reports.append(
                    build_line(
                        LineBuilder,
                        cfg["report_measurement"],
                        tags={**tag_values, "gap_field": field},
                        fields={
                            "gap_start_ns": start_ns,
                            "gap_end_ns": end_ns,
                            "duration_s": (end_ns - start_ns) / NS_PER_SECOND,
                            "points": n_writable if filled_now else 0,
                            "status": "skipped" if too_long else "filled",
                        },
                        time_ns=end_ns,
                    )
                )

    # Attach carried (non-numeric) fields to fill points by last known value.
    if fill_points:
        node_ts: list[int] = sorted(fill_points)
        for field in fields:
            if field in filled_fields:
                continue
            times: list[int] = []
            values_l: list = []
            for row in series_rows:
                value = row.get(field)
                if value is None:
                    continue
                t = int(row["time"])
                if times and times[-1] == t:
                    values_l[-1] = value
                else:
                    times.append(t)
                    values_l.append(value)
            if not times:
                continue
            for ts in node_ts:
                j = bisect.bisect_right(times, ts) - 1
                if j >= 0:
                    fill_points[ts][field] = values_l[j]

    lines: list = []
    for ts in sorted(fill_points):
        values: dict = fill_points[ts]
        if cfg["mark_filled"]:
            values[cfg["filled_field_name"]] = True
        if field_types is None:
            lines.append(
                build_line(
                    LineBuilder,
                    cfg["output_measurement"],
                    tags=tag_values,
                    fields=values,
                    time_ns=ts,
                )
            )
        else:
            lines.append(
                build_line_typed(
                    LineBuilder,
                    cfg["output_measurement"],
                    tags=tag_values,
                    typed_fields={
                        f: (v, field_types.get(f) or _infer_type(v))
                        for f, v in values.items()
                    },
                    time_ns=ts,
                )
            )
    stats["out_of_range"] = sorted(out_of_range_fields)
    return lines, reports, stats


def copy_series_rows(
    series_rows: list[dict],
    tag_values: dict,
    fields: list[str],
    filled_fields: set[str],
    cfg: dict,
    node_start_ns: int,
    node_end_ns: int,
) -> list:
    """Copy existing points into the target measurement (chain mode).

    Blending methods emit float fills, so copied values of method-filled
    fields are cast to float to keep the target field type consistent.
    """
    cast: bool = cfg["method"] in BLENDING_METHODS
    copied: dict[int, dict] = {}
    for row in series_rows:
        ts = int(row["time"])
        if not (node_start_ns <= ts < node_end_ns):
            continue
        values: dict = {}
        for field in fields:
            value = row.get(field)
            if value is None:
                continue
            if (
                cast
                and field in filled_fields
                and isinstance(value, (int, float))
                and not isinstance(value, bool)
            ):
                value = float(value)
            values[field] = value
        if values:
            copied.setdefault(ts, {}).update(values)

    return [
        build_line(
            LineBuilder,
            cfg["output_measurement"],
            tags=tag_values,
            fields=copied[ts],
            time_ns=ts,
        )
        for ts in sorted(copied)
    ]


GAPFILL_VALIDATORS: list = [
    Validator("source_measurement", must_exist=True),
    Validator("interval", default="1s", cast=parse_timedelta),
    Validator(
        "method",
        default="linear",
        is_in=VALID_METHODS,
        cast=lambda v: str(v).lower(),
    ),
    Validator("max_retries", default=5, gte=1, cast=int),
]


def run_gapfill(
    influxdb3_local,
    cfg: dict,
    node_start: datetime,
    node_end: datetime,
    task_id: str,
    schema: dict | None = None,
) -> dict:
    """Detect and fill gaps with fill nodes inside [node_start, node_end).

    The query is padded by `lookback` on both sides so gap boundary points
    stay visible; gaps longer than the lookback are skipped.
    """
    # At least the node-range span, so a small max_fill_gap does not shrink
    # visibility (a gap needs both boundary points in one query window).
    lookback: timedelta = max(
        node_end - node_start, cfg["max_fill_gap"] or timedelta(0)
    )

    if schema is None:
        schema = resolve_schema(
            influxdb3_local,
            cfg["source_measurement"],
            cfg["filled_field_name"] if cfg["mark_filled"] else None,
            task_id,
        )
    tags: list[str] = schema["tags"]
    fields, filled_fields = select_fields(
        influxdb3_local,
        cfg["fields"],
        cfg["excluded_fields"],
        schema["all_fields"],
        schema["numeric_fields"],
        task_id,
    )

    # In-place writes land in existing columns, so fills carry each column's
    # stored type. Chain mode owns the target and infers types from values.
    field_types: dict[str, str] | None = None
    if cfg["in_place"]:
        field_types = dict(schema["line_types"])
        if cfg["mark_filled"]:
            field_types[cfg["filled_field_name"]] = "bool"

    rows: list[dict] = query_window(
        influxdb3_local,
        cfg["source_measurement"],
        start=(node_start - lookback).strftime(TIME_FORMAT),
        end=(node_end + lookback).strftime(TIME_FORMAT),
        columns=["time"] + tags + fields,
    )
    stats: dict = {
        "rows": len(rows),
        "series": 0,
        "gaps_filled": 0,
        "gaps_skipped": 0,
        "fills_written": 0,
        "rows_copied": 0,
    }
    if not rows:
        return stats

    node_start_ns: int = datetime_to_ns(node_start)
    node_end_ns: int = datetime_to_ns(node_end)
    series_map: dict[tuple, list[dict]] = group_by_series(rows, tags)
    stats["series"] = len(series_map)

    builders: list = []
    report_lines: list = []
    sparse_count: int = 0
    sparse_examples: list[str] = []
    oor_count: int = 0
    oor_examples: list[str] = []
    for tag_key, series_rows in series_map.items():
        tag_values: dict = dict(zip(tags, tag_key))
        fill_lines, reports, series_stats = fill_series(
            influxdb3_local,
            series_rows,
            tag_values,
            fields,
            filled_fields,
            cfg,
            node_end_ns,
            node_start_ns,
            field_types,
            MAX_FILL_POINTS - stats["fills_written"],
            task_id,
        )
        stats["gaps_filled"] += series_stats["filled"]
        stats["gaps_skipped"] += series_stats["skipped"]
        stats["fills_written"] += series_stats["points"]
        for sparse_field in series_stats["sparse"]:
            sparse_count += 1
            if len(sparse_examples) < 3:
                label = ",".join(f"{k}={v}" for k, v in tag_values.items())
                sparse_examples.append(f"{{{label}}}/{sparse_field}")
        for oor_field in series_stats["out_of_range"]:
            oor_count += 1
            if len(oor_examples) < 3:
                label = ",".join(f"{k}={v}" for k, v in tag_values.items())
                oor_examples.append(f"{{{label}}}/{oor_field}")
        builders.extend(fill_lines)
        report_lines.extend(reports)
        if not cfg["in_place"]:
            copies: list = copy_series_rows(
                series_rows,
                tag_values,
                fields,
                filled_fields,
                cfg,
                node_start_ns,
                node_end_ns,
            )
            stats["rows_copied"] += len(copies)
            builders.extend(copies)

    if sparse_count:
        influxdb3_local.info(
            f"[{task_id}] Skipped {sparse_count} series-fields with fewer "
            f"than 2 points in the window (e.g. {', '.join(sparse_examples)})."
        )
    if oor_count:
        influxdb3_local.warn(
            f"[{task_id}] Skipped fills for {oor_count} series-fields whose "
            f"values fell outside the integer column range (e.g. "
            f"{', '.join(oor_examples)}); check fill_value or method."
        )

    if builders:
        write_data(
            influxdb3_local,
            builders,
            retries=cfg["max_retries"] - 1,
            no_sync=True,
            database=cfg["target_database"],
        )
    if report_lines:
        write_data(
            influxdb3_local,
            report_lines,
            retries=cfg["max_retries"] - 1,
            no_sync=True,
            database=cfg["target_database"],
        )
    return stats


def process_scheduled_call(
    influxdb3_local, call_time: datetime, args: dict | None = None
):
    """Fill gaps in a sliding window ending at call_time - offset."""
    task_id: str = str(uuid.uuid4())
    try:
        cfg = load_plugin_config(
            args,
            validators=GAPFILL_VALIDATORS
            + [
                Validator("window", default="10min", cast=parse_timedelta),
                Validator("offset", default="0s", cast=parse_timedelta),
            ],
            source="toml" if (args or {}).get("config_file_path") else "args",
        )
        parsed: dict = normalize_config(cfg, task_id)
        window: timedelta = cfg.window
        if window < parsed["interval"]:
            raise Exception(f"[{task_id}] window must be at least one interval.")

        # The engine passes call_time as naive UTC.
        window_end: datetime = call_time.replace(tzinfo=timezone.utc) - cfg.offset
        # The range extends one lookback back so recently closed gaps stay
        # visible; at least one window wide.
        lookback: timedelta = max(window, parsed["max_fill_gap"] or timedelta(0))
        stats: dict = run_gapfill(
            influxdb3_local,
            parsed,
            window_end - window - lookback,
            window_end,
            task_id,
        )
        influxdb3_local.info(
            f"[{task_id}] Scanned {stats['rows']} rows across "
            f"{stats['series']} series: filled {stats['gaps_filled']} gaps "
            f"({stats['fills_written']} points), skipped "
            f"{stats['gaps_skipped']} too-long gaps, copied "
            f"{stats['rows_copied']} rows to '{parsed['output_measurement']}'."
        )
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Error: {str(e)}")


def parse_iso_utc(name: str, raw, task_id: str) -> datetime:
    """Parse an ISO 8601 datetime into aware UTC; naive input is UTC."""
    try:
        ns: int = parse_timestamp_ns(raw, "datetime")
    except (ValueError, TypeError):
        raise Exception(
            f"[{task_id}] {name} must be an ISO 8601 datetime, got '{raw}'."
        )
    return EPOCH + timedelta(microseconds=round(ns / 1000))


def process_request(
    influxdb3_local, query_parameters, request_headers, request_body, args=None
):
    """One-off historical gap repair over [backfill_start, backfill_end)."""
    task_id: str = str(uuid.uuid4())

    if not request_body:
        influxdb3_local.error(f"[{task_id}] No request body provided.")
        return {"message": f"[{task_id}] Error: no request body provided."}
    max_body_bytes: int = 10 * 1024 * 1024
    body_size: int = (
        len(request_body)
        if isinstance(request_body, (bytes, bytearray))
        else len(request_body.encode("utf-8"))
    )
    if body_size > max_body_bytes:
        influxdb3_local.error(f"[{task_id}] Request body too large.")
        return {"message": f"[{task_id}] Error: request body exceeds 10 MiB."}
    try:
        data: dict = json.loads(request_body)
    except Exception:
        influxdb3_local.error(f"[{task_id}] Request body is not valid JSON.")
        return {"message": f"[{task_id}] Error: request body is not valid JSON."}
    if not isinstance(data, dict):
        influxdb3_local.error(f"[{task_id}] Request body must be a JSON object.")
        return {"message": f"[{task_id}] Error: request body must be a JSON object."}

    try:
        # An explicit JSON null means "unset": drop those keys so the
        # validator default applies instead of casting None.
        data = {k: v for k, v in data.items() if v is not None}
        cfg = load_plugin_config(
            data,
            validators=GAPFILL_VALIDATORS
            + [
                Validator("batch_size", default="30d", cast=parse_timedelta),
            ],
            source="toml" if data.get("config_file_path") else "args",
        )
        parsed: dict = normalize_config(cfg, task_id)

        backfill_end: datetime = (
            parse_iso_utc("backfill_end", cfg.get("backfill_end"), task_id)
            if cfg.get("backfill_end")
            else datetime.now(timezone.utc)
        )
        if cfg.get("backfill_start"):
            backfill_start: datetime = parse_iso_utc(
                "backfill_start", cfg.get("backfill_start"), task_id
            )
        else:
            escaped: str = parsed["source_measurement"].replace('"', '""')
            res: list = influxdb3_local.query(
                f'SELECT MIN(time) AS _t FROM "{escaped}"'
            )
            oldest = res[0].get("_t") if res else None
            if oldest is None:
                raise Exception(
                    f"[{task_id}] source_measurement '{parsed['source_measurement']}' "
                    f"has no data."
                )
            backfill_start = datetime.fromtimestamp(
                int(oldest) / NS_PER_SECOND, tz=timezone.utc
            )
        if backfill_start >= backfill_end:
            raise Exception(
                f"[{task_id}] backfill_start must be earlier than backfill_end."
            )

        batch_delta: timedelta = cfg.batch_size
        if batch_delta < parsed["interval"]:
            raise Exception(f"[{task_id}] batch_size must be at least one interval.")
        if (backfill_end - backfill_start) / batch_delta > MAX_BATCHES:
            raise Exception(
                f"[{task_id}] backfill range / batch_size exceeds "
                f"{MAX_BATCHES} batches; increase batch_size."
            )

        totals: dict = {
            "batches": 0,
            "rows_scanned": 0,
            "gaps_filled": 0,
            "gaps_skipped": 0,
            "fills_written": 0,
            "rows_copied": 0,
        }
        # The schema is fixed for a historical range: resolve it once.
        schema: dict = resolve_schema(
            influxdb3_local,
            parsed["source_measurement"],
            parsed["filled_field_name"] if parsed["mark_filled"] else None,
            task_id,
        )
        cursor: datetime = backfill_start
        while cursor < backfill_end:
            batch_end: datetime = min(cursor + batch_delta, backfill_end)
            stats: dict = run_gapfill(
                influxdb3_local,
                parsed,
                cursor,
                batch_end,
                task_id,
                schema=schema,
            )
            totals["batches"] += 1
            totals["rows_scanned"] += stats["rows"]
            totals["gaps_filled"] += stats["gaps_filled"]
            totals["gaps_skipped"] += stats["gaps_skipped"]
            totals["fills_written"] += stats["fills_written"]
            totals["rows_copied"] += stats["rows_copied"]
            cursor = batch_end

        influxdb3_local.info(
            f"[{task_id}] Backfill done: "
            + " ".join(f"{k}={v}" for k, v in totals.items())
        )
        return {"status": "ok", "task_id": task_id, **totals}
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Error: {str(e)}")
        return {"message": f"[{task_id}] Error: {str(e)}"}
