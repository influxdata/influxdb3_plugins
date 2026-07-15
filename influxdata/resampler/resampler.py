"""
{
    "plugin_type": ["scheduled"],
    "scheduled_args_config": [
        {
            "name": "measurement",
            "example": "signal",
            "description": "Name of the source measurement with non-uniform timestamps.",
            "required": true
        },
        {
            "name": "target_measurement",
            "example": "signal_resampled",
            "description": "Name of the target measurement for resampled data. Must differ from the source measurement.",
            "required": true
        },
        {
            "name": "interval",
            "example": "1s",
            "description": "Uniform grid step (e.g., '100ms', '1s', '1min'), at least 1ms. Units: 'us', 'ms', 's', 'min', 'h', 'd', 'w'. Defaults to '1s'.",
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
            "name": "mode",
            "example": "snap",
            "description": "Processing mode. 'interpolate' (default) recalculates numeric fields at grid nodes and carries other fields by last known value; 'snap' only rounds each point's timestamp to the nearest grid node, keeping all values and types unchanged (interpolation_method and max_gap are ignored).",
            "required": false
        },
        {
            "name": "fields",
            "example": "temperature pressure",
            "description": "Space-separated numeric field names to interpolate; numeric fields not listed are dropped. Defaults to all numeric fields. In snap mode: fields of any type to carry (defaults to all).",
            "required": false
        },
        {
            "name": "excluded_fields",
            "example": "status note",
            "description": "Space-separated field names of any type to exclude from the output. Listing a field in both 'fields' and 'excluded_fields' is a configuration error.",
            "required": false
        },
        {
            "name": "interpolation_method",
            "example": "linear",
            "description": "Interpolation method: 'linear', 'nearest', 'cubic', 'previous', or 'next'. Defaults to 'linear'.",
            "required": false
        },
        {
            "name": "max_gap",
            "example": "2s",
            "description": "Source-point spacing above which grid points in between are not written, at least one interval. Units: 'us', 'ms', 's', 'min', 'h', 'd', 'w'. Defaults to 2x interval.",
            "required": false
        },
        {
            "name": "target_database",
            "example": "mydb",
            "description": "Target database for writing resampled data. If not provided, uses the trigger's database.",
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

import math
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np
from influxdata_plugin_utils.config import Validator, load_plugin_config
from influxdata_plugin_utils.introspection import (get_field_names,
                                                   get_tag_names, query_window)
from influxdata_plugin_utils.parsing import (parse_delimited_list,
                                             parse_timedelta)
from influxdata_plugin_utils.write import build_line, write_data
from scipy.interpolate import interp1d

# LineBuilder is injected by the InfluxDB 3 Processing Engine runtime.
# The stub below allows tests to monkeypatch it.
try:
    LineBuilder = LineBuilder  # noqa: F821 — already in scope when injected
except NameError:
    LineBuilder = None  # type: ignore[assignment,misc]

VALID_INTERPOLATION_METHODS: tuple = ("linear", "nearest", "cubic", "previous", "next")

NS_PER_SECOND: int = 1_000_000_000

# Upper bound on grid nodes per run: sub-second intervals combined with long
# windows would otherwise exhaust memory.
MAX_GRID_POINTS: int = 1_000_000

TIME_FORMAT: str = "%Y-%m-%dT%H:%M:%S.%fZ"

EPOCH: datetime = datetime(1970, 1, 1, tzinfo=timezone.utc)


def datetime_to_ns(dt: datetime) -> int:
    """Exact integer nanoseconds; float timestamp() drifts by ~100ns."""
    return ((dt - EPOCH) // timedelta(microseconds=1)) * 1000


def select_fields(
    influxdb3_local,
    mode: str,
    requested_raw,
    excluded_raw,
    all_fields: list[str],
    numeric_fields: list[str],
    task_id: str,
) -> tuple[list[str], set[str]]:
    """Resolve output fields for the run; returns (fields, interpolated set).

    interpolate: `fields` selects numeric fields to interpolate (default all
    numeric); non-numeric fields are always carried unless excluded.
    snap: `fields` selects fields of any type to carry (default all).
    `excluded_fields` removes fields of any type; overlap with `fields` is a
    configuration error.
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

    if mode == "snap":
        if requested is None:
            selected: list[str] = [f for f in all_fields if f not in excluded]
        else:
            selected = []
            for field in requested:
                if field in all_fields:
                    selected.append(field)
                else:
                    influxdb3_local.info(
                        f"[{task_id}] Field '{field}' is not available, skipping."
                    )
        if not selected:
            raise Exception(f"[{task_id}] No fields left to process.")
        return selected, set()

    if requested is None:
        interpolated: list[str] = [f for f in numeric_fields if f not in excluded]
    else:
        interpolated = []
        for field in requested:
            if field in numeric_set:
                interpolated.append(field)
            else:
                influxdb3_local.info(
                    f"[{task_id}] Field '{field}' is not a numeric field, "
                    f"skipping interpolation."
                )
        if not interpolated:
            raise Exception(f"[{task_id}] None of the requested fields are numeric.")

    carried: list[str] = [
        f for f in all_fields if f not in numeric_set and f not in excluded
    ]
    fields: list[str] = interpolated + carried
    if not fields:
        raise Exception(f"[{task_id}] No fields left to process.")
    return fields, set(interpolated)


def compute_grid(start_ns: int, end_ns: int, interval_ns: int) -> np.ndarray:
    """Return grid timestamps: multiples of interval_ns within [start_ns, end_ns).

    Anchoring to multiples of the interval (Unix epoch) makes output timestamps
    deterministic, so overlapping runs overwrite the same points idempotently.
    """
    first: int = -(-start_ns // interval_ns) * interval_ns
    return np.arange(first, end_ns, interval_ns, dtype=np.int64)


def group_by_series(rows: list[dict], tags: list[str]) -> dict[tuple, list[dict]]:
    """Group query rows by unique tag-value combinations."""
    series: dict[tuple, list[dict]] = {}
    for row in rows:
        key: tuple = tuple(row.get(tag) for tag in tags)
        series.setdefault(key, []).append(row)
    return series


def resample_field(
    times_ns: list[int],
    values: list,
    grid: np.ndarray,
    method: str,
    max_gap_ns: int,
) -> list[tuple[int, object]]:
    """Interpolate one field onto grid points.

    times_ns must be sorted and unique. A grid point is produced only when an
    exact source point exists at it, or when both bracketing source points
    exist and are at most max_gap_ns apart.

    Selection methods (previous/next/nearest) return source values unchanged,
    preserving integer types and precision above 2**53; blending methods
    (linear/cubic) produce float64 for every grid point, so the output field
    type stays consistent.
    """
    times = np.asarray(times_ns, dtype=np.int64)
    selection: bool = method in ("previous", "next", "nearest")

    if not selection:
        vals = np.asarray([float(v) for v in values], dtype=np.float64)
        # Offset by the first timestamp before converting to float seconds to
        # avoid losing precision on epoch-scale nanosecond values.
        base: int = int(times[0])
        x = (times - base) / NS_PER_SECOND
        xg = (grid - base) / NS_PER_SECOND
        interpolate = interp1d(
            x,
            vals,
            kind=method,
            bounds_error=False,
            fill_value=np.nan,
            assume_sorted=True,
        )
        yg = interpolate(xg)

    idx = np.searchsorted(times, grid, side="left")
    result: list[tuple[int, object]] = []
    for i, grid_point in enumerate(grid):
        j = int(idx[i])
        exact: bool = j < len(times) and times[j] == grid_point
        if not exact:
            if j == 0 or j >= len(times):
                continue  # no source neighbor on one side
            if int(times[j]) - int(times[j - 1]) > max_gap_ns:
                continue  # gap too large, left for the fill plugin
        if selection:
            if exact or method == "next":
                k = j
            elif method == "previous":
                k = j - 1
            else:  # nearest; ties go left like scipy's 'nearest'
                left = int(grid_point) - int(times[j - 1])
                right = int(times[j]) - int(grid_point)
                k = j - 1 if left <= right else j
            result.append((int(grid_point), values[k]))
            continue
        if exact:
            result.append((int(grid_point), float(values[j])))
            continue
        y = float(yg[i])
        if math.isnan(y):
            continue
        result.append((int(grid_point), y))
    return result


def carry_field_previous(
    times_ns: list[int],
    values: list,
    grid: np.ndarray,
    max_gap_ns: int,
) -> list[tuple[int, object]]:
    """Carry a field onto grid points by last known value (LOCF).

    Same gap rules as interpolation: a grid point gets a value only on an
    exact source match, or when both bracketing source points exist and are
    at most max_gap_ns apart — so real outages stay visible for any type.
    """
    times = np.asarray(times_ns, dtype=np.int64)
    idx = np.searchsorted(times, grid, side="left")
    result: list[tuple[int, object]] = []
    for i, grid_point in enumerate(grid):
        j = int(idx[i])
        if j < len(times) and times[j] == grid_point:
            result.append((int(grid_point), values[j]))
            continue
        if j == 0 or j >= len(times):
            continue
        if int(times[j]) - int(times[j - 1]) > max_gap_ns:
            continue
        result.append((int(grid_point), values[j - 1]))
    return result


def snap_series(
    series_rows: list[dict],
    tag_values: dict[str, str | None],
    fields: list[str],
    interval_ns: int,
    target_measurement: str,
) -> list:
    """Round each source point's timestamp to the nearest grid node.

    Values and types are unchanged. Rows are time-ordered, so on node
    collisions the latest point wins per field.
    """
    snapped: dict[int, dict] = {}
    for row in series_rows:
        values = {f: row[f] for f in fields if row.get(f) is not None}
        if not values:
            continue
        node = ((int(row["time"]) + interval_ns // 2) // interval_ns) * interval_ns
        snapped.setdefault(node, {}).update(values)

    return [
        build_line(
            LineBuilder,
            target_measurement,
            tags=tag_values,
            fields=snapped[node],
            time_ns=node,
        )
        for node in sorted(snapped)
    ]


def resample_series(
    influxdb3_local,
    series_rows: list[dict],
    tag_values: dict[str, str | None],
    fields: list[str],
    interpolated: set[str],
    grid_ns: np.ndarray,
    method: str,
    max_gap_ns: int,
    target_measurement: str,
    task_id: str,
) -> list:
    """Resample all fields of one series and build LineBuilder objects.

    Fields in `interpolated` are recalculated with the configured method;
    the rest are carried by last known value.
    """
    resampled: dict[int, dict] = {}

    for field in fields:
        # Deduplicate timestamps keeping the last value (rows are time-ordered).
        points: dict[int, object] = {}
        for row in series_rows:
            value = row.get(field)
            if value is None:
                continue
            points[int(row["time"])] = value

        if len(points) < 2:
            influxdb3_local.warn(
                f"[{task_id}] Series {tag_values or '{}'} field '{field}': "
                f"fewer than 2 points, skipping."
            )
            continue

        times_ns: list[int] = sorted(points)
        values: list = [points[t] for t in times_ns]

        if field in interpolated:
            field_method: str = method
            if method == "cubic" and len(points) < 4:
                influxdb3_local.warn(
                    f"[{task_id}] Series {tag_values or '{}'} field '{field}': "
                    f"cubic needs >= 4 points, falling back to linear."
                )
                field_method = "linear"
            pairs = resample_field(times_ns, values, grid_ns, field_method, max_gap_ns)
        else:
            pairs = carry_field_previous(times_ns, values, grid_ns, max_gap_ns)

        for t, v in pairs:
            resampled.setdefault(t, {})[field] = v

    return [
        build_line(
            LineBuilder,
            target_measurement,
            tags=tag_values,
            fields=resampled[t],
            time_ns=t,
        )
        for t in sorted(resampled)
    ]


def process_scheduled_call(
    influxdb3_local, call_time: datetime, args: dict | None = None
):
    """Resample a source measurement onto a uniform time grid."""
    task_id: str = str(uuid.uuid4())

    try:
        cfg = load_plugin_config(
            args,
            validators=[
                Validator("measurement", must_exist=True),
                Validator("target_measurement", must_exist=True),
                Validator("interval", default="1s", cast=parse_timedelta),
                Validator("window", default="10min", cast=parse_timedelta),
                Validator("offset", default="0s", cast=parse_timedelta),
                Validator(
                    "interpolation_method",
                    default="linear",
                    is_in=VALID_INTERPOLATION_METHODS,
                    cast=lambda v: str(v).lower(),
                ),
                Validator("max_retries", default=5, gte=1, cast=int),
                Validator(
                    "mode",
                    default="interpolate",
                    is_in=("interpolate", "snap"),
                    cast=lambda v: str(v).lower(),
                ),
            ],
            source="toml" if (args or {}).get("config_file_path") else "args",
        )

        measurement: str = cfg.measurement
        target_measurement: str = cfg.target_measurement
        target_database: str | None = cfg.get("target_database")
        if measurement == target_measurement:
            raise Exception(
                f"[{task_id}] target_measurement must differ from measurement."
            )

        interval: timedelta = cfg.interval
        window: timedelta = cfg.window
        offset: timedelta = cfg.offset
        method: str = cfg.interpolation_method
        max_retries: int = cfg.max_retries
        mode: str = cfg.mode

        if interval < timedelta(milliseconds=1):
            raise Exception(f"[{task_id}] interval must be at least 1ms.")
        if window < interval:
            raise Exception(f"[{task_id}] window must be at least one interval.")

        # Interpolation needs bracketing neighbors beyond the grid edges,
        # so the query is padded by max_gap; snap uses the rows as-is.
        pad: timedelta = timedelta(0)
        grid_pad: timedelta = timedelta(0)
        max_gap: timedelta = 2 * interval
        if mode == "interpolate":
            max_gap_raw = cfg.get("max_gap")
            if max_gap_raw:
                max_gap = parse_timedelta(max_gap_raw)
            if max_gap < interval:
                raise Exception(f"[{task_id}] max_gap must be at least one interval.")
            pad = max_gap
            # The grid also extends max_gap before the window: the previous
            # run skips its freshest points while their right source neighbor
            # is not yet ingested, so this run recomputes them (idempotent
            # overwrite makes the overlap safe even with non-overlapping windows).
            grid_pad = max_gap
            if (window + grid_pad) // interval > MAX_GRID_POINTS:
                raise Exception(
                    f"[{task_id}] (window + max_gap)/interval yields more than "
                    f"{MAX_GRID_POINTS} grid points; reduce window or max_gap, "
                    f"or increase interval."
                )

        # No schema caching: new fields/tags must be visible on the next run,
        # otherwise data written within the sliding window would be lost.
        tags: list[str] = get_tag_names(influxdb3_local, measurement, use_cache=False)
        all_fields: list[str] = get_field_names(
            influxdb3_local, measurement, numeric_only=False, use_cache=False
        )
        if not all_fields:
            raise Exception(
                f"[{task_id}] No fields found for measurement '{measurement}'."
            )
        numeric_fields: list[str] = []
        if mode == "interpolate":
            numeric_fields = get_field_names(
                influxdb3_local, measurement, numeric_only=True, use_cache=False
            )
        fields, interpolated = select_fields(
            influxdb3_local,
            mode,
            cfg.get("fields"),
            cfg.get("excluded_fields"),
            all_fields,
            numeric_fields,
            task_id,
        )

        # The engine passes call_time as naive UTC.
        window_end: datetime = call_time.replace(tzinfo=timezone.utc) - offset
        window_start: datetime = window_end - window

        query_start: datetime = window_start - grid_pad - pad
        rows: list[dict] = query_window(
            influxdb3_local,
            measurement,
            start=query_start.strftime(TIME_FORMAT),
            end=(window_end + pad).strftime(TIME_FORMAT),
            columns=["time"] + tags + fields,
        )
        if not rows:
            influxdb3_local.info(
                f"[{task_id}] No source data in [{query_start}, {window_end + pad})."
            )
            return

        # Integer arithmetic: total_seconds() would truncate sub-second parts.
        interval_ns: int = (interval // timedelta(microseconds=1)) * 1000
        series_map: dict[tuple, list[dict]] = group_by_series(rows, tags)
        builders: list = []

        if mode == "snap":
            for tag_key, series_rows in series_map.items():
                builders.extend(
                    snap_series(
                        series_rows,
                        dict(zip(tags, tag_key)),
                        fields,
                        interval_ns,
                        target_measurement,
                    )
                )
            summary = (
                f"Snapped {len(rows)} source rows onto {len(builders)} grid points"
            )
        else:
            max_gap_ns: int = (max_gap // timedelta(microseconds=1)) * 1000
            grid_ns: np.ndarray = compute_grid(
                datetime_to_ns(window_start - grid_pad),
                datetime_to_ns(window_end),
                interval_ns,
            )
            if grid_ns.size == 0:
                influxdb3_local.info(f"[{task_id}] No grid points in the window.")
                return
            for tag_key, series_rows in series_map.items():
                builders.extend(
                    resample_series(
                        influxdb3_local,
                        series_rows,
                        dict(zip(tags, tag_key)),
                        fields,
                        interpolated,
                        grid_ns,
                        method,
                        max_gap_ns,
                        target_measurement,
                        task_id,
                    )
                )
            skipped: int = len(grid_ns) * len(series_map) - len(builders)
            summary = (
                f"Resampled {len(rows)} source rows into {len(builders)} "
                f"grid points ({skipped} grid points skipped)"
            )

        if not builders:
            influxdb3_local.info(f"[{task_id}] Nothing to write.")
            return

        write_data(
            influxdb3_local,
            builders,
            retries=max_retries - 1,
            no_sync=True,
            database=target_database,
        )
        influxdb3_local.info(
            f"[{task_id}] {summary} across {len(series_map)} series, "
            f"wrote to '{target_measurement}'."
        )
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Error: {e}")
