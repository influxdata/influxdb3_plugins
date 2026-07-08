"""
{
    "plugin_type": ["scheduled"],
    "scheduled_args_config": [
        {
            "name": "source_table",
            "example": "cpu",
            "description": "Source table (measurement) to read and downsample.",
            "required": true
        },
        {
            "name": "target_table",
            "example": "cpu_downsampled",
            "description": "Destination table for aggregated points. Default: '<source_table>_downsampled'.",
            "required": false
        },
        {
            "name": "window",
            "example": "10min",
            "description": "Look-back window ending at call time. Units: 's', 'min', 'h', 'd', 'w'. Default: '10min'.",
            "required": false
        },
        {
            "name": "aggregations",
            "example": "mean min max",
            "description": "Space-separated aggregations to compute. Supported: mean, min, max, sum, count. Default: 'mean min max'.",
            "required": false
        },
        {
            "name": "excluded_fields",
            "example": "usage_guest usage_steal",
            "description": "Space-separated numeric fields to skip. Default: none.",
            "required": false
        },
        {
            "name": "group_by_tags",
            "example": "true",
            "description": "Group rows by their tag combination before aggregating. Default: true.",
            "required": false
        },
        {
            "name": "max_groups",
            "example": "1000",
            "description": "Safety cap on the number of tag groups processed. Default: 1000.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "simple_downsampler.toml",
            "description": "Optional TOML config file (relative to the plugin dir). Overrides args and env vars.",
            "required": false
        }
    ]
}

Demo plugin exercising the shared `influxdata_plugin_utils` helpers end to end:
config loading/validation, schema introspection (cached), windowed query,
and resilient batched writes.
"""

from collections import defaultdict
from datetime import timedelta, timezone

from influxdata_plugin_utils.config import Validator, load_plugin_config
from influxdata_plugin_utils.introspection import (
    get_field_names,
    get_table_names,
    get_tag_names,
    query_window,
)
from influxdata_plugin_utils.parsing import (
    parse_bool,
    parse_delimited_list,
    parse_timedelta,
)
from influxdata_plugin_utils.write import build_line, write_data

_AGGREGATORS = {
    "mean": lambda values: sum(values) / len(values),
    "min": min,
    "max": max,
    "sum": sum,
    "count": len,
}


def _validators() -> list[Validator]:
    """Config schema: defaults, bounds, and parsing casts from the package."""
    return [
        Validator("source_table", must_exist=True),
        Validator("target_table", default=None),
        Validator("window", default="10min", cast=parse_timedelta),
        Validator("aggregations", default="mean min max", cast=parse_delimited_list),
        Validator("excluded_fields", default="", cast=parse_delimited_list),
        Validator("group_by_tags", default=True, cast=parse_bool),
        Validator("max_groups", default=1000, gte=1, lte=100000, cast=int),
    ]


def process_scheduled_call(influxdb3_local, call_time, args):
    cfg = load_plugin_config(args, validators=_validators())
    source = cfg.source_table
    target = cfg.target_table or f"{source}_downsampled"

    aggregations = [agg for agg in cfg.aggregations if agg in _AGGREGATORS]
    if not aggregations:
        influxdb3_local.error(f"No valid aggregations in {cfg.aggregations}")
        return

    if source not in get_table_names(influxdb3_local):
        influxdb3_local.error(f"Source table {source!r} not found")
        return

    tag_names = get_tag_names(influxdb3_local, source) if cfg.group_by_tags else []
    field_names = [
        name
        for name in get_field_names(influxdb3_local, source, numeric_only=True)
        if name not in cfg.excluded_fields
    ]
    if not field_names:
        influxdb3_local.warn(f"No numeric fields to downsample in {source!r}")
        return

    window: timedelta = cfg.window
    # naive call_time is UTC; make it explicit so .timestamp() below is correct
    end = call_time if call_time.tzinfo else call_time.replace(tzinfo=timezone.utc)
    start = end - window
    rows = query_window(
        influxdb3_local,
        source,
        start=start.isoformat(),
        end=end.isoformat(),
        columns=tag_names + field_names,
    )
    if not rows:
        influxdb3_local.info(f"No rows in last {window} for {source!r}")
        return

    groups = _group_rows(rows, tag_names)
    if len(groups) > cfg.max_groups:
        influxdb3_local.warn(
            f"{len(groups)} groups exceed max_groups={cfg.max_groups}; truncating"
        )
        groups = dict(list(groups.items())[: cfg.max_groups])

    time_ns = int(end.timestamp() * 1_000_000_000)
    lines = []
    for group_key, group_rows in groups.items():
        fields = _aggregate_group(group_rows, field_names, aggregations)
        if not fields:  # all selected fields NULL for this group
            continue
        lines.append(
            build_line(
                LineBuilder,  # runtime global injected by the engine  # noqa: F821
                target,
                tags=dict(zip(tag_names, group_key)),
                fields=fields,
                time_ns=time_ns,
            )
        )

    write_data(influxdb3_local, lines)
    influxdb3_local.info(
        f"Downsampled {len(rows)} rows into {len(lines)} points -> {target!r}"
    )


def _group_rows(rows: list[dict], tag_names: list[str]) -> dict[tuple, list[dict]]:
    """Bucket rows by their tag-value tuple (single bucket when no tags)."""
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        key = tuple(str(row.get(tag, "")) for tag in tag_names)
        groups[key].append(row)
    return groups


def _aggregate_group(
    rows: list[dict], field_names: list[str], aggregations: list[str]
) -> dict:
    """Compute each aggregation over each field, skipping non-numeric values."""
    fields: dict = {}
    for field in field_names:
        values = [row[field] for row in rows if isinstance(row.get(field), (int, float))]
        if not values:
            continue
        for agg in aggregations:
            fields[f"{field}_{agg}"] = float(_AGGREGATORS[agg](values))
    return fields