"""
{
    "plugin_type": ["scheduled", "onwrite"],
    "onwrite_args_config": [
        {
            "name": "fields",
            "example": "status method",
            "description": "Space-separated list of field names whose unique values will be counted. Required unless config_file_path is set.",
            "required": false
        },
        {
            "name": "output_suffix",
            "example": "_valuecounts",
            "description": "Suffix appended to the source measurement name for the rollup output. Defaults to '_valuecounts'.",
            "required": false
        },
        {
            "name": "period_seconds",
            "example": "60",
            "description": "Emission period in seconds, at least 1. Cache TTL = 2x this value. Defaults to 60.",
            "required": false
        },
        {
            "name": "period",
            "example": "5min",
            "description": "Emission period as a duration (e.g., '30s', '5min', '1h'), at least 1s. Units: 's', 'min', 'h', 'd', 'w'. Overridden by period_seconds when both are set.",
            "required": false
        },
        {
            "name": "dest_database",
            "example": "rollups",
            "description": "Optional database to write rollups to. Defaults to the trigger's own database.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "valuecounter_config.toml",
            "description": "Path to a TOML config file (relative to PLUGIN_DIR). If set, no other inline args may be set on this trigger.",
            "required": false
        }
    ],
    "scheduled_args_config": [
        {
            "name": "table",
            "example": "http_requests",
            "description": "Source table to query. Required unless config_file_path supplies it.",
            "required": false
        },
        {
            "name": "fields",
            "example": "status method",
            "description": "Space-separated list of field names whose unique values will be counted. Required unless config_file_path is set.",
            "required": false
        },
        {
            "name": "output_suffix",
            "example": "_valuecounts",
            "description": "Suffix appended to the source measurement name for the rollup output. Defaults to '_valuecounts'.",
            "required": false
        },
        {
            "name": "dest_database",
            "example": "rollups",
            "description": "Optional database to write rollups to. Defaults to the trigger's own database.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "valuecounter_config.toml",
            "description": "Path to a TOML config file (relative to PLUGIN_DIR). If set, no other inline args may be set on this trigger.",
            "required": false
        }
    ]
}
"""

import hashlib
import re
import time
import uuid
from dataclasses import dataclass

from influxdata_plugin_utils.config import Validator, load_plugin_config
from influxdata_plugin_utils.introspection import get_tag_names
from influxdata_plugin_utils.parsing import (
    parse_delimited_list,
    parse_int,
    parse_timedelta,
)
from influxdata_plugin_utils.write import build_line, write_data

# At server runtime LineBuilder is injected as a builtin. In test environments
# pytest patches this module-level name to a vendored copy. The reference in
# entry-point bodies uses this name, so patching works without import.
try:
    LineBuilder  # type: ignore
except NameError:
    LineBuilder = None  # placeholder for test patching

_SANITIZE_RE = re.compile(r"[^A-Za-z0-9_]")


def _sanitize_field_name(name):
    return _SANITIZE_RE.sub("_", name)


def _stringify_value(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float, str)):
        return str(v)
    return repr(v)


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")


def _validate_identifier(name, what):
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"invalid {what}: {name!r}")


_BARE_MINUTES_RE = re.compile(r"^\s*(\d+)\s*m\s*$")


def _parse_period_seconds(raw):
    match = _BARE_MINUTES_RE.match(str(raw))
    text = f"{match.group(1)}min" if match else str(raw)
    seconds = int(parse_timedelta(text).total_seconds())
    if seconds < 1:
        raise ValueError(f"invalid period: {raw!r} (must be at least 1 second)")
    return seconds


@dataclass
class Config:
    fields: list
    output_suffix: str = "_valuecounts"
    dest_database: str = ""
    period_seconds: int = 60
    table: str = ""


_MODE_ALLOWED = {
    "wal": {"fields", "output_suffix", "period", "period_seconds", "dest_database"},
    "scheduled": {"table", "fields", "output_suffix", "dest_database"},
}


def _parse_fields(raw):
    """Field names split on any whitespace; TOML may deliver a list already."""
    if isinstance(raw, (list, tuple)):
        return parse_delimited_list(raw)
    return str(raw).split()


_COMMON_VALIDATORS = [
    Validator("fields", default="", cast=_parse_fields),
    Validator("output_suffix", default="_valuecounts", cast=str),
    Validator("dest_database", default="", cast=str),
]

# Only mode-valid keys get a validator, so registered defaults never trip the
# unknown-key check below.
_MODE_VALIDATORS = {
    "wal": _COMMON_VALIDATORS,
    "scheduled": _COMMON_VALIDATORS + [Validator("table", default="", cast=str)],
}


def _reject_unknown_keys(loaded, mode, from_toml):
    key_label = "TOML key" if from_toml else "arg"
    source_label = "the TOML" if from_toml else "inline args"
    allowed = _MODE_ALLOWED[mode]

    for key in (name.lower() for name in loaded.as_dict()):
        if key in allowed:
            continue
        # special-case better error messages for mode-incompatible knobs
        if key == "table" and mode == "wal":
            raise ValueError(
                f"vc-wal: 'table' is determined by the trigger-spec, not {source_label}"
            )
        if key in ("period", "period_seconds") and mode == "scheduled":
            raise ValueError(
                "vc-scheduled: 'period'/'period_seconds' is not used; Mode B is drift-based"
            )
        raise ValueError(f"unknown {key_label}: {key!r}")


def _resolve_config(args, mode):
    if mode not in _MODE_ALLOWED:
        raise ValueError(f"unknown mode: {mode!r}")

    args = dict(args or {})  # defensive copy
    config_file_path = args.get("config_file_path")
    if config_file_path is not None and len(args) > 1:
        raise ValueError("set either config_file_path or inline args, not both")

    loaded = load_plugin_config(
        args,
        validators=_MODE_VALIDATORS[mode],
        source="toml" if config_file_path else "args",
    )
    _reject_unknown_keys(loaded, mode, from_toml=bool(config_file_path))

    cfg = Config(
        fields=[str(f) for f in loaded.fields],
        output_suffix=loaded.output_suffix,
        dest_database=loaded.dest_database,
        table=str(loaded.get("table") or ""),
    )

    if mode == "wal":
        # 'period_seconds' wins when both spellings are present
        if (period := loaded.get("period")) is not None:
            cfg.period_seconds = _parse_period_seconds(period)
        if (period_seconds := loaded.get("period_seconds")) is not None:
            cfg.period_seconds = parse_int(period_seconds, minimum=1)

    if not cfg.fields:
        raise ValueError("config error: 'fields' is empty or missing")

    if mode == "scheduled" and not cfg.table:
        raise ValueError("config error: 'table' is required for Mode B (scheduled)")

    # identifier defense-in-depth
    if cfg.table:
        _validate_identifier(cfg.table, "table")
    for f in cfg.fields:
        _validate_identifier(f, "field")

    if cfg.output_suffix == "":
        raise ValueError(
            "config error: 'output_suffix' cannot be empty (would risk feedback loop in Mode A and ambiguity in Mode B)"
        )

    return cfg


def _series_key(table_name, tags):
    tag_str = "|".join(f"{k}={v}" for k, v in sorted(tags.items()))
    h = hashlib.sha1(tag_str.encode()).hexdigest()[:12]
    return f"{table_name}:{h}"


def _extract_tags(row, tag_names):
    out = {}
    for t in tag_names:
        if t not in row:
            continue
        v = row[t]
        if v is None:
            return None  # drop the row
        out[t] = v
    return out


def _build_rollup_line(table, tags, counts, output_suffix, ts_ns):
    return build_line(
        LineBuilder,
        f"{table}{output_suffix}",
        tags=tags,
        fields=counts,
        time_ns=ts_ns,
    )


def _build_scheduled_query(table, tag_names, field_name):
    tag_cols = ", ".join(f'"{t}"' for t in tag_names)
    field_col = f'"{field_name}"'
    table_ref = f'"{table}"'

    if tag_names:
        projection = f"{tag_cols}, {field_col}, COUNT(*) AS cnt"
        group_by = f"{tag_cols}, {field_col}"
    else:
        projection = f"{field_col}, COUNT(*) AS cnt"
        group_by = field_col

    return (
        f"SELECT {projection}\n"
        f"FROM {table_ref}\n"
        f"WHERE time >= to_timestamp_nanos(arrow_cast($start_ns, 'Int64')) "
        f"AND time < to_timestamp_nanos(arrow_cast($end_ns, 'Int64'))\n"
        f"GROUP BY {group_by}"
    )


def _query_field_distribution(
    influxdb3_local, table, tag_names, field_name, start_ns, end_ns
):
    sql = _build_scheduled_query(table, tag_names, field_name)
    params = {"start_ns": str(start_ns), "end_ns": str(end_ns)}
    return influxdb3_local.query(sql, params)


def process_scheduled_call(influxdb3_local, call_time, args=None):
    task_id = uuid.uuid4().hex[:8]

    # call_time arrives as a PyDateTime per system_py.rs:847,867
    call_time_ns = (
        int(call_time.timestamp()) * 1_000_000_000 + call_time.microsecond * 1000
    )

    cfg = _resolve_config(args, mode="scheduled")

    anchor_key = f"vc:scheduled:last_call_ns:{cfg.table}"
    last_call_ns = influxdb3_local.cache.get(anchor_key)

    if last_call_ns is None:
        influxdb3_local.cache.put(anchor_key, call_time_ns, ttl=None)
        influxdb3_local.info(
            f"[{task_id}] vc-scheduled: first fire — establishing cadence anchor; no rollup emitted"
        )
        return

    tag_names = get_tag_names(influxdb3_local, cfg.table)
    for t in tag_names:
        _validate_identifier(t, "tag column")

    window_start_ns = last_call_ns
    window_end_ns = call_time_ns

    series = {}
    collisions = {}  # (field, sanitized_name) -> set of raw values

    for field_name in cfg.fields:
        try:
            rows = _query_field_distribution(
                influxdb3_local,
                cfg.table,
                tag_names,
                field_name,
                window_start_ns,
                window_end_ns,
            )
        except Exception as e:
            influxdb3_local.error(
                f"[{task_id}] vc-scheduled: query failed for field '{field_name}': {e}"
            )
            return  # anchor unchanged → next fire retries the wider window

        for row in rows:
            tags = _extract_tags(row, tag_names)
            if tags is None:
                continue  # null tag → drop the row
            value_str = _stringify_value(row.get(field_name))
            if value_str is None:
                continue
            cnt = int(row.get("cnt", 0))
            if cnt == 0:
                continue
            raw_key = f"{field_name}_{value_str}"
            field_key = _sanitize_field_name(raw_key)
            sh = _series_key(cfg.table, tags)
            ss = series.setdefault(sh, {"table": cfg.table, "tags": tags, "counts": {}})
            prev = ss["counts"].get(field_key, 0)
            collisions.setdefault((field_name, field_key), set()).add(raw_key)
            ss["counts"][field_key] = prev + cnt

    for (fld, key), raws in collisions.items():
        if len(raws) <= 1:
            continue
        influxdb3_local.warn(
            f"[{task_id}] vc-scheduled: field-name collision on '{key}' "
            f"for watched field '{fld}'; counts summed for raw values {sorted(raws)}"
        )

    if not series:
        influxdb3_local.info(f"[{task_id}] vc-scheduled: no rows in window")
        influxdb3_local.cache.put(anchor_key, call_time_ns, ttl=None)
        return

    builders = [
        _build_rollup_line(
            ss["table"], ss["tags"], ss["counts"], cfg.output_suffix, call_time_ns
        )
        for ss in series.values()
    ]

    try:
        write_data(
            influxdb3_local,
            builders,
            retries=0,
            no_sync=True,
            database=cfg.dest_database or None,
        )
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] vc-scheduled: write failed: {e}")
        return  # anchor unchanged → next fire's window covers two periods

    influxdb3_local.cache.put(anchor_key, call_time_ns, ttl=None)


def process_writes(influxdb3_local, table_batches, args=None):
    task_id = uuid.uuid4().hex[:8]
    now_ns = time.time_ns()

    cfg = _resolve_config(args, mode="wal")

    period_ns = cfg.period_seconds * 1_000_000_000
    ttl = 2 * cfg.period_seconds

    # The trigger spec already binds this trigger to one table, but `process_writes`
    # may receive batches from that table only. Group by table_name to be defensive.
    by_table = {}
    for batch in table_batches:
        name = batch["table_name"]
        by_table.setdefault(name, []).extend(batch["rows"])

    for table_name, rows in by_table.items():
        tag_names = get_tag_names(influxdb3_local, table_name)
        for t in tag_names:
            _validate_identifier(t, "tag column")

        index_key = f"vc:wal:_index:{table_name}"
        active_hashes = list(influxdb3_local.cache.get(index_key, default=[]))
        collisions = {}  # (field, sanitized_name) -> set of raw values

        # Accumulate
        for row in rows:
            tags = _extract_tags(row, tag_names)
            if tags is None:
                continue  # null tag → drop row
            sh = _series_key(table_name, tags)
            state = influxdb3_local.cache.get(f"vc:wal:{sh}")
            if state is None:
                state = {
                    "table": table_name,
                    "tags": tags,
                    "counts": {},
                    "last_emit_ns": now_ns,  # first observation
                }
                if sh not in active_hashes:
                    active_hashes.append(sh)
            for fname in cfg.fields:
                if fname not in row:
                    continue
                value_str = _stringify_value(row[fname])
                if value_str is None:
                    continue
                raw_key = f"{fname}_{value_str}"
                field_key = _sanitize_field_name(raw_key)
                prev = state["counts"].get(field_key, 0)
                collisions.setdefault((fname, field_key), set()).add(raw_key)
                state["counts"][field_key] = prev + 1
            influxdb3_local.cache.put(f"vc:wal:{sh}", state, ttl=ttl)

        for (fld, key), raws in collisions.items():
            if len(raws) <= 1:
                continue
            influxdb3_local.warn(
                f"[{task_id}] vc-wal: field-name collision on '{key}' "
                f"for watched field '{fld}'; counts summed for raw values {sorted(raws)}"
            )

        # Iterate index for emission + prune
        to_emit = []
        live = []
        for sh in active_hashes:
            state = influxdb3_local.cache.get(f"vc:wal:{sh}")
            if state is None:
                continue  # expired; will be pruned from index
            live.append(sh)
            if not state["counts"]:
                continue
            if now_ns - state["last_emit_ns"] < period_ns:
                continue
            to_emit.append(
                (sh, state["table"], dict(state["tags"]), dict(state["counts"]))
            )

        # Prune index — only live hashes survive
        if live != active_hashes:
            influxdb3_local.cache.put(index_key, live, ttl=ttl)
        elif active_hashes:
            # Refresh TTL on the index even when unchanged
            influxdb3_local.cache.put(index_key, live, ttl=ttl)

        if not to_emit:
            continue

        builders = [
            _build_rollup_line(t_, tags, counts, cfg.output_suffix, now_ns)
            for _sh, t_, tags, counts in to_emit
        ]

        try:
            write_data(
                influxdb3_local,
                builders,
                retries=0,
                no_sync=True,
                database=cfg.dest_database or None,
            )
        except Exception as e:
            influxdb3_local.error(
                f"[{task_id}] vc-wal: emit failed, counts retained for retry: {e}"
            )
            # Refresh TTL on live entries so they don't silently expire
            for sh in live:
                state = influxdb3_local.cache.get(f"vc:wal:{sh}")
                if state is not None:
                    influxdb3_local.cache.put(f"vc:wal:{sh}", state, ttl=ttl)
            continue

        # Success: subtract snapshot counts and advance last_emit_ns
        for sh, _t, _tags, snap_counts in to_emit:
            current = influxdb3_local.cache.get(f"vc:wal:{sh}")
            if current is None:
                continue
            cur_counts = current["counts"]
            for k, v in snap_counts.items():
                remaining = cur_counts.get(k, 0) - v
                if remaining > 0:
                    cur_counts[k] = remaining
                else:
                    cur_counts.pop(k, None)
            current["last_emit_ns"] = now_ns
            influxdb3_local.cache.put(f"vc:wal:{sh}", current, ttl=ttl)
