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
            "description": "Emission period in seconds. Cache TTL = 2x this value. Defaults to 60.",
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
import os
import re
import time
import tomllib
import uuid
from dataclasses import dataclass

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


_DURATION_RE = re.compile(r"^\s*(\d+)\s*([smhd])\s*$")
_DURATION_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def _parse_duration(s):
    m = _DURATION_RE.match(s)
    if not m:
        raise ValueError(f"invalid duration: {s!r} (expected e.g. '60s', '5m', '1h', '1d')")
    return int(m.group(1)) * _DURATION_UNITS[m.group(2)]


@dataclass
class Config:
    fields: list
    output_suffix: str = "_valuecounts"
    dest_database: str = ""
    period_seconds: int = 60
    table: str = ""


_MODE_A_ALLOWED = {"fields", "output_suffix", "period_seconds", "period", "dest_database", "config_file_path"}
_MODE_B_ALLOWED = {"table", "fields", "output_suffix", "dest_database", "config_file_path"}


def _parse_inline_args(args, mode):
    if mode == "wal":
        allowed = _MODE_A_ALLOWED
    elif mode == "scheduled":
        allowed = _MODE_B_ALLOWED
    else:
        raise ValueError(f"unknown mode: {mode!r}")

    for key in args:
        if key not in allowed:
            # special-case better error messages for mode-incompatible knobs
            if key == "table" and mode == "wal":
                raise ValueError(
                    "vc-wal: 'table' is determined by the trigger-spec, not inline args"
                )
            if key in ("period_seconds", "period") and mode == "scheduled":
                raise ValueError(
                    "vc-scheduled: 'period_seconds'/'period' is not used; Mode B is drift-based"
                )
            raise ValueError(f"unknown arg: {key!r}")

    cfg = Config(fields=[])
    if "fields" in args:
        cfg.fields = args["fields"].split()
    if "output_suffix" in args:
        cfg.output_suffix = args["output_suffix"]
    if "dest_database" in args:
        cfg.dest_database = args["dest_database"]
    if "table" in args:
        cfg.table = args["table"]
    if "period" in args:
        cfg.period_seconds = _parse_duration(args["period"])
    if "period_seconds" in args:
        cfg.period_seconds = int(args["period_seconds"])
    return cfg


def _load_toml_config(path, mode):
    if mode == "wal":
        allowed = _MODE_A_ALLOWED - {"config_file_path"}
    elif mode == "scheduled":
        allowed = _MODE_B_ALLOWED - {"config_file_path"}
    else:
        raise ValueError(f"unknown mode: {mode!r}")

    try:
        with open(path, "rb") as f:
            data = tomllib.load(f)
    except FileNotFoundError:
        raise
    except tomllib.TOMLDecodeError as e:
        raise ValueError(f"TOML parse error in {path}: {e}")

    for key in data:
        if key not in allowed:
            if key == "table" and mode == "wal":
                raise ValueError(
                    "vc-wal: 'table' is determined by the trigger-spec, not the TOML"
                )
            if key in ("period_seconds", "period") and mode == "scheduled":
                raise ValueError(
                    "vc-scheduled: 'period'/'period_seconds' is not used; Mode B is drift-based"
                )
            raise ValueError(f"unknown TOML key: {key!r}")

    cfg = Config(fields=[])
    if "fields" in data:
        f = data["fields"]
        if isinstance(f, str):
            f = f.split()
        cfg.fields = list(f)
    if "output_suffix" in data:
        cfg.output_suffix = data["output_suffix"]
    if "dest_database" in data:
        cfg.dest_database = data["dest_database"]
    if "table" in data:
        cfg.table = data["table"]
    if "period" in data:
        cfg.period_seconds = _parse_duration(data["period"])
    if "period_seconds" in data:
        cfg.period_seconds = int(data["period_seconds"])
    return cfg


def _resolve_config(args, plugin_dir, mode):
    args = dict(args)  # defensive copy
    cfp = args.pop("config_file_path", None)

    if cfp is not None and args:
        raise ValueError("set either config_file_path or inline args, not both")

    if cfp is not None:
        path = os.path.join(plugin_dir, cfp)
        cfg = _load_toml_config(path, mode=mode)
    else:
        cfg = _parse_inline_args(args, mode=mode)

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
        raise ValueError("config error: 'output_suffix' cannot be empty (would risk feedback loop in Mode A and ambiguity in Mode B)")

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


def _build_line(table, tags, counts, output_suffix, ts_ns, line_builder_cls):
    if not counts:
        return None
    lb = line_builder_cls(f"{table}{output_suffix}")
    for k, v in tags.items():
        lb.tag(k, str(v))
    for k, v in counts.items():
        lb.int64_field(k, int(v))
    lb.time_ns(ts_ns)
    return lb


def _get_tag_names(influxdb3_local, table_name):
    key = f"vc:tags:{table_name}"
    cached = influxdb3_local.cache.get(key)
    if cached is not None:
        return cached
    res = influxdb3_local.query(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $tbl
          AND data_type = 'Dictionary(Int32, Utf8)'
        """,
        {"tbl": table_name},
    )
    tag_names = [r["column_name"] for r in res]
    influxdb3_local.cache.put(key, tag_names, ttl=3600)
    return tag_names


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


def _query_field_distribution(influxdb3_local, table, tag_names, field_name, start_ns, end_ns):
    sql = _build_scheduled_query(table, tag_names, field_name)
    params = {"start_ns": str(start_ns), "end_ns": str(end_ns)}
    return influxdb3_local.query(sql, params)


class _BatchLines:
    def __init__(self, line_builders):
        self._line_builders = list(line_builders)
        self._built = None

    def build(self):
        if self._built is None:
            lines = [b.build() for b in self._line_builders]
            if not lines:
                raise ValueError("batch_write received no lines to build")
            self._built = "\n".join(lines)
        return self._built


def process_scheduled_call(influxdb3_local, call_time, args=None):
    task_id = uuid.uuid4().hex[:8]

    # call_time arrives as a PyDateTime per system_py.rs:847,867
    call_time_ns = int(call_time.timestamp()) * 1_000_000_000 + call_time.microsecond * 1000

    cfg = _resolve_config(args or {}, os.environ.get("PLUGIN_DIR", "."), mode="scheduled")

    anchor_key = f"vc:scheduled:last_call_ns:{cfg.table}"
    last_call_ns = influxdb3_local.cache.get(anchor_key)

    if last_call_ns is None:
        influxdb3_local.cache.put(anchor_key, call_time_ns, ttl=None)
        influxdb3_local.info(
            f"[{task_id}] vc-scheduled: first fire — establishing cadence anchor; no rollup emitted"
        )
        return

    tag_names = _get_tag_names(influxdb3_local, cfg.table)
    for t in tag_names:
        _validate_identifier(t, "tag column")

    window_start_ns = last_call_ns
    window_end_ns = call_time_ns

    series = {}
    collisions = {}  # (field, sanitized_name) -> set of raw values

    for field_name in cfg.fields:
        try:
            rows = _query_field_distribution(
                influxdb3_local, cfg.table, tag_names, field_name,
                window_start_ns, window_end_ns,
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
        _build_line(ss["table"], ss["tags"], ss["counts"], cfg.output_suffix,
                    call_time_ns, LineBuilder)
        for ss in series.values()
    ]
    builders = [b for b in builders if b is not None]
    if not builders:
        influxdb3_local.cache.put(anchor_key, call_time_ns, ttl=None)
        return

    try:
        batch = _BatchLines(builders)
        if cfg.dest_database:
            influxdb3_local.write_sync_to_db(cfg.dest_database, batch, no_sync=True)
        else:
            influxdb3_local.write_sync(batch, no_sync=True)
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] vc-scheduled: write failed: {e}")
        return  # anchor unchanged → next fire's window covers two periods

    influxdb3_local.cache.put(anchor_key, call_time_ns, ttl=None)


def process_writes(influxdb3_local, table_batches, args=None):
    task_id = uuid.uuid4().hex[:8]
    now_ns = time.time_ns()

    cfg = _resolve_config(args or {}, os.environ.get("PLUGIN_DIR", "."), mode="wal")

    period_ns = cfg.period_seconds * 1_000_000_000
    ttl = 2 * cfg.period_seconds

    # The trigger spec already binds this trigger to one table, but `process_writes`
    # may receive batches from that table only. Group by table_name to be defensive.
    by_table = {}
    for batch in table_batches:
        name = batch["table_name"]
        by_table.setdefault(name, []).extend(batch["rows"])

    for table_name, rows in by_table.items():
        tag_names = _get_tag_names(influxdb3_local, table_name)
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
            to_emit.append((sh, state["table"], dict(state["tags"]), dict(state["counts"])))

        # Prune index — only live hashes survive
        if live != active_hashes:
            influxdb3_local.cache.put(index_key, live, ttl=ttl)
        elif active_hashes:
            # Refresh TTL on the index even when unchanged
            influxdb3_local.cache.put(index_key, live, ttl=ttl)

        if not to_emit:
            continue

        builders = [
            _build_line(t_, tags, counts, cfg.output_suffix, now_ns, LineBuilder)
            for _sh, t_, tags, counts in to_emit
        ]
        builders = [b for b in builders if b is not None]
        if not builders:
            continue

        try:
            batch = _BatchLines(builders)
            if cfg.dest_database:
                influxdb3_local.write_sync_to_db(cfg.dest_database, batch, no_sync=True)
            else:
                influxdb3_local.write_sync(batch, no_sync=True)
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
