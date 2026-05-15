"""
{
    "plugin_type": ["onwrite"],
    "onwrite_args_config": [
        {
            "name": "exclude_fields",
            "example": "status.version",
            "description": "Dot-separated list of field names to exclude from profiling.",
            "required": false
        },
        {
            "name": "exclude_tables",
            "example": "debug_info.system_logs",
            "description": "Dot-separated list of table names to skip when using all_tables trigger.",
            "required": false
        },
        {
            "name": "max_series",
            "example": "100",
            "description": "Maximum number of unique series to track. Uses LRU eviction when exceeded. Defaults to 100.",
            "required": false
        },
        {
            "name": "profile_write_interval",
            "example": "50",
            "description": "Write profile to _meta.series_profiles every N observations per series. Defaults to 50.",
            "required": false
        },
        {
            "name": "min_observations",
            "example": "50",
            "description": "Minimum observations before profile is considered mature. Defaults to 50.",
            "required": false
        },
        {
            "name": "log_profiles",
            "example": "false",
            "description": "If 'true', logs profile updates. Defaults to 'false'.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "auto_profiler_config.toml",
            "description": "Path to TOML config file to override trigger arguments.",
            "required": false
        }
    ]
}
"""

import math
import os
import tomllib
import traceback
import uuid
from collections import OrderedDict
from pathlib import Path


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_profiler_toml_config(influxdb3_local, args: dict, task_id: str) -> dict | None:
    """Loads trigger arguments from a TOML config file if config_file_path is set."""
    path = args.get("config_file_path", None)
    if not path:
        return args

    try:
        plugin_dir_var = os.getenv("PLUGIN_DIR", None)
        if not plugin_dir_var:
            influxdb3_local.error(f"[{task_id}] PLUGIN_DIR env var not set")
            return None
        file_path = Path(plugin_dir_var) / path
        influxdb3_local.info(f"[{task_id}] Reading config from {file_path}")
        with open(file_path, "rb") as f:
            return tomllib.load(f)
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to read config file: {e}")
        return None


def parse_profiler_args(args: dict | None) -> dict:
    """Parses trigger arguments with defaults."""
    if not args:
        args = {}
    return {
        "exclude_fields": set(args.get("exclude_fields", "").split(".")) - {""},
        "exclude_tables": set(args.get("exclude_tables", "").split(".")) - {""},
        "max_series": int(args.get("max_series", 100)),
        "profile_write_interval": int(args.get("profile_write_interval", 50)),
        "min_observations": int(args.get("min_observations", 50)),
        "log_profiles": str(args.get("log_profiles", "false")).lower() == "true",
    }


# ---------------------------------------------------------------------------
# Series key helpers
# ---------------------------------------------------------------------------

def build_profiler_series_key(table_name: str, row: dict, field_name: str, tag_names: list[str]) -> str:
    """
    Builds a unique series key from table name, sorted tag values, and field name.
    Format: table_name:tag1=val1,tag2=val2:field_name
    """
    tag_parts = []
    for tag in sorted(tag_names):
        val = row.get(tag, None)
        if val is not None:
            tag_parts.append(f"{tag}={val}")
    tag_str = ",".join(tag_parts)
    return f"{table_name}:{tag_str}:{field_name}"


# ---------------------------------------------------------------------------
# Series profiling
# ---------------------------------------------------------------------------

class SeriesProfile:
    """Tracks streaming statistics for a single series (one field of one tag combo)."""

    def __init__(self):
        from river import stats

        self.mean = stats.Mean()
        self.var = stats.Var()
        self.min = stats.Min()
        self.max = stats.Max()
        self.count = 0
        self.last_timestamp_ns = None
        self.write_interval_sum = 0.0
        self.write_interval_count = 0
        self.last_written_count = 0  # count at last profile write

    def update(self, value: float, timestamp_ns: int | None = None):
        """Update all stats with a new observation."""
        self.count += 1
        self.mean.update(value)
        self.var.update(value)
        self.min.update(value)
        self.max.update(value)

        # Track write frequency
        if timestamp_ns is not None and self.last_timestamp_ns is not None:
            interval_seconds = (timestamp_ns - self.last_timestamp_ns) / 1e9
            if interval_seconds > 0:
                self.write_interval_sum += interval_seconds
                self.write_interval_count += 1
        if timestamp_ns is not None:
            self.last_timestamp_ns = timestamp_ns

    def get_cv(self) -> float:
        """Coefficient of variation = std / |mean|. Returns 0 if mean is 0."""
        mean = self.mean.get()
        var = self.var.get()
        if mean == 0 or var is None or var < 0:
            return 0.0
        std = math.sqrt(var) if var > 0 else 0.0
        return std / abs(mean) if abs(mean) > 0 else 0.0

    def get_avg_write_interval(self) -> float | None:
        """Average write interval in seconds, or None if not enough data."""
        if self.write_interval_count < 2:
            return None
        return self.write_interval_sum / self.write_interval_count

    def should_write(self, profile_write_interval: int) -> bool:
        """Returns True if it's time to write the profile to the database."""
        return (self.count - self.last_written_count) >= profile_write_interval

    def mark_written(self):
        """Mark the profile as just written."""
        self.last_written_count = self.count


# ---------------------------------------------------------------------------
# Tuning rules
# ---------------------------------------------------------------------------

def recommend_threshold(cv: float) -> float:
    """Recommend rolling_std_threshold based on coefficient of variation."""
    if cv < 0.05:
        return 4.0
    elif cv < 0.2:
        return 5.0
    elif cv < 0.5:
        return 6.0
    else:
        return 8.0


def recommend_fading_factor(avg_write_interval: float | None) -> float:
    """Recommend ew_fading_factor based on average write interval in seconds."""
    if avg_write_interval is None:
        return 0.3  # default
    if avg_write_interval < 10:
        return 0.1
    elif avg_write_interval < 60:
        return 0.2
    elif avg_write_interval < 300:
        return 0.3
    else:
        return 0.5


# ---------------------------------------------------------------------------
# Profile storage
# ---------------------------------------------------------------------------

class ProfileStore:
    """LRU cache of SeriesProfile instances, stored in influxdb3_local.cache."""

    CACHE_KEY = "auto_profiler:profiles"

    @staticmethod
    def get_or_create(influxdb3_local, max_series: int, series_key: str,
                      task_id: str) -> SeriesProfile:
        """Gets an existing profile or creates a new one. Enforces LRU eviction."""
        store: OrderedDict = influxdb3_local.cache.get(ProfileStore.CACHE_KEY)
        if store is None:
            store = OrderedDict()

        if series_key in store:
            store.move_to_end(series_key)
        else:
            if len(store) >= max_series:
                evicted_key, _ = store.popitem(last=False)
                influxdb3_local.warn(
                    f"[{task_id}] LRU eviction: dropped profile for '{evicted_key}'"
                )

            store[series_key] = SeriesProfile()

        influxdb3_local.cache.put(ProfileStore.CACHE_KEY, store)

        return store[series_key]


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def build_profile_line(table_name: str, field_name: str, row: dict,
                       tag_names: list[str], profile: SeriesProfile,
                       min_observations: int):
    """Builds a LineBuilder for a series profile output."""
    builder = LineBuilder("_meta.series_profiles")

    # Add source identification tags
    builder.tag("source_table", table_name)
    builder.tag("field_name", field_name)

    # Preserve original tags
    for tag in sorted(tag_names):
        val = row.get(tag, None)
        if val is not None:
            builder.tag(tag, str(val))

    # Stats fields
    builder.int64_field("observations", profile.count)

    mean_val = profile.mean.get()
    var_val = profile.var.get()
    std_val = math.sqrt(var_val) if var_val and var_val > 0 else 0.0

    builder.float64_field("value_mean", mean_val if mean_val is not None else 0.0)
    builder.float64_field("value_std", std_val)
    builder.float64_field("value_min", profile.min.get() if profile.min.get() is not None else 0.0)
    builder.float64_field("value_max", profile.max.get() if profile.max.get() is not None else 0.0)

    cv = profile.get_cv()
    builder.float64_field("coefficient_of_variation", cv)

    avg_interval = profile.get_avg_write_interval()
    if avg_interval is not None:
        builder.float64_field("write_interval_seconds", avg_interval)

    # Tuning recommendations
    builder.float64_field("recommended_threshold", recommend_threshold(cv))
    builder.float64_field("recommended_fading_factor", recommend_fading_factor(avg_interval))

    # Maturity flag
    builder.bool_field("profile_mature", profile.count >= min_observations)

    return builder


def process_writes(influxdb3_local, table_batches: list, args: dict | None = None):
    """
    Auto-Profiler Plugin — Incrementally profiles incoming time-series data
    and writes recommended anomaly detection parameters per series.

    Triggered on each WAL flush. For every numeric field in every row:
    1. Looks up or creates a per-series profile (streaming stats)
    2. Updates the profile with the new value
    3. Periodically writes the profile to _meta.series_profiles

    Args:
        influxdb3_local: The InfluxDB 3 local API object.
        table_batches (list): List of table batch dicts with 'table_name' and 'rows'.
        args (dict | None): Trigger arguments dictionary.
    """
    task_id = str(uuid.uuid4())[:8]

    # Check River is available
    try:
        import river  # noqa: F401
    except ImportError:
        influxdb3_local.error(
            f"[{task_id}] River ML library is not installed. "
            "Run: influxdb3 install package river"
        )
        return

    influxdb3_local.info(f"[{task_id}] Auto-Profiler plugin triggered")

    try:
        # ---- Load TOML config if specified ----
        if args:
            args = load_profiler_toml_config(influxdb3_local, args, task_id)
            if args is None:
                return

        # ---- Parse configuration ----
        config = parse_profiler_args(args)

        # ---- Process each table batch ----
        total_rows = 0
        profiles_written = 0

        for table_batch in table_batches:
            table_name = table_batch["table_name"]
            rows = table_batch["rows"]

            # Skip excluded tables
            if table_name in config["exclude_tables"]:
                continue

            # Skip meta/anomaly output tables to avoid feedback loops
            if table_name.startswith("_meta.") or table_name.startswith("_anomalies."):
                continue

            if not rows:
                continue

            # Identify tag vs field columns from the first row
            sample_row = rows[0]
            tag_names = []
            numeric_fields = []
            for col, val in sample_row.items():
                if col == "time":
                    continue
                if col in config["exclude_fields"]:
                    continue
                if isinstance(val, str):
                    tag_names.append(col)
                elif isinstance(val, (int, float)) and not isinstance(val, bool):
                    numeric_fields.append(col)

            if not numeric_fields:
                continue

            for row in rows:
                total_rows += 1
                timestamp_ns = row.get("time", None)

                for field_name in numeric_fields:
                    value = row.get(field_name, None)
                    if value is None:
                        continue

                    # Skip NaN/Inf
                    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                        continue

                    value = float(value)

                    # Build series key and get/create profile
                    series_key = build_profiler_series_key(
                        table_name, row, field_name, tag_names
                    )
                    profile = ProfileStore.get_or_create(
                        influxdb3_local, config["max_series"],
                        series_key, task_id
                    )

                    # Update profile
                    profile.update(value, timestamp_ns)

                    # Write profile periodically
                    if profile.should_write(config["profile_write_interval"]):
                        builder = build_profile_line(
                            table_name, field_name, row,
                            tag_names, profile, config["min_observations"]
                        )
                        influxdb3_local.write(builder)
                        profile.mark_written()
                        profiles_written += 1

                        if config["log_profiles"]:
                            cv = profile.get_cv()
                            influxdb3_local.info(
                                f"[{task_id}] PROFILE: {series_key} "
                                f"count={profile.count} cv={cv:.3f} "
                                f"rec_threshold={recommend_threshold(cv)} "
                                f"rec_fading={recommend_fading_factor(profile.get_avg_write_interval())}"
                            )

        influxdb3_local.info(
            f"[{task_id}] Complete: {total_rows} rows, "
            f"{profiles_written} profiles written"
        )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Unexpected error: {e}")
        influxdb3_local.error(f"[{task_id}] Traceback: {traceback.format_exc()}")
