"""
{
    "plugin_type": ["onwrite"],
    "onwrite_args_config": [
        {
            "name": "target_database",
            "example": "analytics_db",
            "description": "Target database to write anomaly output to. If omitted, writes to the same database.",
            "required": false
        },
        {
            "name": "exclude_fields",
            "example": "status.version",
            "description": "Dot-separated list of field names to exclude from anomaly detection.",
            "required": false
        },
        {
            "name": "exclude_tables",
            "example": "debug_info.system_logs",
            "description": "Dot-separated list of table names to skip when using all_tables trigger.",
            "required": false
        },
        {
            "name": "rolling_std_threshold",
            "example": "5.0",
            "description": "Number of standard deviations from the mean to flag as anomaly. Defaults to 5.0.",
            "required": false
        },
        {
            "name": "hst_quantile",
            "example": "0.95",
            "description": "Quantile threshold for HalfSpaceTrees. Scores above this percentile are flagged. Defaults to 0.95.",
            "required": false
        },
        {
            "name": "hst_n_trees",
            "example": "10",
            "description": "Number of trees in HalfSpaceTrees ensemble. Defaults to 10.",
            "required": false
        },
        {
            "name": "hst_height",
            "example": "6",
            "description": "Height of each tree in HalfSpaceTrees. Defaults to 6 (127 nodes/tree).",
            "required": false
        },
        {
            "name": "hst_window_size",
            "example": "250",
            "description": "Window size for HalfSpaceTrees mass profile. Model warms up until this many observations. Defaults to 250.",
            "required": false
        },
        {
            "name": "max_series",
            "example": "100",
            "description": "Maximum number of unique series to track. Uses LRU eviction when exceeded. Defaults to 100.",
            "required": false
        },
        {
            "name": "ew_fading_factor",
            "example": "0.3",
            "description": "Fading factor for exponentially weighted mean/variance. Lower values give longer memory. Defaults to 0.3.",
            "required": false
        },
        {
            "name": "log_anomalies",
            "example": "true",
            "description": "If 'true', logs details about detected anomalies. Defaults to 'true'.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "anomaly_detector_config.toml",
            "description": "Path to TOML config file to override trigger arguments.",
            "required": false
        },
        {
            "name": "auto_tune",
            "example": "true",
            "description": "If 'true', reads per-series tuning parameters from _meta.series_profiles written by the auto-profiler plugin. Defaults to 'false'.",
            "required": false
        }
    ]
}
"""

import math
import os
import tomllib
import uuid
from collections import OrderedDict
from pathlib import Path
from typing import Iterable, Optional, Protocol, runtime_checkable


# ---------------------------------------------------------------------------
# Batch write helper
# ---------------------------------------------------------------------------

@runtime_checkable
class _LineBuilderInterface(Protocol):
    def build(self) -> str: ...


class _BatchLines:
    """
    Wraps multiple LineBuilder objects into a single object with a build()
    method that returns a newline-separated string.
    """
    def __init__(self, line_builders: Iterable[_LineBuilderInterface]):
        self._line_builders = list(line_builders)
        self._built: Optional[str] = None

    def _coerce_builder(self, builder: _LineBuilderInterface) -> str:
        build_fn = getattr(builder, "build", None)
        if not callable(build_fn):
            raise TypeError("line_builder is missing a callable build()")
        return str(build_fn())

    def build(self) -> str:
        if self._built is None:
            lines = [self._coerce_builder(builder) for builder in self._line_builders]
            if not lines:
                raise ValueError("batch_write received no lines to build")
            self._built = "\n".join(lines)
        return self._built


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_detector_toml_config(influxdb3_local, args: dict, task_id: str) -> dict | None:
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


def parse_detector_args(args: dict | None) -> dict:
    """Parses trigger arguments with defaults."""
    if not args:
        args = {}
    return {
        "target_database": args.get("target_database", None),
        "exclude_fields": set(args.get("exclude_fields", "").split(".")) - {""},
        "exclude_tables": set(args.get("exclude_tables", "").split(".")) - {""},
        "rolling_std_threshold": float(args.get("rolling_std_threshold", 5.0)),
        "hst_quantile": float(args.get("hst_quantile", 0.95)),
        "hst_n_trees": int(args.get("hst_n_trees", 10)),
        "hst_height": int(args.get("hst_height", 6)),
        "hst_window_size": int(args.get("hst_window_size", 250)),
        "max_series": int(args.get("max_series", 100)),
        "ew_fading_factor": float(args.get("ew_fading_factor", 0.3)),
        "log_anomalies": str(args.get("log_anomalies", "true")).lower() == "true",
        "auto_tune": str(args.get("auto_tune", "false")).lower() == "true",
    }


# ---------------------------------------------------------------------------
# Series key helpers
# ---------------------------------------------------------------------------

def build_detector_series_key(table_name: str, row: dict, field_name: str, tag_names: list[str]) -> str:
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
# Auto-tune support
# ---------------------------------------------------------------------------

def get_tuned_params(influxdb3_local, table_name: str, field_name: str,
                     task_id: str) -> dict | None:
    """
    Queries _meta.series_profiles for recommended parameters.
    Returns dict with 'threshold' and 'fading_factor', or None if not available.
    """
    try:
        results = influxdb3_local.query(
            "SELECT recommended_threshold, recommended_fading_factor, profile_mature "
            "FROM \"_meta.series_profiles\" "
            f"WHERE source_table = '{table_name}' AND field_name = '{field_name}' "
            "ORDER BY time DESC LIMIT 1"
        )
        if results and len(results) > 0:
            row = results[0]
            if row.get("profile_mature", False):
                return {
                    "threshold": float(row["recommended_threshold"]),
                    "fading_factor": float(row["recommended_fading_factor"]),
                }
        return None
    except Exception as e:
        # Silently handle "table not found" — profiles table won't exist
        # until the auto-profiler has written its first profile
        if "not found" not in str(e):
            influxdb3_local.warn(f"[{task_id}] Auto-tune query failed: {e}")
        return None


class TuneCache:
    """Caches auto-tune parameters to avoid querying on every write."""

    CACHE_KEY = "anomaly_detector:tune_cache"
    REFRESH_INTERVAL = 100  # refresh every N observations per series

    @staticmethod
    def get_params(influxdb3_local, table_name: str, field_name: str,
                   series_key: str, observation_count: int,
                   task_id: str) -> dict | None:
        """Returns cached tuned params, refreshing periodically."""
        cache = influxdb3_local.cache.get(TuneCache.CACHE_KEY)
        if cache is None:
            cache = {}

        entry = cache.get(series_key)

        # Refresh if no entry or enough observations have passed
        if entry is None or (observation_count - entry.get("last_refresh", 0)) >= TuneCache.REFRESH_INTERVAL:
            params = get_tuned_params(influxdb3_local, table_name, field_name, task_id)
            cache[series_key] = {
                "params": params,
                "last_refresh": observation_count,
            }
            influxdb3_local.cache.put(TuneCache.CACHE_KEY, cache)
            return params

        return entry.get("params")


# ---------------------------------------------------------------------------
# Model management
# ---------------------------------------------------------------------------

class SeriesModel:
    """Holds both detectors for a single series (one field of one tag combo)."""

    def __init__(self, fading_factor: float, hst_n_trees: int, hst_height: int,
                 hst_window_size: int, hst_quantile: float, seed: int = 42):
        from river import anomaly, compose, preprocessing, stats

        # Rolling stats detector
        self.ew_mean = stats.EWMean(fading_factor=fading_factor)
        self.ew_var = stats.EWVar(fading_factor=fading_factor)
        self.observation_count = 0

        # HalfSpaceTrees detector
        self.hst_pipeline = compose.Pipeline(
            preprocessing.MinMaxScaler(),
            anomaly.QuantileFilter(
                anomaly.HalfSpaceTrees(
                    n_trees=hst_n_trees,
                    height=hst_height,
                    window_size=hst_window_size,
                    seed=seed,
                ),
                q=hst_quantile,
                protect_anomaly_detector=True,
            )
        )
        self.hst_window_size = hst_window_size

    def update_and_score(self, value: float, threshold: float) -> dict:
        """
        Updates both detectors with a new value and returns scoring results.
        Uses test-then-train: score before learning.
        """
        self.observation_count += 1
        result = {
            "original_value": value,
            "observations": self.observation_count,
        }

        # --- Rolling stats ---
        if self.observation_count >= 2:
            mean = self.ew_mean.get()
            var = self.ew_var.get()
            std = math.sqrt(var) if var > 0 else 0.0
            deviation = abs(value - mean) / std if std > 0 else 0.0
            rolling_anomaly = deviation > threshold

            result["rolling_anomaly"] = rolling_anomaly
            result["rolling_mean"] = mean
            result["rolling_std"] = std
            result["rolling_deviation"] = deviation
            result["rolling_threshold"] = threshold
        else:
            result["rolling_anomaly"] = False

        # Always update rolling stats after scoring
        self.ew_mean.update(value)
        self.ew_var.update(value)

        # --- HalfSpaceTrees ---
        x = {"value": value}
        hst_warming_up = self.observation_count < self.hst_window_size
        result["hst_warming_up"] = hst_warming_up

        if not hst_warming_up:
            hst_score = self.hst_pipeline.score_one(x)
            hst_anomaly = self.hst_pipeline.classify(x)
            result["hst_anomaly"] = bool(hst_anomaly)
            result["hst_score"] = hst_score
        else:
            result["hst_anomaly"] = False

        # Always learn
        self.hst_pipeline.learn_one(x)

        # Combined flag
        result["is_anomaly"] = result["rolling_anomaly"] or result["hst_anomaly"]

        return result


class ModelStore:
    """LRU cache of SeriesModel instances, stored in influxdb3_local.cache."""

    CACHE_KEY = "anomaly_detector:models"

    @staticmethod
    def get_or_create(influxdb3_local, max_series: int, series_key: str,
                      config: dict, task_id: str) -> SeriesModel:
        """Gets an existing model or creates a new one. Enforces LRU eviction."""
        store: OrderedDict = influxdb3_local.cache.get(ModelStore.CACHE_KEY)
        if store is None:
            store = OrderedDict()

        if series_key in store:
            # Move to end (most recently used)
            store.move_to_end(series_key)
        else:
            # Evict LRU if at capacity
            if len(store) >= max_series:
                evicted_key, _ = store.popitem(last=False)
                influxdb3_local.warn(
                    f"[{task_id}] LRU eviction: dropped model for '{evicted_key}'"
                )

            store[series_key] = SeriesModel(
                fading_factor=config["ew_fading_factor"],
                hst_n_trees=config["hst_n_trees"],
                hst_height=config["hst_height"],
                hst_window_size=config["hst_window_size"],
                hst_quantile=config["hst_quantile"],
            )

        # Update cache (no TTL — persist until server restart)
        influxdb3_local.cache.put(ModelStore.CACHE_KEY, store)

        return store[series_key]


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def build_anomaly_line(table_name: str, field_name: str, row: dict,
                       tag_names: list[str], result: dict):
    """Builds a LineBuilder for an anomaly detection result."""
    builder = LineBuilder(f"_anomalies.{table_name}")

    # Preserve original tags
    for tag in sorted(tag_names):
        val = row.get(tag, None)
        if val is not None:
            builder.tag(tag, str(val))

    # Add field_name tag
    builder.tag("field_name", field_name)

    # Set timestamp from source row if available
    ts = row.get("time", None)
    if ts is not None:
        builder.time_ns(ts)

    # Core fields
    builder.float64_field("original_value", result["original_value"])
    builder.bool_field("is_anomaly", result["is_anomaly"])
    builder.int64_field("observations", result["observations"])

    # Rolling stats fields
    builder.bool_field("rolling_anomaly", result["rolling_anomaly"])
    if "rolling_mean" in result:
        builder.float64_field("rolling_mean", result["rolling_mean"])
        builder.float64_field("rolling_std", result["rolling_std"])
        builder.float64_field("rolling_deviation", result["rolling_deviation"])
        builder.float64_field("rolling_threshold", result["rolling_threshold"])

    # HST fields (omitted during warm-up except warming_up flag)
    builder.bool_field("hst_warming_up", result["hst_warming_up"])
    if "hst_score" in result:
        builder.bool_field("hst_anomaly", result["hst_anomaly"])
        builder.float64_field("hst_score", result["hst_score"])

    return builder


def process_writes(influxdb3_local, table_batches: list, args: dict | None = None):
    """
    Anomaly Detector Plugin — Detects anomalies in incoming time-series data
    using dual detectors: rolling statistics and HalfSpaceTrees.

    Triggered on each WAL flush. For every numeric field in every row:
    1. Looks up or creates a per-series model pair (rolling stats + HST)
    2. Scores the value with both detectors
    3. If either flags an anomaly, writes the result to _anomalies.{table}

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

    influxdb3_local.info(f"[{task_id}] Anomaly Detector plugin triggered")

    try:
        # ---- Load TOML config if specified ----
        if args:
            args = load_detector_toml_config(influxdb3_local, args, task_id)
            if args is None:
                return

        # ---- Parse configuration ----
        config = parse_detector_args(args)

        # ---- Process each table batch ----
        total_rows = 0
        total_anomalies = 0

        for table_batch in table_batches:
            table_name = table_batch["table_name"]
            rows = table_batch["rows"]

            # Skip excluded tables
            if table_name in config["exclude_tables"]:
                continue

            # Skip anomaly output tables to avoid feedback loops
            if table_name.startswith("_anomalies."):
                continue

            if not rows:
                continue

            # Identify tag vs field columns from the first row
            # Tags are strings, fields are numeric — we detect per-value
            # We need to identify tags once per batch for series key building
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

            # Process rows and collect anomaly line builders
            anomaly_builders = []

            for row in rows:
                total_rows += 1

                for field_name in numeric_fields:
                    value = row.get(field_name, None)
                    if value is None:
                        continue

                    # Skip NaN/Inf
                    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                        influxdb3_local.warn(
                            f"[{task_id}] Skipping NaN/Inf value for "
                            f"{table_name}.{field_name}"
                        )
                        continue

                    value = float(value)

                    # Build series key and get/create model
                    series_key = build_detector_series_key(
                        table_name, row, field_name, tag_names
                    )
                    model = ModelStore.get_or_create(
                        influxdb3_local, config["max_series"],
                        series_key, config, task_id
                    )

                    # Determine threshold (auto-tuned or static)
                    threshold = config["rolling_std_threshold"]
                    if config["auto_tune"]:
                        tuned = TuneCache.get_params(
                            influxdb3_local, table_name, field_name,
                            series_key, model.observation_count, task_id
                        )
                        if tuned:
                            threshold = tuned["threshold"]

                    # Score and update
                    result = model.update_and_score(value, threshold)

                    if result["is_anomaly"]:
                        total_anomalies += 1
                        anomaly_builders.append(
                            build_anomaly_line(
                                table_name, field_name, row,
                                tag_names, result
                            )
                        )

                        if config["log_anomalies"]:
                            rolling_dev = result.get("rolling_deviation", 0)
                            hst_score = result.get("hst_score", "n/a")
                            influxdb3_local.info(
                                f"[{task_id}] ANOMALY: {series_key} "
                                f"value={value} rolling_dev={rolling_dev:.2f} "
                                f"hst_score={hst_score}"
                            )

            # Write anomalies for this table
            if anomaly_builders:
                for builder in anomaly_builders:
                    influxdb3_local.write(builder)

        influxdb3_local.info(
            f"[{task_id}] Complete: {total_rows} rows processed, "
            f"{total_anomalies} anomalies detected"
        )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Unexpected error: {e}")
