"""
{
    "plugin_type": ["onwrite"],
    "onwrite_args_config": [
        {
            "name": "include_fields",
            "example": "temperature humidity",
            "description": "Space-separated list of numeric field names to monitor. If set, only these fields are processed. If empty, all numeric fields are processed. In TOML config use a list.",
            "required": false
        },
        {
            "name": "exclude_fields",
            "example": "status version",
            "description": "Space-separated list of field names to exclude from anomaly detection. In TOML config use a list.",
            "required": false
        },
        {
            "name": "exclude_tables",
            "example": "debug_info system_logs",
            "description": "Space-separated list of table names to skip when using all_tables trigger. In TOML config use a list.",
            "required": false
        },
        {
            "name": "string_fields",
            "example": "status_message description",
            "description": "Space-separated list of string column names that are string fields (not tags). All other string columns are treated as tags. In TOML config use a list.",
            "required": false
        },
        {
            "name": "rolling_std_threshold",
            "example": "5.0",
            "description": "Number of standard deviations from the mean to flag as anomaly. Defaults to 5.0.",
            "required": false
        },
        {
            "name": "combination_mode",
            "example": "any",
            "description": "How to combine detector votes: 'any' (OR), 'majority', or 'all' (AND). Only counts active detectors. Defaults to 'any'.",
            "required": false
        },
        {
            "name": "enable_seasonal",
            "example": "true",
            "description": "Force seasonal detection on/off. Empty string = auto from profiler. Defaults to '' (auto).",
            "required": false
        },
        {
            "name": "enable_adwin",
            "example": "true",
            "description": "Force ADWIN drift detection on/off. Empty string = auto from profiler. Defaults to '' (auto).",
            "required": false
        },
        {
            "name": "detector_mode",
            "example": "zscore seasonal",
            "description": "Explicitly set detector mode. Overrides profiler recommendation. Components: zscore_conservative, zscore_low, zscore_high, zscore_adaptive, seasonal, adwin. Space-separated string or TOML list. Empty = auto from profiler or default. Defaults to '' (auto).",
            "required": false
        },
        {
            "name": "seasonal_period",
            "example": "weekly",
            "description": "Seasonal bucket period: 'hourly' (24 buckets, hour-of-day) or 'weekly' (168 buckets, hour-of-week). Empty string = auto from profiler. Defaults to '' (auto, falls back to 'hourly').",
            "required": false
        },
        {
            "name": "adwin_delta",
            "example": "0.002",
            "description": "ADWIN sensitivity parameter. Lower values make drift detection more sensitive. Defaults to 0.002.",
            "required": false
        },
        {
            "name": "seasonal_fading_factor",
            "example": "0.1",
            "description": "Fading factor for seasonal bucket stats. Lower = longer memory. Defaults to 0.1.",
            "required": false
        },
        {
            "name": "max_series",
            "example": "1000",
            "description": "Maximum number of unique series to track. Uses LRU eviction when exceeded. Defaults to 1000.",
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
            "example": "river_anomaly_detector_config.toml",
            "description": "Path to TOML config file. Supports absolute paths or relative paths (resolved via PLUGIN_DIR env var).",
            "required": false
        },
        {
            "name": "auto_tune",
            "example": "true",
            "description": "If 'true', reads per-series tuning parameters from _meta.series_profiles written by the auto-profiler plugin. Defaults to 'true'.",
            "required": false
        },
        {
            "name": "tune_refresh_interval",
            "example": "100",
            "description": "Number of observations per series between auto-tune refreshes from _meta.series_profiles. Lower values pick up profiler changes faster at the cost of more queries. Defaults to 100.",
            "required": false
        },
        {
            "name": "seasonal_threshold",
            "example": "3.0",
            "description": "Number of standard deviations from the seasonal bucket mean to flag as anomaly. Defaults to 3.0.",
            "required": false
        },
        {
            "name": "min_seasonal_observations",
            "example": "5",
            "description": "Minimum observations per seasonal bucket before it contributes to anomaly detection. Defaults to 5.",
            "required": false
        },
        {
            "name": "min_rolling_observations",
            "example": "10",
            "description": "Minimum total observations per series before the rolling Z-score detector starts scoring. Lower values mean faster cold start but more false positives. Defaults to 10.",
            "required": false
        },
        {
            "name": "checkpoint_interval_seconds",
            "example": "1800",
            "description": "How often to checkpoint models to the database for persistence across restarts. Defaults to 1800 (30 minutes).",
            "required": false
        },
        {
            "name": "max_checkpoint_age_hours",
            "example": "24",
            "description": "Ignore checkpoints older than this when restoring on restart. Defaults to 24.",
            "required": false
        },
        {
            "name": "min_checkpoint_observations",
            "example": "10",
            "description": "Skip checkpointing models with fewer observations than this. Avoids persisting cold-start models that would be rebuilt from scratch anyway. Defaults to 10.",
            "required": false
        }
    ]
}
"""

import base64
import math
import os
import pickle
import time
import tomllib
import traceback
import uuid
import zlib
from collections import OrderedDict, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from river import stats


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------


def load_detector_toml_config(influxdb3_local, args: dict, task_id: str) -> dict | None:
    """Loads trigger arguments from a TOML config file if config_file_path is set."""
    path = args.get("config_file_path", None)
    if not path:
        return args

    if not path.endswith(".toml"):
        influxdb3_local.error(
            f"[{task_id}] Invalid config file format: expected a .toml file"
        )
        return None

    try:
        if os.path.isabs(path):
            file_path = Path(path)
        else:
            plugin_dir_var = os.getenv("PLUGIN_DIR", None)
            if not plugin_dir_var:
                influxdb3_local.error(
                    f"[{task_id}] PLUGIN_DIR env var not set and config_file_path is relative"
                )
                return None
            file_path = Path(plugin_dir_var) / path
        influxdb3_local.info(f"[{task_id}] Reading config from {file_path}")
        with open(file_path, "rb") as f:
            return tomllib.load(f)
    except Exception:
        influxdb3_local.error(f"[{task_id}] Failed to read config file")
        return None


def _parse_list_arg(value) -> list:
    """Parse a list argument: TOML list or space-separated string from trigger args.

    Returns a sorted, deduplicated list. Sorted so that string representations
    built via " ".join(...) are stable across process restarts (set iteration
    order depends on PYTHONHASHSEED).
    """
    if isinstance(value, list):
        items = set(value) - {""}
    else:
        items = set(str(value).split()) - {""}
    return sorted(items)


def parse_detector_args(args: dict | None) -> dict:
    """Parses trigger arguments with defaults."""
    if not args:
        args = {}
    return {
        "include_fields": set(_parse_list_arg(args.get("include_fields", ""))),
        "exclude_fields": set(_parse_list_arg(args.get("exclude_fields", ""))),
        "exclude_tables": set(_parse_list_arg(args.get("exclude_tables", ""))),
        "string_fields": set(_parse_list_arg(args.get("string_fields", ""))),
        "rolling_std_threshold": float(args.get("rolling_std_threshold", 5.0)),
        "max_series": int(args.get("max_series", 1000)),
        "ew_fading_factor": float(args.get("ew_fading_factor", 0.3)),
        "seasonal_fading_factor": float(args.get("seasonal_fading_factor", 0.1)),
        "log_anomalies": str(args.get("log_anomalies", "true")).lower() == "true",
        "auto_tune": str(args.get("auto_tune", "true")).lower() == "true",
        "combination_mode": str(args.get("combination_mode", "any")).lower(),
        "enable_seasonal": str(args.get("enable_seasonal", "")).lower(),
        "enable_adwin": str(args.get("enable_adwin", "")).lower(),
        "detector_mode": " ".join(_parse_list_arg(args.get("detector_mode", ""))),
        "seasonal_period": str(args.get("seasonal_period", "")),
        "adwin_delta": float(args.get("adwin_delta", 0.002)),
        "seasonal_threshold": float(args.get("seasonal_threshold", 3.0)),
        "min_seasonal_observations": int(args.get("min_seasonal_observations", 5)),
        "min_rolling_observations": int(args.get("min_rolling_observations", 10)),
        "tune_refresh_interval": int(args.get("tune_refresh_interval", 100)),
        "checkpoint_interval_seconds": int(
            args.get("checkpoint_interval_seconds", 1800)
        ),
        "max_checkpoint_age_hours": int(args.get("max_checkpoint_age_hours", 24)),
        "min_checkpoint_observations": int(args.get("min_checkpoint_observations", 10)),
    }


# ---------------------------------------------------------------------------
# Series key helpers
# ---------------------------------------------------------------------------


def build_detector_series_key(
    table_name: str, row: dict, field_name: str, tag_names: list[str]
) -> str:
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


def extract_tag_parts(series_key: str) -> str:
    """Extract the tag segment from a series key: 'table:tag1=v1,tag2=v2:field' → 'tag1=v1,tag2=v2'."""
    parts = series_key.split(":")
    if len(parts) >= 3:
        return parts[1]
    return ""


def get_tuned_params(
    influxdb3_local,
    table_name: str,
    field_name: str,
    row: dict,
    tag_names: list[str],
    task_id: str,
) -> dict | None:
    """
    Queries _meta.series_profiles for recommended parameters.
    Returns dict with all profile fields, or None if not available.
    """
    try:
        where_clauses = [
            f"source_table = '{table_name}'",
            f"field_name = '{field_name}'",
        ]
        # Add tag filters from original row data (avoids series key parsing issues)
        for tag in sorted(tag_names):
            val = row.get(tag, None)
            if val is not None:
                escaped_val = str(val).replace("'", "''")
                where_clauses.append(f"\"{tag}\" = '{escaped_val}'")

        where_sql = " AND ".join(where_clauses)
        results = influxdb3_local.query(
            "SELECT recommended_threshold, recommended_fading_factor, "
            "recommended_detector_mode, recommended_seasonal_fading, "
            "pattern_label, profile_mature, seasonality_ready, seasonal_period "
            'FROM "_meta.series_profiles" '
            f"WHERE {where_sql} "
            "ORDER BY time DESC LIMIT 1"
        )
        if results and len(results) > 0:
            row = results[0]
            if row.get("profile_mature", False):
                detector_mode = str(
                    row.get("recommended_detector_mode", "zscore_conservative")
                )
                # Don't use seasonal mode if profiler hasn't collected enough seasonal data
                if (
                    not row.get("seasonality_ready", False)
                    and "seasonal" in detector_mode
                ):
                    detector_mode = detector_mode.replace("seasonal", "").strip()
                    if not detector_mode:
                        detector_mode = "zscore_conservative"
                return {
                    "threshold": float(row.get("recommended_threshold", 5.0)),
                    "fading_factor": float(row.get("recommended_fading_factor", 0.3)),
                    "detector_mode": detector_mode,
                    "seasonal_fading": float(
                        row.get("recommended_seasonal_fading", 0.1)
                    ),
                    "pattern_label": str(row.get("pattern_label", "")),
                    "seasonal_period": str(row.get("seasonal_period", "hourly")),
                }
        return None
    except Exception as e:
        if "not found" not in str(e):
            influxdb3_local.warn(f"[{task_id}] Auto-tune query failed: {e}")
        return None


class TuneCache:
    """Caches auto-tune parameters from _meta.series_profiles with mode hysteresis.

    The profiler is queried at most once per `refresh_interval` observations per
    series. Non-mode fields (threshold, fading factors, seasonal_period) are
    refreshed on each query. `detector_mode` only switches after
    MODE_HYSTERESIS_THRESHOLD consecutive fresh recommendations of the new mode —
    this prevents flip-flopping between borderline pattern classifications.
    """

    CACHE_KEY = "anomaly_detector:tune_cache"
    MODE_HYSTERESIS_THRESHOLD = 3

    @staticmethod
    def get_params(
        influxdb3_local,
        table_name: str,
        field_name: str,
        row: dict,
        tag_names: list[str],
        series_key: str,
        observation_count: int,
        refresh_interval: int,
        task_id: str,
    ) -> dict | None:
        """Returns stabilized tuned params, refreshing every N observations per series."""
        cache = influxdb3_local.cache.get(TuneCache.CACHE_KEY)
        if cache is None:
            cache = {}

        entry = cache.get(series_key)
        needs_refresh = (
            entry is None
            or (observation_count - entry.get("last_refresh", 0)) >= refresh_interval
        )
        if not needs_refresh:
            return entry.get("applied")

        fresh = get_tuned_params(
            influxdb3_local, table_name, field_name, row, tag_names, task_id
        )

        applied = dict(entry["applied"]) if entry and entry.get("applied") else None
        pending_mode = entry.get("pending_mode") if entry else None
        pending_count = entry.get("pending_count", 0) if entry else 0

        if fresh is not None:
            if applied is None:
                # First successful query — seed directly, no hysteresis needed.
                applied = dict(fresh)
                pending_mode = None
                pending_count = 0
            else:
                # Non-mode fields refresh immediately.
                applied["threshold"] = fresh["threshold"]
                applied["fading_factor"] = fresh["fading_factor"]
                applied["seasonal_fading"] = fresh["seasonal_fading"]
                applied["seasonal_period"] = fresh["seasonal_period"]

                fresh_mode = fresh["detector_mode"]
                if fresh_mode == applied.get("detector_mode"):
                    pending_mode = None
                    pending_count = 0
                elif fresh_mode == pending_mode:
                    pending_count += 1
                    if pending_count >= TuneCache.MODE_HYSTERESIS_THRESHOLD:
                        applied["detector_mode"] = fresh_mode
                        applied["pattern_label"] = fresh.get("pattern_label", "")
                        pending_mode = None
                        pending_count = 0
                else:
                    pending_mode = fresh_mode
                    pending_count = 1

        cache[series_key] = {
            "applied": applied,
            "pending_mode": pending_mode,
            "pending_count": pending_count,
            "last_refresh": observation_count,
        }
        influxdb3_local.cache.put(TuneCache.CACHE_KEY, cache)
        return applied


# ---------------------------------------------------------------------------
# Model management
# ---------------------------------------------------------------------------


class SeriesModel:
    """Adaptive anomaly detector for a single series. Detector mode is set by profiler recommendations."""

    def __init__(
        self,
        fading_factor: float = 0.3,
        seasonal_fading_factor: float = 0.1,
        detector_mode: str = "zscore_conservative",
        adwin_delta: float = 0.002,
        seasonal_period: str = "hourly",
    ):
        # Rolling stats detector (always active)
        self.ew_mean = stats.EWMean(fading_factor=fading_factor)
        self.ew_var = stats.EWVar(fading_factor=fading_factor)
        self.observation_count = 0
        self.fading_factor = fading_factor
        self.detector_mode = detector_mode

        # Seasonal (always initialized — "always learn" strategy)
        self.seasonal_grid = {}
        self.seasonal_fading_factor = seasonal_fading_factor
        self.seasonal_period = (
            seasonal_period  # "hourly" (24 buckets) or "weekly" (168 buckets)
        )

        # ADWIN (always initialized — "always learn" strategy)
        self.adwin_delta = adwin_delta
        self._init_adwin()

    def _init_adwin(self):
        from river import drift

        self.adwin = drift.ADWIN(delta=self.adwin_delta)

    def set_detector_mode(
        self,
        new_mode: str,
        seasonal_fading: float | None = None,
        fading_factor: float | None = None,
        seasonal_period: str | None = None,
    ):
        """Apply detector mode and related parameters.

        Called on every observation; idempotent when values haven't changed.
        Mode-change hysteresis is applied upstream in TuneCache (so it counts
        fresh profiler recommendations, not per-row calls).
        """
        if seasonal_fading is not None:
            self.seasonal_fading_factor = seasonal_fading

        if seasonal_period is not None and seasonal_period != self.seasonal_period:
            self.seasonal_period = seasonal_period
            self.seasonal_grid = {}  # reset buckets — different bucketing scheme

        if fading_factor is not None and fading_factor != self.fading_factor:
            current_mean = self.ew_mean.get()
            current_var = self.ew_var.get()
            self.fading_factor = fading_factor
            self.ew_mean = stats.EWMean(fading_factor=fading_factor)
            self.ew_var = stats.EWVar(fading_factor=fading_factor)
            # Seed to avoid cold start
            if current_mean is not None:
                self.ew_mean.update(current_mean)
            if current_var is not None and current_var >= 0:
                std = math.sqrt(current_var) if current_var > 0 else 0.0
                if std > 0 and current_mean is not None:
                    self.ew_var.update(current_mean + std)
                    self.ew_var.update(current_mean - std)

        self.detector_mode = new_mode

    def update_and_score(
        self,
        value: float,
        threshold: float,
        timestamp_ns: int | None = None,
        seasonal_threshold: float = 3.0,
        min_seasonal_observations: int = 5,
        min_rolling_observations: int = 10,
        combination_mode: str = "any",
    ) -> dict:
        """
        Updates ALL detectors and returns scoring results.
        Uses test-then-train: score before learning.
        All detectors always learn, but only active detectors (based on
        detector_mode) contribute votes to the anomaly decision.
        """
        self.observation_count += 1
        result = {
            "original_value": value,
            "observations": self.observation_count,
            "detector_mode": self.detector_mode,
        }

        votes = []  # list of booleans from active detectors

        # --- Rolling Z-Score (always active for scoring) ---
        if self.observation_count >= min_rolling_observations:
            mean = self.ew_mean.get()
            var = self.ew_var.get()
            std = math.sqrt(var) if var > 0 else 0.0

            if std > 0:
                deviation = abs(value - mean) / std
                rolling_anomaly = deviation > threshold
            else:
                # std=0 means all prior values were identical;
                # any different value is anomalous
                deviation = None
                rolling_anomaly = value != mean

            result["rolling_anomaly"] = rolling_anomaly
            result["rolling_mean"] = mean
            result["rolling_std"] = std
            if deviation is not None:
                result["rolling_deviation"] = deviation
            result["rolling_threshold"] = threshold
            votes.append(rolling_anomaly)
        else:
            result["rolling_anomaly"] = False
            votes.append(False)

        # Always update rolling stats after scoring
        self.ew_mean.update(value)
        self.ew_var.update(value)

        # --- Seasonal (always learn, only vote when mode includes "seasonal") ---
        seasonal_is_active = "seasonal" in self.detector_mode
        result["seasonal_anomaly"] = False
        result["seasonal_mature"] = False

        if timestamp_ns is not None:
            try:
                timestamp_ns = int(timestamp_ns)
            except (TypeError, ValueError):
                timestamp_ns = None

        if timestamp_ns is not None:
            dt = datetime.fromtimestamp(timestamp_ns / 1e9, tz=timezone.utc)
            if self.seasonal_period == "weekly":
                bucket_key = dt.weekday() * 24 + dt.hour  # 0-167
                day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                result["seasonal_bucket"] = f"{day_names[dt.weekday()]}-H{dt.hour:02d}"
            else:
                bucket_key = dt.hour  # 0-23
                result["seasonal_bucket"] = f"H{dt.hour:02d}"

            if bucket_key not in self.seasonal_grid:
                self.seasonal_grid[bucket_key] = {
                    "ew_mean": stats.EWMean(fading_factor=self.seasonal_fading_factor),
                    "ew_var": stats.EWVar(fading_factor=self.seasonal_fading_factor),
                    "count": 0,
                }

            bucket = self.seasonal_grid[bucket_key]
            bucket["count"] += 1
            result["seasonal_observations"] = bucket["count"]
            result["seasonal_mature"] = bucket["count"] >= min_seasonal_observations

            if bucket["count"] >= 2:
                s_mean = bucket["ew_mean"].get()
                s_var = bucket["ew_var"].get()
                s_std = math.sqrt(s_var) if s_var > 0 else 0.0
                s_deviation = abs(value - s_mean) / s_std if s_std > 0 else 0.0

                result["seasonal_mean"] = s_mean
                result["seasonal_std"] = s_std
                result["seasonal_deviation"] = s_deviation

                if result["seasonal_mature"] and s_deviation > seasonal_threshold:
                    result["seasonal_anomaly"] = True

            # Always learn
            bucket["ew_mean"].update(value)
            bucket["ew_var"].update(value)

            # Only vote when active
            if seasonal_is_active:
                votes.append(result["seasonal_anomaly"])

        # --- ADWIN (always learn, only vote when mode includes "adwin") ---
        adwin_is_active = "adwin" in self.detector_mode
        result["adwin_anomaly"] = False
        result["drift_detected"] = False

        self.adwin.update(value)
        if self.adwin.drift_detected:
            result["adwin_anomaly"] = True
            result["drift_detected"] = True

        if adwin_is_active:
            votes.append(result["adwin_anomaly"])

        # --- Combine votes ---
        if combination_mode == "all":
            result["is_anomaly"] = all(votes) if votes else False
        elif combination_mode == "majority":
            # >= ceil(n/2): with 2 voters 1/2 is enough, with 3 voters need 2/3
            result["is_anomaly"] = (
                sum(votes) >= (len(votes) + 1) // 2 if votes else False
            )
        else:  # "any"
            result["is_anomaly"] = any(votes)

        return result


class ModelStore:
    """LRU cache of SeriesModel instances, stored in influxdb3_local.cache."""

    CACHE_KEY = "anomaly_detector:models"

    @staticmethod
    def get_or_create(
        influxdb3_local, max_series: int, series_key: str, config: dict, task_id: str
    ) -> SeriesModel:
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

            initial_mode = (
                config["detector_mode"]
                if config["detector_mode"]
                else "zscore_conservative"
            )
            initial_seasonal_period = (
                config["seasonal_period"] if config["seasonal_period"] else "hourly"
            )
            store[series_key] = SeriesModel(
                fading_factor=config["ew_fading_factor"],
                seasonal_fading_factor=config["seasonal_fading_factor"],
                detector_mode=initial_mode,
                adwin_delta=config["adwin_delta"],
                seasonal_period=initial_seasonal_period,
            )

        # Update cache (no TTL — persist until server restart)
        influxdb3_local.cache.put(ModelStore.CACHE_KEY, store)

        return store[series_key]


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def build_anomaly_line(
    table_name: str, field_name: str, row: dict, tag_names: list[str], result: dict
):
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

    # Detector mode
    builder.string_field("detector_mode", result.get("detector_mode", ""))

    # Rolling stats fields
    builder.bool_field("rolling_anomaly", result["rolling_anomaly"])
    if "rolling_mean" in result:
        builder.float64_field("rolling_mean", result["rolling_mean"])
        builder.float64_field("rolling_std", result["rolling_std"])
        if "rolling_deviation" in result:
            builder.float64_field("rolling_deviation", result["rolling_deviation"])
        builder.float64_field("rolling_threshold", result["rolling_threshold"])

    # Seasonal fields
    builder.bool_field("seasonal_anomaly", result.get("seasonal_anomaly", False))
    builder.bool_field("seasonal_mature", result.get("seasonal_mature", False))
    if "seasonal_mean" in result:
        builder.float64_field("seasonal_mean", result["seasonal_mean"])
        builder.float64_field("seasonal_std", result["seasonal_std"])
        builder.float64_field("seasonal_deviation", result["seasonal_deviation"])
    if "seasonal_bucket" in result:
        builder.string_field("seasonal_bucket", result["seasonal_bucket"])
    if "seasonal_observations" in result:
        builder.int64_field("seasonal_observations", result["seasonal_observations"])

    # ADWIN fields
    builder.bool_field("adwin_anomaly", result.get("adwin_anomaly", False))
    builder.bool_field("drift_detected", result.get("drift_detected", False))

    return builder


# ---------------------------------------------------------------------------
# Self-checkpoint and self-restore
# ---------------------------------------------------------------------------

DETECTOR_CHECKPOINT_TIME_KEY = "anomaly_detector:last_checkpoint_time"
DETECTOR_PLUGIN_NAME = "river_anomaly_detector"
DETECTOR_MODEL_TYPE = "SeriesModel"


def detector_should_checkpoint(influxdb3_local, interval_seconds: int) -> bool:
    """Returns True if enough time has elapsed since the last checkpoint."""
    last_time = influxdb3_local.cache.get(DETECTOR_CHECKPOINT_TIME_KEY)
    if last_time is None:
        return True
    return (time.time() - last_time) >= interval_seconds


def detector_mark_checkpoint_done(influxdb3_local):
    """Records the current time as the last checkpoint time."""
    influxdb3_local.cache.put(DETECTOR_CHECKPOINT_TIME_KEY, time.time())


def detector_maybe_restore(influxdb3_local, config: dict, task_id: str):
    """On empty cache, restore models from _system.model_checkpoints."""
    store = influxdb3_local.cache.get(ModelStore.CACHE_KEY)
    if store is not None:
        return  # Already have models

    max_age = config["max_checkpoint_age_hours"]
    try:
        results = influxdb3_local.query(
            f"SELECT time, series_key, model_data, chunk_index, chunk_total "
            f'FROM "_system.model_checkpoints" '
            f"WHERE plugin = '{DETECTOR_PLUGIN_NAME}' "
            f"AND time > now() - interval '{max_age} hours' "
            f"ORDER BY time DESC"
        )
    except Exception:
        return  # Table doesn't exist yet

    if not results or len(results) == 0:
        return

    # Group chunks by series_key, keeping only chunks from the latest checkpoint
    # run per key (timestamp match) to avoid mixing partials from different runs.
    latest_ts_by_key = {}
    for row in results:
        sk = row.get("series_key")
        if not sk:
            continue
        ts = row.get("time")
        if sk not in latest_ts_by_key:
            latest_ts_by_key[sk] = ts

    chunks_by_key = defaultdict(dict)
    totals_by_key = {}
    for row in results:
        sk = row.get("series_key")
        if not sk:
            continue
        ts = row.get("time")
        if ts != latest_ts_by_key.get(sk):
            continue
        chunk_total = int(row.get("chunk_total", 1))
        chunk_index = int(row.get("chunk_index", 0))
        totals_by_key[sk] = chunk_total
        model_data = row.get("model_data", "")
        if model_data and chunk_index not in chunks_by_key[sk]:
            chunks_by_key[sk][chunk_index] = model_data

    store = OrderedDict()
    for sk, chunks in chunks_by_key.items():
        total = totals_by_key[sk]
        if len(chunks) < total:
            influxdb3_local.warn(
                f"[{task_id}] Incomplete checkpoint for {sk}: "
                f"{len(chunks)}/{total} chunks, skipping"
            )
            continue
        try:
            full_encoded = "".join(chunks[i] for i in range(total))
            state = pickle.loads(zlib.decompress(base64.b64decode(full_encoded)))
            model = object.__new__(SeriesModel)
            model.__dict__.update(state)
            store[sk] = model
        except Exception as e:
            influxdb3_local.warn(f"[{task_id}] Failed to restore {sk}: {e}")

    if store:
        influxdb3_local.cache.put(ModelStore.CACHE_KEY, store)
        influxdb3_local.info(
            f"[{task_id}] Restored {len(store)} anomaly detector models from checkpoints"
        )


def detector_maybe_checkpoint(influxdb3_local, config: dict, task_id: str):
    """Periodically pickle all models and write to _system.model_checkpoints."""
    if not detector_should_checkpoint(
        influxdb3_local, config["checkpoint_interval_seconds"]
    ):
        return

    store = influxdb3_local.cache.get(ModelStore.CACHE_KEY)
    if store is None or len(store) == 0:
        detector_mark_checkpoint_done(influxdb3_local)
        return

    max_field_chars = 60_000  # Stay under 64KB string field limit
    min_obs = config["min_checkpoint_observations"]

    count = 0
    skipped = 0
    for series_key, model in store.items():
        if model.observation_count < min_obs:
            skipped += 1
            continue
        try:
            pickled = pickle.dumps(model.__dict__)
            compressed = zlib.compress(pickled)
            encoded = base64.b64encode(compressed).decode("ascii")

            # Split into chunks if needed
            if len(encoded) <= max_field_chars:
                chunks = [encoded]
            else:
                chunks = [
                    encoded[i : i + max_field_chars]
                    for i in range(0, len(encoded), max_field_chars)
                ]

            chunk_total = len(chunks)
            ts = time.time_ns()

            for chunk_index, chunk_data in enumerate(chunks):
                builder = LineBuilder("_system.model_checkpoints")
                builder.tag("plugin", DETECTOR_PLUGIN_NAME)
                builder.tag("series_key", series_key)
                builder.string_field("model_data", chunk_data)
                builder.string_field("model_type", DETECTOR_MODEL_TYPE)
                builder.int64_field("observation_count", model.observation_count)
                builder.int64_field("checkpoint_size_bytes", len(compressed))
                builder.tag("chunk_index", str(chunk_index))
                builder.tag("chunk_total", str(chunk_total))
                builder.time_ns(ts)
                influxdb3_local.write_sync(builder, no_sync=True)

            count += 1
        except Exception as e:
            influxdb3_local.warn(f"[{task_id}] Failed to checkpoint {series_key}: {e}")

    detector_mark_checkpoint_done(influxdb3_local)
    msg = f"[{task_id}] Checkpointed {count} anomaly detector models"
    if skipped:
        msg += f", skipped {skipped} immature"
    influxdb3_local.info(msg)


def process_writes(influxdb3_local, table_batches: list, args: dict | None = None):
    """
    Anomaly Detector Plugin — Detects anomalies in incoming time-series data
    using adaptive detectors driven by profiler recommendations.

    Triggered on each WAL flush. For every numeric field in every row:
    1. Looks up or creates a per-series model
    2. Auto-tunes detector mode from profiler recommendations (or uses config overrides)
    3. Scores the value with active detectors
    4. If anomaly detected, writes the result to _anomalies.{table}

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

        # ---- Restore models from checkpoint if cache is empty ----
        detector_maybe_restore(influxdb3_local, config, task_id)

        # ---- Process each table batch ----
        total_rows = 0
        total_anomalies = 0

        for table_batch in table_batches:
            table_name = table_batch["table_name"]
            rows = table_batch["rows"]

            # Skip excluded tables
            if table_name in config["exclude_tables"]:
                continue

            # Skip internal tables to avoid feedback loops
            if table_name.startswith("_"):
                continue

            if not rows:
                continue

            # Identify tag vs field columns from the first row
            # Type detection: strings → tags (unless in string_fields), numeric → fields
            sample_row = rows[0]
            tag_names = []
            numeric_fields = []
            include_fields = config["include_fields"]
            for col, val in sample_row.items():
                if col == "time":
                    continue
                if isinstance(val, str):
                    if col not in config["string_fields"]:
                        tag_names.append(col)
                elif isinstance(val, (int, float)) and not isinstance(val, bool):
                    if col in config["exclude_fields"]:
                        continue
                    if include_fields and col not in include_fields:
                        continue
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
                    if isinstance(value, float) and (
                        math.isnan(value) or math.isinf(value)
                    ):
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
                        influxdb3_local,
                        config["max_series"],
                        series_key,
                        config,
                        task_id,
                    )

                    # Determine threshold and detector mode
                    threshold = config["rolling_std_threshold"]
                    seasonal_fading = None
                    tuned_fading_factor = None
                    tuned_seasonal_period = None

                    # Always consult profiler when auto_tune is enabled — explicit
                    # detector_mode only overrides the mode itself, not threshold /
                    # fading_factor / seasonal_period which still come from the profile.
                    tuned = None
                    if config["auto_tune"]:
                        tuned = TuneCache.get_params(
                            influxdb3_local,
                            table_name,
                            field_name,
                            row,
                            tag_names,
                            series_key,
                            model.observation_count,
                            config["tune_refresh_interval"],
                            task_id,
                        )

                    if tuned:
                        threshold = tuned["threshold"]
                        seasonal_fading = tuned["seasonal_fading"]
                        tuned_fading_factor = tuned["fading_factor"]
                        tuned_seasonal_period = tuned["seasonal_period"]
                        profiler_mode = tuned["detector_mode"]
                    else:
                        profiler_mode = model.detector_mode

                    # Priority: explicit detector_mode > profiler recommendation > model default
                    if config["detector_mode"]:
                        detector_mode = config["detector_mode"]
                    else:
                        detector_mode = profiler_mode

                    # Explicit seasonal_period config overrides profiler
                    if config["seasonal_period"]:
                        tuned_seasonal_period = config["seasonal_period"]

                    # Apply user overrides for seasonal/adwin
                    if config["enable_seasonal"] == "true":
                        if "seasonal" not in detector_mode:
                            detector_mode = detector_mode + " seasonal"
                    elif config["enable_seasonal"] == "false":
                        detector_mode = detector_mode.replace("seasonal", "").strip()
                        if not detector_mode:
                            detector_mode = "zscore_conservative"

                    if config["enable_adwin"] == "true":
                        if "adwin" not in detector_mode:
                            detector_mode = detector_mode + " adwin"
                    elif config["enable_adwin"] == "false":
                        detector_mode = detector_mode.replace("adwin", "").strip()
                        if not detector_mode:
                            detector_mode = "zscore_conservative"

                    model.set_detector_mode(
                        detector_mode,
                        seasonal_fading,
                        fading_factor=tuned_fading_factor,
                        seasonal_period=tuned_seasonal_period,
                    )

                    # Score and update
                    timestamp_ns = row.get("time", None)
                    result = model.update_and_score(
                        value,
                        threshold,
                        timestamp_ns=timestamp_ns,
                        seasonal_threshold=config["seasonal_threshold"],
                        min_seasonal_observations=config["min_seasonal_observations"],
                        min_rolling_observations=config["min_rolling_observations"],
                        combination_mode=config["combination_mode"],
                    )

                    if result["is_anomaly"]:
                        total_anomalies += 1
                        anomaly_builders.append(
                            build_anomaly_line(
                                table_name, field_name, row, tag_names, result
                            )
                        )

                        if config["log_anomalies"]:
                            rolling_dev = result.get("rolling_deviation", 0)
                            log_parts = [
                                f"[{task_id}] ANOMALY: {series_key}",
                                f"value={value} mode={detector_mode}",
                                f"rolling_dev={rolling_dev:.2f}",
                            ]
                            if "seasonal" in detector_mode:
                                seasonal_dev = result.get("seasonal_deviation", 0)
                                seasonal_bucket = result.get("seasonal_bucket", "n/a")
                                log_parts.append(
                                    f"seasonal_dev={seasonal_dev:.2f} bucket={seasonal_bucket}"
                                )
                            if "adwin" in detector_mode:
                                log_parts.append(
                                    f"drift={result.get('drift_detected', False)}"
                                )
                            influxdb3_local.info(" ".join(log_parts))

            # Write anomalies for this table
            if anomaly_builders:
                for builder in anomaly_builders:
                    influxdb3_local.write_sync(builder, no_sync=True)

        # ---- Checkpoint models periodically ----
        detector_maybe_checkpoint(influxdb3_local, config, task_id)

        store = influxdb3_local.cache.get(ModelStore.CACHE_KEY)
        series_count = len(store) if store else 0
        tracked_fields = set()
        if store:
            for key in store:
                # series_key format: "table:tag1=v1,tag2=v2:field"
                parts = key.rsplit(":", 1)
                if len(parts) == 2:
                    tracked_fields.add(parts[-1])
        fields_str = ", ".join(sorted(tracked_fields)) if tracked_fields else "none"

        influxdb3_local.info(
            f"[{task_id}] Complete: {total_rows} rows processed, "
            f"{total_anomalies} anomalies detected, "
            f"{series_count} series tracked, fields: [{fields_str}]"
        )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Unexpected error: {e}")
        influxdb3_local.error(f"[{task_id}] Traceback: {traceback.format_exc()}")
