"""
{
    "plugin_type": ["onwrite"],
    "onwrite_args_config": [
        {
            "name": "include_fields",
            "example": "temperature humidity",
            "description": "Space-separated list of numeric field names to profile. If set, only these fields are processed. If empty, all numeric fields are processed. In TOML config use a list: ['temperature', 'humidity'].",
            "required": false
        },
        {
            "name": "exclude_fields",
            "example": "status version",
            "description": "Space-separated list of field names to exclude from profiling. In TOML config use a list.",
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
            "name": "max_series",
            "example": "1000",
            "description": "Maximum number of unique series to track. Uses LRU eviction when exceeded. Defaults to 1000.",
            "required": false
        },
        {
            "name": "initial_fading_factor",
            "example": "0.3",
            "description": "Initial fading factor for exponentially weighted stats. The profiler adapts this automatically based on write frequency. Defaults to 0.3.",
            "required": false
        },
        {
            "name": "seasonal_period",
            "example": "weekly",
            "description": "Seasonal period for variance bucket tracking: 'hourly' (24 buckets, hour-of-day) or 'weekly' (168 buckets, hour-of-week). Defaults to 'hourly'.",
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
            "example": "river_auto_profiler_config.toml",
            "description": "Path to TOML config file. Supports absolute paths or relative paths (resolved via PLUGIN_DIR env var).",
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
            "description": "Skip checkpointing profiles with fewer observations than this. Avoids persisting cold-start profiles that would be rebuilt from scratch anyway. Defaults to 10.",
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


def load_profiler_toml_config(influxdb3_local, args: dict, task_id: str) -> dict | None:
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


def _parse_list_arg(value) -> set:
    """Parse a list argument: TOML list or space-separated string from trigger args."""
    if isinstance(value, list):
        return set(value) - {""}
    return set(str(value).split()) - {""}


def parse_profiler_args(args: dict | None) -> dict:
    """Parses trigger arguments with defaults."""
    if not args:
        args = {}
    seasonal_period = str(args.get("seasonal_period", "hourly")).lower()
    if seasonal_period not in ("hourly", "weekly"):
        seasonal_period = "hourly"
    return {
        "include_fields": _parse_list_arg(args.get("include_fields", "")),
        "exclude_fields": _parse_list_arg(args.get("exclude_fields", "")),
        "exclude_tables": _parse_list_arg(args.get("exclude_tables", "")),
        "string_fields": _parse_list_arg(args.get("string_fields", "")),
        "max_series": int(args.get("max_series", 1000)),
        "profile_write_interval": int(args.get("profile_write_interval", 50)),
        "min_observations": int(args.get("min_observations", 50)),
        "log_profiles": str(args.get("log_profiles", "false")).lower() == "true",
        "checkpoint_interval_seconds": int(
            args.get("checkpoint_interval_seconds", 1800)
        ),
        "max_checkpoint_age_hours": int(args.get("max_checkpoint_age_hours", 24)),
        "min_checkpoint_observations": int(args.get("min_checkpoint_observations", 10)),
        "initial_fading_factor": float(args.get("initial_fading_factor", 0.3)),
        "seasonal_period": seasonal_period,
    }


# ---------------------------------------------------------------------------
# Series key helpers
# ---------------------------------------------------------------------------


def build_profiler_series_key(
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
# Series profiling
# ---------------------------------------------------------------------------


class SeriesProfile:
    """Tracks streaming statistics for a single series (one field of one tag combo)."""

    WRITE_TIME_FALLBACK_SECONDS = (
        300  # write profile if 5 min elapsed even if count threshold not met
    )
    CALIBRATION_COUNT = 30  # observations to collect before creating EW stats
    EXCEEDANCE_WINDOW = 200  # calibrate threshold every N observations

    def __init__(
        self, initial_fading_factor: float = 0.3, seasonal_period: str = "hourly"
    ):
        self._initial_fading_factor = initial_fading_factor

        # Calibration phase — collect data before creating EW stats
        self._calibration_phase = True
        self._calibration_values = []  # list of (value, timestamp_ns)

        # EW stats — created after calibration with proper fading factor
        self._fading_factor = initial_fading_factor
        self.ew_mean = None
        self.ew_var = None
        self.ew_mean_fast = None  # short-term trend
        self.ew_mean_slow = None  # long-term trend

        # Non-EW stats (always active)
        self.min = stats.Min()
        self.max = stats.Max()
        self.count = 0
        self.skew = stats.Skew()
        self.kurtosis = stats.Kurtosis()

        # Write interval tracking (per-observation EWMean)
        self._last_timestamp_ns = None
        self._write_interval_ew = None  # stats.EWMean, created after first interval

        self.last_written_count = 0  # count at last profile write
        self.last_written_time = None  # wall-clock time at last profile write

        # Seasonal variance tracking (configurable period)
        self.seasonal_period = (
            seasonal_period  # "hourly" (24 buckets) or "weekly" (168 buckets)
        )
        self.seasonal_vars = {}  # bucket_key → stats.EWVar
        self.seasonal_counts = {}  # bucket_key → int

        # Exceedance rate calibration
        self.exceedance_threshold = 5.0  # calibrated over time
        self._exceedance_window_count = 0
        self._exceedance_outside_count = 0

    def _get_seasonal_bucket(self, dt) -> int:
        """Get seasonal bucket key from datetime. Hourly: 0-23, Weekly: 0-167."""
        if self.seasonal_period == "weekly":
            return dt.weekday() * 24 + dt.hour  # 0-167
        return dt.hour  # 0-23

    def _get_seasonal_bucket_count(self) -> int:
        """Total buckets for current seasonal period."""
        return 168 if self.seasonal_period == "weekly" else 24

    def _update_seasonal(self, value: float, timestamp_ns: int):
        """Update seasonal variance bucket for the given timestamp."""
        try:
            dt = datetime.fromtimestamp(int(timestamp_ns) / 1e9, tz=timezone.utc)
            bucket = self._get_seasonal_bucket(dt)
            if bucket not in self.seasonal_vars:
                sf = recommend_seasonal_fading(self.get_avg_write_interval())
                self.seasonal_vars[bucket] = stats.EWVar(fading_factor=sf)
                self.seasonal_counts[bucket] = 0
            self.seasonal_vars[bucket].update(value)
            self.seasonal_counts[bucket] += 1
        except (TypeError, ValueError, OSError):
            pass

    def _update_exceedance(self, value: float):
        """Track exceedance rate and calibrate threshold."""
        mean = self.ew_mean.get() if self.ew_mean else None
        var = self.ew_var.get() if self.ew_var else None
        if mean is None or var is None or var <= 0:
            return
        std = math.sqrt(var)
        if abs(value - mean) > self.exceedance_threshold * std:
            self._exceedance_outside_count += 1
        self._exceedance_window_count += 1

        if self._exceedance_window_count >= self.EXCEEDANCE_WINDOW:
            rate = self._exceedance_outside_count / self._exceedance_window_count
            target = 0.01  # 1% anomaly rate
            if rate > target * 2:
                self.exceedance_threshold = min(10.0, self.exceedance_threshold + 0.5)
            elif rate < target / 2:
                self.exceedance_threshold = max(2.5, self.exceedance_threshold - 0.5)
            self._exceedance_window_count = 0
            self._exceedance_outside_count = 0

    def _finalize_calibration(self):
        """End calibration phase: create EW stats with calibrated fading factor and replay data."""
        fading = recommend_fading_factor(self.get_avg_write_interval())
        self._fading_factor = fading
        self.ew_mean = stats.EWMean(fading_factor=fading)
        self.ew_var = stats.EWVar(fading_factor=fading)
        self.ew_mean_fast = stats.EWMean(fading_factor=0.3)
        self.ew_mean_slow = stats.EWMean(fading_factor=0.05)

        # Replay calibration data through EW stats and seasonal tracking
        for value, ts_ns in self._calibration_values:
            self.ew_mean.update(value)
            self.ew_var.update(value)
            self.ew_mean_fast.update(value)
            self.ew_mean_slow.update(value)
            if ts_ns is not None:
                self._update_seasonal(value, ts_ns)

        # Initialize exceedance threshold from initial classification
        cv = self.get_cv()
        pattern = classify_pattern(self)
        self.exceedance_threshold = recommend_threshold(cv, pattern["pattern_label"])

        self._calibration_values = None  # free memory
        self._calibration_phase = False

    def update(self, value: float, timestamp_ns: int | None = None):
        """Update all stats with a new observation."""
        self.count += 1

        # Write interval tracking (per-observation)
        if timestamp_ns is not None and self._last_timestamp_ns is not None:
            interval_s = (timestamp_ns - self._last_timestamp_ns) / 1e9
            if 0 < interval_s < 86400:
                if self._write_interval_ew is None:
                    self._write_interval_ew = stats.EWMean(fading_factor=0.1)
                self._write_interval_ew.update(interval_s)
        if timestamp_ns is not None:
            self._last_timestamp_ns = timestamp_ns

        # Non-EW stats — always active
        self.min.update(value)
        self.max.update(value)
        self.skew.update(value)
        self.kurtosis.update(value)

        if self._calibration_phase:
            self._calibration_values.append((value, timestamp_ns))
            if self.count >= self.CALIBRATION_COUNT:
                self._finalize_calibration()
            return

        # EW stats
        self.ew_mean.update(value)
        self.ew_var.update(value)
        self.ew_mean_fast.update(value)
        self.ew_mean_slow.update(value)

        # Exceedance rate calibration
        self._update_exceedance(value)

        # Seasonal variance tracking
        if timestamp_ns is not None:
            self._update_seasonal(value, timestamp_ns)

    def get_cv(self) -> float:
        """Coefficient of variation = std / |mean|. Returns 0 if stats not ready or mean is 0."""
        if self.ew_mean is None or self.ew_var is None:
            return 0.0
        mean = self.ew_mean.get()
        var = self.ew_var.get()
        if mean is None or mean == 0 or var is None or var < 0:
            return 0.0
        std = math.sqrt(var) if var > 0 else 0.0
        return std / abs(mean) if abs(mean) > 0 else 0.0

    def get_avg_write_interval(self) -> float | None:
        """Average write interval in seconds, or None if not enough data."""
        if self._write_interval_ew is None:
            return None
        return self._write_interval_ew.get()

    def should_write(self, profile_write_interval: int) -> bool:
        """Returns True if it's time to write the profile to the database."""
        if self._calibration_phase:
            return False
        if (self.count - self.last_written_count) >= profile_write_interval:
            return True
        # Time-based fallback for slow-arriving data
        if self.last_written_time is not None:
            if (
                time.time() - self.last_written_time
            ) >= self.WRITE_TIME_FALLBACK_SECONDS:
                return self.count > self.last_written_count
        return False

    def mark_written(self):
        """Mark the profile as just written."""
        self.last_written_count = self.count
        self.last_written_time = time.time()


# ---------------------------------------------------------------------------
# Tuning rules
# ---------------------------------------------------------------------------


def compute_seasonality_strength(profile: SeriesProfile) -> float:
    """Compute seasonality strength (0.0-1.0) from seasonal variance buckets."""
    total_buckets = profile._get_seasonal_bucket_count()
    min_qualified = max(6, total_buckets // 4)  # at least 25% of buckets

    qualified = []
    for bucket, count in profile.seasonal_counts.items():
        if count >= 2:
            var_obj = profile.seasonal_vars.get(bucket)
            if var_obj is not None:
                v = var_obj.get()
                if v is not None and v >= 0:
                    qualified.append(v)
    if len(qualified) < min_qualified:
        return 0.0
    mean_var = sum(qualified) / len(qualified)
    if mean_var <= 0:
        return 0.0
    std_var = math.sqrt(sum((v - mean_var) ** 2 for v in qualified) / len(qualified))
    cv_of_vars = std_var / mean_var
    return min(1.0, cv_of_vars / 2.0)


def compute_trend_strength(profile: SeriesProfile) -> float:
    """
    Compute trend strength (0.0-1.0) from EW mean drift.
    Compares fast EW mean (recent) vs slow EW mean (long-term).
    Large divergence relative to std indicates a trend.
    """
    if profile.count < 20:
        return 0.0
    try:
        if (
            profile.ew_mean_fast is None
            or profile.ew_mean_slow is None
            or profile.ew_var is None
        ):
            return 0.0
        fast = profile.ew_mean_fast.get()
        slow = profile.ew_mean_slow.get()
        var = profile.ew_var.get()
        if fast is None or slow is None or var is None or var <= 0:
            return 0.0
        std = math.sqrt(var)
        if std <= 0:
            return 0.0
        # Normalized drift: how many stds the fast mean has drifted from slow mean
        drift = abs(fast - slow) / std
        # Map drift to 0-1 range: drift of 1 std → 0.5, 2+ stds → ~1.0
        return min(1.0, drift / 2.0)
    except Exception:
        return 0.0


def classify_pattern(profile: SeriesProfile) -> dict:
    """Classify data pattern and recommend detector mode."""
    cv = profile.get_cv()
    seasonality = compute_seasonality_strength(profile)
    trend = compute_trend_strength(profile)

    kurtosis_val = 0.0
    try:
        k = profile.kurtosis.get()
        if k is not None:
            kurtosis_val = k
    except Exception:
        pass

    if seasonality > 0.4:
        label, mode = "seasonal", "zscore_conservative seasonal"
    elif trend > 0.7:
        label, mode = "trending", "zscore_conservative adwin"
    elif cv < 0.05:
        label, mode = "stable", "zscore_low"
    elif kurtosis_val > 5 or cv > 1.0:
        label, mode = "bursty", "zscore_adaptive"
    elif cv > 0.2:
        label, mode = "noisy", "zscore_high"
    else:
        label, mode = "stable", "zscore_low"

    return {
        "pattern_label": label,
        "recommended_detector_mode": mode,
        "seasonality_strength": seasonality,
        "trend_strength": trend,
    }


def recommend_threshold(cv: float, pattern_label: str = "") -> float:
    """Recommend rolling_std_threshold based on CV and pattern."""
    if pattern_label == "stable":
        return 3.5
    elif pattern_label == "trending":
        return 5.0
    elif pattern_label == "seasonal":
        return 5.0
    elif pattern_label == "bursty":
        return 7.0
    elif pattern_label == "noisy":
        if cv > 0.5:
            return 8.0
        return 6.0
    # Fallback (no pattern yet)
    if cv < 0.05:
        return 3.5
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


def recommend_seasonal_fading(avg_write_interval: float | None) -> float:
    """Recommend seasonal fading factor (needs longer memory than rolling)."""
    if avg_write_interval is None:
        return 0.1
    if avg_write_interval < 60:
        return 0.05
    elif avg_write_interval < 300:
        return 0.1
    else:
        return 0.2


# ---------------------------------------------------------------------------
# Profile storage
# ---------------------------------------------------------------------------


class ProfileStore:
    """Two-tier LRU cache: mature profiles are protected from eviction by immature ones."""

    CACHE_KEY = "auto_profiler:profiles"
    IMMATURE_KEY = "auto_profiler:immature_profiles"

    @staticmethod
    def get_or_create(
        influxdb3_local, max_series: int, series_key: str, task_id: str, config: dict
    ) -> SeriesProfile:
        """Gets an existing profile or creates a new one. Evicts immature profiles first."""
        mature: OrderedDict = influxdb3_local.cache.get(ProfileStore.CACHE_KEY)
        if mature is None:
            mature = OrderedDict()
        immature: OrderedDict = influxdb3_local.cache.get(ProfileStore.IMMATURE_KEY)
        if immature is None:
            immature = OrderedDict()

        min_obs = config["min_observations"]

        # Check mature store
        if series_key in mature:
            mature.move_to_end(series_key)
            influxdb3_local.cache.put(ProfileStore.CACHE_KEY, mature)
            return mature[series_key]

        # Check immature store
        if series_key in immature:
            immature.move_to_end(series_key)
            profile = immature[series_key]
            # Promote to mature if ready
            if profile.count >= min_obs:
                del immature[series_key]
                mature[series_key] = profile
                influxdb3_local.cache.put(ProfileStore.CACHE_KEY, mature)
            influxdb3_local.cache.put(ProfileStore.IMMATURE_KEY, immature)
            return profile

        # New series — evict if at capacity, prefer evicting immature
        total = len(mature) + len(immature)
        if total >= max_series:
            if immature:
                evicted_key, _ = immature.popitem(last=False)
            else:
                evicted_key, _ = mature.popitem(last=False)
            influxdb3_local.warn(
                f"[{task_id}] LRU eviction: dropped profile for '{evicted_key}'"
            )

        profile = SeriesProfile(
            initial_fading_factor=config["initial_fading_factor"],
            seasonal_period=config["seasonal_period"],
        )
        immature[series_key] = profile
        influxdb3_local.cache.put(ProfileStore.IMMATURE_KEY, immature)
        influxdb3_local.cache.put(ProfileStore.CACHE_KEY, mature)

        return profile


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def build_profile_line(
    table_name: str,
    field_name: str,
    row: dict,
    tag_names: list[str],
    profile: SeriesProfile,
    min_observations: int,
):
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

    mean_val = profile.ew_mean.get()
    var_val = profile.ew_var.get()
    std_val = math.sqrt(var_val) if var_val and var_val > 0 else 0.0

    builder.float64_field("value_mean", mean_val if mean_val is not None else 0.0)
    builder.float64_field("value_std", std_val)
    builder.float64_field(
        "value_min", profile.min.get() if profile.min.get() is not None else 0.0
    )
    builder.float64_field(
        "value_max", profile.max.get() if profile.max.get() is not None else 0.0
    )

    cv = profile.get_cv()
    builder.float64_field("coefficient_of_variation", cv)

    avg_interval = profile.get_avg_write_interval()
    if avg_interval is not None:
        builder.float64_field("write_interval_seconds", avg_interval)

    # Pattern classification
    pattern = classify_pattern(profile)
    pattern_label = pattern["pattern_label"]
    builder.string_field("pattern_label", pattern_label)
    builder.string_field(
        "recommended_detector_mode", pattern["recommended_detector_mode"]
    )
    builder.float64_field("seasonality_strength", pattern["seasonality_strength"])
    builder.float64_field("trend_strength", pattern["trend_strength"])

    # Additional stats
    skew_val = profile.skew.get()
    builder.float64_field("data_skewness", skew_val if skew_val is not None else 0.0)
    kurtosis_val = profile.kurtosis.get()
    builder.float64_field(
        "data_kurtosis", kurtosis_val if kurtosis_val is not None else 0.0
    )

    # Tuning recommendations (exceedance-calibrated threshold)
    builder.float64_field("recommended_threshold", profile.exceedance_threshold)
    builder.float64_field(
        "recommended_fading_factor", recommend_fading_factor(avg_interval)
    )
    builder.float64_field(
        "recommended_seasonal_fading", recommend_seasonal_fading(avg_interval)
    )

    # Maturity flag
    builder.bool_field("profile_mature", profile.count >= min_observations)

    # Seasonality maturity
    seasonal_buckets_filled = sum(1 for c in profile.seasonal_counts.values() if c >= 2)
    seasonal_buckets_needed = max(6, profile._get_seasonal_bucket_count() // 4)
    builder.bool_field(
        "seasonality_ready", seasonal_buckets_filled >= seasonal_buckets_needed
    )
    builder.int64_field("seasonal_buckets_filled", seasonal_buckets_filled)

    # Seasonal period info
    builder.string_field("seasonal_period", profile.seasonal_period)

    return builder


# ---------------------------------------------------------------------------
# Self-checkpoint and self-restore
# ---------------------------------------------------------------------------

PROFILER_CHECKPOINT_TIME_KEY = "auto_profiler:last_checkpoint_time"
PROFILER_PLUGIN_NAME = "river_auto_profiler"
PROFILER_MODEL_TYPE = "SeriesProfile"
PROFILER_CONFIG_CACHE_KEY = "auto_profiler:config"


def profiler_should_checkpoint(influxdb3_local, interval_seconds: int) -> bool:
    """Returns True if enough time has elapsed since the last checkpoint."""
    last_time = influxdb3_local.cache.get(PROFILER_CHECKPOINT_TIME_KEY)
    if last_time is None:
        return True
    return (time.time() - last_time) >= interval_seconds


def profiler_mark_checkpoint_done(influxdb3_local):
    """Records the current time as the last checkpoint time."""
    influxdb3_local.cache.put(PROFILER_CHECKPOINT_TIME_KEY, time.time())


def profiler_maybe_restore(influxdb3_local, config: dict, task_id: str):
    """On empty cache, restore profiles from _system.model_checkpoints."""
    if (
        influxdb3_local.cache.get(ProfileStore.CACHE_KEY) is not None
        or influxdb3_local.cache.get(ProfileStore.IMMATURE_KEY) is not None
    ):
        return  # Already have profiles

    max_age = config["max_checkpoint_age_hours"]
    try:
        results = influxdb3_local.query(
            f"SELECT time, series_key, model_data, chunk_index, chunk_total "
            f'FROM "_system.model_checkpoints" '
            f"WHERE plugin = '{PROFILER_PLUGIN_NAME}' "
            f"AND time > now() - interval '{max_age} hours' "
            f"ORDER BY time DESC"
        )
    except Exception:
        return  # Table doesn't exist yet

    if not results or len(results) == 0:
        return

    # Group chunks by series_key, keeping only chunks from the latest checkpoint
    # run per key (timestamp match) to avoid mixing partials from different runs.
    chunks_by_key = defaultdict(dict)
    totals_by_key = {}
    timestamps_by_key = {}
    for row in results:
        sk = row.get("series_key")
        if not sk:
            continue
        chunk_total = int(row.get("chunk_total", 1))
        chunk_index = int(row.get("chunk_index", 0))
        ts = row.get("time")
        if sk not in totals_by_key:
            totals_by_key[sk] = chunk_total
            timestamps_by_key[sk] = ts
        if ts != timestamps_by_key[sk]:
            continue  # chunk from a different checkpoint run
        if len(chunks_by_key[sk]) >= totals_by_key[sk]:
            continue
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
            profile = object.__new__(SeriesProfile)
            profile.__dict__.update(state)
            profile._last_timestamp_ns = None
            store[sk] = profile
        except Exception as e:
            influxdb3_local.warn(f"[{task_id}] Failed to restore {sk}: {e}")

    if store:
        min_obs = config["min_observations"]
        mature_store = OrderedDict()
        immature_store = OrderedDict()
        for sk, profile in store.items():
            if profile.count >= min_obs:
                mature_store[sk] = profile
            else:
                immature_store[sk] = profile
        influxdb3_local.cache.put(ProfileStore.CACHE_KEY, mature_store)
        influxdb3_local.cache.put(ProfileStore.IMMATURE_KEY, immature_store)
        influxdb3_local.info(
            f"[{task_id}] Restored {len(mature_store)} mature + "
            f"{len(immature_store)} immature profiles from checkpoints"
        )


def profiler_maybe_checkpoint(influxdb3_local, config: dict, task_id: str):
    """Periodically pickle all profiles and write to _system.model_checkpoints."""
    if not profiler_should_checkpoint(
        influxdb3_local, config["checkpoint_interval_seconds"]
    ):
        return

    mature = influxdb3_local.cache.get(ProfileStore.CACHE_KEY)
    immature = influxdb3_local.cache.get(ProfileStore.IMMATURE_KEY)
    all_profiles = {}
    if mature:
        all_profiles.update(mature)
    if immature:
        all_profiles.update(immature)

    if not all_profiles:
        profiler_mark_checkpoint_done(influxdb3_local)
        return

    min_obs = config["min_checkpoint_observations"]
    max_field_chars = 60_000  # Stay under 64KB string field limit

    count = 0
    skipped = 0
    for series_key, profile in all_profiles.items():
        if profile.count < min_obs:
            skipped += 1
            continue
        try:
            pickled = pickle.dumps(profile.__dict__)
            compressed = zlib.compress(pickled)
            encoded = base64.b64encode(compressed).decode("ascii")

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
                builder.tag("plugin", PROFILER_PLUGIN_NAME)
                builder.tag("series_key", series_key)
                builder.string_field("model_data", chunk_data)
                builder.string_field("model_type", PROFILER_MODEL_TYPE)
                builder.int64_field("observation_count", profile.count)
                builder.int64_field("checkpoint_size_bytes", len(compressed))
                builder.tag("chunk_index", str(chunk_index))
                builder.tag("chunk_total", str(chunk_total))
                builder.time_ns(ts)
                influxdb3_local.write_sync(builder, no_sync=True)

            count += 1
        except Exception as e:
            influxdb3_local.warn(f"[{task_id}] Failed to checkpoint {series_key}: {e}")

    profiler_mark_checkpoint_done(influxdb3_local)
    msg = f"[{task_id}] Checkpointed {count} auto-profiler profiles"
    if skipped:
        msg += f", skipped {skipped} immature"
    influxdb3_local.info(msg)


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

    influxdb3_local.info(f"[{task_id}] Auto-Profiler plugin triggered")

    try:
        # ---- Use cached config or parse and cache ----
        config = influxdb3_local.cache.get(PROFILER_CONFIG_CACHE_KEY)
        if config is None:
            if args:
                args = load_profiler_toml_config(influxdb3_local, args, task_id)
                if args is None:
                    return
            config = parse_profiler_args(args)
            influxdb3_local.cache.put(PROFILER_CONFIG_CACHE_KEY, config, 60 * 60)
            influxdb3_local.info(f"[{task_id}] Config parsed and cached (TTL 1h)")

        # ---- Restore profiles from checkpoint if cache is empty ----
        profiler_maybe_restore(influxdb3_local, config, task_id)

        # ---- Process each table batch ----
        total_rows = 0
        profiles_written = 0
        errors = 0

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

            try:
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

                for row in rows:
                    total_rows += 1
                    try:
                        timestamp_ns = row.get("time", None)

                        for field_name in numeric_fields:
                            value = row.get(field_name, None)
                            if value is None:
                                continue

                            # Skip NaN/Inf
                            if isinstance(value, float) and (
                                math.isnan(value) or math.isinf(value)
                            ):
                                continue

                            value = float(value)

                            # Build series key and get/create profile
                            series_key = build_profiler_series_key(
                                table_name, row, field_name, tag_names
                            )
                            profile = ProfileStore.get_or_create(
                                influxdb3_local,
                                config["max_series"],
                                series_key,
                                task_id,
                                config,
                            )

                            # Update profile
                            profile.update(value, timestamp_ns)

                            # Write profile periodically
                            if profile.should_write(config["profile_write_interval"]):
                                builder = build_profile_line(
                                    table_name,
                                    field_name,
                                    row,
                                    tag_names,
                                    profile,
                                    config["min_observations"],
                                )
                                influxdb3_local.write_sync(builder, no_sync=True)
                                profile.mark_written()
                                profiles_written += 1

                                if config["log_profiles"]:
                                    cv = profile.get_cv()
                                    pat = classify_pattern(profile)
                                    influxdb3_local.info(
                                        f"[{task_id}] PROFILE: {series_key} "
                                        f"count={profile.count} cv={cv:.3f} "
                                        f"pattern={pat['pattern_label']} "
                                        f"rec_threshold={profile.exceedance_threshold:.1f} "
                                        f"rec_fading={recommend_fading_factor(profile.get_avg_write_interval())}"
                                    )
                    except Exception as e:
                        errors += 1
                        if errors <= 5:
                            influxdb3_local.warn(
                                f"[{task_id}] Error processing row in {table_name}: {e}"
                            )
            except Exception as e:
                errors += 1
                influxdb3_local.warn(
                    f"[{task_id}] Error processing table {table_name}: {e}"
                )

        # ---- Checkpoint profiles periodically ----
        profiler_maybe_checkpoint(influxdb3_local, config, task_id)

        if errors > 0:
            influxdb3_local.warn(
                f"[{task_id}] Complete: {total_rows} rows, "
                f"{profiles_written} profiles written, {errors} errors"
            )
        else:
            influxdb3_local.info(
                f"[{task_id}] Complete: {total_rows} rows, "
                f"{profiles_written} profiles written"
            )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Unexpected error: {e}")
        influxdb3_local.error(f"[{task_id}] Traceback: {traceback.format_exc()}")
