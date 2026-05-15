"""
{
    "plugin_type": ["scheduled", "onwrite"],
    "onwrite_args_config": [
        {
            "name": "include_tables",
            "example": "system_cpu.system_memory",
            "description": "Dot-separated list of table names to forecast. Required — forecaster is idle if not set.",
            "required": true
        },
        {
            "name": "include_fields",
            "example": "idle.used.available",
            "description": "Dot-separated list of field names to forecast. If omitted, all numeric fields in included tables are used.",
            "required": false
        },
        {
            "name": "max_series",
            "example": "50",
            "description": "Maximum number of unique series to track. Uses LRU eviction when exceeded. Defaults to 50.",
            "required": false
        },
        {
            "name": "snarimax_p",
            "example": "12",
            "description": "SNARIMAX autoregressive order. Defaults to 12.",
            "required": false
        },
        {
            "name": "snarimax_d",
            "example": "1",
            "description": "SNARIMAX differencing order. Defaults to 1.",
            "required": false
        },
        {
            "name": "snarimax_q",
            "example": "1",
            "description": "SNARIMAX moving average order. Defaults to 1.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "forecaster_config.toml",
            "description": "Path to TOML config file to override trigger arguments.",
            "required": false
        }
    ],
    "scheduled_args_config": [
        {
            "name": "max_series",
            "example": "50",
            "description": "Maximum number of unique series to track. Uses LRU eviction when exceeded. Defaults to 50.",
            "required": false
        },
        {
            "name": "min_observations",
            "example": "30",
            "description": "Minimum observations before producing forecasts. Defaults to 30.",
            "required": false
        },
        {
            "name": "default_horizon",
            "example": "12",
            "description": "Fallback forecast horizon if auto-profiler data unavailable. Defaults to 12.",
            "required": false
        },
        {
            "name": "forecast_target_seconds",
            "example": "3600",
            "description": "Target forecast window in seconds for auto-horizon calculation. Defaults to 3600 (1 hour).",
            "required": false
        },
        {
            "name": "snarimax_p",
            "example": "12",
            "description": "SNARIMAX autoregressive order. Defaults to 12.",
            "required": false
        },
        {
            "name": "snarimax_d",
            "example": "1",
            "description": "SNARIMAX differencing order. Defaults to 1.",
            "required": false
        },
        {
            "name": "snarimax_q",
            "example": "1",
            "description": "SNARIMAX moving average order. Defaults to 1.",
            "required": false
        },
        {
            "name": "log_forecasts",
            "example": "false",
            "description": "If 'true', logs forecast details. Defaults to 'false'.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "forecaster_config.toml",
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
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def forecaster_load_toml_config(influxdb3_local, args: dict, task_id: str) -> dict | None:
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


def forecaster_parse_args(args: dict | None) -> dict:
    """Parses trigger arguments with defaults."""
    if not args:
        args = {}
    return {
        "include_tables": set(args.get("include_tables", "").split(".")) - {""},
        "include_fields": set(args.get("include_fields", "").split(".")) - {""},
        "max_series": int(args.get("max_series", 50)),
        "min_observations": int(args.get("min_observations", 30)),
        "default_horizon": int(args.get("default_horizon", 12)),
        "forecast_target_seconds": int(args.get("forecast_target_seconds", 3600)),
        "snarimax_p": int(args.get("snarimax_p", 12)),
        "snarimax_d": int(args.get("snarimax_d", 1)),
        "snarimax_q": int(args.get("snarimax_q", 1)),
        "log_forecasts": str(args.get("log_forecasts", "false")).lower() == "true",
    }


# ---------------------------------------------------------------------------
# Series key helpers
# ---------------------------------------------------------------------------

def forecaster_build_series_key(table_name: str, row: dict, field_name: str, tag_names: list[str]) -> str:
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
# Model management
# ---------------------------------------------------------------------------

class SeriesForecaster:
    """Holds a SNARIMAX model and metadata for a single series."""

    def __init__(self, p: int = 12, d: int = 1, q: int = 1):
        from river import time_series

        self.model = time_series.SNARIMAX(p=p, d=d, q=q)
        self.observation_count = 0
        self.last_timestamp_ns = None
        self.prev_timestamp_ns = None  # for computing write interval
        self.table_name = ""
        self.field_name = ""
        self.tag_data = {}  # tag_name -> value, for output

    def learn(self, value: float, timestamp_ns: int | None = None):
        """Feed one observation to the model."""
        self.observation_count += 1
        self.model.learn_one(value)

        # Track timestamps for write interval estimation
        if timestamp_ns is not None:
            self.prev_timestamp_ns = self.last_timestamp_ns
            self.last_timestamp_ns = timestamp_ns

    def get_write_interval_ns(self) -> int | None:
        """Estimated write interval in nanoseconds from last two observations."""
        if self.prev_timestamp_ns is not None and self.last_timestamp_ns is not None:
            interval = self.last_timestamp_ns - self.prev_timestamp_ns
            if interval > 0:
                return interval
        return None

    def can_forecast(self, min_observations: int) -> bool:
        """Returns True if the model has enough data to forecast."""
        return self.observation_count >= min_observations

    def forecast(self, horizon: int) -> list[float] | None:
        """
        Produce a multi-step forecast. Returns list of predicted values,
        or None if forecast fails.
        """
        try:
            return self.model.forecast(horizon=horizon)
        except Exception:
            return None


class ForecasterStore:
    """LRU cache of SeriesForecaster instances, stored in influxdb3_local.cache."""

    CACHE_KEY = "forecaster:models"

    @staticmethod
    def get_or_create(influxdb3_local, max_series: int, series_key: str,
                      config: dict, task_id: str) -> SeriesForecaster:
        """Gets an existing model or creates a new one. Enforces LRU eviction."""
        store: OrderedDict = influxdb3_local.cache.get(ForecasterStore.CACHE_KEY)
        if store is None:
            store = OrderedDict()

        if series_key in store:
            store.move_to_end(series_key)
        else:
            if len(store) >= max_series:
                evicted_key, _ = store.popitem(last=False)
                influxdb3_local.warn(
                    f"[{task_id}] LRU eviction: dropped forecast model for '{evicted_key}'"
                )

            store[series_key] = SeriesForecaster(
                p=config["snarimax_p"],
                d=config["snarimax_d"],
                q=config["snarimax_q"],
            )

        influxdb3_local.cache.put(ForecasterStore.CACHE_KEY, store)
        return store[series_key]

    @staticmethod
    def get_all(influxdb3_local) -> OrderedDict | None:
        """Returns the full model store, or None if empty."""
        return influxdb3_local.cache.get(ForecasterStore.CACHE_KEY)


# ---------------------------------------------------------------------------
# Auto-horizon
# ---------------------------------------------------------------------------

def forecaster_get_horizon(influxdb3_local, table_name: str, field_name: str,
                           forecaster: "SeriesForecaster", config: dict,
                           task_id: str) -> int:
    """
    Determines the forecast horizon for a series.
    Priority: auto-profiler write_interval -> model's own interval estimate -> default.
    Target: forecast_target_seconds worth of data.
    Clamped to [6, 72].
    """
    write_interval_s = None

    # Try auto-profiler first
    try:
        results = influxdb3_local.query(
            "SELECT write_interval_seconds FROM \"_meta.series_profiles\" "
            f"WHERE source_table = '{table_name}' AND field_name = '{field_name}' "
            "ORDER BY time DESC LIMIT 1"
        )
        if results and len(results) > 0:
            val = results[0].get("write_interval_seconds")
            if val is not None and float(val) > 0:
                write_interval_s = float(val)
    except Exception:
        pass  # Table may not exist yet

    # Fallback to model's own estimate
    if write_interval_s is None:
        interval_ns = forecaster.get_write_interval_ns()
        if interval_ns is not None:
            write_interval_s = interval_ns / 1e9

    # Compute horizon or use default
    if write_interval_s is not None and write_interval_s > 0:
        horizon = round(config["forecast_target_seconds"] / write_interval_s)
        return max(6, min(72, horizon))

    return config["default_horizon"]


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def forecaster_build_forecast_line(table_name: str, field_name: str,
                                   tag_names: list[str], tag_data: dict,
                                   step: int, horizon_total: int,
                                   forecast_value: float, observations: int,
                                   forecast_time_ns: int):
    """Builds a LineBuilder for one forecast step."""
    builder = LineBuilder(f"_forecasts.{table_name}")

    # Preserve original tags
    for tag in sorted(tag_names):
        val = tag_data.get(tag)
        if val is not None:
            builder.tag(tag, str(val))

    # Add forecast-specific tags
    builder.tag("field_name", field_name)
    builder.tag("horizon_step", str(step))

    # Set the future timestamp
    builder.time_ns(forecast_time_ns)

    # Fields
    builder.float64_field("forecast_value", forecast_value)
    builder.string_field("model", "snarimax")
    builder.int64_field("horizon_total", horizon_total)
    builder.int64_field("observations", observations)

    return builder


# ---------------------------------------------------------------------------
# WAL trigger — Learning
# ---------------------------------------------------------------------------

def process_writes(influxdb3_local, table_batches: list, args: dict | None = None):
    """
    Forecaster Plugin — WAL Trigger (Learning)

    Fires on each WAL flush. For every numeric field in every row,
    calls SNARIMAX.learn_one(y) to keep forecast models current.
    No output is written — learning only.
    """
    task_id = str(uuid.uuid4())[:8]

    try:
        import river  # noqa: F401
    except ImportError:
        influxdb3_local.error(
            f"[{task_id}] River ML library is not installed. "
            "Run: influxdb3 install package river"
        )
        return

    try:
        # Load config
        if args:
            args = forecaster_load_toml_config(influxdb3_local, args, task_id)
            if args is None:
                return
        config = forecaster_parse_args(args)

        # Require include_tables to be configured
        if not config["include_tables"]:
            influxdb3_local.info(
                f"[{task_id}] No include_tables configured — forecaster idle. "
                "Set include_tables to specify which tables to forecast."
            )
            return

        total_learned = 0

        for table_batch in table_batches:
            table_name = table_batch["table_name"]
            rows = table_batch["rows"]

            # Only process tables in the include list
            if table_name not in config["include_tables"]:
                continue
            if not rows:
                continue

            # Identify tags vs numeric fields
            sample_row = rows[0]
            tag_names = []
            numeric_fields = []
            include_fields = config["include_fields"]
            for col, val in sample_row.items():
                if col == "time":
                    continue
                if isinstance(val, str):
                    tag_names.append(col)
                elif isinstance(val, (int, float)) and not isinstance(val, bool):
                    # If include_fields is set, only include those fields
                    if include_fields and col not in include_fields:
                        continue
                    numeric_fields.append(col)

            if not numeric_fields:
                continue

            for row in rows:
                timestamp_ns = row.get("time", None)

                for field_name in numeric_fields:
                    value = row.get(field_name, None)
                    if value is None:
                        continue
                    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                        continue

                    value = float(value)

                    series_key = forecaster_build_series_key(
                        table_name, row, field_name, tag_names
                    )
                    model = ForecasterStore.get_or_create(
                        influxdb3_local, config["max_series"],
                        series_key, config, task_id
                    )

                    # Store metadata for scheduled trigger output
                    model.table_name = table_name
                    model.field_name = field_name
                    model.tag_data = {tag: row.get(tag) for tag in tag_names if row.get(tag) is not None}

                    model.learn(value, timestamp_ns)
                    total_learned += 1

        if total_learned > 0:
            influxdb3_local.info(
                f"[{task_id}] Forecaster learned from {total_learned} observations"
            )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Unexpected error in process_writes: {e}")
        influxdb3_local.error(f"[{task_id}] Traceback: {traceback.format_exc()}")


# ---------------------------------------------------------------------------
# Scheduled trigger — Forecast Output
# ---------------------------------------------------------------------------

def process_scheduled_call(influxdb3_local, call_time, args=None):
    """
    Forecaster Plugin — Scheduled Trigger (Forecast Output)

    Runs every 15 minutes. For each warmed-up forecast model:
    1. Determines the forecast horizon (auto or default)
    2. Calls model.forecast(horizon) for multi-step prediction
    3. Writes each forecast step to _forecasts.{table}
    """
    task_id = str(uuid.uuid4())[:8]

    try:
        import river  # noqa: F401
    except ImportError:
        influxdb3_local.error(
            f"[{task_id}] River ML library is not installed. "
            "Run: influxdb3 install package river"
        )
        return

    influxdb3_local.info(f"[{task_id}] Forecaster scheduled run starting")

    try:
        # Load config
        if args:
            args = forecaster_load_toml_config(influxdb3_local, args, task_id)
            if args is None:
                return
        config = forecaster_parse_args(args)

        # Get all models from cache
        store = ForecasterStore.get_all(influxdb3_local)
        if store is None or len(store) == 0:
            influxdb3_local.info(f"[{task_id}] No forecast models in cache yet")
            return

        total_forecasts = 0
        skipped_warmup = 0
        skipped_failed = 0

        for series_key, model in store.items():
            # Skip models still warming up
            if not model.can_forecast(config["min_observations"]):
                skipped_warmup += 1
                continue

            # Determine horizon
            horizon = forecaster_get_horizon(
                influxdb3_local, model.table_name, model.field_name,
                model, config, task_id
            )

            # Produce forecast
            predictions = model.forecast(horizon)
            if predictions is None:
                skipped_failed += 1
                influxdb3_local.warn(
                    f"[{task_id}] Forecast failed for {series_key}"
                )
                continue

            # Determine write interval for timestamp spacing
            write_interval_ns = model.get_write_interval_ns()
            if write_interval_ns is None:
                # Fallback: assume forecast_target_seconds / horizon
                write_interval_ns = int((config["forecast_target_seconds"] / horizon) * 1e9)

            # Base timestamp for forecasts
            base_time_ns = model.last_timestamp_ns
            if base_time_ns is None:
                # Use call_time if no timestamp available
                if isinstance(call_time, datetime):
                    if call_time.tzinfo is None:
                        base_time_ns = int(call_time.replace(tzinfo=timezone.utc).timestamp() * 1e9)
                    else:
                        base_time_ns = int(call_time.timestamp() * 1e9)
                else:
                    base_time_ns = int(call_time)

            # Parse tag names from model metadata for output
            tag_names = list(model.tag_data.keys())

            # Write each forecast step
            for step_idx, forecast_value in enumerate(predictions):
                # Skip NaN/Inf forecast values
                if isinstance(forecast_value, float) and (math.isnan(forecast_value) or math.isinf(forecast_value)):
                    continue

                step = step_idx + 1  # 1-based
                forecast_time_ns = base_time_ns + (step * write_interval_ns)

                builder = forecaster_build_forecast_line(
                    model.table_name, model.field_name,
                    tag_names, model.tag_data,
                    step, horizon,
                    forecast_value, model.observation_count,
                    forecast_time_ns
                )
                influxdb3_local.write(builder)
                total_forecasts += 1

            if config["log_forecasts"]:
                influxdb3_local.info(
                    f"[{task_id}] FORECAST: {series_key} "
                    f"horizon={horizon} obs={model.observation_count} "
                    f"values={[round(v, 2) for v in predictions[:3]]}..."
                )

        influxdb3_local.info(
            f"[{task_id}] Forecast run complete: {total_forecasts} forecast points written, "
            f"{skipped_warmup} warming up, {skipped_failed} failed"
        )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Unexpected error in process_scheduled_call: {e}")
        influxdb3_local.error(f"[{task_id}] Traceback: {traceback.format_exc()}")
