"""
{
    "plugin_type": ["onwrite"],
    "onwrite_args_config": [
        {
            "name": "include_tables",
            "example": "system_cpu system_memory",
            "description": "Space-separated list of table names to forecast (in TOML config use a list). Required — forecaster is idle if not set.",
            "required": true
        },
        {
            "name": "include_fields",
            "example": "idle used available",
            "description": "Space-separated list of field names to forecast (in TOML config use a list). If omitted, all numeric fields in included tables are used.",
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
            "description": "Fallback forecast horizon if write_interval estimate unavailable. Defaults to 12.",
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
            "example": "0",
            "description": "SNARIMAX autoregressive order. 0 = auto-tune from write frequency (~1 hour lookback, clamped 4-30). Defaults to 0 (auto).",
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
            "example": "river_forecaster_config.toml",
            "description": "Path to TOML config file to override trigger arguments.",
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
        },
        {
            "name": "calibration_count",
            "example": "30",
            "description": "Number of observations buffered before auto-finalizing SNARIMAX (choosing p from write frequency). Only used when snarimax_p=0 (auto). Larger = more reliable interval estimate, longer warm-up. Defaults to 30.",
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
import zlib
import tomllib
import traceback
import uuid
from collections import OrderedDict, defaultdict
from pathlib import Path

from river import stats


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------


def forecaster_load_toml_config(
    influxdb3_local, args: dict, task_id: str
) -> dict | None:
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


def _forecaster_parse_list_arg(value) -> set:
    """Parse a list argument: TOML list or space-separated string from trigger args."""
    if isinstance(value, list):
        return set(value) - {""}
    if isinstance(value, str):
        return set(value.split()) - {""}
    raise TypeError(f"Expected list or str, got {type(value).__name__}")


def forecaster_parse_args(args: dict | None) -> dict:
    """Parses trigger arguments with defaults."""
    if not args:
        args = {}

    def _pos_int(name: str, default: int, minimum: int = 1) -> int:
        try:
            val = int(args.get(name, default))
        except (TypeError, ValueError):
            val = default
        return max(minimum, val)

    return {
        "include_tables": _forecaster_parse_list_arg(args.get("include_tables", "")),
        "include_fields": _forecaster_parse_list_arg(args.get("include_fields", "")),
        "string_fields": _forecaster_parse_list_arg(args.get("string_fields", "")),
        "max_series": _pos_int("max_series", 50),
        "min_observations": _pos_int("min_observations", 30),
        "default_horizon": _pos_int("default_horizon", 12),
        "forecast_target_seconds": _pos_int("forecast_target_seconds", 3600),
        "snarimax_p": _pos_int("snarimax_p", 0, minimum=0),
        "snarimax_d": _pos_int("snarimax_d", 1, minimum=0),
        "snarimax_q": _pos_int("snarimax_q", 1, minimum=0),
        "log_forecasts": str(args.get("log_forecasts", "false")).lower() == "true",
        "checkpoint_interval_seconds": _pos_int("checkpoint_interval_seconds", 1800),
        "max_checkpoint_age_hours": _pos_int("max_checkpoint_age_hours", 24, minimum=0),
        "min_checkpoint_observations": _pos_int(
            "min_checkpoint_observations", 10, minimum=0
        ),
        "calibration_count": _pos_int("calibration_count", 30),
    }


def forecaster_recommend_p(write_interval_s: float | None) -> int:
    """
    Recommend SNARIMAX AR order (p) based on write frequency.
    Goal: capture ~1 hour of lag history.
    Clamped to [4, 30].
    """
    if write_interval_s is None or write_interval_s <= 0:
        return 12  # safe default
    target_lookback_s = 3600  # 1 hour of history
    p = round(target_lookback_s / write_interval_s)
    return max(4, min(30, p))


# ---------------------------------------------------------------------------
# Series key helpers
# ---------------------------------------------------------------------------


def forecaster_build_series_key(
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
# Model management
# ---------------------------------------------------------------------------


class SeriesForecaster:
    """Holds a SNARIMAX model and metadata for a single series.

    When p=0 (auto), enters a calibration phase: buffers observations and
    tracks write interval until calibration_count is reached, then creates
    the SNARIMAX model with auto-tuned p and replays the buffer.
    When p>0 (user-set), creates the model immediately.
    """

    def __init__(self, p: int = 0, d: int = 1, q: int = 1, calibration_count: int = 30):
        self._d = d
        self._q = q
        self._calibration_count = calibration_count
        self.observation_count = 0
        self.last_timestamp_ns = None
        self.table_name = ""
        self.field_name = ""
        self.tag_data = {}  # tag_name -> value, for output

        # Smoothed write interval tracking (EWMean, in seconds)
        self._write_interval_ew = None  # stats.EWMean, created after first interval

        # Forecast evaluation: compare predictions to actuals step-by-step
        self._pending_predictions = None  # list[float] from last forecast
        self._eval_step = 0  # next step index to evaluate
        self._eval_count = 0  # total evaluation comparisons
        self._mae = None  # river.metrics.MAE, created on first evaluation
        self._last_logged_eval_count = 0  # eval_count at last MAE log line

        if p > 0:
            # Explicit p — create model immediately, no calibration
            from river import time_series

            self.model = time_series.SNARIMAX(p=p, d=d, q=q)
            self._calibrating = False
            self._calibration_buffer = None
        else:
            # Auto-p — enter calibration phase
            self.model = None
            self._calibrating = True
            self._calibration_buffer = []  # list of (value, timestamp_ns)

    def learn(self, value: float, timestamp_ns: int | None = None):
        """Feed one observation to the model (or buffer during calibration)."""
        # Evaluate pending forecast prediction against this actual value
        if self._pending_predictions is not None and self._eval_step < len(
            self._pending_predictions
        ):
            predicted = self._pending_predictions[self._eval_step]
            if isinstance(predicted, (int, float)) and not (
                math.isnan(predicted) or math.isinf(predicted)
            ):
                from river import metrics

                if self._mae is None:
                    self._mae = metrics.MAE()
                self._mae.update(value, predicted)
                self._eval_count += 1
            self._eval_step += 1
            if self._eval_step >= len(self._pending_predictions):
                self._pending_predictions = None

        self.observation_count += 1

        # Track write interval with exponentially weighted mean
        if timestamp_ns is not None:
            if self.last_timestamp_ns is not None:
                interval_s = (timestamp_ns - self.last_timestamp_ns) / 1e9
                if 0 < interval_s < 86400:  # ignore nonsensical intervals
                    if self._write_interval_ew is None:
                        self._write_interval_ew = stats.EWMean(fading_factor=0.1)
                    self._write_interval_ew.update(interval_s)
            self.last_timestamp_ns = timestamp_ns

        if self._calibrating:
            self._calibration_buffer.append((value, timestamp_ns))
            return

        self.model.learn_one(value)

    def needs_finalization(self) -> bool:
        """Returns True if calibration phase is complete and model needs to be created."""
        return (
            self._calibrating
            and len(self._calibration_buffer) >= self._calibration_count
        )

    def finalize_calibration(self):
        """End calibration: create SNARIMAX with auto-tuned p and replay buffer."""
        from river import time_series

        write_interval = self.get_write_interval_s()
        p = forecaster_recommend_p(write_interval)

        self.model = time_series.SNARIMAX(p=p, d=self._d, q=self._q)

        # Replay buffered observations
        for value, _ in self._calibration_buffer:
            self.model.learn_one(value)

        self._calibration_buffer = None
        self._calibrating = False
        return p

    def get_write_interval_s(self) -> float | None:
        """Smoothed write interval in seconds, or None if not enough data."""
        if self._write_interval_ew is None:
            return None
        val = self._write_interval_ew.get()
        return val if val is not None and val > 0 else None

    def can_forecast(self, min_observations: int) -> bool:
        """Returns True if the model has enough data to forecast."""
        if self._calibrating:
            return False
        return self.observation_count >= min_observations

    def forecast(self, horizon: int) -> list[float] | None:
        """
        Produce a multi-step forecast. Returns list of predicted values,
        or None if forecast fails.
        """
        if self.model is None:
            return None
        try:
            return self.model.forecast(horizon=horizon)
        except Exception:
            return None

    def set_pending_predictions(self, predictions: list[float]):
        """Store forecast predictions for step-by-step evaluation against actuals."""
        self._pending_predictions = list(predictions)
        self._eval_step = 0

    def get_mae(self) -> float | None:
        """Returns current MAE value, or None if no evaluations yet."""
        if self._mae is None:
            return None
        return self._mae.get()


class ForecasterStore:
    """LRU cache of SeriesForecaster instances, stored in influxdb3_local.cache."""

    CACHE_KEY = "forecaster:models"

    @staticmethod
    def get_or_create(
        influxdb3_local, max_series: int, series_key: str, config: dict, task_id: str
    ) -> SeriesForecaster:
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

            # p=0 → calibration phase (auto-tune later), p>0 → immediate model
            store[series_key] = SeriesForecaster(
                p=config["snarimax_p"],
                d=config["snarimax_d"],
                q=config["snarimax_q"],
                calibration_count=config["calibration_count"],
            )

        influxdb3_local.cache.put(ForecasterStore.CACHE_KEY, store)
        return store[series_key]

    @staticmethod
    def get_all(influxdb3_local) -> OrderedDict | None:
        """Returns the full model store, or None if empty."""
        return influxdb3_local.cache.get(ForecasterStore.CACHE_KEY)


# ---------------------------------------------------------------------------
# Self-checkpoint and self-restore
# ---------------------------------------------------------------------------

FORECASTER_CHECKPOINT_TIME_KEY = "forecaster:last_checkpoint_time"
FORECASTER_PLUGIN_NAME = "river_forecaster"
FORECASTER_MODEL_TYPE = "SeriesForecaster"


def forecaster_should_checkpoint(influxdb3_local, interval_seconds: int) -> bool:
    """Returns True if enough time has elapsed since the last checkpoint."""
    last_time = influxdb3_local.cache.get(FORECASTER_CHECKPOINT_TIME_KEY)
    if last_time is None:
        return True
    return (time.time() - last_time) >= interval_seconds


def forecaster_mark_checkpoint_done(influxdb3_local):
    """Records the current time as the last checkpoint time."""
    influxdb3_local.cache.put(FORECASTER_CHECKPOINT_TIME_KEY, time.time())


def forecaster_maybe_restore(influxdb3_local, config: dict, task_id: str):
    """On empty cache, restore models from _system.model_checkpoints."""
    store = influxdb3_local.cache.get(ForecasterStore.CACHE_KEY)
    if store is not None:
        return  # Already have models

    max_age = config["max_checkpoint_age_hours"]
    try:
        results = influxdb3_local.query(
            f"SELECT time, series_key, chunk_index, model_data, chunk_total "
            f'FROM "_system.model_checkpoints" '
            f"WHERE plugin = '{FORECASTER_PLUGIN_NAME}' "
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
            model = object.__new__(SeriesForecaster)
            model.__dict__.update(state)
            # Reset per-restart state: stale predictions mustn't be scored
            # against new actuals; interval tracking restarts from the next
            # observation.
            model._pending_predictions = None
            model._eval_step = 0
            model.last_timestamp_ns = None
            store[sk] = model
        except Exception as e:
            influxdb3_local.warn(f"[{task_id}] Failed to restore {sk}: {e}")

    if store:
        influxdb3_local.cache.put(ForecasterStore.CACHE_KEY, store)
        influxdb3_local.info(
            f"[{task_id}] Restored {len(store)} forecaster models from checkpoints"
        )


def forecaster_maybe_checkpoint(influxdb3_local, config: dict, task_id: str):
    """Periodically pickle all models and write to _system.model_checkpoints."""
    if not forecaster_should_checkpoint(
        influxdb3_local, config["checkpoint_interval_seconds"]
    ):
        return

    store = influxdb3_local.cache.get(ForecasterStore.CACHE_KEY)
    if store is None or len(store) == 0:
        forecaster_mark_checkpoint_done(influxdb3_local)
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
                builder.tag("plugin", FORECASTER_PLUGIN_NAME)
                builder.tag("series_key", series_key)
                builder.tag("chunk_index", str(chunk_index))
                builder.tag("chunk_total", str(chunk_total))
                builder.string_field("model_data", chunk_data)
                builder.string_field("model_type", FORECASTER_MODEL_TYPE)
                builder.int64_field("observation_count", model.observation_count)
                builder.int64_field("checkpoint_size_bytes", len(compressed))
                builder.time_ns(ts)
                influxdb3_local.write_sync(builder, no_sync=True)

            count += 1
        except Exception as e:
            influxdb3_local.warn(f"[{task_id}] Failed to checkpoint {series_key}: {e}")

    forecaster_mark_checkpoint_done(influxdb3_local)
    msg = f"[{task_id}] Checkpointed {count} forecaster models"
    if skipped:
        msg += f", skipped {skipped} immature"
    influxdb3_local.info(msg)


# ---------------------------------------------------------------------------
# Auto-horizon
# ---------------------------------------------------------------------------


def forecaster_get_horizon(forecaster: "SeriesForecaster", config: dict) -> int:
    """
    Determines the forecast horizon for a series from the model's own
    smoothed write interval. Falls back to default_horizon if not yet
    available (e.g. just after restore from checkpoint, before the second
    observation has arrived). Target: forecast_target_seconds worth of data.
    """
    write_interval_s = forecaster.get_write_interval_s()

    if write_interval_s is not None and write_interval_s > 0:
        horizon = round(config["forecast_target_seconds"] / write_interval_s)
        return max(1, horizon)

    return config["default_horizon"]


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def forecaster_build_forecast_line(
    table_name: str,
    field_name: str,
    tag_names: list[str],
    tag_data: dict,
    step: int,
    horizon_total: int,
    forecast_value: float,
    observations: int,
    forecast_time_ns: int,
):
    """Builds a LineBuilder for one forecast step."""
    builder = LineBuilder(f"_forecasts.{table_name}")

    # Preserve original tags
    for tag in sorted(tag_names):
        val = tag_data.get(tag)
        if val is not None:
            builder.tag(tag, str(val))

    # Add forecast-specific tags
    builder.tag("field_name", field_name)
    builder.tag("model", "snarimax")

    # Set the future timestamp
    builder.time_ns(forecast_time_ns)

    # Fields
    builder.int64_field("horizon_step", step)
    builder.float64_field("forecast_value", forecast_value)
    builder.int64_field("horizon_total", horizon_total)
    builder.int64_field("observations", observations)

    return builder


# ---------------------------------------------------------------------------
# Forecast output (called from within WAL trigger when interval has elapsed)
# ---------------------------------------------------------------------------


def forecaster_produce_forecasts(influxdb3_local, config: dict, task_id: str):
    """
    Produces forecasts for models whose previous predictions have been
    fully consumed. Each model self-throttles — a new forecast is only
    produced when _pending_predictions is None.
    """
    store = ForecasterStore.get_all(influxdb3_local)
    if store is None or len(store) == 0:
        return

    total_forecasts = 0
    forecasted_series = 0
    skipped_warmup = 0
    skipped_pending = 0
    skipped_failed = 0
    horizon_min: int | None = None
    horizon_max: int | None = None
    tables_touched: set[str] = set()

    for series_key, model in store.items():
        # Skip models still warming up
        if not model.can_forecast(config["min_observations"]):
            skipped_warmup += 1
            continue

        # Skip models whose previous forecast hasn't been fully evaluated yet
        if model._pending_predictions is not None:
            skipped_pending += 1
            continue

        # Determine horizon
        horizon = forecaster_get_horizon(model, config)

        # Produce forecast
        predictions = model.forecast(horizon)
        if predictions is None:
            skipped_failed += 1
            influxdb3_local.warn(f"[{task_id}] Forecast failed for {series_key}")
            continue

        forecasted_series += 1
        tables_touched.add(f"_forecasts.{model.table_name}")
        if horizon_min is None or horizon < horizon_min:
            horizon_min = horizon
        if horizon_max is None or horizon > horizon_max:
            horizon_max = horizon

        # Determine write interval for timestamp spacing
        write_interval_s = model.get_write_interval_s()
        if write_interval_s is not None:
            write_interval_ns = int(write_interval_s * 1e9)
        else:
            # Fallback: assume forecast_target_seconds / horizon
            write_interval_ns = int((config["forecast_target_seconds"] / horizon) * 1e9)

        # Base timestamp for forecasts
        base_time_ns = model.last_timestamp_ns
        if base_time_ns is None:
            base_time_ns = int(time.time() * 1e9)

        # Parse tag names from model metadata for output
        tag_names = list(model.tag_data.keys())

        # Write each forecast step
        for step_idx, forecast_value in enumerate(predictions):
            # Skip NaN/Inf forecast values
            if isinstance(forecast_value, float) and (
                math.isnan(forecast_value) or math.isinf(forecast_value)
            ):
                continue

            step = step_idx + 1  # 1-based
            forecast_time_ns = base_time_ns + (step * write_interval_ns)

            builder = forecaster_build_forecast_line(
                model.table_name,
                model.field_name,
                tag_names,
                model.tag_data,
                step,
                horizon,
                forecast_value,
                model.observation_count,
                forecast_time_ns,
            )
            influxdb3_local.write_sync(builder, no_sync=True)
            total_forecasts += 1

        if config["log_forecasts"]:
            mae_value = model.get_mae()
            if (
                mae_value is not None
                and model._eval_count > model._last_logged_eval_count
            ):
                influxdb3_local.info(
                    f"[{task_id}] EVAL: {series_key} "
                    f"MAE={mae_value:.4f} ({model._eval_count} points)"
                )
                model._last_logged_eval_count = model._eval_count

        # Save predictions for step-by-step evaluation against future actuals
        model.set_pending_predictions(predictions)

        if config["log_forecasts"]:
            influxdb3_local.info(
                f"[{task_id}] FORECAST: {series_key} "
                f"horizon={horizon} obs={model.observation_count} "
                f"values={[round(v, 2) for v in predictions[:3]]}..."
            )

    if total_forecasts > 0 or skipped_failed > 0:
        if horizon_min is None:
            horizon_str = "n/a"
        elif horizon_min == horizon_max:
            horizon_str = str(horizon_min)
        else:
            horizon_str = f"{horizon_min}..{horizon_max}"
        tables_str = ",".join(sorted(tables_touched)) if tables_touched else "-"
        influxdb3_local.info(
            f"[{task_id}] Forecast run: {total_forecasts} points written, "
            f"series_total={len(store)} forecasted={forecasted_series} "
            f"horizon={horizon_str} tables=[{tables_str}], "
            f"{skipped_warmup} warming up, {skipped_pending} pending eval, "
            f"{skipped_failed} failed"
        )


# ---------------------------------------------------------------------------
# WAL trigger — Learning + Periodic Forecasting
# ---------------------------------------------------------------------------


def process_writes(influxdb3_local, table_batches: list, args: dict | None = None):
    """
    Forecaster Plugin — WAL Trigger

    Fires on each WAL flush. For every numeric field in every row,
    calls SNARIMAX.learn_one(y) to keep forecast models current.
    Each model self-throttles: a new forecast is produced only when
    the previous predictions have been fully consumed by incoming
    actuals. Forecasts are written to _forecasts.{table}.
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

        # ---- Restore models from checkpoint if cache is empty ----
        forecaster_maybe_restore(influxdb3_local, config, task_id)

        # Require include_tables to be configured
        if not config["include_tables"]:
            influxdb3_local.info(
                f"[{task_id}] No include_tables configured — forecaster idle. "
                "Set include_tables to specify which tables to forecast."
            )
            return

        total_learned = 0
        learned_series: set[str] = set()
        learned_tables: set[str] = set()

        for table_batch in table_batches:
            table_name = table_batch["table_name"]
            rows = table_batch["rows"]

            # Only process tables in the include list
            if table_name not in config["include_tables"]:
                continue
            if not rows:
                continue

            # Sort rows by timestamp to ensure temporal order for SNARIMAX
            rows = sorted(rows, key=lambda r: r.get("time", 0) or 0)

            # Identify tags vs numeric fields
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
                    if isinstance(value, float) and (
                        math.isnan(value) or math.isinf(value)
                    ):
                        continue

                    value = float(value)

                    series_key = forecaster_build_series_key(
                        table_name, row, field_name, tag_names
                    )

                    model = ForecasterStore.get_or_create(
                        influxdb3_local,
                        config["max_series"],
                        series_key,
                        config,
                        task_id,
                    )

                    # Store metadata for forecast output
                    model.table_name = table_name
                    model.field_name = field_name
                    model.tag_data = {
                        tag: row.get(tag)
                        for tag in tag_names
                        if row.get(tag) is not None
                    }

                    model.learn(value, timestamp_ns)
                    total_learned += 1
                    learned_series.add(series_key)
                    learned_tables.add(table_name)

                    # Finalize calibration phase when enough observations collected
                    if model.needs_finalization():
                        chosen_p = model.finalize_calibration()
                        write_interval = model.get_write_interval_s()
                        interval_str = (
                            f"{write_interval:.1f}s"
                            if write_interval is not None
                            else "unknown"
                        )
                        influxdb3_local.info(
                            f"[{task_id}] Calibration complete for {series_key}: "
                            f"p={chosen_p} (interval={interval_str})"
                        )

        if total_learned > 0:
            tables_str = ",".join(sorted(learned_tables)) if learned_tables else "-"
            influxdb3_local.info(
                f"[{task_id}] Forecaster learned from {total_learned} observations "
                f"(series={len(learned_series)} tables=[{tables_str}])"
            )

        # --- Forecast output (per-model self-throttling) ---
        forecaster_produce_forecasts(influxdb3_local, config, task_id)

        # --- Periodic checkpoint ---
        forecaster_maybe_checkpoint(influxdb3_local, config, task_id)

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Unexpected error in process_writes: {e}")
        influxdb3_local.error(f"[{task_id}] Traceback: {traceback.format_exc()}")
