"""
{
    "plugin_type": ["onwrite"],
    "onwrite_args_config": [
        {
            "name": "input_measurement",
            "example": "signal",
            "description": "Table to filter. If omitted, every table the trigger fires for is filtered.",
            "required": false
        },
        {
            "name": "input_fields",
            "example": "value",
            "description": "Space-separated numeric fields to filter; each is filtered independently. Defaults to 'value'.",
            "required": false
        },
        {
            "name": "tag_keys",
            "example": "host region",
            "description": "Space-separated tag columns that define a series. Defaults to all string-valued columns except 'time' and the input fields.",
            "required": false
        },
        {
            "name": "design_type",
            "example": "preset",
            "description": "'preset' (SciPy-designed IIR) or 'manual' (SOS coefficients supplied via 'sos'). Defaults to 'preset'.",
            "required": false
        },
        {
            "name": "prototype",
            "example": "butter",
            "description": "Preset prototype: 'butter', 'cheby1', or 'bessel'. Defaults to 'butter'.",
            "required": false
        },
        {
            "name": "order",
            "example": "4",
            "description": "Filter order, 1-12. Band filters yield effective order 2N. Defaults to 4.",
            "required": false
        },
        {
            "name": "ripple",
            "example": "1.0",
            "description": "Passband ripple in dB, 0.01-80 (0.1-3 typical). Required for 'cheby1'; invalid otherwise.",
            "required": false
        },
        {
            "name": "filter_type",
            "example": "lowpass",
            "description": "'lowpass', 'highpass', 'bandpass', or 'bandstop'. Defaults to 'lowpass'.",
            "required": false
        },
        {
            "name": "fc",
            "example": "5.0",
            "description": "Convenience alias for the single cutoff (Hz) of lowpass/highpass. Invalid for band types or together with the parameter it maps to.",
            "required": false
        },
        {
            "name": "fc1",
            "example": "1.0",
            "description": "Lower cutoff (Hz). Required for highpass and band filters.",
            "required": false
        },
        {
            "name": "fc2",
            "example": "5.0",
            "description": "Upper cutoff (Hz). Required for lowpass and band filters.",
            "required": false
        },
        {
            "name": "bessel_norm",
            "example": "phase",
            "description": "Bessel normalization: 'phase', 'delay', or 'mag'. Defaults to 'phase'. Bessel only.",
            "required": false
        },
        {
            "name": "sos",
            "example": "[[0.1, 0.2, 0.1, 1.0, -0.5, 0.2]]",
            "description": "Manual second-order sections as JSON [[b0,b1,b2,a0,a1,a2], ...]. Required for design_type 'manual'; invalid otherwise.",
            "required": false
        },
        {
            "name": "sample_rate",
            "example": "10.0",
            "description": "Sample rate in Hz for preset design. If omitted, inferred per series from median inter-sample interval and frozen once enough samples are seen.",
            "required": false
        },
        {
            "name": "init_from_first_sample",
            "example": "true",
            "description": "Initialize filter state from the first sample to suppress the startup transient. Defaults to true.",
            "required": false
        },
        {
            "name": "output_target_database",
            "example": "filtered_db",
            "description": "Database to write filtered output to. Defaults to the trigger's database.",
            "required": false
        },
        {
            "name": "output_measurement",
            "example": "signal_filtered",
            "description": "Measurement to write filtered output to. Defaults to the source measurement.",
            "required": false
        },
        {
            "name": "output_field",
            "example": "smoothed",
            "description": "Base name override for the output field. Only valid when a single input field is configured.",
            "required": false
        },
        {
            "name": "field_prefix",
            "example": "flt_",
            "description": "Prefix for the output field name. Defaults to ''.",
            "required": false
        },
        {
            "name": "field_suffix",
            "example": "_filtered",
            "description": "Suffix for the output field name. Defaults to '_filtered'.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "config.toml",
            "description": "Path to a TOML file supplying all parameters; mutually exclusive with inline arguments. Relative paths resolve against PLUGIN_DIR.",
            "required": false
        }
    ]
}
"""

import hashlib
import json
import math
import tomllib
import uuid
from dataclasses import dataclass

try:
    import numpy as np
    from scipy import signal as sp_signal

    _IMPORT_ERROR = None
except ImportError as exc:  # engine without numpy/scipy installed
    np = None
    sp_signal = None
    _IMPORT_ERROR = str(exc)

try:
    # Shared helpers published by InfluxData; see influxdata-plugin-utils on PyPI.
    from influxdata_plugin_utils.config import load_plugin_config
    from influxdata_plugin_utils.parsing import (
        parse_bool,
        parse_delimited_list,
        parse_int,
    )
    from influxdata_plugin_utils.write import build_line, write_data

    _UTILS_IMPORT_ERROR = None
except ImportError as exc:  # engine without influxdata-plugin-utils installed
    load_plugin_config = None
    parse_bool = parse_delimited_list = parse_int = None
    build_line = write_data = None
    _UTILS_IMPORT_ERROR = str(exc)

try:
    from influxdb3_pe import LineBuilder  # type: ignore
except ImportError:  # the processing engine injects LineBuilder at runtime
    pass


# ---------------------------------------------------------------------------
# Registries and defaults
# ---------------------------------------------------------------------------

FILTER_TYPES = ("lowpass", "highpass", "bandpass", "bandstop")
BESSEL_NORMS = ("phase", "delay", "mag")
DESIGN_TYPES = ("preset", "manual")

WARMUP_MIN_INTERVALS = 8
WARMUP_MAX_TIMES = 64
CACHE_KEY_FMT = "signal_filter:{table}:{field}:{series_hash}"

DEFAULT_INPUT_FIELDS = ("value",)
DEFAULT_FIELD_SUFFIX = "_filtered"

_DESIGN_CACHE = {}  # (design key) -> (sos ndarray, coeff_hash)


class ConfigError(Exception):
    """Raised when trigger arguments fail validation."""


# ---------------------------------------------------------------------------
# Config parsing + validation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Config:
    input_measurement: str | None
    input_fields: tuple
    tag_keys: tuple | None
    design_type: str
    prototype: str
    order: int
    ripple: float | None
    filter_type: str
    fc1: float | None
    fc2: float | None
    bessel_norm: str
    sos: tuple | None
    sample_rate: float | None
    init_from_first_sample: bool
    output_target_database: str | None
    output_measurement: str | None
    output_field: str | None
    field_prefix: str
    field_suffix: str


def _to_float(name, value):
    try:
        result = float(value)
    except (TypeError, ValueError):
        raise ConfigError(f"'{name}' must be a number, got {value!r}") from None
    if not math.isfinite(result):
        raise ConfigError(f"'{name}' must be finite, got {value!r}")
    return result


def _to_int(name, value):
    try:
        return parse_int(value)
    except ValueError:
        raise ConfigError(f"'{name}' must be an integer, got {value!r}") from None


def _to_bool(name, value):
    try:
        return parse_bool(value)
    except ValueError:
        raise ConfigError(f"'{name}' must be a boolean, got {value!r}") from None


def _parse_manual_sos(raw):
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"'sos' is not valid JSON: {exc}") from None
    if not isinstance(raw, list) or not raw:
        raise ConfigError("'sos' must be a non-empty JSON array of sections")
    normalized = []
    for i, section in enumerate(raw):
        if not isinstance(section, list) or len(section) != 6:
            raise ConfigError(f"'sos' section {i} must have 6 coefficients [b0,b1,b2,a0,a1,a2]")
        try:
            coeffs = [float(c) for c in section]
        except (TypeError, ValueError):
            raise ConfigError(f"'sos' section {i} contains a non-numeric coefficient") from None
        if any(not math.isfinite(c) for c in coeffs):
            raise ConfigError(f"'sos' section {i} contains a non-finite coefficient")
        a0 = coeffs[3]
        if a0 == 0.0:
            raise ConfigError(f"'sos' section {i} has a0 == 0")
        normalized.append(tuple(c / a0 for c in coeffs))
    return tuple(normalized)


def _resolve_cutoffs(merged):
    """Apply the fc alias and per-band presence rules; returns (fc1, fc2)."""
    filter_type = merged.get("filter_type", "lowpass")
    fc = merged.get("fc")
    fc1 = merged.get("fc1")
    fc2 = merged.get("fc2")

    if fc is not None:
        if filter_type in ("bandpass", "bandstop"):
            raise ConfigError("'fc' is not valid for band filters; use 'fc1' and 'fc2'")
        if filter_type == "lowpass":
            if fc2 is not None:
                raise ConfigError("'fc' and 'fc2' are both set; use one")
            fc2 = fc
        else:  # highpass
            if fc1 is not None:
                raise ConfigError("'fc' and 'fc1' are both set; use one")
            fc1 = fc

    fc1 = _to_float("fc1", fc1) if fc1 is not None else None
    fc2 = _to_float("fc2", fc2) if fc2 is not None else None
    for name, value in (("fc1", fc1), ("fc2", fc2)):
        if value is not None and value <= 0:
            raise ConfigError(f"'{name}' must be > 0, got {value}")

    if filter_type == "lowpass":
        if fc2 is None:
            raise ConfigError("lowpass requires a cutoff: set 'fc2' (or 'fc')")
        if fc1 is not None:
            raise ConfigError("'fc1' is not used for lowpass")
    elif filter_type == "highpass":
        if fc1 is None:
            raise ConfigError("highpass requires a cutoff: set 'fc1' (or 'fc')")
        if fc2 is not None:
            raise ConfigError("'fc2' is not used for highpass")
    else:  # bandpass / bandstop
        if fc1 is None or fc2 is None:
            raise ConfigError(f"{filter_type} requires both 'fc1' and 'fc2'")
        if not fc1 < fc2:
            raise ConfigError(f"'fc1' must be < 'fc2', got {fc1} >= {fc2}")
    return fc1, fc2


def parse_config(args):
    """Validate trigger args, sourced either inline or entirely from a TOML file.

    Following the other influxdata plugins, ``config_file_path`` is mutually
    exclusive with inline arguments: when it is set the TOML file supplies every
    parameter, which keeps precedence explicit rather than silently merging.
    """
    args = dict(args or {})
    has_toml = bool(args.get("config_file_path"))
    if has_toml:
        extra = sorted(k for k in args if k != "config_file_path" and args[k] is not None)
        if extra:
            raise ConfigError(
                "'config_file_path' is mutually exclusive with inline trigger "
                f"arguments; also set: {', '.join(extra)}"
            )
    # load_plugin_config resolves the TOML path (PLUGIN_DIR -> INFLUXDB3_PLUGIN_DIR
    # -> parent of VIRTUAL_ENV) and layers the sources; "toml" vs "args" keeps the
    # two mutually exclusive rather than silently merging.
    try:
        settings = load_plugin_config(args, source="toml" if has_toml else "args")
    except FileNotFoundError:
        raise ConfigError(f"config file not found: {args['config_file_path']}") from None
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"config file is not valid TOML: {exc}") from None
    except ValueError as exc:  # plugin directory could not be resolved
        raise ConfigError(str(exc)) from None
    merged = {k.lower(): v for k, v in settings.as_dict().items()}

    design_type = str(merged.get("design_type", "preset"))
    if design_type not in DESIGN_TYPES:
        raise ConfigError(f"'design_type' must be one of {DESIGN_TYPES}, got {design_type!r}")

    input_fields = tuple(parse_delimited_list(merged.get("input_fields", "value"))) or DEFAULT_INPUT_FIELDS
    tag_keys_raw = merged.get("tag_keys")
    tag_keys = tuple(parse_delimited_list(tag_keys_raw)) if tag_keys_raw else None

    output_field = merged.get("output_field")
    if output_field is not None and len(input_fields) != 1:
        raise ConfigError("'output_field' is only valid with a single input field")

    filter_type = str(merged.get("filter_type", "lowpass"))
    if filter_type not in FILTER_TYPES:
        raise ConfigError(f"'filter_type' must be one of {FILTER_TYPES}, got {filter_type!r}")

    prototype = str(merged.get("prototype", "butter"))
    order = 4
    ripple = None
    bessel_norm = str(merged.get("bessel_norm", "phase"))
    fc1 = fc2 = None
    sos = None
    sample_rate = None

    if design_type == "manual":
        if merged.get("sos") is None:
            raise ConfigError("design_type 'manual' requires 'sos'")
        sos = _parse_manual_sos(merged["sos"])
    else:
        if merged.get("sos") is not None:
            raise ConfigError("'sos' is only valid with design_type 'manual'")
        if prototype not in PRESET_PROTOTYPES:
            raise ConfigError(
                f"'prototype' must be one of {tuple(PRESET_PROTOTYPES)}, got {prototype!r}"
            )
        order = _to_int("order", merged.get("order", 4))
        if not 1 <= order <= 12:
            raise ConfigError(f"'order' must be 1-12, got {order}")
        if bessel_norm not in BESSEL_NORMS:
            raise ConfigError(f"'bessel_norm' must be one of {BESSEL_NORMS}, got {bessel_norm!r}")
        if prototype == "cheby1":
            if merged.get("ripple") is None:
                raise ConfigError("'ripple' is required for prototype 'cheby1'")
            ripple = _to_float("ripple", merged["ripple"])
            if not 0.01 <= ripple <= 80:
                raise ConfigError(f"'ripple' must be 0.01-80 dB, got {ripple}")
        elif merged.get("ripple") is not None:
            raise ConfigError("'ripple' is only valid for prototype 'cheby1'")
        fc1, fc2 = _resolve_cutoffs(merged)
        if merged.get("sample_rate") is not None:
            sample_rate = _to_float("sample_rate", merged["sample_rate"])
            if sample_rate <= 0:
                raise ConfigError(f"'sample_rate' must be > 0, got {sample_rate}")

    return Config(
        input_measurement=str(merged["input_measurement"]) if merged.get("input_measurement") else None,
        input_fields=input_fields,
        tag_keys=tag_keys,
        design_type=design_type,
        prototype=prototype,
        order=order,
        ripple=ripple,
        filter_type=filter_type,
        fc1=fc1,
        fc2=fc2,
        bessel_norm=bessel_norm,
        sos=sos,
        sample_rate=sample_rate,
        init_from_first_sample=_to_bool(
            "init_from_first_sample", merged.get("init_from_first_sample", True)
        ),
        output_target_database=str(merged["output_target_database"])
        if merged.get("output_target_database")
        else None,
        output_measurement=str(merged["output_measurement"])
        if merged.get("output_measurement")
        else None,
        output_field=str(output_field) if output_field else None,
        field_prefix=str(merged.get("field_prefix", "")),
        field_suffix=str(merged.get("field_suffix", DEFAULT_FIELD_SUFFIX)),
    )


def resolve_output_field(cfg, input_field):
    return f"{cfg.field_prefix}{cfg.output_field or input_field}{cfg.field_suffix}"


def loop_hazard_fields(cfg):
    """Input fields whose resolved output name loops back onto themselves.

    The natural null-skip protection only works while the output field name
    differs from the input field name; a same-name write into the same
    measurement and database re-fires the trigger with a non-null input field.
    """
    if cfg.output_target_database is not None:
        return []
    if (
        cfg.output_measurement is not None
        and cfg.input_measurement is not None
        and cfg.output_measurement != cfg.input_measurement
    ):
        return []
    return [f for f in cfg.input_fields if resolve_output_field(cfg, f) == f]


# ---------------------------------------------------------------------------
# Sample-rate inference
# ---------------------------------------------------------------------------


def infer_sample_rate(times_ns):
    """fs = 1e9 / median(diff(unique sorted times)); None when < 2 distinct."""
    distinct = sorted(set(times_ns))
    if len(distinct) < 2:
        return None
    diffs = np.diff(np.asarray(distinct, dtype=np.float64))
    return 1e9 / float(np.median(diffs))


def merge_warmup_times(state, times_ns):
    """Accumulate distinct timestamps across commits, keeping the most recent."""
    previous = state.get("warmup_times", []) if state else []
    merged = sorted(set(previous) | set(times_ns))
    return merged[-WARMUP_MAX_TIMES:]


# ---------------------------------------------------------------------------
# Filter design
# ---------------------------------------------------------------------------


def _design_butter(cfg, wn, fs):
    return sp_signal.butter(cfg.order, wn, btype=cfg.filter_type, output="sos", fs=fs)


def _design_cheby1(cfg, wn, fs):
    return sp_signal.cheby1(
        cfg.order, cfg.ripple, wn, btype=cfg.filter_type, output="sos", fs=fs
    )


def _design_bessel(cfg, wn, fs):
    return sp_signal.bessel(
        cfg.order, wn, btype=cfg.filter_type, norm=cfg.bessel_norm, output="sos", fs=fs
    )


PRESET_PROTOTYPES = {
    "butter": _design_butter,
    "cheby1": _design_cheby1,
    "bessel": _design_bessel,
}


def check_stability(sos):
    _, poles, _ = sp_signal.sos2zpk(np.asarray(sos, dtype=np.float64))
    if poles.size and float(np.max(np.abs(poles))) >= 1.0:
        raise ValueError(
            f"unstable filter: pole magnitude {float(np.max(np.abs(poles))):.6f} >= 1"
        )


def _design_key(cfg, fs):
    if cfg.design_type == "manual":
        return ("manual", json.dumps(cfg.sos))
    params = {
        "prototype": cfg.prototype,
        "order": cfg.order,
        "ripple": cfg.ripple,
        "filter_type": cfg.filter_type,
        "fc1": cfg.fc1,
        "fc2": cfg.fc2,
        "bessel_norm": cfg.bessel_norm,
    }
    return ("preset", json.dumps(params, sort_keys=True), repr(float(fs)))


def design_iir(cfg, fs):
    if cfg.design_type == "manual":
        sos = np.asarray(cfg.sos, dtype=np.float64)
        check_stability(sos)
        return sos
    nyquist = fs / 2.0
    for name, cutoff in (("fc1", cfg.fc1), ("fc2", cfg.fc2)):
        if cutoff is not None and not 0 < cutoff < nyquist:
            raise ValueError(
                f"cutoff {name}={cutoff} Hz must be within (0, fs/2) = (0, {nyquist}) Hz"
            )
    if cfg.filter_type == "lowpass":
        wn = cfg.fc2
    elif cfg.filter_type == "highpass":
        wn = cfg.fc1
    else:
        wn = [cfg.fc1, cfg.fc2]
    sos = PRESET_PROTOTYPES[cfg.prototype](cfg, wn, float(fs))
    check_stability(sos)
    return sos


FILTER_FAMILIES = {"iir": design_iir}  # Seam 2: add "fir" in a later PR


def design_filter(cfg, fs):
    """Design (or fetch memoized) coefficients; returns (sos, coeff_hash)."""
    key = _design_key(cfg, fs)
    cached = _DESIGN_CACHE.get(key)
    if cached is not None:
        return cached
    sos = FILTER_FAMILIES["iir"](cfg, fs)
    coeff_hash = hashlib.sha256(repr(key).encode()).hexdigest()
    _DESIGN_CACHE[key] = (sos, coeff_hash)
    return sos, coeff_hash


# ---------------------------------------------------------------------------
# Streaming runtime
# ---------------------------------------------------------------------------


def init_zi(sos, first_value, init_from_first_sample):
    zi_unit = sp_signal.sosfilt_zi(sos)
    if init_from_first_sample:
        return zi_unit * float(first_value)
    return np.zeros_like(zi_unit)


def apply_filter(sos, values, zi):
    x = np.asarray(values, dtype=np.float64)
    filtered, zf = sp_signal.sosfilt(sos, x, zi=zi)
    return filtered, zf


# ---------------------------------------------------------------------------
# Per-series state
# ---------------------------------------------------------------------------


def series_hash(tag_items):
    canonical = ",".join(f"{k}={v}" for k, v in tag_items)
    return hashlib.sha256(canonical.encode()).hexdigest()


def state_key(table, field, tag_items):
    return CACHE_KEY_FMT.format(table=table, field=field, series_hash=series_hash(tag_items))


# ---------------------------------------------------------------------------
# Row/series extraction
# ---------------------------------------------------------------------------


def extract_series(rows, input_fields, tag_keys):
    """Group rows into per-(field, series) samples.

    Applies the input hygiene rules: null and non-numeric values contribute no
    sample, non-finite values are dropped (they would poison IIR state), and
    duplicate timestamps keep the last occurrence in row order to match the
    database's last-write-wins semantics.
    """
    groups = {}
    dropped_nonfinite = 0
    input_field_set = set(input_fields)
    for row in rows:
        time_ns = row.get("time")
        if time_ns is None:
            continue
        if tag_keys is not None:
            tag_items = tuple(
                sorted((k, str(row[k])) for k in tag_keys if row.get(k) is not None)
            )
        else:
            tag_items = tuple(
                sorted(
                    (k, v)
                    for k, v in row.items()
                    if k != "time" and k not in input_field_set and isinstance(v, str)
                )
            )
        for field in input_fields:
            value = row.get(field)
            if value is None or isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            if not math.isfinite(value):
                dropped_nonfinite += 1
                continue
            groups.setdefault((field, tag_items), []).append((time_ns, float(value)))

    deduped = {}
    for key, samples in groups.items():
        by_time = {}
        for time_ns, value in samples:  # later rows overwrite earlier ones
            by_time[time_ns] = value
        deduped[key] = sorted(by_time.items())
    return deduped, dropped_nonfinite


# ---------------------------------------------------------------------------
# WAL entry point (thin adapter over the entry-agnostic core above; Seam 1)
# ---------------------------------------------------------------------------


def _batch_parts(batch):
    """Return (table_name, rows) for a WAL batch.

    The live engine (verified on 3.10.2) passes plain dicts; the influxdb3_pe
    reference documents a TableBatch object. Accept both.
    """
    if isinstance(batch, dict):
        return batch["table_name"], batch["rows"]
    return batch.table_name, batch.rows


def process_writes(influxdb3_local, table_batches: list, args: dict | None = None):
    task_id = str(uuid.uuid4())
    if np is None or sp_signal is None or load_plugin_config is None:
        influxdb3_local.error(
            f"[{task_id}] signal_filter: required packages are not installed in the "
            f"plugin environment (import error: {_IMPORT_ERROR or _UTILS_IMPORT_ERROR}). "
            "Run: influxdb3 install package numpy scipy influxdata-plugin-utils"
        )
        return

    try:
        cfg = parse_config(dict(args or {}))
    except ConfigError as exc:
        influxdb3_local.error(f"[{task_id}] signal_filter: invalid configuration: {exc}")
        return

    for field in loop_hazard_fields(cfg):
        influxdb3_local.warn(
            f"[{task_id}] signal_filter: output field for '{field}' resolves to the same "
            "name in the same measurement and database; this feeds the filter its own "
            "output (unbounded write loop). Set field_suffix/output_field/"
            "output_measurement to break the cycle."
        )

    stats = {
        "tables": 0,
        "series": 0,
        "samples_in": 0,
        "points_written": 0,
        "dropped_nonfinite": 0,
        "dropped_stale": 0,
        "warmup_skipped": 0,
    }

    for batch in table_batches:
        table, rows = _batch_parts(batch)
        if cfg.input_measurement is not None and table != cfg.input_measurement:
            continue
        groups, dropped_nonfinite = extract_series(rows, cfg.input_fields, cfg.tag_keys)
        stats["dropped_nonfinite"] += dropped_nonfinite
        if not groups:
            continue
        stats["tables"] += 1
        for (field, tag_items), samples in sorted(groups.items()):
            stats["series"] += 1
            stats["samples_in"] += len(samples)
            _process_series(
                influxdb3_local, cfg, table, field, tag_items, samples, stats, task_id
            )

    influxdb3_local.info(
        f"[{task_id}] "
        + "signal_filter: {tables} table(s), {series} series: {samples_in} samples in, "
        "{points_written} points written, dropped {dropped_nonfinite} non-finite, "
        "{dropped_stale} out-of-order, {warmup_skipped} in warm-up".format(**stats)
    )


def _process_series(influxdb3_local, cfg, table, field, tag_items, samples, stats, task_id):
    cache = influxdb3_local.cache
    key = state_key(table, field, tag_items)
    state = cache.get(key)

    last_time_ns = state.get("last_time_ns") if state else None
    if last_time_ns is not None:
        fresh = [(t, v) for t, v in samples if t > last_time_ns]
        stale = len(samples) - len(fresh)
        if stale:
            stats["dropped_stale"] += stale
            influxdb3_local.warn(
                f"[{task_id}] signal_filter: {table}.{field}: dropped {stale} out-of-order "
                "sample(s) at or before the last processed timestamp (backfill through a "
                "stateful causal filter would corrupt output)"
            )
        samples = fresh
    if not samples:
        return

    if cfg.design_type == "manual":
        fs = None
    else:
        fs = cfg.sample_rate
        if fs is None and state:
            fs = state.get("fs")
        if fs is None:
            merged_times = merge_warmup_times(state, [t for t, _ in samples])
            if len(merged_times) - 1 < WARMUP_MIN_INTERVALS:
                new_state = dict(state or {})
                new_state["warmup_times"] = merged_times
                cache.put(key, new_state)
                stats["warmup_skipped"] += len(samples)
                influxdb3_local.info(
                    f"[{task_id}] signal_filter: {table}.{field}: inferring sample rate "
                    f"({len(merged_times) - 1}/{WARMUP_MIN_INTERVALS} intervals seen); "
                    "batch skipped"
                )
                return
            fs = infer_sample_rate(merged_times)

    try:
        sos, coeff_hash = design_filter(cfg, fs)
    except ValueError as exc:
        influxdb3_local.error(
            f"[{task_id}] signal_filter: {table}.{field}: filter design failed: {exc}"
        )
        return

    zi = None
    if state and state.get("coeff_hash") == coeff_hash and state.get("zi") is not None:
        zi = np.asarray(state["zi"], dtype=np.float64)
        if zi.shape != (sos.shape[0], 2):  # corrupt/stale cache entry
            influxdb3_local.warn(
                f"[{task_id}] signal_filter: {table}.{field}: cached filter state has shape "
                f"{zi.shape}, expected {(sos.shape[0], 2)}; re-initializing"
            )
            zi = None
    if zi is None:
        zi = init_zi(sos, samples[0][1], cfg.init_from_first_sample)

    filtered, zf = apply_filter(sos, [v for _, v in samples], zi)

    out_field = resolve_output_field(cfg, field)
    out_measurement = cfg.output_measurement or table
    tags = dict(tag_items)
    builders = [
        build_line(
            LineBuilder,
            out_measurement,
            tags=tags,
            fields={out_field: float(value)},
            time_ns=time_ns,
        )
        for (time_ns, _), value in zip(samples, filtered)
    ]

    # Write the whole series in one batch before saving state: a failed write
    # leaves state unadvanced, so an engine retry re-filters the same samples
    # and re-emits identical points (same timestamps overwrite). retries=0 keeps
    # the engine's error_behavior as the single retry authority. no_sync=True
    # avoids deadlocking the ingest pipeline from inside a synchronous WAL
    # trigger (the flush cannot complete until this trigger returns). database
    # routes to the trigger's own database when output_target_database is None.
    write_data(
        influxdb3_local,
        builders,
        batch=True,
        retries=0,
        no_sync=True,
        database=cfg.output_target_database,
    )
    stats["points_written"] += len(samples)

    cache.put(
        key,
        {
            "fs": fs,
            "coeff_hash": coeff_hash,
            "zi": zf.tolist(),
            "last_time_ns": samples[-1][0],
            "warmup_times": [],
        },
    )
