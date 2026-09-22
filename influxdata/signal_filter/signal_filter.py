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
            "description": "Prefix for the output field name. Use 'none' for no prefix; an empty value counts as unset. Defaults to ''.",
            "required": false
        },
        {
            "name": "field_suffix",
            "example": "_filtered",
            "description": "Suffix for the output field name. Use 'none' to write into the source field, replacing its samples; an empty value counts as unset. Defaults to '_filtered'.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "config.toml",
            "description": "Path to a TOML file supplying parameters; its values override inline arguments. Relative paths resolve against PLUGIN_DIR.",
            "required": false
        }
    ]
}
"""

import hashlib
import json
import math
import uuid

from influxdata_plugin_utils.config import Config, load_config
from influxdata_plugin_utils.parsing import parse_bool, parse_delimited_list, parse_int
from influxdata_plugin_utils.sources import (
    KeySpec,
    parse_env,
    parse_toml,
    parse_trigger_args,
)
from influxdata_plugin_utils.validation import Validator, validate
from influxdata_plugin_utils.write import build_line, write_data

try:
    import numpy as np
    from scipy import signal as sp_signal

    _IMPORT_ERROR = None
except ImportError as exc:  # engine without numpy/scipy installed
    np = None
    sp_signal = None
    _IMPORT_ERROR = str(exc)

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
PROTOTYPES = ("butter", "cheby1", "bessel")

WARMUP_MIN_INTERVALS = 8
WARMUP_MAX_TIMES = 64
CACHE_KEY_FMT = "signal_filter:{table}:{field}:{series_hash}"

DEFAULT_INPUT_FIELDS = ("value",)
DEFAULT_ORDER = 4
DEFAULT_FIELD_SUFFIX = "_filtered"
NO_AFFIX = "none"

_DESIGN_CACHE = {}  # (design key) -> (sos ndarray, coeff_hash)


# ---------------------------------------------------------------------------
# Config parsing + validation
# ---------------------------------------------------------------------------


def _finite_float(raw):
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise ValueError(f"must be a number, got {raw!r}") from None
    if not math.isfinite(value):
        raise ValueError(f"must be finite, got {raw!r}")
    return value


def _manual_sos(raw):
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"is not valid JSON: {exc}") from None
    if not isinstance(raw, list) or not raw:
        raise ValueError("must be a non-empty JSON array of sections")
    normalized = []
    for i, section in enumerate(raw):
        if not isinstance(section, list) or len(section) != 6:
            raise ValueError(f"section {i} must have 6 coefficients [b0,b1,b2,a0,a1,a2]")
        try:
            coeffs = [float(c) for c in section]
        except (TypeError, ValueError):
            raise ValueError(f"section {i} contains a non-numeric coefficient") from None
        if any(not math.isfinite(c) for c in coeffs):
            raise ValueError(f"section {i} contains a non-finite coefficient")
        a0 = coeffs[3]
        if a0 == 0.0:
            raise ValueError(f"section {i} has a0 == 0")
        normalized.append(tuple(c / a0 for c in coeffs))
    return tuple(normalized)


def _input_field_names(value):
    return tuple(parse_delimited_list(value)) or DEFAULT_INPUT_FIELDS


def _tag_key_names(value):
    return tuple(parse_delimited_list(value)) or None


def _resolve_affix(value):
    """``none`` asks for no affix; an empty argument is read as unset upstream."""
    text = str(value)
    return "" if text.strip().lower() == NO_AFFIX else text


CHEBY1 = Validator("prototype", eq="cheby1")

SETTING_VALIDATORS: list = [
    Validator("design_type", default="preset", cast=str, is_in=DESIGN_TYPES),
    Validator("filter_type", default="lowpass", cast=str, is_in=FILTER_TYPES),
    Validator("prototype", default="butter", cast=str),
    Validator("bessel_norm", default="phase", cast=str),
    Validator("input_measurement", cast=str),
    Validator("input_fields", default=DEFAULT_INPUT_FIELDS, cast=_input_field_names),
    Validator("tag_keys", cast=_tag_key_names),
    Validator("init_from_first_sample", default=True, cast=parse_bool),
    Validator("output_target_database", cast=str),
    Validator("output_measurement", cast=str),
    Validator("output_field", cast=str),
    Validator("field_prefix", default="", cast=_resolve_affix),
    Validator("field_suffix", default=DEFAULT_FIELD_SUFFIX, cast=_resolve_affix),
]

PRESET_VALIDATORS: list = [
    Validator("prototype", is_in=PROTOTYPES),
    Validator("bessel_norm", is_in=BESSEL_NORMS),
    Validator("order", default=DEFAULT_ORDER, cast=parse_int, gte=1, lte=12),
    Validator(
        "ripple", required=True, cast=_finite_float, gte=0.01, lte=80, when=CHEBY1
    ),
    Validator("fc", cast=_finite_float, gt=0),
    Validator("fc1", cast=_finite_float, gt=0),
    Validator("fc2", cast=_finite_float, gt=0),
    Validator("sample_rate", cast=_finite_float, gt=0),
]

MANUAL_VALIDATORS: list = [
    Validator("sos", required=True, cast=_manual_sos),
]

SETTINGS = KeySpec(
    allowlist=tuple(
        dict.fromkeys(
            name
            for rules in (SETTING_VALIDATORS, PRESET_VALIDATORS, MANUAL_VALIDATORS)
            for rule in rules
            for name in rule.names
        )
    )
)

ENV_PREFIX = "INFLUXDB3_SIGNAL_FILTER_"


def env_spec(*names: str) -> KeySpec:
    """Read the named settings from ``INFLUXDB3_SIGNAL_FILTER_<SETTING>``.

    The prefix is stripped again, so a variable merges with the same setting
    coming from a trigger argument or the TOML file.
    """
    rename = {f"{ENV_PREFIX}{name.upper()}": name for name in names}
    return KeySpec(allowlist=tuple(rename), rename=rename)


ENV_SETTINGS = env_spec(*SETTINGS.allowlist)

OPTIONAL_SETTINGS = (
    "input_measurement",
    "tag_keys",
    "output_target_database",
    "output_measurement",
    "output_field",
    "ripple",
    "sample_rate",
)


def _resolve_cutoffs(cfg):
    """Apply the fc alias and per-band presence rules; returns (fc1, fc2)."""
    filter_type = cfg["filter_type"]
    fc = cfg.get("fc")
    fc1 = cfg.get("fc1")
    fc2 = cfg.get("fc2")

    if fc is not None:
        if filter_type in ("bandpass", "bandstop"):
            raise ValueError("'fc' is not valid for band filters; use 'fc1' and 'fc2'")
        if filter_type == "lowpass":
            if fc2 is not None:
                raise ValueError("'fc' and 'fc2' are both set; use one")
            fc2 = fc
        else:  # highpass
            if fc1 is not None:
                raise ValueError("'fc' and 'fc1' are both set; use one")
            fc1 = fc

    if filter_type == "lowpass":
        if fc2 is None:
            raise ValueError("lowpass requires a cutoff: set 'fc2' (or 'fc')")
        if fc1 is not None:
            raise ValueError("'fc1' is not used for lowpass")
    elif filter_type == "highpass":
        if fc1 is None:
            raise ValueError("highpass requires a cutoff: set 'fc1' (or 'fc')")
        if fc2 is not None:
            raise ValueError("'fc2' is not used for highpass")
    else:  # bandpass / bandstop
        if fc1 is None or fc2 is None:
            raise ValueError(f"{filter_type} requires both 'fc1' and 'fc2'")
        if not fc1 < fc2:
            raise ValueError(f"'fc1' must be < 'fc2', got {fc1} >= {fc2}")
    return fc1, fc2


def prepare_config(cfg: Config) -> Config:
    """Check the settings against each other and neutralize the unused ones."""
    if "output_field" in cfg and len(cfg["input_fields"]) != 1:
        raise ValueError("'output_field' is only valid with a single input field")

    if cfg["design_type"] == "manual":
        cfg.update(
            order=DEFAULT_ORDER, ripple=None, fc1=None, fc2=None, sample_rate=None
        )
    else:
        if "sos" in cfg:
            raise ValueError("'sos' is only valid with design_type 'manual'")
        if cfg["prototype"] != "cheby1" and "ripple" in cfg:
            raise ValueError("'ripple' is only valid for prototype 'cheby1'")
        cfg["sos"] = None
        cfg["fc1"], cfg["fc2"] = _resolve_cutoffs(cfg)

    for name in OPTIONAL_SETTINGS:
        cfg.setdefault(name, None)
    return cfg


def design_validators(cfg: Config) -> list:
    return MANUAL_VALIDATORS if cfg["design_type"] == "manual" else PRESET_VALIDATORS


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
    if np is None or sp_signal is None:
        influxdb3_local.error(
            f"[{task_id}] signal_filter: required packages are not installed in the "
            f"plugin environment (import error: {_IMPORT_ERROR}). "
            "Run: influxdb3 install package numpy scipy influxdata-plugin-utils"
        )
        return

    args = args or {}
    try:
        config_file_path = args.get("config_file_path") or parse_env(
            env_spec("config_file_path")
        ).get("config_file_path")
        cfg = load_config(
            parse_env(ENV_SETTINGS),
            parse_trigger_args(args, SETTINGS),
            parse_toml(config_file_path, SETTINGS),
            validators=SETTING_VALIDATORS,
        )
        cfg = prepare_config(Config(validate(cfg, design_validators(cfg))))
    except Exception as exc:
        influxdb3_local.error(f"[{task_id}] signal_filter: invalid configuration: {exc}")
        return

    for field in loop_hazard_fields(cfg):
        influxdb3_local.warn(
            f"[{task_id}] signal_filter: output field for '{field}' resolves to the same "
            "name in the same measurement and database; the filtered values replace the "
            "source samples at the same timestamps. Set output_field, field_suffix, "
            "output_measurement or output_target_database to write elsewhere."
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
