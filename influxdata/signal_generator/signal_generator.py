"""
{
    "plugin_type": ["scheduled"],
    "scheduled_args_config": [
        {
            "name": "waveforms",
            "example": "[{\"type\": \"sine\"}, {\"type\": \"noise\", \"stddev\": 0.3}]",
            "description": "JSON array of waveform config objects. Each object has a 'type' key and optional parameter overrides. Supported types: constant, sine, square, triangle, sawtooth, noise, spike.",
            "required": false
        },
        {
            "name": "measurement",
            "example": "signal",
            "description": "Output measurement name. Defaults to 'signal'.",
            "required": false
        },
        {
            "name": "field",
            "example": "value",
            "description": "Output field name. Defaults to 'value'.",
            "required": false
        },
        {
            "name": "tags",
            "example": "{\"host\": \"server01\"}",
            "description": "Optional JSON object of tags to add to each data point.",
            "required": false
        },
        {
            "name": "points_per_second",
            "example": "1.0",
            "description": "Data point resolution in points per second. Defaults to 1.0.",
            "required": false
        },
        {
            "name": "jitter_amplitude_seconds",
            "example": "0.2",
            "description": "Maximum timestamp offset in seconds. Defaults to 10% of the point interval. Set to 0 to disable timestamp jitter.",
            "required": false
        },
        {
            "name": "jitter_seed",
            "example": "42",
            "description": "Optional integer seed for reproducible per-timestamp jitter.",
            "required": false
        },
        {
            "name": "gap_enabled",
            "example": "false",
            "description": "Enables simulated data-source outage gaps. Enabled by default; set to 'false' for continuous output.",
            "required": false
        },
        {
            "name": "gap_min_duration_seconds",
            "example": "10",
            "description": "Shortest simulated outage duration in seconds. Defaults to 10.",
            "required": false
        },
        {
            "name": "gap_max_duration_seconds",
            "example": "30",
            "description": "Longest simulated outage duration in seconds. Defaults to 30.",
            "required": false
        },
        {
            "name": "gap_min_interval_seconds",
            "example": "120",
            "description": "Shortest time between consecutive outage starts in seconds. Must be at least gap_max_duration_seconds. Defaults to 120.",
            "required": false
        },
        {
            "name": "gap_max_interval_seconds",
            "example": "240",
            "description": "Longest time between consecutive outage starts in seconds. Defaults to 240.",
            "required": false
        },
        {
            "name": "gap_seed",
            "example": "123",
            "description": "Optional integer seed for reproducible gap timing.",
            "required": false
        },
        {
            "name": "target_database",
            "example": "my_db",
            "description": "Optional target database. If omitted, writes to the trigger's database.",
            "required": false
        }
    ]
}
"""

import hashlib
import json
import math
import random
import uuid
from typing import Iterable, Optional, Protocol, runtime_checkable

# LineBuilder is injected by the InfluxDB 3 Processing Engine runtime.
# The stub below allows tests to monkeypatch it.
try:
    LineBuilder = LineBuilder  # noqa: F821 — already in scope when injected
except NameError:
    LineBuilder = None  # type: ignore[assignment,misc]


# ---------------------------------------------------------------------------
# Waveform factories
# ---------------------------------------------------------------------------

def make_constant(value=0.0):
    def waveform(t):
        return value
    return waveform


def make_sine(frequency=0.05, amplitude=1.0, offset=0.0, phase=0.0):
    def waveform(t):
        return offset + amplitude * math.sin(2 * math.pi * frequency * t + phase)
    return waveform


def make_square(frequency=0.05, amplitude=1.0, offset=0.0, phase=0.0, duty_cycle=0.5):
    def waveform(t):
        cycle_position = (frequency * t + phase / (2 * math.pi)) % 1.0
        if cycle_position < duty_cycle:
            return offset + amplitude
        else:
            return offset - amplitude
    return waveform


def make_triangle(frequency=0.05, amplitude=1.0, offset=0.0, phase=0.0):
    def waveform(t):
        cycle_position = (frequency * t + phase / (2 * math.pi)) % 1.0
        if cycle_position < 0.5:
            return offset + amplitude * (4 * cycle_position - 1)
        else:
            return offset + amplitude * (3 - 4 * cycle_position)
    return waveform


def make_sawtooth(frequency=0.05, amplitude=1.0, offset=0.0, phase=0.0):
    def waveform(t):
        cycle_position = (frequency * t + phase / (2 * math.pi)) % 1.0
        return offset + amplitude * (2 * cycle_position - 1)
    return waveform


def make_noise(stddev=0.1, mean=0.0, seed=None):
    rng = random.Random(seed)
    def waveform(t):
        return rng.gauss(mean, stddev)
    return waveform


def make_spike(probability=0.01, min_amplitude=5.0, max_amplitude=10.0, seed=None):
    rng = random.Random(seed)
    def waveform(t):
        if rng.random() < probability:
            magnitude = rng.uniform(min_amplitude, max_amplitude)
            sign = rng.choice([-1, 1])
            return sign * magnitude
        return 0.0
    return waveform


def combine(waveform_fns):
    def combined(t):
        return sum(fn(t) for fn in waveform_fns)
    return combined


# ---------------------------------------------------------------------------
# Waveform registry and default preset
# ---------------------------------------------------------------------------

WAVEFORM_REGISTRY = {
    "constant": make_constant,
    "sine": make_sine,
    "square": make_square,
    "triangle": make_triangle,
    "sawtooth": make_sawtooth,
    "noise": make_noise,
    "spike": make_spike,
}

PARAM_CONSTRAINTS = {
    "frequency": lambda v: v >= 0,
    "amplitude": lambda v: v >= 0,
    "stddev": lambda v: v >= 0,
    "probability": lambda v: 0 <= v <= 1,
    "duty_cycle": lambda v: 0 <= v <= 1,
    "min_amplitude": lambda v: v >= 0,
    "max_amplitude": lambda v: v >= 0,
}


def _validate_params(params):
    for key, value in params.items():
        check = PARAM_CONSTRAINTS.get(key)
        if check is not None and not check(value):
            return False
    if "min_amplitude" in params and "max_amplitude" in params:
        if params["min_amplitude"] > params["max_amplitude"]:
            return False
    return True

DEFAULT_WAVEFORMS = [
    {"type": "constant", "value": 30.0},
    {"type": "sine", "frequency": 0.005, "amplitude": 10.0},
    {"type": "noise", "stddev": 0.5},
    {"type": "spike", "probability": 0.005, "min_amplitude": 8.0, "max_amplitude": 15.0},
]

DEFAULT_JITTER_INTERVAL_FRACTION = 0.10
MIN_JITTER_GAP_SECONDS = 1e-6

DEFAULT_GAP_MIN_DURATION_SECONDS = 10.0
DEFAULT_GAP_MAX_DURATION_SECONDS = 30.0
DEFAULT_GAP_MIN_INTERVAL_SECONDS = 120.0
DEFAULT_GAP_MAX_INTERVAL_SECONDS = 240.0


# ---------------------------------------------------------------------------
# Config parsing
# ---------------------------------------------------------------------------

def parse_waveform_config(waveforms_json):
    """Parse waveforms JSON string into a list of waveform functions.
    Returns None on error (caller should log and return early).
    """
    if waveforms_json is None:
        waveform_configs = DEFAULT_WAVEFORMS
    else:
        try:
            waveform_configs = json.loads(waveforms_json)
        except (json.JSONDecodeError, TypeError):
            return None

    if not isinstance(waveform_configs, list):
        return None

    fns = []
    for config in waveform_configs:
        waveform_type = config.get("type")
        if waveform_type is None:
            return None

        factory = WAVEFORM_REGISTRY.get(waveform_type)
        if factory is None:
            return None

        params = {k: v for k, v in config.items() if k != "type"}
        if not _validate_params(params):
            return None
        try:
            fns.append(factory(**params))
        except TypeError:
            return None

    return fns


def parse_output_config(args):
    """Parse output configuration from trigger args.
    Returns (measurement, field, tags_dict, target_database) or None on error.
    """
    if args is None:
        args = {}

    measurement = args.get("measurement", "signal")
    field = args.get("field", "value")
    target_database = args.get("target_database", None)

    tags_json = args.get("tags", None)
    if tags_json is not None:
        try:
            tags = json.loads(tags_json)
        except (json.JSONDecodeError, TypeError):
            return None
    else:
        tags = {}

    return (measurement, field, tags, target_database)


def parse_points_per_second(args):
    """Parse points_per_second from trigger args.
    Returns float or None on error.
    """
    if args is None:
        return 1.0
    raw = args.get("points_per_second", "1.0")
    try:
        value = float(raw)
    except (ValueError, TypeError):
        return None
    if not math.isfinite(value) or value <= 0:
        return None
    return value


def parse_jitter_config(args, points_per_second):
    """Parse timestamp jitter config from trigger args.
    Returns ((amplitude_seconds, seed), None) on success,
    or (None, error_message) on error.
    """
    if args is None:
        args = {}

    interval = 1.0 / points_per_second
    raw_amplitude = args.get("jitter_amplitude_seconds")
    if raw_amplitude is None:
        amplitude = DEFAULT_JITTER_INTERVAL_FRACTION * interval
    else:
        try:
            amplitude = float(raw_amplitude)
        except (ValueError, TypeError):
            return (None, "Invalid jitter config: jitter_amplitude_seconds must be a float")

    if not math.isfinite(amplitude) or amplitude < 0:
        return (None, f"Invalid jitter config: amplitude {amplitude}s must be finite and >= 0")

    raw_seed = args.get("jitter_seed")
    if raw_seed is None:
        seed = None
    else:
        try:
            if isinstance(raw_seed, bool):
                raise ValueError
            if isinstance(raw_seed, float) and not raw_seed.is_integer():
                raise ValueError
            seed = int(raw_seed)
        except (ValueError, TypeError):
            return (None, "Invalid jitter config: jitter_seed must be an integer")

    if interval - (2 * amplitude) < MIN_JITTER_GAP_SECONDS:
        return (
            None,
            f"Invalid jitter config: amplitude {amplitude}s must satisfy "
            f"interval ({interval}s) - 2*amplitude >= 1us; reduce "
            f"jitter_amplitude_seconds or points_per_second",
        )

    return ((amplitude, seed), None)


def _parse_bool(raw):
    """Parse a boolean trigger argument. Returns None on error."""
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        lowered = raw.strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    return None


def parse_gap_config(args):
    """Parse simulated gap config from trigger args.
    Returns (config_dict, None) on success, or (None, error_message) on error.
    All provided arguments are validated even when gaps are disabled."""
    if args is None:
        args = {}

    raw_enabled = args.get("gap_enabled")
    if raw_enabled is None:
        enabled = True
    else:
        enabled = _parse_bool(raw_enabled)
        if enabled is None:
            return (None, "Invalid gap config: gap_enabled must be 'true' or 'false'")

    bounds = {}
    for name, default in (
        ("gap_min_duration_seconds", DEFAULT_GAP_MIN_DURATION_SECONDS),
        ("gap_max_duration_seconds", DEFAULT_GAP_MAX_DURATION_SECONDS),
        ("gap_min_interval_seconds", DEFAULT_GAP_MIN_INTERVAL_SECONDS),
        ("gap_max_interval_seconds", DEFAULT_GAP_MAX_INTERVAL_SECONDS),
    ):
        raw = args.get(name)
        if raw is None:
            bounds[name] = default
            continue
        try:
            value = float(raw)
        except (ValueError, TypeError):
            return (None, f"Invalid gap config: {name} must be a float")
        if not math.isfinite(value):
            return (None, f"Invalid gap config: {name} must be finite")
        bounds[name] = value

    min_duration = bounds["gap_min_duration_seconds"]
    max_duration = bounds["gap_max_duration_seconds"]
    min_interval = bounds["gap_min_interval_seconds"]
    max_interval = bounds["gap_max_interval_seconds"]

    if min_duration < 0:
        return (None, "Invalid gap config: gap_min_duration_seconds must be >= 0")
    if max_duration < min_duration:
        return (None, "Invalid gap config: gap_max_duration_seconds must be >= gap_min_duration_seconds")
    if min_interval <= 0:
        return (None, "Invalid gap config: gap_min_interval_seconds must be > 0")
    if max_interval < min_interval:
        return (None, "Invalid gap config: gap_max_interval_seconds must be >= gap_min_interval_seconds")
    if min_interval < max_duration:
        return (
            None,
            f"Invalid gap config: gap_min_interval_seconds ({min_interval}s) must be >= "
            f"gap_max_duration_seconds ({max_duration}s) so outages cannot overlap",
        )

    raw_seed = args.get("gap_seed")
    if raw_seed is None:
        seed = None
    else:
        try:
            if isinstance(raw_seed, bool):
                raise ValueError
            if isinstance(raw_seed, float) and not raw_seed.is_integer():
                raise ValueError
            seed = int(raw_seed)
        except (ValueError, TypeError):
            return (None, "Invalid gap config: gap_seed must be an integer")

    return (
        {
            "enabled": enabled,
            "min_duration": min_duration,
            "max_duration": max_duration,
            "min_interval": min_interval,
            "max_interval": max_interval,
            "pitch": (min_interval + max_interval) / 2,
            "offset": (max_interval - min_interval) / 4,
            "seed": seed,
        },
        None,
    )


# ---------------------------------------------------------------------------
# Time series generation
# ---------------------------------------------------------------------------

def generate_timestamps(start, end, points_per_second):
    """Generate timestamps in the half-open interval (start, end].
    Returns a list of floats (Unix epoch seconds).
    """
    interval = 1.0 / points_per_second
    timestamps = []
    t = start + interval
    while t <= end + (interval * 1e-9):  # tiny epsilon for float comparison
        timestamps.append(t)
        t += interval
    return timestamps


def generate_signal(waveform_fn, timestamps):
    """Evaluate a waveform function at each timestamp.
    Returns a list of (timestamp, value) tuples.
    """
    return [(t, waveform_fn(t)) for t in timestamps]


def _rng_for_timestamp(seed, timestamp):
    timestamp_ns = int(round(timestamp * 1_000_000_000))
    payload = f"{seed}:{timestamp_ns}".encode("ascii")
    digest = hashlib.blake2b(payload, digest_size=8).digest()
    return random.Random(int.from_bytes(digest, "big"))


def apply_timestamp_jitter(points, amplitude_seconds, seed=None):
    """Apply independent timestamp offsets to points without changing values."""
    if amplitude_seconds == 0:
        return list(points)

    if seed is None:
        rng = random.Random()
        return [
            (t + rng.uniform(-amplitude_seconds, amplitude_seconds), value)
            for t, value in points
        ]

    jittered = []
    for t, value in points:
        rng = _rng_for_timestamp(seed, t)
        jittered.append((t + rng.uniform(-amplitude_seconds, amplitude_seconds), value))
    return jittered


# ---------------------------------------------------------------------------
# Simulated gaps (see GAP_DESIGN.md)
# ---------------------------------------------------------------------------

def _rng_for_gap_index(seed, n):
    payload = f"{seed}:gap:{n}".encode("ascii")
    digest = hashlib.blake2b(payload, digest_size=8).digest()
    return random.Random(int.from_bytes(digest, "big"))


def gap_window(gap_config, n):
    """Gap n's half-open [start, end) window in epoch seconds.
    The draw order (start offset, then duration) and the payload format in
    _rng_for_gap_index are a reproducibility contract: changing either
    reshuffles every seeded schedule."""
    rng = _rng_for_gap_index(gap_config["seed"], n)
    start = n * gap_config["pitch"] + rng.uniform(-gap_config["offset"], gap_config["offset"])
    duration = rng.uniform(gap_config["min_duration"], gap_config["max_duration"])
    return (start, start + duration)


def in_gap(gap_config, t, window_cache=None):
    """Return True if timestamp t falls inside a gap window.
    Under the min_interval >= max_duration validation rule, only two gap
    indices can contain t, so this is O(1)."""
    n_hi = int((t + gap_config["offset"]) // gap_config["pitch"])
    for n in (n_hi, n_hi - 1):
        if n < 0:
            continue
        if window_cache is None:
            window = gap_window(gap_config, n)
        else:
            window = window_cache.get(n)
            if window is None:
                window = gap_window(gap_config, n)
                window_cache[n] = window
        start, end = window
        if start <= t < end:
            return True
    return False


def apply_gaps(points, gap_config):
    """Remove points whose final timestamp falls inside a gap window.
    Returns (kept_points, removed_count, gap_window_count) where
    gap_window_count is the number of gap windows intersecting the time span
    of the given points."""
    if not points:
        return ([], 0, 0)

    window_cache = {}
    kept = [p for p in points if not in_gap(gap_config, p[0], window_cache)]
    removed = len(points) - len(kept)

    timestamps = [t for t, _ in points]
    t_min, t_max = min(timestamps), max(timestamps)
    pitch = gap_config["pitch"]
    offset = gap_config["offset"]
    n_lo = int((t_min - offset - gap_config["max_duration"]) // pitch)
    n_hi = int((t_max + offset) // pitch)
    gap_window_count = 0
    for n in range(max(0, n_lo), n_hi + 1):
        window = window_cache.get(n)
        if window is None:
            window = gap_window(gap_config, n)
        start, end = window
        if start <= t_max and end > t_min:
            gap_window_count += 1
    return (kept, removed, gap_window_count)


# ---------------------------------------------------------------------------
# Batch write helper
# ---------------------------------------------------------------------------

@runtime_checkable
class _LineBuilderInterface(Protocol):
    def build(self) -> str: ...


class _BatchLines:
    """Wraps multiple LineBuilder objects into a single object with a build()
    method that returns a newline-separated string. This allows batched writes
    through the write_sync / write_sync_to_db APIs."""

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
            lines = [self._coerce_builder(b) for b in self._line_builders]
            if not lines:
                raise ValueError("batch_write received no lines to build")
            self._built = "\n".join(lines)
        return self._built


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

def build_line(measurement, field, value, tags, timestamp_ns):
    """Build a single LineBuilder for one data point."""
    line = LineBuilder(measurement)
    for tag_key, tag_value in tags.items():
        line.tag(tag_key, str(tag_value))
    line.float64_field(field, value)
    line.time_ns(timestamp_ns)
    return line


def write_points(influxdb3_local, points, measurement, field, tags, target_database):
    """Build LineBuilder objects for all points and write as a single batch."""
    line_builders = []
    for t, value in points:
        line = build_line(measurement, field, value, tags, int(t * 1_000_000_000))
        line_builders.append(line)

    if not line_builders:
        return 0

    batch = _BatchLines(line_builders)
    if target_database:
        influxdb3_local.write_sync_to_db(target_database, batch, no_sync=True)
    else:
        influxdb3_local.write_sync(batch, no_sync=True)
    return len(line_builders)


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

CACHE_KEY = "signal_gen:last_time"
GAP_SEED_CACHE_KEY = "signal_gen:gap_seed"


def get_last_time(cache):
    return cache.get(CACHE_KEY)


def set_last_time(cache, t):
    cache.put(CACHE_KEY, t)


def resolve_gap_seed(cache, configured_seed):
    """Return the gap seed: the configured value, else the cached auto-seed,
    else a newly generated auto-seed persisted to the cache. Cache loss means
    a new schedule (documented in GAP_DESIGN.md)."""
    if configured_seed is not None:
        return configured_seed
    cached = cache.get(GAP_SEED_CACHE_KEY)
    if cached is not None:
        return int(cached)
    seed = random.getrandbits(63)
    cache.put(GAP_SEED_CACHE_KEY, seed)
    return seed


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def process_scheduled_call(influxdb3_local, call_time, args=None):
    task_id = str(uuid.uuid4())[:8]

    now = call_time.timestamp()

    # --- Parse config ---
    waveforms_json = args.get("waveforms", None) if args else None
    waveform_fns = parse_waveform_config(waveforms_json)
    if waveform_fns is None:
        influxdb3_local.error(f"[{task_id}] Failed to parse waveform config")
        return

    output_config = parse_output_config(args)
    if output_config is None:
        influxdb3_local.error(f"[{task_id}] Failed to parse output config")
        return
    measurement, field, tags, target_database = output_config

    pps = parse_points_per_second(args)
    if pps is None:
        influxdb3_local.error(f"[{task_id}] Failed to parse points_per_second")
        return

    jitter_config, jitter_error = parse_jitter_config(args, pps)
    if jitter_config is None:
        influxdb3_local.error(f"[{task_id}] {jitter_error}")
        return

    gap_config, gap_error = parse_gap_config(args)
    if gap_config is None:
        influxdb3_local.error(f"[{task_id}] {gap_error}")
        return

    # --- Check cache for last execution time ---
    last_time = get_last_time(influxdb3_local.cache)
    if last_time is None:
        influxdb3_local.info(f"[{task_id}] Signal generator initializing, first data on next execution")
        if gap_config["enabled"]:
            resolve_gap_seed(influxdb3_local.cache, gap_config["seed"])
        set_last_time(influxdb3_local.cache, now)
        return

    # --- Generate and write data ---
    combined = combine(waveform_fns)
    timestamps = generate_timestamps(last_time, now, pps)

    if not timestamps:
        influxdb3_local.info(f"[{task_id}] No points to generate (interval too short)")
        set_last_time(influxdb3_local.cache, now)
        return

    points = generate_signal(combined, timestamps)
    jitter_amplitude_seconds, jitter_seed = jitter_config
    points = apply_timestamp_jitter(points, jitter_amplitude_seconds, jitter_seed)

    generated = len(points)
    removed = 0
    gap_window_count = 0
    if gap_config["enabled"]:
        gap_config["seed"] = resolve_gap_seed(influxdb3_local.cache, gap_config["seed"])
        points, removed, gap_window_count = apply_gaps(points, gap_config)

    try:
        written = write_points(influxdb3_local, points, measurement, field, tags, target_database)
        if removed:
            influxdb3_local.info(
                f"[{task_id}] Wrote {written} points to {measurement}.{field} "
                f"({generated} generated, {removed} removed by {gap_window_count} gap windows)"
            )
        else:
            influxdb3_local.info(f"[{task_id}] Wrote {written} points to {measurement}.{field}")
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Batch write failed: {e}")

    set_last_time(influxdb3_local.cache, now)
