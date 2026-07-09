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


def get_last_time(cache):
    return cache.get(CACHE_KEY)


def set_last_time(cache, t):
    cache.put(CACHE_KEY, t)


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

    # --- Check cache for last execution time ---
    last_time = get_last_time(influxdb3_local.cache)
    if last_time is None:
        influxdb3_local.info(f"[{task_id}] Signal generator initializing, first data on next execution")
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

    try:
        written = write_points(influxdb3_local, points, measurement, field, tags, target_database)
        influxdb3_local.info(f"[{task_id}] Wrote {written} points to {measurement}.{field}")
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Batch write failed: {e}")

    set_last_time(influxdb3_local.cache, now)
