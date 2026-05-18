"""
Unit tests for the Signal Generator Plugin.
"""

import math
import json
import sys
import os


# ---------------------------------------------------------------------------
# Mocks for the InfluxDB 3 Processing Engine runtime
# ---------------------------------------------------------------------------

class MockLineBuilder:
    """Simulates the LineBuilder class injected by the processing engine."""
    def __init__(self, measurement):
        self.measurement = measurement
        self.tags_list = []
        self.fields_list = []
        self.timestamp = None

    def tag(self, key, value):
        self.tags_list.append((key, str(value)))
        return self

    def float64_field(self, key, value):
        self.fields_list.append((key, float(value)))
        return self

    def time_ns(self, ts):
        self.timestamp = ts
        return self

    def build(self):
        parts = [self.measurement]
        if self.tags_list:
            tag_str = ",".join(f"{k}={v}" for k, v in self.tags_list)
            parts[0] = f"{self.measurement},{tag_str}"
        field_strs = [f"{k}={v}" for k, v in self.fields_list]
        parts.append(",".join(field_strs))
        if self.timestamp is not None:
            parts.append(str(self.timestamp))
        return " ".join(parts)


class MockCache:
    """Simulates the influxdb3_local.cache object."""
    def __init__(self):
        self._store = {}

    def get(self, key, default=None):
        return self._store.get(key, default)

    def put(self, key, value, ttl=None):
        self._store[key] = value

    def clear(self):
        self._store.clear()


class MockInfluxDB3Local:
    """Simulates the influxdb3_local object passed to process_scheduled_call."""
    def __init__(self):
        self.cache = MockCache()
        self.written_lines = []
        self.written_to_db = []
        self.logs = {"info": [], "warn": [], "error": []}

    def write_sync(self, line, no_sync=False):
        self.written_lines.append(line)

    def write_sync_to_db(self, db_name, line, no_sync=False):
        self.written_to_db.append((db_name, line))

    def info(self, *args):
        self.logs["info"].append(" ".join(str(a) for a in args))

    def warn(self, *args):
        self.logs["warn"].append(" ".join(str(a) for a in args))

    def error(self, *args):
        self.logs["error"].append(" ".join(str(a) for a in args))

    def reset(self):
        self.written_lines.clear()
        self.written_to_db.clear()
        self.logs = {"info": [], "warn": [], "error": []}
        self.cache.clear()


# ---------------------------------------------------------------------------
# Waveform factory tests
# ---------------------------------------------------------------------------

def test_make_constant_default():
    from signal_generator import make_constant
    fn = make_constant()
    assert fn(0.0) == 0.0
    assert fn(100.0) == 0.0
    assert fn(-50.0) == 0.0


def test_make_constant_custom_value():
    from signal_generator import make_constant
    fn = make_constant(value=30.0)
    assert fn(0.0) == 30.0
    assert fn(999.9) == 30.0


def test_make_sine_default_at_zero():
    from signal_generator import make_sine
    fn = make_sine()
    assert fn(0.0) == 0.0

def test_make_sine_default_period():
    from signal_generator import make_sine
    fn = make_sine()
    assert abs(fn(5.0) - 1.0) < 1e-10

def test_make_sine_custom_params():
    from signal_generator import make_sine
    fn = make_sine(frequency=1.0, amplitude=5.0, offset=10.0, phase=0.0)
    assert abs(fn(0.25) - 15.0) < 1e-10

def test_make_sine_with_offset():
    from signal_generator import make_sine
    fn = make_sine(offset=100.0)
    assert fn(0.0) == 100.0

def test_make_sine_is_deterministic():
    from signal_generator import make_sine
    fn = make_sine()
    t = 12345.6789
    assert fn(t) == fn(t)


def test_make_square_default_first_half():
    from signal_generator import make_square
    fn = make_square()
    assert fn(0.0) == 1.0
    assert fn(5.0) == 1.0

def test_make_square_default_second_half():
    from signal_generator import make_square
    fn = make_square()
    assert fn(10.0) == -1.0
    assert fn(15.0) == -1.0

def test_make_square_custom_duty_cycle():
    from signal_generator import make_square
    fn = make_square(frequency=1.0, duty_cycle=0.25)
    assert fn(0.0) == 1.0
    assert fn(0.1) == 1.0
    assert fn(0.3) == -1.0
    assert fn(0.9) == -1.0

def test_make_square_with_offset():
    from signal_generator import make_square
    fn = make_square(offset=5.0)
    assert fn(0.0) == 6.0
    assert fn(10.0) == 4.0


def test_make_triangle_default_start():
    from signal_generator import make_triangle
    fn = make_triangle()
    assert abs(fn(0.0) - (-1.0)) < 1e-10

def test_make_triangle_default_quarter():
    from signal_generator import make_triangle
    fn = make_triangle()
    assert abs(fn(5.0) - 0.0) < 1e-10

def test_make_triangle_default_half():
    from signal_generator import make_triangle
    fn = make_triangle()
    assert abs(fn(10.0) - 1.0) < 1e-10

def test_make_triangle_default_three_quarter():
    from signal_generator import make_triangle
    fn = make_triangle()
    assert abs(fn(15.0) - 0.0) < 1e-10

def test_make_triangle_custom_amplitude():
    from signal_generator import make_triangle
    fn = make_triangle(frequency=1.0, amplitude=10.0)
    assert abs(fn(0.25) - 0.0) < 1e-10
    assert abs(fn(0.5) - 10.0) < 1e-10


def test_make_sawtooth_default_start():
    from signal_generator import make_sawtooth
    fn = make_sawtooth()
    assert abs(fn(0.0) - (-1.0)) < 1e-10

def test_make_sawtooth_default_half():
    from signal_generator import make_sawtooth
    fn = make_sawtooth()
    assert abs(fn(10.0) - 0.0) < 1e-10

def test_make_sawtooth_default_near_end():
    from signal_generator import make_sawtooth
    fn = make_sawtooth()
    assert abs(fn(19.0) - 0.9) < 1e-10

def test_make_sawtooth_custom_amplitude():
    from signal_generator import make_sawtooth
    fn = make_sawtooth(frequency=1.0, amplitude=5.0, offset=10.0)
    assert abs(fn(0.5) - 10.0) < 1e-10
    assert abs(fn(0.75) - 12.5) < 1e-10


def test_make_noise_returns_float():
    from signal_generator import make_noise
    fn = make_noise(seed=42)
    result = fn(0.0)
    assert isinstance(result, float)

def test_make_noise_seeded_produces_sequence():
    from signal_generator import make_noise
    fn = make_noise(seed=42)
    values = [fn(i) for i in range(10)]
    fn2 = make_noise(seed=42)
    values2 = [fn2(i) for i in range(10)]
    assert values == values2

def test_make_noise_unseeded_varies():
    from signal_generator import make_noise
    fn1 = make_noise()
    fn2 = make_noise()
    vals1 = [fn1(i) for i in range(100)]
    vals2 = [fn2(i) for i in range(100)]
    assert vals1 != vals2

def test_make_noise_mean_and_stddev():
    from signal_generator import make_noise
    fn = make_noise(mean=10.0, stddev=1.0, seed=42)
    samples = [fn(i) for i in range(10000)]
    sample_mean = sum(samples) / len(samples)
    assert abs(sample_mean - 10.0) < 0.1


def test_make_spike_zero_probability():
    from signal_generator import make_spike
    fn = make_spike(probability=0.0, seed=42)
    values = [fn(i) for i in range(1000)]
    assert all(v == 0.0 for v in values)

def test_make_spike_full_probability():
    from signal_generator import make_spike
    fn = make_spike(probability=1.0, min_amplitude=5.0, max_amplitude=10.0, seed=42)
    values = [fn(i) for i in range(100)]
    assert all(v != 0.0 for v in values)
    assert all(5.0 <= abs(v) <= 10.0 for v in values)

def test_make_spike_produces_positive_and_negative():
    from signal_generator import make_spike
    fn = make_spike(probability=1.0, min_amplitude=5.0, max_amplitude=10.0, seed=42)
    values = [fn(i) for i in range(100)]
    has_positive = any(v > 0 for v in values)
    has_negative = any(v < 0 for v in values)
    assert has_positive and has_negative

def test_make_spike_seeded_reproducible():
    from signal_generator import make_spike
    fn1 = make_spike(probability=0.5, seed=42)
    fn2 = make_spike(probability=0.5, seed=42)
    vals1 = [fn1(i) for i in range(50)]
    vals2 = [fn2(i) for i in range(50)]
    assert vals1 == vals2


# ---------------------------------------------------------------------------
# Composition tests
# ---------------------------------------------------------------------------

def test_combine_sums_waveforms():
    from signal_generator import combine, make_constant
    fn = combine([make_constant(10.0), make_constant(20.0)])
    assert fn(0.0) == 30.0

def test_combine_empty_list():
    from signal_generator import combine
    fn = combine([])
    assert fn(0.0) == 0.0

def test_combine_single_waveform():
    from signal_generator import combine, make_constant
    fn = combine([make_constant(5.0)])
    assert fn(0.0) == 5.0

def test_combine_sine_and_constant():
    from signal_generator import combine, make_sine, make_constant
    fn = combine([make_constant(100.0), make_sine(frequency=1.0, amplitude=1.0)])
    assert abs(fn(0.0) - 100.0) < 1e-10
    assert abs(fn(0.25) - 101.0) < 1e-10


# ---------------------------------------------------------------------------
# Registry tests
# ---------------------------------------------------------------------------

def test_waveform_registry_has_all_types():
    from signal_generator import WAVEFORM_REGISTRY
    expected = {"constant", "sine", "square", "triangle", "sawtooth", "noise", "spike"}
    assert set(WAVEFORM_REGISTRY.keys()) == expected


def test_default_waveforms_is_list():
    from signal_generator import DEFAULT_WAVEFORMS
    assert isinstance(DEFAULT_WAVEFORMS, list)
    assert len(DEFAULT_WAVEFORMS) > 0


def test_default_waveforms_types_are_valid():
    from signal_generator import DEFAULT_WAVEFORMS, WAVEFORM_REGISTRY
    for wf in DEFAULT_WAVEFORMS:
        assert "type" in wf
        assert wf["type"] in WAVEFORM_REGISTRY


# ---------------------------------------------------------------------------
# Config parsing tests
# ---------------------------------------------------------------------------

def test_parse_waveform_config_default():
    from signal_generator import parse_waveform_config
    fns = parse_waveform_config(None)
    assert len(fns) == 4
    assert all(callable(fn) for fn in fns)


def test_parse_waveform_config_json_string():
    from signal_generator import parse_waveform_config
    config = json.dumps([{"type": "sine"}, {"type": "constant", "value": 5.0}])
    fns = parse_waveform_config(config)
    assert len(fns) == 2


def test_parse_waveform_config_unknown_type():
    from signal_generator import parse_waveform_config
    config = json.dumps([{"type": "unknown_wave"}])
    result = parse_waveform_config(config)
    assert result is None


def test_parse_waveform_config_bad_json():
    from signal_generator import parse_waveform_config
    result = parse_waveform_config("not valid json")
    assert result is None


def test_parse_waveform_config_missing_type():
    from signal_generator import parse_waveform_config
    config = json.dumps([{"frequency": 0.1}])
    result = parse_waveform_config(config)
    assert result is None


def test_parse_waveform_config_invalid_params():
    from signal_generator import parse_waveform_config
    config = json.dumps([{"type": "sine", "bogus_param": 1}])
    result = parse_waveform_config(config)
    assert result is None


def test_parse_waveform_config_negative_frequency():
    from signal_generator import parse_waveform_config
    config = json.dumps([{"type": "sine", "frequency": -1.0}])
    result = parse_waveform_config(config)
    assert result is None


def test_parse_waveform_config_negative_amplitude():
    from signal_generator import parse_waveform_config
    config = json.dumps([{"type": "sine", "amplitude": -5.0}])
    result = parse_waveform_config(config)
    assert result is None


def test_parse_waveform_config_negative_stddev():
    from signal_generator import parse_waveform_config
    config = json.dumps([{"type": "noise", "stddev": -0.5}])
    result = parse_waveform_config(config)
    assert result is None


def test_parse_waveform_config_probability_above_one():
    from signal_generator import parse_waveform_config
    config = json.dumps([{"type": "spike", "probability": 1.5}])
    result = parse_waveform_config(config)
    assert result is None


def test_parse_waveform_config_negative_probability():
    from signal_generator import parse_waveform_config
    config = json.dumps([{"type": "spike", "probability": -0.1}])
    result = parse_waveform_config(config)
    assert result is None


def test_parse_waveform_config_duty_cycle_out_of_range():
    from signal_generator import parse_waveform_config
    config = json.dumps([{"type": "square", "duty_cycle": 1.5}])
    result = parse_waveform_config(config)
    assert result is None


def test_parse_waveform_config_spike_min_greater_than_max():
    from signal_generator import parse_waveform_config
    config = json.dumps([{"type": "spike", "min_amplitude": 10.0, "max_amplitude": 5.0}])
    result = parse_waveform_config(config)
    assert result is None


def test_parse_output_config_defaults():
    from signal_generator import parse_output_config
    result = parse_output_config(None)
    assert result == ("signal", "value", {}, None)


def test_parse_output_config_custom():
    from signal_generator import parse_output_config
    args = {
        "measurement": "cpu_temp",
        "field": "temperature",
        "tags": '{"host": "server01", "location": "dc1"}',
        "target_database": "my_db",
    }
    result = parse_output_config(args)
    assert result == ("cpu_temp", "temperature", {"host": "server01", "location": "dc1"}, "my_db")


def test_parse_output_config_no_tags():
    from signal_generator import parse_output_config
    args = {"measurement": "test"}
    result = parse_output_config(args)
    assert result == ("test", "value", {}, None)


def test_parse_output_config_bad_tags_json():
    from signal_generator import parse_output_config
    args = {"tags": "not json"}
    result = parse_output_config(args)
    assert result is None


def test_parse_points_per_second_default():
    from signal_generator import parse_points_per_second
    assert parse_points_per_second(None) == 1.0


def test_parse_points_per_second_custom():
    from signal_generator import parse_points_per_second
    assert parse_points_per_second({"points_per_second": "10"}) == 10.0


def test_parse_points_per_second_invalid():
    from signal_generator import parse_points_per_second
    result = parse_points_per_second({"points_per_second": "abc"})
    assert result is None


def test_parse_points_per_second_zero():
    from signal_generator import parse_points_per_second
    result = parse_points_per_second({"points_per_second": "0"})
    assert result is None


def test_parse_points_per_second_negative():
    from signal_generator import parse_points_per_second
    result = parse_points_per_second({"points_per_second": "-5"})
    assert result is None


# ---------------------------------------------------------------------------
# Time series generation tests
# ---------------------------------------------------------------------------

def test_generate_timestamps_basic():
    from signal_generator import generate_timestamps
    ts = generate_timestamps(0.0, 5.0, 1.0)
    assert ts == [1.0, 2.0, 3.0, 4.0, 5.0]


def test_generate_timestamps_high_resolution():
    from signal_generator import generate_timestamps
    ts = generate_timestamps(0.0, 1.0, 2.0)
    assert len(ts) == 2
    assert abs(ts[0] - 0.5) < 1e-10
    assert abs(ts[1] - 1.0) < 1e-10


def test_generate_timestamps_no_points():
    from signal_generator import generate_timestamps
    ts = generate_timestamps(0.0, 0.1, 1.0)
    assert ts == []


def test_generate_timestamps_excludes_start():
    from signal_generator import generate_timestamps
    ts = generate_timestamps(10.0, 12.0, 1.0)
    assert ts[0] == 11.0
    assert ts[-1] == 12.0


def test_generate_signal_basic():
    from signal_generator import generate_signal, make_constant
    fn = make_constant(42.0)
    timestamps = [1.0, 2.0, 3.0]
    points = generate_signal(fn, timestamps)
    assert points == [(1.0, 42.0), (2.0, 42.0), (3.0, 42.0)]


def test_generate_signal_empty():
    from signal_generator import generate_signal, make_constant
    fn = make_constant(1.0)
    points = generate_signal(fn, [])
    assert points == []


# ---------------------------------------------------------------------------
# Writing and cache tests
# ---------------------------------------------------------------------------

def test_write_points_default_db(monkeypatch):
    import signal_generator
    monkeypatch.setattr(signal_generator, "LineBuilder", MockLineBuilder)
    from signal_generator import write_points
    mock = MockInfluxDB3Local()
    points = [(1000000000.0, 42.5), (2000000000.0, 43.1)]
    written = write_points(mock, points, "signal", "value", {}, None)
    assert written == 2
    # Single batch write
    assert len(mock.written_lines) == 1
    batch = mock.written_lines[0]
    # Batch should produce a newline-separated string with 2 lines
    built = batch.build()
    assert len(built.split("\n")) == 2
    assert len(mock.written_to_db) == 0


def test_write_points_target_db(monkeypatch):
    import signal_generator
    monkeypatch.setattr(signal_generator, "LineBuilder", MockLineBuilder)
    from signal_generator import write_points
    mock = MockInfluxDB3Local()
    points = [(1000000000.0, 42.5)]
    written = write_points(mock, points, "signal", "value", {}, "other_db")
    assert written == 1
    assert len(mock.written_lines) == 0
    assert len(mock.written_to_db) == 1
    assert mock.written_to_db[0][0] == "other_db"


def test_write_points_with_tags(monkeypatch):
    import signal_generator
    monkeypatch.setattr(signal_generator, "LineBuilder", MockLineBuilder)
    from signal_generator import write_points, _BatchLines
    mock = MockInfluxDB3Local()
    points = [(1000000000.0, 42.5)]
    written = write_points(mock, points, "signal", "value", {"host": "srv1"}, None)
    assert written == 1
    batch = mock.written_lines[0]
    built = batch.build()
    assert "host=srv1" in built


def test_write_points_empty(monkeypatch):
    import signal_generator
    monkeypatch.setattr(signal_generator, "LineBuilder", MockLineBuilder)
    from signal_generator import write_points
    mock = MockInfluxDB3Local()
    written = write_points(mock, [], "signal", "value", {}, None)
    assert written == 0
    assert len(mock.written_lines) == 0


def test_get_last_time_empty_cache():
    from signal_generator import get_last_time
    cache = MockCache()
    assert get_last_time(cache) is None


def test_get_set_last_time():
    from signal_generator import get_last_time, set_last_time
    cache = MockCache()
    set_last_time(cache, 12345.678)
    assert get_last_time(cache) == 12345.678


# ---------------------------------------------------------------------------
# Entry point tests
# ---------------------------------------------------------------------------

from datetime import datetime, timezone


def count_written_points(mock):
    """Count total data points across all batch writes (default db + target db)."""
    total = 0
    for batch in mock.written_lines:
        total += len(batch.build().split("\n"))
    for _, batch in mock.written_to_db:
        total += len(batch.build().split("\n"))
    return total


def test_process_scheduled_call_first_run(monkeypatch):
    import signal_generator
    monkeypatch.setattr(signal_generator, "LineBuilder", MockLineBuilder)
    from signal_generator import process_scheduled_call
    mock = MockInfluxDB3Local()
    call_time = datetime(2026, 4, 10, 12, 0, 0)
    process_scheduled_call(mock, call_time)
    # First run: no writes, only cache initialization
    assert len(mock.written_lines) == 0
    assert mock.cache.get("signal_gen:last_time") is not None


def test_process_scheduled_call_second_run(monkeypatch):
    import signal_generator
    monkeypatch.setattr(signal_generator, "LineBuilder", MockLineBuilder)
    from signal_generator import process_scheduled_call
    mock = MockInfluxDB3Local()
    # Simulate first run
    first_time = datetime(2026, 4, 10, 12, 0, 0)
    process_scheduled_call(mock, first_time)
    # Second run 5 seconds later — should produce 5 points at 1 pps
    second_time = datetime(2026, 4, 10, 12, 0, 5)
    process_scheduled_call(mock, second_time)
    assert count_written_points(mock) == 5


def test_process_scheduled_call_custom_pps(monkeypatch):
    import signal_generator
    monkeypatch.setattr(signal_generator, "LineBuilder", MockLineBuilder)
    from signal_generator import process_scheduled_call
    mock = MockInfluxDB3Local()
    args = {"points_per_second": "2"}
    first_time = datetime(2026, 4, 10, 12, 0, 0)
    process_scheduled_call(mock, first_time, args)
    # Second run 5 seconds later — 2 pps * 5s = 10 points
    second_time = datetime(2026, 4, 10, 12, 0, 5)
    process_scheduled_call(mock, second_time, args)
    assert count_written_points(mock) == 10


def test_process_scheduled_call_target_database(monkeypatch):
    import signal_generator
    monkeypatch.setattr(signal_generator, "LineBuilder", MockLineBuilder)
    from signal_generator import process_scheduled_call
    mock = MockInfluxDB3Local()
    args = {"target_database": "other_db"}
    first_time = datetime(2026, 4, 10, 12, 0, 0)
    process_scheduled_call(mock, first_time, args)
    second_time = datetime(2026, 4, 10, 12, 0, 5)
    process_scheduled_call(mock, second_time, args)
    # All writes should go to target db
    assert len(mock.written_lines) == 0
    assert count_written_points(mock) == 5
    assert all(db == "other_db" for db, _ in mock.written_to_db)


def test_process_scheduled_call_bad_waveform_config(monkeypatch):
    import signal_generator
    monkeypatch.setattr(signal_generator, "LineBuilder", MockLineBuilder)
    from signal_generator import process_scheduled_call
    mock = MockInfluxDB3Local()
    args = {"waveforms": "not valid json"}
    call_time = datetime(2026, 4, 10, 12, 0, 0)
    process_scheduled_call(mock, call_time, args)
    # Should log error and not crash
    assert len(mock.written_lines) == 0
    assert any("error" in log.lower() or "invalid" in log.lower() or "failed" in log.lower()
               for log in mock.logs["error"])


def test_process_scheduled_call_updates_cache(monkeypatch):
    import signal_generator
    monkeypatch.setattr(signal_generator, "LineBuilder", MockLineBuilder)
    from signal_generator import process_scheduled_call
    mock = MockInfluxDB3Local()
    first_time = datetime(2026, 4, 10, 12, 0, 0)
    process_scheduled_call(mock, first_time)
    cached_after_first = mock.cache.get("signal_gen:last_time")
    second_time = datetime(2026, 4, 10, 12, 0, 10)
    process_scheduled_call(mock, second_time)
    cached_after_second = mock.cache.get("signal_gen:last_time")
    # Cache should be updated to second call time
    assert cached_after_second > cached_after_first
