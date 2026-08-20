from collections import OrderedDict, namedtuple
from typing import Optional

import psutil
import pytest

class InfluxDBError(Exception):
    pass


class InvalidMeasurementError(InfluxDBError):
    pass


class InvalidKeyError(InfluxDBError):
    pass


class InvalidLineError(InfluxDBError):
    pass


class LineBuilder:
    def __init__(self, measurement: str):
        if " " in measurement:
            raise InvalidMeasurementError("Measurement name cannot contain spaces")
        self.measurement = measurement
        self.tags: OrderedDict[str, str] = OrderedDict()
        self.fields: OrderedDict[str, str] = OrderedDict()
        self._timestamp_ns: Optional[int] = None

    def _validate_key(self, key: str, key_type: str) -> None:
        if not key:
            raise InvalidKeyError(f"{key_type} key cannot be empty")
        if " " in key:
            raise InvalidKeyError(f"{key_type} key '{key}' cannot contain spaces")
        if "," in key:
            raise InvalidKeyError(f"{key_type} key '{key}' cannot contain commas")
        if "=" in key:
            raise InvalidKeyError(f"{key_type} key '{key}' cannot contain equals signs")

    def tag(self, key: str, value: str) -> "LineBuilder":
        self._validate_key(key, "tag")
        self.tags[key] = str(value)
        return self

    def uint64_field(self, key: str, value: int) -> "LineBuilder":
        self._validate_key(key, "field")
        if value < 0:
            raise ValueError(f"uint64 field '{key}' cannot be negative")
        self.fields[key] = f"{value}u"
        return self

    def int64_field(self, key: str, value: int) -> "LineBuilder":
        self._validate_key(key, "field")
        self.fields[key] = f"{value}i"
        return self

    def float64_field(self, key: str, value: float) -> "LineBuilder":
        self._validate_key(key, "field")
        self.fields[key] = f"{int(value)}.0" if value % 1 == 0 else str(value)
        return self

    def string_field(self, key: str, value: str) -> "LineBuilder":
        self._validate_key(key, "field")
        escaped_value = value.replace("\\", "\\\\").replace('"', '\\"')
        self.fields[key] = f'"{escaped_value}"'
        return self

    def bool_field(self, key: str, value: bool) -> "LineBuilder":
        self._validate_key(key, "field")
        self.fields[key] = "t" if value else "f"
        return self

    def time_ns(self, timestamp_ns: int) -> "LineBuilder":
        self._timestamp_ns = timestamp_ns
        return self

    def build(self) -> str:
        line = self.measurement.replace(",", "\\,").replace(" ", "\\ ")
        if self.tags:
            line += "," + ",".join(f"{k}={v}" for k, v in self.tags.items())
        if not self.fields:
            raise InvalidLineError(f"At least one field is required: {line}")
        line += " " + ",".join(f"{k}={v}" for k, v in self.fields.items())
        if self._timestamp_ns is not None:
            line += f" {self._timestamp_ns}"
        return line


class FakeCache:
    def __init__(self):
        self._values = {}
        self.ttls = {}

    def get(self, key, default=None, use_global=None):
        return self._values.get(key, default)

    def put(self, key, value, ttl=None, use_global=None):
        self._values[key] = value
        self.ttls[key] = ttl

    def delete(self, key, use_global=None):
        return self._values.pop(key, None) is not None


class FakeInfluxdb3Local:
    def __init__(self):
        self.cache = FakeCache()
        self.logs = []
        self.writes = []

    def info(self, message):
        self.logs.append(("info", message))

    def warn(self, message):
        self.logs.append(("warn", message))

    def error(self, message):
        self.logs.append(("error", message))

    def write(self, line):
        self.writes.append(line.build())

    def write_sync(self, line, no_sync=False):
        raise AssertionError("plugin must use the buffered write API")

    def messages(self, level):
        return [message for log_level, message in self.logs if log_level == level]


import system_metrics
from system_metrics import (
    _COLLECTORS,
    _CPU_TIME_FIELDS,
    _CPU_TIMES_STATE_KEY,
    _DISK_IO_STATE_KEY,
    _collect_with_retry,
    _cpu_percent_fields,
    _cpu_usage_percent,
    _disk_io_sample,
    _disk_performance_fields,
    _float_fields,
    _load_config,
    _uint_fields,
    collect_cpu_metrics,
    collect_disk_metrics,
    collect_memory_metrics,
    collect_network_metrics,
    process_scheduled_call,
)

FakePartition = namedtuple("FakePartition", "device mountpoint fstype")
FakeUsage = namedtuple("FakeUsage", "total used free percent")
FakeNetIO = namedtuple(
    "FakeNetIO",
    "bytes_sent bytes_recv packets_sent packets_recv errin errout dropin dropout",
)


class FakeDiskIO:
    """Stand-in for psutil's sdiskio, minus the counters a platform may not expose."""

    def __init__(self, **counters):
        self.__dict__.update(counters)


@pytest.fixture
def client():
    return FakeInfluxdb3Local()


@pytest.fixture(autouse=True)
def line_builder(monkeypatch):
    """The engine injects LineBuilder as a global; tests use the vendored copy."""
    monkeypatch.setattr(system_metrics, "LineBuilder", LineBuilder, raising=False)


@pytest.fixture
def plugin_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    return tmp_path


def raiser(error):
    """Build a stand-in that raises instead of returning psutil data."""

    def raise_error(*args, **kwargs):
        raise error

    return raise_error


def parse_line(text):
    """Split a built line into (measurement, tags, fields)."""
    head, field_part = text.split(" ", 1)
    parts = head.split(",")
    tags = dict(item.split("=", 1) for item in parts[1:])
    fields = dict(item.split("=", 1) for item in field_part.split(","))
    return parts[0], tags, fields


def measurements(lines):
    return [parse_line(line.build())[0] for line in lines]


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------


def test_config_defaults(client):
    config = _load_config(client, None, "task")

    assert config["hostname"] == "localhost"
    assert config["max_retries"] == 3
    assert all(
        config[key]
        for key in ("include_cpu", "include_memory", "include_disk", "include_network")
    )


def test_config_casts_inline_args(client):
    config = _load_config(
        client,
        {
            "hostname": "web-1",
            "include_cpu": "no",
            "include_memory": "0",
            "include_disk": "off",
            "include_network": "yes",
            "max_retries": "5",
        },
        "task",
    )

    assert config["hostname"] == "web-1"
    assert config["include_cpu"] is False
    assert config["include_memory"] is False
    assert config["include_disk"] is False
    assert config["include_network"] is True
    assert config["max_retries"] == 5


@pytest.mark.parametrize(
    "args, expected",
    [
        ({"include_cpu": "maybe"}, "Invalid boolean: 'maybe'"),
        ({"max_retries": "abc"}, "Invalid integer: 'abc'"),
        ({"max_retries": "-1"}, "below minimum 0"),
    ],
)
def test_config_rejects_invalid_values(client, args, expected):
    assert _load_config(client, args, "task") is None
    assert expected in client.messages("error")[0]


def test_config_toml_overrides_inline_args(client, plugin_dir):
    (plugin_dir / "sm.toml").write_text(
        'hostname = "from-toml"\ninclude_cpu = false\nmax_retries = 1\n'
    )

    config = _load_config(
        client,
        {"hostname": "from-args", "config_file_path": "sm.toml"},
        "task",
    )

    assert config["hostname"] == "from-toml"
    assert config["include_cpu"] is False
    assert config["max_retries"] == 1
    assert config["include_memory"] is True  # untouched default
    assert "Loaded configuration from sm.toml" in client.messages("info")[0]


def test_config_missing_toml_keeps_inline_args(client, plugin_dir):
    config = _load_config(
        client,
        {"hostname": "fallback", "config_file_path": "absent.toml"},
        "task",
    )

    assert config["hostname"] == "fallback"
    error = client.messages("error")[0]
    assert "Failed to apply config file 'absent.toml'" in error
    assert "Continuing with inline arguments" in error


def test_config_ignores_file_with_wrong_extension(client, plugin_dir):
    (plugin_dir / "sm.yaml").write_text('hostname = "from-file"\n')

    config = _load_config(
        client,
        {"hostname": "from-args", "config_file_path": "sm.yaml"},
        "task",
    )

    assert config["hostname"] == "from-args"
    assert "expected a .toml file" in client.messages("error")[0]


def test_config_invalid_toml_value_keeps_inline_args(client, plugin_dir):
    (plugin_dir / "sm.toml").write_text('include_cpu = "maybe"\n')

    config = _load_config(
        client,
        {"include_cpu": "false", "config_file_path": "sm.toml"},
        "task",
    )

    assert config["include_cpu"] is False
    assert "Invalid boolean: 'maybe'" in client.messages("error")[0]


def test_config_accepts_absolute_toml_path(client, tmp_path, monkeypatch):
    monkeypatch.delenv("PLUGIN_DIR", raising=False)
    config_file = tmp_path / "abs.toml"
    config_file.write_text('hostname = "abs-path"\n')

    config = _load_config(client, {"config_file_path": str(config_file)}, "task")

    assert config["hostname"] == "abs-path"


# --------------------------------------------------------------------------
# Field typing helpers
# --------------------------------------------------------------------------


def test_field_helpers_tag_values_with_their_type():
    assert _float_fields(percent=1.5) == {"percent": (1.5, "float")}
    assert _uint_fields(total=10, used=4) == {
        "total": (10, "uint"),
        "used": (4, "uint"),
    }


# --------------------------------------------------------------------------
# CPU shares
# --------------------------------------------------------------------------


def _cpu_sample(**counters):
    sample = {name: 0.0 for name in _CPU_TIME_FIELDS}
    sample.update(counters)
    return sample


def test_cpu_percent_fields_computes_shares_of_the_interval():
    previous = _cpu_sample(user=100.0, system=50.0, idle=850.0)
    current = _cpu_sample(user=140.0, system=60.0, idle=1000.0)

    percentages = _cpu_percent_fields(previous, current)

    assert percentages["user"] == 20.0
    assert percentages["system"] == 5.0
    assert percentages["idle"] == 75.0
    assert _cpu_usage_percent(percentages) == 25.0


def test_cpu_percent_fields_excludes_guest_time_from_the_total():
    current = _cpu_sample(user=50.0, idle=50.0, guest=50.0)

    assert _cpu_percent_fields(_cpu_sample(), current)["user"] == 50.0


def test_cpu_percent_fields_requires_progress():
    assert _cpu_percent_fields(_cpu_sample(idle=10.0), _cpu_sample(idle=10.0)) is None


def test_cpu_percent_fields_skips_on_counter_reset():
    previous = _cpu_sample(idle=100.0, user=50.0)
    current = _cpu_sample(idle=200.0, user=1.0)

    assert _cpu_percent_fields(previous, current) is None


# --------------------------------------------------------------------------
# Disk I/O rates
# --------------------------------------------------------------------------


def _sample(timestamp_ns, **counters):
    defaults = {
        "read_count": 0,
        "write_count": 0,
        "read_bytes": 0,
        "write_bytes": 0,
        "read_time": 0,
        "write_time": 0,
        "busy_time": 0,
    }
    defaults.update(counters)
    defaults["timestamp_ns"] = timestamp_ns
    return defaults


def test_disk_performance_computes_rates_over_the_interval():
    previous = _sample(
        0,
        read_count=100,
        write_count=200,
        read_bytes=1000,
        write_bytes=2000,
        read_time=50,
        write_time=100,
        busy_time=500,
    )
    current = _sample(
        2_000_000_000,
        read_count=110,
        write_count=220,
        read_bytes=3000,
        write_bytes=6000,
        read_time=70,
        write_time=140,
        busy_time=900,
    )

    fields = _disk_performance_fields(previous, current)

    assert fields == {
        "read_bytes_per_sec": (1000.0, "float"),
        "write_bytes_per_sec": (2000.0, "float"),
        "read_iops": (5.0, "float"),
        "write_iops": (10.0, "float"),
        "avg_read_latency_ms": (2.0, "float"),
        "avg_write_latency_ms": (2.0, "float"),
        "util_percent": (20.0, "float"),
    }


def test_disk_performance_idle_device_reports_zero_latency():
    fields = _disk_performance_fields(_sample(0), _sample(1_000_000_000))

    assert fields["avg_read_latency_ms"] == (0, "float")
    assert fields["read_iops"] == (0.0, "float")


def test_disk_performance_requires_a_positive_interval():
    assert _disk_performance_fields(_sample(5), _sample(5)) is None


def test_disk_performance_skips_on_counter_reset():
    previous = _sample(0, read_bytes=5000)
    current = _sample(1_000_000_000, read_bytes=10)

    assert _disk_performance_fields(previous, current) is None


def test_disk_io_sample_defaults_counters_the_platform_omits():
    sample = _disk_io_sample(FakeDiskIO(read_count=1), 42)

    assert sample["read_count"] == 1
    assert sample["busy_time"] == 0
    assert sample["timestamp_ns"] == 42


# --------------------------------------------------------------------------
# Collectors
# --------------------------------------------------------------------------


def _age_cpu_sample(sample, seconds=100.0):
    """Shift a cached sample backwards so the next run sees a non-zero delta."""
    shift = lambda counters: {  # noqa: E731
        name: max(0.0, value - seconds) for name, value in counters.items()
    }
    return {
        "total": shift(sample["total"]),
        "per_cpu": [shift(core) for core in sample["per_cpu"]],
    }


def test_cpu_metrics_first_run_reports_no_percentages(client):
    lines = collect_cpu_metrics(client, "web-1", "task")

    measurement, tags, fields = parse_line(lines[0].build())
    assert measurement == "system_cpu"
    assert tags == {"host": "web-1", "cpu": "total"}
    assert {"load1", "load15", "ctx_switches", "syscalls"} <= set(fields)
    assert not {"user", "system", "idle", "iowait"} & set(fields)
    assert fields["ctx_switches"].endswith("u")  # uint64
    assert "No previous CPU sample" in client.messages("info")[0]
    assert client.cache.get(_CPU_TIMES_STATE_KEY)["per_cpu"]


def test_cpu_metrics_second_run_reports_percentages(client):
    collect_cpu_metrics(client, "web-1", "task")
    client.cache.put(
        _CPU_TIMES_STATE_KEY, _age_cpu_sample(client.cache.get(_CPU_TIMES_STATE_KEY))
    )
    client.logs.clear()

    lines = collect_cpu_metrics(client, "web-1", "task")

    _, _, fields = parse_line(lines[0].build())
    assert {"user", "system", "idle", "iowait", "guest_nice"} <= set(fields)
    _, core_tags, core_fields = parse_line(lines[1].build())
    assert measurements(lines[1:]) == ["system_cpu_cores"] * (len(lines) - 1)
    assert core_tags == {"host": "web-1", "core": "0"}
    assert {"usage", "idle"} <= set(core_fields)
    assert not client.logs


def test_cpu_metrics_warn_when_per_core_frequency_is_unavailable(client, monkeypatch):
    real_cpu_freq = psutil.cpu_freq

    def cpu_freq(percpu=False):
        if percpu:
            raise NotImplementedError("no per-core frequency")
        return real_cpu_freq()

    monkeypatch.setattr(psutil, "cpu_freq", cpu_freq)

    lines = collect_cpu_metrics(client, "web-1", "task")

    # first run: neither percentages nor frequencies, so no core line has fields
    assert measurements(lines) == ["system_cpu"]
    assert "Error reading per-core CPU frequency" in client.messages("warn")[0]


def test_memory_metrics_emit_memory_and_swap(client):
    lines = collect_memory_metrics(client, "web-1", "task")

    assert measurements(lines)[:2] == ["system_memory", "system_swap"]
    _, tags, fields = parse_line(lines[0].build())
    assert tags == {"host": "web-1"}
    assert fields["total"].endswith("u")
    assert not fields["percent"].endswith("u")


def test_memory_metrics_skip_faults_when_psutil_denies_access(client, monkeypatch):
    monkeypatch.setattr(psutil, "Process", raiser(psutil.AccessDenied()))

    lines = collect_memory_metrics(client, "web-1", "task")

    assert measurements(lines) == ["system_memory", "system_swap"]
    assert not client.logs


def test_network_metrics_emit_one_line_per_interface(client, monkeypatch):
    monkeypatch.setattr(
        psutil,
        "net_io_counters",
        lambda pernic=False: {
            "eth0": FakeNetIO(1, 2, 3, 4, 5, 6, 7, 8),
            "lo": FakeNetIO(9, 10, 11, 12, 13, 14, 15, 16),
        },
    )

    lines = collect_network_metrics(client, "web-1", "task")

    assert measurements(lines) == ["system_network"] * 2
    _, tags, fields = parse_line(lines[0].build())
    assert tags == {"host": "web-1", "interface": "eth0"}
    assert fields["bytes_sent"] == "1u"


def test_disk_metrics_skips_unreadable_mountpoint(client, monkeypatch):
    monkeypatch.setattr(
        psutil,
        "disk_partitions",
        lambda all=False: [
            FakePartition("/dev/sda1", "/", "ext4"),
            FakePartition("/dev/sr0", "/media/cdrom", "iso9660"),
        ],
    )

    def disk_usage(mountpoint):
        if mountpoint == "/":
            return FakeUsage(100, 60, 40, 60.0)
        raise OSError("No such device")

    monkeypatch.setattr(psutil, "disk_usage", disk_usage)
    monkeypatch.setattr(psutil, "disk_io_counters", lambda perdisk=False: {})

    lines = collect_disk_metrics(client, "web-1", "task")

    assert measurements(lines) == ["system_disk_usage"]
    _, tags, _ = parse_line(lines[0].build())
    assert tags["mountpoint"] == "/"


def _patch_disk_io(monkeypatch, **counters):
    monkeypatch.setattr(psutil, "disk_partitions", lambda all=False: [])
    monkeypatch.setattr(
        psutil, "disk_io_counters", lambda perdisk=False: {"sda": FakeDiskIO(**counters)}
    )


def test_disk_metrics_first_run_caches_counters_without_rates(client, monkeypatch):
    _patch_disk_io(
        monkeypatch,
        read_count=1,
        write_count=2,
        read_bytes=10,
        write_bytes=20,
        read_time=3,
        write_time=4,
        busy_time=5,
        read_merged_count=0,
        write_merged_count=0,
    )
    monkeypatch.setattr(system_metrics.time, "time_ns", lambda: 1_000_000_000)

    lines = collect_disk_metrics(client, "web-1", "task")

    assert measurements(lines) == ["system_disk_io"]
    assert "No previous disk I/O sample" in client.messages("info")[0]
    cached = client.cache.get(_DISK_IO_STATE_KEY)
    assert cached["sda"]["read_bytes"] == 10
    assert client.cache.ttls[_DISK_IO_STATE_KEY] is None


def test_disk_metrics_second_run_emits_rates(client, monkeypatch):
    client.cache.put(
        _DISK_IO_STATE_KEY,
        {"sda": _sample(1_000_000_000, read_bytes=10, write_bytes=20)},
    )
    _patch_disk_io(
        monkeypatch,
        read_count=0,
        write_count=0,
        read_bytes=1010,
        write_bytes=20,
        read_time=0,
        write_time=0,
        busy_time=0,
        read_merged_count=0,
        write_merged_count=0,
    )
    monkeypatch.setattr(system_metrics.time, "time_ns", lambda: 3_000_000_000)

    lines = collect_disk_metrics(client, "web-1", "task")

    assert measurements(lines) == ["system_disk_io", "system_disk_performance"]
    _, tags, fields = parse_line(lines[1].build())
    assert tags == {"host": "web-1", "device": "sda"}
    assert fields["read_bytes_per_sec"] == "500.0"
    assert fields["write_bytes_per_sec"] == "0.0"
    assert not client.messages("info")


def test_disk_metrics_warns_when_io_counters_fail(client, monkeypatch):
    monkeypatch.setattr(
        psutil,
        "disk_partitions",
        lambda all=False: [FakePartition("/dev/sda1", "/", "ext4")],
    )
    monkeypatch.setattr(psutil, "disk_usage", lambda mp: FakeUsage(100, 60, 40, 60.0))
    monkeypatch.setattr(psutil, "disk_io_counters", raiser(psutil.Error()))

    lines = collect_disk_metrics(client, "web-1", "task")

    assert measurements(lines) == ["system_disk_usage"]
    assert "Error collecting disk I/O metrics" in client.messages("warn")[0]


# --------------------------------------------------------------------------
# Retry wrapper
# --------------------------------------------------------------------------


def test_collect_with_retry_returns_lines_on_first_attempt(client):
    def collect(influxdb3_local, hostname, task_id):
        return ["line"]

    assert _collect_with_retry(client, collect, "CPU", "web-1", 3, "task") == ["line"]
    assert not client.logs


def test_collect_with_retry_recovers_after_failures(client):
    attempts = []

    def collect(influxdb3_local, hostname, task_id):
        attempts.append(hostname)
        if len(attempts) < 3:
            raise RuntimeError("flaky")
        return ["line"]

    assert _collect_with_retry(client, collect, "disk", "web-1", 3, "task") == ["line"]
    assert len(attempts) == 3
    assert len(client.messages("warn")) == 2
    assert "disk metrics collection attempt 1 failed" in client.messages("warn")[0]


def test_collect_with_retry_gives_up_after_max_retries(client):
    collect = raiser(RuntimeError("always down"))

    assert _collect_with_retry(client, collect, "network", "web-1", 0, "task") is None
    assert not client.messages("warn")
    assert (
        "Failed to collect network metrics after 0 retries: always down"
        in client.messages("error")[0]
    )


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------


def _fake_collectors(calls):
    """Mirror the real collector table, recording calls instead of reading psutil."""

    def make(name):
        def collect(influxdb3_local, hostname, task_id):
            calls.append((name, hostname))
            return [
                LineBuilder(f"fake_{name}").tag("host", hostname).float64_field("v", 1.0)
            ]

        return collect

    collectors = []
    for config_key, metric_type, _ in _COLLECTORS:
        name = config_key.removeprefix("include_")
        collectors.append((config_key, metric_type, make(name)))
    return tuple(collectors)


def test_process_runs_every_enabled_collector(client, monkeypatch):
    calls = []
    monkeypatch.setattr(system_metrics, "_COLLECTORS", _fake_collectors(calls))

    process_scheduled_call(client, None, {"hostname": "web-1"})

    assert [name for name, _ in calls] == ["cpu", "memory", "disk", "network"]
    assert all(hostname == "web-1" for _, hostname in calls)
    assert client.writes == [
        "fake_cpu,host=web-1 v=1.0",
        "fake_memory,host=web-1 v=1.0",
        "fake_disk,host=web-1 v=1.0",
        "fake_network,host=web-1 v=1.0",
    ]
    assert "Successfully collected system metrics" in client.messages("info")[-1]


def test_process_skips_disabled_collectors(client, monkeypatch):
    calls = []
    monkeypatch.setattr(system_metrics, "_COLLECTORS", _fake_collectors(calls))

    process_scheduled_call(
        client, None, {"include_cpu": "false", "include_network": "false"}
    )

    assert [name for name, _ in calls] == ["memory", "disk"]


def test_process_writes_nothing_when_config_is_invalid(client, monkeypatch):
    calls = []
    monkeypatch.setattr(system_metrics, "_COLLECTORS", _fake_collectors(calls))

    process_scheduled_call(client, None, {"include_cpu": "maybe"})

    assert not calls
    assert not client.writes
    assert client.messages("error")
    assert not client.messages("info")


def test_process_keeps_other_collectors_when_one_fails(client, monkeypatch):
    def cpu(influxdb3_local, hostname, task_id):
        return [LineBuilder("fake_cpu").float64_field("v", 1.0)]

    def network(influxdb3_local, hostname, task_id):
        return [LineBuilder("fake_net").float64_field("v", 1.0)]

    monkeypatch.setattr(
        system_metrics,
        "_COLLECTORS",
        (
            ("include_cpu", "CPU", cpu),
            ("include_disk", "disk", raiser(RuntimeError("disk gone"))),
            ("include_network", "network", network),
        ),
    )

    process_scheduled_call(client, None, {"max_retries": "0"})

    assert client.writes == ["fake_cpu v=1.0", "fake_net v=1.0"]
    assert (
        "Failed to collect disk metrics after 0 retries: disk gone"
        in client.messages("error")[0]
    )
    assert "skipped after repeated failures: disk" in client.messages("error")[1]
    assert not any("Successfully" in message for message in client.messages("info"))