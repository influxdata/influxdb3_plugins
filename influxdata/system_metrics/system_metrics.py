"""
{
    "plugin_type": ["scheduled"],
    "scheduled_args_config": [
        {
            "name": "hostname",
            "example": "localhost",
            "description": "Hostname to tag metrics with",
            "required": false
        },
        {
            "name": "include_cpu",
            "example": "true",
            "description": "Include CPU metrics collection",
            "required": false
        },
        {
            "name": "include_memory",
            "example": "true",
            "description": "Include memory metrics collection",
            "required": false
        },
        {
            "name": "include_disk",
            "example": "true",
            "description": "Include disk metrics collection",
            "required": false
        },
        {
            "name": "include_network",
            "example": "true",
            "description": "Include network metrics collection",
            "required": false
        },
        {
            "name": "max_retries",
            "example": "3",
            "description": "Maximum number of retry attempts on failure",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "system_metrics_config_scheduler.toml",
            "description": "Path to a TOML configuration file, relative to the plugin directory",
            "required": false
        }
    ]
}
"""

import time
import uuid

import psutil
from influxdata_plugin_utils.config import Validator, load_plugin_config
from influxdata_plugin_utils.parsing import parse_bool, parse_int
from influxdata_plugin_utils.write import build_line_typed, write_data

_VALIDATORS = [
    Validator("hostname", default="localhost", cast=str),
    Validator("include_cpu", default=True, cast=parse_bool),
    Validator("include_memory", default=True, cast=parse_bool),
    Validator("include_disk", default=True, cast=parse_bool),
    Validator("include_network", default=True, cast=parse_bool),
    Validator("max_retries", default=3, cast=lambda raw: parse_int(raw, minimum=0)),
]

# Cached psutil counters, used to derive rates and shares between two runs
_DISK_IO_STATE_KEY = "system_metrics:disk_io"
_DISK_IO_COUNTERS = (
    "read_count",
    "write_count",
    "read_bytes",
    "write_bytes",
    "read_time",
    "write_time",
    "busy_time",
)

_CPU_TIMES_STATE_KEY = "system_metrics:cpu_times"
_CPU_TIME_FIELDS = (
    "user",
    "system",
    "idle",
    "iowait",
    "nice",
    "irq",
    "softirq",
    "steal",
    "guest",
    "guest_nice",
)


def _load_config(influxdb3_local, args: dict, task_id: str) -> dict | None:
    """
    Load the plugin configuration, applying defaults and type casts.

    Values from a TOML file referenced by 'config_file_path' override the inline
    trigger arguments. A config file that cannot be read is reported and skipped,
    so collection continues with the inline arguments.

    Args:
        influxdb3_local: InfluxDB client instance.
        args (dict): Runtime arguments of the trigger.
        task_id (str): Unique task identifier.

    Returns:
        dict | None: Config values keyed by lower-case name, or None if the
        inline arguments themselves are invalid.
    """
    args = args or {}
    config_file_path = args.get("config_file_path")
    if config_file_path and not str(config_file_path).endswith(".toml"):
        influxdb3_local.error(
            f"[{task_id}] Invalid config file format: expected a .toml file"
        )
        config_file_path = None

    try:
        loaded = load_plugin_config(args, validators=_VALIDATORS, source="args")
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to load configuration: {e}")
        return None

    if config_file_path:
        try:
            loaded = load_plugin_config(args, validators=_VALIDATORS, source="merge")
            influxdb3_local.info(
                f"[{task_id}] Loaded configuration from {config_file_path}"
            )
        except Exception as e:
            influxdb3_local.error(
                f"[{task_id}] Failed to apply config file '{config_file_path}': {e}. "
                f"Continuing with inline arguments"
            )

    return {key.lower(): value for key, value in loaded.as_dict().items()}


def _float_fields(**values) -> dict:
    """Type every value as a float64 field."""
    return {name: (value, "float") for name, value in values.items()}


def _uint_fields(**values) -> dict:
    """Type every value as a uint64 field."""
    return {name: (value, "uint") for name, value in values.items()}


def _cpu_times_sample(times) -> dict:
    """Snapshot the cumulative CPU time counters of one CPU."""
    return {name: getattr(times, name, 0.0) for name in _CPU_TIME_FIELDS}


def _cpu_total_time(sample: dict) -> float:
    # guest time is already counted inside user and nice, so it is excluded here
    return sum(sample.values()) - sample["guest"] - sample["guest_nice"]


def _cpu_percent_fields(previous: dict, current: dict) -> dict | None:
    """
    Derive the share of every CPU state from two cumulative samples.

    Returns None when the samples cannot be compared, which happens on the first
    run of the plugin and after the counters are reset.
    """
    total = _cpu_total_time(current) - _cpu_total_time(previous)
    if total <= 0:
        return None

    percentages = {}
    for name in _CPU_TIME_FIELDS:
        delta = current[name] - previous[name]
        if delta < 0:
            return None
        percentages[name] = round(min(100.0, delta * 100.0 / total), 1)
    return percentages


def _cpu_usage_percent(percentages: dict) -> float:
    """Busy share of a CPU: everything except idle and waiting for I/O."""
    return round(max(0.0, 100.0 - percentages["idle"] - percentages["iowait"]), 1)


def collect_cpu_metrics(influxdb3_local, hostname: str, task_id: str) -> list:
    """Build overall CPU lines plus one line per core."""
    cpu_freq = psutil.cpu_freq(percpu=False)
    cpu_stats = psutil.cpu_stats()
    load_avg = psutil.getloadavg()

    previous: dict = influxdb3_local.cache.get(_CPU_TIMES_STATE_KEY, default={})
    current: dict = {
        "total": _cpu_times_sample(psutil.cpu_times()),
        "per_cpu": [_cpu_times_sample(times) for times in psutil.cpu_times(percpu=True)],
    }
    influxdb3_local.cache.put(_CPU_TIMES_STATE_KEY, current, None)

    if not previous:
        influxdb3_local.info(
            f"[{task_id}] No previous CPU sample, "
            f"usage percentages start with the next run"
        )

    typed_fields = {
        **_float_fields(
            frequency_current=getattr(cpu_freq, "current", 0),
            frequency_min=getattr(cpu_freq, "min", 0),
            frequency_max=getattr(cpu_freq, "max", 0),
            load1=load_avg[0],
            load5=load_avg[1],
            load15=load_avg[2],
        ),
        **_uint_fields(
            ctx_switches=cpu_stats.ctx_switches,
            interrupts=cpu_stats.interrupts,
            soft_interrupts=cpu_stats.soft_interrupts,
            syscalls=getattr(cpu_stats, "syscalls", 0),
        ),
    }
    if previous:
        total_percentages = _cpu_percent_fields(previous["total"], current["total"])
        if total_percentages:
            typed_fields.update(_float_fields(**total_percentages))

    lines = [
        build_line_typed(
            LineBuilder,
            "system_cpu",
            tags={"host": hostname, "cpu": "total"},
            typed_fields=typed_fields,
        )
    ]

    try:
        per_cpu_freq = psutil.cpu_freq(percpu=True)
    except Exception as e:
        per_cpu_freq = []
        influxdb3_local.warn(f"[{task_id}] Error reading per-core CPU frequency: {e}")

    previous_per_cpu: list = previous.get("per_cpu", [])
    for core_id, core_times in enumerate(current["per_cpu"]):
        core_fields = {}

        if core_id < len(previous_per_cpu):
            core_percentages = _cpu_percent_fields(previous_per_cpu[core_id], core_times)
            if core_percentages:
                core_fields.update(_float_fields(**core_percentages))
                core_fields.update(
                    _float_fields(usage=_cpu_usage_percent(core_percentages))
                )

        if per_cpu_freq and core_id < len(per_cpu_freq):
            freq = per_cpu_freq[core_id]
            core_fields.update(
                _float_fields(
                    frequency_current=freq.current,
                    frequency_min=getattr(freq, "min", 0),
                    frequency_max=getattr(freq, "max", 0),
                )
            )

        if not core_fields:
            continue

        lines.append(
            build_line_typed(
                LineBuilder,
                "system_cpu_cores",
                tags={"host": hostname, "core": str(core_id)},
                typed_fields=core_fields,
            )
        )

    return lines


def collect_memory_metrics(influxdb3_local, hostname: str, task_id: str) -> list:
    """Build memory, swap, and page fault lines."""
    mem = psutil.virtual_memory()
    swap = psutil.swap_memory()

    lines = [
        build_line_typed(
            LineBuilder,
            "system_memory",
            tags={"host": hostname},
            typed_fields={
                **_uint_fields(
                    total=mem.total,
                    available=mem.available,
                    used=mem.used,
                    free=mem.free,
                    active=getattr(mem, "active", 0),
                    inactive=getattr(mem, "inactive", 0),
                    buffers=getattr(mem, "buffers", 0),
                    cached=getattr(mem, "cached", 0),
                    shared=getattr(mem, "shared", 0),
                    slab=getattr(mem, "slab", 0),
                ),
                **_float_fields(percent=mem.percent),
            },
        ),
        build_line_typed(
            LineBuilder,
            "system_swap",
            tags={"host": hostname},
            typed_fields={
                **_uint_fields(
                    total=swap.total,
                    used=swap.used,
                    free=swap.free,
                    sin=swap.sin,
                    sout=swap.sout,
                ),
                **_float_fields(percent=swap.percent),
            },
        ),
    ]

    try:
        page_faults = psutil.Process().memory_full_info()
        lines.append(
            build_line_typed(
                LineBuilder,
                "system_memory_faults",
                tags={"host": hostname},
                typed_fields=_uint_fields(
                    page_faults=getattr(page_faults, "num_page_faults", 0),
                    major_faults=getattr(page_faults, "maj_faults", 0),
                    minor_faults=getattr(page_faults, "min_faults", 0),
                    rss=getattr(page_faults, "rss", 0),
                    vms=getattr(page_faults, "vms", 0),
                    dirty=getattr(page_faults, "dirty", 0),
                    uss=getattr(page_faults, "uss", 0),
                    pss=getattr(page_faults, "pss", 0),
                ),
            )
        )
    except psutil.Error:
        pass

    return lines


def _disk_io_sample(stats, timestamp_ns: int) -> dict:
    """Snapshot the cumulative I/O counters of one device."""
    sample = {name: getattr(stats, name, 0) for name in _DISK_IO_COUNTERS}
    sample["timestamp_ns"] = timestamp_ns
    return sample


def _disk_performance_fields(previous: dict, current: dict) -> dict | None:
    """
    Derive I/O rates from two counter samples of the same device.

    Returns None when the samples cannot be compared, which happens on the first
    run of the plugin and after the counters are reset.
    """
    elapsed_seconds = (current["timestamp_ns"] - previous["timestamp_ns"]) / 1_000_000_000
    if elapsed_seconds <= 0:
        return None

    deltas = {name: current[name] - previous[name] for name in _DISK_IO_COUNTERS}
    if any(delta < 0 for delta in deltas.values()):
        return None

    return _float_fields(
        read_bytes_per_sec=deltas["read_bytes"] / elapsed_seconds,
        write_bytes_per_sec=deltas["write_bytes"] / elapsed_seconds,
        read_iops=deltas["read_count"] / elapsed_seconds,
        write_iops=deltas["write_count"] / elapsed_seconds,
        avg_read_latency_ms=(
            deltas["read_time"] / deltas["read_count"] if deltas["read_count"] else 0
        ),
        avg_write_latency_ms=(
            deltas["write_time"] / deltas["write_count"] if deltas["write_count"] else 0
        ),
        util_percent=deltas["busy_time"] / (elapsed_seconds * 1000) * 100,
    )


def collect_disk_metrics(influxdb3_local, hostname: str, task_id: str) -> list:
    """Build per-partition usage lines plus per-device I/O and rate lines."""
    lines = []

    for partition in psutil.disk_partitions(all=False):
        try:
            usage = psutil.disk_usage(partition.mountpoint)
        except OSError:
            # an unreadable or disconnected mountpoint must not abort the collector
            continue
        lines.append(
            build_line_typed(
                LineBuilder,
                "system_disk_usage",
                tags={
                    "host": hostname,
                    "device": partition.device,
                    "mountpoint": partition.mountpoint,
                    "fstype": partition.fstype,
                },
                typed_fields={
                    **_uint_fields(total=usage.total, used=usage.used, free=usage.free),
                    **_float_fields(percent=usage.percent),
                },
            )
        )

    try:
        disk_io = psutil.disk_io_counters(perdisk=True)
    except (psutil.Error, AttributeError) as e:
        influxdb3_local.warn(f"[{task_id}] Error collecting disk I/O metrics: {e}")
        return lines

    timestamp_ns: int = time.time_ns()
    previous_samples: dict = influxdb3_local.cache.get(_DISK_IO_STATE_KEY, default={})
    current_samples: dict = {}

    if not previous_samples:
        influxdb3_local.info(
            f"[{task_id}] No previous disk I/O sample, "
            f"performance rates start with the next run"
        )

    for device, stats in disk_io.items():
        lines.append(
            build_line_typed(
                LineBuilder,
                "system_disk_io",
                tags={"host": hostname, "device": device},
                typed_fields=_uint_fields(
                    reads=stats.read_count,
                    writes=stats.write_count,
                    read_bytes=stats.read_bytes,
                    write_bytes=stats.write_bytes,
                    read_time=stats.read_time,
                    write_time=stats.write_time,
                    busy_time=getattr(stats, "busy_time", 0),
                    read_merged_count=getattr(stats, "read_merged_count", 0),
                    write_merged_count=getattr(stats, "write_merged_count", 0),
                ),
            )
        )

        current_samples[device] = _disk_io_sample(stats, timestamp_ns)
        previous_sample = previous_samples.get(device)
        if previous_sample is None:
            continue

        performance = _disk_performance_fields(previous_sample, current_samples[device])
        if performance is None:
            continue

        lines.append(
            build_line_typed(
                LineBuilder,
                "system_disk_performance",
                tags={"host": hostname, "device": device},
                typed_fields=performance,
            )
        )

    influxdb3_local.cache.put(_DISK_IO_STATE_KEY, current_samples, ttl=None)

    return lines


def collect_network_metrics(influxdb3_local, hostname: str, task_id: str) -> list:
    """Build one line per network interface."""
    return [
        build_line_typed(
            LineBuilder,
            "system_network",
            tags={"host": hostname, "interface": interface},
            typed_fields=_uint_fields(
                bytes_sent=stats.bytes_sent,
                bytes_recv=stats.bytes_recv,
                packets_sent=stats.packets_sent,
                packets_recv=stats.packets_recv,
                errin=stats.errin,
                errout=stats.errout,
                dropin=stats.dropin,
                dropout=stats.dropout,
            ),
        )
        for interface, stats in psutil.net_io_counters(pernic=True).items()
    ]


_COLLECTORS = (
    ("include_cpu", "CPU", collect_cpu_metrics),
    ("include_memory", "memory", collect_memory_metrics),
    ("include_disk", "disk", collect_disk_metrics),
    ("include_network", "network", collect_network_metrics),
)


def _collect_with_retry(
    influxdb3_local,
    collect,
    metric_type: str,
    hostname: str,
    max_retries: int,
    task_id: str,
) -> list | None:
    """
    Run one collector, retrying on failure.

    Returns the lines it built, or None once the retries are used up, so that the
    remaining collectors still run and their points reach the database.
    """
    for attempt in range(max_retries + 1):
        try:
            return collect(influxdb3_local, hostname, task_id)
        except Exception as e:
            if attempt == max_retries:
                influxdb3_local.error(
                    f"[{task_id}] Failed to collect {metric_type} metrics "
                    f"after {max_retries} retries: {e}"
                )
                return None
            influxdb3_local.warn(
                f"[{task_id}] {metric_type} metrics collection attempt "
                f"{attempt + 1} failed, retrying: {e}"
            )


def process_scheduled_call(influxdb3_local, call_time, args=None):
    task_id = str(uuid.uuid4())

    config: dict | None = _load_config(influxdb3_local, args, task_id)
    if config is None:
        return

    hostname: str = config["hostname"]
    max_retries: int = config["max_retries"]

    influxdb3_local.info(
        f"[{task_id}] Starting system metrics collection for host: {hostname}"
    )

    skipped: list[str] = []
    try:
        for config_key, metric_type, collect in _COLLECTORS:
            if not config[config_key]:
                continue
            lines = _collect_with_retry(
                influxdb3_local, collect, metric_type, hostname, max_retries, task_id
            )
            if lines is None:
                skipped.append(metric_type)
                continue
            write_data(influxdb3_local, lines, batch=False, retries=0)
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Error collecting system metrics: {e}")
        raise

    if skipped:
        influxdb3_local.error(
            f"[{task_id}] Collected system metrics for host: {hostname}, "
            f"skipped after repeated failures: {', '.join(skipped)}"
        )
    else:
        influxdb3_local.info(
            f"[{task_id}] Successfully collected system metrics for host: {hostname}"
        )