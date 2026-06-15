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
            "description": "Path to configuration file from PLUGIN_DIR env var",
            "required": false
        }
    ]
}
"""

import psutil
import uuid
import os
import tomllib
from pathlib import Path

def collect_cpu_metrics(influxdb3_local, hostname, task_id):
    # Get CPU frequencies
    cpu_freq = psutil.cpu_freq(percpu=False)
    cpu_stats = psutil.cpu_stats()
    cpu_times = psutil.cpu_times_percent()
    load_avg = psutil.getloadavg()
    
    # Overall CPU usage and stats
    line = LineBuilder("system_cpu")\
        .tag("host", hostname)\
        .tag("cpu", "total")\
        .float64_field("user", cpu_times.user)\
        .float64_field("system", cpu_times.system)\
        .float64_field("idle", cpu_times.idle)\
        .float64_field("iowait", getattr(cpu_times, 'iowait', 0))\
        .float64_field("nice", getattr(cpu_times, 'nice', 0))\
        .float64_field("irq", getattr(cpu_times, 'irq', 0))\
        .float64_field("softirq", getattr(cpu_times, 'softirq', 0))\
        .float64_field("steal", getattr(cpu_times, 'steal', 0))\
        .float64_field("guest", getattr(cpu_times, 'guest', 0))\
        .float64_field("guest_nice", getattr(cpu_times, 'guest_nice', 0))\
        .float64_field("frequency_current", getattr(cpu_freq, 'current', 0))\
        .float64_field("frequency_min", getattr(cpu_freq, 'min', 0))\
        .float64_field("frequency_max", getattr(cpu_freq, 'max', 0))\
        .uint64_field("ctx_switches", cpu_stats.ctx_switches)\
        .uint64_field("interrupts", cpu_stats.interrupts)\
        .uint64_field("soft_interrupts", cpu_stats.soft_interrupts)\
        .uint64_field("syscalls", getattr(cpu_stats, 'syscalls', 0))\
        .float64_field("load1", load_avg[0])\
        .float64_field("load5", load_avg[1])\
        .float64_field("load15", load_avg[2])
    influxdb3_local.write(line)
    
    # Per CPU core metrics
    try:
        per_cpu_percent = psutil.cpu_percent(interval=None, percpu=True)
        per_cpu_times = psutil.cpu_times_percent(percpu=True)
        per_cpu_freq = psutil.cpu_freq(percpu=True)
        
        for core_id in range(len(per_cpu_percent)):
            line = LineBuilder("system_cpu_cores")\
                .tag("host", hostname)\
                .tag("core", str(core_id))
            
            # Add usage percentage
            line.float64_field("usage", per_cpu_percent[core_id])
            
            # Add CPU time breakdowns if available
            if core_id < len(per_cpu_times):
                core_times = per_cpu_times[core_id]
                line.float64_field("user", core_times.user)\
                    .float64_field("system", core_times.system)\
                    .float64_field("idle", core_times.idle)\
                    .float64_field("iowait", getattr(core_times, 'iowait', 0))\
                    .float64_field("nice", getattr(core_times, 'nice', 0))\
                    .float64_field("irq", getattr(core_times, 'irq', 0))\
                    .float64_field("softirq", getattr(core_times, 'softirq', 0))\
                    .float64_field("steal", getattr(core_times, 'steal', 0))\
                    .float64_field("guest", getattr(core_times, 'guest', 0))\
                    .float64_field("guest_nice", getattr(core_times, 'guest_nice', 0))
            
            # Add frequency metrics if available
            if per_cpu_freq and core_id < len(per_cpu_freq):
                freq = per_cpu_freq[core_id]
                line.float64_field("frequency_current", freq.current)\
                    .float64_field("frequency_min", getattr(freq, 'min', 0))\
                    .float64_field("frequency_max", getattr(freq, 'max', 0))
            
            influxdb3_local.write(line)
    except Exception as e:
        influxdb3_local.warn(f"[{task_id}] Error collecting per-core CPU metrics: {str(e)}")

    
def collect_memory_metrics(influxdb3_local, hostname, task_id):
    # Virtual memory metrics
    mem = psutil.virtual_memory()
    swap = psutil.swap_memory()
    
    # Main memory metrics
    line = LineBuilder("system_memory")\
        .tag("host", hostname)\
        .uint64_field("total", mem.total)\
        .uint64_field("available", mem.available)\
        .uint64_field("used", mem.used)\
        .uint64_field("free", mem.free)\
        .uint64_field("active", getattr(mem, 'active', 0))\
        .uint64_field("inactive", getattr(mem, 'inactive', 0))\
        .uint64_field("buffers", getattr(mem, 'buffers', 0))\
        .uint64_field("cached", getattr(mem, 'cached', 0))\
        .uint64_field("shared", getattr(mem, 'shared', 0))\
        .uint64_field("slab", getattr(mem, 'slab', 0))\
        .float64_field("percent", mem.percent)
    influxdb3_local.write(line)
    
    # Swap metrics in separate measurement
    line = LineBuilder("system_swap")\
        .tag("host", hostname)\
        .uint64_field("total", swap.total)\
        .uint64_field("used", swap.used)\
        .uint64_field("free", swap.free)\
        .float64_field("percent", swap.percent)\
        .uint64_field("sin", swap.sin)\
        .uint64_field("sout", swap.sout)
    influxdb3_local.write(line)
    
    # Try to collect memory page faults if available
    try:
        page_faults = psutil.Process().memory_full_info()
        line = LineBuilder("system_memory_faults")\
            .tag("host", hostname)\
            .uint64_field("page_faults", getattr(page_faults, 'num_page_faults', 0))\
            .uint64_field("major_faults", getattr(page_faults, 'maj_faults', 0))\
            .uint64_field("minor_faults", getattr(page_faults, 'min_faults', 0))\
            .uint64_field("rss", getattr(page_faults, 'rss', 0))\
            .uint64_field("vms", getattr(page_faults, 'vms', 0))\
            .uint64_field("dirty", getattr(page_faults, 'dirty', 0))\
            .uint64_field("uss", getattr(page_faults, 'uss', 0))\
            .uint64_field("pss", getattr(page_faults, 'pss', 0))
        influxdb3_local.write(line)
    except (psutil.AccessDenied, psutil.Error):
        pass

def collect_disk_metrics(influxdb3_local, hostname, task_id):
    # Collect disk partition usage metrics
    for partition in psutil.disk_partitions(all=False):
        try:
            usage = psutil.disk_usage(partition.mountpoint)
            line = LineBuilder("system_disk_usage")\
                .tag("host", hostname)\
                .tag("device", partition.device)\
                .tag("mountpoint", partition.mountpoint)\
                .tag("fstype", partition.fstype)\
                .uint64_field("total", usage.total)\
                .uint64_field("used", usage.used)\
                .uint64_field("free", usage.free)\
                .float64_field("percent", usage.percent)
            influxdb3_local.write(line)
        except PermissionError:
            continue

    # Collect disk I/O statistics
    try:
        disk_io = psutil.disk_io_counters(perdisk=True)
        for disk_name, stats in disk_io.items():
            line = LineBuilder("system_disk_io")\
                .tag("host", hostname)\
                .tag("device", disk_name)\
                .uint64_field("reads", stats.read_count)\
                .uint64_field("writes", stats.write_count)\
                .uint64_field("read_bytes", stats.read_bytes)\
                .uint64_field("write_bytes", stats.write_bytes)\
                .uint64_field("read_time", stats.read_time)\
                .uint64_field("write_time", stats.write_time)\
                .uint64_field("busy_time", getattr(stats, 'busy_time', 0))\
                .uint64_field("read_merged_count", getattr(stats, 'read_merged_count', 0))\
                .uint64_field("write_merged_count", getattr(stats, 'write_merged_count', 0))
            influxdb3_local.write(line)

            # Calculate and write IOPS and throughput metrics
            # Note: These are instantaneous rates since the last measurement
            line = LineBuilder("system_disk_performance")\
                .tag("host", hostname)\
                .tag("device", disk_name)\
                .float64_field("read_bytes_per_sec", getattr(stats, 'read_bytes_per_sec', 0))\
                .float64_field("write_bytes_per_sec", getattr(stats, 'write_bytes_per_sec', 0))\
                .float64_field("read_iops", getattr(stats, 'read_count_per_sec', 0))\
                .float64_field("write_iops", getattr(stats, 'write_count_per_sec', 0))\
                .float64_field("avg_read_latency_ms", stats.read_time / stats.read_count if stats.read_count > 0 else 0)\
                .float64_field("avg_write_latency_ms", stats.write_time / stats.write_count if stats.write_count > 0 else 0)\
                .float64_field("util_percent", getattr(stats, 'busy_time_percent', 0))
            influxdb3_local.write(line)
    except (psutil.Error, AttributeError) as e:
        influxdb3_local.warn(f"[{task_id}] Error collecting disk I/O metrics: {str(e)}")


def collect_network_metrics(influxdb3_local, hostname, task_id):
    net_io = psutil.net_io_counters(pernic=True)
    
    for interface, stats in net_io.items():
        line = LineBuilder("system_network")\
            .tag("host", hostname)\
            .tag("interface", interface)\
            .uint64_field("bytes_sent", stats.bytes_sent)\
            .uint64_field("bytes_recv", stats.bytes_recv)\
            .uint64_field("packets_sent", stats.packets_sent)\
            .uint64_field("packets_recv", stats.packets_recv)\
            .uint64_field("errin", stats.errin)\
            .uint64_field("errout", stats.errout)\
            .uint64_field("dropin", stats.dropin)\
            .uint64_field("dropout", stats.dropout)
        influxdb3_local.write(line)

def process_scheduled_call(influxdb3_local, time, args=None):
    task_id = str(uuid.uuid4())
    
    try:
        # Load configuration from TOML file if provided
        if args and 'config_file_path' in args:
            config_file = args['config_file_path']
            if not config_file.endswith('.toml'):
                influxdb3_local.error(
                    f"[{task_id}] Invalid config file format: expected a .toml file"
                )
            else:
                plugin_dir = os.environ.get('PLUGIN_DIR')
                if plugin_dir:
                    config_path = os.path.join(plugin_dir, config_file)
                else:
                    # Fallbacks for servers where the operator has not exported PLUGIN_DIR:
                    #  - INFLUXDB3_PLUGIN_DIR: set when the server is configured via env var
                    #  - VIRTUAL_ENV: exported by the processing engine; default venv is <plugin-dir>/.venv
                    candidates = []
                    if influxdb3_plugin_dir := os.environ.get('INFLUXDB3_PLUGIN_DIR'):
                        candidates.append(influxdb3_plugin_dir)
                    if virtual_env := os.environ.get('VIRTUAL_ENV'):
                        candidates.append(str(Path(virtual_env).parent))

                    resolved = None
                    for base in candidates:
                        candidate = os.path.join(base, config_file)
                        if os.path.exists(candidate):
                            resolved = candidate
                            break

                    if resolved:
                        config_path = resolved
                    else:
                        # Match original behavior: continue with inline args
                        # when the config file cannot be resolved.
                        config_path = None
                        candidate_str = ', '.join(candidates) if candidates else 'none available'
                        influxdb3_local.error(
                            f"[{task_id}] PLUGIN_DIR env var not set and config file path '{config_file}' was not found via fallbacks (tried: {candidate_str})"
                        )

                if config_path:
                    try:
                        with open(config_path, 'rb') as f:
                            config = tomllib.load(f)
                        # Override args with config values
                        args.update(config)
                        influxdb3_local.info(f"[{task_id}] Loaded configuration from {config_path}")
                    except Exception:
                        influxdb3_local.error(f"[{task_id}] Failed to read config file")
        
        # Set default values if args is None
        if args is None:
            args = {}
        
        # Get configuration values with defaults
        hostname = args.get("hostname", "localhost")
        include_cpu = str(args.get("include_cpu", "true")).lower() == "true"
        include_memory = str(args.get("include_memory", "true")).lower() == "true"
        include_disk = str(args.get("include_disk", "true")).lower() == "true"
        include_network = str(args.get("include_network", "true")).lower() == "true"
        max_retries = int(args.get("max_retries", 3))
        
        influxdb3_local.info(f"[{task_id}] Starting system metrics collection for host: {hostname}")
        
        # Collect metrics with retry logic
        def collect_with_retry(collect_func, metric_type):
            for attempt in range(max_retries + 1):
                try:
                    collect_func(influxdb3_local, hostname, task_id)
                    break
                except Exception as e:
                    if attempt == max_retries:
                        influxdb3_local.error(f"[{task_id}] Failed to collect {metric_type} metrics after {max_retries} retries: {e}")
                        raise
                    influxdb3_local.warn(f"[{task_id}] {metric_type} metrics collection attempt {attempt + 1} failed, retrying: {e}")
        
        # Collect enabled metrics
        if include_cpu:
            collect_with_retry(collect_cpu_metrics, "CPU")
        
        if include_memory:
            collect_with_retry(collect_memory_metrics, "memory")
        
        if include_disk:
            collect_with_retry(collect_disk_metrics, "disk")
        
        if include_network:
            collect_with_retry(collect_network_metrics, "network")
        
        influxdb3_local.info(f"[{task_id}] Successfully collected system metrics for host: {hostname}")
        
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Error collecting system metrics: {str(e)}")
        raise