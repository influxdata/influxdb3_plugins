# System Metrics Plugin

⚡ scheduled  
🏷️ monitoring, system-metrics, performance
🔧 InfluxDB 3 Core, InfluxDB 3 Enterprise

A comprehensive system monitoring plugin that collects CPU, memory, disk, and network metrics from the host system. This plugin provides detailed performance insights including per-core CPU statistics, memory usage breakdowns, disk I/O performance, and network interface statistics.

## Prerequisites

- InfluxDB 3.0+
- Python psutil library (for system metrics collection)

## Files

- `system_metrics.py`: Main plugin file containing metric collection logic
- `system_metrics_config_scheduler.toml`: Configuration template for scheduled triggers
- `README.md`: This documentation file

## Features

- **CPU Metrics**: Overall and per-core CPU usage, frequency, load averages, context switches, and interrupts
- **Memory Metrics**: RAM usage, swap statistics, and memory page fault information  
- **Disk Metrics**: Partition usage, I/O statistics, throughput, IOPS, and latency calculations
- **Network Metrics**: Interface statistics including bytes/packets sent/received and error counts
- **Configurable Collection**: Enable/disable specific metric types via configuration
- **Robust Error Handling**: Retry logic and graceful handling of permission errors
- **Task Tracking**: UUID-based task identification for debugging and log correlation

## Logging

Logs are stored in the `_internal` database (or the database where the trigger is created) in the `system.processing_engine_logs` table. To view logs, use the following query:

```bash
influxdb3 query --database _internal "SELECT * FROM system.processing_engine_logs"
```

### Log Columns Description
- **event_time**: Timestamp of the log event.
- **trigger_name**: Name of the trigger that generated the log.
- **log_level**: Severity level (`INFO`, `WARN`, `ERROR`).
- **log_text**: Message describing the action or error.

## Setup & Run

1. **Copy plugin files to your plugin directory**:
   ```bash
   cp system_metrics.py $PLUGIN_DIR/
   cp system_metrics_config_scheduler.toml $PLUGIN_DIR/
   ```

2. **Install required Python dependencies**:
   ```bash
   pip install psutil
   ```

3. **Configure the plugin** by editing `system_metrics_config_scheduler.toml`:
   - Set hostname for metric tagging
   - Enable/disable specific metric types
   - Configure retry behavior

## Configure & Create Triggers

### Basic Scheduled Trigger
```bash
influxdb3 create trigger \
  --database system_monitoring \
  --plugin-filename system_metrics.py \
  --trigger-spec "every:30s" \
  system_metrics_trigger
```

### Using Configuration File
```bash
influxdb3 create trigger \
  --database system_monitoring \
  --plugin-filename system_metrics.py \
  --trigger-spec "every:1m" \
  --trigger-arguments config_file_path=system_metrics_config_scheduler.toml \
  system_metrics_config_trigger
```

### Custom Configuration
```bash
influxdb3 create trigger \
  --database system_monitoring \
  --plugin-filename system_metrics.py \
  --trigger-spec "every:30s" \
  --trigger-arguments hostname=web-server-01,include_disk=false,max_retries=5 \
  system_metrics_custom_trigger
```

## Arguments

| Argument | Example | Description | Required |
|----------|---------|-------------|----------|
| `hostname` | `localhost` | Hostname to tag metrics with | No |
| `include_cpu` | `true` | Include CPU metrics collection | No |
| `include_memory` | `true` | Include memory metrics collection | No |
| `include_disk` | `true` | Include disk metrics collection | No |
| `include_network` | `true` | Include network metrics collection | No |
| `max_retries` | `3` | Maximum number of retry attempts on failure | No |
| `config_file_path` | `system_metrics_config_scheduler.toml` | Path to configuration file from PLUGIN_DIR env var | No |

## Measurements and Fields

### system_cpu
Overall CPU statistics and metrics:
- **Tags**: `host`, `cpu=total`
- **Fields**: `user`, `system`, `idle`, `iowait`, `nice`, `irq`, `softirq`, `steal`, `guest`, `guest_nice`, `frequency_current`, `frequency_min`, `frequency_max`, `ctx_switches`, `interrupts`, `soft_interrupts`, `syscalls`, `load1`, `load5`, `load15`

### system_cpu_cores  
Per-core CPU statistics:
- **Tags**: `host`, `core` (core number)
- **Fields**: `usage`, `user`, `system`, `idle`, `iowait`, `nice`, `irq`, `softirq`, `steal`, `guest`, `guest_nice`, `frequency_current`, `frequency_min`, `frequency_max`

### system_memory
System memory statistics:
- **Tags**: `host`
- **Fields**: `total`, `available`, `used`, `free`, `active`, `inactive`, `buffers`, `cached`, `shared`, `slab`, `percent`

### system_swap
Swap memory statistics:
- **Tags**: `host`  
- **Fields**: `total`, `used`, `free`, `percent`, `sin`, `sout`

### system_memory_faults
Memory page fault information (when available):
- **Tags**: `host`
- **Fields**: `page_faults`, `major_faults`, `minor_faults`, `rss`, `vms`, `dirty`, `uss`, `pss`

### system_disk_usage
Disk partition usage:
- **Tags**: `host`, `device`, `mountpoint`, `fstype`
- **Fields**: `total`, `used`, `free`, `percent`

### system_disk_io
Disk I/O statistics:
- **Tags**: `host`, `device` 
- **Fields**: `reads`, `writes`, `read_bytes`, `write_bytes`, `read_time`, `write_time`, `busy_time`, `read_merged_count`, `write_merged_count`

### system_disk_performance
Calculated disk performance metrics:
- **Tags**: `host`, `device`
- **Fields**: `read_bytes_per_sec`, `write_bytes_per_sec`, `read_iops`, `write_iops`, `avg_read_latency_ms`, `avg_write_latency_ms`, `util_percent`

### system_network
Network interface statistics:
- **Tags**: `host`, `interface`
- **Fields**: `bytes_sent`, `bytes_recv`, `packets_sent`, `packets_recv`, `errin`, `errout`, `dropin`, `dropout`

## Usage Examples

### Monitor Web Server Performance
```bash
# Create trigger for web server monitoring every 15 seconds
influxdb3 create trigger \
  --database web_monitoring \
  --plugin-filename system_metrics.py \
  --trigger-spec "every:15s" \
  --trigger-arguments hostname=web-server-01,include_network=true \
  web_server_metrics
```

### Database Server Monitoring
```bash
# Focus on CPU and disk metrics for database server
influxdb3 create trigger \
  --database db_monitoring \
  --plugin-filename system_metrics.py \
  --trigger-spec "every:30s" \
  --trigger-arguments hostname=db-primary,include_disk=true,include_cpu=true,include_network=false \
  database_metrics
```

### High-Frequency System Monitoring
```bash
# Collect all metrics every 10 seconds with higher retry tolerance
influxdb3 create trigger \
  --database system_monitoring \
  --plugin-filename system_metrics.py \
  --trigger-spec "every:10s" \
  --trigger-arguments hostname=critical-server,max_retries=10 \
  high_freq_metrics
```

## Questions/Comments

If you have questions or run into any issues with this plugin, please reach out to the InfluxData support team or open an issue in the plugin repository.
