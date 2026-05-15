# River Auto Profiler Plugin

⚡ data-write 🏷️ profiling, river, anomaly-detection 🔧 InfluxDB 3 Core, InfluxDB 3 Enterprise

## Description

The River Auto Profiler Plugin incrementally profiles incoming time-series writes with River ML streaming statistics.
It tracks each unique table, tag set, and field as a separate series, then writes per-series profile records to `_meta.series_profiles`.
The profile output includes running statistics, write frequency, and recommended tuning parameters for River Anomaly Detection.
Use this plugin with a data write trigger when you want anomaly detection to auto-tune from observed data instead of static defaults.

## Configuration

Plugin parameters may be specified as key-value pairs in the `--trigger-arguments` flag (CLI) or in the `trigger_arguments` field (API) when creating a trigger.
This plugin also supports TOML configuration files, which can be specified using the `config_file_path` parameter.

### Plugin metadata

This plugin includes a JSON metadata schema in its docstring that defines supported trigger types and configuration parameters.
This metadata enables the [InfluxDB 3 Explorer](https://docs.influxdata.com/influxdb3/explorer/) UI to display and configure the plugin.

### Optional parameters

This plugin has no required parameters.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `exclude_fields` | string | `""` | Dot-separated field names to skip, such as `status.version` |
| `exclude_tables` | string | `""` | Dot-separated table names to skip when using `all_tables` |
| `max_series` | integer | `100` | Maximum unique series to track with LRU eviction |
| `profile_write_interval` | integer | `50` | Write one profile record every N observations per series |
| `min_observations` | integer | `50` | Observation count required before `profile_mature` is true |
| `log_profiles` | boolean | `false` | Log profile updates to the server log |

### TOML configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `config_file_path` | string | none | TOML config file path relative to `PLUGIN_DIR` |

*To use a TOML configuration file, set the `PLUGIN_DIR` environment variable and specify the `config_file_path` in the trigger arguments.* This is in addition to the `--plugin-dir` flag when starting InfluxDB 3.

#### Example TOML configuration

[auto_profiler_config.toml](auto_profiler_config.toml)

For more information on using TOML configuration files, see the Using TOML Configuration Files section in the [influxdb3_plugins/README.md](/README.md).

## Requirements

### Data requirements

The plugin processes numeric fields from incoming writes.
Each unique combination of source table, tag values, and field name becomes one profiled series.
Use `exclude_fields` and `exclude_tables` to prevent high-cardinality or non-signal data from being profiled.

### Schema requirements

The plugin writes profile output to `_meta.series_profiles`.

Tags:

- `source_table`: source table name
- `field_name`: source field name
- all source row tags

Fields:

- `observations`: total observation count
- `write_interval_seconds`: average seconds between writes
- `value_mean`, `value_std`, `value_min`, `value_max`: streaming statistics
- `coefficient_of_variation`: standard deviation divided by mean
- `recommended_threshold`: recommended rolling standard deviation threshold
- `recommended_fading_factor`: recommended exponentially weighted fading factor
- `profile_mature`: true after `min_observations`

### Software requirements

- **InfluxDB 3 Core/Enterprise**: with the Processing Engine enabled
- **Python packages**:
  - `river`

## Installation steps

1. Start InfluxDB 3 with the Processing Engine enabled (`--plugin-dir /path/to/plugins`):

   ```bash
   influxdb3 serve \
     --node-id node0 \
     --object-store file \
     --data-dir ~/.influxdb3 \
     --plugin-dir ~/.plugins
   ```

2. Install River into the Processing Engine Python environment:

   ```bash
   influxdb3 install package river
   ```

## Trigger setup

### Data write trigger

```bash
influxdb3 create trigger \
  --database mydb \
  --path "gh:influxdata/river_auto_profiler/auto_profiler.py" \
  --trigger-spec "all_tables" \
  auto_profiler
```

### Data write trigger with TOML configuration

```bash
influxdb3 create trigger \
  --database mydb \
  --path "gh:influxdata/river_auto_profiler/auto_profiler.py" \
  --trigger-spec "all_tables" \
  --trigger-arguments config_file_path=auto_profiler_config.toml \
  auto_profiler_config
```

## Example usage

### Profile incoming CPU metrics

```bash
# Create the profiler trigger.
influxdb3 create trigger \
  --database mydb \
  --path "gh:influxdata/river_auto_profiler/auto_profiler.py" \
  --trigger-spec "all_tables" \
  auto_profiler

# Write sample data.
influxdb3 write \
  --database mydb \
  "cpu,host=server01 usage=72.4"

# Query profile output after enough writes.
influxdb3 query \
  --database mydb \
  "SELECT * FROM \"_meta.series_profiles\" ORDER BY time DESC LIMIT 5"
```

### Expected output

```text
source_table | field_name | host     | observations | value_mean | value_std | recommended_threshold | profile_mature | time
-------------|------------|----------|--------------|------------|-----------|-----------------------|----------------|---------------------
cpu          | usage      | server01 | 50           | 71.8       | 3.2       | 5.0                   | true           | 2026-05-15T12:00:00Z
```

## Code overview

### Files

- `auto_profiler.py`: main plugin code for data write profiling
- `auto_profiler_config.toml`: example TOML configuration
- `requirements.txt`: Python package requirements

### Main functions

#### `process_writes(influxdb3_local, table_batches, args)`

Processes incoming write batches, filters configured tables and fields, updates River streaming statistics, and writes profile output at the configured interval.

#### `parse_profiler_args(args)`

Parses trigger arguments and applies defaults for exclusions, series limits, write interval, maturity threshold, and logging.

### Key logic

1. Builds a stable series key from table, tags, and field.
2. Stores `SeriesProfile` objects in `influxdb3_local.cache` with LRU eviction.
3. Updates River streaming statistics for each numeric field.
4. Writes `_meta.series_profiles` records when each series reaches its write interval.

## Troubleshooting

### Common issues

- **No profiles written**: verify numeric fields are being written and `profile_write_interval` has been reached.
- **Too many tracked series**: lower cardinality with `exclude_tables`, `exclude_fields`, or a lower `max_series`.
- **TOML config not loaded**: verify `PLUGIN_DIR` is set and `config_file_path` is relative to that directory.
- **Missing River package**: run `influxdb3 install package river`.

## Questions/Comments

For questions, bug reports, or improvements, open an issue or pull request in the [influxdb3_plugins repository](https://github.com/influxdata/influxdb3_plugins).
