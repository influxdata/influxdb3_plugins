# River Anomaly Detection Plugin

⚡ data-write 🏷️ anomaly-detection, river, online-ml 🔧 InfluxDB 3 Core, InfluxDB 3 Enterprise

## Description

The River Anomaly Detection Plugin detects anomalies in incoming time-series writes with online River ML models.
It combines exponentially weighted rolling statistics with River HalfSpaceTrees so each series can be scored incrementally without batch retraining.
The plugin writes detected anomalies to `_anomalies.{source_table}` with diagnostic fields for rolling and HalfSpaceTrees results.
It can optionally read tuning recommendations from River Auto Profiler output in `_meta.series_profiles`.

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
| `target_database` | string | same database | Database to write anomaly output to |
| `exclude_fields` | string | `""` | Dot-separated field names to skip |
| `exclude_tables` | string | `""` | Dot-separated table names to skip when using `all_tables` |
| `rolling_std_threshold` | float | `5.0` | Standard deviations from mean required for rolling anomaly |
| `hst_quantile` | float | `0.95` | HalfSpaceTrees score quantile threshold |
| `hst_n_trees` | integer | `10` | Number of HalfSpaceTrees in the ensemble |
| `hst_height` | integer | `6` | Height of each HalfSpaceTree |
| `hst_window_size` | integer | `250` | Warm-up window size for HalfSpaceTrees |
| `max_series` | integer | `100` | Maximum unique series to track with LRU eviction |
| `ew_fading_factor` | float | `0.3` | Exponentially weighted statistic fading factor |
| `log_anomalies` | boolean | `true` | Log detected anomalies |
| `auto_tune` | boolean | `false` | Read per-series tuning from `_meta.series_profiles` |

### TOML configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `config_file_path` | string | none | TOML config file path relative to `PLUGIN_DIR` |

*To use a TOML configuration file, set the `PLUGIN_DIR` environment variable and specify the `config_file_path` in the trigger arguments.* This is in addition to the `--plugin-dir` flag when starting InfluxDB 3.

#### Example TOML configuration

[anomaly_detector_config.toml](anomaly_detector_config.toml)

For more information on using TOML configuration files, see the Using TOML Configuration Files section in the [influxdb3_plugins/README.md](/README.md).

## Requirements

### Data requirements

The plugin processes numeric fields from incoming writes.
Each unique combination of source table, tag values, and field name gets a separate detector pair.
HalfSpaceTrees needs warm-up observations before it starts scoring; rolling statistics can begin after a few observations.

### Schema requirements

The plugin writes anomalies to `_anomalies.{source_table}` only when `is_anomaly` is true.

Tags:

- all source row tags
- `field_name`: source field name

Fields:

- `original_value`: source field value
- `is_anomaly`: true when either detector flags the value
- `observations`: number of observations processed for the series
- `rolling_anomaly`, `rolling_mean`, `rolling_std`, `rolling_deviation`, `rolling_threshold`
- `hst_warming_up`, `hst_anomaly`, `hst_score`

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
  --path "gh:influxdata/river_anomaly_detection/anomaly_detector.py" \
  --trigger-spec "all_tables" \
  anomaly_detector
```

### Data write trigger with auto-tuning

```bash
influxdb3 create trigger \
  --database mydb \
  --path "gh:influxdata/river_anomaly_detection/anomaly_detector.py" \
  --trigger-spec "all_tables" \
  --trigger-arguments auto_tune=true \
  anomaly_detector_auto_tune
```

### Data write trigger with TOML configuration

```bash
influxdb3 create trigger \
  --database mydb \
  --path "gh:influxdata/river_anomaly_detection/anomaly_detector.py" \
  --trigger-spec "all_tables" \
  --trigger-arguments config_file_path=anomaly_detector_config.toml \
  anomaly_detector_config
```

## Example usage

### Detect anomalies in CPU metrics

```bash
# Create the anomaly detector trigger.
influxdb3 create trigger \
  --database mydb \
  --path "gh:influxdata/river_anomaly_detection/anomaly_detector.py" \
  --trigger-spec "all_tables" \
  --trigger-arguments rolling_std_threshold=2.5,hst_quantile=0.99 \
  anomaly_detector_cpu

# Write sample data.
influxdb3 write \
  --database mydb \
  "cpu,host=server01 usage=72.4"

# Query detected anomalies.
influxdb3 query \
  --database mydb \
  "SELECT * FROM \"_anomalies.cpu\" ORDER BY time DESC LIMIT 5"
```

### Expected output

```text
host     | field_name | original_value | is_anomaly | rolling_anomaly | rolling_deviation | hst_anomaly | time
---------|------------|----------------|------------|-----------------|-------------------|-------------|---------------------
server01 | usage      | 99.8           | true       | true            | 3.4               | false       | 2026-05-15T12:00:00Z
```

## Code overview

### Files

- `anomaly_detector.py`: main plugin code for anomaly detection
- `anomaly_detector_config.toml`: example TOML configuration
- `requirements.txt`: Python package requirements

### Main functions

#### `process_writes(influxdb3_local, table_batches, args)`

Processes incoming write batches, updates per-series detector state, and writes anomaly records when either detector flags a value.

#### `parse_detector_args(args)`

Parses trigger arguments and applies defaults for output database, exclusions, detector thresholds, model sizes, and auto-tuning.

#### `get_tuned_params(influxdb3_local, table_name, field_name, task_id)`

Reads recommended detector parameters from `_meta.series_profiles` when `auto_tune` is enabled.

### Key logic

1. Builds a stable series key from table, tags, and field.
2. Stores `SeriesModel` objects in `influxdb3_local.cache` with LRU eviction.
3. Scores each numeric value with rolling statistics and HalfSpaceTrees.
4. Writes rows to `_anomalies.{source_table}` only when an anomaly is detected.

## Troubleshooting

### Common issues

- **No anomalies written**: the plugin writes only detected anomalies; lower thresholds for test data.
- **HalfSpaceTrees not scoring**: wait for at least `hst_window_size` observations.
- **Auto-tune not changing thresholds**: run River Auto Profiler first and wait for mature profiles.
- **Missing River package**: run `influxdb3 install package river`.

## Questions/Comments

For questions, bug reports, or improvements, open an issue or pull request in the [influxdb3_plugins repository](https://github.com/influxdata/influxdb3_plugins).
