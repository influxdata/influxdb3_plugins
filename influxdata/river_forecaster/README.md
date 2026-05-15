# River Forecaster Plugin

⚡ scheduled, data-write 🏷️ forecasting, river, online-ml 🔧 InfluxDB 3 Core, InfluxDB 3 Enterprise

## Description

The River Forecaster Plugin provides online time-series forecasting with River ML SNARIMAX models.
It uses a data write trigger to learn incrementally from selected tables and fields, then uses a scheduled trigger to write multi-step forecasts.
Each unique table, tag set, and field gets a separate model stored in the Processing Engine cache.
The plugin can use River Auto Profiler output to choose forecast horizons from observed write frequency.

## Configuration

Plugin parameters may be specified as key-value pairs in the `--trigger-arguments` flag (CLI) or in the `trigger_arguments` field (API) when creating a trigger.
This plugin supports both data write and scheduled triggers, and some parameters apply only to one trigger type.
This plugin also supports TOML configuration files, which can be specified using the `config_file_path` parameter.

### Plugin metadata

This plugin includes a JSON metadata schema in its docstring that defines supported trigger types and configuration parameters.
This metadata enables the [InfluxDB 3 Explorer](https://docs.influxdata.com/influxdb3/explorer/) UI to display and configure the plugin.

### Required parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `include_tables` | string | required for data write trigger | Dot-separated table names to forecast, such as `system_cpu.system_memory` |

### Optional parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `include_fields` | string | all numeric fields | Dot-separated field names to forecast |
| `max_series` | integer | `50` | Maximum unique series to track with LRU eviction |
| `min_observations` | integer | `30` | Minimum observations before scheduled forecasts are written |
| `default_horizon` | integer | `12` | Fallback forecast horizon if auto-profiler data is unavailable |
| `forecast_target_seconds` | integer | `3600` | Target forecast window in seconds for auto-horizon calculation |
| `snarimax_p` | integer | `12` | SNARIMAX autoregressive order |
| `snarimax_d` | integer | `1` | SNARIMAX differencing order |
| `snarimax_q` | integer | `1` | SNARIMAX moving average order |
| `log_forecasts` | boolean | `false` | Log forecast details during scheduled runs |

### TOML configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `config_file_path` | string | none | TOML config file path relative to `PLUGIN_DIR` |

*To use a TOML configuration file, set the `PLUGIN_DIR` environment variable and specify the `config_file_path` in the trigger arguments.* This is in addition to the `--plugin-dir` flag when starting InfluxDB 3.

#### Example TOML configuration

[forecaster_config.toml](forecaster_config.toml)

For more information on using TOML configuration files, see the Using TOML Configuration Files section in the [influxdb3_plugins/README.md](/README.md).

## Requirements

### Data requirements

The data write trigger learns from numeric fields in tables listed by `include_tables`.
The scheduled trigger writes forecasts only for models that have at least `min_observations`.
Run both triggers with the same plugin file and database so they share Processing Engine cache state.

### Schema requirements

The plugin writes forecasts to `_forecasts.{source_table}`.

Tags:

- all source row tags
- `field_name`: source field name
- `horizon_step`: forecast step number

Fields:

- `forecast_value`: predicted value
- `model`: model name, currently `snarimax`
- `horizon_total`: total number of forecast steps
- `observations`: number of observations learned by the model

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
  --path "gh:influxdata/river_forecaster/forecaster.py" \
  --trigger-spec "all_tables" \
  --trigger-arguments include_tables=system_cpu.system_memory \
  forecaster_learner
```

### Scheduled trigger

```bash
influxdb3 create trigger \
  --database mydb \
  --path "gh:influxdata/river_forecaster/forecaster.py" \
  --trigger-spec "every:15m" \
  forecaster_scheduler
```

### Scheduled trigger with TOML configuration

```bash
influxdb3 create trigger \
  --database mydb \
  --path "gh:influxdata/river_forecaster/forecaster.py" \
  --trigger-spec "every:15m" \
  --trigger-arguments config_file_path=forecaster_config.toml \
  forecaster_scheduler_config
```

## Example usage

### Forecast CPU usage

```bash
# Create the learner trigger.
influxdb3 create trigger \
  --database mydb \
  --path "gh:influxdata/river_forecaster/forecaster.py" \
  --trigger-spec "all_tables" \
  --trigger-arguments include_tables=cpu,include_fields=usage \
  forecaster_learner

# Create the scheduled forecast trigger.
influxdb3 create trigger \
  --database mydb \
  --path "gh:influxdata/river_forecaster/forecaster.py" \
  --trigger-spec "every:15m" \
  --trigger-arguments min_observations=30,default_horizon=12 \
  forecaster_scheduler

# Write sample data.
influxdb3 write \
  --database mydb \
  "cpu,host=server01 usage=72.4"

# Query forecast output after the scheduled trigger runs.
influxdb3 query \
  --database mydb \
  "SELECT * FROM \"_forecasts.cpu\" WHERE field_name = 'usage' ORDER BY time DESC LIMIT 12"
```

### Expected output

```text
host     | field_name | horizon_step | forecast_value | model    | horizon_total | observations | time
---------|------------|--------------|----------------|----------|---------------|--------------|---------------------
server01 | usage      | 1            | 73.1           | snarimax | 12            | 50           | 2026-05-15T12:15:00Z
server01 | usage      | 2            | 73.4           | snarimax | 12            | 50           | 2026-05-15T12:30:00Z
```

## Code overview

### Files

- `forecaster.py`: main plugin code for learner and scheduled forecast triggers
- `forecaster_config.toml`: example TOML configuration
- `requirements.txt`: Python package requirements

### Main functions

#### `process_writes(influxdb3_local, table_batches, args)`

Processes incoming write batches, filters configured tables and fields, and updates per-series SNARIMAX models.

#### `process_scheduled_call(influxdb3_local, call_time, args)`

Reads trained models from cache, calculates forecast horizons, and writes future forecast points.

#### `forecaster_get_horizon(influxdb3_local, table_name, field_name, tags, config, task_id)`

Uses River Auto Profiler output when available, then falls back to model write interval estimates or `default_horizon`.

### Key logic

1. Learns per-series SNARIMAX models from data write triggers.
2. Stores model state in `influxdb3_local.cache` with LRU eviction.
3. Computes forecast horizon from profile data, observed intervals, or configured defaults.
4. Writes scheduled forecasts to `_forecasts.{source_table}` with future timestamps.

## Troubleshooting

### Common issues

- **No forecasts written**: verify both triggers are enabled and the learner has reached `min_observations`.
- **Learner ignores writes**: verify the table name appears in `include_tables`.
- **Unexpected horizon**: run River Auto Profiler or set `default_horizon` explicitly.
- **Missing River package**: run `influxdb3 install package river`.

## Questions/Comments

For questions, bug reports, or improvements, open an issue or pull request in the [influxdb3_plugins repository](https://github.com/influxdata/influxdb3_plugins).
