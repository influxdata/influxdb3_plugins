# Basic Transformation Plugin

⚡ scheduled, data-write 🏷️ transformation, data-cleaning, unit-conversion 🔧 InfluxDB 3 Core, InfluxDB 3 Enterprise

## Description

The Basic Transformation Plugin enables real-time and scheduled transformation of time series data in InfluxDB 3. Transform field and tag names, convert values between units, and apply custom string replacements to standardize or clean your data. The plugin supports both scheduled batch processing of historical data and real-time transformation as data is written.

## Configuration

Plugin parameters may be specified as key-value pairs in the `--trigger-arguments` flag (CLI) or in the `trigger_arguments` field (API) when creating a trigger. Some plugins support TOML configuration files, which can be specified using the plugin's `config_file_path` parameter.

If a plugin supports multiple trigger specifications, some parameters may depend on the trigger specification that you use.

### Plugin metadata

This plugin includes a JSON metadata schema in its docstring that defines supported trigger types and configuration parameters. This metadata enables the [InfluxDB 3 Explorer](https://docs.influxdata.com/influxdb3/explorer/) UI to display and configure the plugin.

### Required parameters

| Parameter            | Type   | Default          | Description                                       |
|----------------------|--------|------------------|---------------------------------------------------|
| `measurement`        | string | required         | Source measurement containing data to transform   |
| `target_measurement` | string | required         | Destination measurement for transformed data      |
| `target_database`    | string | current database | Database for storing transformed data             |
| `dry_run`            | string | "false"          | When "true", logs transformations without writing |

### Transformation parameters

| Parameter                | Type   | Default | Description                                                                                         |
|--------------------------|--------|---------|-----------------------------------------------------------------------------------------------------|
| `names_transformations`  | string | none    | Field/tag name transformation rules. Format: `'field1:"transform1 transform2".field2:"transform3"'` |
| `values_transformations` | string | none    | Field value transformation rules. Format: `'field1:"transform1".field2:"transform2"'`               |
| `custom_replacements`    | string | none    | Custom string replacements. Format: `'rule_name:"find=replace"'`                                    |
| `custom_regex`           | string | none    | Regex patterns for field matching. Format: `'pattern_name:"temp%"'`                                 |

### Data selection parameters

| Parameter         | Type   | Default                   | Description                                                          |
|-------------------|--------|---------------------------|----------------------------------------------------------------------|
| `window`          | string | required (scheduled only) | Historical data window. Format: `<number><unit>` (e.g., "30d", "1h") |
| `included_fields` | string | all fields                | Dot-separated list of fields to include (e.g., "temp.humidity")      |
| `excluded_fields` | string | none                      | Dot-separated list of fields to exclude                              |
| `filters`         | string | none                      | Query filters. Format: `'field:"operator value"'`                    |

### TOML configuration

| Parameter          | Type   | Default | Description                                                                      |
|--------------------|--------|---------|----------------------------------------------------------------------------------|
| `config_file_path` | string | none    | TOML config file path relative to `PLUGIN_DIR` (required for TOML configuration) |

*To use a TOML configuration file, set the `PLUGIN_DIR` environment variable and specify the `config_file_path` in the trigger arguments.* This is in addition to the `--plugin-dir` flag when starting InfluxDB 3.

#### Example TOML configurations

- [basic_transformation_config_scheduler.toml](basic_transformation_config_scheduler.toml) - for scheduled triggers
- [basic_transformation_config_data_writes.toml](basic_transformation_config_data_writes.toml) - for data write triggers

For more information on using TOML configuration files, see the Using TOML Configuration Files section in the [influxdb3_plugins/README.md](../../README.md).

## Data requirements

The plugin assumes that the table schema is already defined in the database, as it relies on this schema to retrieve field and tag names required for processing.

## Software Requirements

- **InfluxDB 3 Core/Enterprise**: with the Processing Engine enabled
- **Python packages**:
 	- `pint` (for unit conversions)

## Installation steps

1. Start InfluxDB 3 with the Processing Engine enabled (`--plugin-dir /path/to/plugins`):

   ```bash
   influxdb3 serve \
     --node-id node0 \
     --object-store file \
     --data-dir ~/.influxdb3 \
     --plugin-dir ~/.plugins
   ```

2. Install required Python packages:

   ```bash
   influxdb3 install package pint
   ```

## Trigger setup

### Scheduled transformation

Run transformations periodically on historical data:

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename gh:influxdata/basic_transformation/basic_transformation.py \
  --trigger-spec "every:1h" \
  --trigger-arguments 'measurement=temperature,window=24h,target_measurement=temperature_normalized,names_transformations=temp:"snake",values_transformations=temp:"convert_degC_to_degF"' \
  hourly_temp_transform
```

### Real-time transformation

Transform data as it's written:

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename gh:influxdata/basic_transformation/basic_transformation.py \
  --trigger-spec "all_tables" \
  --trigger-arguments 'measurement=sensor_data,target_measurement=sensor_data_clean,names_transformations=.*:"snake alnum_underscore_only"' \
  realtime_clean
```

## Example usage

### Example 1: Temperature unit conversion

Convert temperature readings from Celsius to Fahrenheit while standardizing field names:

```bash
# Create the trigger
influxdb3 create trigger \
  --database weather \
  --plugin-filename gh:influxdata/basic_transformation/basic_transformation.py \
  --trigger-spec "every:30m" \
  --trigger-arguments 'measurement=raw_temps,window=1h,target_measurement=temps_fahrenheit,names_transformations=Temperature:"snake",values_transformations=temperature:"convert_degC_to_degF"' \
  temp_converter

# Write test data
influxdb3 write \
  --database weather \
  "raw_temps,location=office Temperature=22.5"

# Query transformed data (after trigger runs)
influxdb3 query \
  --database weather \
  "SELECT * FROM temps_fahrenheit"
```

### Expected output

 location | temperature | time
 ---------|-------------|-----
 office   | 72.5        | 2024-01-01T00:00:00Z

**Transformation details:**

- Before: `Temperature=22.5` (Celsius)
- After: `temperature=72.5` (Fahrenheit, field name converted to snake_case)

### Example 2: Field name standardization

Clean and standardize field names from various sensors:

```bash
# Create trigger with multiple transformations
influxdb3 create trigger \
  --database sensors \
  --plugin-filename gh:influxdata/basic_transformation/basic_transformation.py \
  --trigger-spec "all_tables" \
  --trigger-arguments 'measurement=raw_sensors,target_measurement=clean_sensors,names_transformations=.*:"snake alnum_underscore_only collapse_underscore trim_underscore"' \
  field_cleaner

# Write data with inconsistent field names
influxdb3 write \
  --database sensors \
  "raw_sensors,device=sensor1 \"Room Temperature\"=20.1,\"__Humidity_%\"=45.2"

# Query cleaned data
influxdb3 query \
  --database sensors \
  "SELECT * FROM clean_sensors"
```

### Expected output

 device  | room_temperature | humidity | time
 --------|------------------|----------|-----
 sensor1 | 20.1            | 45.2     | 2024-01-01T00:00:00Z

**Transformation details:**

- Before: `"Room Temperature"=20.1`, `"__Humidity_%"=45.2`
- After: `room_temperature=20.1`, `humidity=45.2` (field names standardized)

### Example 3: Custom string replacements

Replace specific strings in field values:

```bash
# Create trigger with custom replacements
influxdb3 create trigger \
  --database inventory \
  --plugin-filename gh:influxdata/basic_transformation/basic_transformation.py \
  --trigger-spec "every:1d" \
  --trigger-arguments 'measurement=products,window=7d,target_measurement=products_updated,values_transformations=status:"status_replace",custom_replacements=status_replace:"In Stock=available.Out of Stock=unavailable"' \
  status_updater
```

## Using TOML Configuration Files

This plugin supports using TOML configuration files to specify all plugin arguments. This is useful for complex configurations or when you want to version control your plugin settings.

### Important Requirements

**To use TOML configuration files, you must set the `PLUGIN_DIR` environment variable in the InfluxDB 3 host environment.** This is required in addition to the `--plugin-dir` flag when starting InfluxDB 3:

- `--plugin-dir` tells InfluxDB 3 where to find plugin Python files
- `PLUGIN_DIR` environment variable tells the plugins where to find TOML configuration files

### Setting Up TOML Configuration

1. **Start InfluxDB 3 with the PLUGIN_DIR environment variable set**:

   ```bash
   PLUGIN_DIR=~/.plugins influxdb3 serve \
     --node-id node0 \
     --object-store file \
     --data-dir ~/.influxdb3 \
     --plugin-dir ~/.plugins
   ```

2. **Copy the example TOML configuration file to your plugin directory**:

   ```bash
   cp basic_transformation_config_scheduler.toml ~/.plugins/
   # or for data writes:
   cp basic_transformation_config_data_writes.toml ~/.plugins/
   ```

3. **Edit the TOML file** to match your requirements. The TOML file contains all the arguments defined in the plugin's argument schema (see the JSON schema in the docstring at the top of basic_transformation.py).

4. **Create a trigger using the `config_file_path` argument**:

   ```bash
   influxdb3 create trigger \
     --database mydb \
     --plugin-filename basic_transformation.py \
     --trigger-spec "every:1d" \
     --trigger-arguments config_file_path=basic_transformation_config_scheduler.toml \
     basic_transform_trigger
   ```

## Code overview

### Files

- `basic_transformation.py`: The main plugin code containing handlers for scheduled tasks and data write transformations
- `basic_transformation_config_data_writes.toml`: Example TOML configuration file for data write triggers
- `basic_transformation_config_scheduler.toml`: Example TOML configuration file for scheduled triggers

### Logging

Logs are stored in the `_internal` database (or the database where the trigger is created) in the `system.processing_engine_logs` table. To view logs:

```bash
influxdb3 query --database _internal "SELECT * FROM system.processing_engine_logs WHERE trigger_name = 'your_trigger_name'"
```

Log columns:

- **event_time**: Timestamp of the log event
- **trigger_name**: Name of the trigger that generated the log
- **log_level**: Severity level (INFO, WARN, ERROR)
- **log_text**: Message describing the action or error

### Main functions

#### `process_scheduled_call(influxdb3_local, call_time, args)`

Handles scheduled transformation tasks. Queries historical data within the specified window and applies transformations.

Key operations:

1. Parses configuration from arguments
2. Queries source measurement with filters
3. Applies name and value transformations
4. Writes transformed data to target measurement

#### `process_writes(influxdb3_local, table_batches, args)`

Handles real-time transformation during data writes. Processes incoming data batches and applies transformations before writing.

Key operations:

1. Filters relevant table batches
2. Applies transformations to each row
3. Writes to target measurement immediately

#### `apply_transformations(value, transformations)`

Core transformation engine that applies a chain of transformations to a value.

Supported transformations:

- String operations: `lower`, `upper`, `snake`
- Space handling: `space_to_underscore`, `remove_space`
- Character filtering: `alnum_underscore_only`
- Underscore management: `collapse_underscore`, `trim_underscore`
- Unit conversions: `convert_<from>_to_<to>`
- Custom replacements: User-defined string substitutions

## Troubleshooting

### Common issues

#### Issue: Transformations not applying

**Solution**: Check that field names match exactly (case-sensitive). Use regex patterns for flexible matching:

```bash
--trigger-arguments 'custom_regex=temp_fields:"temp%",values_transformations=temp_fields:"convert_degC_to_degF"'
```

#### Issue: "Permission denied" errors in logs

**Solution**: Ensure the plugin file has execute permissions:

```bash
chmod +x ~/.plugins/basic_transformation.py
```

#### Issue: Unit conversion failing

**Solution**: Verify unit names are valid pint units. Common units:

- Temperature: `degC`, `degF`, `degK`
- Length: `meter`, `foot`, `inch`
- Time: `second`, `minute`, `hour`

#### Issue: No data in target measurement

**Solution**:

1. Check dry_run is not set to "true"
2. Verify source measurement contains data
3. Check logs for errors:

   ```bash
   influxdb3 query \
     --database _internal \
     "SELECT * FROM system.processing_engine_logs WHERE trigger_name = 'your_trigger_name'"
   ```

### Debugging tips

1. **Enable dry run** to test transformations:

   ```bash
   --trigger-arguments 'dry_run=true,...'
   ```

2. **Use specific time windows** for testing:

   ```bash
   --trigger-arguments 'window=1h,...'
   ```

3. **Check field names** in source data:

   ```bash
   influxdb3 query --database mydb "SHOW FIELD KEYS FROM measurement"
   ```

### Performance considerations

- Field name caching reduces query overhead (1-hour cache)
- Batch processing for scheduled tasks improves throughput
- Retry mechanism (3 attempts) handles transient write failures
- Use filters to process only relevant data

## Questions/Comments

For additional support, see the [Support section](../README.md#support).
