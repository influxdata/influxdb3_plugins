# Basic Transformation Plugin for InfluxDB 3

This plugin enables the transformation of time series data stored in InfluxDB 3 through both scheduled tasks and on-write transformations. It supports transformations for field/tag names and their values, allowing users to clean, standardize, or convert data as needed. The plugin can run scheduled tasks that periodically transform historical data or apply transformations in real-time as data is written to the database.

## Prerequisites
- **InfluxDB v3 Core/Enterprise**: with the Processing Engine enabled.

## Files
- `basic_transformation.py`: The main plugin code containing handlers for scheduled tasks.
- `basic_transformation_config_data_writes.toml`: Example TOML configuration file for data write triggers.
- `basic_transformation_config_scheduler.toml`: Example TOML configuration file for scheduled triggers.

## Features
- **Scheduler and Data Write Support**: Run periodically or on data writes via InfluxDB triggers.
- **Data Transformation**: Applies transformations to field names and values based on predefined and custom rules.
- **Data Writing**: Writes transformed data to a specified InfluxDB measurement.
- **Argument Overriding**: Allows overriding arguments for scheduler and data write types via a TOML file (requires setting the `PLUGIN_DIR` environment variable and the `config_file_path` parameter, see toml files example on [Git](https://github.com/influxdata/influxdb3_plugins/tree/main/influxdata/basic_transformation). Override args parameter in handler function). The `config_file_path` must be specified as a path relative to the directory defined by PLUGIN_DIR.
- **Time Interval Parsing**: Supports a wide range of time units for intervals, including seconds (`s`), minutes (`min`), hours (`h`), days (`d`), weeks (`w`), months (`m`), quarters (`q`), and years (`y`).

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

### 1. Install & Run InfluxDB v3 Core/Enterprise
- Download and install InfluxDB v3 Core/Enterprise.
- Ensure the `plugins` directory exists; if not, create it:
  ```bash
  mkdir ~/.plugins
  ```
- Place `basic_transformation.py` in `~/.plugins/`.
- Start InfluxDB 3 with the correct paths:
  ```bash
  influxdb3 serve --node-id node0 --object-store file --data-dir ~/.influxdb3 --plugin-dir ~/.plugins
  ```

### 2. Install Required Python Packages
```bash
influxdb3 install package pint
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
   PLUGIN_DIR=~/.plugins influxdb3 serve --node-id node0 --object-store file --data-dir ~/.influxdb3 --plugin-dir ~/.plugins
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

### Important Notes
- The `PLUGIN_DIR` environment variable must be set when starting InfluxDB 3 for TOML configuration to work
- When using `config_file_path`, specify only the filename (not the full path)
- The TOML file must be located in the directory specified by `PLUGIN_DIR`
- All parameters in the TOML file will override any command-line arguments
- Example TOML configuration files are provided:
  - `basic_transformation_config_scheduler.toml` - for scheduled triggers
  - `basic_transformation_config_data_writes.toml` - for data write triggers

## Configure & Create Triggers

### Scheduled Tasks
The Scheduled Tasks feature periodically performs data transformation on the specified InfluxDB measurement, using the provided configuration to transform field names and values, and writes the transformed data to a target measurement.

#### Arguments for Scheduler
The following arguments are extracted from the `args` dictionary:

| Argument                   | Description                                                                                                                                                          | Required | Example                                                       |
|----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------------------------------------------------------|
| `measurement`              | The InfluxDB measurement to query for historical data.                                                                                                               | Yes      | `"temperature"`                                               |
| `window`                   | Historical window duration for data retrieval (e.g., `30d`). Format: `<number><unit>` where unit is `s`, `min`, `h`, `d`, `w`, `m`, `q`, `y`.                        | Yes      | `"30d"`                                                       |
| `target_measurement`       | Destination measurement for storing transformed data.                                                                                                                | Yes      | `"transformed_temperature"`                                   |
| `names_transformations`    | Rules for transforming field and tag names. Format: `'field1:"transform1 transform2".pattern_name:"transform3 transform4'`.                                          | No       | `'room:"lower snake".temp:"upper".name:"custom_replacement"'` |
| `values_transformations`   | Rules for transforming field values. Format: `'field1:"transform1 transform2".pattern:"transform3"'`.                                                                | No       | `'temp:"convert_degC_to_degF".hum:"upper".something:"lower"'` |
| `target_database`          | Optional InfluxDB database name for writing transformed data.                                                                                                        | No       | `"transformed_db"`                                            |
| `included_fields`          | Dot-separated list of field names to include in the query.                                                                                                           | No       | `"temp.hum.something"`                                        |
| `excluded_fields`          | Dot-separated list of field names to exclude from the query.                                                                                                         | No       | `"co.h-u_m2"`                                                 |
| `dry_run`                  | If `"true"`, simulates the transformation without writing to the database. Defaults to `"false"`.                                                                    | No       | `"true"`                                                      |
| `custom_replacements`      | Custom replacement rules for transformations. Format: `'replace_space_underscore:" =_".cust_replace:"Some text=Another text"'`.                                      | No       | `'replace_space_underscore:" =_"'`                            |
| `custom_regex`             | Custom regex patterns for applying transformations. Format: `'regex_temp:"temp%"'`. Just `%` (zero, one or more) and `_` (exactly one) are allowed in regex patterns | No       | `'regex_temp:"temp%"'`                                        |
| `filters`                  | Filters for querying specific data. Format: `'field:"=value".field2:">value2"'`. Supported operators: `=`, `!=`, `>`, `<`, `>=`, `<=`.                               | No       | `'temp:>=101.hum:<=182'`                                      |
| `config_file_path`         | Path to the configuration file from `PLUGIN_DIR` env var. Format: `'example.toml'`.                                                                                  | No       | `'example.toml'`                                              |

#### Example
```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename basic_transformation.py \
  --trigger-spec "every:1d" \
  --trigger-arguments measurement=temperature,window=30d,target_measurement=transformed_temperature,names_transformations='room:"lower".temp:"snake"',values_transformations='temp:"convert_degC_to_degF".something:"upper"' \
  basic_transform_trigger
```

### Data Writes
The Data Writes feature transforms data in real-time as it is written to the database, applying the specified transformations and saving the results to a target measurement. This type does not require the `window` parameter since it processes data on the fly.

#### Arguments for Data Writes
The following arguments are extracted from the `args` dictionary:

| Argument                   | Description                                                                                                                                                          | Required | Example                                                       |
|----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------------------------------------------------------|
| `measurement`              | The InfluxDB measurement to query for historical data.                                                                                                               | Yes      | `"temperature"`                                               |
| `target_measurement`       | Destination measurement for storing transformed data.                                                                                                                | Yes      | `"transformed_temperature"`                                   |
| `names_transformations`    | Rules for transforming field and tag names. Format: `'field1:"transform1 transform2".pattern_name:"transform3 transform4'`.                                          | No       | `'room:"lower snake".temp:"upper".name:"custom_replacement"'` |
| `values_transformations`   | Rules for transforming field values. Format: `'field1:"transform1 transform2".pattern:"transform3"'`.                                                                | No       | `'temp:"convert_degC_to_degF".hum:"upper".something:"lower"'` |
| `target_database`          | Optional InfluxDB database name for writing transformed data.                                                                                                        | No       | `"transformed_db"`                                            |
| `included_fields`          | Dot-separated list of field names to include in the query.                                                                                                           | No       | `"temp.hum.something"`                                        |
| `excluded_fields`          | Dot-separated list of field names to exclude from the query.                                                                                                         | No       | `"co.h-u_m2"`                                                 |
| `dry_run`                  | If `"true"`, simulates the transformation without writing to the database. Defaults to `"false"`.                                                                    | No       | `"true"`                                                      |
| `custom_replacements`      | Custom replacement rules for transformations. Format: `'replace_space_underscore:" =_".cust_replace:"Some text=Another text"'`.                                      | No       | `'replace_space_underscore:" =_"'`                            |
| `custom_regex`             | Custom regex patterns for applying transformations. Format: `'regex_temp:"temp%"'`. Just `%` (zero, one or more) and `_` (exactly one) are allowed in regex patterns | No       | `'regex_temp:"temp%"'`                                        |
| `filters`                  | Filters for querying specific data. Format: `'field:"=value".field2:">value2"'`. Supported operators: `=`, `!=`, `>`, `<`, `>=`, `<=`.                               | No       | `'temp:>=101.hum:<=182'`                                      |
| `config_file_path`         | Path to the configuration file from `PLUGIN_DIR` env var. Format: `'example.toml'`.                                                                                  | No       | `'example.toml'`                                              |

#### Example for Data Writes
```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename basic_transformation.py \
  --trigger-spec "all_tables" \
  --trigger-arguments measurement=temperature,target_measurement=transformed_temperature,names_transformations='room:"lower".temp:"snake"',values_transformations='temp:"convert_degC_to_degF".something:"upper"' \
  basic_transform_trigger
```

## Available Transformations
### Predefined Transformations
The plugin supports the following predefined transformations for both field/tag names and values:

| Transformation           | Description                                                                                          | Example (Before → After)    |
|--------------------------|------------------------------------------------------------------------------------------------------|-----------------------------|
| `lower`                  | Converts the string to lowercase.                                                                    | `Room → room`               |
| `upper`                  | Converts the string to uppercase.                                                                    | `room → ROOM`               |
| `space_to_underscore`    | Replaces spaces with underscores.                                                                    | `Living Room → Living_Room` |
| `remove_space`           | Removes all spaces from the string.                                                                  | `Living Room → LivingRoom`  |
| `alnum_underscore_only`  | Keeps only alphanumeric characters and underscores, removing all other characters.                   | `temp_1! → temp_1`          |
| `collapse_underscore`    | Collapses multiple consecutive underscores into a single underscore.                                 | `temp__value → temp_value`  |
| `trim_underscore`        | Trims underscores from the beginning and end of the string.                                          | `_temp_ → temp`             |
| `snake`                  | Converts the string to snake_case (lowercase with underscores).                                      | `Living Room → living_room` |

### Unit Conversions using `pint` Library
- **Unit Conversions**: Use `convert_<from>_to_<to>` to convert units of measurement. For example, `convert_degC_to_degF` converts Celsius to Fahrenheit. The plugin uses the `pint` library for unit conversions. Supported units include temperature (e.g., `degC`, `degF`, `degK`, `degR`), length, time, and more. Refer to the [pint documentation](https://pint.readthedocs.io/en/stable/) for a full list of supported units.

  **Example**:
  - Before: `temp: 25` (assuming Celsius)
  - Transformation: `convert_degC_to_degF`
  - After: `temp: 77.0` (Fahrenheit)

### Custom Transformations
- **Custom Replacements**: Define custom string/substring replacements using the `custom_replacements` parameter. For example, `'replace_my_text:"Old text=New text"'` replaces `Old text` with `New text` in the specified fields or values.

  **Example**:
  - Before: `field_value: "Old text Value"`
  - Transformation: value_transformations=field:"replace_my_text" (with `custom_replacements='replace_my_text:"Old text=New text"'`)
  - After: `field_value: "New text Value"`

## Transformation Logic
- **Specific Field Transformations**: If a field or tag is explicitly specified in the transformation rules (e.g., `temp:"snake"`), the transformations are applied only to that field or tag, and regex patterns are not applied to it.
- **Regex-based Transformations**: If a regex pattern is specified in the transformation rules (e.g., `regex_temp:"temp%"` in `custom_regex`), the transformations are applied to all fields or tags that match the pattern, except those that are explicitly specified. Use `%` to match zero or more characters and `_` to match exactly one character in the regex patterns. To apply transformations to fields matching a regex pattern, specify the pattern name in `values_transformations` or `names_transformations` (e.g., `regex_temp:"snake lower"`).
- **Order of Application**: Transformations are applied in the order they are specified in the rules.

## Error Handling
- If a transformation fails (e.g., due to incompatible types or units), the plugin logs a warning in the `system.processing_engine_logs` table and retains the original value unchanged.
- Errors during data writing are handled with a retry mechanism (up to 3 attempts) before logging an error.

## Usage Examples
### Example 1: Basic Transformation
Transform the `temperature` measurement by converting field names to lowercase and values to uppercase.

**Command**:
```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename basic_transformation.py \
  --trigger-spec "every:1d" \
  --trigger-arguments measurement=temperature,window=30d,target_measurement=transformed_temperature,names_transformations='room:"lower".temp:"lower"',values_transformations='room:"upper".something:"upper"' \
  basic_transform_trigger
```

**Before Transformation**:
- Field: `Room`
- Value: `living room`

**After Transformation**:
- Field: `room`
- Value: `LIVING ROOM`

### Example 2: Using Custom Replacements
Transform the `temperature` measurement by replacing `Old text` with `New text` in field names and values.

**Command**:
```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename basic_transformation.py \
  --trigger-spec "every:1d" \
  --trigger-arguments measurement=temperature,window=30d,target_measurement=transformed_temperature,names_transformations=room:"replace_my_text",values_transformations=room:"replace_my_text",custom_replacements=replace_my_text:"Old text=New text" \
  basic_transform_trigger
```

**Before Transformation**:
- Field: `Old text Field`
- Value: `Old text Value`

**After Transformation**:
- Field: `New text Field`
- Value: `New text Value`

### Example 3: Using Regex Patterns
Transform all fields starting with `temp` by converting their values to Fahrenheit.

**Command**:
```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename basic_transformation.py \
  --trigger-spec "every:1d" \
  --trigger-arguments measurement=temperature,window=30d,target_measurement=transformed_temperature,values_transformations='regex_temp:"convert_degC_to_degF"',custom_regex='regex_temp:"temp%"' \
  basic_transform_trigger
```

**Before Transformation**:
- Field: `temp_celsius`
- Value: `25`

**After Transformation**:
- Field: `temp_celsius`
- Value: `77.0` (Fahrenheit)

## Important Notes
- **Data Requirements**: The queried measurement must contain a `time` column and the specified fields for transformation.
- **Field Inclusion/Exclusion**: Only one of `included_fields` or `excluded_fields` can be specified. If both are provided, the plugin will raise an error.
- **Dry Run Mode**: When `dry_run` is set to `"true"`, the plugin logs the transformed data without writing it to the database, allowing for testing and validation.
- **Field/Tag Name Caching**: The plugin caches the list of field and tag names for each measurement for one hour to avoid unnecessary repeated queries and improve performance.

## Questions/Comments
For support, open a GitHub issue or contact us via [Discord](https://discord.com/invite/vZe2w2Ds8B) in the `#influxdb3_core` channel, [Slack](https://influxcommunity.slack.com/) in the `#influxdb3_core` channel, or the [Community Forums](https://community.influxdata.com/).