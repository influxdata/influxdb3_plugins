# Plugin Name

⚡ scheduled, data-write, http 🏷️ tag1, tag2, tag3 🔧 InfluxDB 3 Core, InfluxDB 3 Enterprise

## Description

Brief description of what the plugin does and its primary use case. Include the trigger types supported (write, scheduled, HTTP) and main functionality. Mention any special features or capabilities that distinguish this plugin. Add a fourth sentence if needed for additional context.

## Configuration

Plugin parameters may be specified as key-value pairs in the `--trigger-arguments` flag (CLI) or in the `trigger_arguments` field (API) when creating a trigger. Some plugins support TOML configuration files, which can be specified using the plugin's `config_file_path` parameter.

If a plugin supports multiple trigger specifications, some parameters may depend on the trigger specification that you use.

### Plugin metadata

This plugin includes a JSON metadata schema in its docstring that defines supported trigger types and configuration parameters. This metadata enables the [InfluxDB 3 Explorer](https://docs.influxdata.com/influxdb3/explorer/) UI to display and configure the plugin.

### Required parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `parameter_name` | string | required | Description of the parameter |
| `another_param` | integer | required | Description with any constraints or requirements |

### Optional parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `optional_param` | boolean | "false" | Description of optional parameter |
| `timeout` | integer | 30 | Connection timeout in seconds |

### [Category] parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `category_param` | string | "default" | Parameters grouped by functionality |

### TOML configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `config_file_path` | string | none | TOML config file path relative to `PLUGIN_DIR` (required for TOML configuration) |

*To use a TOML configuration file, set the `PLUGIN_DIR` environment variable and specify the `config_file_path` in the trigger arguments.* This is in addition to the `--plugin-dir` flag when starting InfluxDB 3.

#### Example TOML configurations

- [plugin_config_scheduler.toml](plugin_config_scheduler.toml) - for scheduled triggers
- [plugin_config_data_writes.toml](plugin_config_data_writes.toml) - for data write triggers

For more information on using TOML configuration files, see the Using TOML Configuration Files section in the [influxdb3_plugins/README.md](/README.md).

## [Special Requirements Section]

<!-- Add this section only if the plugin has special requirements like:
- Data requirements
- Schema requirements  
- Software requirements
- Prerequisites
-->

### Data requirements

The plugin requires [specific data format or schema requirements].

### Software requirements

- **InfluxDB 3 Core/Enterprise**: with the Processing Engine enabled
- **Python packages**:
  - `package_name` (for specific functionality)

## Installation steps

1. Start InfluxDB 3 with the Processing Engine enabled (`--plugin-dir /path/to/plugins`):

   ```bash
   influxdb3 serve \
     --node-id node0 \
     --object-store file \
     --data-dir ~/.influxdb3 \
     --plugin-dir ~/.plugins
   ```

2. Install required Python packages (if any):

   ```bash
   influxdb3 install package package_name
   ```

## Trigger setup

### Scheduled trigger

Run the plugin periodically on historical data:

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename gh:influxdata/plugin_name/plugin_name.py \
  --trigger-spec "every:1h" \
  --trigger-arguments 'parameter_name=value,another_param=100' \
  scheduled_trigger_name
```

### Data write trigger

Process data as it's written:

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename gh:influxdata/plugin_name/plugin_name.py \
  --trigger-spec "all_tables" \
  --trigger-arguments 'parameter_name=value,another_param=100' \
  write_trigger_name
```

### HTTP trigger

Process data via HTTP requests:

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename gh:influxdata/plugin_name/plugin_name.py \
  --trigger-spec "request:endpoint" \
  --trigger-arguments 'parameter_name=value,another_param=100' \
  http_trigger_name
```

## Example usage

### Example 1: [Use case name]

[Description of what this example demonstrates]

```bash
# Create the trigger
influxdb3 create trigger \
  --database weather \
  --plugin-filename gh:influxdata/plugin_name/plugin_name.py \
  --trigger-spec "every:30m" \
  --trigger-arguments 'parameter_name=value,another_param=100' \
  example_trigger

# Write test data
influxdb3 write \
  --database weather \
  "measurement,tag=value field=22.5"

# Query results (after trigger runs)
influxdb3 query \
  --database weather \
  "SELECT * FROM result_measurement"
```

### Expected output

```
tag | field | time
----|-------|-----
value | 22.5 | 2024-01-01T00:00:00Z
```

**Transformation details:**
- Before: `field=22.5` (original value)
- After: `field=22.5` (processed value with description of changes)

### Example 2: [Another use case]

[Description of what this example demonstrates]

```bash
# Create trigger with different configuration
influxdb3 create trigger \
  --database sensors \
  --plugin-filename gh:influxdata/plugin_name/plugin_name.py \
  --trigger-spec "all_tables" \
  --trigger-arguments 'parameter_name=different_value,optional_param=true' \
  another_trigger

# Write data with specific format
influxdb3 write \
  --database sensors \
  "raw_data,device=sensor1 value1=20.1,value2=45.2"

# Query processed data
influxdb3 query \
  --database sensors \
  "SELECT * FROM processed_data"
```

### Expected output

```
device | value1 | value2 | time
-------|--------|--------|-----
sensor1 | 20.1 | 45.2 | 2024-01-01T00:00:00Z
```

**Processing details:**
- Before: `value1=20.1`, `value2=45.2`
- After: [Description of any transformations applied]

### Example 3: [Complex scenario]

[Add more examples as needed to demonstrate different features]

## Code overview

### Files

- `plugin_name.py`: Main plugin code containing handlers for trigger types
- `plugin_config_scheduler.toml`: Example TOML configuration for scheduled triggers
- `plugin_config_data_writes.toml`: Example TOML configuration for data write triggers

### Main functions

#### `process_scheduled_call(influxdb3_local, call_time, args)`
Handles scheduled trigger execution. Queries historical data within the specified window and applies processing logic.

Key operations:
1. Parses configuration from arguments
2. Queries source data with filters
3. Applies processing logic
4. Writes results to target measurement

#### `process_writes(influxdb3_local, table_batches, args)`
Handles real-time data processing during writes. Processes incoming data batches and applies transformations before writing.

Key operations:
1. Filters relevant table batches
2. Applies processing to each row
3. Writes to target measurement immediately

#### `process_http_request(influxdb3_local, request_body, args)`
Handles HTTP-triggered processing. Processes data sent via HTTP requests.

Key operations:
1. Parses request body
2. Validates input data
3. Applies processing logic
4. Returns response

### Key logic

1. **Data Validation**: Checks input data format and required fields
2. **Processing**: Applies the main plugin logic to transform/analyze data
3. **Output Generation**: Formats results and metadata
4. **Error Handling**: Manages exceptions and provides meaningful error messages

### Plugin Architecture

```
Plugin Module
├── process_scheduled_call()   # Scheduled trigger handler
├── process_writes()           # Data write trigger handler
├── process_http_request()     # HTTP trigger handler
├── validate_config()          # Configuration validation
├── apply_processing()         # Core processing logic
└── helper_functions()         # Utility functions
```

## Troubleshooting

### Common issues

#### Issue: "Configuration parameter missing"
**Solution**: Check that all required parameters are provided in the trigger arguments. Verify parameter names match exactly (case-sensitive).

#### Issue: "Permission denied" errors
**Solution**: Ensure the plugin file has execute permissions:
```bash
chmod +x ~/.plugins/plugin_name.py
```

#### Issue: "Module not found" error
**Solution**: Install required Python packages:
```bash
influxdb3 install package package_name
```

#### Issue: No data in target measurement
**Solution**: 
1. Check that source measurement contains data
2. Verify trigger is enabled and running
3. Check logs for errors:
   ```bash
   influxdb3 query \
     --database _internal \
     "SELECT * FROM system.processing_engine_logs WHERE trigger_name = 'your_trigger_name'"
   ```

### Debugging tips

1. **Enable debug logging**: Add `debug=true` to trigger arguments
2. **Use dry run mode**: Set `dry_run=true` to test without writing data
3. **Check field names**: Use `SHOW FIELD KEYS FROM measurement` to verify field names
4. **Test with small windows**: Use short time windows for testing (e.g., `window=1h`)
5. **Monitor resource usage**: Check CPU and memory usage during processing

### Performance considerations

- Processing large datasets may require increased memory
- Use filters to process only relevant data
- Batch size affects memory usage and processing speed
- Consider using specific_fields to limit processing scope
- Cache frequently accessed data when possible

## Questions/Comments

For questions or comments about this plugin, please open an issue in the [influxdb3_plugins repository](https://github.com/influxdata/influxdb3_plugins/issues).

---

## Documentation Sync

After making changes to this README, sync to the documentation site:

1. **Commit your changes** to the influxdb3_plugins repository
2. **Look for the sync reminder** - A comment will appear on your commit with a sync link  
3. **Click the link** - This opens a pre-filled form to trigger the docs-v2 sync
4. **Submit the sync request** - A workflow will validate, transform, and create a PR

The documentation site will be automatically updated with your changes after review.

---

## Template Usage Notes

When using this template:

1. Replace `Plugin Name` with the actual plugin name
2. Update emoji metadata with appropriate trigger types and tags
3. Fill in all parameter tables with actual configuration options
4. Provide real, working examples with expected output
5. Include actual function names and signatures
6. Add plugin-specific troubleshooting scenarios
7. Remove any sections that don't apply to your plugin
8. Remove this "Template Usage Notes" section from the final README

### Section Guidelines

- **Description**: 2-4 sentences, be specific about capabilities
- **Configuration**: Group parameters logically, mark required clearly
- **Examples**: At least 2 complete, working examples
- **Expected output**: Show actual output format
- **Troubleshooting**: Include plugin-specific issues and solutions
- **Code overview**: Document main functions and logic flow