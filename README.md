# InfluxDB 3 Plugins

This repository contains publicly shared plugins compatible with the [InfluxDB 3 Core](https://www.influxdata.com/products/influxdb/) and [InfluxDB 3 Enterprise](https://www.influxdata.com/products/influxdb-3-enterprise/?dl=enterprise) built-in processing engine.
You can install these plugins to your InfluxDB 3 Enterprise or Core instance with a single command from the `influxdb3` CLI.

## Description

InfluxDB 3 plugins extend the functionality of your InfluxDB instance with custom data processing, transformation, and notification capabilities.
Plugins and triggers support three main types: scheduled execution, data write events, and HTTP requests.

For more information about using plugins and triggers, see the InfluxDB 3 Get Started tutorial:
- [Process data in InfluxDB 3 Core](https://docs.influxdata.com/influxdb3/core/get-started/process/)
- [Process data in InfluxDB 3 Enterprise](https://docs.influxdata.com/influxdb3/enterprise/get-started/process/)

## Plugin Organization

Plugins in this repo are organized in a structured directory hierarchy that reflects their contributor or organizational grouping:

```
organization/
├── plugin_name/
│   ├── README.md
│   ├── plugin_name.py
│   ├── config_*.toml (optional)
│   └── plugin_library.json (for influxdata plugins)
```

### Directory Structure Examples

```
influxdata/basic_transformation/basic_transformation.py
examples/schedule/system_metrics/system_metrics.py
suyashcjoshi/data-replicator/data-replicator.py
```

### Naming Conventions

- The Python file must have the same name as its parent directory
- Use snake_case for Python files and directory names
- Plugin directories may contain additional files such as configuration templates, test data, and documentation

## Using Plugins 

To use a plugin, you create a trigger in InfluxDB 3 (via CLI or HTTP API) that specifies the trigger name, database, trigger specification, plugin file, and any additional parameters--for example:

```bash
# Create trigger for scheduled plugin
influxdb3 create trigger \
  --database mydb \
  --plugin-filename plugin_name.py \
  --trigger-spec "every:1h" \
  --trigger-arguments param1=value1 \
  trigger_name
```

Replace the following:
- `mydb`: the name of the [database](https://docs.influxdata.com/influxdb3/core/admin/databases/) to use
- `every:1h`: the [trigger specification](https://docs.influxdata.com/influxdb3/core/get-started/process/#trigger-specifications) (for example, `every:1h`, `on_write`, or `request:notify`)
- `plugin_name.py`: the path to the plugin file (relative to the plugin directory)
- `trigger_name`: a unique name for this trigger instance
- `param1=value1`: any additional `key=value` [arguments](https://docs.influxdata.com/influxdb3/core/plugins/#pass-arguments-to-plugins) to pass to the plugin

## Plugin Metadata

Plugins in this repository require metadata in a JSON-formatted
docstring at the beginning of the Python file.
This metadata:
- Defines the plugin's supported trigger types
(`scheduled`, `onwrite`, `http`)
- Specifies configuration parameters for each
trigger type
- Enables the InfluxDB 3 Explorer UI to display
and configure the plugin

To display the plugin metadata in the Explorer UI,
repository owners generate a `plugin_library.json` registry file
that contains metadata for all plugins in this repository.

For complete metadata specifications, formatting
 requirements, and examples, see
[REQUIRED_PLUGIN_METADATA.md](REQUIRED_PLUGIN_MET
ADATA.md).

## Plugin Development

### Requirements

- Python 3.7 or higher
- InfluxDB 3 CLI installed and configured
- Required Python libraries (varies by plugin)

### Plugin Types

1. **Scheduled Plugins**: Execute on time intervals
   - Implement `process_scheduled_call(influxdb3_local, call_time, args)`
   
2. **Data Write Plugins**: Trigger on WAL flush events
   - Implement `process_writes(influxdb3_local, table_batches, args)`
   
3. **HTTP Plugins**: Respond to HTTP requests
   - Implement `process_http_request(influxdb3_local, request_body, args)`

### Development Guidelines

- Follow Google Python docstring style
- Use type hints where possible
- Include comprehensive error handling
- Support both dry-run and live execution modes
- Follow the [Style Guide](CONTRIBUTING.md) for documentation standards

### Run Tests 

InfluxDB 3 Core and InfluxDB 3 Enterprise provide the `influxdb3 test` CLI command to validate plugins without creating a trigger--for example:

```bash
influxdb3 test schedule_plugin \
  --database DATABASE_NAME \
  --token AUTH_TOKEN \
  --input-arguments threshold=10,unit=seconds \
  --schedule "0 0 * * * ?" \
  PLUGIN_FILENAME.py
```

You can also use Docker Compose to run tests for plugins in a containerized environment:

```bash
# Test all influxdata plugins with InfluxDB 3 Core
docker compose --profile test run --rm test-core-all

# Test a specific plugin
PLUGIN_PATH="influxdata/basic_transformation" \
docker compose --profile test run --rm test-core-specific

# Test with TOML configuration
PLUGIN_PATH="influxdata/basic_transformation" \
PLUGIN_FILE="basic_transformation.py" \
TOML_CONFIG="basic_transformation_config_scheduler.toml" \
docker compose --profile test run --rm test-core-toml
```

## Configuration

Plugins use TOML configuration files for parameter management:

```toml
# Required parameters
measurement = "temperature"
window = "30d"
target_measurement = "transformed_temperature"

# Optional parameters
dry_run = false
target_database = "analytics"
```

Configuration supports:
- Environment variable substitution via `PLUGIN_DIR`
- Runtime argument parsing from trigger configurations
- Both inline arguments and external configuration files

## Contributing

When contributing new plugins:

1. Create a new directory under your organization/username
2. Follow the established directory structure
3. Include comprehensive documentation following the [Style Guide](STYLE_GUIDE.md)
4. Test your plugin thoroughly before submission
5. Update the plugin library metadata if applicable
6. Submit a pull request to [this repository](https://github.com/influxdata/influxdb3_plugins) with a clear description of changes

## License

All plugins in this repository are dual licensed MIT or Apache 2 at the user's choosing, unless a LICENSE file is present in the plugin's directory.
