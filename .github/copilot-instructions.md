# AI Assistant Instructions

This file provides guidance to AI assistants when working with code in this repository.

## Project Overview

This repository contains publicly shared plugins for InfluxDB 3 Core and Enterprise. See [README.md](/README.md) for detailed project description and plugin organization structure.

## Plugin Architecture
See [README.md](/README.md#plugin-types) for plugin types and development guidelines.

### Core Plugin Structure
Each plugin must implement specific functions based on its type:

- `process_scheduled_call(influxdb3_local, call_time, args)` - for scheduled plugins
- `process_writes(influxdb3_local, table_batches, args)` - for data write plugins  
- `process_http_request(influxdb3_local, request_body, args)` - for HTTP plugins

### Plugin Organization
See [README.md](/README.md#plugin-organization) for directory structure and naming conventions.

### Required Plugin Metadata
All plugins must include JSON metadata schema in docstring defining:
- Supported trigger types (`scheduled`, `onwrite`, `http`)
- Configuration parameters for each trigger type
- Enable InfluxDB 3 Explorer UI integration

See [REQUIRED_PLUGIN_METADATA.md](../REQUIRED_PLUGIN_METADATA.md) for complete metadata specifications.

## Key Components

### LineBuilder Usage
```python
line = LineBuilder("measurement_name")
    .tag("tag_key", "tag_value")
    .int64_field("field_name", value)
    .time_ns(timestamp)
influxdb3_local.write(line)
```

### Configuration Management
- TOML configuration files for plugin parameters
- Environment variable support via `PLUGIN_DIR`
- Runtime argument parsing from trigger configurations

### Common Patterns
- Configuration validation with required/optional parameters
- Error handling with task IDs for logging
- Retry logic for database operations
- Caching for performance optimization

## Documentation Standards

Follow [CONTRIBUTING.md](../CONTRIBUTING.md) for:
- README structure with emoji metadata (⚡ trigger types, 🏷️ tags, 🔧 compatibility)
- Configuration tables with types and defaults
- Complete usage examples with expected outputs
- Troubleshooting sections
- Google Developer Documentation style

### Emoji Conventions
- ⚡ `scheduled`, `data-write`, `http` (trigger types)
- 🏷️ `monitoring`, `transformation`, `alerting` (functionality tags)
- 🔧 `InfluxDB 3 Core`, `InfluxDB 3 Enterprise` (compatibility)

## Testing

### Testing Methods
1. Docker Compose testing (recommended): `docker compose --profile test run --rm test-core-all`
2. Python environment testing: `python test/test_plugins.py`
3. TOML configuration testing with `PLUGIN_DIR` environment variable

### Test Requirements
- Docker and Docker Compose for containerized testing
- Python packages installed automatically via InfluxDB API
- PLUGIN_DIR environment variable for TOML configuration
- Validate plugins using the `influxdb3` CLI or HTTP API

## Configuration Files

See [README.md](/README.md#configuration) for plugin configuration patterns and examples.

## Dependencies and Libraries

### Common Python Libraries
- `pint` - Unit conversions
- `pandas` - Data manipulation
- `requests` - HTTP communications
- `prophet` - Time series forecasting
- `adtk` - Anomaly detection
- `psutil` - System metrics

### Install Dependencies
Using the CLI:

```bash
influxdb3 install package <package_name>
```

Or the HTTP API:

```
POST /api/v3/configure/plugin_environment/install_packages
{
  "packages": [
    "<package_name>"
  ]
}
```

## Error Handling Patterns

Follow the error handling standards in [CONTRIBUTING.md](../CONTRIBUTING.md#error-handling).

### Standard Error Response
```python
try:
    # Plugin logic
    pass
except Exception as e:
    influxdb3_local.error(f"[{task_id}] Error: {e}")
    return
```

### Logging Levels
- `influxdb3_local.info()` - General information
- `influxdb3_local.warn()` - Warnings and non-critical issues
- `influxdb3_local.error()` - Critical errors and failures

## Testing Plugin Functionality

1. Start InfluxDB 3 with plugin support:
   ```bash
   influxdb3 serve --node-id node0 --object-store file --data-dir ~/.influxdb3 --plugin-dir ~/.plugins
   ```
2. Create a database:
   ```bash
   influxdb3 create testdb
   ```
3. [Install required Python packages](#install-dependencies)
4. Run the test without creating a trigger:
   ```bash
   influxdb3 test <plugin_name> --database testdb
   ```

See [README.md](/README.md#plugin-installation) for plugin installation and trigger creation examples.

## Plugin Library Management

The `influxdata/library/plugin_library.json` file maintains metadata for all official plugins including:
- Plugin descriptions and documentation links
- Required libraries and dependencies
- Supported trigger types
- Version information