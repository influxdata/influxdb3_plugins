# AI Assistant Instructions

This file provides guidance to AI assistants when working with code in this repository.

## Project Overview

This repository contains publicly shared plugins for InfluxDB 3 Core and Enterprise. See [README.md](/README.md) for detailed project description and plugin organization structure.

## Plugin Architecture

### Core Plugin Structure
Each plugin implements specific functions based on trigger type:

- `process_scheduled_call(influxdb3_local, call_time, args)` - for scheduled plugins
- `process_writes(influxdb3_local, table_batches, args)` - for data write plugins  
- `process_http_request(influxdb3_local, request_body, args)` - for HTTP plugins

### Plugin Organization
Structured directory hierarchy by organization:
```
organization/plugin_name/plugin_name.py
examples/schedule/system_metrics/system_metrics.py
influxdata/basic_transformation/basic_transformation.py
```

### Required Plugin Metadata
All plugins must include JSON metadata schema in docstring defining:
- Supported trigger types (`scheduled`, `onwrite`, `http`)
- Configuration parameters for each trigger type
- Enable InfluxDB 3 Explorer UI integration

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
1. Docker-based testing (recommended): `./docker-test.sh`
2. Python environment testing: `python test-plugins.py`
3. TOML configuration testing with `PLUGIN_DIR` environment variable

### Test Requirements
- Docker and Docker Compose for containerized testing
- Python packages installed automatically via InfluxDB API
- PLUGIN_DIR environment variable for TOML configuration

## Dependencies and Libraries

### Common Python Libraries
- `pint` - Unit conversions
- `pandas` - Data manipulation  
- `requests` - HTTP communications
- `prophet` - Time series forecasting
- `adtk` - Anomaly detection
- `psutil` - System metrics

### Installation
```bash
influxdb3 install package <package_name>
```

## Error Handling Patterns

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