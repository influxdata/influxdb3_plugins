# MQTT Subscriber Plugin

⚡ scheduled 🏷️ mqtt, ingestion, iot 🔧 InfluxDB 3 Core, InfluxDB 3 Enterprise

> **Note:** This plugin requires InfluxDB 3.8.2 or later.

## Description

The MQTT Subscriber Plugin enables real-time ingestion of MQTT messages into InfluxDB 3. Subscribe to MQTT broker topics and automatically transform messages into time-series data with support for JSON, Line Protocol, and custom text formats. The plugin uses persistent MQTT sessions (`clean_session=False`) to ensure message delivery between executions and provides comprehensive error tracking and statistics.

## Configuration

Plugin parameters may be specified as key-value pairs in the `--trigger-arguments` flag (CLI) or in the `trigger_arguments` field (API) when creating a trigger. This plugin supports TOML configuration files for complex mapping scenarios, which can be specified using the `config_file_path` parameter.

### Plugin metadata

This plugin includes a JSON metadata schema in its docstring that defines supported trigger types and configuration parameters. This metadata enables the [InfluxDB 3 Explorer](https://docs.influxdata.com/influxdb3/explorer/) UI to display and configure the plugin.

### Required parameters

In TOML configuration, `broker_host` and `topics` are placed under the `[mqtt]` section; `table_name` and `table_name_field` are placed under `[mapping.json]` or `[mapping.text]` section.

| Parameter     | Type   | Default                   | Description                                                                         |
|---------------|--------|---------------------------|-------------------------------------------------------------------------------------|
| `broker_host` | string | required                  | MQTT broker hostname or IP address                                                  |
| `topics`      | string | required                  | Space-separated list of topics (e.g., "sensors/temp sensors/humidity")              |
| `table_name`  | string | required (json/text only) | InfluxDB measurement name for storing data. Not required for `lineprotocol` format or when `table_name_field` is set. |
| `table_name_field` | string | none               | JSON field name or regex pattern to extract table name dynamically from each message. Alternative to static `table_name`. |

### Connection parameters

In TOML configuration, these parameters are placed under the `[mqtt]` section.

| Parameter     | Type    | Default        | Description                                        |
|---------------|---------|----------------|----------------------------------------------------|
| `broker_port` | integer | 1883           | MQTT broker port (1883 for non-TLS, 8883 for TLS)  |
| `qos`         | integer | 1              | MQTT Quality of Service level (0, 1, or 2)         |
| `client_id`   | string  | auto-generated | MQTT client identifier (must be unique per broker) |


**Recommendation:** Use QoS 1 for most IoT scenarios. It provides reliable delivery with minimal overhead.

### Authentication parameters

In TOML configuration, these parameters are placed under the `[mqtt.auth]` section.

| Parameter  | Type   | Default | Description                                     |
|------------|--------|---------|-------------------------------------------------|
| `username` | string | none    | MQTT broker username (required with password)   |
| `password` | string | none    | MQTT broker password (required with username)   |

**Note:** Both `username` and `password` must be provided together for authentication.

### TLS/SSL parameters

In TOML configuration, these parameters are placed under the `[mqtt.tls]` section.

| Parameter     | Type   | Default | Description                               |
|---------------|--------|---------|-------------------------------------------|
| `ca_cert`     | string | none    | Path to CA certificate file               |
| `client_cert` | string | none    | Path to client certificate for mutual TLS |
| `client_key`  | string | none    | Path to client private key for mutual TLS |

**Note:** For mutual TLS, both `client_cert` and `client_key` must be provided together.

### Message format parameters

In TOML configuration, `format` is placed under the `[mqtt]` section; `timestamp_field` is placed under `[mapping.json]` or `[mapping.text]` section.

| Parameter         | Type   | Default | Description                                                    |
|-------------------|--------|---------|----------------------------------------------------------------|
| `format`          | string | "json"  | Message format: json, lineprotocol, or text                    |
| `timestamp_field` | string | none    | Field containing timestamp (format depends on message format)  |

**Format-specific timestamp_field syntax:**

The timestamp field format differs between JSON and Text formats:

| Format   | Syntax              | Split Method       | Example (CLI)    | Example (TOML)     |
|----------|---------------------|--------------------|------------------|--------------------|
| JSON     | `field_name:format` | Split by first `:` | `"timestamp:ms"` | `"$.timestamp:ms"` |
| Text     | `regex:format`      | Split by last `:`  | `"ts:(\\d+):ms"` | `"ts:(\\d+):ms"`   |

**Note:** In CLI arguments, JSON paths are specified without `$.` prefix (added automatically). In TOML configuration, use full JSONPath syntax with `$.` prefix.

**Note:** Text format uses the last colon to split, allowing regex patterns to contain colons (e.g., time patterns).

**Supported timestamp formats:**
- `ns` - nanoseconds (Unix timestamp)
- `ms` - milliseconds (Unix timestamp)
- `s` - seconds (Unix timestamp)
- `datetime` - ISO 8601 string (e.g., "2021-12-01T12:00:00Z")

### JSON format parameters

In TOML configuration, `tags` are placed under `[mapping.json.tags]` section and `fields` under `[mapping.json.fields]` section.

| Parameter | Type   | Default  | Description                                                               |
|-----------|--------|----------|---------------------------------------------------------------------------|
| `tags`    | string | none     | Space-separated tag names. Example: "room sensor location"                |
| `fields`  | string | required | Space-separated field mappings. Format: "name:type=jsonpath" without `$.` |

**Field specification format:** `"temp:float=temperature hum:int=humidity status:bool=online"`

**Supported field types:** `int`, `uint`, `float`, `string`, `bool`

### Text format parameters

In TOML configuration, `tags` are placed under `[mapping.text.tags]` section and `fields` under `[mapping.text.fields]` section.

| Parameter | Type   | Default   | Description                                                       |
|-----------|--------|-----------|-------------------------------------------------------------------|
| `tags`    | string | none      | Space-separated tag patterns. Format: "name=regex_pattern"        |
| `fields`  | string | required  | Space-separated field patterns. Format: "name:type=regex_pattern" |

**Tag specification format:** `"room=room:([^,\\s]+) sensor=sensor:(\\w+)"`

**Field specification format:** `"temp:float=temp:([\\d.]+) status:bool=(true|false)"`

### TOML configuration

| Parameter          | Type   | Default | Description                                     |
|--------------------|--------|---------|-------------------------------------------------|
| `config_file_path` | string | none    | Path to TOML config file (absolute or relative) |

*To use a TOML configuration file, specify the `config_file_path` in the trigger arguments.*

### File path resolution

All file paths in the plugin (configuration file, TLS certificates) follow the same resolution logic:

- **Absolute paths** (e.g., `/etc/mqtt/config.toml`) are used as-is
- **Relative paths** (e.g., `config.toml`, `certs/ca.crt`) are resolved from `PLUGIN_DIR` environment variable

If a relative path is specified and `PLUGIN_DIR` is not set, the plugin will return an error.

#### Example TOML configuration

[mqtt_config_example.toml](mqtt_config_example.toml) - comprehensive configuration example with all three message formats

## Data requirements

The plugin automatically creates the target measurement table on first write. Field mappings are required for JSON and Text formats to specify which fields to extract and their data types.

### Message encoding requirements

- **UTF-8 text only**: The plugin only processes UTF-8 encoded text messages. Binary messages are automatically skipped with a warning logged.
- **Non-empty payloads**: Empty or whitespace-only messages are automatically skipped.

## Software Requirements

- **InfluxDB 3 Core/Enterprise**: with the Processing Engine enabled
- **Python packages**:
  - `paho-mqtt` (MQTT client library)
  - `jsonpath-ng` (JSON path parsing for JSON format)

### Installation steps

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
   influxdb3 install package paho-mqtt
   influxdb3 install package jsonpath-ng
   ```

## Trigger setup

### Scheduled ingestion with TOML configuration

Recommended for production use with complex mappings:

```bash
# 1. Set PLUGIN_DIR environment variable
export PLUGIN_DIR=~/.plugins

# 2. Copy and edit configuration file
cp mqtt_config_example.toml $PLUGIN_DIR/my_mqtt_config.toml
# Edit my_mqtt_config.toml with your broker and mapping settings

# 3. Create the trigger
influxdb3 create trigger \
  --database mydb \
  --plugin-filename gh:influxdata/mqtt_subscriber/mqtt_subscriber.py \
  --trigger-spec "every:10m" \
  --trigger-arguments config_file_path=my_mqtt_config.toml \
  mqtt_ingestion
```

### Scheduled ingestion with command-line arguments

For simple JSON message ingestion:

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename gh:influxdata/mqtt_subscriber/mqtt_subscriber.py \
  --trigger-spec "every:5m" \
  --trigger-arguments 'broker_host=broker.hivemq.com,topics=sensors/temperature sensors/humidity,format=json,table_name=sensor_data,fields=temp:float=temperature hum:int=humidity,tags=location sensor_id' \
  mqtt_sensors
```

### Secure MQTT connection with TLS

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename gh:influxdata/mqtt_subscriber/mqtt_subscriber.py \
  --trigger-spec "every:10m" \
  --trigger-arguments 'broker_host=secure-broker.example.com,broker_port=8883,topics=secure/data,format=json,table_name=secure_data,ca_cert=certs/ca.crt,username=myuser,password=mypass,fields=value:float=value' \
  secure_mqtt
```

## MQTT Topic Wildcards

The plugin supports standard MQTT wildcard patterns in topic subscriptions:

- `+` - Single level wildcard (matches one topic level)
- `#` - Multi-level wildcard (matches any number of levels, must be last)

**Examples:**
- `sensors/+/temperature` - matches `sensors/room1/temperature`, `sensors/room2/temperature`
- `sensors/#` - matches `sensors/temp`, `sensors/room1/humidity`, `sensors/building/floor1/co2`
- `+/+/data` - matches `home/kitchen/data`, `office/room1/data`

**Note:** Wildcards are processed by the MQTT broker, not the plugin. Ensure your broker supports the wildcard patterns you use.

## Message Formats

### JSON Format

The primary use case for structured IoT data. Supports nested fields using JSONPath expressions.

#### TOML Configuration

```toml
[mqtt]
broker_host = "broker.hivemq.com"
broker_port = 1883
topics = ["sensors/temperature"]
qos = 1
format = "json"

[mapping.json]
table_name = "sensor_data"
timestamp_field = "$.timestamp:ms"

[mapping.json.tags]
location = "$.location"
sensor_id = "$.sensor.id"

[mapping.json.fields]
temperature = ["$.temp", "float"]
humidity = ["$.humidity", "int"]
status = ["$.online", "bool"]
```

#### Example Message

```json
{
  "timestamp": 1638360000000,
  "location": "warehouse_a",
  "sensor": {
    "id": "sensor_001"
  },
  "temp": 22.5,
  "humidity": 65,
  "online": true
}
```

#### Resulting Data

```
sensor_data,location=warehouse_a,sensor_id=sensor_001 temperature=22.5,humidity=65i,status=true 1638360000000000000
```

#### JSON Array Support

Process batch messages containing arrays of JSON objects:

```json
[
  {"timestamp": 1638360000000, "sensor_id": "001", "temperature": 22.5},
  {"timestamp": 1638360001000, "sensor_id": "002", "temperature": 23.1},
  {"timestamp": 1638360002000, "sensor_id": "003", "temperature": 21.8}
]
```

**Array processing behavior:**
- Each array element is processed independently as a separate data point
- If one element fails to parse, the others continue processing (partial success)
- Parse errors for individual elements are logged to `mqtt_exceptions` table
- Statistics count 1 MQTT message = 1 unit (regardless of array size)

### Line Protocol Format

For messages already in InfluxDB line protocol format, use passthrough mode. No mapping configuration needed - messages are validated and written directly.

#### TOML Configuration

```toml
[mqtt]
broker_host = "broker.example.com"
topics = ["influxdb/metrics"]
format = "lineprotocol"
```

#### CLI Configuration

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename mqtt_subscriber.py \
  --trigger-spec "every:10m" \
  --trigger-arguments 'broker_host=broker.example.com,topics=influxdb/metrics,format=lineprotocol' \
  mqtt_lineprotocol
```

#### Example Message

```
sensor_data,location=warehouse_a,sensor_id=001 temperature=22.5,humidity=65i 1638360000000000000
```

#### Supported Line Protocol Types

| Type             | Suffix         | Example            |
|------------------|----------------|--------------------|
| Float            | none           | `temperature=22.5` |
| Integer          | `i`            | `count=100i`       |
| Unsigned Integer | `u`            | `bytes=1024u`      |
| String           | `"..."`        | `status="running"` |
| Boolean          | `true`/`false` | `active=true`      |

### Text Format

Parse plain text messages using regular expressions:

#### TOML Configuration

```toml
[mqtt]
format = "text"

[mapping.text]
table_name = "sensor_logs"
timestamp_field = "ts:(\\d+):ms"

[mapping.text.tags]
location = "location=([^,\\s]+)"

[mapping.text.fields]
temperature = ["temp:([\\d.]+)", "float"]
humidity = ["hum:(\\d+)", "int"]
status = ["status:(true|false)", "bool"]
```

#### Example Message

```
location=warehouse_a,temp:22.5,hum:65,status:true,ts:1638360000000
```

## Statistics and Monitoring

The plugin tracks comprehensive statistics and writes them to the `mqtt_stats` table on every plugin invocation.

**Important notes:**
- Statistics are written **on every plugin invocation**
- Each topic is tracked separately with independent statistics
- Statistics persist across plugin restarts using the InfluxDB cache

### mqtt_stats Table

| Field                 | Type  | Description                            |
|-----------------------|-------|----------------------------------------|
| `topic` (tag)         | tag   | MQTT topic name                        |
| `broker_host` (tag)   | tag   | MQTT broker address                    |
| `messages_received`   | int   | Total messages received on this topic  |
| `messages_processed`  | int   | Successfully processed messages        |
| `messages_failed`     | int   | Failed messages                        |
| `success_rate`        | float | Percentage of successful messages      |

### Querying Statistics

```bash
# Get latest statistics
influxdb3 query --database mydb \
  "SELECT * FROM mqtt_stats ORDER BY time DESC LIMIT 10"

# Success rate over time
influxdb3 query --database mydb \
  "SELECT topic, success_rate, messages_processed, messages_failed
   FROM mqtt_stats
   WHERE time > now() - INTERVAL '1 hour'
   ORDER BY time DESC"
```

## Error Handling

Parse errors and message processing failures are logged to the `mqtt_exceptions` table:

### mqtt_exceptions Table

| Field             | Type   | Description                              |
|-------------------|--------|------------------------------------------|
| `topic` (tag)     | tag    | MQTT topic where error occurred          |
| `error_type` (tag)| tag    | Type of error (e.g., JSONDecodeError)    |
| `error_message`   | string | Detailed error message                   |
| `raw_message`     | string | Original MQTT message (truncated to 1KB) |

### Checking for Errors

```bash
influxdb3 query --database mydb \
  "SELECT * FROM mqtt_exceptions ORDER BY time DESC LIMIT 10"
```

## Troubleshooting

### Check Plugin Logs

```bash
influxdb3 query --database _internal \
  "SELECT * FROM system.processing_engine_logs
   WHERE trigger_name = 'mqtt_ingestion'
   ORDER BY time DESC LIMIT 20"
```

### Common Issues

#### "paho-mqtt library not installed"

```bash
influxdb3 install package paho-mqtt
```

#### "Configuration file not found"

- For relative paths, ensure `PLUGIN_DIR` environment variable is set
- For absolute paths, verify the file exists at the specified location

```bash
# For relative paths
export PLUGIN_DIR=~/.plugins
ls $PLUGIN_DIR/my_mqtt_config.toml

# Or use absolute path
ls /etc/mqtt/my_mqtt_config.toml
```

#### "Failed to connect to MQTT broker"

- Verify broker address and port
- Check network connectivity
- For TLS connections, verify certificate paths
- Verify authentication credentials (both username and password required)

#### "Both username and password must be provided for authentication"

Either provide both `username` and `password`, or omit both for anonymous connection.

#### "Both client_cert and client_key must be provided for mutual TLS"

For mutual TLS authentication, both client certificate and key are required.

#### "No fields were mapped from JSON data"

- Verify JSONPath expressions in field mappings (use `$.` prefix)
- Check that JSON structure matches your paths
- Review `mqtt_exceptions` table for detailed errors

#### Messages not being processed

- Check trigger status: `influxdb3 show summary --database mydb`
- Verify MQTT connection in plugin logs
- Increase trigger frequency (e.g., from `every:5s` to `every:1s`)

## Architecture

### How It Works

1. **Scheduled Trigger**: Plugin runs on schedule (e.g., `every:10m`)
2. **Configuration Caching**: Plugin configuration is parsed once and cached between executions
3. **Message Queue**: Incoming MQTT messages are queued in-memory during callback execution
4. **Batch Processing**: Each trigger execution processes all queued messages
5. **Parse & Write**: Messages parsed according to format and written to InfluxDB
6. **Error Tracking**: Parse errors logged to `mqtt_exceptions` table
7. **Statistics**: Written to `mqtt_stats` table on every plugin invocation

### Performance Optimization

The plugin includes several optimizations for high-throughput scenarios:

- **Configuration Caching**: Plugin configuration is parsed once and reused across all trigger executions
- **Pre-compiled Patterns**: JSONPath expressions and regex patterns are compiled once during parser initialization, not per-message
- **Persistent Session**: MQTT client uses `clean_session=False` - broker preserves subscriptions and queued messages between connections
