# Kafka Subscriber Plugin

⚡ scheduled 🏷️ kafka, ingestion, streaming 🔧 InfluxDB 3 Core, InfluxDB 3 Enterprise

> **Note:** This plugin requires InfluxDB 3.8.2 or later.

## Description

The Kafka Subscriber Plugin enables real-time ingestion of Kafka messages into InfluxDB 3. Subscribe to Kafka topics and automatically transform messages into time-series data with support for JSON, Line Protocol, and custom text formats. The plugin uses consumer groups for reliable message delivery and provides flexible offset commit policies for different use cases.

**Important:** Protobuf and Avro formats are NOT supported as they require Schema Registry integration. For these formats, consider using an external deserialization layer before writing to Kafka in a supported format.

## Configuration

Plugin parameters may be specified as key-value pairs in the `--trigger-arguments` flag (CLI) or in the `trigger_arguments` field (API) when creating a trigger. This plugin supports TOML configuration files for complex mapping scenarios, which can be specified using the `config_file_path` parameter.

### Plugin metadata

This plugin includes a JSON metadata schema in its docstring that defines supported trigger types and configuration parameters. This metadata enables the [InfluxDB 3 Explorer](https://docs.influxdata.com/influxdb3/explorer/) UI to display and configure the plugin.

### Required parameters

In TOML configuration, `bootstrap_servers`, `topics`, and `group_id` are placed under the `[kafka]` section; `table_name` and `table_name_field` are placed under `[mapping.json]` or `[mapping.text]` section.

| Parameter           | Type   | Default                   | Description                                                                         |
|---------------------|--------|---------------------------|-------------------------------------------------------------------------------------|
| `bootstrap_servers` | string | required                  | Space-separated list of Kafka broker addresses (e.g., "kafka1:9092 kafka2:9092")    |
| `topics`            | string | required                  | Space-separated list of topics (e.g., "sensor_data metrics")                        |
| `group_id`          | string | required                  | Kafka consumer group ID (must be unique per consumer group)                         |
| `table_name`        | string | required (json/text only) | InfluxDB measurement name for storing data. Not required for `lineprotocol` format or when `table_name_field` is set. |
| `table_name_field`  | string | none                      | JSON field name or regex pattern to extract table name dynamically from each message. Alternative to static `table_name`. |

### Connection parameters

In TOML configuration, these parameters are placed under the `[kafka]` section.

| Parameter           | Type   | Default     | Description                                                    |
|---------------------|--------|-------------|----------------------------------------------------------------|
| `auto_offset_reset` | string | "earliest"  | Where to start consuming on first connect: "earliest" or "latest" |
| `max_poll_records`  | int    | 500         | Maximum messages per scheduled call. Set to 0 for unlimited.   |

### Offset Commit Policy

In TOML configuration, this parameter is placed under the `[kafka]` section.

| Parameter              | Type   | Default      | Description                                                       |
|------------------------|--------|--------------|-------------------------------------------------------------------|
| `offset_commit_policy` | string | "on_success" | When to commit offsets: "on_success" or "always"                  |

**Policy behavior:**

| Policy       | Behavior                                                                                     |
|--------------|----------------------------------------------------------------------------------------------|
| `on_success` | Commit offsets only after ALL messages in the batch are successfully processed and written   |
| `always`     | Commit offsets immediately after receiving messages, regardless of processing success        |

**When to use each policy:**

- **on_success (recommended)**: Use when data integrity is important. Failed messages will be reprocessed on the next trigger execution.
- **always**: Use in high-throughput scenarios where occasional data loss is acceptable, or when you have external error handling (failed messages are logged to `kafka_exceptions` table).

### Security parameters

In TOML configuration, this parameter is placed under the `[kafka]` section.

| Parameter           | Type   | Default     | Description                                                              |
|---------------------|--------|-------------|--------------------------------------------------------------------------|
| `security_protocol` | string | "PLAINTEXT" | Security protocol: "PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"      |

### SASL Authentication parameters

In TOML configuration, these parameters are placed under the `[kafka.sasl]` section with shortened names.

| Parameter (CLI)  | Parameter (TOML)             | Type   | Default | Description                                                          |
|------------------|------------------------------|--------|---------|----------------------------------------------------------------------|
| `sasl_mechanism` | `[kafka.sasl]` `mechanism`   | string | none    | SASL mechanism: "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"            |
| `sasl_username`  | `[kafka.sasl]` `username`    | string | none    | SASL username (required with sasl_mechanism)                         |
| `sasl_password`  | `[kafka.sasl]` `password`    | string | none    | SASL password (required with sasl_mechanism)                         |

**Note:** All three SASL parameters must be provided together when using SASL authentication.

### SSL/TLS parameters

In TOML configuration, these parameters are placed under the `[kafka.ssl]` section with shortened names.

| Parameter (CLI)    | Parameter (TOML)               | Type   | Default | Description                                    |
|--------------------|--------------------------------|--------|---------|------------------------------------------------|
| `ssl_ca_cert`      | `[kafka.ssl]` `ca_cert`        | string | none    | Path to CA certificate file                    |
| `ssl_cert`         | `[kafka.ssl]` `client_cert`    | string | none    | Path to client certificate for mutual TLS      |
| `ssl_key`          | `[kafka.ssl]` `client_key`     | string | none    | Path to client private key for mutual TLS      |
| `ssl_key_password` | `[kafka.ssl]` `key_password`   | string | none    | Password for encrypted client private key      |

**Note:** For mutual TLS, both `ssl_cert` and `ssl_key` must be provided together.

### Message format parameters

In TOML configuration, `format` is placed under the `[kafka]` section; `timestamp_field` is placed under `[mapping.json]` or `[mapping.text]` section.

| Parameter         | Type   | Default | Description                                                    |
|-------------------|--------|---------|----------------------------------------------------------------|
| `format`          | string | "json"  | Message format: json, lineprotocol, or text                    |
| `timestamp_field` | string | none    | Field containing timestamp (format depends on message format)  |

**Supported formats:**

| Format         | Description                                      |
|----------------|--------------------------------------------------|
| `json`         | JSON with JSONPath field mapping                 |
| `lineprotocol` | InfluxDB Line Protocol passthrough               |
| `text`         | Plain text with regex-based parsing              |

**Format-specific timestamp_field syntax:**

| Format   | Syntax              | Split Method       | Example (CLI)    | Example (TOML)     |
|----------|---------------------|--------------------|------------------|--------------------|
| JSON     | `field_name:format` | Split by first `:` | `"timestamp:ms"` | `"$.timestamp:ms"` |
| Text     | `regex:format`      | Split by last `:`  | `"ts:(\\d+):ms"` | `"ts:(\\d+):ms"`   |

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

### TOML configuration

| Parameter          | Type   | Default | Description                                     |
|--------------------|--------|---------|-------------------------------------------------|
| `config_file_path` | string | none    | Path to TOML config file (absolute or relative) |

### File path resolution

All file paths in the plugin (configuration file, TLS certificates) follow the same resolution logic:

- **Absolute paths** (e.g., `/etc/kafka/config.toml`) are used as-is
- **Relative paths** (e.g., `config.toml`, `certs/ca.crt`) are resolved from `PLUGIN_DIR` environment variable

If a relative path is specified and `PLUGIN_DIR` is not set, the plugin will return an error.

#### Example TOML configuration

[kafka_config_example.toml](kafka_config_example.toml) - comprehensive configuration example with all formats and security options

## Data requirements

The plugin automatically creates the target measurement table on first write. Field mappings are required for JSON and Text formats to specify which fields to extract and their data types.

### Message encoding requirements

- **UTF-8 text only**: The plugin only processes UTF-8 encoded text messages. Binary messages (Protobuf, Avro) are not supported.
- **Non-empty payloads**: Empty or whitespace-only messages are automatically skipped.

## Software Requirements

- **InfluxDB 3 Core/Enterprise**: with the Processing Engine enabled
- **Python packages**:
  - `confluent-kafka` (High-performance Kafka client library based on librdkafka)
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
   influxdb3 install package confluent-kafka
   influxdb3 install package jsonpath-ng
   ```

## Trigger setup

### Scheduled ingestion with TOML configuration

Recommended for production use with complex mappings:

```bash
# 1. Set PLUGIN_DIR environment variable
export PLUGIN_DIR=~/.plugins

# 2. Copy and edit configuration file
cp kafka_config_example.toml $PLUGIN_DIR/my_kafka_config.toml
# Edit my_kafka_config.toml with your Kafka cluster and mapping settings

# 3. Create the trigger
influxdb3 create trigger \
  --database mydb \
  --plugin-filename gh:influxdata/kafka_subscriber/kafka_subscriber.py \
  --trigger-spec "every:10s" \
  --trigger-arguments config_file_path=my_kafka_config.toml \
  kafka_ingestion
```

### Scheduled ingestion with command-line arguments

For simple JSON message ingestion:

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename gh:influxdata/kafka_subscriber/kafka_subscriber.py \
  --trigger-spec "every:5s" \
  --trigger-arguments 'bootstrap_servers=kafka1:9092 kafka2:9092,topics=sensors.temperature sensors.humidity,group_id=influxdb3_consumer,format=json,table_name=sensor_data,fields=temp:float=temperature hum:int=humidity,tags=location sensor_id' \
  kafka_sensors
```

### Secure Kafka connection with SASL_SSL

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename gh:influxdata/kafka_subscriber/kafka_subscriber.py \
  --trigger-spec "every:10s" \
  --trigger-arguments 'bootstrap_servers=kafka1:9093 kafka2:9093,topics=secure.data,group_id=influxdb3_secure,format=json,table_name=secure_data,security_protocol=SASL_SSL,sasl_mechanism=SCRAM-SHA-512,sasl_username=myuser,sasl_password=mypass,ssl_ca_cert=certs/ca.crt,fields=value:float=value' \
  secure_kafka
```

## Message Formats

### JSON Format

The primary use case for structured data. Supports nested fields using JSONPath expressions.

#### TOML Configuration

```toml
[kafka]
bootstrap_servers = ["kafka:9092"]
topics = ["sensors.temperature"]
group_id = "influxdb3_json"
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
- Parse errors for individual elements are logged to `kafka_exceptions` table
- Statistics count 1 Kafka message = 1 unit (regardless of array size)

### Line Protocol Format

For messages already in InfluxDB line protocol format, use passthrough mode. No mapping configuration needed.

#### TOML Configuration

```toml
[kafka]
bootstrap_servers = ["kafka:9092"]
topics = ["influxdb.metrics"]
group_id = "influxdb3_lineprotocol"
format = "lineprotocol"
```

#### Example Message

```
sensor_data,location=warehouse_a,sensor_id=001 temperature=22.5,humidity=65i 1638360000000000000
```

### Text Format

Parse plain text messages using regular expressions:

#### TOML Configuration

```toml
[kafka]
bootstrap_servers = ["kafka:9092"]
topics = ["legacy.logs"]
group_id = "influxdb3_text"
format = "text"

[mapping.text]
table_name = "sensor_logs"
timestamp_field = "ts:(\\d+):ms"

[mapping.text.tags]
location = "location=([^,\\s]+)"

[mapping.text.fields]
temperature = ["temp:([\\d.]+)", "float"]
humidity = ["hum:(\\d+)", "int"]
```

#### Example Message

```
location=warehouse_a,temp:22.5,hum:65,ts:1638360000000
```

## Statistics and Monitoring

The plugin tracks comprehensive statistics and writes them to the `kafka_stats` table on every plugin invocation.

### kafka_stats Table

| Field                       | Type  | Description                               |
|-----------------------------|-------|-------------------------------------------|
| `topic` (tag)               | tag   | Kafka topic name                          |
| `partition` (tag)           | tag   | Partition number                          |
| `consumer_group` (tag)      | tag   | Consumer group ID                         |
| `bootstrap_servers` (tag)   | tag   | Kafka cluster address                     |
| `messages_received`         | int   | Total messages received                   |
| `messages_processed`        | int   | Successfully processed messages           |
| `messages_failed`           | int   | Failed messages                           |
| `last_offset`               | int   | Last processed offset for this partition  |
| `success_rate`              | float | Percentage of successful messages         |

### Querying Statistics

```bash
# Get latest statistics
influxdb3 query --database mydb \
  "SELECT * FROM kafka_stats ORDER BY time DESC LIMIT 10"

# Success rate by topic
influxdb3 query --database mydb \
  "SELECT topic, partition, success_rate, messages_processed, messages_failed
   FROM kafka_stats
   WHERE time > now() - INTERVAL '1 hour'
   ORDER BY time DESC"

# Check consumer lag indicators
influxdb3 query --database mydb \
  "SELECT topic, partition, last_offset
   FROM kafka_stats
   WHERE consumer_group = 'influxdb3_consumer'
   ORDER BY time DESC LIMIT 10"
```

## Error Handling

Parse errors and message processing failures are logged to the `kafka_exceptions` table:

### kafka_exceptions Table

| Field               | Type   | Description                              |
|---------------------|--------|------------------------------------------|
| `topic` (tag)       | tag    | Kafka topic where error occurred         |
| `partition` (tag)   | tag    | Partition number                         |
| `error_type` (tag)  | tag    | Type of error (e.g., JSONDecodeError)    |
| `offset`            | int    | Message offset                           |
| `error_message`     | string | Detailed error message                   |
| `raw_message`       | string | Original message (truncated to 1KB)      |

### Checking for Errors

```bash
influxdb3 query --database mydb \
  "SELECT * FROM kafka_exceptions ORDER BY time DESC LIMIT 10"
```

## Troubleshooting

### Check Plugin Logs

```bash
influxdb3 query --database _internal \
  "SELECT * FROM system.processing_engine_logs
   WHERE trigger_name = 'kafka_ingestion'
   ORDER BY time DESC LIMIT 20"
```

### Common Issues

#### "confluent-kafka library not installed" or "No module named 'confluent_kafka'"

```bash
influxdb3 install package confluent-kafka
```

If you encounter `librdkafka` related errors:
- Ubuntu/Debian: `sudo apt-get install librdkafka-dev`
- macOS: `brew install librdkafka`

#### "Configuration file not found"

- For relative paths, ensure `PLUGIN_DIR` environment variable is set
- For absolute paths, verify the file exists at the specified location

```bash
# For relative paths
export PLUGIN_DIR=~/.plugins
ls $PLUGIN_DIR/my_kafka_config.toml

# Or use absolute path
ls /etc/kafka/my_kafka_config.toml
```

#### "Failed to connect to Kafka cluster"

- Verify bootstrap_servers addresses and ports
- Check network connectivity to Kafka brokers
- For SSL connections, verify certificate paths
- For SASL authentication, verify credentials

#### "SASL mechanism required when security_protocol includes SASL"

When using `SASL_PLAINTEXT` or `SASL_SSL`, you must provide:
- `sasl_mechanism`
- `sasl_username`
- `sasl_password`

#### "No fields were mapped from JSON data"

- Verify JSONPath expressions in field mappings (use `$.` prefix)
- Check that JSON structure matches your paths
- Review `kafka_exceptions` table for detailed errors

#### Messages not being processed

- Check trigger status: `influxdb3 show summary --database mydb`
- Verify Kafka connection in plugin logs
- Check consumer group lag using Kafka tools
- Increase trigger frequency (e.g., from `every:10s` to `every:5s`)
- If many messages are queued, increase `max_poll_records` or set to `0` (unlimited)

#### Offset commit issues with `on_success` policy

If using `offset_commit_policy=on_success` and seeing repeated message processing:
- Check `kafka_exceptions` table for processing errors
- Fix the root cause of failures
- Consider using `offset_commit_policy=always` if some data loss is acceptable

## Architecture

### How It Works

1. **Scheduled Trigger**: Plugin runs on schedule (e.g., `every:10s`)
2. **Configuration Caching**: Plugin configuration is parsed once and cached between executions
3. **Consumer Poll**: Kafka consumer polls for available messages (drains all available messages)
4. **Offset Commit**: Based on `offset_commit_policy`:
   - `always`: Commits immediately after poll
   - `on_success`: Commits only after successful processing of all messages
5. **Parse & Write**: Messages parsed according to format and written to InfluxDB
6. **Error Tracking**: Parse errors logged to `kafka_exceptions` table
7. **Statistics**: Written to `kafka_stats` table every 10 plugin calls

### Performance Optimization

The plugin includes several optimizations for high-throughput scenarios:

- **Configuration Caching**: Plugin configuration is parsed once and reused across all trigger executions
- **Pre-compiled Patterns**: JSONPath expressions and regex patterns are compiled once during parser initialization
- **Batch Polling**: Consumer drains available messages in a single trigger execution (limited by `max_poll_records`, default 500)
- **Manual Offset Commit**: Provides control over exactly-once vs at-least-once semantics

### Consumer Group Behavior

- Each trigger execution creates a new consumer connection
- Consumer group ID ensures partitions are assigned consistently
- Offsets are committed to Kafka for durability
- Multiple plugin instances with the same `group_id` will share partitions (load balancing)
