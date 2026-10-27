"""
{
    "plugin_type": [
        "onwrite",
        "scheduled",
        "http"
    ],
    "onwrite_args_config": [
        {
            "name": "broker_url",
            "example": "mqtt://broker.example.com:1883",
            "description": "Broker address and transport in one value: mqtt://, mqtts://, ws://, wss://, unix:///path, or a bare host[:port]. The port defaults by scheme (1883, 8883, 80, 443) and TLS is chosen by the scheme.",
            "required": true
        },
        {
            "name": "client_id",
            "example": "plant-publisher",
            "description": "MQTT client identifier, defaulting to influxdb3-pub-<random> and kept across reconnects. The broker allows one session per identifier and evicts the previous holder, so set it only when ACLs require a known one, per trigger rather than in a shared file, and only on a trigger pinned to one node.",
            "required": false
        },
        {
            "name": "username",
            "example": "publisher",
            "description": "User name for the broker.",
            "required": false
        },
        {
            "name": "password",
            "example": "<your-password>",
            "description": "Password for the broker. Prefer the TOML file or the environment: trigger arguments are readable from the system tables.",
            "required": false
        },
        {
            "name": "mqtt_version",
            "example": "5",
            "description": "Protocol version: '3.1.1' (default), '5' or '3.1'. Version 5 adds reason codes on acknowledgements, message expiry and broker-declared limits; without it a refused message cannot be told from a delivered one.",
            "required": false
        },
        {
            "name": "keep_alive",
            "example": "60",
            "description": "Seconds declared in CONNECT, 300 by default. Large on purpose because nothing pings between invocations, but it also caps a TLS handshake and a WebSocket upgrade, so lower it on an unreliable TLS path.",
            "required": false
        },
        {
            "name": "max_silence",
            "example": "150s",
            "description": "How long the connection may be quiet before an invocation replaces it rather than writing into it blind. Defaults to half of keep_alive, and follows an MQTT 5 broker's Server Keep Alive when one is imposed.",
            "required": false
        },
        {
            "name": "connect_timeout",
            "example": "5s",
            "description": "Bounds the TCP connect and the wait for CONNACK. It does not bound name resolution or a TLS handshake. Defaults to 2s.",
            "required": false
        },
        {
            "name": "pump_budget",
            "example": "200ms",
            "description": "How long one write invocation may spend running the network loop collecting acknowledgements. Raise it together with max_inflight when a QoS 1 trigger buffers against a healthy broker. Defaults to 50ms.",
            "required": false
        },
        {
            "name": "reconnect_min_backoff",
            "example": "2s",
            "description": "Shortest gap between connection attempts, doubling on each failure. Defaults to 1s.",
            "required": false
        },
        {
            "name": "reconnect_max_backoff",
            "example": "60s",
            "description": "Ceiling for the backoff doubling, reset once a CONNACK succeeds. Defaults to 120s.",
            "required": false
        },
        {
            "name": "tls_ca_cert",
            "example": "certs/ca.pem",
            "description": "CA certificate for mqtts:// or wss://. A relative path resolves under PLUGIN_DIR; omit to use the system trust store.",
            "required": false
        },
        {
            "name": "tls_cert",
            "example": "certs/client.pem",
            "description": "Client certificate for mutual TLS.",
            "required": false
        },
        {
            "name": "tls_key",
            "example": "certs/client.key",
            "description": "Client private key for mutual TLS. Must be unencrypted; a passphrase-protected key is refused at configuration time.",
            "required": false
        },
        {
            "name": "tls_insecure",
            "example": "true",
            "description": "Skip server certificate verification. Defaults to false and should stay there outside a laboratory.",
            "required": false
        },
        {
            "name": "ws_path",
            "example": "/mqtt",
            "description": "Path for a WebSocket connection when it differs from the one in broker_url. Ignored outside ws:// and wss://.",
            "required": false
        },
        {
            "name": "ws_headers",
            "example": "Authorization:'Bearer abc' X-Tenant:'plant-1'",
            "description": "Extra headers on the WebSocket upgrade, for brokers that authenticate there: space-separated name:'value' pairs, or a table of name to value in a TOML file or a request body. Ignored outside ws:// and wss://.",
            "required": false
        },
        {
            "name": "status_topic",
            "example": "plant/publisher/status",
            "description": "Retained availability topic. The online payload is published on every connect and the offline one is registered as the MQTT will. Meaningful only when invocations are more frequent than keep_alive; the HTTP trigger never touches it.",
            "required": false
        },
        {
            "name": "status_online_payload",
            "example": "up",
            "description": "Payload published retained on status_topic after every successful connect. Defaults to 'online'.",
            "required": false
        },
        {
            "name": "status_offline_payload",
            "example": "down",
            "description": "Payload registered as the MQTT will, so the broker emits it when the connection dies unexpectedly. Defaults to 'offline'.",
            "required": false
        },
        {
            "name": "source_measurements",
            "example": "press_line weld_line",
            "description": "Tables to publish, space-separated or as a list. On a write trigger, batches for any other table are ignored, so the trigger specification may safely be all_tables. On a schedule each is read in turn; on HTTP the first is the default table a request may override.",
            "required": true
        },
        {
            "name": "filters",
            "example": "temp:'>=185' state:'!=error'",
            "description": "Comparisons applied before any message is built, with =, !=, >, <, >= and <=. As a string, space-separated column:'operator value' pairs with each value quoted; in a TOML file or a request body, also a list of [column, operator, value] triples. A row lacking the column fails a positive test and passes '!='.",
            "required": false
        },
        {
            "name": "allow_values",
            "example": "machine:'press7 press8' line:'l1'",
            "description": "Membership tests: as a string, space-separated column:'values' pairs with each list quoted; in a TOML file or a request body, also a table of column to values. Only rows whose value is listed are published, and a column not named here is unconstrained. Values are matched as strings, so for a numeric or boolean column use filters instead.",
            "required": false
        },
        {
            "name": "deny_values",
            "example": "machine:'press9' line:'l9'",
            "description": "Same forms as allow_values. Rows whose value is listed are skipped, a deny match wins over an allow match, and a row lacking the column is published.",
            "required": false
        },
        {
            "name": "topic",
            "example": "plant/{machine}/{_table}",
            "description": "Topic to publish to. A plain string is valid; {column} is replaced by that column's value in the row, {_table} by the table name and {_field} by the field name under layout=field. A substituted value may not contain /, + or #.",
            "required": true
        },
        {
            "name": "topic_missing_value",
            "example": "unknown",
            "description": "Substitute for a placeholder whose column is empty on a row. Unset, such a row is skipped and counted.",
            "required": false
        },
        {
            "name": "payload_format",
            "example": "lineprotocol",
            "description": "Encoder: 'json' (default), 'lineprotocol' or 'template'. Ignored under layout=field, whose payload is the bare value.",
            "required": false
        },
        {
            "name": "payload_fields",
            "example": "temp pressure",
            "description": "Columns that reach the payload, space-separated or as a list; empty means every field and tag of the row. It narrows the payload only, not what a topic placeholder may refer to.",
            "required": false
        },
        {
            "name": "payload_template",
            "example": "{'setpoint': 180, 'reason': 'temp {temp}'}",
            "description": "Required when payload_format=template. Uses the same placeholders as topic; a brace group that is not a placeholder stays literal, and {{ }} produce literal braces.",
            "required": false
        },
        {
            "name": "layout",
            "example": "field",
            "description": "'row' (default) publishes one message per row, 'batch' one message per topic per invocation, 'field' one message per field carrying the bare value.",
            "required": false
        },
        {
            "name": "time_format",
            "example": "ms",
            "description": "How a timestamp is rendered in a json payload and in a {time} placeholder, which payload_template accepts and topic refuses: 'rfc3339' (default), 'ms' or 'ns'. 'ns' exceeds the largest integer JavaScript represents exactly. Ignored by lineprotocol, whose grammar requires integer nanoseconds.",
            "required": false
        },
        {
            "name": "qos",
            "example": "1",
            "description": "Delivery guarantee asked of the broker: 0 (default), 1 or 2. QoS 2 is not recommended, being not exactly-once under a clean session and hiding a refusal at PUBREC.",
            "required": false
        },
        {
            "name": "retain",
            "example": "true",
            "description": "Set the MQTT retain flag, so a subscriber arriving later sees the current value at once. Defaults to false.",
            "required": false
        },
        {
            "name": "max_messages",
            "example": "2000",
            "description": "Source rows one query reads at a time, which is what LIMIT receives: the page size of a scheduled tick and of a backfill call. The number of messages follows from layout. It does not apply to a write trigger, whose batch is encoded whole and bounded by max_queue_bytes instead. Defaults to 10000.",
            "required": false
        },
        {
            "name": "max_payload_bytes",
            "example": "262144",
            "description": "Largest message built, counting topic and properties. A batch is split to stay under it; a single row that still does not fit is dropped, counted and logged. Defaults to 122880, eight bytes short of AWS IoT Core's cap.",
            "required": false
        },
        {
            "name": "max_delivery_attempts",
            "example": "5",
            "description": "Hand-overs before a message is treated as undeliverable, then dropped, counted and logged. An attempt is charged only to the oldest unconfirmed message. Defaults to 3.",
            "required": false
        },
        {
            "name": "max_queue_bytes",
            "example": "16777216",
            "description": "Bound on the in-memory buffer, counted as payload plus topic plus a per-message overhead so that it tracks real memory. Per trigger, and every trigger shares one engine process. Defaults to 67108864.",
            "required": false
        },
        {
            "name": "drop_policy",
            "example": "drop_newest",
            "description": "What happens at max_queue_bytes: 'drop_oldest' (default) keeps the freshest data, 'drop_newest' preserves the earliest events.",
            "required": false
        },
        {
            "name": "max_message_age",
            "example": "5min",
            "description": "Discard a message whose row is older than this, measured from the row's timestamp and applied on both the direct and the buffered path. On a schedule it also clamps the watermark and must exceed settle_delay. A backfill is exempt. Defaults to 0, which never discards.",
            "required": false
        },
        {
            "name": "max_unconfirmed_bytes",
            "example": "4194304",
            "description": "Back-pressure threshold, as the total size of messages handed over and not yet confirmed. Defaults to 16777216.",
            "required": false
        },
        {
            "name": "max_inflight",
            "example": "100",
            "description": "How many QoS >= 1 messages may await acknowledgement at once. Nothing past it is handed over, because paho would hold it outside every policy here. Defaults to 20.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "mqtt_publisher_config.toml",
            "description": "TOML file under PLUGIN_DIR holding any of these settings. Read after the environment and the trigger arguments and winning over both; the place for values containing commas and for credentials.",
            "required": false
        },
        {
            "name": "stats_interval",
            "example": "5min",
            "description": "How often counters are written to mqtt_publisher_stats and summarised in the log. Defaults to 1min; 0 writes no measurement.",
            "required": false
        },
        {
            "name": "instance_name",
            "example": "press-line",
            "description": "Tag on mqtt_publisher_stats telling several triggers on one broker apart. Defaults to a stable hash of source_measurements and topic.",
            "required": false
        },
        {
            "name": "enable_full_logging",
            "example": "true",
            "description": "When false (the default), an exception's own message is withheld so that broker URLs and paths cannot reach the log. Reason codes, errno and the setting an error names are logged either way.",
            "required": false
        }
    ],
    "scheduled_args_config": [
        {
            "name": "broker_url",
            "example": "mqtt://broker.example.com:1883",
            "description": "Broker address and transport in one value: mqtt://, mqtts://, ws://, wss://, unix:///path, or a bare host[:port]. The port defaults by scheme (1883, 8883, 80, 443) and TLS is chosen by the scheme.",
            "required": true
        },
        {
            "name": "client_id",
            "example": "plant-publisher",
            "description": "MQTT client identifier, defaulting to influxdb3-pub-<random> and kept across reconnects. The broker allows one session per identifier and evicts the previous holder, so set it only when ACLs require a known one, per trigger rather than in a shared file, and only on a trigger pinned to one node.",
            "required": false
        },
        {
            "name": "username",
            "example": "publisher",
            "description": "User name for the broker.",
            "required": false
        },
        {
            "name": "password",
            "example": "<your-password>",
            "description": "Password for the broker. Prefer the TOML file or the environment: trigger arguments are readable from the system tables.",
            "required": false
        },
        {
            "name": "mqtt_version",
            "example": "5",
            "description": "Protocol version: '3.1.1' (default), '5' or '3.1'. Version 5 adds reason codes on acknowledgements, message expiry and broker-declared limits; without it a refused message cannot be told from a delivered one.",
            "required": false
        },
        {
            "name": "keep_alive",
            "example": "60",
            "description": "Seconds declared in CONNECT, 300 by default. Large on purpose because nothing pings between invocations, but it also caps a TLS handshake and a WebSocket upgrade, so lower it on an unreliable TLS path.",
            "required": false
        },
        {
            "name": "max_silence",
            "example": "150s",
            "description": "How long the connection may be quiet before an invocation replaces it rather than writing into it blind. Defaults to half of keep_alive, and follows an MQTT 5 broker's Server Keep Alive when one is imposed.",
            "required": false
        },
        {
            "name": "connect_timeout",
            "example": "5s",
            "description": "Bounds the TCP connect and the wait for CONNACK. It does not bound name resolution or a TLS handshake. Defaults to 2s.",
            "required": false
        },
        {
            "name": "reconnect_min_backoff",
            "example": "2s",
            "description": "Shortest gap between connection attempts, doubling on each failure. Defaults to 1s.",
            "required": false
        },
        {
            "name": "reconnect_max_backoff",
            "example": "60s",
            "description": "Ceiling for the backoff doubling, reset once a CONNACK succeeds. Defaults to 120s.",
            "required": false
        },
        {
            "name": "tls_ca_cert",
            "example": "certs/ca.pem",
            "description": "CA certificate for mqtts:// or wss://. A relative path resolves under PLUGIN_DIR; omit to use the system trust store.",
            "required": false
        },
        {
            "name": "tls_cert",
            "example": "certs/client.pem",
            "description": "Client certificate for mutual TLS.",
            "required": false
        },
        {
            "name": "tls_key",
            "example": "certs/client.key",
            "description": "Client private key for mutual TLS. Must be unencrypted; a passphrase-protected key is refused at configuration time.",
            "required": false
        },
        {
            "name": "tls_insecure",
            "example": "true",
            "description": "Skip server certificate verification. Defaults to false and should stay there outside a laboratory.",
            "required": false
        },
        {
            "name": "ws_path",
            "example": "/mqtt",
            "description": "Path for a WebSocket connection when it differs from the one in broker_url. Ignored outside ws:// and wss://.",
            "required": false
        },
        {
            "name": "ws_headers",
            "example": "Authorization:'Bearer abc' X-Tenant:'plant-1'",
            "description": "Extra headers on the WebSocket upgrade, for brokers that authenticate there: space-separated name:'value' pairs, or a table of name to value in a TOML file or a request body. Ignored outside ws:// and wss://.",
            "required": false
        },
        {
            "name": "status_topic",
            "example": "plant/publisher/status",
            "description": "Retained availability topic. The online payload is published on every connect and the offline one is registered as the MQTT will. Meaningful only when invocations are more frequent than keep_alive; the HTTP trigger never touches it.",
            "required": false
        },
        {
            "name": "status_online_payload",
            "example": "up",
            "description": "Payload published retained on status_topic after every successful connect. Defaults to 'online'.",
            "required": false
        },
        {
            "name": "status_offline_payload",
            "example": "down",
            "description": "Payload registered as the MQTT will, so the broker emits it when the connection dies unexpectedly. Defaults to 'offline'.",
            "required": false
        },
        {
            "name": "source_measurements",
            "example": "press_line weld_line",
            "description": "Tables to publish, space-separated or as a list. On a write trigger, batches for any other table are ignored, so the trigger specification may safely be all_tables. On a schedule each is read in turn; on HTTP the first is the default table a request may override.",
            "required": true
        },
        {
            "name": "settle_delay",
            "example": "30s",
            "description": "Hold back the upper bound of the range by this much, so rows still below the server's write-to-visibility lag (up to --wal-flush-interval, default 1s) are not skipped and rows a batching source writes late relative to their own timestamp are still above the watermark when they appear. Keep it at or above the server's --wal-flush-interval; a table another plugin derives needs that plugin's whole interval plus its offset and run time. Defaults to 2s.",
            "required": false
        },
        {
            "name": "tick_budget",
            "example": "20s",
            "description": "How long one tick may keep working through pages before leaving the rest to the next one. One page per tick cannot catch up with a source writing more than max_messages per interval. Defaults to 5s.",
            "required": false
        },
        {
            "name": "max_group_rows",
            "example": "500000",
            "description": "Most rows read for a single timestamp when a whole page carries one, since a page cannot stop inside a timestamp group without losing the rest of it. Beyond it the surplus is dropped, counted and logged, and the cursor advances anyway. Defaults to 100000.",
            "required": false
        },
        {
            "name": "filters",
            "example": "temp:'>=185' state:'!=error'",
            "description": "Comparisons applied before any message is built, with =, !=, >, <, >= and <=. As a string, space-separated column:'operator value' pairs with each value quoted; in a TOML file or a request body, also a list of [column, operator, value] triples. A row lacking the column fails a positive test and passes '!='.",
            "required": false
        },
        {
            "name": "allow_values",
            "example": "machine:'press7 press8' line:'l1'",
            "description": "Membership tests: as a string, space-separated column:'values' pairs with each list quoted; in a TOML file or a request body, also a table of column to values. Only rows whose value is listed are published, and a column not named here is unconstrained. Values are matched as strings, so for a numeric or boolean column use filters instead.",
            "required": false
        },
        {
            "name": "deny_values",
            "example": "machine:'press9' line:'l9'",
            "description": "Same forms as allow_values. Rows whose value is listed are skipped, a deny match wins over an allow match, and a row lacking the column is published.",
            "required": false
        },
        {
            "name": "topic",
            "example": "plant/{machine}/{_table}",
            "description": "Topic to publish to. A plain string is valid; {column} is replaced by that column's value in the row, {_table} by the table name and {_field} by the field name under layout=field. A substituted value may not contain /, + or #.",
            "required": true
        },
        {
            "name": "topic_missing_value",
            "example": "unknown",
            "description": "Substitute for a placeholder whose column is empty on a row. Unset, such a row is skipped and counted.",
            "required": false
        },
        {
            "name": "payload_format",
            "example": "lineprotocol",
            "description": "Encoder: 'json' (default), 'lineprotocol' or 'template'. Ignored under layout=field, whose payload is the bare value.",
            "required": false
        },
        {
            "name": "payload_fields",
            "example": "temp pressure",
            "description": "Columns that reach the payload, space-separated or as a list; empty means every field and tag of the row. It narrows the payload only, not what a topic placeholder may refer to.",
            "required": false
        },
        {
            "name": "payload_template",
            "example": "{'setpoint': 180, 'reason': 'temp {temp}'}",
            "description": "Required when payload_format=template. Uses the same placeholders as topic; a brace group that is not a placeholder stays literal, and {{ }} produce literal braces.",
            "required": false
        },
        {
            "name": "layout",
            "example": "field",
            "description": "'row' (default) publishes one message per row, 'batch' one message per topic per invocation, 'field' one message per field carrying the bare value.",
            "required": false
        },
        {
            "name": "time_format",
            "example": "ms",
            "description": "How a timestamp is rendered in a json payload and in a {time} placeholder, which payload_template accepts and topic refuses: 'rfc3339' (default), 'ms' or 'ns'. 'ns' exceeds the largest integer JavaScript represents exactly. Ignored by lineprotocol, whose grammar requires integer nanoseconds.",
            "required": false
        },
        {
            "name": "qos",
            "example": "1",
            "description": "Delivery guarantee asked of the broker: 0 (default), 1 or 2. QoS 2 is not recommended, being not exactly-once under a clean session and hiding a refusal at PUBREC.",
            "required": false
        },
        {
            "name": "retain",
            "example": "true",
            "description": "Set the MQTT retain flag, so a subscriber arriving later sees the current value at once. Defaults to false.",
            "required": false
        },
        {
            "name": "max_messages",
            "example": "2000",
            "description": "Source rows one query reads at a time, which is what LIMIT receives: the page size of a scheduled tick and of a backfill call. The number of messages follows from layout. It does not apply to a write trigger, whose batch is encoded whole and bounded by max_queue_bytes instead. Defaults to 10000.",
            "required": false
        },
        {
            "name": "max_payload_bytes",
            "example": "262144",
            "description": "Largest message built, counting topic and properties. A batch is split to stay under it; a single row that still does not fit is dropped, counted and logged. Defaults to 122880, eight bytes short of AWS IoT Core's cap.",
            "required": false
        },
        {
            "name": "max_delivery_attempts",
            "example": "5",
            "description": "Hand-overs before a message is treated as undeliverable, then dropped, counted and logged. An attempt is charged only to the oldest unconfirmed message. Defaults to 3.",
            "required": false
        },
        {
            "name": "max_queue_bytes",
            "example": "16777216",
            "description": "Bound on the in-memory buffer, counted as payload plus topic plus a per-message overhead so that it tracks real memory. Per trigger, and every trigger shares one engine process. Defaults to 67108864.",
            "required": false
        },
        {
            "name": "drop_policy",
            "example": "drop_newest",
            "description": "What happens at max_queue_bytes: 'drop_oldest' (default) keeps the freshest data, 'drop_newest' preserves the earliest events.",
            "required": false
        },
        {
            "name": "max_message_age",
            "example": "5min",
            "description": "Discard a message whose row is older than this, measured from the row's timestamp and applied on both the direct and the buffered path. On a schedule it also clamps the watermark and must exceed settle_delay. A backfill is exempt. Defaults to 0, which never discards.",
            "required": false
        },
        {
            "name": "max_unconfirmed_bytes",
            "example": "4194304",
            "description": "Back-pressure threshold, as the total size of messages handed over and not yet confirmed. Defaults to 16777216.",
            "required": false
        },
        {
            "name": "max_inflight",
            "example": "100",
            "description": "How many QoS >= 1 messages may await acknowledgement at once. Nothing past it is handed over, because paho would hold it outside every policy here. Defaults to 20.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "mqtt_publisher_config.toml",
            "description": "TOML file under PLUGIN_DIR holding any of these settings. Read after the environment and the trigger arguments and winning over both; the place for values containing commas and for credentials.",
            "required": false
        },
        {
            "name": "stats_interval",
            "example": "5min",
            "description": "How often counters are written to mqtt_publisher_stats and summarised in the log. Defaults to 1min; 0 writes no measurement.",
            "required": false
        },
        {
            "name": "instance_name",
            "example": "press-line",
            "description": "Tag on mqtt_publisher_stats telling several triggers on one broker apart. Defaults to a stable hash of source_measurements and topic.",
            "required": false
        },
        {
            "name": "enable_full_logging",
            "example": "true",
            "description": "When false (the default), an exception's own message is withheld so that broker URLs and paths cannot reach the log. Reason codes, errno and the setting an error names are logged either way.",
            "required": false
        }
    ],
    "http_args_config": [
        {
            "name": "broker_url",
            "example": "mqtt://broker.example.com:1883",
            "description": "Broker address and transport in one value: mqtt://, mqtts://, ws://, wss://, unix:///path, or a bare host[:port]. The port defaults by scheme (1883, 8883, 80, 443) and TLS is chosen by the scheme.",
            "required": true
        },
        {
            "name": "client_id",
            "example": "plant-publisher",
            "description": "MQTT client identifier, defaulting to influxdb3-pub-<random> and kept across reconnects. The broker allows one session per identifier and evicts the previous holder, so set it only when ACLs require a known one, per trigger rather than in a shared file, and only on a trigger pinned to one node.",
            "required": false
        },
        {
            "name": "username",
            "example": "publisher",
            "description": "User name for the broker.",
            "required": false
        },
        {
            "name": "password",
            "example": "<your-password>",
            "description": "Password for the broker. Prefer the TOML file or the environment: trigger arguments are readable from the system tables.",
            "required": false
        },
        {
            "name": "mqtt_version",
            "example": "5",
            "description": "Protocol version: '3.1.1' (default), '5' or '3.1'. Version 5 adds reason codes on acknowledgements, message expiry and broker-declared limits; without it a refused message cannot be told from a delivered one.",
            "required": false
        },
        {
            "name": "keep_alive",
            "example": "60",
            "description": "Seconds declared in CONNECT, 300 by default. Large on purpose because nothing pings between invocations, but it also caps a TLS handshake and a WebSocket upgrade, so lower it on an unreliable TLS path.",
            "required": false
        },
        {
            "name": "max_silence",
            "example": "150s",
            "description": "How long the connection may be quiet before an invocation replaces it rather than writing into it blind. Defaults to half of keep_alive, and follows an MQTT 5 broker's Server Keep Alive when one is imposed.",
            "required": false
        },
        {
            "name": "connect_timeout",
            "example": "5s",
            "description": "Bounds the TCP connect and the wait for CONNACK. It does not bound name resolution or a TLS handshake. Defaults to 2s.",
            "required": false
        },
        {
            "name": "reconnect_min_backoff",
            "example": "2s",
            "description": "Shortest gap between connection attempts, doubling on each failure. Defaults to 1s.",
            "required": false
        },
        {
            "name": "reconnect_max_backoff",
            "example": "60s",
            "description": "Ceiling for the backoff doubling, reset once a CONNACK succeeds. Defaults to 120s.",
            "required": false
        },
        {
            "name": "tls_ca_cert",
            "example": "certs/ca.pem",
            "description": "CA certificate for mqtts:// or wss://. A relative path resolves under PLUGIN_DIR; omit to use the system trust store.",
            "required": false
        },
        {
            "name": "tls_cert",
            "example": "certs/client.pem",
            "description": "Client certificate for mutual TLS.",
            "required": false
        },
        {
            "name": "tls_key",
            "example": "certs/client.key",
            "description": "Client private key for mutual TLS. Must be unencrypted; a passphrase-protected key is refused at configuration time.",
            "required": false
        },
        {
            "name": "tls_insecure",
            "example": "true",
            "description": "Skip server certificate verification. Defaults to false and should stay there outside a laboratory.",
            "required": false
        },
        {
            "name": "ws_path",
            "example": "/mqtt",
            "description": "Path for a WebSocket connection when it differs from the one in broker_url. Ignored outside ws:// and wss://.",
            "required": false
        },
        {
            "name": "ws_headers",
            "example": "Authorization:'Bearer abc' X-Tenant:'plant-1'",
            "description": "Extra headers on the WebSocket upgrade, for brokers that authenticate there: space-separated name:'value' pairs, or a table of name to value in a TOML file or a request body. Ignored outside ws:// and wss://.",
            "required": false
        },
        {
            "name": "source_measurements",
            "example": "press_line weld_line",
            "description": "Tables to publish, space-separated or as a list. On a write trigger, batches for any other table are ignored, so the trigger specification may safely be all_tables. On a schedule each is read in turn; on HTTP the first is the default table a request may override.",
            "required": true
        },
        {
            "name": "max_group_rows",
            "example": "500000",
            "description": "Most rows read for a single timestamp when a whole page carries one, since a page cannot stop inside a timestamp group without losing the rest of it. Beyond it the surplus is dropped, counted and logged, and the cursor advances anyway. Defaults to 100000.",
            "required": false
        },
        {
            "name": "filters",
            "example": "temp:'>=185' state:'!=error'",
            "description": "Comparisons applied before any message is built, with =, !=, >, <, >= and <=. As a string, space-separated column:'operator value' pairs with each value quoted; in a TOML file or a request body, also a list of [column, operator, value] triples. A row lacking the column fails a positive test and passes '!='.",
            "required": false
        },
        {
            "name": "allow_values",
            "example": "machine:'press7 press8' line:'l1'",
            "description": "Membership tests: as a string, space-separated column:'values' pairs with each list quoted; in a TOML file or a request body, also a table of column to values. Only rows whose value is listed are published, and a column not named here is unconstrained. Values are matched as strings, so for a numeric or boolean column use filters instead.",
            "required": false
        },
        {
            "name": "deny_values",
            "example": "machine:'press9' line:'l9'",
            "description": "Same forms as allow_values. Rows whose value is listed are skipped, a deny match wins over an allow match, and a row lacking the column is published.",
            "required": false
        },
        {
            "name": "topic",
            "example": "plant/{machine}/{_table}",
            "description": "Topic to publish to. A plain string is valid; {column} is replaced by that column's value in the row, {_table} by the table name and {_field} by the field name under layout=field. A substituted value may not contain /, + or #.",
            "required": true
        },
        {
            "name": "topic_missing_value",
            "example": "unknown",
            "description": "Substitute for a placeholder whose column is empty on a row. Unset, such a row is skipped and counted.",
            "required": false
        },
        {
            "name": "payload_format",
            "example": "lineprotocol",
            "description": "Encoder: 'json' (default), 'lineprotocol' or 'template'. Ignored under layout=field, whose payload is the bare value.",
            "required": false
        },
        {
            "name": "payload_fields",
            "example": "temp pressure",
            "description": "Columns that reach the payload, space-separated or as a list; empty means every field and tag of the row. It narrows the payload only, not what a topic placeholder may refer to.",
            "required": false
        },
        {
            "name": "payload_template",
            "example": "{'setpoint': 180, 'reason': 'temp {temp}'}",
            "description": "Required when payload_format=template. Uses the same placeholders as topic; a brace group that is not a placeholder stays literal, and {{ }} produce literal braces.",
            "required": false
        },
        {
            "name": "layout",
            "example": "field",
            "description": "'row' (default) publishes one message per row, 'batch' one message per topic per invocation, 'field' one message per field carrying the bare value.",
            "required": false
        },
        {
            "name": "time_format",
            "example": "ms",
            "description": "How a timestamp is rendered in a json payload and in a {time} placeholder, which payload_template accepts and topic refuses: 'rfc3339' (default), 'ms' or 'ns'. 'ns' exceeds the largest integer JavaScript represents exactly. Ignored by lineprotocol, whose grammar requires integer nanoseconds.",
            "required": false
        },
        {
            "name": "qos",
            "example": "1",
            "description": "Delivery guarantee asked of the broker: 0 (default), 1 or 2. QoS 2 is not recommended, being not exactly-once under a clean session and hiding a refusal at PUBREC.",
            "required": false
        },
        {
            "name": "retain",
            "example": "true",
            "description": "Set the MQTT retain flag, so a subscriber arriving later sees the current value at once. Defaults to false.",
            "required": false
        },
        {
            "name": "max_messages",
            "example": "2000",
            "description": "Source rows one query reads at a time, which is what LIMIT receives: the page size of a scheduled tick and of a backfill call. The number of messages follows from layout. It does not apply to a write trigger, whose batch is encoded whole and bounded by max_queue_bytes instead. Defaults to 10000.",
            "required": false
        },
        {
            "name": "max_payload_bytes",
            "example": "262144",
            "description": "Largest message built, counting topic and properties. A batch is split to stay under it; a single row that still does not fit is dropped, counted and logged. Defaults to 122880, eight bytes short of AWS IoT Core's cap.",
            "required": false
        },
        {
            "name": "max_delivery_attempts",
            "example": "5",
            "description": "Hand-overs before a message is treated as undeliverable, then dropped, counted and logged. An attempt is charged only to the oldest unconfirmed message. Defaults to 3.",
            "required": false
        },
        {
            "name": "max_queue_bytes",
            "example": "16777216",
            "description": "Bound on the in-memory buffer, counted as payload plus topic plus a per-message overhead so that it tracks real memory. Per trigger, and every trigger shares one engine process. Defaults to 67108864.",
            "required": false
        },
        {
            "name": "drop_policy",
            "example": "drop_newest",
            "description": "What happens at max_queue_bytes: 'drop_oldest' (default) keeps the freshest data, 'drop_newest' preserves the earliest events.",
            "required": false
        },
        {
            "name": "max_message_age",
            "example": "5min",
            "description": "Discard a message whose row is older than this, measured from the row's timestamp and applied on both the direct and the buffered path. On a schedule it also clamps the watermark and must exceed settle_delay. A backfill is exempt. Defaults to 0, which never discards.",
            "required": false
        },
        {
            "name": "max_unconfirmed_bytes",
            "example": "4194304",
            "description": "Back-pressure threshold, as the total size of messages handed over and not yet confirmed. Defaults to 16777216.",
            "required": false
        },
        {
            "name": "max_inflight",
            "example": "100",
            "description": "How many QoS >= 1 messages may await acknowledgement at once. Nothing past it is handed over, because paho would hold it outside every policy here. Defaults to 20.",
            "required": false
        },
        {
            "name": "backfill_start",
            "example": "2026-10-05T00:00:00Z",
            "description": "Inclusive lower bound of the range. Omitted, the range starts at the beginning of the table.",
            "required": false
        },
        {
            "name": "backfill_end",
            "example": "2026-10-05T06:00:00Z",
            "description": "Exclusive upper bound of the range, so adjacent calls chain without republishing the row on the seam. Omitted, the moment the request arrives.",
            "required": false
        },
        {
            "name": "backfill_order",
            "example": "newest_first",
            "description": "'oldest_first' (default) or 'newest_first'.",
            "required": false
        },
        {
            "name": "max_backfill_messages",
            "example": "50000",
            "description": "Most messages one call publishes, honored at timestamp-group granularity (a group is published whole even if it overshoots). Reached with rows still remaining, the call answers complete=false with a resume_from to continue from; reached with the range exhausted, it answers complete=true. Defaults to 1000000; 0 removes the cap. Not settable from a request.",
            "required": false
        },
        {
            "name": "publish_timeout",
            "example": "30s",
            "description": "How long the call waits at the end for its last page to be confirmed before answering. Defaults to 5s.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "mqtt_publisher_config.toml",
            "description": "TOML file under PLUGIN_DIR holding any of these settings. Read after the environment and the trigger arguments and winning over both; the place for values containing commas and for credentials.",
            "required": false
        },
        {
            "name": "stats_interval",
            "example": "5min",
            "description": "How often counters are written to mqtt_publisher_stats and summarised in the log. Defaults to 1min; 0 writes no measurement.",
            "required": false
        },
        {
            "name": "instance_name",
            "example": "press-line",
            "description": "Tag on mqtt_publisher_stats telling several triggers on one broker apart. Defaults to a stable hash of source_measurements and topic.",
            "required": false
        },
        {
            "name": "enable_full_logging",
            "example": "true",
            "description": "When false (the default), an exception's own message is withheld so that broker URLs and paths cannot reach the log. Reason codes, errno and the setting an error names are logged either way.",
            "required": false
        }
    ],
    "http_body_config": [
        {
            "name": "backfill_start",
            "example": "2026-10-05T00:00:00Z",
            "description": "Inclusive lower bound of the range. Omitted, the range starts at the beginning of the table. Overridable from a request; otherwise taken from the trigger arguments, the TOML file or the environment.",
            "required": false
        },
        {
            "name": "backfill_end",
            "example": "2026-10-05T06:00:00Z",
            "description": "Exclusive upper bound of the range, so adjacent calls chain without republishing the row on the seam. Omitted, the moment the request arrives. Overridable from a request; otherwise taken from the trigger arguments, the TOML file or the environment.",
            "required": false
        },
        {
            "name": "backfill_order",
            "example": "newest_first",
            "description": "'oldest_first' (default) or 'newest_first'. Overridable from a request; otherwise taken from the trigger arguments, the TOML file or the environment.",
            "required": false
        },
        {
            "name": "source_measurements",
            "example": "press_line weld_line",
            "description": "Tables to publish, space-separated or as a list. On a write trigger, batches for any other table are ignored, so the trigger specification may safely be all_tables. On a schedule each is read in turn; on HTTP the first is the default table a request may override. Overridable from a request; otherwise taken from the trigger arguments, the TOML file or the environment.",
            "required": false
        },
        {
            "name": "filters",
            "example": "temp:'>=185' state:'!=error'",
            "description": "Comparisons applied before any message is built, with =, !=, >, <, >= and <=. As a string, space-separated column:'operator value' pairs with each value quoted; in a TOML file or a request body, also a list of [column, operator, value] triples. A row lacking the column fails a positive test and passes '!='. Overridable from a request; otherwise taken from the trigger arguments, the TOML file or the environment.",
            "required": false
        },
        {
            "name": "allow_values",
            "example": "machine:'press7 press8' line:'l1'",
            "description": "Membership tests: as a string, space-separated column:'values' pairs with each list quoted; in a TOML file or a request body, also a table of column to values. Only rows whose value is listed are published, and a column not named here is unconstrained. Values are matched as strings, so for a numeric or boolean column use filters instead. Overridable from a request; otherwise taken from the trigger arguments, the TOML file or the environment.",
            "required": false
        },
        {
            "name": "deny_values",
            "example": "machine:'press9' line:'l9'",
            "description": "Same forms as allow_values. Rows whose value is listed are skipped, a deny match wins over an allow match, and a row lacking the column is published. Overridable from a request; otherwise taken from the trigger arguments, the TOML file or the environment.",
            "required": false
        },
        {
            "name": "topic",
            "example": "plant/{machine}/{_table}",
            "description": "Topic to publish to. A plain string is valid; {column} is replaced by that column's value in the row, {_table} by the table name and {_field} by the field name under layout=field. A substituted value may not contain /, + or #. Overridable from a request; otherwise taken from the trigger arguments, the TOML file or the environment.",
            "required": false
        },
        {
            "name": "topic_missing_value",
            "example": "unknown",
            "description": "Substitute for a placeholder whose column is empty on a row. Unset, such a row is skipped and counted. Overridable from a request; otherwise taken from the trigger arguments, the TOML file or the environment.",
            "required": false
        },
        {
            "name": "payload_format",
            "example": "lineprotocol",
            "description": "Encoder: 'json' (default), 'lineprotocol' or 'template'. Ignored under layout=field, whose payload is the bare value. Overridable from a request; otherwise taken from the trigger arguments, the TOML file or the environment.",
            "required": false
        },
        {
            "name": "payload_template",
            "example": "{'setpoint': 180, 'reason': 'temp {temp}'}",
            "description": "Required when payload_format=template. Uses the same placeholders as topic; a brace group that is not a placeholder stays literal, and {{ }} produce literal braces. Overridable from a request; otherwise taken from the trigger arguments, the TOML file or the environment.",
            "required": false
        },
        {
            "name": "payload_fields",
            "example": "temp pressure",
            "description": "Columns that reach the payload, space-separated or as a list; empty means every field and tag of the row. It narrows the payload only, not what a topic placeholder may refer to. Overridable from a request; otherwise taken from the trigger arguments, the TOML file or the environment.",
            "required": false
        },
        {
            "name": "layout",
            "example": "field",
            "description": "'row' (default) publishes one message per row, 'batch' one message per topic per invocation, 'field' one message per field carrying the bare value. Overridable from a request; otherwise taken from the trigger arguments, the TOML file or the environment.",
            "required": false
        },
        {
            "name": "time_format",
            "example": "ms",
            "description": "How a timestamp is rendered in a json payload and in a {time} placeholder, which payload_template accepts and topic refuses: 'rfc3339' (default), 'ms' or 'ns'. 'ns' exceeds the largest integer JavaScript represents exactly. Ignored by lineprotocol, whose grammar requires integer nanoseconds. Overridable from a request; otherwise taken from the trigger arguments, the TOML file or the environment.",
            "required": false
        }
    ]
}
"""
import hashlib
import json
import re
import socket
import ssl
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from influxdata_plugin_utils.config import load_config, resolve_path
from influxdata_plugin_utils.introspection import TAG_DATA_TYPE, get_schema
from influxdata_plugin_utils.parsing import (
    parse_bool,
    parse_delimited_list,
    parse_int,
    parse_timedelta,
)
from influxdata_plugin_utils.sources import (
    KeySpec,
    parse_env,
    parse_json_body,
    parse_query_parameters,
    parse_request_headers,
    parse_toml,
    parse_trigger_args,
)
from influxdata_plugin_utils.validation import Validator

STATE_CACHE_KEY = "mqtt_publisher_state"
STATS_MEASUREMENT = "mqtt_publisher_stats"

# Bytes charged to a buffered message on top of its payload and topic, to
# account for the Python object overhead of a queue entry.
MESSAGE_OVERHEAD_BYTES = 320

# Bytes an MQTT PUBLISH adds around payload and topic — fixed header, topic
# length prefix, packet id and properties — reserved when sizing against a
# broker's Maximum Packet Size so a message at the limit still fits the packet.
PACKET_OVERHEAD_RESERVE = 128

# MQTT 5 reason codes that blame the message rather than the link.
MESSAGE_ATTRIBUTABLE_CODES = frozenset({0x87, 0x90, 0x95, 0x99})

# How often a backfill reports progress, since it outlives its HTTP response.
PROGRESS_LOG_INTERVAL = 30.0

# How long a table's cached schema is trusted. After it, the next read re-reads
# the catalog, so a column added to a source table is picked up within the hour
# without recreating the trigger.
SCHEMA_TTL_SECONDS = 3600.0

# Pace for the log summary when stats_interval = 0 silences the measurement.
DEFAULT_STATS_INTERVAL = 60.0
QUOTA_EXCEEDED = 0x97

DEFAULT_PORTS = {"mqtt": 1883, "mqtts": 8883, "ws": 80, "wss": 443}
PROTOCOL_VERSIONS = {"3.1": mqtt.MQTTv31, "3.1.1": mqtt.MQTTv311, "5": mqtt.MQTTv5}

# A placeholder, or a doubled brace standing for a literal one. Everything else,
# braces included, is literal — so a JSON template needs no escaping.
TEMPLATE_TOKEN = re.compile(r"\{\{|\}\}|\{([A-Za-z_][A-Za-z0-9_]*)\}")
PLACEHOLDER_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# An RFC 3339 timestamp, with the fractional second kept as digits so that
# nanosecond precision survives a round trip through the plugin.
TIMESTAMP_PATTERN = re.compile(
    r"^(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})(?:\.(\d+))?([Zz]|[+-]\d{2}:?\d{2})?$"
)

TOPIC_FORBIDDEN = ("/", "+", "#", "\x00")
COMPARISON_OPERATORS = (">=", "<=", "!=", ">", "<", "=")
NEGATIVE_OPERATORS = frozenset({"!="})

ENV_PREFIX = "INFLUXDB3_MQTT_PUBLISHER_"
HEADER_PREFIX = "X-Influxdb3-Mqtt-Publisher-"


# --- setting parsers -------------------------------------------------------

def typed_literal(raw: str):
    """Read a filter literal as an int, a float or a string."""
    text = raw.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in ("'", '"'):
        return text[1:-1]
    for cast in (int, float):
        try:
            return cast(text)
        except ValueError:
            continue
    return text


def split_outside_quotes(raw: str) -> list[str]:
    """Split on whitespace that is not inside quotes.

    Quote awareness is what lets one separator serve both levels: a space
    separates the pairs, and a space inside quotes separates the values of a
    list. It also keeps a decimal literal intact, which a dot separator cannot.
    """
    parts: list[str] = []
    current: list[str] = []
    quote = None
    for char in raw:
        if quote is not None:
            current.append(char)
            if char == quote:
                quote = None
        elif char in "'\"":
            current.append(char)
            quote = char
        elif char.isspace():
            if current:
                parts.append("".join(current))
                current = []
        else:
            current.append(char)
    if quote is not None:
        raise ValueError(f"unbalanced {quote} in {raw!r}")
    if current:
        parts.append("".join(current))
    return parts


def parse_pairs(raw) -> list[tuple[str, str]]:
    """Split ``col:'expr' col2:'expr'`` into ``(column, expression)`` pairs."""
    pairs: list[tuple[str, str]] = []
    for part in split_outside_quotes(str(raw)):
        if ":" not in part:
            raise ValueError(
                f"{part!r} has no ':' between the column and the value; pairs are "
                f"separated by spaces, as in \"temp:'>=185' state:'!=error'\""
            )
        column, expression = part.split(":", 1)
        column = column.strip()
        expression = expression.strip()
        if len(expression) >= 2 and expression[0] == expression[-1] and expression[0] in ("'", '"'):
            expression = expression[1:-1]
        if not column:
            raise ValueError(f"{part!r} names no column")
        pairs.append((column, expression))
    return pairs


def require_operator(column: str, operator: str) -> str:
    """Refuse a comparison operator the filter grammar does not define."""
    if operator not in COMPARISON_OPERATORS:
        raise ValueError(
            f"filter on {column!r} uses {operator!r}; expected one of "
            f"{', '.join(COMPARISON_OPERATORS)}"
        )
    return operator


def parse_filters(raw) -> list[tuple[str, str, object]]:
    """Parse filters from either the compact string or a structured list.

    A trigger argument can only be a string, so the compact form exists for it:
    ``temp:'>=185' state:'!=error'``. A TOML file and a request body carry real
    data structures, where a list of ``[column, operator, value]`` triples says
    the same thing without a grammar to learn — and keeps the type the author
    wrote, so ``185`` stays a number and ``"185"`` is read as one too.
    """
    if not raw:
        return []

    if isinstance(raw, (list, tuple)):
        filters: list[tuple[str, str, object]] = []
        for entry in raw:
            if isinstance(entry, dict):
                missing = {"column", "operator", "value"} - set(entry)
                if missing:
                    raise ValueError(
                        f"filter {entry!r} is missing {', '.join(sorted(missing))}"
                    )
                column, operator, value = entry["column"], entry["operator"], entry["value"]
            elif isinstance(entry, (list, tuple)) and len(entry) == 3:
                column, operator, value = entry
            else:
                raise ValueError(
                    f"filter {entry!r} is not a [column, operator, value] triple"
                )
            column = str(column).strip()
            operator = require_operator(column, str(operator).strip())
            filters.append(
                (column, operator, typed_literal(value) if isinstance(value, str) else value)
            )
        return filters

    filters = []
    for column, expression in parse_pairs(raw):
        operator = next(
            (candidate for candidate in COMPARISON_OPERATORS if expression.startswith(candidate)),
            None,
        )
        if operator is None:
            raise ValueError(
                f"filter on {column!r} starts with no operator; expected one of "
                f"{', '.join(COMPARISON_OPERATORS)}"
            )
        filters.append((column, operator, typed_literal(expression[len(operator) :])))
    return filters


def parse_header_map(raw) -> dict[str, str]:
    """Parse headers from ``name:'value' other:'value'`` or from a table."""
    if not raw:
        return {}
    if isinstance(raw, dict):
        return {str(name): str(value) for name, value in raw.items()}
    return dict(parse_pairs(raw))


def parse_value_lists(raw) -> dict[str, list[str]]:
    """Parse membership tests from the compact string or from a table.

    The structured form is a table of column to values, where the values may
    be a list or the same space-separated string:
    ``{ machine = ["press7", "press8"], line = "l1" }``.
    """
    if not raw:
        return {}
    if isinstance(raw, dict):
        return {
            str(column): [str(value) for value in parse_delimited_list(values)]
            for column, values in raw.items()
        }
    return {
        column: parse_delimited_list(expression)
        for column, expression in parse_pairs(raw)
    }


# --- configuration ----------------------------------------------------------


def trimmed(value) -> str:
    return str(value).strip()


def keyword(value) -> str:
    return str(value).strip().lower()


def duration_seconds(value) -> float:
    """Read a duration as seconds, accepting ``30s`` and a bare number alike."""
    if value in (None, "", 0, "0"):
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return parse_timedelta(value).total_seconds()


def version_text(value) -> str:
    text = str(value).strip()
    return {"3": "3.1", "311": "3.1.1", "5.0": "5"}.get(text, text)


SETTING_VALIDATORS: list = [
    Validator("broker_url", required=True, cast=trimmed),
    Validator("source_measurements", required=True, cast=parse_delimited_list),
    Validator("topic", required=True, cast=trimmed),
    Validator("topic_missing_value", default=None),
    Validator("client_id", default="", cast=trimmed),
    Validator("instance_name", default="", cast=trimmed),
    Validator("username", default="", cast=trimmed),
    Validator("password", default="", cast=str),
    Validator("mqtt_version", default="3.1.1", cast=version_text, is_in=["3.1", "3.1.1", "5"]),
    Validator("keep_alive", default=300, cast=parse_int, gte=1),
    Validator("max_silence", default=0.0, cast=duration_seconds, gte=0.0),
    Validator("connect_timeout", default=2.0, cast=duration_seconds, gte=1.0),
    Validator("pump_budget", default=0.05, cast=duration_seconds, gt=0.0),
    Validator("reconnect_min_backoff", default=1.0, cast=duration_seconds, gte=1.0),
    Validator("reconnect_max_backoff", default=120.0, cast=duration_seconds, gte=1.0),
    Validator("tls_ca_cert", default="", cast=trimmed),
    Validator("tls_cert", default="", cast=trimmed),
    Validator("tls_key", default="", cast=trimmed),
    Validator("tls_insecure", default=False, cast=parse_bool),
    Validator("ws_path", default="", cast=trimmed),
    Validator("ws_headers", default={}, cast=parse_header_map),
    Validator("status_topic", default="", cast=trimmed),
    Validator("status_online_payload", default="online", cast=str),
    Validator("status_offline_payload", default="offline", cast=str),
    Validator("payload_format", default="json", cast=keyword, is_in=["json", "lineprotocol", "template"]),
    Validator("payload_fields", default=[], cast=parse_delimited_list),
    Validator("payload_template", default="", cast=str),
    Validator("layout", default="row", cast=keyword, is_in=["row", "batch", "field"]),
    Validator("time_format", default="rfc3339", cast=keyword, is_in=["rfc3339", "ms", "ns"]),
    Validator("qos", default=0, cast=parse_int, gte=0, lte=2),
    Validator("retain", default=False, cast=parse_bool),
    Validator("filters", default=[], cast=parse_filters),
    Validator("allow_values", default={}, cast=parse_value_lists),
    Validator("deny_values", default={}, cast=parse_value_lists),
    Validator("settle_delay", default=2.0, cast=duration_seconds, gte=0.0),
    Validator("tick_budget", default=5.0, cast=duration_seconds, gte=0.1),
    Validator("max_messages", default=10000, cast=parse_int, gte=1),
    Validator("max_group_rows", default=100000, cast=parse_int, gte=1),
    Validator("max_payload_bytes", default=122880, cast=parse_int, gte=1024),
    Validator("max_delivery_attempts", default=3, cast=parse_int, gte=1),
    Validator("max_queue_bytes", default=67108864, cast=parse_int, gte=1024),
    Validator("drop_policy", default="drop_oldest", cast=keyword, is_in=["drop_oldest", "drop_newest"]),
    Validator("max_message_age", default=0.0, cast=duration_seconds, gte=0.0),
    Validator("max_unconfirmed_bytes", default=16777216, cast=parse_int, gte=65536),
    Validator("max_inflight", default=20, cast=parse_int, gte=1),
    Validator("stats_interval", default=60.0, cast=duration_seconds, gte=0.0),
    Validator("enable_full_logging", default=False, cast=parse_bool),
]

BACKFILL_VALIDATORS: list = [
    Validator("backfill_start", default="", cast=trimmed),
    Validator("backfill_end", default="", cast=trimmed),
    Validator("backfill_order", default="oldest_first", cast=keyword, is_in=["oldest_first", "newest_first"]),
    Validator("max_backfill_messages", default=1000000, cast=parse_int, gte=0),
    Validator("publish_timeout", default=5.0, cast=duration_seconds, gte=0.0),
]

# Which rows to take, where they go and what the message says stay open to a
# request; the connection and every limit belong to the trigger.
REQUEST_SETTINGS = (
    "backfill_start",
    "backfill_end",
    "backfill_order",
    "source_measurements",
    "filters",
    "allow_values",
    "deny_values",
    "topic",
    "topic_missing_value",
    "payload_format",
    "payload_template",
    "payload_fields",
    "layout",
    "time_format",
)

SETTING_NAMES = tuple(
    dict.fromkeys(
        name
        for validator in SETTING_VALIDATORS + BACKFILL_VALIDATORS
        for name in validator.names
    )
)


def env_spec(*names: str) -> KeySpec:
    """Read the named settings from ``INFLUXDB3_MQTT_PUBLISHER_<SETTING>``."""
    rename = {f"{ENV_PREFIX}{name.upper()}": name for name in names}
    return KeySpec(allowlist=tuple(rename), rename=rename)


def header_name(name: str) -> str:
    """The header a setting is spelled as: ``X-Influxdb3-Mqtt-Publisher-Topic``."""
    return HEADER_PREFIX + name.replace("_", "-")


SETTING_ENV = env_spec(*SETTING_NAMES)

# A misspelled setting that silently does nothing costs an afternoon, so the
# layers the operator controls name what they accept and refuse the rest.
ARG_KEYS = KeySpec(allowlist=SETTING_NAMES + ("config_file_path",), unknown="reject")

# `config_file_path` names a layer rather than setting a value, so it is a
# trigger argument alone: neither the file nor a request may point at another.
TOML_KEYS = KeySpec(allowlist=SETTING_NAMES, unknown="reject")

BODY_KEYS = KeySpec(allowlist=REQUEST_SETTINGS, unknown="reject")
HEADER_KEYS = KeySpec(
    allowlist=tuple(header_name(name) for name in REQUEST_SETTINGS),
    rename={header_name(name): name for name in REQUEST_SETTINGS},
)
QUERY_KEYS = KeySpec(allowlist=REQUEST_SETTINGS, unknown="reject")


def check_settings(influxdb3_local, cfg: dict, task_id: str) -> None:
    """The refusals whose message has to name a second setting."""
    if cfg["payload_format"] == "template" and not cfg["payload_template"]:
        raise ValueError("payload_format='template' needs 'payload_template'")

    # A broker address that cannot be parsed will not start parsing after a
    # backoff, so it is refused here rather than failing every invocation with
    # a message `enable_full_logging` withholds.
    try:
        parse_broker_url(cfg["broker_url"])
    except ValueError as exc:
        raise ValueError(f"broker_url: {exc}") from None

    # Unreadable TLS material never fixes itself by retrying, so it is a
    # configuration error rather than a failed connection attempt. The path is
    # named; the file's contents never are.
    for setting in ("tls_ca_cert", "tls_cert", "tls_key"):
        path = cfg[setting]
        if not path:
            continue
        try:
            material = resolve_path(path).read_bytes()
        except OSError as exc:
            raise ValueError(
                f"{setting}: cannot read {path!r} ({exc.strerror or type(exc).__name__})"
            ) from None
        # An encrypted key would make the TLS handshake block on a passphrase
        # prompt that never comes, so it is refused here with a clear message
        # rather than hanging every invocation.
        if setting == "tls_key" and (
            b"ENCRYPTED" in material or b"Proc-Type: 4,ENCRYPTED" in material
        ):
            raise ValueError(
                f"tls_key: {path!r} is passphrase-protected, which is not "
                f"supported; provide an unencrypted key"
            )
    if bool(cfg["tls_cert"]) != bool(cfg["tls_key"]):
        raise ValueError("mutual TLS needs both 'tls_cert' and 'tls_key'")
    if cfg["max_message_age"] and cfg["settle_delay"]:
        if cfg["max_message_age"] <= cfg["settle_delay"]:
            raise ValueError(
                f"max_message_age ({cfg['max_message_age']:.0f}s) must exceed "
                f"settle_delay ({cfg['settle_delay']:.0f}s): the clamp would put the "
                f"lower bound of every window above its upper bound and nothing "
                f"would ever be published"
            )
    # Parsed here as well as in the backfill, so that a malformed timestamp is
    # a configuration error naming the setting rather than a failure half way
    # through a call.
    for name in ("backfill_start", "backfill_end"):
        parse_timestamp_argument(cfg[name], name)

    if cfg["qos"] == 2:
        influxdb3_local.warn(
            f"[{task_id}] qos=2 is not exactly-once under a clean session and a "
            f"refusal at PUBREC is invisible; qos=1 is recommended."
        )
    if "time" in placeholder_names(cfg["topic"]):
        raise ValueError(
            "topic may not use {time}: a topic per timestamp is a new topic for "
            "every row, which no consumer can subscribe to and which a broker "
            "keeps for ever once retained. Put it in the payload instead"
        )
    # `+` and `#` are MQTT subscription wildcards and a placeholder name can
    # never contain them, so one in the topic is a literal and a publish to it
    # would be refused by paho on every row — a configuration error, not a
    # per-row one.
    for topic_setting in ("topic", "status_topic"):
        for wildcard in ("+", "#"):
            if wildcard in cfg[topic_setting]:
                raise ValueError(
                    f"{topic_setting} may not contain {wildcard!r}: it is an MQTT wildcard"
                )
    probe = {name: "x" for name in placeholder_names(cfg["topic"])}
    _, unpublishable = render_topic(cfg["topic"], probe, None)
    if unpublishable:
        raise ValueError(
            f"{unpublishable}: a topic may not be empty, start with '$' or "
            "contain an empty level"
        )
    for name in placeholder_names(cfg["topic"]) + placeholder_names(cfg["payload_template"]):
        if name in ("_table", "_field", "time"):
            continue
        if not PLACEHOLDER_NAME.match(name):
            raise ValueError(f"placeholder {{{name}}} is not a usable column name")
    if not cfg["instance_name"]:
        material = f"{sorted(cfg['source_measurements'])}|{cfg['topic']}"
        cfg["instance_name"] = hashlib.sha256(material.encode()).hexdigest()[:8]


def validate_placeholders(influxdb3_local, state, cfg: dict, task_id: str) -> None:
    """Check every placeholder against the schemas of the source tables.

    A placeholder must exist in **at least one** of them: requiring it in every
    table would make a trigger over several unlike tables impossible. A table
    that does not exist yet cannot be checked at all — the normal order for a
    write trigger — so that is one warning and the check waits for the table.
    """
    names = [
        name
        for name in placeholder_names(cfg["topic"]) + placeholder_names(cfg["payload_template"])
        if name not in ("_table", "_field", "time")
    ]
    if not names:
        return

    seen: set = set()
    missing_tables = []
    for table in cfg["source_measurements"]:
        try:
            schema = cached_schema(state, table) or cache_schema(
                state, table, table_schema(influxdb3_local, table)
            )
        except Exception:
            missing_tables.append(table)
            continue
        seen.update(schema["columns"])

    if missing_tables:
        influxdb3_local.warn(
            f"[{task_id}] Not checking placeholders against "
            f"{', '.join(missing_tables)}: no schema yet. They are checked once "
            f"the table exists."
        )
        return

    unknown = [name for name in names if name not in seen]
    if unknown:
        raise ValueError(
            f"placeholder(s) {', '.join('{' + name + '}' for name in unknown)} name no "
            f"column of {', '.join(cfg['source_measurements'])}; available: "
            f"{', '.join(sorted(seen))}"
        )


NUMERIC_LINE_TYPES = frozenset({"int", "uint", "float"})
ORDERING_OPERATORS = frozenset({">", "<", ">=", "<="})


def validate_filters(influxdb3_local, state, cfg: dict, task_id: str) -> None:
    """Check filter columns and literals against the schemas, once per table.

    The check waits for the schema rather than running at startup, because a
    write trigger is routinely created before the first write to its table and
    there is no type to compare against until then.
    """
    tests = (
        [(column, operator, value) for column, operator, value in cfg["filters"]]
        + [(column, "=", None) for column in cfg["allow_values"]]
        + [(column, "=", None) for column in cfg["deny_values"]]
    )
    if not tests:
        return

    types: dict = {}
    for table in cfg["source_measurements"]:
        schema = cached_schema(state, table)
        if schema is None:
            try:
                schema = cache_schema(state, table, table_schema(influxdb3_local, table))
            except Exception:
                return  # the table is not there yet; a warning was already logged
        types.update(
            {name: kind for name, kind in schema["types"].items() if name not in types}
        )

    for column, operator, value in tests:
        if column not in types:
            raise ValueError(
                f"filter names column {column!r}, which no source table has; "
                f"available: {', '.join(sorted(types))}"
            )
        kind = types[column]
        numeric_column = kind in NUMERIC_LINE_TYPES
        if operator in ORDERING_OPERATORS and not numeric_column:
            raise ValueError(
                f"filter {column!r} {operator} needs a numeric column, and {column!r} "
                f"is {kind or 'of an unknown type'}"
            )
        if value is not None and isinstance(value, (int, float)) and not isinstance(value, bool):
            if kind is not None and not numeric_column:
                raise ValueError(
                    f"filter compares {column!r}, which is {kind}, with the number "
                    f"{value!r}; quote it to compare as text"
                )


def load_settings(influxdb3_local, args: dict, task_id: str, request_layers=()):
    """Merge every layer that applies and validate the result."""
    config_file_path = args.get("config_file_path") or parse_env(
        env_spec("config_file_path")
    ).get("config_file_path")
    cfg = load_config(
        parse_env(SETTING_ENV),
        parse_trigger_args(args, ARG_KEYS),
        parse_toml(config_file_path, TOML_KEYS),
        *request_layers,
        validators=SETTING_VALIDATORS + BACKFILL_VALIDATORS,
    )
    check_settings(influxdb3_local, cfg, task_id)
    return cfg


# --- logging ----------------------------------------------------------------


def describe_error(exc: BaseException, full_logging: bool) -> str:
    """Describe an exception without leaking what the settings contain.

    The diagnosis is never withheld — the exception type, ``errno`` and
    ``strerror`` name the problem and carry no secret. ``enable_full_logging``
    adds the exception's own message, which may quote a broker URL or a path.
    """
    parts = [type(exc).__name__]
    errno_value = getattr(exc, "errno", None)
    if errno_value is not None:
        parts.append(f"errno={errno_value}")
    strerror = getattr(exc, "strerror", None)
    if strerror:
        parts.append(str(strerror))
    if full_logging:
        parts.append(str(exc))
    return ": ".join(parts)


def reason_text(reason_code) -> str:
    """Render a paho reason code as ``0x87 Not authorized``."""
    if reason_code is None:
        return "no reason code"
    value = getattr(reason_code, "value", reason_code)
    try:
        return f"0x{int(value):02X} {reason_code}"
    except (TypeError, ValueError):
        return str(reason_code)


# --- templates --------------------------------------------------------------


def placeholder_names(template: str) -> list[str]:
    """The column names a template interpolates, in order of first appearance."""
    names: list[str] = []
    for match in TEMPLATE_TOKEN.finditer(template or ""):
        name = match.group(1)
        if name and name not in names:
            names.append(name)
    return names


def render_template(template: str, values: dict, missing_value: str | None):
    """Interpolate a template against one row's values.

    Returns ``None`` when a placeholder has no value and ``missing_value`` is
    unset, which is how a caller learns the row must be skipped.
    """
    out: list[str] = []
    position = 0
    for match in TEMPLATE_TOKEN.finditer(template):
        out.append(template[position : match.start()])
        token = match.group(0)
        if token == "{{":
            out.append("{")
        elif token == "}}":
            out.append("}")
        else:
            value = values.get(match.group(1))
            if value is None or value == "":
                if missing_value is None:
                    return None
                value = missing_value
            out.append(str(value))
        position = match.end()
    out.append(template[position:])
    return "".join(out)


def render_topic(template: str, values: dict, missing_value: str | None):
    """Interpolate a topic, refusing a substitution that would change its shape.

    Returns ``(topic, None)`` or ``(None, reason)``. A substituted value may not
    add a topic level or a wildcard: the value is data, and data must not be
    able to steer a message somewhere it was not meant to go.
    """
    out: list[str] = []
    position = 0
    for match in TEMPLATE_TOKEN.finditer(template):
        out.append(template[position : match.start()])
        token = match.group(0)
        if token == "{{":
            out.append("{")
        elif token == "}}":
            out.append("}")
        else:
            name = match.group(1)
            value = values.get(name)
            if value is None or value == "":
                if missing_value is None:
                    return None, f"no value for {{{name}}}"
                value = missing_value
            value = str(value)
            for forbidden in TOPIC_FORBIDDEN:
                if forbidden in value:
                    return None, f"value of {{{name}}} contains {forbidden!r}"
            out.append(value)
        position = match.end()
    out.append(template[position:])
    topic = "".join(out)
    if not topic or "//" in topic or topic.startswith("$"):
        return None, f"topic {topic!r} is not publishable"
    return topic, None


# --- payload encoders -------------------------------------------------------


def format_timestamp(time_ns: int, time_format: str):
    """Render integer nanoseconds as ``rfc3339``, ``ms`` or ``ns``."""
    if time_ns is None:
        return None
    if time_format == "ns":
        return time_ns
    if time_format == "ms":
        return time_ns // 1_000_000
    seconds, nanos = divmod(int(time_ns), 1_000_000_000)
    moment = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return f"{moment.strftime('%Y-%m-%dT%H:%M:%S')}.{nanos:09d}Z"


def escape_line_key(value: str) -> str:
    """Escape a measurement name, tag key, tag value or field key."""
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace(",", "\\,")
        .replace(" ", "\\ ")
        .replace("=", "\\=")
    )


def escape_line_measurement(value: str) -> str:
    """Escape a measurement name, where ``=`` needs no escaping."""
    return str(value).replace("\\", "\\\\").replace(",", "\\,").replace(" ", "\\ ")


def encode_field_value(value, line_type: str | None) -> str:
    """Render one field value with the type suffix line protocol requires.

    A bare number is a float in line protocol, so an integer published without
    its ``i`` comes back as a float and collides with the column it came from.
    ``bool`` is checked before ``int`` because in Python it is one.
    """
    if line_type is None:
        if isinstance(value, bool):
            line_type = "bool"
        elif isinstance(value, int):
            line_type = "int"
        elif isinstance(value, float):
            line_type = "float"
        else:
            line_type = "string"
    if line_type == "bool":
        return "t" if value else "f"
    if line_type == "int":
        return f"{int(value)}i"
    if line_type == "uint":
        return f"{int(value)}u"
    if line_type == "float":
        return repr(float(value))
    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def encode_line_protocol(table: str, tags: dict, fields: dict, time_ns, types: dict):
    """Encode one row as line protocol, or ``None`` when it carries no field.

    Tags and fields are written in name order so that the same row encodes
    identically whichever trigger type read it: a write trigger receives the
    row in the engine's order, while a query returns the columns in the order
    the statement selected them.
    """
    if not fields:
        return None
    head = escape_line_measurement(table)
    for key, value in sorted(tags.items()):
        if value is None or value == "":
            continue
        head += f",{escape_line_key(key)}={escape_line_key(value)}"
    body = ",".join(
        f"{escape_line_key(key)}={encode_field_value(value, types.get(key))}"
        for key, value in sorted(fields.items())
    )
    line = f"{head} {body}"
    if time_ns is not None:
        line += f" {int(time_ns)}"
    return line


def encode_json(values: dict, time_ns, time_format: str, with_time: bool) -> str:
    """Encode one row as a JSON object: the timestamp, then the columns by name.

    Name order is what makes the same row encode identically from a write
    trigger and from a query, whose column orders differ.
    """
    document: dict = {}
    if with_time and time_ns is not None:
        document["time"] = format_timestamp(time_ns, time_format)
    for name in sorted(values):
        document[name] = values[name]
    return json.dumps(document, separators=(",", ":"), default=str)


# --- filters ----------------------------------------------------------------


def row_passes(row: dict, cfg: dict) -> bool:
    """Apply the filters to one row, exactly as the compiled SQL would.

    A row that does not carry the column fails a positive test, fails
    ``allow_values`` and passes both ``!=`` and ``deny_values`` — it cannot
    match a value it does not have.
    """
    for column, operator, expected in cfg["filters"]:
        value = row.get(column)
        if value is None:
            if operator not in NEGATIVE_OPERATORS:
                return False
            continue
        try:
            if operator == "=" and not value == expected:
                return False
            if operator == "!=" and not value != expected:
                return False
            if operator == ">" and not value > expected:
                return False
            if operator == "<" and not value < expected:
                return False
            if operator == ">=" and not value >= expected:
                return False
            if operator == "<=" and not value <= expected:
                return False
        except TypeError:
            return False

    for column, allowed in cfg["allow_values"].items():
        value = row.get(column)
        if value is None or str(value) not in allowed:
            return False

    for column, denied in cfg["deny_values"].items():
        value = row.get(column)
        if value is not None and str(value) in denied:
            return False

    return True


def quote_identifier(name: str) -> str:
    """Quote a column or table name as an SQL identifier."""
    return '"' + str(name).replace('"', '""') + '"'


def filter_clauses(cfg: dict) -> tuple[list[str], dict]:
    """Compile the filters into SQL fragments and their bound parameters.

    A negative test carries its own ``IS NULL`` arm: a bare ``!=`` or ``NOT IN``
    would drop rows where the column is null, so it is added to keep the SQL and
    in-memory paths selecting the same rows.
    """
    clauses: list[str] = []
    params: dict = {}

    for index, (column, operator, value) in enumerate(cfg["filters"]):
        quoted = quote_identifier(column)
        # The runtime binds only string parameters, so a numeric literal is
        # inlined instead; it came from the typed parser and is a plain number,
        # injection-safe by construction. A string value stays a bound parameter.
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            operand = repr(value)
        else:
            name = f"filter_{index}"
            params[name] = value
            operand = f"${name}"
        if operator in NEGATIVE_OPERATORS:
            clauses.append(f"({quoted} IS NULL OR {quoted} {operator} {operand})")
        else:
            clauses.append(f"{quoted} {operator} {operand}")

    for index, (column, allowed) in enumerate(cfg["allow_values"].items()):
        names = []
        for position, value in enumerate(allowed):
            name = f"allow_{index}_{position}"
            params[name] = value
            names.append(f"${name}")
        clauses.append(f"{quote_identifier(column)} IN ({', '.join(names)})")

    for index, (column, denied) in enumerate(cfg["deny_values"].items()):
        names = []
        for position, value in enumerate(denied):
            name = f"deny_{index}_{position}"
            params[name] = value
            names.append(f"${name}")
        quoted = quote_identifier(column)
        clauses.append(f"({quoted} IS NULL OR {quoted} NOT IN ({', '.join(names)}))")

    return clauses, params


# --- schema -----------------------------------------------------------------


def table_schema(influxdb3_local, table: str, refresh: bool = False) -> dict:
    """Return ``{"tags": [...], "types": {column: line_type}}`` for a table.

    The data type is kept alongside the tag/field split because line protocol
    needs it to tell an integer from an unsigned one, which no Python value
    does, and a compiled filter needs it to refuse a comparison the column
    cannot support.
    """
    columns = get_schema(influxdb3_local, table, refresh=refresh)
    line_types = {
        "Int64": "int",
        "Int32": "int",
        "UInt64": "uint",
        "Float64": "float",
        "Float32": "float",
        "Boolean": "bool",
        "Utf8": "string",
    }
    return {
        "tags": [name for name, kind in columns.items() if kind == TAG_DATA_TYPE],
        "types": {
            name: (line_types.get(kind) if kind != TAG_DATA_TYPE else "string")
            for name, kind in columns.items()
            if name != "time"
        },
        "columns": list(columns),
    }


def cache_schema(state, table: str, schema: dict) -> dict:
    """Store a table's schema and stamp when it was read, for the TTL."""
    state.schema[table] = schema
    state.schema_at[table] = time.monotonic()
    return schema


def cached_schema(state, table: str):
    """The cached schema if it is still within ``SCHEMA_TTL_SECONDS``, else None.

    Past the TTL the caller re-reads the catalog, which is how a scheduled or
    backfill read, whose SELECT lists only the columns it already knows, comes
    to notice a column added after the first read.
    """
    schema = state.schema.get(table)
    if schema is None:
        return None
    if time.monotonic() - state.schema_at.get(table, 0.0) >= SCHEMA_TTL_SECONDS:
        return None
    return schema


def known_schema(influxdb3_local, state, table: str, row: dict):
    """The cached schema for a table, re-read once when a row brings a new column.

    A column still unknown after the re-read is recorded as one whose type is
    inferred from the value, so it cannot make every row that follows re-read
    the catalog.
    """
    schema = cached_schema(state, table)
    if schema is None:
        schema = cache_schema(state, table, table_schema(influxdb3_local, table, refresh=True))
    unknown = [key for key in row if key != "time" and key not in schema["types"]]
    if not unknown:
        return schema

    schema = table_schema(influxdb3_local, table, refresh=True)
    for key in unknown:
        if key not in schema["types"]:
            schema["types"][key] = None
            schema["columns"].append(key)
    return cache_schema(state, table, schema)


# --- messages and buffer ----------------------------------------------------


class Message:
    """One MQTT message waiting to be handed over, or awaiting acknowledgement."""

    __slots__ = ("topic", "payload", "qos", "retain", "time_ns", "attempts", "size")

    def __init__(self, topic: str, payload, qos: int, retain: bool, time_ns):
        self.topic = topic
        self.payload = payload
        self.qos = qos
        self.retain = retain
        self.time_ns = time_ns
        self.attempts = 0
        self.size = len(payload) + len(topic) + MESSAGE_OVERHEAD_BYTES


def message_age_seconds(message: Message, now_ns: int) -> float:
    """How old the message's row is, in seconds, or 0 when it carries no time."""
    if message.time_ns is None:
        return 0.0
    return max(0.0, (now_ns - message.time_ns) / 1_000_000_000)


class CallbackState:
    """What the paho callbacks touch, and nothing else.

    It references neither the client nor the state that owns the client, so
    dropping that state releases the client by reference count rather than
    waiting for the cyclic collector. ``unconfirmed`` lives here because the
    callbacks are what retire its entries.
    """

    __slots__ = (
        "generation",
        "unconfirmed",
        "unconfirmed_bytes",
        "counters",
        "last_inbound",
        "connected",
        "connect_reason",
        "disconnect_reason",
        "disconnect_from_broker",
        "disconnected",
        "server_keep_alive",
        "assigned_client_id",
        "receive_maximum",
        "maximum_packet_size",
        "quota_refused",
        "quota_returned",
        "lock",
    )

    def __init__(self, generation: int):
        self.generation = generation
        self.unconfirmed: dict = {}
        self.unconfirmed_bytes = 0
        self.counters = new_counters()
        self.last_inbound = time.monotonic()
        self.connected = False
        self.connect_reason = None
        self.disconnect_reason = None
        self.disconnect_from_broker = False
        self.disconnected = False
        self.server_keep_alive = None
        self.assigned_client_id = None
        self.receive_maximum = None
        self.maximum_packet_size = None
        self.quota_refused = False
        # Messages the broker threw back with 0x97; drain, which can reach the
        # buffer, returns them to the head of pending.
        self.quota_returned: list = []
        self.lock = threading.Lock()


def new_counters() -> dict:
    """The counters reported to the log and to ``mqtt_publisher_stats``."""
    return {
        "published": 0,
        "confirmed": 0,
        "failed": 0,
        "buffered": 0,
        "dropped_policy": 0,
        "dropped_age": 0,
        "dropped_undeliverable": 0,
        "dropped_oversized": 0,
        "lost_disconnect": 0,
        "reconnects": 0,
        "skipped_reads": 0,
        "quota_refusals": 0,
    }


def on_connect(client, callbacks, connect_flags, reason_code, properties=None):
    """Record the CONNACK, including the properties paho itself ignores."""
    callbacks.last_inbound = time.monotonic()
    callbacks.connect_reason = reason_code
    callbacks.connected = getattr(reason_code, "value", reason_code) == 0
    if properties is not None:
        callbacks.server_keep_alive = getattr(properties, "ServerKeepAlive", None)
        callbacks.assigned_client_id = getattr(
            properties, "AssignedClientIdentifier", None
        )
        # Paho enforces neither of these, so the plugin clamps its own limits to
        # them; exceeding one otherwise draws a 0x93 or 0x95 disconnection.
        callbacks.receive_maximum = getattr(properties, "ReceiveMaximum", None)
        callbacks.maximum_packet_size = getattr(properties, "MaximumPacketSize", None)


def on_disconnect(client, callbacks, disconnect_flags, reason_code, properties=None):
    """Record that the connection is gone, why, and who said so.

    A reason code paho synthesised for a socket that died says nothing about
    the message that was in flight; only one the broker sent does.
    """
    callbacks.connected = False
    callbacks.disconnected = True
    callbacks.disconnect_reason = reason_code
    callbacks.disconnect_from_broker = bool(
        getattr(disconnect_flags, "is_disconnect_packet_from_server", False)
    )


def on_publish(client, callbacks, mid, reason_code=None, properties=None):
    """Retire a message, or drop it when the broker answered with a refusal.

    A reason code of ``0x80`` or above names the message it refers to, so there
    is nothing to retry; ``0x97`` is about the rate rather than the message and
    the entry is handed back for the throttle path to re-queue.
    """
    key = (callbacks.generation, mid)
    message = callbacks.unconfirmed.pop(key, None)
    if message is None:
        # A QoS 0 publish also calls back here on the local socket write, which
        # is not inbound traffic, so the silence timer is refreshed only once a
        # real PUBACK/PUBCOMP has matched an outstanding message.
        return
    callbacks.last_inbound = time.monotonic()
    callbacks.unconfirmed_bytes -= message.size
    code = getattr(reason_code, "value", reason_code)
    if code is not None and code >= 0x80:
        if code == QUOTA_EXCEEDED:
            callbacks.quota_refused = True
            callbacks.counters["quota_refusals"] += 1
            callbacks.quota_returned.append(message)
            return
        callbacks.counters["failed"] += 1
        callbacks.counters["dropped_undeliverable"] += 1
        return
    callbacks.counters["confirmed"] += 1


# --- publisher state --------------------------------------------------------


class PublisherState:
    """Everything one trigger keeps between invocations."""

    __slots__ = (
        "client",
        "callbacks",
        "fingerprint",
        "client_id",
        "pending",
        "pending_bytes",
        "next_attempt",
        "backoff",
        "schema",
        "schema_at",
        "watermark",
        "queue_lock",
        "client_lock",
        "next_stats",
        "keep_alive",
        "inflight_limit",
        "packet_size_limit",
    )

    def __init__(self, fingerprint: str, client_id: str, keep_alive: int):
        self.client = None
        self.callbacks = CallbackState(generation=0)
        self.fingerprint = fingerprint
        self.client_id = client_id
        self.pending: deque = deque()
        self.pending_bytes = 0
        self.next_attempt = 0.0
        self.backoff = 0.0
        self.schema: dict = {}
        self.schema_at: dict = {}
        self.watermark: dict = {}
        self.queue_lock = threading.Lock()
        # Reentrant: the scheduled path holds it across a read while draining.
        self.client_lock = threading.RLock()
        self.next_stats = 0.0
        self.keep_alive = keep_alive
        self.inflight_limit = None
        self.packet_size_limit = None


def connection_fingerprint(cfg: dict) -> str:
    """Hash of every setting a live connection depends on."""
    relevant = (
        "broker_url",
        "client_id",
        "username",
        "password",
        "mqtt_version",
        "keep_alive",
        "tls_ca_cert",
        "tls_cert",
        "tls_key",
        "tls_insecure",
        "ws_path",
        "ws_headers",
        "status_topic",
        "status_offline_payload",
        "max_inflight",
    )
    material = "|".join(f"{name}={cfg.get(name)!r}" for name in relevant)
    return hashlib.sha256(material.encode()).hexdigest()[:16]


def is_publisher_state(value) -> bool:
    """Is this a ``PublisherState`` left by an earlier invocation?

    ``isinstance`` cannot answer that. The engine executes the plugin's source
    into a fresh namespace per invocation, so every invocation defines its own
    class object: a state cached by the previous one is an instance of *that*
    invocation's class, and ``isinstance(state, PublisherState)`` is false
    against this one. Using it would build a new client on every invocation and
    leak the old one's socket, so the type is matched by name instead.
    """
    return type(value).__name__ == "PublisherState" and hasattr(value, "fingerprint")


def get_state(influxdb3_local, cfg: dict) -> PublisherState:
    """The cached state for this trigger, built on a miss.

    A settings change does not discard the state — the buffer and the
    watermarks survive it — only the client, which ``ensure_client`` rebuilds
    when the fingerprint no longer matches. Two invocations racing on a cold
    cache both build and both store; the convergence at the end of the
    invocation settles which one survives.
    """
    state = influxdb3_local.cache.get(STATE_CACHE_KEY)
    if not is_publisher_state(state):
        client_id = cfg["client_id"] or f"influxdb3-pub-{uuid.uuid4().hex[:8]}"
        state = PublisherState(connection_fingerprint(cfg), client_id, cfg["keep_alive"])
        influxdb3_local.cache.put(STATE_CACHE_KEY, state, None)
    apply_declared_limits(state, cfg)
    return state


def apply_declared_limits(state: PublisherState, cfg: dict) -> None:
    """Re-apply the limits the broker declared to this invocation's settings.

    ``cfg`` is rebuilt from its layers every invocation while the client is
    cached across them, so a clamp applied once at connect time is gone by the
    next invocation — and the plugin would go back to building messages the
    broker refuses, such as a payload above its ``Maximum Packet Size``.
    """
    if state.inflight_limit:
        cfg["max_inflight"] = min(cfg["max_inflight"], state.inflight_limit)
    if state.packet_size_limit:
        cfg["max_payload_bytes"] = min(
            cfg["max_payload_bytes"], state.packet_size_limit
        )


def converge(influxdb3_local, state: PublisherState, cfg: dict, task_id: str) -> None:
    """Settle a cold-cache race at the end of the invocation.

    The cache offers no compare-and-set and reading a key back does not
    arbitrate, so the plugin converges instead: whoever is no longer in the
    cache hands over what it holds and closes. Its unconfirmed messages go with
    its buffer, because a clean ``DISCONNECT`` abandons whatever the broker has
    not acknowledged.
    """
    current = influxdb3_local.cache.get(STATE_CACHE_KEY)
    if current is state or not is_publisher_state(current):
        return

    callbacks = state.callbacks
    returning = [
        message
        for message in callbacks.unconfirmed.values()
        if message.qos > 0
    ]
    with current.queue_lock:
        for message in reversed(returning):
            current.pending.appendleft(message)
            current.pending_bytes += message.size
        while state.pending:
            message = state.pending.pop()
            current.pending.appendleft(message)
            current.pending_bytes += message.size
    state.pending_bytes = 0
    callbacks.unconfirmed.clear()
    callbacks.unconfirmed_bytes = 0
    influxdb3_local.info(
        f"[{task_id}] Lost the cold-cache race; handed "
        f"{len(returning)} unconfirmed and the buffer to the live client."
    )
    close_client(state)


# --- client lifecycle -------------------------------------------------------


def parse_broker_url(raw: str) -> dict:
    """Split ``broker_url`` into transport, TLS, host, port and path."""
    text = str(raw).strip()
    if "://" not in text:
        text = "mqtt://" + text
    parsed = urlparse(text)
    scheme = (parsed.scheme or "mqtt").lower()
    if scheme == "unix":
        return {"transport": "unix", "tls": False, "host": parsed.path, "port": 0, "path": ""}
    if scheme not in DEFAULT_PORTS:
        raise ValueError(
            f"broker_url scheme {scheme!r} is not one of mqtt, mqtts, ws, wss, unix"
        )
    if not parsed.hostname:
        raise ValueError(f"broker_url {raw!r} names no host")
    return {
        "transport": "websockets" if scheme in ("ws", "wss") else "tcp",
        "tls": scheme in ("mqtts", "wss"),
        "host": parsed.hostname,
        "port": parsed.port or DEFAULT_PORTS[scheme],
        "path": parsed.path or "/mqtt",
    }


def apply_socket_options(client, max_silence: float) -> None:
    """Bound how long a silently dropped flow can look alive.

    ``SO_KEEPALIVE`` on its own inherits the system's idle time, which on Linux
    is two hours — long enough for every NAT in between to have forgotten the
    flow. The probes also keep that translation open, which matters because
    nothing else of this plugin sends anything between invocations.
    """
    raw = client.socket()
    sock = getattr(raw, "_socket", raw)
    if sock is None or not hasattr(sock, "setsockopt"):
        return
    idle = max(1, int(max_silence))
    options = [
        (socket.SOL_SOCKET, "SO_KEEPALIVE", 1),
        (socket.IPPROTO_TCP, "TCP_KEEPIDLE", idle),
        (socket.IPPROTO_TCP, "TCP_KEEPINTVL", 10),
        (socket.IPPROTO_TCP, "TCP_KEEPCNT", 3),
        (socket.IPPROTO_TCP, "TCP_NODELAY", 1),
        # Bounded by the keepalive window (idle + INTVL*CNT), not connect_timeout:
        # un-acked data gets as long as the probes do, so a brief loss does not
        # tear down a healthy connection the way a 2-second limit would.
        (socket.IPPROTO_TCP, "TCP_USER_TIMEOUT", int((idle + 10 * 3) * 1000)),
    ]
    for level, name, value in options:
        option = getattr(socket, name, None)
        if option is None:
            continue
        try:
            sock.setsockopt(level, option, value)
        except OSError:
            # An option the kernel refuses is not worth failing a publish over.
            continue


def build_client(influxdb3_local, state: PublisherState, cfg: dict, task_id: str):
    """Open a connection and return a connected client, or ``None``.

    Connecting happens on the invocation's own thread, so the attempt is made
    at most once per backoff interval and is bounded by ``connect_timeout`` —
    for the TCP connect, which paho applies, and for the CONNACK wait, which
    this does.
    """
    now = time.monotonic()
    if now < state.next_attempt:
        return None

    protocol = PROTOCOL_VERSIONS[cfg["mqtt_version"]]
    address = parse_broker_url(cfg["broker_url"])
    callbacks = CallbackState(generation=state.callbacks.generation + 1)

    try:
        if protocol == mqtt.MQTTv5:
            client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id=state.client_id,
                protocol=protocol,
                transport=address["transport"],
                userdata=callbacks,
            )
        else:
            client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id=state.client_id,
                clean_session=True,
                protocol=protocol,
                transport=address["transport"],
                userdata=callbacks,
            )
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_publish = on_publish
        if state.inflight_limit:
            cfg["max_inflight"] = min(cfg["max_inflight"], state.inflight_limit)
        client.max_inflight_messages_set(cfg["max_inflight"])

        if cfg["username"]:
            client.username_pw_set(cfg["username"], cfg["password"] or None)
        if address["tls"]:
            context = ssl.create_default_context(
                cafile=str(resolve_path(cfg["tls_ca_cert"])) if cfg["tls_ca_cert"] else None
            )
            if cfg["tls_cert"] and cfg["tls_key"]:
                # A callback for the passphrase turns an encrypted key into a
                # clear error: without it OpenSSL prompts on the terminal, and
                # with none the invocation hangs instead of failing.
                def reject_passphrase():
                    raise ValueError(
                        "tls_key is passphrase-protected, which is not supported; "
                        "provide an unencrypted key"
                    )

                context.load_cert_chain(
                    str(resolve_path(cfg["tls_cert"])),
                    str(resolve_path(cfg["tls_key"])),
                    password=reject_passphrase,
                )
            if cfg["tls_insecure"]:
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
            client.tls_set_context(context)
        if address["transport"] == "websockets":
            client.ws_set_options(
                path=cfg["ws_path"] or address["path"],
                headers=cfg["ws_headers"] or None,
            )
        if cfg["status_topic"]:
            client.will_set(
                cfg["status_topic"],
                payload=cfg["status_offline_payload"],
                qos=1,
                retain=True,
            )

        client.connect_timeout = cfg["connect_timeout"]
        if protocol == mqtt.MQTTv5:
            client.connect(
                address["host"],
                address["port"],
                keepalive=cfg["keep_alive"],
                clean_start=True,
            )
        else:
            client.connect(address["host"], address["port"], keepalive=cfg["keep_alive"])

        apply_socket_options(client, state.keep_alive / 2)

        # Nothing is handed over before the CONNACK: a publish written into a
        # connection the server then rejects is discarded without any signal.
        deadline = time.monotonic() + cfg["connect_timeout"]
        while not callbacks.connected and not callbacks.disconnected:
            if time.monotonic() >= deadline:
                raise TimeoutError("no CONNACK within connect_timeout")
            client.loop(timeout=0.05)
        if not callbacks.connected:
            raise ConnectionError(
                f"broker refused the connection: {reason_text(callbacks.connect_reason)}"
            )
    except Exception as exc:
        state.backoff = min(
            cfg["reconnect_max_backoff"],
            max(cfg["reconnect_min_backoff"], state.backoff * 2 or cfg["reconnect_min_backoff"]),
        )
        state.next_attempt = time.monotonic() + state.backoff
        # The CONNACK reason code is the diagnosis — `0x85 client identifier not
        # valid` and `0x86 bad user name or password` are different problems
        # with different fixes — so it is logged beside the exception rather
        # than inside its message, which `enable_full_logging` withholds.
        refusal = ""
        if callbacks.connect_reason is not None:
            code = getattr(callbacks.connect_reason, "value", callbacks.connect_reason)
            if code != 0:
                refusal = f" CONNACK {reason_text(callbacks.connect_reason)};"
        influxdb3_local.warn(
            f"[{task_id}] Connection failed, retrying in {state.backoff:.0f}s:"
            f"{refusal} {describe_error(exc, cfg['enable_full_logging'])}"
        )
        return None

    state.backoff = 0.0
    state.next_attempt = 0.0
    callbacks.counters = merge_counters(callbacks.counters, state.callbacks.counters)
    state.client = client
    state.callbacks = callbacks

    # The limit the plugin checks is on payload plus topic, so the protocol
    # overhead is reserved below the broker's packet cap or a message at the cap
    # would still overflow it and the broker would close with 0x95.
    broker_payload_cap = (
        int(callbacks.maximum_packet_size) - PACKET_OVERHEAD_RESERVE
        if callbacks.maximum_packet_size
        else None
    )
    if broker_payload_cap and broker_payload_cap < cfg["max_payload_bytes"]:
        # The plugin's own limit, applied while a message is being built, so
        # the clamped value takes effect immediately.
        influxdb3_local.warn(
            f"[{task_id}] Broker accepts at most {callbacks.maximum_packet_size} bytes "
            f"per packet; max_payload_bytes clamped from {cfg['max_payload_bytes']} "
            f"to {broker_payload_cap}."
        )
        cfg["max_payload_bytes"] = broker_payload_cap
        state.packet_size_limit = broker_payload_cap

    if callbacks.receive_maximum and callbacks.receive_maximum < cfg["max_inflight"]:
        # Paho's, and its setter raises on an established connection, so the
        # client is rebuilt with the clamped value — once, because the clamp is
        # remembered on the state and every later client is built with it.
        influxdb3_local.warn(
            f"[{task_id}] Broker allows {callbacks.receive_maximum} messages in flight; "
            f"max_inflight clamped from {cfg['max_inflight']} and the client rebuilt."
        )
        cfg["max_inflight"] = int(callbacks.receive_maximum)
        state.inflight_limit = int(callbacks.receive_maximum)
        close_client(state)
        return build_client(influxdb3_local, state, cfg, task_id)

    if callbacks.server_keep_alive:
        state.keep_alive = int(callbacks.server_keep_alive)
        influxdb3_local.info(
            f"[{task_id}] Broker imposed keep_alive={state.keep_alive}s; "
            f"the silence check follows it."
        )
    if cfg["status_topic"]:
        client.publish(
            cfg["status_topic"], cfg["status_online_payload"], qos=1, retain=True
        )
        client.loop(timeout=0.05)
    return client


def merge_counters(fresh: dict, previous: dict) -> dict:
    """Carry the counters across a rebuilt client, so a reconnect resets nothing."""
    merged = dict(previous)
    for name, value in fresh.items():
        merged[name] = merged.get(name, 0) + value
    return merged


def close_client(state: PublisherState):
    """Disconnect cleanly and let go of the client.

    Every client this plugin closes is being **replaced**, never shut down —
    the engine gives a plugin no teardown hook — so none of them publishes an
    offline payload: the trigger stays online through another client, and an
    offline payload on a retained topic would state the opposite of the truth.
    A clean ``DISCONNECT`` also keeps the will from firing for the same reason.
    """
    client = state.client
    state.client = None
    if client is None:
        return
    try:
        client.disconnect()
        client.loop(timeout=0.05)
    except Exception:
        # The connection is going away either way.
        pass


def requeue_unconfirmed(
    influxdb3_local, state: PublisherState, cfg: dict, charge_attempt: bool, task_id: str
) -> None:
    """Return what the dead client never confirmed, and charge one attempt.

    The attempt is charged to the oldest unconfirmed message alone: the broker
    handles publishes in order, so that is the one a refusal refers to, and
    charging the whole window would retire the innocent messages behind it.
    """
    callbacks = state.callbacks
    outstanding = list(callbacks.unconfirmed.values())
    callbacks.unconfirmed.clear()
    callbacks.unconfirmed_bytes = 0
    if not outstanding:
        return

    lost = [message for message in outstanding if message.qos == 0]
    returning = [message for message in outstanding if message.qos > 0]
    callbacks.counters["lost_disconnect"] += len(lost)

    if charge_attempt and returning:
        oldest = returning[0]
        oldest.attempts += 1
        if oldest.attempts >= cfg["max_delivery_attempts"]:
            returning.pop(0)
            callbacks.counters["dropped_undeliverable"] += 1
            influxdb3_local.error(
                f"[{task_id}] Message to '{oldest.topic}' ({len(oldest.payload)} bytes) "
                f"dropped after {oldest.attempts} attempts; the queue behind it continues."
            )

    with state.queue_lock:
        for message in reversed(returning):
            state.pending.appendleft(message)
            state.pending_bytes += message.size


def rebuild_client(
    influxdb3_local,
    state: PublisherState,
    cfg: dict,
    task_id: str,
    reason: str,
    charge_attempt: bool,
):
    """Replace the client rather than repairing the connection.

    ``reconnect()`` would leave paho holding the messages that were in flight
    and resending them itself, so the plugin could never retire one the broker
    keeps refusing. Building a new client puts it back in charge of what is
    retried and when.
    """
    callbacks = state.callbacks
    if callbacks.unconfirmed:
        requeue_unconfirmed(influxdb3_local, state, cfg, charge_attempt, task_id)
    close_client(state)
    callbacks.counters["reconnects"] += 1
    influxdb3_local.info(f"[{task_id}] Replacing the client: {reason}.")
    return build_client(influxdb3_local, state, cfg, task_id)


def ensure_client(influxdb3_local, state: PublisherState, cfg: dict, task_id: str):
    """The client to publish on, rebuilt when the old one cannot be trusted."""
    client = state.client
    callbacks = state.callbacks

    fingerprint = connection_fingerprint(cfg)
    if fingerprint != state.fingerprint:
        state.fingerprint = fingerprint
        state.keep_alive = cfg["keep_alive"]
        if cfg["client_id"]:
            state.client_id = cfg["client_id"]
        if client is not None:
            return rebuild_client(
                influxdb3_local,
                state,
                cfg,
                task_id,
                "a connection setting changed",
                charge_attempt=False,
            )

    if client is None:
        return build_client(influxdb3_local, state, cfg, task_id)

    # A pending FIN is seen before anything is handed over.
    try:
        client.loop(timeout=0.01)
    except Exception:
        callbacks.disconnected = True

    if callbacks.disconnected or not client.is_connected():
        code = getattr(callbacks.disconnect_reason, "value", callbacks.disconnect_reason)
        # Only a reason code the broker sent can be attributed to a message.
        # MQTT 5 lets a server close without a DISCONNECT, and treating that as
        # a network event would leave a message that provokes it never retiring.
        from_broker = callbacks.disconnect_from_broker
        charge = (
            cfg["mqtt_version"] != "5"
            or not from_broker
            or code in MESSAGE_ATTRIBUTABLE_CODES
        )
        if from_broker and code == QUOTA_EXCEEDED:
            charge = False
            callbacks.counters["quota_refusals"] += 1
        return rebuild_client(
            influxdb3_local,
            state,
            cfg,
            task_id,
            f"connection lost ({reason_text(callbacks.disconnect_reason)})",
            charge_attempt=charge,
        )

    silence_limit = min(cfg["max_silence"] or state.keep_alive / 2, state.keep_alive / 2)
    silence = time.monotonic() - callbacks.last_inbound
    if silence > silence_limit:
        # Proving the link needs a PINGREQ, which paho does not expose; a
        # connection this quiet was close to being dropped anyway.
        return rebuild_client(
            influxdb3_local,
            state,
            cfg,
            task_id,
            f"silent for {silence:.0f}s",
            charge_attempt=False,
        )

    return client


# --- buffering and publishing -----------------------------------------------


def queue_messages(state: PublisherState, cfg: dict, messages: list, counters: dict):
    """Append to the buffer under the byte budget and the drop policy.

    The accounting needs the lock even though the ``deque`` does not: an
    integer ``+=`` is a load, an add and a store, so two invocations queueing
    at once would lose a count and the budget would drift from what is held.
    """
    with state.queue_lock:
        for message in messages:
            if state.pending_bytes + message.size > cfg["max_queue_bytes"]:
                if cfg["drop_policy"] == "drop_newest":
                    counters["dropped_policy"] += 1
                    continue
                while state.pending and state.pending_bytes + message.size > cfg["max_queue_bytes"]:
                    evicted = state.pending.popleft()
                    state.pending_bytes -= evicted.size
                    counters["dropped_policy"] += 1
            state.pending.append(message)
            state.pending_bytes += message.size
            counters["buffered"] += 1


def publish_properties(cfg: dict, message: Message, now_ns: int):
    """MQTT 5 properties for one message, or ``None`` under 3.1.1.

    The expiry interval is the message's remaining life rather than the whole
    limit, because the broker counts expiry from when it receives the message.
    Anything that reached here has passed the age check, so the remainder is
    positive and a non-positive interval is never sent.
    """
    if cfg["mqtt_version"] != "5":
        return None
    properties = Properties(PacketTypes.PUBLISH)
    if cfg["payload_format"] == "json" and cfg["layout"] != "field":
        properties.ContentType = "application/json"
    if cfg["max_message_age"] and message.time_ns is not None:
        remaining = int(cfg["max_message_age"] - message_age_seconds(message, now_ns))
        if remaining > 0:
            properties.MessageExpiryInterval = remaining
    return properties


def hand_over(influxdb3_local, state, client, cfg, message, task_id) -> bool:
    """Hand one message to the client, recording it only if it is outstanding.

    Whether to record is decided from the ``MQTTMessageInfo`` ``publish()``
    returns, never from the callback: with no network thread a QoS 0 publish
    reaches the socket inside ``publish()``, so ``on_publish`` has already run
    by the time it returns, and an id recorded afterwards would never clear.

    Returns ``True`` when the message is finished with — published or dropped as
    undeliverable — and ``False`` only for a transient refusal the caller should
    retry, so one poison message never blocks the queue behind it.
    """
    callbacks = state.callbacks
    try:
        info = client.publish(
            message.topic,
            message.payload,
            qos=message.qos,
            retain=message.retain,
            properties=publish_properties(cfg, message, time.time_ns()),
        )
    except Exception as exc:
        # publish() raises on a message the client cannot send at all — a topic
        # carrying a wildcard, an un-encodable payload — which no retry fixes, so
        # it is dropped and counted rather than returned to the head for ever.
        callbacks.counters["failed"] += 1
        callbacks.counters["dropped_undeliverable"] += 1
        influxdb3_local.warn(
            f"[{task_id}] Publish to '{message.topic}' rejected, dropped: "
            f"{describe_error(exc, cfg['enable_full_logging'])}"
        )
        return True

    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        callbacks.counters["failed"] += 1
        return False

    callbacks.counters["published"] += 1
    if info.is_published():
        callbacks.counters["confirmed"] += 1
    else:
        callbacks.unconfirmed[(callbacks.generation, info.mid)] = message
        callbacks.unconfirmed_bytes += message.size
    return True


def pump(state: PublisherState, seconds: float) -> None:
    """Run the network loop for a bounded time, which is what collects acks."""
    client = state.client
    if client is None:
        return
    deadline = time.monotonic() + max(0.0, seconds)
    while True:
        try:
            client.loop(timeout=0.01)
        except Exception:
            state.callbacks.disconnected = True
            return
        if time.monotonic() >= deadline:
            return


def drain(influxdb3_local, state: PublisherState, cfg: dict, budget: float, task_id: str):
    """Hand over what the buffer holds, within a time budget.

    Nothing goes to the client while it is disconnected, and nothing past the
    in-flight window: at QoS >= 1 a message beyond it sits in paho's own queue,
    which honours neither ``max_message_age`` nor ``drop_policy`` nor the byte
    budget, and is invisible to the counters.
    """
    if not state.client_lock.acquire(blocking=False):
        return
    try:
        deadline = time.monotonic() + budget
        callbacks = state.callbacks
        # Reclaim anything the broker refused with 0x97 back to the head of the
        # buffer, in the order it was refused, before handing over more.
        if callbacks.quota_returned:
            with state.queue_lock:
                while callbacks.quota_returned:
                    message = callbacks.quota_returned.pop()
                    state.pending.appendleft(message)
                    state.pending_bytes += message.size
        while True:
            client = ensure_client(influxdb3_local, state, cfg, task_id)
            if client is None:
                return
            callbacks = state.callbacks
            now_ns = time.time_ns()

            while state.pending:
                if len(callbacks.unconfirmed) >= cfg["max_inflight"]:
                    break
                if callbacks.unconfirmed_bytes >= cfg["max_unconfirmed_bytes"]:
                    break
                if callbacks.quota_refused:
                    break
                with state.queue_lock:
                    message = state.pending.popleft()
                    state.pending_bytes -= message.size
                if cfg["max_message_age"] and message_age_seconds(message, now_ns) > cfg["max_message_age"]:
                    callbacks.counters["dropped_age"] += 1
                    continue
                if not hand_over(influxdb3_local, state, client, cfg, message, task_id):
                    with state.queue_lock:
                        state.pending.appendleft(message)
                        state.pending_bytes += message.size
                    break

            pump(state, 0.01)

            if callbacks.quota_refused:
                callbacks.quota_refused = False
                influxdb3_local.warn(
                    f"[{task_id}] Broker refused with 0x97 quota exceeded; "
                    f"{len(state.pending)} messages wait for the next invocation."
                )
                return
            if not state.pending:
                # The holder re-checks before letting go, so rows queued by
                # another invocation a moment ago do not wait for the next one.
                pump(state, min(0.01, max(0.0, deadline - time.monotonic())))
                if not state.pending:
                    return
            if time.monotonic() >= deadline:
                return
    finally:
        state.client_lock.release()


# --- rows to messages -------------------------------------------------------


def payload_values(row: dict, cfg: dict) -> dict:
    """The columns of one row that reach the payload, empties left out.

    A column that is empty on a row does not appear in that row's payload:
    a WAL row simply has no such key while a query fills the gap with ``None``,
    and without this rule the same logical row would be published differently
    by the two triggers.
    """
    selected = cfg["payload_fields"]
    values = {}
    for key, value in row.items():
        if key == "time":
            continue
        if selected and key not in selected:
            continue
        if value is None or value == "":
            continue
        values[key] = value
    return values


def build_messages(influxdb3_local, state, cfg, table, rows, task_id) -> list:
    """Turn rows of one table into the messages they publish as."""
    messages: list = []
    counters = state.callbacks.counters
    batches: dict = {}

    for row in rows:
        if not row_passes(row, cfg):
            continue
        # A failed schema read raises rather than returning a half-built page:
        # the scheduled caller retries the whole page, the write caller counts
        # the batch it cannot build — neither drops the tail of a page silently.
        schema = known_schema(influxdb3_local, state, table, row)

        time_ns = row.get("time")
        if isinstance(time_ns, datetime):
            time_ns = int(time_ns.timestamp() * 1_000_000_000)
        values = payload_values(row, cfg)
        if not values:
            continue

        # Placeholders resolve against the whole row: payload_fields chooses
        # what the message carries, not what the topic may refer to.
        substitutions = {
            key: value
            for key, value in row.items()
            if key != "time" and value is not None and value != ""
        }
        substitutions["_table"] = table

        if cfg["layout"] == "field":
            for key, value in values.items():
                if key in schema["tags"]:
                    continue
                per_field = dict(substitutions)
                per_field["_field"] = key
                per_field["_value"] = value
                topic, refusal = render_topic(
                    cfg["topic"], per_field, cfg["topic_missing_value"]
                )
                if topic is None:
                    counters["failed"] += 1
                    influxdb3_local.warn(f"[{task_id}] Field skipped: {refusal}.")
                    continue
                if "_field" not in placeholder_names(cfg["topic"]):
                    if any(ch in key for ch in TOPIC_FORBIDDEN):
                        counters["failed"] += 1
                        influxdb3_local.warn(
                            f"[{task_id}] Field '{key}' skipped: its name is not a "
                            f"valid topic level."
                        )
                        continue
                    topic = f"{topic}/{key}"
                messages.append(
                    make_message(str(value), topic, cfg, time_ns, counters, influxdb3_local, task_id)
                )
            continue

        topic, refusal = render_topic(cfg["topic"], substitutions, cfg["topic_missing_value"])
        if topic is None:
            counters["failed"] += 1
            influxdb3_local.warn(f"[{task_id}] Row skipped: {refusal}.")
            continue

        payload = encode_payload(table, values, substitutions, schema, time_ns, cfg)
        if payload is None:
            continue

        if cfg["layout"] == "batch":
            batches.setdefault(topic, []).append((payload, time_ns))
        else:
            messages.append(
                make_message(payload, topic, cfg, time_ns, counters, influxdb3_local, task_id)
            )

    for topic, parts in batches.items():
        messages.extend(
            make_batch_messages(topic, parts, cfg, counters, influxdb3_local, task_id)
        )
    return [message for message in messages if message is not None]


def encode_payload(table, values, substitutions, schema, time_ns, cfg):
    """Encode one row in the configured payload format.

    ``values`` is what ``payload_fields`` selected; ``substitutions`` is the
    whole row, which is what a template interpolates against — selecting the
    columns a JSON payload carries should not decide what a template may name.
    """
    if cfg["payload_format"] == "lineprotocol":
        tags = {key: value for key, value in values.items() if key in schema["tags"]}
        fields = {key: value for key, value in values.items() if key not in schema["tags"]}
        return encode_line_protocol(table, tags, fields, time_ns, schema["types"])
    if cfg["payload_format"] == "template":
        # `{time}` is offered to a template but not to a topic: a topic per
        # timestamp is a new topic for every row, which no consumer can
        # subscribe to usefully and which a retaining broker stores for ever.
        if time_ns is not None:
            substitutions = dict(substitutions)
            substitutions["time"] = format_timestamp(time_ns, cfg["time_format"])
        return render_template(
            cfg["payload_template"], substitutions, cfg["topic_missing_value"]
        )
    # `time` reaches a JSON payload whenever payload_fields selects it, which
    # the empty default does.
    with_time = not cfg["payload_fields"] or "time" in cfg["payload_fields"]
    return encode_json(values, time_ns, cfg["time_format"], with_time)


def make_message(payload, topic, cfg, time_ns, counters, influxdb3_local, task_id):
    """Wrap an encoded payload, refusing one that cannot fit a packet."""
    encoded = payload.encode() if isinstance(payload, str) else payload
    if len(encoded) + len(topic.encode()) > cfg["max_payload_bytes"]:
        counters["dropped_oversized"] += 1
        influxdb3_local.warn(
            f"[{task_id}] Message to '{topic}' is {len(encoded)} bytes, "
            f"over max_payload_bytes; dropped."
        )
        return None
    return Message(topic, encoded, cfg["qos"], cfg["retain"], time_ns)


def make_batch_messages(topic, parts, cfg, counters, influxdb3_local, task_id) -> list:
    """Join a topic's rows into as many messages as the size limit allows.

    A ``batch`` message is as old as its newest row, so one stale row does not
    discard the fresh ones beside it.
    """
    separator = "," if cfg["payload_format"] == "json" else "\n"
    # Sizes are in bytes, as the per-message limit is: a chunk that fits in
    # characters can still overflow once multi-byte UTF-8 is encoded.
    budget = cfg["max_payload_bytes"] - len(topic.encode()) - 2
    messages: list = []
    chunk: list = []
    chunk_bytes = 0
    newest = None

    def flush(rows, time_ns):
        if not rows:
            return
        body = separator.join(rows)
        if cfg["payload_format"] == "json":
            body = f"[{body}]"
        messages.append(
            make_message(body, topic, cfg, time_ns, counters, influxdb3_local, task_id)
        )

    for payload, time_ns in parts:
        size = len(payload.encode()) + len(separator)
        if chunk and chunk_bytes + size > budget:
            flush(chunk, newest)
            chunk, chunk_bytes, newest = [], 0, None
        chunk.append(payload)
        chunk_bytes += size
        if time_ns is not None and (newest is None or time_ns > newest):
            newest = time_ns
    flush(chunk, newest)
    return messages


# --- scheduled reading ------------------------------------------------------


def to_nanos(moment: datetime) -> int:
    """Integer nanoseconds for a UTC datetime."""
    return int(moment.timestamp() * 1_000_000_000)


def select_columns(cfg: dict, schema: dict) -> str:
    """The column list to read: what the message needs and nothing more.

    ``SELECT *`` is avoided deliberately — it fails outright on a system table
    and reads columns no message would carry.
    """
    wanted = {"time"}
    wanted.update(schema["tags"])
    if cfg["payload_fields"]:
        wanted.update(cfg["payload_fields"])
    else:
        wanted.update(schema["types"])
    wanted.update(placeholder_names(cfg["topic"]))
    wanted.update(placeholder_names(cfg["payload_template"]))
    wanted.update(column for column, _, _ in cfg["filters"])
    wanted.update(cfg["allow_values"])
    wanted.update(cfg["deny_values"])
    # `time` is always selected and is never checked against the schema:
    # `get_schema` reports the tags and fields of a table and leaves the
    # timestamp out, so filtering it through that list would drop it from the
    # statement — and a row without its timestamp has no cursor and no payload
    # time.
    known = set(schema["columns"])
    columns = ["time"] + sorted(
        name for name in wanted if name != "time" and name in known
    )
    return ", ".join(quote_identifier(name) for name in columns)


def read_page(
    influxdb3_local, cfg, table, columns, lower_ns, upper_ns, limit, descending=False
):
    """Read one page of a table between two timestamps, filters pushed down.

    The bounds are integers the plugin computed, so they are written into the
    statement; every value that came from configuration or a request is bound
    as a parameter instead.
    """
    clauses, params = filter_clauses(cfg)
    where = [
        f"time > to_timestamp_nanos({int(lower_ns)})",
        f"time <= to_timestamp_nanos({int(upper_ns)})",
    ]
    where.extend(clauses)
    order = "DESC" if descending else "ASC"
    query = (
        f"SELECT {columns} FROM {quote_identifier(table)} "
        f"WHERE {' AND '.join(where)} ORDER BY time {order} LIMIT {int(limit)}"
    )
    return influxdb3_local.query(query, params)


def read_timestamp_group(influxdb3_local, cfg, table, columns, time_ns, limit):
    """Read one whole timestamp group, up to ``max_group_rows`` plus one."""
    clauses, params = filter_clauses(cfg)
    where = [f"time = to_timestamp_nanos({int(time_ns)})"]
    where.extend(clauses)
    query = (
        f"SELECT {columns} FROM {quote_identifier(table)} "
        f"WHERE {' AND '.join(where)} LIMIT {int(limit) + 1}"
    )
    return influxdb3_local.query(query, params)


def row_time_ns(row: dict):
    """The row's ``time`` as integer nanoseconds."""
    value = row.get("time")
    if isinstance(value, datetime):
        return int(value.timestamp() * 1_000_000_000)
    return int(value) if value is not None else None


def page_on_group_boundary(influxdb3_local, cfg, table, columns, rows, limit, task_id):
    """Trim a page so it ends on a timestamp boundary, or take the group whole.

    The cursor is a timestamp, so a page stopping inside a group would advance
    it past rows that were never published. A page that is entirely one
    timestamp is re-read whole instead: dropping its last group would drop the
    page, leave the cursor where it was and never advance at all — and taking
    the page as the group would skip whatever the limit cut off.

    The caller reads the new cursor off the last row returned, which works in
    both directions and for both cases.
    """
    if len(rows) < limit:
        return rows

    first = row_time_ns(rows[0])
    last = row_time_ns(rows[-1])
    if first != last:
        return [row for row in rows if row_time_ns(row) != last]

    group = read_timestamp_group(
        influxdb3_local, cfg, table, columns, last, cfg["max_group_rows"]
    )
    if len(group) > cfg["max_group_rows"]:
        influxdb3_local.error(
            f"[{task_id}] Timestamp {last} in '{table}' carries more rows than "
            f"max_group_rows={cfg['max_group_rows']}; the surplus is dropped and the "
            f"cursor advances past it. Raise max_group_rows to publish the whole group."
        )
        group = group[: cfg["max_group_rows"]]
    return group


def publish_scheduled(influxdb3_local, state, cfg, task_id) -> dict:
    """Work through each source table's backlog within ``tick_budget``.

    The database is the buffer: while the client cannot take anything, the tick
    reads nothing and leaves the watermark where it is, so an outage costs
    memory nothing and loses nothing. A page is taken whole and the watermark
    follows it only once it has drained, which is what keeps a timestamp group
    larger than the in-flight window from republishing for ever.
    """
    summary = {"pages": 0, "rows": 0, "tables": 0, "truncated": False}
    if not state.client_lock.acquire(blocking=False):
        state.callbacks.counters["skipped_reads"] += 1
        return summary
    try:
        return read_and_publish(influxdb3_local, state, cfg, summary, task_id)
    finally:
        state.client_lock.release()


def read_and_publish(influxdb3_local, state, cfg, summary: dict, task_id: str) -> dict:
    """The body of a tick, with the client already held."""
    deadline = time.monotonic() + cfg["tick_budget"]
    now = datetime.now(timezone.utc)

    for table in cfg["source_measurements"]:
        summary["tables"] += 1
        while True:
            if time.monotonic() >= deadline:
                summary["truncated"] = True
                return summary
            if state.pending:
                drain(influxdb3_local, state, cfg, max(0.0, deadline - time.monotonic()), task_id)
                if state.pending:
                    return summary

            client = ensure_client(influxdb3_local, state, cfg, task_id)
            if client is None:
                state.callbacks.counters["skipped_reads"] += 1
                influxdb3_local.info(
                    f"[{task_id}] Broker unavailable; '{table}' not read and the "
                    f"watermark left where it is."
                )
                return summary
            if state.callbacks.unconfirmed_bytes >= cfg["max_unconfirmed_bytes"]:
                state.callbacks.counters["skipped_reads"] += 1
                return summary

            upper_ns = to_nanos(now - timedelta(seconds=cfg["settle_delay"]))
            lower_ns = state.watermark.get(table)
            if lower_ns is None:
                lower_ns = upper_ns
                state.watermark[table] = lower_ns
                influxdb3_local.info(
                    f"[{task_id}] Cold start for '{table}'; publishing from "
                    f"{format_timestamp(lower_ns, 'rfc3339')} onwards."
                )
                break

            if cfg["max_message_age"]:
                floor_ns = to_nanos(now - timedelta(seconds=cfg["max_message_age"]))
                if lower_ns < floor_ns:
                    influxdb3_local.warn(
                        f"[{task_id}] Watermark for '{table}' older than "
                        f"max_message_age; skipping "
                        f"{format_timestamp(lower_ns, 'rfc3339')} .. "
                        f"{format_timestamp(floor_ns, 'rfc3339')}."
                    )
                    lower_ns = floor_ns
                    state.watermark[table] = floor_ns

            if lower_ns >= upper_ns:
                break

            try:
                schema = cached_schema(state, table) or cache_schema(
                    state, table, table_schema(influxdb3_local, table, refresh=True)
                )
            except Exception as exc:
                influxdb3_local.warn(
                    f"[{task_id}] '{table}' not readable yet: "
                    f"{describe_error(exc, cfg['enable_full_logging'])}"
                )
                break

            columns = select_columns(cfg, schema)
            rows = read_page(
                influxdb3_local, cfg, table, columns, lower_ns, upper_ns, cfg["max_messages"]
            )
            if not rows:
                state.watermark[table] = upper_ns
                break

            rows = page_on_group_boundary(
                influxdb3_local, cfg, table, columns, rows, cfg["max_messages"], task_id
            )
            if not rows:
                break

            page_end = row_time_ns(rows[-1])
            try:
                messages = build_messages(influxdb3_local, state, cfg, table, rows, task_id)
            except Exception as exc:
                # The schema read failed mid-page; drop the partial page and leave
                # the watermark so the whole page is retried on the next tick.
                influxdb3_local.warn(
                    f"[{task_id}] '{table}' page not built, retrying next tick: "
                    f"{describe_error(exc, cfg['enable_full_logging'])}"
                )
                return summary
            queue_messages(state, cfg, messages, state.callbacks.counters)
            # The page is in the buffer now, so the watermark advances with it:
            # the buffer, drained across ticks, is what delivers it, and a page
            # larger than one tick's budget is never re-read from the old mark.
            state.watermark[table] = max(lower_ns, page_end)
            summary["pages"] += 1
            summary["rows"] += len(rows)
            drain(influxdb3_local, state, cfg, max(0.0, deadline - time.monotonic()), task_id)

            if state.pending:
                return summary

    return summary


# --- backfill ---------------------------------------------------------------


def parse_timestamp_argument(value, name: str):
    """Read an RFC 3339 timestamp into integer nanoseconds.

    The fractional second is read as digits rather than handed to
    ``datetime``, which holds microseconds and would silently drop the last
    three. That matters because ``resume_from`` is ``reached + 1ns``: parsed
    through a ``datetime`` it comes back as ``reached`` itself, and the row on
    the seam is published a second time.
    """
    if value in (None, ""):
        return None
    text = str(value).strip()
    match = TIMESTAMP_PATTERN.match(text)
    if not match:
        raise ValueError(f"{name} is not an RFC 3339 timestamp: {value!r}")
    seconds, fraction, zone = match.groups()
    zone = "+00:00" if zone in (None, "", "Z", "z") else zone
    try:
        moment = datetime.fromisoformat(f"{seconds}{zone}")
    except ValueError as exc:
        raise ValueError(f"{name} is not an RFC 3339 timestamp: {value!r}") from exc
    nanos = int(moment.timestamp()) * 1_000_000_000
    if fraction:
        nanos += int(fraction[:9].ljust(9, "0"))
    return nanos


def run_backfill(influxdb3_local, state, cfg, table, task_id) -> dict:
    """Publish a time range in pages, leaving the scheduled watermark alone."""
    descending = cfg["backfill_order"] == "newest_first"
    start_ns = parse_timestamp_argument(cfg["backfill_start"], "backfill_start")
    end_ns = parse_timestamp_argument(cfg["backfill_end"], "backfill_end")
    if start_ns is None:
        start_ns = 0
    if end_ns is None:
        end_ns = time.time_ns()

    lower_ns, upper_ns = start_ns - 1, end_ns - 1
    published = 0
    confirmed_through = None
    complete = True
    cap = cfg["max_backfill_messages"]
    opening = dict(state.callbacks.counters)

    if lower_ns >= upper_ns:
        # An inverted or empty range is nothing to publish, not an error — and
        # the server refuses such a predicate outright rather than returning no
        # rows, so the query must not be issued at all.
        influxdb3_local.info(
            f"[{task_id}] Range is empty: backfill_start is at or after "
            f"backfill_end; nothing to publish."
        )
        return {
            "status": "ok",
            "table": table,
            "published": 0,
            "confirmed": 0,
            "dropped": 0,
            "reached": None,
            "resume_from": cfg["backfill_end"] if descending else cfg["backfill_start"],
            "complete": True,
        }
    next_progress_log = time.monotonic() + PROGRESS_LOG_INTERVAL
    schema = cache_schema(state, table, table_schema(influxdb3_local, table))
    columns = select_columns(cfg, schema)

    while True:
        if cap and published >= cap:
            # The cap is a per-call budget, not a sign the range is done; one
            # cheap read tells an exhausted range from a stop with rows to come.
            leftover = read_page(
                influxdb3_local, cfg, table, columns, lower_ns, upper_ns, 1,
                descending=descending,
            )
            if leftover:
                complete = False
                influxdb3_local.info(
                    f"[{task_id}] Reached max_backfill_messages={cap}; answer "
                    f"carries resume_from."
                )
            break

        limit = cfg["max_messages"]
        if cap:
            # Bound the page by the remaining budget so the cap holds per call,
            # not only between pages.
            limit = min(limit, cap - published)
        rows = read_page(
            influxdb3_local,
            cfg,
            table,
            columns,
            lower_ns,
            upper_ns,
            limit,
            descending=descending,
        )
        if not rows:
            break

        rows = page_on_group_boundary(
            influxdb3_local, cfg, table, columns, rows, limit, task_id
        )
        if not rows:
            break

        messages = build_messages(influxdb3_local, state, cfg, table, rows, task_id)
        queue_messages(state, cfg, messages, state.callbacks.counters)
        drain(influxdb3_local, state, cfg, cfg["publish_timeout"], task_id)
        published += len(messages)

        if state.pending or state.client is None:
            complete = False
            break

        # The page has drained, so its last timestamp is confirmed through.
        confirmed_through = row_time_ns(rows[-1])
        if time.monotonic() >= next_progress_log:
            # A long range outlasts the caller's HTTP timeout; the plugin
            # finishes regardless, so the log is where progress is visible.
            next_progress_log = time.monotonic() + PROGRESS_LOG_INTERVAL
            influxdb3_local.info(
                f"[{task_id}] Backfill of '{table}': {published} messages published, "
                f"reached {format_timestamp(confirmed_through, 'rfc3339')}."
            )
        if descending:
            upper_ns = confirmed_through - 1
            if upper_ns < start_ns:
                break
        else:
            lower_ns = confirmed_through
            if lower_ns >= end_ns - 1:
                break

    drain(influxdb3_local, state, cfg, cfg["publish_timeout"], task_id)
    if state.pending:
        complete = False

    reached = (
        format_timestamp(confirmed_through, "rfc3339")
        if confirmed_through is not None
        else None
    )
    if confirmed_through is None:
        resume_from = cfg["backfill_end"] if descending else cfg["backfill_start"]
    elif descending:
        resume_from = format_timestamp(confirmed_through, "rfc3339")
    else:
        resume_from = format_timestamp(confirmed_through + 1, "rfc3339")

    # The answer describes this call, so the counters are reported as the
    # difference from where they stood when it began: they are the trigger's
    # lifetime totals and carry across every client the state has built.
    counters = state.callbacks.counters
    return {
        "status": "ok" if complete else "aborted",
        "table": table,
        "published": published,
        "confirmed": max(0, counters["confirmed"] - opening["confirmed"]),
        "dropped": max(0, sum(
            counters[name] - opening[name]
            for name in (
                "dropped_policy", "dropped_age",
                "dropped_undeliverable", "dropped_oversized",
            )
        )),
        "reached": reached,
        "resume_from": resume_from,
        "complete": complete,
    }


# --- statistics -------------------------------------------------------------


def report(influxdb3_local, state: PublisherState, cfg: dict, task_id: str) -> None:
    """Log a summary and write the counters, at most once per ``stats_interval``."""
    counters = state.callbacks.counters
    now = time.monotonic()
    if now < state.next_stats:
        return
    # `0` writes no measurement but keeps the log summary, which is the setting
    # for an operator who does not want the plugin writing into their database.
    interval = cfg["stats_interval"] or DEFAULT_STATS_INTERVAL
    state.next_stats = now + interval

    influxdb3_local.info(
        f"[{task_id}] published={counters['published']} "
        f"confirmed={counters['confirmed']} failed={counters['failed']} "
        f"queued={len(state.pending)} ({state.pending_bytes} bytes) "
        f"unconfirmed={len(state.callbacks.unconfirmed)} "
        f"reconnects={counters['reconnects']}"
    )

    if cfg["stats_interval"] <= 0:
        return

    try:
        address = parse_broker_url(cfg["broker_url"])
        line = LineBuilder(STATS_MEASUREMENT)
        line.tag("broker", f"{address['host']}:{address['port']}")
        line.tag("instance_name", cfg["instance_name"])
        line.string_field("client_id", state.client_id)
        for name, value in counters.items():
            line.int64_field(name, int(value))
        line.int64_field("queue_depth", len(state.pending))
        line.int64_field("queue_bytes", state.pending_bytes)
        line.int64_field("unconfirmed", len(state.callbacks.unconfirmed))
        influxdb3_local.write(line)
    except Exception as exc:
        influxdb3_local.warn(
            f"[{task_id}] Statistics not written: "
            f"{describe_error(exc, cfg['enable_full_logging'])}"
        )


# --- entry points -----------------------------------------------------------


def process_writes(influxdb3_local, table_batches: list, args: dict | None = None):
    """Publish the rows of this write, for the tables the trigger names."""
    task_id = str(uuid.uuid4())[:8]
    args = args or {}
    try:
        cfg = load_settings(influxdb3_local, args, task_id)
    except Exception as exc:
        influxdb3_local.error(f"[{task_id}] Configuration error: {exc}")
        return

    state = get_state(influxdb3_local, cfg)
    try:
        validate_placeholders(influxdb3_local, state, cfg, task_id)
        validate_filters(influxdb3_local, state, cfg, task_id)
    except Exception as exc:
        influxdb3_local.error(f"[{task_id}] Configuration error: {exc}")
        return

    try:
        for batch in table_batches:
            table = batch["table_name"]
            if table not in cfg["source_measurements"]:
                continue
            try:
                messages = build_messages(
                    influxdb3_local, state, cfg, table, batch["rows"], task_id
                )
            except Exception as exc:
                # A write batch cannot be re-read, so rows whose schema would not
                # load are counted and logged rather than disappearing.
                state.callbacks.counters["failed"] += len(batch["rows"])
                influxdb3_local.error(
                    f"[{task_id}] '{table}' batch not built, {len(batch['rows'])} "
                    f"rows dropped: {describe_error(exc, cfg['enable_full_logging'])}"
                )
                continue
            queue_messages(state, cfg, messages, state.callbacks.counters)
        drain(influxdb3_local, state, cfg, cfg["pump_budget"], task_id)
        report(influxdb3_local, state, cfg, task_id)
    except Exception as exc:
        influxdb3_local.error(
            f"[{task_id}] Publishing failed: "
            f"{describe_error(exc, cfg['enable_full_logging'])}"
        )
    finally:
        converge(influxdb3_local, state, cfg, task_id)


def process_scheduled_call(influxdb3_local, call_time, args: dict | None = None):
    """Publish what each source table has gained since the previous tick."""
    task_id = str(uuid.uuid4())[:8]
    args = args or {}
    try:
        cfg = load_settings(influxdb3_local, args, task_id)
    except Exception as exc:
        influxdb3_local.error(f"[{task_id}] Configuration error: {exc}")
        return

    state = get_state(influxdb3_local, cfg)
    try:
        validate_placeholders(influxdb3_local, state, cfg, task_id)
        validate_filters(influxdb3_local, state, cfg, task_id)
    except Exception as exc:
        influxdb3_local.error(f"[{task_id}] Configuration error: {exc}")
        return

    try:
        summary = publish_scheduled(influxdb3_local, state, cfg, task_id)
        if summary["truncated"]:
            influxdb3_local.warn(
                f"[{task_id}] Tick spent its budget with data still waiting; "
                f"raise tick_budget or shorten the schedule."
            )
        report(influxdb3_local, state, cfg, task_id)
    except Exception as exc:
        influxdb3_local.error(
            f"[{task_id}] Scheduled publishing failed: "
            f"{describe_error(exc, cfg['enable_full_logging'])}"
        )
    finally:
        converge(influxdb3_local, state, cfg, task_id)


def process_request(
    influxdb3_local,
    query_parameters: dict | None,
    request_headers: dict | None,
    request_body,
    args: dict | None = None,
):
    """Backfill a time range on demand, without touching the watermark."""
    task_id = str(uuid.uuid4())[:8]
    args = args or {}
    try:
        cfg = load_settings(
            influxdb3_local,
            args,
            task_id,
            request_layers=(
                parse_json_body(request_body, BODY_KEYS),
                parse_request_headers(request_headers, HEADER_KEYS),
                parse_query_parameters(query_parameters, QUERY_KEYS),
            ),
        )
    except Exception as exc:
        influxdb3_local.error(f"[{task_id}] Configuration error: {exc}")
        return {"error": str(exc)}, 400

    # A backfill client exists for one request, and its coming and going says
    # nothing about availability, so this trigger leaves the status topic alone.
    cfg["status_topic"] = ""

    # Publishing old rows is the whole of what a backfill does, so the staleness
    # limit is lifted here — and with it the expiry interval derived from it.
    cfg["max_message_age"] = 0.0

    table = cfg["source_measurements"][0]
    if len(cfg["source_measurements"]) > 1:
        influxdb3_local.warn(
            f"[{task_id}] One call backfills one table; using '{table}' and ignoring "
            f"{', '.join(cfg['source_measurements'][1:])}."
        )

    state = get_state(influxdb3_local, cfg)
    try:
        cache_schema(state, table, table_schema(influxdb3_local, table))
        validate_placeholders(influxdb3_local, state, cfg, task_id)
        validate_filters(influxdb3_local, state, cfg, task_id)
    except Exception as exc:
        influxdb3_local.error(f"[{task_id}] Configuration error: {exc}")
        return {"error": str(exc)}, 400

    try:
        answer = run_backfill(influxdb3_local, state, cfg, table, task_id)
        report(influxdb3_local, state, cfg, task_id)
        return answer, 200
    except Exception as exc:
        influxdb3_local.error(
            f"[{task_id}] Backfill failed: "
            f"{describe_error(exc, cfg['enable_full_logging'])}"
        )
        return {"error": describe_error(exc, cfg["enable_full_logging"])}, 500
    finally:
        converge(influxdb3_local, state, cfg, task_id)
