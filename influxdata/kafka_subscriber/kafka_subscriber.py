"""
{
    "plugin_type": ["scheduled"],
    "scheduled_args_config": [
        {
            "name": "config_file_path",
            "example": "kafka_config.toml",
            "description": "Path to TOML configuration file (absolute or relative to PLUGIN_DIR).",
            "required": false
        },
        {
            "name": "bootstrap_servers",
            "example": "kafka1:9092 kafka2:9092",
            "description": "Space-separated list of Kafka broker addresses (host:port).",
            "required": true
        },
        {
            "name": "topics",
            "example": "sensor_data metrics",
            "description": "Space-separated list of Kafka topics to subscribe to.",
            "required": true
        },
        {
            "name": "group_id",
            "example": "influxdb3_consumer",
            "description": "Kafka consumer group ID. Must be unique per consumer group.",
            "required": true
        },
        {
            "name": "format",
            "example": "json",
            "description": "Message format: 'json', 'lineprotocol', or 'text'. Default: 'json'. Note: Protobuf and Avro are not supported.",
            "required": false
        },
        {
            "name": "table_name",
            "example": "sensor_data",
            "description": "InfluxDB table name (measurement) for storing data. Required for 'json' and 'text' formats unless table_name_field is set.",
            "required": false
        },
        {
            "name": "table_name_field",
            "example": "measurement",
            "description": "JSON field name or regex pattern to extract table name from each message. Alternative to static table_name.",
            "required": false
        },
        {
            "name": "tags",
            "example": "location sensor_id",
            "description": "Space-separated tag mappings. JSON: 'room sensor'. Text: 'room=room:([^,]+) sensor=sensor:(\\\\w+)'.",
            "required": false
        },
        {
            "name": "fields",
            "example": "temp:float=temperature hum:int=humidity",
            "description": "Space-separated field mappings. Format: 'name:type=path'. Types: int, uint, float, string, bool.",
            "required": false
        },
        {
            "name": "timestamp_field",
            "example": "timestamp:ms",
            "description": "Timestamp field. JSON: 'field:format'. Text: 'regex:format'. Formats: ns, ms, s, datetime.",
            "required": false
        },
        {
            "name": "offset_commit_policy",
            "example": "on_success",
            "description": "When to commit offsets: 'on_success' (only after successful processing) or 'always' (even on errors). Default: 'on_success'.",
            "required": false
        },
        {
            "name": "auto_offset_reset",
            "example": "earliest",
            "description": "Where to start consuming on first connect: 'earliest' (all messages) or 'latest' (new only). Default: 'earliest'.",
            "required": false
        },
        {
            "name": "security_protocol",
            "example": "SASL_SSL",
            "description": "Security protocol: 'PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'. Default: 'PLAINTEXT'.",
            "required": false
        },
        {
            "name": "sasl_mechanism",
            "example": "PLAIN",
            "description": "SASL mechanism: 'PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'. Required if security_protocol includes SASL.",
            "required": false
        },
        {
            "name": "sasl_username",
            "example": "kafka_user",
            "description": "SASL username. Required with sasl_mechanism.",
            "required": false
        },
        {
            "name": "sasl_password",
            "example": "kafka_password",
            "description": "SASL password. Required with sasl_mechanism.",
            "required": false
        },
        {
            "name": "ssl_ca_cert",
            "example": "certs/ca.crt",
            "description": "Path to CA certificate file for SSL (absolute or relative to PLUGIN_DIR).",
            "required": false
        },
        {
            "name": "ssl_cert",
            "example": "certs/client.crt",
            "description": "Path to client certificate for mutual TLS.",
            "required": false
        },
        {
            "name": "ssl_key",
            "example": "certs/client.key",
            "description": "Path to client private key for mutual TLS.",
            "required": false
        },
        {
            "name": "ssl_key_password",
            "example": "key_password",
            "description": "Password for encrypted client private key.",
            "required": false
        },
        {
            "name": "max_poll_records",
            "example": "500",
            "description": "Maximum number of messages to retrieve per scheduled call. Default: 500. Set to 0 for unlimited.",
            "required": false
        },
        {
            "name": "dlq_topic",
            "example": "sensor_data_dlq",
            "description": "Kafka topic to publish messages that fail parsing or writing (dead-letter queue). When unset, no DLQ topic is used. Original payload, key and error details (as headers) are produced to this topic.",
            "required": false
        },
        {
            "name": "dedup_id_field",
            "example": "event_id",
            "description": "Enables deduplication using a unique message id. Holds only the extractor: JSON a field name (extracted via $.event_id), Text a regex pattern. Only active when no timestamp_field is configured (with a message timestamp InfluxDB already deduplicates). The extracted id is written as a field and checked against existing records.",
            "required": false
        },
        {
            "name": "dedup_id_name",
            "example": "event_id",
            "description": "Name of the field the extracted deduplication id is written under and tracked by. Optional; defaults to 'dedup_id'. Only used with dedup_id_field.",
            "required": false
        },
        {
            "name": "dedup_window",
            "example": "24h",
            "description": "Lookback window for the duplicate check (e.g. '30m', '24h', '7d'). Only used with dedup_id_field. Default: 24h.",
            "required": false
        },
        {
            "name": "max_poll_interval",
            "example": "300",
            "description": "Maximum seconds between scheduled poll cycles before the broker considers the consumer dead (mapped to 'max.poll.interval.ms'). The consumer is reused across invocations to avoid a rebalance every cycle; it stays in the group only while the schedule interval is below this value. Increase it for longer schedule intervals. Default: 300.",
            "required": false
        },
        {
            "name": "enable_full_logging",
            "example": "true",
            "description": "When true, full exception details (messages) are written to logs. When false (default), only the exception type is logged to avoid leaking sensitive values. Default: false.",
            "required": false
        }
    ]
}
"""

import json
import math
import os
import re
import time
import tomllib
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol, runtime_checkable

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)
from jsonpath_ng.ext import parse as jsonpath_parse

# Internal constants
_POLL_TIMEOUT_MS = 1000
_POLL_TIMEOUT_MS_FAST = 100
_MAX_POLL_RECORDS = 500
_DEFAULT_DEDUP_WINDOW = "24h"
_DEFAULT_MAX_POLL_INTERVAL_S = 300
# Fixed field name the extracted deduplication id is written under.
_DEDUP_FIELD_NAME = "dedup_id"

# Cache key for the KafkaManager reused across scheduled invocations
_CACHE_CONSUMER = "kafka_consumer"

_DURATION_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}

# InfluxDB integer field ranges
_INT64_MIN: int = -9223372036854775808
_INT64_MAX: int = 9223372036854775807
_UINT64_MAX: int = 18446744073709551615

# Default True so config/validation errors log in full; set from config
# (default False) once known so runtime errors don't leak values.
_ENABLE_FULL_LOGGING: bool = True


def _exc(e: BaseException) -> str:
    """Return exception detail when full logging is enabled, else the type name."""
    return str(e) if _ENABLE_FULL_LOGGING else type(e).__name__

"""
Helper for batching multiple line protocol builders into a single write.
"""


@runtime_checkable
class _LineBuilderInterface(Protocol):
    def build(self) -> str: ...


class _BatchLines:
    def __init__(self, line_builders: list[_LineBuilderInterface]):
        self._line_builders = list(line_builders)
        self._built: str | None = None

    def build(self) -> str:
        if self._built is None:
            lines = [str(b.build()) for b in self._line_builders]
            if not lines:
                raise ValueError("batch_write received no lines to build")
            self._built = "\n".join(lines)
        return self._built


def add_field_with_type(line, field_key: str, value: Any, field_type: str):
    """Add field to LineBuilder with explicit type conversion.

    Supported types: int, uint, float, string, bool
    """
    if field_type == "int":
        ival: int = int(value)
        if not (_INT64_MIN <= ival <= _INT64_MAX):
            raise ValueError(
                f"int field '{field_key}' out of int64 range "
                f"[{_INT64_MIN}, {_INT64_MAX}]: {ival}"
            )
        line.int64_field(field_key, ival)
    elif field_type == "uint":
        uval: int = int(value)
        if not (0 <= uval <= _UINT64_MAX):
            raise ValueError(
                f"uint field '{field_key}' out of uint64 range "
                f"[0, {_UINT64_MAX}]: {uval}"
            )
        line.uint64_field(field_key, uval)
    elif field_type == "float":
        fval: float = float(value)
        if not math.isfinite(fval):
            raise ValueError(f"float field '{field_key}' is not finite: {fval}")
        line.float64_field(field_key, fval)
    elif field_type == "string":
        line.string_field(field_key, str(value))
    elif field_type == "bool":
        if isinstance(value, str):
            converted = value.lower() in ("true", "t", "1", "yes", "on")
        else:
            converted = bool(value)
        line.bool_field(field_key, converted)
    else:
        raise ValueError(
            f"Unknown field type: {field_type}. Supported: int, uint, float, string, bool"
        )


def convert_timestamp(value: Any, time_format: str) -> int:
    """Convert timestamp to nanoseconds based on format.

    Args:
        value: Timestamp value (int, float, or datetime string)
        time_format: Format specifier (ns, ms, s, datetime)

    Returns:
        Timestamp in nanoseconds

    Raises:
        ValueError: If format is unknown or value cannot be converted
    """
    if time_format == "ns":
        return int(value)
    elif time_format == "ms":
        return int(value) * 1_000_000
    elif time_format == "s":
        return int(value) * 1_000_000_000
    elif time_format == "datetime":
        if isinstance(value, str):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1_000_000_000)
        else:
            raise ValueError(
                f"datetime format requires string value, got {type(value)}"
            )
    else:
        raise ValueError(
            f"Unknown time format: {time_format}. Supported: ns, ms, s, datetime"
        )


def parse_duration_seconds(value: str) -> int:
    """Parse a duration string like '30m', '24h', '7d' into seconds."""
    text = str(value).strip().lower()
    match = re.fullmatch(r"(\d+)\s*([smhd])", text)
    if not match:
        raise ValueError(
            f"Invalid duration '{value}'. Expected '<number><unit>', "
            f"unit one of s, m, h, d (e.g. '30m', '24h')."
        )
    amount, unit = int(match.group(1)), match.group(2)
    if amount <= 0:
        raise ValueError(f"Duration '{value}' must be positive")
    return amount * _DURATION_UNITS[unit]


class KafkaConfig:
    """Configuration loader and validator for Kafka plugin"""

    VALID_TIMESTAMP_FORMATS = {"ns", "ms", "s", "datetime"}
    VALID_FIELD_TYPES = {"int", "uint", "float", "string", "bool"}
    VALID_SECURITY_PROTOCOLS = {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}
    VALID_SASL_MECHANISMS = {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}
    VALID_OFFSET_RESET = {"earliest", "latest"}
    VALID_COMMIT_POLICIES = {"on_success", "always"}

    def __init__(self, influxdb3_local, args: dict[str, str] | None, task_id: str):
        self.influxdb3_local = influxdb3_local
        self.args: dict = args or {}
        self.config: dict[str, Any] = {}
        self.task_id: str = task_id
        self._load_config()

    def _load_config(self):
        """Load configuration from TOML file or command-line arguments"""
        config_file: str | None = self.args.get("config_file_path")

        if config_file:
            self.config = self._load_toml_config(config_file)
        else:
            self.config = self._build_config_from_args()

    @staticmethod
    def _resolve_path(path: str, description: str) -> str:
        """Resolve path - absolute paths used as-is, relative paths resolved from PLUGIN_DIR."""
        if os.path.isabs(path):
            return path

        plugin_dir: str | None = os.environ.get("PLUGIN_DIR")
        if not plugin_dir:
            raise ValueError(
                f"PLUGIN_DIR environment variable not set. "
                f"Required for relative {description} path."
            )
        return os.path.join(plugin_dir, path)

    @staticmethod
    def _validate_topics(topics: list) -> None:
        """Reject regex topic subscriptions (leading '^')."""
        for topic in topics:
            if isinstance(topic, str) and topic.startswith("^"):
                raise ValueError(
                    f"Invalid topic '{topic}': regex topic subscriptions "
                    f"(leading '^') are not allowed"
                )

    @staticmethod
    def _validate_bootstrap_servers(servers: list) -> None:
        """Validate each bootstrap server is in 'host:port' form."""
        for server in servers:
            if not isinstance(server, str) or not re.match(r"^[^,\s]+:\d+$", server):
                raise ValueError(
                    f"Invalid bootstrap server '{server}'. "
                    f"Expected 'host:port' format."
                )

    def _load_toml_config(self, config_file: str) -> dict[str, Any]:
        """Load configuration from TOML file"""
        if not config_file.endswith(".toml"):
            raise ValueError(
                "Invalid config file format: expected a .toml file"
            )

        config_path: str = self._resolve_path(config_file, "configuration file")

        try:
            with open(config_path, "rb") as f:
                config: dict[str, Any] = tomllib.load(f)
        except Exception:
            raise ValueError("Failed to read config file") from None

        self._validate_toml_config(config)
        return config

    def _validate_and_parse_timestamp_field(
        self, timestamp_field: str, field_name: str
    ) -> dict[str, str]:
        """Validate and parse timestamp_field format"""
        if ":" in timestamp_field:
            field_path, time_format = timestamp_field.rsplit(":", 1)
            field_path = field_path.strip()
            time_format = time_format.strip()

            if time_format not in self.VALID_TIMESTAMP_FORMATS:
                raise ValueError(
                    f"Invalid timestamp format in '{field_name}': '{time_format}'. "
                    f"Supported formats: {', '.join(sorted(self.VALID_TIMESTAMP_FORMATS))}"
                )

            return {"field": field_path, "format": time_format}
        else:
            return {"field": timestamp_field.strip(), "format": "ns"}

    def _build_dedup_config(
        self,
        extractor: Any,
        name: Any,
        message_format: str,
        label: str,
    ) -> dict[str, str]:
        """Build the dedup config. `extractor` is the resolved JSONPath (json)
        or regex (text); `name` is the field the id is written under (defaults
        to `_DEDUP_FIELD_NAME`)."""
        if not isinstance(extractor, str) or not extractor.strip():
            raise ValueError(f"'{label}' must be a non-empty string")

        if name is not None and (not isinstance(name, str) or not name.strip()):
            raise ValueError("dedup_id_name must be a non-empty string when set")
        field_name: str = (
            name.strip()
            if isinstance(name, str) and name.strip()
            else _DEDUP_FIELD_NAME
        )

        if message_format == "json":
            return {"field_name": field_name, "path": extractor.strip()}

        pattern = extractor.strip()
        try:
            re.compile(pattern)
        except re.error as e:
            raise ValueError(f"Invalid regex in '{label}': {e}") from None
        return {"field_name": field_name, "pattern": pattern}

    def _validate_toml_config(self, config: dict[str, Any]):
        """Validate that all required configuration parameters are present"""
        if "kafka" not in config:
            raise ValueError("Missing required 'kafka' section in configuration")

        kafka_config: dict[str, Any] = config["kafka"]

        enable_full_logging = kafka_config.get("enable_full_logging")
        if enable_full_logging is not None and not isinstance(
            enable_full_logging, (bool, str)
        ):
            raise ValueError(
                "Parameter 'kafka.enable_full_logging' must be a boolean or string (true/false)"
            )
        global _ENABLE_FULL_LOGGING
        _ENABLE_FULL_LOGGING = str(enable_full_logging).lower() == "true"

        if "bootstrap_servers" not in kafka_config:
            raise ValueError(
                "Missing required parameter 'kafka.bootstrap_servers' in configuration"
            )

        if "topics" not in kafka_config:
            raise ValueError(
                "Missing required parameter 'kafka.topics' in configuration"
            )

        if "group_id" not in kafka_config:
            raise ValueError(
                "Missing required parameter 'kafka.group_id' in configuration"
            )

        # Validate bootstrap_servers is a non-empty list
        servers: list[str] = kafka_config["bootstrap_servers"]
        if not isinstance(servers, list) or len(servers) == 0:
            raise ValueError(
                "Parameter 'kafka.bootstrap_servers' must be a non-empty list"
            )
        self._validate_bootstrap_servers(servers)

        # Validate topics is a non-empty list
        topics: list[str] = kafka_config["topics"]
        if not isinstance(topics, list) or len(topics) == 0:
            raise ValueError("Parameter 'kafka.topics' must be a non-empty list")
        self._validate_topics(topics)

        # Validate security_protocol if present
        security_protocol = kafka_config.get("security_protocol", "PLAINTEXT")
        if security_protocol not in self.VALID_SECURITY_PROTOCOLS:
            raise ValueError(
                f"Invalid security_protocol: {security_protocol}. "
                f"Supported: {', '.join(sorted(self.VALID_SECURITY_PROTOCOLS))}"
            )

        # Validate SASL configuration
        if "SASL" in security_protocol:
            sasl_config = config.get("kafka", {}).get("sasl", {})
            if not sasl_config.get("mechanism"):
                raise ValueError(
                    "SASL mechanism required when security_protocol includes SASL"
                )
            if sasl_config["mechanism"] not in self.VALID_SASL_MECHANISMS:
                raise ValueError(
                    f"Invalid SASL mechanism: {sasl_config['mechanism']}. "
                    f"Supported: {', '.join(sorted(self.VALID_SASL_MECHANISMS))}"
                )
            if not sasl_config.get("username") or not sasl_config.get("password"):
                raise ValueError("SASL username and password required")

        # Validate offset_commit_policy if present
        commit_policy = kafka_config.get("offset_commit_policy", "on_success")
        if commit_policy not in self.VALID_COMMIT_POLICIES:
            raise ValueError(
                f"Invalid offset_commit_policy: {commit_policy}. "
                f"Supported: {', '.join(sorted(self.VALID_COMMIT_POLICIES))}"
            )

        # Validate auto_offset_reset if present
        offset_reset = kafka_config.get("auto_offset_reset", "earliest")
        if offset_reset not in self.VALID_OFFSET_RESET:
            raise ValueError(
                f"Invalid auto_offset_reset: {offset_reset}. "
                f"Supported: {', '.join(sorted(self.VALID_OFFSET_RESET))}"
            )

        # Validate max_poll_records if present
        max_poll_records = kafka_config.get("max_poll_records", _MAX_POLL_RECORDS)
        if not isinstance(max_poll_records, int) or max_poll_records < 0:
            raise ValueError(
                "max_poll_records must be a non-negative integer (0 means unlimited)"
            )

        # Validate max_poll_interval (seconds) if present
        max_poll_interval = kafka_config.get(
            "max_poll_interval", _DEFAULT_MAX_POLL_INTERVAL_S
        )
        if not isinstance(max_poll_interval, int) or max_poll_interval <= 0:
            raise ValueError("max_poll_interval must be a positive integer (seconds)")

        # Validate dedup_window if present (used only with dedup_id_field)
        dedup_window = kafka_config.get("dedup_window", _DEFAULT_DEDUP_WINDOW)
        parse_duration_seconds(dedup_window)

        # Validate dlq_topic if present
        dlq_topic = kafka_config.get("dlq_topic")
        if dlq_topic is not None and (
            not isinstance(dlq_topic, str) or not dlq_topic.strip()
        ):
            raise ValueError("dlq_topic must be a non-empty string when set")

        # Get message format (default to json if not specified)
        message_format: str = kafka_config.get("format", "json")

        # Validate format-specific mapping configuration
        if message_format == "json":
            self._validate_json_mapping(config)
        elif message_format == "text":
            self._validate_text_mapping(config)
        elif message_format == "lineprotocol":
            pass
        else:
            raise ValueError(
                f"Invalid message format: {message_format}. "
                f"Supported formats: json, text, lineprotocol. "
                f"Note: Protobuf and Avro are not supported (require Schema Registry)."
            )

    def _validate_json_mapping(self, config: dict[str, Any]):
        """Validate JSON mapping configuration"""
        if "mapping" not in config or "json" not in config["mapping"]:
            raise ValueError("Missing required 'mapping.json' section in configuration")

        json_mapping: dict[str, Any] = config["mapping"]["json"]

        if "table_name" not in json_mapping and "table_name_field" not in json_mapping:
            raise ValueError(
                "Missing required parameter 'mapping.json.table_name' or "
                "'mapping.json.table_name_field' in configuration"
            )

        if "fields" not in json_mapping or not json_mapping["fields"]:
            raise ValueError(
                "Missing required parameter 'mapping.json.fields' in configuration"
            )

        if "timestamp_field" in json_mapping:
            parsed_timestamp: dict[str, str] = self._validate_and_parse_timestamp_field(
                json_mapping["timestamp_field"], "mapping.json.timestamp_field"
            )
            json_mapping["timestamp_config"] = parsed_timestamp
            del json_mapping["timestamp_field"]

        dedup_id_field = json_mapping.get("dedup_id_field")
        if dedup_id_field is not None:
            json_mapping["dedup"] = self._build_dedup_config(
                dedup_id_field,
                json_mapping.get("dedup_id_name"),
                "json",
                "mapping.json.dedup_id_field",
            )
            json_mapping.pop("dedup_id_field", None)
            json_mapping.pop("dedup_id_name", None)

    def _validate_text_mapping(self, config: dict[str, Any]):
        """Validate text mapping configuration"""
        if "mapping" not in config or "text" not in config["mapping"]:
            raise ValueError("Missing required 'mapping.text' section in configuration")

        text_mapping: dict[str, Any] = config["mapping"]["text"]

        if "table_name" not in text_mapping and "table_name_field" not in text_mapping:
            raise ValueError(
                "Missing required parameter 'mapping.text.table_name' or "
                "'mapping.text.table_name_field' in configuration"
            )

        if "fields" not in text_mapping or not text_mapping["fields"]:
            raise ValueError(
                "Missing required parameter 'mapping.text.fields' in configuration"
            )

        fields: dict[str, list[str]] = text_mapping["fields"]
        for field_name, field_config in fields.items():
            if not isinstance(field_config, list) or len(field_config) != 2:
                raise ValueError(
                    f"Invalid field configuration for 'mapping.text.fields.{field_name}'. "
                    f'Expected format: ["pattern", "type"]'
                )
            pattern, field_type = field_config
            if not isinstance(pattern, str) or not pattern:
                raise ValueError(
                    f"Invalid pattern for 'mapping.text.fields.{field_name}'. "
                    f"Pattern must be a non-empty string."
                )
            if field_type not in self.VALID_FIELD_TYPES:
                raise ValueError(
                    f"Invalid field type '{field_type}' for "
                    f"'mapping.text.fields.{field_name}'. "
                    f"Supported types: {', '.join(sorted(self.VALID_FIELD_TYPES))}"
                )

        if "timestamp_field" in text_mapping:
            parsed_timestamp: dict[str, str] = self._validate_and_parse_timestamp_field(
                text_mapping["timestamp_field"], "mapping.text.timestamp_field"
            )
            text_mapping["timestamp_config"] = parsed_timestamp
            del text_mapping["timestamp_field"]

        dedup_id_field = text_mapping.get("dedup_id_field")
        if dedup_id_field is not None:
            text_mapping["dedup"] = self._build_dedup_config(
                dedup_id_field,
                text_mapping.get("dedup_id_name"),
                "text",
                "mapping.text.dedup_id_field",
            )
            text_mapping.pop("dedup_id_field", None)
            text_mapping.pop("dedup_id_name", None)

    def _build_config_from_args(self) -> dict[str, Any]:
        """Build configuration from command-line arguments"""
        enable_full_logging: bool = (
            str(self.args.get("enable_full_logging", "false")).lower() == "true"
        )
        global _ENABLE_FULL_LOGGING
        _ENABLE_FULL_LOGGING = enable_full_logging

        required_keys: list = ["topics", "bootstrap_servers", "group_id"]

        if self.args.get("format", "json") in ["json", "text"]:
            if not self.args.get("table_name_field"):
                required_keys.append("table_name")

        if not self.args or any(key not in self.args for key in required_keys):
            raise ValueError(
                f"Missing some of the required arguments: {', '.join(required_keys)}"
            )

        # Parse space-separated values into lists
        topics_list: list[str] = self.args.get("topics").split()
        servers_list: list[str] = self.args.get("bootstrap_servers").split()

        self._validate_topics(topics_list)
        self._validate_bootstrap_servers(servers_list)

        # Validate offset_commit_policy
        commit_policy = self.args.get("offset_commit_policy", "on_success")
        if commit_policy not in self.VALID_COMMIT_POLICIES:
            raise ValueError(
                f"Invalid offset_commit_policy: {commit_policy}. "
                f"Supported: {', '.join(sorted(self.VALID_COMMIT_POLICIES))}"
            )

        # Validate auto_offset_reset
        offset_reset = self.args.get("auto_offset_reset", "earliest")
        if offset_reset not in self.VALID_OFFSET_RESET:
            raise ValueError(
                f"Invalid auto_offset_reset: {offset_reset}. "
                f"Supported: {', '.join(sorted(self.VALID_OFFSET_RESET))}"
            )

        # Validate security_protocol
        security_protocol = self.args.get("security_protocol", "PLAINTEXT")
        if security_protocol not in self.VALID_SECURITY_PROTOCOLS:
            raise ValueError(
                f"Invalid security_protocol: {security_protocol}. "
                f"Supported: {', '.join(sorted(self.VALID_SECURITY_PROTOCOLS))}"
            )

        # Build SASL config if provided
        sasl_config: dict = {}
        sasl_mechanism = self.args.get("sasl_mechanism")
        sasl_username = self.args.get("sasl_username")
        sasl_password = self.args.get("sasl_password")

        if sasl_mechanism:
            if sasl_mechanism not in self.VALID_SASL_MECHANISMS:
                raise ValueError(
                    f"Invalid SASL mechanism: {sasl_mechanism}. "
                    f"Supported: {', '.join(sorted(self.VALID_SASL_MECHANISMS))}"
                )
            if not sasl_username or not sasl_password:
                raise ValueError(
                    "Both sasl_username and sasl_password required with sasl_mechanism"
                )
            sasl_config = {
                "mechanism": sasl_mechanism,
                "username": sasl_username,
                "password": sasl_password,
            }

        # Build SSL config if provided
        ssl_config: dict = {}
        ssl_ca_cert = self.args.get("ssl_ca_cert")
        ssl_cert = self.args.get("ssl_cert")
        ssl_key = self.args.get("ssl_key")

        ssl_key_password = self.args.get("ssl_key_password")

        if ssl_ca_cert:
            ssl_config["ca_cert"] = ssl_ca_cert
        if ssl_cert and ssl_key:
            ssl_config["client_cert"] = ssl_cert
            ssl_config["client_key"] = ssl_key
            if ssl_key_password:
                ssl_config["key_password"] = ssl_key_password
        elif ssl_cert or ssl_key:
            raise ValueError(
                "Both ssl_cert and ssl_key must be provided for mutual TLS"
            )

        # Parse max_poll_records (0 means unlimited)
        max_poll_records_str = self.args.get("max_poll_records")
        max_poll_records = (
            int(max_poll_records_str) if max_poll_records_str else _MAX_POLL_RECORDS
        )
        if max_poll_records < 0:
            raise ValueError("max_poll_records must be >= 0 (0 means unlimited)")

        # Parse max_poll_interval (seconds)
        max_poll_interval_str = self.args.get("max_poll_interval")
        max_poll_interval = (
            int(max_poll_interval_str)
            if max_poll_interval_str
            else _DEFAULT_MAX_POLL_INTERVAL_S
        )
        if max_poll_interval <= 0:
            raise ValueError("max_poll_interval must be a positive integer (seconds)")

        # Validate dedup_window (only used with dedup_id_field)
        dedup_window = self.args.get("dedup_window", _DEFAULT_DEDUP_WINDOW)
        parse_duration_seconds(dedup_window)

        dlq_topic = self.args.get("dlq_topic")
        if dlq_topic is not None and not dlq_topic.strip():
            raise ValueError("dlq_topic must be a non-empty string when set")

        return {
            "kafka": {
                "bootstrap_servers": servers_list,
                "topics": topics_list,
                "group_id": self.args.get("group_id"),
                "format": self.args.get("format", "json"),
                "offset_commit_policy": commit_policy,
                "auto_offset_reset": offset_reset,
                "security_protocol": security_protocol,
                "sasl": sasl_config,
                "ssl": ssl_config,
                "max_poll_records": max_poll_records,
                "max_poll_interval": max_poll_interval,
                "dedup_window": dedup_window,
                "dlq_topic": dlq_topic,
                "enable_full_logging": enable_full_logging,
            },
            "mapping": self._build_mapping_from_args(),
        }

    def _build_mapping_from_args(self) -> dict[str, Any]:
        """Build mapping configuration from args (supports JSON and text formats)"""
        message_format: str = self.args.get("format", "json")

        if message_format == "json":
            return self._build_json_mapping_from_args()
        elif message_format == "text":
            return self._build_text_mapping_from_args()
        elif message_format == "lineprotocol":
            return {}
        else:
            raise ValueError(
                f"Unsupported format: {message_format}. "
                f"Use 'json', 'text', or 'lineprotocol'. "
                f"Note: Protobuf and Avro are not supported."
            )

    def _build_json_mapping_from_args(self) -> dict[str, Any]:
        """Build JSON mapping configuration from args"""
        tags_config: dict = {}
        tags_arg: str | None = self.args.get("tags")
        if tags_arg:
            tag_names: list[str] = tags_arg.split(" ")
            for tag_name in tag_names:
                tag_name = tag_name.strip()
                if tag_name:
                    tags_config[tag_name] = f"$.{tag_name}"

        fields_config: dict = {}
        fields_arg: str | None = self.args.get("fields")
        if fields_arg:
            field_specs: list[str] = fields_arg.split(" ")
            for field_spec in field_specs:
                field_spec = field_spec.strip()
                if not field_spec:
                    continue

                if ":" not in field_spec:
                    raise ValueError(
                        f"Invalid field specification: '{field_spec}'. "
                        f"Expected format: 'name:type=jsonpath'"
                    )

                field_name, rest = field_spec.split(":", 1)
                field_name = field_name.strip()

                if "=" not in rest:
                    raise ValueError(
                        f"Invalid field specification: '{field_spec}'. "
                        f"Expected format: 'name:type=jsonpath'"
                    )

                field_type, json_path = rest.split("=", 1)
                field_type = field_type.strip()
                json_path = json_path.strip()

                if field_type not in self.VALID_FIELD_TYPES:
                    raise ValueError(
                        f"Invalid field type '{field_type}' in field specification "
                        f"'{field_spec}'. Supported types: "
                        f"{', '.join(sorted(self.VALID_FIELD_TYPES))}"
                    )

                if field_name and field_type and json_path:
                    fields_config[field_name] = [f"$.{json_path}", field_type]

        timestamp_config: dict | None = None
        timestamp_field_arg: str | None = self.args.get("timestamp_field")
        if timestamp_field_arg:
            if ":" in timestamp_field_arg:
                field_name, time_format = timestamp_field_arg.split(":", 1)
                field_name = field_name.strip()
                time_format = time_format.strip()

                if time_format not in self.VALID_TIMESTAMP_FORMATS:
                    raise ValueError(
                        f"Invalid timestamp format: '{time_format}'. "
                        f"Supported formats: "
                        f"{', '.join(sorted(self.VALID_TIMESTAMP_FORMATS))}"
                    )

                if field_name and time_format:
                    timestamp_config = {
                        "field": f"$.{field_name}",
                        "format": time_format,
                    }
            else:
                raise ValueError(
                    f"Invalid timestamp_field specification: '{timestamp_field_arg}'. "
                    f"Expected format: 'field_name:time_format'"
                )

        json_config: dict[str, Any] = {
            "timestamp_config": timestamp_config,
            "tags": tags_config,
            "fields": fields_config,
        }

        table_name = self.args.get("table_name")
        if table_name:
            json_config["table_name"] = table_name

        table_name_field = self.args.get("table_name_field")
        if table_name_field:
            json_config["table_name_field"] = f"$.{table_name_field}"

        dedup_id_field = self.args.get("dedup_id_field")
        if dedup_id_field:
            json_config["dedup"] = self._build_dedup_config(
                f"$.{dedup_id_field.strip()}",
                self.args.get("dedup_id_name"),
                "json",
                "dedup_id_field",
            )

        return {"json": json_config}

    def _build_text_mapping_from_args(self) -> dict[str, Any]:
        """Build text mapping configuration from args"""
        tags_config: dict = {}
        tags_arg: str | None = self.args.get("tags")
        if tags_arg:
            tag_specs: list[str] = tags_arg.split(" ")
            for tag_spec in tag_specs:
                tag_spec = tag_spec.strip()
                if not tag_spec:
                    continue

                if "=" not in tag_spec:
                    raise ValueError(
                        f"Invalid tag specification: '{tag_spec}'. "
                        f"Expected format: 'name=pattern'"
                    )

                tag_name, pattern = tag_spec.split("=", 1)
                tag_name = tag_name.strip()
                pattern = pattern.strip()

                if tag_name and pattern:
                    tags_config[tag_name] = pattern

        fields_config: dict = {}
        fields_arg = self.args.get("fields")
        if fields_arg:
            field_specs: list = fields_arg.split(" ")
            for field_spec in field_specs:
                field_spec = field_spec.strip()
                if not field_spec:
                    continue

                if ":" not in field_spec:
                    raise ValueError(
                        f"Invalid field specification: '{field_spec}'. "
                        f"Expected format: 'name:type=pattern'"
                    )

                field_name, rest = field_spec.split(":", 1)
                field_name = field_name.strip()

                if "=" not in rest:
                    raise ValueError(
                        f"Invalid field specification: '{field_spec}'. "
                        f"Expected format: 'name:type=pattern'"
                    )

                field_type, pattern = rest.split("=", 1)
                field_type = field_type.strip()
                pattern = pattern.strip()

                if field_type not in self.VALID_FIELD_TYPES:
                    raise ValueError(
                        f"Invalid field type '{field_type}' for field '{field_name}'. "
                        f"Supported types: {', '.join(sorted(self.VALID_FIELD_TYPES))}"
                    )

                if field_name and field_type and pattern:
                    fields_config[field_name] = [pattern, field_type]

        timestamp_config: dict | None = None
        timestamp_field_arg: str | None = self.args.get("timestamp_field")
        if timestamp_field_arg:
            if ":" in timestamp_field_arg:
                pattern, time_format = timestamp_field_arg.rsplit(":", 1)
                pattern = pattern.strip()
                time_format = time_format.strip()

                if time_format not in self.VALID_TIMESTAMP_FORMATS:
                    raise ValueError(
                        f"Invalid timestamp format: '{time_format}'. "
                        f"Supported formats: "
                        f"{', '.join(sorted(self.VALID_TIMESTAMP_FORMATS))}"
                    )

                if pattern and time_format:
                    timestamp_config = {"field": pattern, "format": time_format}
            else:
                raise ValueError(
                    f"Invalid timestamp_field specification: '{timestamp_field_arg}'. "
                    f"Expected format: 'regex:time_format'"
                )

        text_config: dict[str, Any] = {
            "timestamp_config": timestamp_config,
            "tags": tags_config,
            "fields": fields_config,
        }

        table_name = self.args.get("table_name")
        if table_name:
            text_config["table_name"] = table_name

        table_name_field = self.args.get("table_name_field")
        if table_name_field:
            text_config["table_name_field"] = table_name_field

        dedup_id_field = self.args.get("dedup_id_field")
        if dedup_id_field:
            text_config["dedup"] = self._build_dedup_config(
                dedup_id_field,
                self.args.get("dedup_id_name"),
                "text",
                "dedup_id_field",
            )

        return {"text": text_config}

    def get(self, key: str, default: Any = None):
        """Get configuration value by key"""
        return self.config.get(key, default)

    def get_kafka_config(self) -> dict[str, Any]:
        """Get Kafka connection configuration"""
        return self.config.get("kafka")

    def get_mapping_config(self, format_type: str) -> dict[str, Any]:
        """Get mapping configuration for specified format"""
        mapping = self.config.get("mapping")
        if mapping:
            return mapping.get(format_type)
        return None


class KafkaManager:
    """Kafka consumer (+ optional DLQ producer). Reused across invocations so
    the consumer stays in its group and avoids a rebalance every cycle."""

    def __init__(self, config: dict[str, Any], influxdb3_local, task_id: str):
        self.config: dict[str, Any] = config
        self.influxdb3_local = influxdb3_local
        self.task_id: str = task_id
        self.consumer: Consumer | None = None
        self.producer: Producer | None = None  # Lazily created for DLQ
        self.connected: bool = False
        self._pending_messages: list = []  # Messages received during assignment wait
        self.poll_error: bool = False  # Set when a poll fails (auth/cert/transport)
        # DLQ delivery status per (topic, partition, offset): False until the
        # delivery callback confirms it. Reconciliation is scoped to _dlq_produced.
        self._dlq_status: dict[tuple[str, int, int], bool] = {}
        self._dlq_produced: set[tuple[str, int, int]] = set()

    @staticmethod
    def _resolve_path(path: str, description: str) -> str:
        """Resolve path - absolute paths used as-is, relative paths resolved from PLUGIN_DIR."""
        if os.path.isabs(path):
            return path

        plugin_dir: str | None = os.environ.get("PLUGIN_DIR")
        if not plugin_dir:
            raise ValueError(
                f"PLUGIN_DIR environment variable not set. "
                f"Required for relative {description} path."
            )
        return os.path.join(plugin_dir, path)

    @classmethod
    def build_security_config(cls, config: dict[str, Any]) -> dict[str, Any]:
        """Build the bootstrap.servers + security/SASL/SSL part shared by
        the consumer and the DLQ producer."""
        bootstrap_servers = config.get("bootstrap_servers", [])
        servers_str = (
            ",".join(bootstrap_servers)
            if isinstance(bootstrap_servers, list)
            else bootstrap_servers
        )

        client_config: dict[str, Any] = {"bootstrap.servers": servers_str}

        security_protocol = config.get("security_protocol", "PLAINTEXT")
        client_config["security.protocol"] = security_protocol

        if "SASL" in security_protocol:
            sasl_config = config.get("sasl", {})
            client_config["sasl.mechanism"] = sasl_config.get("mechanism", "PLAIN")
            client_config["sasl.username"] = sasl_config.get("username")
            client_config["sasl.password"] = sasl_config.get("password")

        if "SSL" in security_protocol:
            ssl_config = config.get("ssl", {})

            # Pin TLS verification explicitly
            client_config["enable.ssl.certificate.verification"] = True
            client_config["ssl.endpoint.identification.algorithm"] = "https"

            ca_cert = ssl_config.get("ca_cert")
            if ca_cert:
                client_config["ssl.ca.location"] = cls._resolve_path(
                    ca_cert, "CA certificate"
                )

            client_cert = ssl_config.get("client_cert")
            client_key = ssl_config.get("client_key")
            if client_cert and client_key:
                client_config["ssl.certificate.location"] = cls._resolve_path(
                    client_cert, "client certificate"
                )
                client_config["ssl.key.location"] = cls._resolve_path(
                    client_key, "client key"
                )

                key_password = ssl_config.get("key_password")
                if key_password:
                    client_config["ssl.key.password"] = key_password

        return client_config

    def _build_consumer_config(self) -> dict[str, Any]:
        """Build confluent-kafka consumer configuration dictionary"""
        consumer_config: dict[str, Any] = self.build_security_config(self.config)

        group_id = self.config.get("group_id")
        consumer_config.update(
            {
                "group.id": group_id,
                "client.id": f"{group_id}-influxdb3",
                "auto.offset.reset": self.config.get("auto_offset_reset", "earliest"),
                "enable.auto.commit": False,  # Manual commit for offset_commit_policy
            }
        )

        # Reused consumer must stay in the group between polls; raise this for
        # schedule intervals longer than max.poll.interval.ms.
        max_poll_interval_s = self.config.get(
            "max_poll_interval", _DEFAULT_MAX_POLL_INTERVAL_S
        )
        consumer_config["max.poll.interval.ms"] = int(max_poll_interval_s) * 1000

        return consumer_config

    def rebind(self, influxdb3_local, task_id: str) -> None:
        """Refresh per-invocation references when reusing a cached instance."""
        self.influxdb3_local = influxdb3_local
        self.task_id = task_id

    def reset_cycle(self) -> None:
        """Reset per-cycle state before polling (kept across invocations)."""
        self.poll_error = False
        self._dlq_status = {}
        self._dlq_produced = set()

    def _on_dlq_delivery(self, err, msg, source: tuple[str, int, int]) -> None:
        """Per-message DLQ delivery report: records the outcome per source
        offset so only the failed messages are replayed, and logs the reason."""
        self._dlq_status[source] = err is None
        if err is not None:
            hint = ""
            if err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                hint = (
                    " - the DLQ topic does not exist; create it on the broker "
                    "(auto-creation is usually disabled)"
                )
            self.influxdb3_local.error(
                f"[{self.task_id}] DLQ delivery failed for topic "
                f"'{msg.topic()}': {err.str()}{hint}"
            )

    def _ensure_producer(self) -> Producer | None:
        """Lazily create the DLQ producer (reused across invocations)."""
        if self.producer is not None:
            return self.producer
        try:
            producer_config = self.build_security_config(self.config)
            producer_config["client.id"] = (
                f"{self.config.get('group_id')}-influxdb3-dlq"
            )
            self.producer = Producer(producer_config)
        except Exception as e:
            self.influxdb3_local.error(
                f"[{self.task_id}] Failed to create DLQ producer: {_exc(e)}"
            )
            self.producer = None
        return self.producer

    def send_to_dlq(
        self,
        source_topic: str,
        partition: int,
        offset: int,
        key: str | None,
        payload: str,
        error_type: str,
        error_message: str,
    ) -> bool:
        """Enqueue a failed message to the DLQ topic. Returns True if enqueued;
        delivery is confirmed per-message and reconciled in flush_dlq()."""
        dlq_topic: str | None = self.config.get("dlq_topic")
        if not dlq_topic:
            return False

        producer = self._ensure_producer()
        if producer is None:
            return False

        headers = [
            ("source_topic", source_topic.encode("utf-8")),
            ("error_type", error_type.encode("utf-8")),
            ("error_message", error_message.encode("utf-8")[:1024]),
        ]

        # Bind source coords so the delivery report maps back to this offset.
        source = (source_topic, partition, offset)
        on_delivery = lambda err, msg, s=source: self._on_dlq_delivery(err, msg, s)

        try:
            value_bytes = payload.encode("utf-8") if payload is not None else None
            key_bytes = key.encode("utf-8") if key else None
            try:
                producer.produce(
                    dlq_topic,
                    value=value_bytes,
                    key=key_bytes,
                    headers=headers,
                    on_delivery=on_delivery,
                )
            except BufferError:
                # Local queue full - flush and retry once.
                producer.flush(10)
                producer.produce(
                    dlq_topic,
                    value=value_bytes,
                    key=key_bytes,
                    headers=headers,
                    on_delivery=on_delivery,
                )
            # Produced but unconfirmed; the callback flips it on report.
            self._dlq_status[source] = False
            self._dlq_produced.add(source)
            producer.poll(0)
            return True
        except Exception as e:
            self.influxdb3_local.error(
                f"[{self.task_id}] Failed to publish to DLQ topic "
                f"'{dlq_topic}': {_exc(e)}"
            )
            return False

    def flush_dlq(self) -> set[tuple[str, int, int]]:
        """Flush the DLQ producer and return the (topic, partition, offset) keys
        not confirmed delivered (timed out or failed). Empty set = all delivered."""
        if self.producer is None:
            return set()
        try:
            remaining = self.producer.flush(15)
            if remaining > 0:
                self.influxdb3_local.warn(
                    f"[{self.task_id}] {remaining} DLQ messages were not "
                    f"delivered within the flush timeout"
                )
        except Exception as e:
            self.influxdb3_local.error(
                f"[{self.task_id}] Error flushing DLQ producer: {_exc(e)}"
            )
        # Scope to this cycle's keys so a late callback from a prior cycle
        # cannot trigger a spurious replay.
        return {
            key
            for key in self._dlq_produced
            if not self._dlq_status.get(key, False)
        }

    def connect(self) -> bool:
        """Establish connection to Kafka cluster"""
        try:
            consumer_config = self._build_consumer_config()
            topics = self.config.get("topics", [])

            self.influxdb3_local.info(
                f"[{self.task_id}] Connecting to Kafka cluster: "
                f"{consumer_config.get('bootstrap.servers')}"
            )

            self.consumer = Consumer(consumer_config)
            self.consumer.subscribe(topics)

            # Wait for partition assignment
            # First polls trigger the rebalance process
            assignment_timeout = 10  # seconds
            start_time = time.time()
            assignment = None
            while time.time() - start_time < assignment_timeout:
                # Poll to trigger rebalance - save any messages received
                msg = self.consumer.poll(timeout=0.5)
                if msg is not None:
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            self.influxdb3_local.error(
                                f"[{self.task_id}] Error connecting to Kafka"
                            )
                            return False
                    else:
                        self._pending_messages.append(msg)
                assignment = self.consumer.assignment()
                if assignment:
                    self.influxdb3_local.info(
                        f"[{self.task_id}] Partitions assigned: {assignment}"
                    )
                    break

            if not assignment:
                # No assignment within the timeout is treated as a failure
                self.influxdb3_local.error(
                    f"[{self.task_id}] No partitions assigned within "
                    f"{assignment_timeout}s. Check broker availability, "
                    f"credentials, certificates, and topic names."
                )
                return False

            self.connected = True
            self.influxdb3_local.info(
                f"[{self.task_id}] Kafka consumer connected, "
                f"subscribed to topics: {topics}"
            )
            return True

        except (KafkaException, Exception) as e:
            if "SSL" in self.config.get("security_protocol", ""):
                self.influxdb3_local.error(
                    f"[{self.task_id}] Error connecting to Kafka: "
                    f"connection failed (SSL/TLS configured). "
                    f"Check broker address, certificates, and key files."
                )
            else:
                self.influxdb3_local.error(
                    f"[{self.task_id}] Error connecting to Kafka: {_exc(e)}"
                )
            return False

    def _process_message(self, msg) -> dict[str, Any] | None:
        """Process a single Kafka message and return dict or None if invalid"""
        if msg.error():
            error = msg.error()
            if error.code() == KafkaError._PARTITION_EOF:
                return None
            else:
                self.influxdb3_local.error(f"[{self.task_id}] Kafka error: {error}")
                return None

        # Decode value
        try:
            value = (
                msg.value().decode("utf-8", errors="replace") if msg.value() else None
            )
        except Exception:
            value = None

        # Skip empty payloads
        if not value or not value.strip():
            self.influxdb3_local.warn(
                f"[{self.task_id}] Skipping empty message on "
                f"{msg.topic()}:{msg.partition()}"
            )
            return None

        # Decode key
        key = None
        if msg.key():
            try:
                key = msg.key().decode("utf-8")
            except Exception:
                pass

        return {
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "key": key,
            "payload": value,
            "timestamp": msg.timestamp()[1] if msg.timestamp() else None,
        }

    def get_messages(self) -> list[dict[str, Any]]:
        """Retrieve all available messages from subscribed topics"""
        messages: list[dict[str, Any]] = []

        if not self.consumer:
            return messages

        try:
            # First, process any messages received during partition assignment
            for msg in self._pending_messages:
                message_data = self._process_message(msg)
                if message_data:
                    messages.append(message_data)
            self._pending_messages.clear()

            # Then poll for more messages
            poll_timeout = _POLL_TIMEOUT_MS / 1000.0
            max_poll_records = self.config.get("max_poll_records", _MAX_POLL_RECORDS)

            while True:
                # Check limit (0 means unlimited)
                if max_poll_records > 0 and len(messages) >= max_poll_records:
                    break

                msg = self.consumer.poll(timeout=poll_timeout)

                if msg is None:
                    # No more messages
                    break

                if msg.error():
                    # _PARTITION_EOF is a normal end-of-partition marker; any
                    # other error means the poll itself failed (authentication,
                    # certificate, transport). Flag it and stop polling.
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        self.poll_error = True
                        self.influxdb3_local.error(
                            f"[{self.task_id}] Error polling Kafka messages"
                        )
                        break
                    continue

                message_data = self._process_message(msg)
                if message_data:
                    messages.append(message_data)

                # After first message, use shorter timeout to quickly drain
                poll_timeout = _POLL_TIMEOUT_MS_FAST / 1000.0

        except Exception as e:
            self.poll_error = True
            self.influxdb3_local.error(
                f"[{self.task_id}] Error polling Kafka messages: {_exc(e)}"
            )

        return messages

    def commit_offsets(self, override: dict[tuple[str, int], int] | None = None):
        """Commit current offsets. `override` maps (topic, partition) to an
        explicit offset to commit instead of position() - used after a rewind,
        since position() is not reliably updated by an un-consumed seek()."""
        if not self.consumer:
            return

        override = override or {}
        try:
            assignment = self.consumer.assignment()
            if not assignment:
                return

            # When there is no position yet (new/empty topic), bootstrap the
            # offset from the watermark matching auto_offset_reset.
            offset_reset = self.config.get("auto_offset_reset", "earliest")
            offsets_to_commit = []
            for tp in assignment:
                # Commit the explicit replay offset for rewound partitions.
                if (tp.topic, tp.partition) in override:
                    offsets_to_commit.append(
                        TopicPartition(
                            tp.topic,
                            tp.partition,
                            override[(tp.topic, tp.partition)],
                        )
                    )
                    continue
                # First try to get current position
                position = self.consumer.position([tp])
                if position and position[0] and position[0].offset >= 0:
                    offsets_to_commit.append(position[0])
                elif not self.poll_error:
                    # No fetch position yet and the poll was healthy. Bootstrap
                    # an offset only for a group that has never committed one -
                    # if a committed offset already exists, leave it untouched
                    committed = self.consumer.committed([tp], timeout=10)
                    if committed and committed[0] and committed[0].offset >= 0:
                        # An offset already exists for this partition.
                        continue
                    low, high = self.consumer.get_watermark_offsets(tp)
                    bootstrap = low if offset_reset == "earliest" else high
                    if bootstrap >= 0:
                        tp_to_commit = TopicPartition(
                            tp.topic, tp.partition, bootstrap
                        )
                        offsets_to_commit.append(tp_to_commit)
                # else: poll failed and no valid position - skip this partition
                #       so the previously committed offset stays intact.

            if offsets_to_commit:
                # Synchronous: durable before we return and surfaces errors.
                self.consumer.commit(offsets=offsets_to_commit, asynchronous=False)

        except Exception as e:
            self.influxdb3_local.error(
                f"[{self.task_id}] Error committing offsets: {_exc(e)}"
            )

    def seek_offsets(self, offsets: dict[tuple[str, int], int]) -> bool:
        """Rewind the (reused) consumer so the given messages are re-read on the
        next poll - skipping the commit alone is not enough, the fetch position
        has already advanced. `offsets` maps (topic, partition) to the lowest
        offset to reprocess. Returns False if any seek failed (e.g. rebalance)."""
        if not self.consumer or not offsets:
            return True
        all_ok: bool = True
        for (topic, partition), offset in offsets.items():
            try:
                self.consumer.seek(TopicPartition(topic, partition, offset))
            except Exception as e:
                all_ok = False
                self.influxdb3_local.error(
                    f"[{self.task_id}] Failed to rewind {topic}:{partition} to "
                    f"offset {offset} for reprocessing: {_exc(e)}"
                )
        return all_ok

    def close(self):
        """Close consumer and producer. Only called on fatal errors or
        teardown - the instance is otherwise reused across invocations."""
        if self.producer is not None:
            self.flush_dlq()
            self.producer = None

        if self.consumer:
            try:
                self.consumer.close()
                self.influxdb3_local.info(
                    f"[{self.task_id}] Disconnected from Kafka cluster"
                )
            except Exception as e:
                self.influxdb3_local.error(
                    f"[{self.task_id}] Error disconnecting from Kafka: {_exc(e)}"
                )
            finally:
                self.consumer = None
        self.connected = False


class JSONParser:
    """Parse JSON messages and convert to Line Protocol"""

    def __init__(self, mapping_config: dict[str, Any], task_id: str, influxdb3_local):
        self.mapping_config: dict = mapping_config
        self.task_id: str = task_id
        self.influxdb3_local = influxdb3_local
        # Dedup only without a message timestamp (else InfluxDB dedups by tags+time).
        self.dedup: dict | None = mapping_config.get("dedup")
        self.dedup_active: bool = bool(self.dedup) and not mapping_config.get(
            "timestamp_config"
        )
        self._compiled_paths: dict[str, Any] = self._compile_jsonpath_expressions()

    def _compile_jsonpath_expressions(self) -> dict[str, Any]:
        """Pre-compile all JSONPath expressions for performance optimization."""
        compiled: dict[str, Any] = {}

        table_name_field = self.mapping_config.get("table_name_field")
        if table_name_field:
            compiled["table_name"] = jsonpath_parse(table_name_field)

        if self.dedup_active:
            compiled["dedup"] = jsonpath_parse(self.dedup["path"])

        tags_config = self.mapping_config.get("tags", {})
        for tag_key, json_path in tags_config.items():
            compiled[f"tag:{tag_key}"] = jsonpath_parse(json_path)

        fields_config = self.mapping_config.get("fields", {})
        for field_key, field_spec in fields_config.items():
            json_path = field_spec[0]
            compiled[f"field:{field_key}"] = jsonpath_parse(json_path)

        timestamp_config = self.mapping_config.get("timestamp_config")
        if timestamp_config:
            field_path = timestamp_config.get("field")
            if field_path:
                compiled["timestamp"] = jsonpath_parse(field_path)

        return compiled

    def parse(self, payload: str) -> list:
        """Parse JSON payload and return a list of (line, table, dedup_id) tuples."""
        try:
            data: dict = json.loads(payload)

            if isinstance(data, list):
                if len(data) == 0:
                    self.influxdb3_local.warn(
                        f"[{self.task_id}] Empty JSON array received, skipping"
                    )
                    return []

                results: list = []
                for i, item in enumerate(data):
                    if not isinstance(item, dict):
                        self.influxdb3_local.warn(
                            f"[{self.task_id}] Skipping non-object array element "
                            f"at index {i}"
                        )
                        continue
                    try:
                        record = self._parse_object(item)
                        if record:
                            results.append(record)
                    except Exception as e:
                        self.influxdb3_local.warn(
                            f"[{self.task_id}] Error parsing array element {i}: "
                            f"{_exc(e)}"
                        )

                if len(results) == 0:
                    self.influxdb3_local.warn(
                        f"[{self.task_id}] No valid objects in JSON array, skipping"
                    )
                    return []

                return results

            elif isinstance(data, dict):
                record = self._parse_object(data)
                return [record] if record else []

            else:
                raise ValueError(f"Unsupported JSON type: {type(data).__name__}")

        except json.JSONDecodeError as e:
            self.influxdb3_local.error(f"[{self.task_id}] Invalid JSON: {_exc(e)}")
            raise
        except Exception as e:
            self.influxdb3_local.error(f"[{self.task_id}] Error parsing JSON: {_exc(e)}")
            raise

    def _parse_object(self, data: dict[str, Any]):
        """Parse a single JSON object and return (line, table, dedup_id)."""
        table_name: str | None = self._get_table_name(data)
        if not table_name:
            raise ValueError("Could not determine table name")

        line = LineBuilder(table_name)

        self._add_tags(line, data)

        field_count: int = self._add_fields(line, data)

        if field_count == 0:
            raise ValueError("No fields were mapped from JSON data")

        dedup_id: str | None = self._add_dedup_id(line, data)

        timestamp_ns: int | None = self._get_timestamp(data)
        if timestamp_ns is not None:
            line.time_ns(timestamp_ns)

        return line, table_name, dedup_id

    def _add_dedup_id(self, line, data: dict[str, Any]) -> str | None:
        """Extract the dedup id, write it as a field, and return it."""
        if not self.dedup_active:
            return None

        raw: Any = self._get_json_value(data, self.dedup["path"], "dedup")
        if raw is None or str(raw).strip() == "":
            raise ValueError(
                f"Deduplication id field '{self.dedup['field_name']}' "
                f"is missing or empty in message"
            )
        dedup_id: str = str(raw)
        line.string_field(self.dedup["field_name"], dedup_id)
        return dedup_id

    def _get_table_name(self, data: dict[str, Any]) -> str | None:
        """Get table name from config or data"""
        table_name: str | None = self.mapping_config.get("table_name")
        if table_name:
            return table_name

        table_name_field: str | None = self.mapping_config.get("table_name_field")
        if table_name_field:
            return self._get_json_value(data, table_name_field, "table_name")

        return None

    def _add_tags(self, line, data: dict[str, Any]):
        """Add tags to LineBuilder from JSON data"""
        tags_config: dict[str, Any] = self.mapping_config.get("tags", {})

        for tag_key, json_path in tags_config.items():
            value: Any = self._get_json_value(data, json_path, f"tag:{tag_key}")
            if value is not None:
                line.tag(tag_key, str(value))

    def _add_fields(self, line, data: dict[str, Any]) -> int:
        """Add fields to LineBuilder from JSON data. Returns count of fields added."""
        fields_config: dict = self.mapping_config.get("fields", {})
        field_count: int = 0

        if not fields_config:
            raise ValueError(
                "No field mappings configured. Please specify fields in configuration."
            )

        for field_key, field_spec in fields_config.items():
            if not isinstance(field_spec, list) or len(field_spec) != 2:
                raise ValueError(
                    f"Invalid field specification for '{field_key}': {field_spec}. "
                    f'Expected format: ["$.path", "type"]'
                )

            json_path, field_type = field_spec
            value: Any = self._get_json_value(data, json_path, f"field:{field_key}")

            if value is not None:
                try:
                    add_field_with_type(line, field_key, value, field_type)
                except (ValueError, TypeError) as e:
                    raise ValueError(
                        f"Failed to convert field '{field_key}' to {field_type}: {e}"
                    )
                field_count += 1

        return field_count

    def _get_timestamp(self, data: dict[str, Any]) -> int | None:
        """Get timestamp from data with format conversion"""
        timestamp_config: dict | None = self.mapping_config.get("timestamp_config")
        if not timestamp_config:
            return time.time_ns()

        field_path: str = timestamp_config.get("field")
        time_format: str = timestamp_config.get("format", "ns")

        timestamp_value: Any = self._get_json_value(data, field_path, "timestamp")
        if timestamp_value is None:
            raise ValueError(
                f"Configured timestamp field '{field_path}' is missing or null in message"
            )

        try:
            return convert_timestamp(timestamp_value, time_format)
        except Exception as e:
            raise ValueError(
                f"Failed to convert timestamp '{timestamp_value}' "
                f"with format '{time_format}': {e}"
            )

    def _get_json_value(
        self, data: dict[str, Any], path: str, cache_key: str | None = None
    ) -> Any:
        """Get value from JSON using JSONPath notation."""
        try:
            if cache_key and cache_key in self._compiled_paths:
                jsonpath_expr = self._compiled_paths[cache_key]
            else:
                jsonpath_expr = jsonpath_parse(path)

            matches = jsonpath_expr.find(data)
            return matches[0].value if matches else None

        except Exception as e:
            self.influxdb3_local.warn(
                f"[{self.task_id}] Error parsing JSONPath '{path}': {e}"
            )
            return None


class LineProtocolParser:
    """Parse Line Protocol format and convert to LineBuilder"""

    def __init__(self, influxdb3_local, task_id):
        self.influxdb3_local = influxdb3_local
        self.task_id: str = task_id

    def parse(self, payload: str):
        """Parse line protocol and return [(line, measurement, None)]."""
        try:
            payload = payload.strip()

            parts: list[str] = self._split_quoted(
                payload, " ", max_splits=2, skip_empty=True
            )

            if len(parts) < 2:
                raise ValueError("Invalid line protocol format: missing field set")

            measurement_and_tags: str = parts[0]
            fields_str: str = parts[1]
            timestamp_ns: int | None = None
            if len(parts) == 3:
                timestamp_ns = int(parts[2])
                if not (_INT64_MIN <= timestamp_ns <= _INT64_MAX):
                    raise ValueError(
                        f"line protocol timestamp out of int64 range "
                        f"[{_INT64_MIN}, {_INT64_MAX}]: {timestamp_ns}"
                    )

            measurement, tags = self._parse_measurement_and_tags(measurement_and_tags)

            line = LineBuilder(measurement)

            for tag_key, tag_value in tags.items():
                line.tag(tag_key, tag_value)

            fields: dict = self._parse_fields(fields_str)
            if len(fields) == 0:
                raise ValueError("No fields found in line protocol")

            for field_key, (field_value, field_type) in fields.items():
                add_field_with_type(line, field_key, field_value, field_type)

            if timestamp_ns:
                line.time_ns(timestamp_ns)

            return [(line, measurement, None)]

        except Exception as e:
            self.influxdb3_local.error(
                f"[{self.task_id}] Error parsing line protocol: {_exc(e)}"
            )
            raise

    def _split_quoted(
        self, text: str, delimiter: str, max_splits: int = -1, skip_empty: bool = False
    ) -> list[str]:
        """Split text by delimiter, respecting quoted strings."""
        parts: list = []
        current: list = []
        in_quotes: bool = False
        splits_made: int = 0

        for char in text:
            if char == '"':
                backslash_count = 0
                i = len(current) - 1
                while i >= 0 and current[i] == "\\":
                    backslash_count += 1
                    i -= 1

                if backslash_count % 2 == 0:
                    in_quotes = not in_quotes

                current.append(char)
            elif char == delimiter and not in_quotes:
                if 0 < max_splits <= splits_made:
                    current.append(char)
                else:
                    if current or not skip_empty:
                        parts.append("".join(current))
                    current = []
                    splits_made += 1
            else:
                current.append(char)

        if current or not skip_empty:
            parts.append("".join(current))

        return parts

    def _parse_measurement_and_tags(
        self, measurement_and_tags: str
    ) -> tuple[str, dict[str, str]]:
        """Parse measurement and tags from first part of line protocol"""
        parts: list = measurement_and_tags.split(",")
        measurement: str = parts[0]

        tags: dict = {}
        for tag_part in parts[1:]:
            if "=" in tag_part:
                key, value = tag_part.split("=", 1)
                tags[key] = self._unescape_value(value)
            else:
                raise ValueError(
                    f"Invalid line protocol format: tag must be in 'key=value' format, "
                    f"got '{tag_part}'"
                )

        return measurement, tags

    def _parse_fields(self, fields_str: str) -> dict[str, tuple[Any, str]]:
        """Parse fields from line protocol"""
        fields: dict = {}

        field_parts: list = self._split_quoted(fields_str, ",")

        for field_part in field_parts:
            if "=" not in field_part:
                continue

            key, value_str = field_part.split("=", 1)
            key = key.strip()
            value_str = value_str.strip()

            value, field_type = self._parse_field_value(value_str)
            fields[key] = (value, field_type)

        return fields

    def _parse_field_value(self, value_str: str) -> tuple[Any, str]:
        """Parse field value and determine its type"""
        if (
            value_str.startswith('"')
            and value_str.endswith('"')
            and len(value_str) >= 2
        ):
            return self._unescape_value(value_str[1:-1]), "string"

        if value_str.endswith("i"):
            return int(value_str[:-1]), "int"

        if value_str.endswith("u"):
            return int(value_str[:-1]), "uint"

        lower_val: str = value_str.lower()
        if lower_val in ("true", "t", "false", "f"):
            return lower_val in ("true", "t"), "bool"

        try:
            return float(value_str), "float"
        except ValueError:
            raise ValueError(f"Invalid field value: {value_str}")

    def _unescape_value(self, value: str) -> str:
        """Unescape special characters in line protocol values"""
        return (
            value.replace("\\,", ",")
            .replace("\\=", "=")
            .replace("\\ ", " ")
            .replace("\\\\", "\\")
            .replace('\\"', '"')
        )


class TextParser:
    """Parse text messages using individual regex patterns for each field/tag"""

    def __init__(self, mapping_config: dict[str, Any], task_id: str, influxdb3_local):
        self.mapping_config: dict = mapping_config
        self.task_id: str = task_id
        self.influxdb3_local = influxdb3_local
        # Dedup only without a message timestamp.
        self.dedup: dict | None = mapping_config.get("dedup")
        self.dedup_active: bool = bool(self.dedup) and not mapping_config.get(
            "timestamp_config"
        )
        self._compiled_patterns: dict[str, re.Pattern] = self._compile_regex_patterns()

    def _compile_regex_patterns(self) -> dict[str, re.Pattern]:
        """Pre-compile all regex patterns for performance optimization."""
        compiled: dict[str, re.Pattern] = {}

        table_name_field = self.mapping_config.get("table_name_field")
        if table_name_field:
            try:
                compiled["table_name"] = re.compile(table_name_field)
            except re.error as e:
                self.influxdb3_local.warn(
                    f"[{self.task_id}] Invalid regex for table_name_field: {e}"
                )

        if self.dedup_active:
            try:
                compiled["dedup"] = re.compile(self.dedup["pattern"])
            except re.error as e:
                self.influxdb3_local.warn(
                    f"[{self.task_id}] Invalid regex for dedup_id_field: {e}"
                )

        tags_config = self.mapping_config.get("tags", {})
        for tag_key, pattern_str in tags_config.items():
            try:
                compiled[f"tag:{tag_key}"] = re.compile(pattern_str)
            except re.error as e:
                self.influxdb3_local.warn(
                    f"[{self.task_id}] Invalid regex for tag '{tag_key}': {e}"
                )

        fields_config = self.mapping_config.get("fields", {})
        for field_key, pattern_config in fields_config.items():
            pattern_str = pattern_config[0]
            try:
                compiled[f"field:{field_key}"] = re.compile(pattern_str)
            except re.error as e:
                self.influxdb3_local.warn(
                    f"[{self.task_id}] Invalid regex for field '{field_key}': {e}"
                )

        timestamp_config = self.mapping_config.get("timestamp_config")
        if timestamp_config:
            pattern_str = timestamp_config.get("field")
            if pattern_str:
                try:
                    compiled["timestamp"] = re.compile(pattern_str)
                except re.error as e:
                    self.influxdb3_local.warn(
                        f"[{self.task_id}] Invalid regex for timestamp: {e}"
                    )

        return compiled

    def _get_table_name(self, payload: str) -> str | None:
        """Get table name from config or extract from payload using regex"""
        table_name: str | None = self.mapping_config.get("table_name")
        if table_name:
            return table_name

        table_name_field: str | None = self.mapping_config.get("table_name_field")
        if table_name_field:
            return self._extract_value(
                payload, table_name_field, "table_name", "table_name"
            )

        return None

    def parse(self, payload: str):
        """Parse text payload using individual regex patterns"""
        try:
            table_name: str | None = self._get_table_name(payload)
            if not table_name:
                raise ValueError(
                    "Could not determine table name from text mapping configuration"
                )

            line = LineBuilder(table_name)

            tags_config: dict = self.mapping_config.get("tags", {})
            for tag_key, pattern_str in tags_config.items():
                value: str | None = self._extract_value(
                    payload, pattern_str, tag_key, f"tag:{tag_key}"
                )
                if value is not None:
                    line.tag(tag_key, value)

            fields_config: dict = self.mapping_config.get("fields", {})
            field_count: int = 0

            if not fields_config:
                raise ValueError(
                    "No field patterns configured. Please specify fields in configuration."
                )

            for field_key, pattern_config in fields_config.items():
                pattern_str = pattern_config[0]
                field_type = pattern_config[1]

                value = self._extract_value(
                    payload, pattern_str, field_key, f"field:{field_key}"
                )
                if value is not None:
                    try:
                        add_field_with_type(line, field_key, value, field_type)
                    except (ValueError, TypeError) as e:
                        self.influxdb3_local.error(
                            f"[{self.task_id}] Failed to convert field '{field_key}' "
                            f"value '{value}' to type '{field_type}': {_exc(e)}"
                        )
                        raise
                    field_count += 1

            if field_count == 0:
                raise ValueError("No fields were extracted from text message")

            dedup_id: str | None = self._add_dedup_id(line, payload)

            timestamp_ns: int = self._get_timestamp(payload)
            line.time_ns(timestamp_ns)

            return [(line, table_name, dedup_id)]

        except Exception as e:
            self.influxdb3_local.error(f"[{self.task_id}] Error parsing text: {_exc(e)}")
            raise

    def _add_dedup_id(self, line, payload: str) -> str | None:
        """Extract the dedup id, write it as a field, and return it."""
        if not self.dedup_active:
            return None

        value: str | None = self._extract_value(
            payload, self.dedup["pattern"], self.dedup["field_name"], "dedup"
        )
        if value is None or value.strip() == "":
            raise ValueError(
                f"Deduplication id pattern for '{self.dedup['field_name']}' "
                f"did not match message"
            )
        line.string_field(self.dedup["field_name"], value)
        return value

    def _extract_value(
        self, text: str, pattern_str: str, field_name: str, cache_key: str | None = None
    ) -> str | None:
        """Extract value from text using a pre-compiled regex pattern"""
        pattern = self._compiled_patterns.get(cache_key) if cache_key else None
        if pattern is None:
            self.influxdb3_local.warn(
                f"[{self.task_id}] No compiled pattern for '{field_name}' "
                f"(pattern failed to compile at init): {pattern_str}"
            )
            return None

        match = pattern.search(text)
        if not match:
            self.influxdb3_local.warn(
                f"[{self.task_id}] Pattern for '{field_name}' did not match: "
                f"{pattern_str}"
            )
            return None

        if match.groups():
            return match.group(1)
        else:
            return match.group(0)

    def _get_timestamp(self, payload: str) -> int:
        """Extract and convert timestamp from text payload"""
        timestamp_config: dict = self.mapping_config.get("timestamp_config")
        if not timestamp_config:
            return time.time_ns()

        pattern_str: str = timestamp_config.get("field")
        if not pattern_str:
            return time.time_ns()

        timestamp_value: Any = self._extract_value(
            payload, pattern_str, "timestamp", "timestamp"
        )
        if timestamp_value is None:
            raise ValueError(
                f"Configured timestamp pattern '{pattern_str}' did not match message"
            )

        time_format = timestamp_config.get("format", "ns")
        try:
            return convert_timestamp(timestamp_value, time_format)
        except Exception as e:
            raise ValueError(
                f"Failed to convert timestamp '{timestamp_value}' "
                f"with format '{time_format}': {e}"
            )


class KafkaStats:
    """Track and persist Kafka plugin statistics"""

    def __init__(self):
        self.reset()

    def reset(self):
        """Reset all statistics"""
        self.messages_received: int = 0
        self.messages_processed: int = 0
        self.messages_failed: int = 0
        # Track stats per topic-partition: {(topic, partition): {received, processed, failed, offset}}
        self.stats_by_topic_partition: dict = {}
        self.last_message_time: int | None = None
        self.current_topic: str | None = None
        self.current_partition: int | None = None

    def record_message_received(
        self, topic: str, partition: int, offset: int, count: int = 1
    ):
        """Record received message(s)"""
        self.messages_received += count
        self.last_message_time = time.time_ns()
        self.current_topic = topic
        self.current_partition = partition

        key = (topic, partition)
        if key not in self.stats_by_topic_partition:
            self.stats_by_topic_partition[key] = {
                "received": 0,
                "processed": 0,
                "failed": 0,
                "last_offset": -1,
            }

        self.stats_by_topic_partition[key]["received"] += count
        self.stats_by_topic_partition[key]["last_offset"] = max(
            self.stats_by_topic_partition[key]["last_offset"], offset
        )

    def record_message_processed(self, count: int = 1):
        """Record successfully processed message(s)"""
        self.messages_processed += count

        if self.current_topic and self.current_partition is not None:
            key = (self.current_topic, self.current_partition)
            if key in self.stats_by_topic_partition:
                self.stats_by_topic_partition[key]["processed"] += count

    def record_message_failed(self, count: int = 1):
        """Record failed message(s)"""
        self.messages_failed += count

        if self.current_topic and self.current_partition is not None:
            key = (self.current_topic, self.current_partition)
            if key in self.stats_by_topic_partition:
                self.stats_by_topic_partition[key]["failed"] += count

    def get_topic_partition_stats(self) -> dict[tuple[str, int], dict[str, Any]]:
        """Get statistics by topic-partition with calculated success rates"""
        result: dict = {}
        for key, stats in self.stats_by_topic_partition.items():
            total: int = stats["processed"] + stats["failed"]
            success_rate: float = (
                (stats["processed"] / total * 100) if total > 0 else 0.0
            )

            result[key] = {
                "received": stats["received"],
                "processed": stats["processed"],
                "failed": stats["failed"],
                "last_offset": stats["last_offset"],
                "success_rate": round(success_rate, 2),
            }
        return result


def write_stats(
    influxdb3_local,
    stats: KafkaStats,
    bootstrap_servers: str,
    group_id: str,
    task_id: str,
):
    """Write per-topic-partition statistics to kafka_stats table."""
    try:
        topic_partition_stats: dict = stats.get_topic_partition_stats()

        lines = []
        for (topic, partition), data in topic_partition_stats.items():
            line = LineBuilder("kafka_stats")

            line.tag("topic", topic)
            line.tag("partition", str(partition))
            line.tag("consumer_group", group_id)
            line.tag("bootstrap_servers", bootstrap_servers)

            line.int64_field("messages_received", data["received"])
            line.int64_field("messages_processed", data["processed"])
            line.int64_field("messages_failed", data["failed"])
            line.int64_field("last_offset", data["last_offset"])
            line.float64_field("success_rate", data["success_rate"])

            line.time_ns(time.time_ns())
            lines.append(line)

        if lines:
            influxdb3_local.write_sync(_BatchLines(lines), no_sync=True)
            influxdb3_local.info(
                f"[{task_id}] Wrote statistics for {len(topic_partition_stats)} "
                f"topic-partitions to kafka_stats table"
            )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to write statistics: {_exc(e)}")


def write_exception(
    influxdb3_local,
    topic: str,
    partition: int,
    offset: int,
    error_type: str,
    error_message: str,
    task_id: str,
):
    """Write exception to kafka_exceptions table."""
    try:
        line = LineBuilder("kafka_exceptions")
        line.tag("topic", topic)
        line.tag("partition", str(partition))
        line.tag("error_type", error_type)
        line.int64_field("offset", offset)
        line.string_field("error_message", error_message)
        line.time_ns(time.time_ns())

        influxdb3_local.write_sync(line, no_sync=True)
        influxdb3_local.info(
            f"[{task_id}] Wrote exception to kafka_exceptions table: {error_type}"
        )

    except Exception as e:
        influxdb3_local.error(
            f"[{task_id}] Failed to write exception to table: {_exc(e)}"
        )


def quote_identifier(name: str) -> str:
    """Quote a SQL identifier, escaping embedded double quotes."""
    return '"' + str(name).replace('"', '""') + '"'


def query_existing_ids(
    influxdb3_local,
    table: str,
    id_field: str,
    ids: list[str],
    window_seconds: int,
    task_id: str,
) -> set[str]:
    """Return the subset of `ids` already present in `table` within the window.
    Fails open (returns empty set) on query error so messages aren't dropped."""
    if not ids:
        return set()

    placeholders: list[str] = []
    params: dict[str, str] = {}
    for i, value in enumerate(ids):
        param_name = f"dedup_id_{i}"
        placeholders.append(f"${param_name}")
        params[param_name] = value

    cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
    cutoff_iso = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")

    quoted_id = quote_identifier(id_field)
    query = (
        f"SELECT DISTINCT {quoted_id} "
        f"FROM {quote_identifier(table)} "
        f"WHERE time >= '{cutoff_iso}' "
        f"AND {quoted_id} IN ({', '.join(placeholders)})"
    )

    try:
        rows: list = influxdb3_local.query(query, params)
    except Exception as e:
        influxdb3_local.warn(
            f"[{task_id}] Deduplication query failed for table '{table}', "
            f"writing without dedup check: {_exc(e)}"
        )
        return set()

    existing: set[str] = set()
    for row in rows:
        value = row.get(id_field)
        if value is not None:
            existing.add(str(value))
    return existing


def process_scheduled_call(
    influxdb3_local, call_time: datetime, args: dict | None = None
):
    """
    Main plugin entry point - called on schedule by InfluxDB 3 Processing Engine

    Args:
        influxdb3_local: Shared API for InfluxDB operations
        call_time: Timestamp when trigger was called
        args: Trigger arguments
    """
    task_id: str = str(uuid.uuid4())
    manager: KafkaManager | None = None
    # When set, the cached manager is closed and evicted (rebuilt next cycle).
    fatal: bool = False

    if not args:
        influxdb3_local.error(f"[{task_id}] No arguments provided")
        return

    try:
        # Load configuration from cache or parse fresh
        cached_config: dict | None = influxdb3_local.cache.get("kafka_config")
        if cached_config is None:
            config_loader: KafkaConfig = KafkaConfig(influxdb3_local, args, task_id)
            cached_config = {
                "kafka": config_loader.get_kafka_config(),
                "mapping": {
                    "json": config_loader.get_mapping_config("json"),
                    "text": config_loader.get_mapping_config("text"),
                },
            }
            influxdb3_local.cache.put("kafka_config", cached_config, 60 * 60)
            # Config (re)built - drop any stale manager to pick up new settings.
            stale = influxdb3_local.cache.get(_CACHE_CONSUMER)
            if stale is not None:
                stale.rebind(influxdb3_local, task_id)
                stale.close()
                influxdb3_local.cache.delete(_CACHE_CONSUMER)
            influxdb3_local.info(
                f"[{task_id}] Kafka Plugin initialized, "
                f"format: {cached_config['kafka']['format']}"
            )
            # dedup_id_field is ignored when a message timestamp is configured
            # (InfluxDB dedups natively by tags + time); warn so it's not silent.
            init_fmt: str = cached_config["kafka"]["format"]
            init_map: dict = (
                cached_config["mapping"].get(init_fmt) or {}
                if init_fmt in ("json", "text")
                else {}
            )
            if init_map.get("dedup") and init_map.get("timestamp_config"):
                influxdb3_local.warn(
                    f"[{task_id}] dedup_id_field is ignored because "
                    f"timestamp_field is set; deduplication falls back to "
                    f"InfluxDB's native tag+time dedup"
                )

        kafka_config: dict = cached_config["kafka"]
        global _ENABLE_FULL_LOGGING
        _ENABLE_FULL_LOGGING = (
            str(kafka_config.get("enable_full_logging", False)).lower() == "true"
        )
        message_format: str = kafka_config["format"]
        offset_commit_policy: str = kafka_config.get(
            "offset_commit_policy", "on_success"
        )

        # Dedup is active only with an id field AND no message timestamp
        # (a timestamp already lets InfluxDB dedup by tags + time).
        fmt_mapping: dict = (
            cached_config["mapping"].get(message_format) or {}
            if message_format in ("json", "text")
            else {}
        )
        dedup_cfg: dict | None = fmt_mapping.get("dedup")
        dedup_active: bool = bool(dedup_cfg) and not fmt_mapping.get("timestamp_config")
        dedup_field_name: str | None = (
            dedup_cfg["field_name"] if dedup_active else None
        )
        dedup_window_s: int = parse_duration_seconds(
            kafka_config.get("dedup_window", _DEFAULT_DEDUP_WINDOW)
        )

        # Get or create stats tracker from cache
        stats: KafkaStats | None = influxdb3_local.cache.get("kafka_stats")
        if stats is None:
            stats = KafkaStats()
            influxdb3_local.cache.put("kafka_stats", stats)

        # Reuse the persistent consumer if one is cached, otherwise connect once.
        manager = influxdb3_local.cache.get(_CACHE_CONSUMER)
        if manager is not None:
            manager.rebind(influxdb3_local, task_id)
            influxdb3_local.info(f"[{task_id}] Reusing cached Kafka consumer")
        else:
            influxdb3_local.info(f"[{task_id}] Creating new Kafka consumer connection")
            manager = KafkaManager(kafka_config, influxdb3_local, task_id)
            if not manager.connect():
                influxdb3_local.error(f"[{task_id}] Failed to connect to Kafka cluster")
                manager.close()
                manager = None
                return
            # Cache without TTL so the consumer stays in its group across cycles.
            influxdb3_local.cache.put(_CACHE_CONSUMER, manager)

        manager.reset_cycle()

        # Retrieve messages
        messages: list = manager.get_messages()
        poll_error: bool = manager.poll_error
        if poll_error:
            # Rebuild the consumer next cycle - the error is likely persistent
            # (authentication, certificate, transport).
            fatal = True

        # "always": commit immediately, but not after a failed poll (don't
        # advance past a transport error mid-drain).
        if offset_commit_policy == "always" and messages and not poll_error:
            manager.commit_offsets()

        servers_str: str = ",".join(kafka_config.get("bootstrap_servers", []))
        group_id: str = kafka_config.get("group_id", "unknown")

        if len(messages) == 0:
            if poll_error:
                # Do not commit after a failed poll
                influxdb3_local.warn(
                    f"[{task_id}] Skipping offset commit: poll finished with "
                    f"an error and no messages were retrieved"
                )
            else:
                # Commit to save current position (e.g. bootstrap a new topic)
                manager.commit_offsets()
            write_stats(influxdb3_local, stats, servers_str, group_id, task_id)
            return

        influxdb3_local.info(f"[{task_id}] Processing {len(messages)} messages")

        # Initialize parser based on format
        if message_format == "json":
            mapping_config: dict = cached_config["mapping"].get("json", {})
            parser = JSONParser(mapping_config, task_id, influxdb3_local)
        elif message_format == "lineprotocol":
            parser = LineProtocolParser(influxdb3_local, task_id)
        elif message_format == "text":
            mapping_config = cached_config["mapping"].get("text", {})
            parser = TextParser(mapping_config, task_id, influxdb3_local)
        else:
            influxdb3_local.error(
                f"[{task_id}] Unknown message format: {message_format}"
            )
            return

        # Phase 1: Parse all messages into (line, table, dedup_id) records
        # Per-message: (msg, "ok", records) or (msg, "fail", exception)
        parse_results: list[tuple] = []

        for msg in messages:
            topic: str = msg.get("topic", "unknown")
            partition: int = msg.get("partition", 0)
            offset: int = msg.get("offset", 0)
            payload: str = msg.get("payload", "")

            # Record message received BEFORE parsing
            stats.record_message_received(topic, partition, offset)

            try:
                records: list = parser.parse(payload)
                parse_results.append((msg, "ok", records))
            except Exception as e:
                parse_results.append((msg, "fail", e))

        # Phase 1.5: look up which ids already exist (per table), constrained by
        # the time window AND the batch ids, so each query returns few rows.
        existing_by_table: dict[str, set] = {}
        if dedup_active:
            ids_by_table: dict[str, set] = {}
            for msg, status, data in parse_results:
                if status != "ok":
                    continue
                for _line, table, dedup_id in data:
                    if dedup_id is not None:
                        ids_by_table.setdefault(table, set()).add(dedup_id)
            for table, idset in ids_by_table.items():
                existing_by_table[table] = query_existing_ids(
                    influxdb3_local,
                    table,
                    dedup_field_name,
                    list(idset),
                    dedup_window_s,
                    task_id,
                )

        # Phase 2: select lines to write, dropping duplicates (already in the
        # window or seen earlier in this batch). Track kept/duplicate per message.
        all_line_builders: list = []
        seen_by_table: dict[str, set] = {}
        # Per-message: (msg, status, kept, dup, exception_or_None)
        per_msg: list[tuple] = []

        for msg, status, data in parse_results:
            if status != "ok":
                per_msg.append((msg, status, 0, 0, data))
                continue

            kept: int = 0
            dup: int = 0
            for line, table, dedup_id in data:
                if dedup_id is not None:
                    seen = seen_by_table.setdefault(table, set())
                    existing = existing_by_table.get(table, set())
                    if dedup_id in existing or dedup_id in seen:
                        dup += 1
                        continue
                    seen.add(dedup_id)
                all_line_builders.append(line)
                kept += 1
            per_msg.append((msg, status, kept, dup, None))

        # Phase 3: Batch write all selected lines
        write_failed: bool = False
        if all_line_builders:
            try:
                influxdb3_local.write_sync(
                    _BatchLines(all_line_builders), no_sync=True
                )
            except Exception as e:
                write_failed = True
                influxdb3_local.error(
                    f"[{task_id}] Batch write failed: {_exc(e)}"
                )

        # Phase 4: Update stats, route failures to DLQ, decide offset commit
        success_count: int = 0
        dedup_count: int = 0
        error_count: int = 0
        dlq_sent: bool = False
        dlq_count: int = 0
        # Lowest offset per (topic, partition) to reprocess (write failures and
        # poison messages not offloaded to the DLQ); the consumer is rewound here.
        replay_offsets: dict[tuple[str, int], int] = {}

        def mark_replay(t: str, p: int, o: int) -> None:
            key = (t, p)
            if key not in replay_offsets or o < replay_offsets[key]:
                replay_offsets[key] = o

        for msg, status, kept, dup, exc in per_msg:
            topic = msg.get("topic", "unknown")
            partition = msg.get("partition", 0)
            offset = msg.get("offset", 0)

            if status == "ok" and not write_failed:
                success_count += kept
                dedup_count += dup
                stats.record_message_processed(1)
                continue

            error_count += 1
            stats.record_message_failed()

            if status == "fail":
                error_type: str = type(exc).__name__
                error_msg: str = str(exc)
            else:
                error_type = "BatchWriteError"
                error_msg = "Batch write to InfluxDB failed"

            influxdb3_local.error(
                f"[{task_id}] Error processing message from "
                f"{topic}:{partition}@{offset}: {error_msg}"
            )

            write_exception(
                influxdb3_local,
                topic,
                partition,
                offset,
                error_type,
                error_msg,
                task_id,
            )

            if status == "fail":
                # Poison message: offload to the DLQ; if it can't be offloaded,
                # replay it so it is not lost. DLQ delivery is reconciled below.
                if manager.send_to_dlq(
                    topic,
                    partition,
                    offset,
                    msg.get("key"),
                    msg.get("payload", ""),
                    error_type,
                    error_msg,
                ):
                    dlq_sent = True
                    dlq_count += 1
                else:
                    mark_replay(topic, partition, offset)
            else:
                # Transient write failure: reprocess on the next cycle.
                mark_replay(topic, partition, offset)

        # Replay only the DLQ offsets whose delivery was NOT confirmed, so
        # messages that did land in the DLQ are not re-sent next cycle.
        if dlq_sent:
            dlq_failed = manager.flush_dlq()
            for t, p, o in dlq_failed:
                mark_replay(t, p, o)
            dlq_count -= len(dlq_failed)  # count only delivered ones

        # on_success: commit, or rewind failed partitions. "always" already
        # committed upfront and does not reprocess.
        if offset_commit_policy == "on_success":
            if not replay_offsets:
                manager.commit_offsets()
            elif manager.seek_offsets(replay_offsets):
                # Rewind failed partitions for live re-read AND commit them at
                # the replay offset (override needed: position() doesn't reflect
                # an un-consumed seek). Successful partitions commit as usual.
                manager.commit_offsets(override=replay_offsets)
                influxdb3_local.warn(
                    f"[{task_id}] Rewound {len(replay_offsets)} partition(s) to "
                    f"reprocess failed messages (offset_commit_policy=on_success)"
                )
            else:
                # Rewind failed (likely a rebalance): skip commit and rebuild so
                # a fresh consumer re-reads from the last committed offset.
                fatal = True
                influxdb3_local.warn(
                    f"[{task_id}] Could not rewind after failure; rebuilding the "
                    f"consumer so uncommitted messages are re-read next cycle"
                )

        influxdb3_local.info(
            f"[{task_id}] Data write complete: {success_count} records inserted "
            f"into DB, {dedup_count} duplicates skipped, {error_count} errors, "
            f"{dlq_count} sent to DLQ"
        )

        write_stats(influxdb3_local, stats, servers_str, group_id, task_id)

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Error in Kafka plugin: {_exc(e)}")
        influxdb3_local.cache.delete("kafka_config")
        fatal = True

    finally:
        # Keep the consumer alive for reuse; only tear it down on fatal errors.
        if fatal and manager is not None:
            manager.close()
            influxdb3_local.cache.delete(_CACHE_CONSUMER)


