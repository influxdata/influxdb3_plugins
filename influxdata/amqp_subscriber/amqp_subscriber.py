"""
{
    "plugin_type": ["scheduled"],
    "scheduled_args_config": [
        {
            "name": "config_file_path",
            "example": "amqp_config.toml",
            "description": "Path to TOML configuration file (absolute or relative to PLUGIN_DIR).",
            "required": false
        },
        {
            "name": "host",
            "example": "localhost",
            "description": "AMQP broker hostname or IP address.",
            "required": true
        },
        {
            "name": "port",
            "example": "5672",
            "description": "AMQP broker port. Default: 5672, or 5671 if SSL is enabled.",
            "required": false
        },
        {
            "name": "virtual_host",
            "example": "/",
            "description": "AMQP virtual host. Default: '/'.",
            "required": false
        },
        {
            "name": "username",
            "example": "guest",
            "description": "AMQP broker username. Default: 'guest'.",
            "required": false
        },
        {
            "name": "password",
            "example": "guest",
            "description": "AMQP broker password. Default: 'guest'.",
            "required": false
        },
        {
            "name": "queues",
            "example": "sensor_data alerts",
            "description": "Space-separated list of queue names to consume messages from.",
            "required": true
        },
        {
            "name": "format",
            "example": "json",
            "description": "Message format: 'json', 'lineprotocol', or 'text'. Default: 'json'.",
            "required": false
        },
        {
            "name": "table_name",
            "example": "sensor_data",
            "description": "Static InfluxDB table name (measurement). Required for 'json' and 'text' formats if 'table_name_field' is not set.",
            "required": false
        },
        {
            "name": "table_name_field",
            "example": "measurement",
            "description": "Field path to extract table name from message data. JSON: field name (auto-prefixed with '$.'). Text: regex pattern with capture group.",
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
            "name": "ack_policy",
            "example": "on_success",
            "description": "When to acknowledge messages: 'on_success' (only after successful processing) or 'always' (even on errors). Default: 'on_success'.",
            "required": false
        },
        {
            "name": "requeue_on_failure",
            "example": "false",
            "description": "Whether to requeue messages that fail processing. When true, failed messages are returned to the queue for retry. Default: false.",
            "required": false
        },
        {
            "name": "max_messages",
            "example": "500",
            "description": "Maximum number of messages to retrieve per scheduled call. Default: 500.",
            "required": false
        },
        {
            "name": "ssl_ca_cert",
            "example": "certs/ca.crt",
            "description": "Path to CA certificate file for SSL (absolute or relative to PLUGIN_DIR). Enables SSL when provided.",
            "required": false
        },
        {
            "name": "ssl_client_cert",
            "example": "certs/client.crt",
            "description": "Path to client certificate for mutual TLS.",
            "required": false
        },
        {
            "name": "ssl_client_key",
            "example": "certs/client.key",
            "description": "Path to client private key for mutual TLS.",
            "required": false
        }
    ]
}
"""

import json
import os
import re
import ssl
import time
import tomllib
import uuid
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

import pika
from jsonpath_ng.ext import parse as jsonpath_parse

# Internal constants
_CONNECTION_TIMEOUT = 10
_MAX_MESSAGES = 500
_PREFETCH_COUNT = 100


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
        line.int64_field(field_key, int(value))
    elif field_type == "uint":
        line.uint64_field(field_key, int(value))
    elif field_type == "float":
        line.float64_field(field_key, float(value))
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


class AMQPConfig:
    """Configuration loader and validator for AMQP plugin"""

    VALID_TIMESTAMP_FORMATS = {"ns", "ms", "s", "datetime"}
    VALID_FIELD_TYPES = {"int", "uint", "float", "string", "bool"}
    VALID_ACK_POLICIES = {"on_success", "always"}

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

    def _load_toml_config(self, config_file: str) -> dict[str, Any]:
        """Load configuration from TOML file"""
        config_path: str = self._resolve_path(config_file, "configuration file")

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found or not accessible: {config_file}")

        try:
            with open(config_path, "rb") as f:
                config: dict[str, Any] = tomllib.load(f)
        except OSError:
            raise OSError(f"Configuration file not found or not accessible: {config_file}") from None

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

    def _validate_toml_config(self, config: dict[str, Any]):
        """Validate that all required configuration parameters are present"""
        if "amqp" not in config:
            raise ValueError("Missing required 'amqp' section in configuration")

        amqp_config: dict[str, Any] = config["amqp"]

        if "host" not in amqp_config:
            raise ValueError("Missing required parameter 'amqp.host' in configuration")

        if "queues" not in amqp_config:
            raise ValueError(
                "Missing required parameter 'amqp.queues' in configuration"
            )

        # Validate queues is a non-empty list
        queues: list[str] = amqp_config["queues"]
        if not isinstance(queues, list) or len(queues) == 0:
            raise ValueError("Parameter 'amqp.queues' must be a non-empty list")

        # Validate ack_policy if present
        ack_policy = amqp_config.get("ack_policy", "on_success")
        if ack_policy not in self.VALID_ACK_POLICIES:
            raise ValueError(
                f"Invalid ack_policy: {ack_policy}. "
                f"Supported: {', '.join(sorted(self.VALID_ACK_POLICIES))}"
            )

        # Validate requeue_on_failure if present
        requeue_on_failure = amqp_config.get("requeue_on_failure", False)
        if not isinstance(requeue_on_failure, bool):
            raise ValueError("requeue_on_failure must be a boolean")

        # Validate max_messages if present
        max_messages = amqp_config.get("max_messages", _MAX_MESSAGES)
        if not isinstance(max_messages, int) or max_messages < 1:
            raise ValueError("max_messages must be a positive integer")

        # Get message format (default to json if not specified)
        message_format: str = amqp_config.get("format", "json")

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
                f"Supported formats: json, text, lineprotocol."
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

    def _build_config_from_args(self) -> dict[str, Any]:
        """Build configuration from command-line arguments"""
        required_keys: list = ["host", "queues"]

        if self.args.get("format", "json") in ["json", "text"]:
            if not self.args.get("table_name_field"):
                required_keys.append("table_name")

        if not self.args or any(key not in self.args for key in required_keys):
            raise ValueError(
                f"Missing some of the required arguments: {', '.join(required_keys)}"
            )

        # Parse space-separated queues into list
        queues_list: list[str] = self.args.get("queues").split()

        # Validate ack_policy
        ack_policy = self.args.get("ack_policy", "on_success")
        if ack_policy not in self.VALID_ACK_POLICIES:
            raise ValueError(
                f"Invalid ack_policy: {ack_policy}. "
                f"Supported: {', '.join(sorted(self.VALID_ACK_POLICIES))}"
            )

        # Parse max_messages
        max_messages_str = self.args.get("max_messages")
        max_messages = int(max_messages_str) if max_messages_str else _MAX_MESSAGES
        if max_messages < 1:
            raise ValueError("max_messages must be a positive integer")

        # Parse requeue_on_failure
        requeue_str = self.args.get("requeue_on_failure", "false")
        requeue_on_failure = requeue_str.lower() in ("true", "1", "yes")

        # Build SSL config if provided
        ssl_config: dict = {}
        ssl_ca_cert = self.args.get("ssl_ca_cert")
        ssl_client_cert = self.args.get("ssl_client_cert")
        ssl_client_key = self.args.get("ssl_client_key")

        if ssl_ca_cert:
            ssl_config["ca_cert"] = ssl_ca_cert
        if ssl_client_cert and ssl_client_key:
            ssl_config["client_cert"] = ssl_client_cert
            ssl_config["client_key"] = ssl_client_key
        elif ssl_client_cert or ssl_client_key:
            raise ValueError(
                "Both ssl_client_cert and ssl_client_key must be provided for mutual TLS"
            )

        # Determine default port based on SSL
        default_port = 5671 if ssl_config else 5672

        return {
            "amqp": {
                "host": self.args.get("host"),
                "port": int(self.args.get("port", default_port)),
                "virtual_host": self.args.get("virtual_host", "/"),
                "username": self.args.get("username", "guest"),
                "password": self.args.get("password", "guest"),
                "queues": queues_list,
                "format": self.args.get("format", "json"),
                "ack_policy": ack_policy,
                "requeue_on_failure": requeue_on_failure,
                "max_messages": max_messages,
                "ssl": ssl_config,
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
                f"Use 'json', 'text', or 'lineprotocol'."
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
        return {"text": text_config}

    def get(self, key: str, default: Any = None):
        """Get configuration value by key"""
        return self.config.get(key, default)

    def get_amqp_config(self) -> dict[str, Any]:
        """Get AMQP connection configuration"""
        return self.config.get("amqp")

    def get_mapping_config(self, format_type: str) -> dict[str, Any]:
        """Get mapping configuration for specified format"""
        mapping = self.config.get("mapping")
        if mapping:
            return mapping.get(format_type)
        return None


class AMQPConsumerManager:
    """Manages AMQP consumer connection and message retrieval using pika"""

    def __init__(self, config: dict[str, Any], influxdb3_local, task_id: str):
        self.config: dict[str, Any] = config
        self.influxdb3_local = influxdb3_local
        self.task_id: str = task_id
        self.connection: pika.BlockingConnection | None = None
        self.channel: pika.adapters.blocking_connection.BlockingChannel | None = None
        self.connected: bool = False

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

    def _build_ssl_context(self) -> ssl.SSLContext | None:
        """Build SSL context if SSL configuration is provided"""
        ssl_config = self.config.get("ssl", {})
        if not ssl_config:
            return None

        ca_cert = ssl_config.get("ca_cert")
        if not ca_cert:
            return None

        # Resolve paths
        ca_cert_orig = ca_cert
        ca_cert = self._resolve_path(ca_cert, "CA certificate")
        if not os.path.exists(ca_cert):
            raise FileNotFoundError(f"TLS configuration failed. ca_cert not found: {ca_cert_orig}")

        # Create SSL context
        try:
            ssl_context = ssl.create_default_context(cafile=ca_cert)
        except Exception:
            raise OSError(f"TLS configuration failed. ca_cert not usable: {ca_cert_orig}") from None

        # Client certificate for mutual TLS
        client_cert = ssl_config.get("client_cert")
        client_key = ssl_config.get("client_key")
        if client_cert and client_key:
            client_cert_orig = client_cert
            client_key_orig = client_key
            client_cert = self._resolve_path(client_cert, "client certificate")
            client_key = self._resolve_path(client_key, "client key")
            if not os.path.exists(client_cert):
                raise FileNotFoundError(f"TLS configuration failed. client_cert not found: {client_cert_orig}")
            if not os.path.exists(client_key):
                raise FileNotFoundError(f"TLS configuration failed. client_key not found: {client_key_orig}")
            try:
                ssl_context.load_cert_chain(client_cert, client_key)
            except Exception:
                raise OSError("TLS configuration failed. Check client certificate and key files.") from None

        return ssl_context

    def _build_connection_parameters(self) -> pika.ConnectionParameters:
        """Build pika connection parameters"""
        host = self.config.get("host")
        ssl_config = self.config.get("ssl", {})
        default_port = 5671 if ssl_config else 5672
        port = self.config.get("port", default_port)
        virtual_host = self.config.get("virtual_host", "/")
        username = self.config.get("username", "guest")
        password = self.config.get("password", "guest")

        credentials = pika.PlainCredentials(username, password)

        # Build SSL options
        ssl_context = self._build_ssl_context()
        ssl_options = None
        if ssl_context:
            ssl_options = pika.SSLOptions(ssl_context, host)

        return pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=virtual_host,
            credentials=credentials,
            ssl_options=ssl_options,
            blocked_connection_timeout=_CONNECTION_TIMEOUT,
            socket_timeout=_CONNECTION_TIMEOUT,
        )

    def connect(self) -> bool:
        """Establish connection to AMQP broker"""
        try:
            params = self._build_connection_parameters()
            host = self.config.get("host")
            port = params.port

            self.influxdb3_local.info(
                f"[{self.task_id}] Connecting to AMQP broker: {host}:{port}"
            )

            self.connection = pika.BlockingConnection(params)
            self.channel = self.connection.channel()

            # Set QoS (prefetch count)
            self.channel.basic_qos(prefetch_count=_PREFETCH_COUNT)

            self.connected = True
            self.influxdb3_local.info(
                f"[{self.task_id}] AMQP consumer connected successfully"
            )
            return True

        except (pika.exceptions.AMQPConnectionError, Exception) as e:
            if self.config.get("ssl", {}):
                self.influxdb3_local.error(
                    f"[{self.task_id}] Error connecting to AMQP broker: "
                    f"connection failed (SSL/TLS configured). "
                    f"Check broker address, certificates, and key files."
                )
            else:
                self.influxdb3_local.error(
                    f"[{self.task_id}] Error connecting to AMQP broker: {str(e)}"
                )
            return False

    def get_messages(self) -> list[dict[str, Any]]:
        """Retrieve messages from all configured queues"""
        messages: list[dict[str, Any]] = []

        if not self.channel:
            return messages

        queues = self.config.get("queues", [])
        max_messages = self.config.get("max_messages", _MAX_MESSAGES)

        try:
            # Iterate over all queues until max_messages reached
            for queue_name in queues:
                if len(messages) >= max_messages:
                    break

                while len(messages) < max_messages:
                    method, properties, body = self.channel.basic_get(
                        queue=queue_name, auto_ack=False
                    )

                    if method is None:
                        # No more messages in this queue, move to next
                        break

                    # Decode body
                    try:
                        payload = body.decode("utf-8") if body else None
                    except UnicodeDecodeError:
                        self.influxdb3_local.warn(
                            f"[{self.task_id}] Skipping binary message from {queue_name} "
                            f"({len(body)} bytes) - only UTF-8 text supported"
                        )
                        # Reject binary message
                        self.channel.basic_nack(
                            delivery_tag=method.delivery_tag, requeue=False
                        )
                        continue

                    # Skip empty payloads
                    if not payload or not payload.strip():
                        self.influxdb3_local.warn(
                            f"[{self.task_id}] Skipping empty message from {queue_name}"
                        )
                        self.channel.basic_ack(delivery_tag=method.delivery_tag)
                        continue

                    messages.append(
                        {
                            "queue": queue_name,
                            "delivery_tag": method.delivery_tag,
                            "routing_key": method.routing_key,
                            "exchange": method.exchange,
                            "payload": payload,
                            "message_id": properties.message_id if properties else None,
                            "timestamp": properties.timestamp if properties else None,
                            "content_type": properties.content_type
                            if properties
                            else None,
                        }
                    )

        except Exception as e:
            self.influxdb3_local.error(
                f"[{self.task_id}] Error retrieving AMQP messages: {str(e)}"
            )

        return messages

    def ack_message(self, delivery_tag: int):
        """Acknowledge a message"""
        if self.channel:
            self.channel.basic_ack(delivery_tag=delivery_tag)

    def nack_message(self, delivery_tag: int, requeue: bool = True):
        """Negative acknowledge a message"""
        if self.channel:
            self.channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)

    def disconnect(self):
        """Disconnect from AMQP broker"""
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
            if self.connection and self.connection.is_open:
                self.connection.close()
            self.connected = False
            self.influxdb3_local.info(f"[{self.task_id}] Disconnected from AMQP broker")
        except Exception as e:
            self.influxdb3_local.error(
                f"[{self.task_id}] Error disconnecting from AMQP broker: {str(e)}"
            )


class JSONParser:
    """Parse JSON messages and convert to Line Protocol"""

    def __init__(self, mapping_config: dict[str, Any], task_id: str, influxdb3_local):
        self.mapping_config: dict = mapping_config
        self.task_id: str = task_id
        self.influxdb3_local = influxdb3_local
        self._compiled_paths: dict[str, Any] = self._compile_jsonpath_expressions()

    def _compile_jsonpath_expressions(self) -> dict[str, Any]:
        """Pre-compile all JSONPath expressions for performance optimization."""
        compiled: dict[str, Any] = {}

        table_name_field = self.mapping_config.get("table_name_field")
        if table_name_field:
            compiled["table_name"] = jsonpath_parse(table_name_field)

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
        """Parse JSON payload and return list of LineBuilder objects."""
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
                        line = self._parse_object(item)
                        if line:
                            results.append(line)
                    except Exception as e:
                        self.influxdb3_local.warn(
                            f"[{self.task_id}] Error parsing array element {i}: "
                            f"{str(e)}"
                        )

                if len(results) == 0:
                    self.influxdb3_local.warn(
                        f"[{self.task_id}] No valid objects in JSON array, skipping"
                    )
                    return []

                return results

            elif isinstance(data, dict):
                line = self._parse_object(data)
                return [line] if line else []

            else:
                raise ValueError(f"Unsupported JSON type: {type(data).__name__}")

        except json.JSONDecodeError as e:
            self.influxdb3_local.error(f"[{self.task_id}] Invalid JSON: {str(e)}")
            raise
        except Exception as e:
            self.influxdb3_local.error(f"[{self.task_id}] Error parsing JSON: {str(e)}")
            raise

    def _parse_object(self, data: dict[str, Any]):
        """Parse a single JSON object and return LineBuilder"""
        table_name: str | None = self._get_table_name(data)
        if not table_name:
            raise ValueError("Could not determine table name")

        line = LineBuilder(table_name)

        self._add_tags(line, data)

        field_count: int = self._add_fields(line, data)

        if field_count == 0:
            raise ValueError("No fields were mapped from JSON data")

        timestamp_ns: int | None = self._get_timestamp(data)
        if timestamp_ns is not None:
            line.time_ns(timestamp_ns)

        return line

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
            return time.time_ns()

        try:
            return convert_timestamp(timestamp_value, time_format)
        except Exception as e:
            self.influxdb3_local.error(
                f"[{self.task_id}] Failed to convert timestamp '{timestamp_value}' "
                f"with format '{time_format}': {str(e)}"
            )
            return time.time_ns()

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
            if not matches:
                self.influxdb3_local.warn(
                    f"[{self.task_id}] JSONPath '{path}' did not match any value, skipping"
                )
                return None
            return matches[0].value

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
        """Parse line protocol string and return LineBuilder object"""
        try:
            payload = payload.strip()

            parts: list[str] = self._split_quoted(
                payload, " ", max_splits=2, skip_empty=True
            )

            if len(parts) < 2:
                raise ValueError("Invalid line protocol format: missing field set")

            measurement_and_tags: str = parts[0]
            fields_str: str = parts[1]
            timestamp_ns: int | None = int(parts[2]) if len(parts) == 3 else None

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

            return line

        except Exception as e:
            self.influxdb3_local.error(
                f"[{self.task_id}] Error parsing line protocol: {str(e)}"
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
                            f"value '{value}' to type '{field_type}': {str(e)}"
                        )
                        raise
                    field_count += 1

            if field_count == 0:
                raise ValueError("No fields were extracted from text message")

            timestamp_ns: int = self._get_timestamp(payload)
            line.time_ns(timestamp_ns)

            return line

        except Exception as e:
            self.influxdb3_local.error(f"[{self.task_id}] Error parsing text: {str(e)}")
            raise

    def _extract_value(
        self, text: str, pattern_str: str, field_name: str, cache_key: str | None = None
    ) -> str | None:
        """Extract value from text using regex pattern"""
        try:
            if cache_key and cache_key in self._compiled_patterns:
                pattern = self._compiled_patterns[cache_key]
            else:
                pattern = re.compile(pattern_str)

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

        except re.error as e:
            self.influxdb3_local.error(
                f"[{self.task_id}] Invalid regex pattern for '{field_name}': "
                f"{pattern_str} - {e}"
            )
            return None

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
            return time.time_ns()

        time_format = timestamp_config.get("format", "ns")
        try:
            return convert_timestamp(timestamp_value, time_format)
        except Exception as e:
            self.influxdb3_local.error(
                f"[{self.task_id}] Failed to convert timestamp '{timestamp_value}' "
                f"with format '{time_format}': {str(e)}"
            )
            return time.time_ns()


class AMQPStats:
    """Track and persist AMQP plugin statistics"""

    def __init__(self):
        self.reset()

    def reset(self):
        """Reset all statistics"""
        # Total stats (accumulated over entire plugin lifetime)
        self.total_stats_by_queue: dict = {}
        # Period stats (reset after each stats write)
        self.period_stats_by_queue: dict = {}
        self.last_message_time: int | None = None
        self.current_queue: str | None = None

    def _ensure_queue_stats(self, queue: str):
        """Ensure stats dicts exist for a queue"""
        if queue not in self.total_stats_by_queue:
            self.total_stats_by_queue[queue] = {
                "received": 0,
                "processed": 0,
                "failed": 0,
            }
        if queue not in self.period_stats_by_queue:
            self.period_stats_by_queue[queue] = {
                "received": 0,
                "processed": 0,
                "failed": 0,
            }

    def record_message_received(self, queue: str, count: int = 1):
        """Record received message(s)"""
        self.last_message_time = time.time_ns()
        self.current_queue = queue
        self._ensure_queue_stats(queue)

        self.total_stats_by_queue[queue]["received"] += count
        self.period_stats_by_queue[queue]["received"] += count

    def record_message_processed(self, count: int = 1):
        """Record successfully processed message(s)"""
        if self.current_queue:
            self._ensure_queue_stats(self.current_queue)
            self.total_stats_by_queue[self.current_queue]["processed"] += count
            self.period_stats_by_queue[self.current_queue]["processed"] += count

    def record_message_failed(self, count: int = 1):
        """Record failed message(s)"""
        if self.current_queue:
            self._ensure_queue_stats(self.current_queue)
            self.total_stats_by_queue[self.current_queue]["failed"] += count
            self.period_stats_by_queue[self.current_queue]["failed"] += count

    def reset_period_stats(self):
        """Reset period statistics after writing to database"""
        self.period_stats_by_queue = {}

    def get_queue_stats(self) -> dict[str, dict[str, Any]]:
        """Get statistics by queue with calculated success rates for both period and total"""
        result: dict = {}

        # Get all queues from both dicts
        all_queues = set(self.total_stats_by_queue.keys()) | set(
            self.period_stats_by_queue.keys()
        )

        for queue in all_queues:
            total = self.total_stats_by_queue.get(
                queue, {"received": 0, "processed": 0, "failed": 0}
            )
            period = self.period_stats_by_queue.get(
                queue, {"received": 0, "processed": 0, "failed": 0}
            )

            # Calculate success rates
            total_handled = total["processed"] + total["failed"]
            total_success_rate = (
                (total["processed"] / total_handled * 100) if total_handled > 0 else 0.0
            )

            period_handled = period["processed"] + period["failed"]
            period_success_rate = (
                (period["processed"] / period_handled * 100)
                if period_handled > 0
                else 0.0
            )

            result[queue] = {
                # Period stats (current interval)
                "received": period["received"],
                "processed": period["processed"],
                "failed": period["failed"],
                "success_rate": round(period_success_rate, 2),
                # Total stats (all time)
                "total_received": total["received"],
                "total_processed": total["processed"],
                "total_failed": total["failed"],
                "total_success_rate": round(total_success_rate, 2),
            }
        return result


def write_stats(
    influxdb3_local,
    stats: AMQPStats,
    host: str,
    virtual_host: str,
    task_id: str,
):
    """Write per-queue statistics to amqp_stats table."""
    try:
        queue_stats: dict = stats.get_queue_stats()
        lines: list = []

        for queue, data in queue_stats.items():
            line = LineBuilder("amqp_stats")

            line.tag("queue", queue)
            line.tag("host", host)
            line.tag("virtual_host", virtual_host)

            # Period stats (current interval since last write)
            line.int64_field("period_received", data["received"])
            line.int64_field("period_processed", data["processed"])
            line.int64_field("period_failed", data["failed"])
            # Only write period_success_rate if there were messages in this period
            if data["received"] > 0:
                line.float64_field("period_success_rate", data["success_rate"])

            # Total stats (accumulated over entire plugin lifetime)
            line.int64_field("messages_received", data["total_received"])
            line.int64_field("messages_processed", data["total_processed"])
            line.int64_field("messages_failed", data["total_failed"])
            # Only write success_rate if there were any messages ever
            if data["total_received"] > 0:
                line.float64_field("success_rate", data["total_success_rate"])

            line.time_ns(time.time_ns())
            lines.append(line)

        if lines:
            influxdb3_local.write_sync(_BatchLines(lines), no_sync=True)

        # Reset period stats after writing
        stats.reset_period_stats()

        influxdb3_local.info(
            f"[{task_id}] Wrote statistics for {len(queue_stats)} queues "
            f"to amqp_stats table"
        )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to write statistics: {str(e)}")


def write_exception(
    influxdb3_local,
    queue: str,
    error_type: str,
    error_message: str,
    raw_message: str,
    task_id: str,
):
    """Write exception to amqp_exceptions table."""
    try:
        line = LineBuilder("amqp_exceptions")
        line.tag("queue", queue)
        line.tag("error_type", error_type)
        line.string_field("error_message", error_message)
        line.string_field("raw_message", raw_message)
        line.time_ns(time.time_ns())

        influxdb3_local.write_sync(line, no_sync=True)
        influxdb3_local.info(
            f"[{task_id}] Wrote exception to amqp_exceptions table: {error_type}"
        )

    except Exception as e:
        influxdb3_local.error(
            f"[{task_id}] Failed to write exception to table: {str(e)}"
        )


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
    amqp_consumer: AMQPConsumerManager | None = None

    if not args:
        influxdb3_local.error(f"[{task_id}] No arguments provided")
        return

    try:
        # Load configuration from cache or parse fresh
        cached_config: dict | None = influxdb3_local.cache.get("amqp_config")
        if cached_config is None:
            config_loader: AMQPConfig = AMQPConfig(influxdb3_local, args, task_id)
            cached_config = {
                "amqp": config_loader.get_amqp_config(),
                "mapping": {
                    "json": config_loader.get_mapping_config("json"),
                    "text": config_loader.get_mapping_config("text"),
                },
            }
            influxdb3_local.cache.put("amqp_config", cached_config, 60 * 60)
            influxdb3_local.info(
                f"[{task_id}] AMQP Plugin initialized, "
                f"format: {cached_config['amqp']['format']}"
            )

        amqp_config: dict = cached_config["amqp"]
        message_format: str = amqp_config["format"]
        ack_policy: str = amqp_config.get("ack_policy", "on_success")
        requeue_on_failure: bool = amqp_config.get("requeue_on_failure", False)

        # Get or create stats tracker from cache
        stats: AMQPStats | None = influxdb3_local.cache.get("amqp_stats")
        if stats is None:
            stats = AMQPStats()
            influxdb3_local.cache.put("amqp_stats", stats)

        # Create new AMQP consumer connection
        influxdb3_local.info(f"[{task_id}] Creating new AMQP consumer connection")
        amqp_consumer = AMQPConsumerManager(amqp_config, influxdb3_local, task_id)

        if not amqp_consumer.connect():
            influxdb3_local.error(f"[{task_id}] Failed to connect to AMQP broker")
            return

        # Retrieve messages
        messages: list = amqp_consumer.get_messages()

        # Write stats every 10 calls
        call_count: int = influxdb3_local.cache.get("amqp_call_count")
        if call_count is None:
            call_count = 0

        call_count += 1

        host: str = amqp_config.get("host", "unknown")
        virtual_host: str = amqp_config.get("virtual_host", "/")

        if call_count >= 10:
            write_stats(influxdb3_local, stats, host, virtual_host, task_id)
            call_count = 0

        influxdb3_local.cache.put("amqp_call_count", call_count)

        if len(messages) == 0:
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

        # Phase 1: Parse all messages, collect line builders
        all_line_builders: list = []
        # Per-message results: list of (msg, "ok", line_count) or (msg, "fail", exception)
        message_results: list[tuple] = []

        for msg in messages:
            queue_name: str = msg.get("queue", "unknown")
            payload: str = msg.get("payload", "")

            try:
                stats.record_message_received(queue_name)

                if message_format == "json":
                    line_builders: list = parser.parse(payload)
                    all_line_builders.extend(line_builders)
                    message_results.append((msg, "ok", len(line_builders)))
                else:
                    line_builder = parser.parse(payload)
                    if line_builder:
                        all_line_builders.append(line_builder)
                        message_results.append((msg, "ok", 1))
                    else:
                        message_results.append((msg, "ok", 0))

            except Exception as e:
                message_results.append((msg, "fail", e))

        # Phase 2: Batch write all parsed lines at once
        write_failed: bool = False
        if all_line_builders:
            try:
                influxdb3_local.write_sync(
                    _BatchLines(all_line_builders), no_sync=True
                )
            except Exception as e:
                write_failed = True
                influxdb3_local.error(
                    f"[{task_id}] Batch write failed: {str(e)}"
                )

        # Phase 3: Ack/nack and update stats based on results
        success_count: int = 0
        error_count: int = 0

        for msg, status, result in message_results:
            delivery_tag: int = msg.get("delivery_tag")
            queue_name: str = msg.get("queue", "unknown")
            payload: str = msg.get("payload", "")

            if status == "ok" and not write_failed:
                success_count += result
                stats.record_message_processed(1)
                if ack_policy == "on_success":
                    amqp_consumer.ack_message(delivery_tag)
            else:
                error_count += 1
                stats.record_message_failed()

                if status == "fail":
                    exc = result
                    error_type: str = type(exc).__name__
                    error_msg: str = str(exc)
                else:
                    error_type = "BatchWriteError"
                    error_msg = "Batch write to InfluxDB failed"

                influxdb3_local.error(
                    f"[{task_id}] Error processing message from {queue_name}: "
                    f"{error_msg}"
                )

                write_exception(
                    influxdb3_local,
                    queue_name,
                    error_type,
                    error_msg,
                    payload[:1000],
                    task_id,
                )

                if ack_policy == "on_success":
                    amqp_consumer.nack_message(
                        delivery_tag, requeue=requeue_on_failure
                    )

        # If ack_policy is "always", acknowledge all messages at the end
        if ack_policy == "always":
            for msg in messages:
                amqp_consumer.ack_message(msg.get("delivery_tag"))

        influxdb3_local.info(
            f"[{task_id}] Data write complete: {success_count} records inserted into DB, "
            f"{error_count} errors"
        )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Error in AMQP plugin: {str(e)}")
        influxdb3_local.cache.delete("amqp_config")

    finally:
        if amqp_consumer is not None:
            amqp_consumer.disconnect()
