"""
{
    "plugin_type": ["scheduled"],
    "scheduled_args_config": [
        {
            "name": "config_file_path",
            "example": "mqtt_config.toml",
            "description": "Path to TOML configuration file (absolute or relative to PLUGIN_DIR).",
            "required": false
        },
        {
            "name": "broker_host",
            "example": "broker.hivemq.com",
            "description": "MQTT broker hostname or IP address.",
            "required": true
        },
        {
            "name": "broker_port",
            "example": "1883",
            "description": "MQTT broker port. Default: 1883 for non-TLS, 8883 for TLS.",
            "required": false
        },
        {
            "name": "topics",
            "example": "sensors/temperature sensors/humidity",
            "description": "Space-separated list of MQTT topics to subscribe to. Supports wildcards: + (single level), # (multi-level).",
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
            "name": "qos",
            "example": "1",
            "description": "MQTT Quality of Service level: 0 (at most once), 1 (at least once), 2 (exactly once). Default: 1.",
            "required": false
        },
        {
            "name": "client_id",
            "example": "influxdb3_mqtt_client",
            "description": "MQTT client identifier. Must be unique per broker. Auto-generated if not specified.",
            "required": false
        },
        {
            "name": "username",
            "example": "mqtt_user",
            "description": "MQTT broker username. Both username and password must be provided together.",
            "required": false
        },
        {
            "name": "password",
            "example": "mqtt_password",
            "description": "MQTT broker password. Both username and password must be provided together.",
            "required": false
        },
        {
            "name": "ca_cert",
            "example": "certs/ca.crt",
            "description": "Path to CA certificate file for TLS (absolute or relative to PLUGIN_DIR).",
            "required": false
        },
        {
            "name": "client_cert",
            "example": "certs/client.crt",
            "description": "Path to client certificate for mutual TLS. Both client_cert and client_key must be provided together.",
            "required": false
        },
        {
            "name": "client_key",
            "example": "certs/client.key",
            "description": "Path to client private key for mutual TLS. Both client_cert and client_key must be provided together.",
            "required": false
        }
    ]
}
"""

import json
import os
import re
import time
import tomllib
import uuid
from datetime import datetime
from queue import Empty, Queue
from typing import Any, Protocol, runtime_checkable

from jsonpath_ng import parse as jsonpath_parse
from paho.mqtt.client import CallbackAPIVersion, Client


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


class MQTTConfig:
    """Configuration loader and validator for MQTT plugin"""

    VALID_TIMESTAMP_FORMATS = {"ns", "ms", "s", "datetime"}
    VALID_FIELD_TYPES = {"int", "uint", "float", "string", "bool"}

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
            # Use command-line arguments as config
            self.config = self._build_config_from_args()

    @staticmethod
    def _resolve_path(path: str, description: str) -> str:
        """Resolve path - absolute paths used as-is, relative paths resolved from PLUGIN_DIR.

        Args:
            path: File path (absolute or relative)
            description: Description for error messages (e.g., "configuration file")

        Returns:
            Resolved absolute path

        Raises:
            ValueError: If path is relative and PLUGIN_DIR is not set
        """
        if os.path.isabs(path):
            return path

        plugin_dir: str | None = os.environ.get("PLUGIN_DIR")
        if not plugin_dir:
            raise ValueError(
                f"PLUGIN_DIR environment variable not set. "
                f"Required for relative {description} path: {path}"
            )
        return os.path.join(plugin_dir, path)

    def _load_toml_config(self, config_file: str) -> dict[str, Any]:
        """Load configuration from TOML file"""
        config_path: str = self._resolve_path(config_file, "configuration file")

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, "rb") as f:
            config: dict[str, Any] = tomllib.load(f)

        # Validate required MQTT configuration
        self._validate_toml_config(config)

        return config

    def _validate_and_parse_timestamp_field(
        self, timestamp_field: str, field_name: str
    ) -> dict[str, str]:
        """Validate and parse timestamp_field format

        Args:
            timestamp_field: Field value (e.g., "$.timestamp:ms" or "pattern:datetime")
            field_name: Name of the field for error messages (e.g., "mapping.json.timestamp_field")

        Returns:
            Dict with 'field' and 'format' keys

        Raises:
            ValueError: If format is invalid
        """
        if ":" in timestamp_field:
            # Extract field and format from "field:format"
            field_path, time_format = timestamp_field.rsplit(":", 1)
            field_path = field_path.strip()
            time_format = time_format.strip()

            if time_format not in self.VALID_TIMESTAMP_FORMATS:
                raise ValueError(
                    f"Invalid timestamp format in '{field_name}': '{time_format}'. Supported formats: {', '.join(sorted(self.VALID_TIMESTAMP_FORMATS))}"
                )

            return {"field": field_path, "format": time_format}
        else:
            # Default to nanoseconds if no format specified
            return {"field": timestamp_field.strip(), "format": "ns"}

    def _validate_toml_config(self, config: dict[str, Any]):
        """Validate that all required configuration parameters are present"""
        # Check for mqtt section
        if "mqtt" not in config:
            raise ValueError("Missing required 'mqtt' section in configuration")

        mqtt_config: dict[str, Any] = config["mqtt"]

        # Check required MQTT parameters
        if "broker_host" not in mqtt_config:
            raise ValueError(
                "Missing required parameter 'mqtt.broker_host' in configuration"
            )

        if "topics" not in mqtt_config:
            raise ValueError(
                "Missing required parameter 'mqtt.topics' in configuration"
            )

        # Validate topics is a non-empty list
        topics: list[str] = mqtt_config["topics"]
        if not isinstance(topics, list) or len(topics) == 0:
            raise ValueError("Parameter 'mqtt.topics' must be a non-empty list")

        # Get message format (default to json if not specified)
        message_format: str = mqtt_config.get("format", "json")

        # Validate format-specific mapping configuration
        if message_format == "json":
            self._validate_json_mapping(config)
        elif message_format == "text":
            self._validate_text_mapping(config)
        elif message_format == "lineprotocol":
            # Line protocol doesn't require mapping configuration
            pass
        else:
            raise ValueError(
                f"Invalid message format: {message_format}. Supported formats: json, text, lineprotocol"
            )

    def _validate_json_mapping(self, config: dict[str, Any]):
        """Validate JSON mapping configuration"""
        if "mapping" not in config or "json" not in config["mapping"]:
            raise ValueError("Missing required 'mapping.json' section in configuration")

        json_mapping: dict[str, Any] = config["mapping"]["json"]

        # Check for table_name or table_name_field
        if "table_name" not in json_mapping and "table_name_field" not in json_mapping:
            raise ValueError(
                "Missing required parameter 'mapping.json.table_name' or 'mapping.json.table_name_field' in configuration"
            )

        # Check for fields
        if "fields" not in json_mapping or not json_mapping["fields"]:
            raise ValueError(
                "Missing required parameter 'mapping.json.fields' in configuration"
            )

        # Validate and parse timestamp_field format if present
        if "timestamp_field" in json_mapping:
            parsed_timestamp: dict[str, str] = self._validate_and_parse_timestamp_field(
                json_mapping["timestamp_field"], "mapping.json.timestamp_field"
            )
            # Convert to unified timestamp_config format
            json_mapping["timestamp_config"] = parsed_timestamp
            del json_mapping["timestamp_field"]

    def _validate_text_mapping(self, config: dict[str, Any]):
        """Validate text mapping configuration"""
        if "mapping" not in config or "text" not in config["mapping"]:
            raise ValueError("Missing required 'mapping.text' section in configuration")

        text_mapping: dict[str, Any] = config["mapping"]["text"]

        # Check for table_name or table_name_field
        if "table_name" not in text_mapping and "table_name_field" not in text_mapping:
            raise ValueError(
                "Missing required parameter 'mapping.text.table_name' or 'mapping.text.table_name_field' in configuration"
            )

        # Check for fields
        if "fields" not in text_mapping or not text_mapping["fields"]:
            raise ValueError(
                "Missing required parameter 'mapping.text.fields' in configuration"
            )

        # Validate field format: each field must be [pattern, type]
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
                    f"Invalid pattern for 'mapping.text.fields.{field_name}'. Pattern must be a non-empty string."
                )
            if field_type not in self.VALID_FIELD_TYPES:
                raise ValueError(
                    f"Invalid field type '{field_type}' for 'mapping.text.fields.{field_name}'. "
                    f"Supported types: {', '.join(sorted(self.VALID_FIELD_TYPES))}"
                )

        # Validate and parse timestamp_field format if present
        if "timestamp_field" in text_mapping:
            parsed_timestamp: dict[str, str] = self._validate_and_parse_timestamp_field(
                text_mapping["timestamp_field"], "mapping.text.timestamp_field"
            )
            # Convert to unified timestamp_config format
            text_mapping["timestamp_config"] = parsed_timestamp
            del text_mapping["timestamp_field"]

    def _build_config_from_args(self) -> dict[str, Any]:
        """Build configuration from command-line arguments"""
        required_keys: list = ["topics", "broker_host"]

        if self.args.get("format", "json") in ["json", "text"]:
            if not self.args.get("table_name_field"):
                required_keys.append("table_name")

        if not self.args or any(key not in self.args for key in required_keys):
            raise ValueError(
                f"Missing some of the required arguments: {', '.join(required_keys)}"
            )

        # Parse space-separated topics string into list
        topics_arg: str = self.args.get("topics")
        topics_list: list[str] = topics_arg.split()

        # Build auth config if credentials provided
        auth_config: dict = {}
        username: str | None = self.args.get("username")
        password: str | None = self.args.get("password")
        if username and password:
            auth_config["username"] = username
            auth_config["password"] = password
        elif username or password:
            raise ValueError(
                "Both username and password must be provided for authentication"
            )

        # Build TLS config if certificates provided
        tls_config: dict = {}
        ca_cert: str | None = self.args.get("ca_cert")
        client_cert: str | None = self.args.get("client_cert")
        client_key: str | None = self.args.get("client_key")
        if ca_cert:
            tls_config = {"ca_cert": ca_cert}
            # For mutual TLS, both client cert and key are required
            if client_cert and client_key:
                tls_config["client_cert"] = client_cert
                tls_config["client_key"] = client_key
            elif client_cert or client_key:
                raise ValueError(
                    "Both client_cert and client_key must be provided for mutual TLS"
                )

        return {
            "mqtt": {
                "broker_host": self.args.get("broker_host"),
                "broker_port": int(self.args.get("broker_port", 1883)),
                "topics": topics_list,
                "qos": int(self.args.get("qos", 1)),
                "client_id": self.args.get("client_id", "influxdb3_mqtt_subscriber"),
                "format": self.args.get("format", "json"),
                "auth": auth_config,
                "tls": tls_config,
            },
            "mapping": self._build_mapping_from_args(),
        }

    def _build_mapping_from_args(self) -> dict[str, Any]:
        """Build mapping configuration from args (supports JSON and text formats)

        JSON format:
        - tags: "room sensor location" (space-separated) -> {'room': '$.room', ...}
        - fields: "temp:float=temperature hum:int=humidity" (name:type=jsonpath without $.) -> {'temp': ['$.temperature', 'float'], ...}
        - timestamp_field: "timestamp:ms" (field_name:format) -> {'field': '$.timestamp', 'format': 'ms'}

        Text format:
        - tags: "room=([^,\\s]+) sensor=(\\w+)" (space-separated name=regex) -> {'room': 'regex', ...}
        - fields: "temp:float=([\\d.]+) hum:int=(\\d+)" (name:type=regex) -> {'temp': ['regex', 'float'], ...}
        - timestamp_field: "ts:(\\d+):ms" (regex:format) -> {'field': 'regex', 'format': 'ms'}

        Note: For complex patterns/paths, use TOML configuration instead.
        """
        message_format: str = self.args.get("format", "json")

        if message_format == "json":
            return self._build_json_mapping_from_args()
        elif message_format == "text":
            return self._build_text_mapping_from_args()
        elif message_format == "lineprotocol":
            # Line protocol doesn't need mapping - messages are passed through directly
            return {}
        else:
            raise ValueError(
                f"Unsupported format: {message_format}. Use 'json', 'text', or 'lineprotocol'."
            )

    def _build_json_mapping_from_args(self) -> dict[str, Any]:
        """Build JSON mapping configuration from args

        Format: field:type=jsonpath (without $.)
        Example: "temp:float=temperature hum:int=humidity"
        """
        # Parse tags
        tags_config: dict = {}
        tags_arg: str | None = self.args.get("tags")
        if tags_arg:
            tag_names: list[str] = tags_arg.split(" ")
            for tag_name in tag_names:
                tag_name = tag_name.strip()
                if tag_name:
                    tags_config[tag_name] = f"$.{tag_name}"

        # Parse fields: field:type=jsonpath
        fields_config: dict = {}
        fields_arg: str | None = self.args.get("fields")
        if fields_arg:
            field_specs: list[str] = fields_arg.split(" ")
            for field_spec in field_specs:
                field_spec = field_spec.strip()
                if not field_spec:
                    continue

                # Split by first ':' to get field_name and rest
                if ":" not in field_spec:
                    raise ValueError(
                        f"Invalid field specification: '{field_spec}'. Expected format: 'name:type=jsonpath'"
                    )

                field_name, rest = field_spec.split(":", 1)
                field_name = field_name.strip()

                # Split rest by first '=' to get field_type and json_path
                if "=" not in rest:
                    raise ValueError(
                        f"Invalid field specification: '{field_spec}'. Expected format: 'name:type=jsonpath'"
                    )

                field_type, json_path = rest.split("=", 1)
                field_type = field_type.strip()
                json_path = json_path.strip()

                # Validate field type
                if field_type not in self.VALID_FIELD_TYPES:
                    raise ValueError(
                        f"Invalid field type '{field_type}' in field specification '{field_spec}'. "
                        f"Supported types: {', '.join(sorted(self.VALID_FIELD_TYPES))}"
                    )

                if field_name and field_type and json_path:
                    # Add $. prefix to json path
                    fields_config[field_name] = [f"$.{json_path}", field_type]

        # Parse timestamp field (format: "field_name:time_format")
        timestamp_config: dict | None = None
        timestamp_field_arg: str | None = self.args.get("timestamp_field")
        if timestamp_field_arg:
            if ":" in timestamp_field_arg:
                field_name, time_format = timestamp_field_arg.split(":", 1)
                field_name = field_name.strip()
                time_format = time_format.strip()

                # Validate timestamp format
                if time_format not in self.VALID_TIMESTAMP_FORMATS:
                    raise ValueError(
                        f"Invalid timestamp format: '{time_format}'. "
                        f"Supported formats: {', '.join(sorted(self.VALID_TIMESTAMP_FORMATS))}"
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
        """Build text mapping configuration from args

        Format: field:type=regex
        Example: "temp:float=([\\d.]+) hum:int=(\\d+)"
        """
        # Parse tags: name=pattern
        tags_config: dict = {}
        tags_arg: str | None = self.args.get("tags")
        if tags_arg:
            tag_specs: list[str] = tags_arg.split(" ")
            for tag_spec in tag_specs:
                tag_spec = tag_spec.strip()
                if not tag_spec:
                    continue

                # Split by first '=' to get tag_name and pattern
                if "=" not in tag_spec:
                    raise ValueError(
                        f"Invalid tag specification: '{tag_spec}'. Expected format: 'name=pattern'"
                    )

                tag_name, pattern = tag_spec.split("=", 1)
                tag_name = tag_name.strip()
                pattern = pattern.strip()

                if tag_name and pattern:
                    tags_config[tag_name] = pattern

        # Parse fields: field:type=regex
        fields_config: dict = {}
        fields_arg = self.args.get("fields")
        if fields_arg:
            field_specs: list = fields_arg.split(" ")
            for field_spec in field_specs:
                field_spec = field_spec.strip()
                if not field_spec:
                    continue

                # Split by first ':' to get field_name and rest
                if ":" not in field_spec:
                    raise ValueError(
                        f"Invalid field specification: '{field_spec}'. Expected format: 'name:type=pattern'"
                    )

                field_name, rest = field_spec.split(":", 1)
                field_name = field_name.strip()

                # Split rest by first '=' to get field_type and pattern
                if "=" not in rest:
                    raise ValueError(
                        f"Invalid field specification: '{field_spec}'. Expected format: 'name:type=pattern'"
                    )

                field_type, pattern = rest.split("=", 1)
                field_type = field_type.strip()
                pattern = pattern.strip()

                # Validate field type
                if field_type not in self.VALID_FIELD_TYPES:
                    raise ValueError(
                        f"Invalid field type '{field_type}' for field '{field_name}'. "
                        f"Supported types: {', '.join(sorted(self.VALID_FIELD_TYPES))}"
                    )

                if field_name and field_type and pattern:
                    fields_config[field_name] = [pattern, field_type]

        # Parse timestamp field (format: "regex:time_format")
        timestamp_config: dict | None = None
        timestamp_field_arg: str | None = self.args.get("timestamp_field")
        if timestamp_field_arg:
            # For text format, timestamp_field is "regex:format"
            # Need to find last ':' to split regex from format
            if ":" in timestamp_field_arg:
                # Split by last ':' to handle regex with ':' inside
                pattern, time_format = timestamp_field_arg.rsplit(":", 1)
                pattern = pattern.strip()
                time_format = time_format.strip()

                # Validate timestamp format
                if time_format not in self.VALID_TIMESTAMP_FORMATS:
                    raise ValueError(
                        f"Invalid timestamp format: '{time_format}'. "
                        f"Supported formats: {', '.join(sorted(self.VALID_TIMESTAMP_FORMATS))}"
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

    def get_mqtt_config(self) -> dict[str, Any]:
        """Get MQTT connection configuration"""
        return self.config.get("mqtt")

    def get_mapping_config(self, format_type: str) -> dict[str, Any]:
        """Get mapping configuration for specified format"""
        return self.config.get("mapping").get(format_type)


class MQTTConnectionManager:
    """Manages MQTT client connection and message queue"""

    def __init__(self, config: dict[str, Any], influxdb3_local, task_id):
        self.config: dict[str, Any] = config
        self.influxdb3_local = influxdb3_local
        self.task_id: str = task_id
        self.client = None
        self.message_queue: Queue = Queue()
        self.connected: bool = False
        self.subscribed_topics: set[str] = set()

    @staticmethod
    def _create_mqtt_client(client_id: str):
        """Create MQTT client"""
        return Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=client_id,
            clean_session=False,
        )

    @staticmethod
    def _resolve_path(path: str, description: str) -> str:
        """Resolve path - absolute paths used as-is, relative paths resolved from PLUGIN_DIR.

        Args:
            path: File path (absolute or relative)
            description: Description for error messages (e.g., "CA certificate")

        Returns:
            Resolved absolute path

        Raises:
            ValueError: If path is relative and PLUGIN_DIR is not set
        """
        if os.path.isabs(path):
            return path

        plugin_dir: str | None = os.environ.get("PLUGIN_DIR")
        if not plugin_dir:
            raise ValueError(
                f"PLUGIN_DIR environment variable not set. "
                f"Required for relative {description} path: {path}"
            )
        return os.path.join(plugin_dir, path)

    def _configure_tls(self, tls_config: dict[str, Any]):
        """Configure TLS/SSL for the MQTT client"""
        ca_cert: str | None = tls_config.get("ca_cert")
        client_cert: str | None = tls_config.get("client_cert")
        client_key: str | None = tls_config.get("client_key")

        # Only configure TLS if at least CA cert is provided
        if not ca_cert:
            self.influxdb3_local.info(
                f"[{self.task_id}] No ca_cert specified - skipping TLS configuration"
            )
            return

        # Resolve paths (absolute used as-is, relative resolved from PLUGIN_DIR)
        ca_cert = self._resolve_path(ca_cert, "CA certificate")
        if client_cert:
            client_cert = self._resolve_path(client_cert, "client certificate")
        if client_key:
            client_key = self._resolve_path(client_key, "client key")

        # Validate certificate files exist
        if not os.path.exists(ca_cert):
            raise FileNotFoundError(f"CA certificate not found: {ca_cert}")

        if client_cert and not os.path.exists(client_cert):
            raise FileNotFoundError(f"Client certificate not found: {client_cert}")

        if client_key and not os.path.exists(client_key):
            raise FileNotFoundError(f"Client key not found: {client_key}")

        self.client.tls_set(ca_certs=ca_cert, certfile=client_cert, keyfile=client_key)
        self.influxdb3_local.info(f"[{self.task_id}] TLS configured successfully")

    def connect(self) -> bool:
        """Establish connection to MQTT broker"""
        try:
            # Create client
            client_id: str = self.config.get("client_id", "influxdb3_mqtt_subscriber")
            self.client = self._create_mqtt_client(client_id)

            # Set callbacks
            self.client.on_connect = self._on_connect
            self.client.on_disconnect = self._on_disconnect
            self.client.on_message = self._on_message

            # Configure authentication if provided
            if "auth" in self.config:
                username: str | None = self.config["auth"].get("username")
                password: str | None = self.config["auth"].get("password")
                if username:
                    self.client.username_pw_set(username, password)

            # Configure TLS if provided
            if "tls" in self.config:
                tls_config: dict = self.config["tls"]
                self._configure_tls(tls_config)

            # Connect to broker
            broker: str = self.config.get("broker_host")
            port: int = self.config.get("broker_port", 1883)

            self.influxdb3_local.info(
                f"[{self.task_id}] Connecting to MQTT broker: {broker}:{port}"
            )
            self.client.connect(broker, port)

            # Start network loop in background
            self.client.loop_start()

            # Wait for connection (with timeout)
            timeout: int = 10
            start_time: float = time.time()
            while not self.connected and (time.time() - start_time) < timeout:
                time.sleep(0.1)

            if not self.connected:
                self.influxdb3_local.error(
                    f"[{self.task_id}] Failed to connect to MQTT broker within timeout"
                )
                return False

            return True

        except Exception as e:
            self.influxdb3_local.error(
                f"[{self.task_id}] Error connecting to MQTT broker: {str(e)}"
            )
            return False

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        """Callback when client connects to broker (API VERSION2)"""
        if reason_code == 0:
            self.connected = True
            self.influxdb3_local.info(
                f"[{self.task_id}] MQTT client connected successfully"
            )

            # Re-subscribe to topics on connect (handles reconnection and persistent session)
            topics = self.config.get("topics", [])
            qos = self.config.get("qos", 1)
            for topic in topics:
                if topic not in self.subscribed_topics:
                    try:
                        client.subscribe(topic, qos)
                        self.subscribed_topics.add(topic)
                        self.influxdb3_local.info(
                            f"[{self.task_id}] Subscribed to topic: {topic} (QoS {qos})"
                        )
                    except Exception as e:
                        self.influxdb3_local.error(
                            f"[{self.task_id}] Error subscribing to topic {topic}: {str(e)}"
                        )
        else:
            self.connected = False
            self.influxdb3_local.error(
                f"[{self.task_id}] MQTT connection failed with code: {reason_code}"
            )

    def _on_disconnect(
        self, client, userdata, disconnect_flags, reason_code, properties
    ):
        """Callback when client disconnects from broker (API VERSION2)"""
        self.connected = False
        # Don't log warning on intentional disconnect (reason_code == 0)
        if reason_code != 0:
            self.influxdb3_local.info(
                f"[{self.task_id}] MQTT disconnected with code: {reason_code}"
            )

    def _on_message(self, client, userdata, msg):
        """Callback when message is received"""
        try:
            # Attempt to decode payload as UTF-8
            try:
                payload: str = msg.payload.decode("utf-8")
            except UnicodeDecodeError:
                # Binary payload - skip with warning
                self.influxdb3_local.warn(
                    f"[{self.task_id}] Skipping binary message on topic {msg.topic} ({len(msg.payload)} bytes) - only UTF-8 text supported"
                )
                return

            # Skip empty payloads
            if not payload or not payload.strip():
                self.influxdb3_local.warn(
                    f"[{self.task_id}] Skipping empty message on topic {msg.topic}"
                )
                return

            # Add message to queue
            message_data: dict[str, Any] = {
                "topic": msg.topic,
                "payload": payload,
                "qos": msg.qos,
            }

            self.message_queue.put(message_data)

        except Exception as e:
            self.influxdb3_local.error(
                f"[{self.task_id}] Error processing MQTT message: {str(e)}"
            )

    def get_messages(self) -> list[dict[str, Any]]:
        """Retrieve all messages from queue"""
        messages: list = []
        try:
            while True:
                message = self.message_queue.get_nowait()
                messages.append(message)
        except Empty:
            pass

        return messages

    def disconnect(self):
        """Disconnect from MQTT broker"""
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            self.connected = False
            self.influxdb3_local.info(f"[{self.task_id}] Disconnected from MQTT broker")


class JSONParser:
    """Parse JSON messages and convert to Line Protocol"""

    def __init__(self, mapping_config: dict[str, Any], task_id: str, influxdb3_local):
        self.mapping_config: dict = mapping_config
        self.task_id: str = task_id
        self.influxdb3_local = influxdb3_local
        # Pre-compile JSONPath expressions for performance
        self._compiled_paths: dict[str, Any] = self._compile_jsonpath_expressions()

    def _compile_jsonpath_expressions(self) -> dict[str, Any]:
        """Pre-compile all JSONPath expressions for performance optimization.

        Returns:
            Dict mapping path keys to compiled jsonpath expressions
        """
        compiled: dict[str, Any] = {}

        # Compile table_name_field if present
        table_name_field = self.mapping_config.get("table_name_field")
        if table_name_field:
            compiled["table_name"] = jsonpath_parse(table_name_field)

        # Compile tags
        tags_config = self.mapping_config.get("tags", {})
        for tag_key, json_path in tags_config.items():
            compiled[f"tag:{tag_key}"] = jsonpath_parse(json_path)

        # Compile fields
        fields_config = self.mapping_config.get("fields", {})
        for field_key, field_spec in fields_config.items():
            json_path = field_spec[0]
            compiled[f"field:{field_key}"] = jsonpath_parse(json_path)

        # Compile timestamp
        timestamp_config = self.mapping_config.get("timestamp_config")
        if timestamp_config:
            field_path = timestamp_config.get("field")
            if field_path:
                compiled["timestamp"] = jsonpath_parse(field_path)

        return compiled

    def parse(self, payload: str) -> list[LineBuilder]:
        """Parse JSON payload and return list of LineBuilder objects.

        Handles both single JSON objects and arrays of objects.
        Returns a list (possibly with one element for single objects).
        """
        try:
            data: dict = json.loads(payload)

            # Handle JSON array - parse each element
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
                            f"[{self.task_id}] Skipping non-object array element at index {i}"
                        )
                        continue
                    try:
                        line: LineBuilder = self._parse_object(item)
                        if line:
                            results.append(line)
                    except Exception as e:
                        # Log but continue processing other array elements
                        self.influxdb3_local.warn(
                            f"[{self.task_id}] Error parsing array element {i}: {str(e)}"
                        )

                if len(results) == 0:
                    self.influxdb3_local.warn(
                        f"[{self.task_id}] No valid objects in JSON array, skipping"
                    )
                    return []

                return results

            # Handle single JSON object
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

    def _parse_object(self, data: dict[str, Any]) -> LineBuilder:
        """Parse a single JSON object and return LineBuilder"""
        # Get table name
        table_name: str | None = self._get_table_name(data)
        if not table_name:
            raise ValueError("Could not determine table name")

        # Create LineBuilder
        line = LineBuilder(table_name)

        # Add tags
        self._add_tags(line, data)

        # Add fields
        field_count: int = self._add_fields(line, data)

        if field_count == 0:
            raise ValueError("No fields were mapped from JSON data")

        # Add timestamp
        timestamp_ns: int | None = self._get_timestamp(data)
        if timestamp_ns:
            line.time_ns(timestamp_ns)

        return line

    def _get_table_name(self, data: dict[str, Any]) -> str | None:
        """Get table name from config or data"""
        # Check for static table name
        table_name: str | None = self.mapping_config.get("table_name")
        if table_name:
            return table_name

        # Check for table name field
        table_name_field: str | None = self.mapping_config.get("table_name_field")
        if table_name_field:
            return self._get_json_value(data, table_name_field, "table_name")

        return None

    def _add_tags(self, line: LineBuilder, data: dict[str, Any]):
        """Add tags to LineBuilder from JSON data"""
        tags_config: dict[str, Any] = self.mapping_config.get("tags", {})

        for tag_key, json_path in tags_config.items():
            value: Any = self._get_json_value(data, json_path, f"tag:{tag_key}")
            if value is not None:
                line.tag(tag_key, str(value))

    def _add_fields(self, line: LineBuilder, data: dict[str, Any]) -> int:
        """Add fields to LineBuilder from JSON data. Returns count of fields added.

        Field format: [json_path, type]
        Example: temperature = ["$.temp", "float"]
        """
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
                    raise ValueError(f"Failed to convert field '{field_key}' to {field_type}: {e}")
                field_count += 1

        return field_count

    def _get_timestamp(self, data: dict[str, Any]) -> int | None:
        """Get timestamp from data with format conversion or use received timestamp

        Supports:
        - ns: nanoseconds (direct passthrough)
        - ms: milliseconds (convert to nanoseconds)
        - s: seconds (convert to nanoseconds)
        - datetime: ISO 8601 string (parse and convert to nanoseconds)
        """
        # Check for timestamp_config (unified format from both args and TOML)
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
                f"[{self.task_id}] Failed to convert timestamp '{timestamp_value}' with format '{time_format}': {str(e)}"
            )
            return time.time_ns()

    def _get_json_value(self, data: dict[str, Any], path: str, cache_key: str | None = None) -> Any:
        """Get value from JSON using JSONPath notation.

        Args:
            data: JSON data to search in
            path: JSONPath expression (e.g., '$.temperature')
            cache_key: Optional key to use cached compiled expression

        Examples:
            $.temperature -> data['temperature']
            $.sensor.id -> data['sensor']['id']
            $[0].value -> data[0]['value']
        """
        try:
            # Use cached expression if available
            if cache_key and cache_key in self._compiled_paths:
                jsonpath_expr = self._compiled_paths[cache_key]
            else:
                # Fall back to parsing on-demand (for dynamic paths)
                jsonpath_expr = jsonpath_parse(path)

            # Find matches
            matches = jsonpath_expr.find(data)

            # Return first match value, or None if no matches
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

    def parse(self, payload: str) -> LineBuilder:
        """Parse line protocol string and return LineBuilder object

        Format: measurement,tag1=val1,tag2=val2 field1=val1,field2=val2 timestamp
        """
        try:
            payload = payload.strip()

            # Split into parts: measurement+tags, fields, timestamp (optional)
            # Must respect quoted strings when splitting by spaces
            parts: list[str] = self._split_quoted(payload, " ", max_splits=2, skip_empty=True)

            if len(parts) < 2:
                raise ValueError("Invalid line protocol format: missing field set")

            measurement_and_tags: str = parts[0]
            fields_str: str = parts[1]
            timestamp_ns: int | None = int(parts[2]) if len(parts) == 3 else None

            # Parse measurement and tags
            measurement, tags = self._parse_measurement_and_tags(measurement_and_tags)

            # Create LineBuilder
            line = LineBuilder(measurement)

            # Add tags
            for tag_key, tag_value in tags.items():
                line.tag(tag_key, tag_value)

            # Parse and add fields
            fields: dict = self._parse_fields(fields_str)
            if len(fields) == 0:
                raise ValueError("No fields found in line protocol")

            for field_key, (field_value, field_type) in fields.items():
                add_field_with_type(line, field_key, field_value, field_type)

            # Add timestamp
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
        """Split text by delimiter, respecting quoted strings.

        Args:
            text: Text to split
            delimiter: Character to split by (e.g., ' ' or ',')
            max_splits: Maximum number of splits (-1 for unlimited)
            skip_empty: If True, skip empty parts (used for space splitting)

        Returns:
            List of parts
        """
        parts: list = []
        current: list = []
        in_quotes: bool = False
        splits_made: int = 0

        for char in text:
            if char == '"':
                # Count consecutive backslashes before the quote
                backslash_count = 0
                i = len(current) - 1
                while i >= 0 and current[i] == "\\":
                    backslash_count += 1
                    i -= 1

                # Toggle quotes only if even number of backslashes (including 0)
                if backslash_count % 2 == 0:
                    in_quotes = not in_quotes

                current.append(char)
            elif char == delimiter and not in_quotes:
                # Check if we've reached max_splits
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
        """Parse measurement and tags from first part of line protocol

        Tags are optional. Valid formats:
        - measurement (no tags)
        - measurement,tag1=val1,tag2=val2 (with tags)
        """
        parts: list = measurement_and_tags.split(",")
        measurement: str = parts[0]

        tags: dict = {}
        for tag_part in parts[1:]:
            if "=" in tag_part:
                key, value = tag_part.split("=", 1)
                tags[key] = self._unescape_value(value)
            else:
                raise ValueError(
                    f"Invalid line protocol format: tag must be in 'key=value' format, got '{tag_part}'"
                )

        return measurement, tags

    def _parse_fields(self, fields_str: str) -> dict[str, tuple[Any, str]]:
        """Parse fields from line protocol

        Returns: {field_name: (value, type)}
        Types: 'int', 'uint', 'float', 'string', 'bool'
        """
        fields: dict = {}

        # Split by comma, but respect quoted strings
        field_parts: list = self._split_quoted(fields_str, ",")

        for field_part in field_parts:
            if "=" not in field_part:
                continue

            key, value_str = field_part.split("=", 1)
            key = key.strip()
            value_str = value_str.strip()

            # Determine type and parse value
            value, field_type = self._parse_field_value(value_str)
            fields[key] = (value, field_type)

        return fields

    def _parse_field_value(self, value_str: str) -> tuple[Any, str]:
        """Parse field value and determine its type

        Returns: (parsed_value, type)
        """
        # String (quoted)
        if value_str.startswith('"') and value_str.endswith('"') and len(value_str) >= 2:
            return self._unescape_value(value_str[1:-1]), "string"

        # Integer (ends with 'i')
        if value_str.endswith("i"):
            return int(value_str[:-1]), "int"

        # Unsigned integer (ends with 'u')
        if value_str.endswith("u"):
            return int(value_str[:-1]), "uint"

        # Boolean
        lower_val: str = value_str.lower()
        if lower_val in ("true", "t", "false", "f"):
            return lower_val in ("true", "t"), "bool"

        # Float (default for numeric values)
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
        # Pre-compile regex patterns for performance
        self._compiled_patterns: dict[str, re.Pattern] = self._compile_regex_patterns()

    def _compile_regex_patterns(self) -> dict[str, re.Pattern]:
        """Pre-compile all regex patterns for performance optimization.

        Returns:
            Dict mapping pattern keys to compiled regex patterns
        """
        compiled: dict[str, re.Pattern] = {}

        # Compile table_name_field if present
        table_name_field = self.mapping_config.get("table_name_field")
        if table_name_field:
            try:
                compiled["table_name"] = re.compile(table_name_field)
            except re.error as e:
                self.influxdb3_local.warn(
                    f"[{self.task_id}] Invalid regex for table_name_field: {e}"
                )

        # Compile tags
        tags_config = self.mapping_config.get("tags", {})
        for tag_key, pattern_str in tags_config.items():
            try:
                compiled[f"tag:{tag_key}"] = re.compile(pattern_str)
            except re.error as e:
                self.influxdb3_local.warn(
                    f"[{self.task_id}] Invalid regex for tag '{tag_key}': {e}"
                )

        # Compile fields
        fields_config = self.mapping_config.get("fields", {})
        for field_key, pattern_config in fields_config.items():
            pattern_str = pattern_config[0]
            try:
                compiled[f"field:{field_key}"] = re.compile(pattern_str)
            except re.error as e:
                self.influxdb3_local.warn(
                    f"[{self.task_id}] Invalid regex for field '{field_key}': {e}"
                )

        # Compile timestamp
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
        """Get table name from config or extract from payload using regex

        Priority:
        1. Static table_name if present
        2. Dynamic table_name_field (regex pattern) to extract from payload
        """
        # Check for static table name first
        table_name: str | None = self.mapping_config.get("table_name")
        if table_name:
            return table_name

        # Check for dynamic table name field (regex pattern)
        table_name_field: str | None = self.mapping_config.get("table_name_field")
        if table_name_field:
            return self._extract_value(payload, table_name_field, "table_name", "table_name")

        return None

    def parse(self, payload: str) -> LineBuilder:
        """Parse text payload using individual regex patterns

        Config format (TOML):
        [mapping.text]
        table_name = "sensor_data"  # Static table name (or use table_name_field for dynamic)
        # table_name_field = "measurement=([\\w]+)"  # Extract table name from payload
        timestamp_field = "ts:(\\d+):ms"  # Format: "pattern:format" (optional)

        [mapping.text.tags]
        sensor_id = "sensor:(\\w+)"  # Regex with capturing group

        [mapping.text.fields]
        temperature = ["temp:([\\d.]+)", "float"]  # Format: [pattern, type]
        humidity = ["hum:(\\d+)", "int"]  # Types: int, uint, float, string, bool
        """
        try:
            # Get table name (required)
            table_name: str | None = self._get_table_name(payload)
            if not table_name:
                raise ValueError(
                    "Could not determine table name from text mapping configuration"
                )

            line = LineBuilder(table_name)

            # Extract and add tags using individual patterns
            tags_config: dict = self.mapping_config.get("tags", {})
            for tag_key, pattern_str in tags_config.items():
                value: str | None = self._extract_value(payload, pattern_str, tag_key, f"tag:{tag_key}")
                if value is not None:
                    line.tag(tag_key, value)

            # Extract and add fields using individual patterns
            fields_config: dict = self.mapping_config.get("fields", {})
            field_count: int = 0

            if not fields_config:
                raise ValueError(
                    "No field patterns configured. Please specify fields in configuration."
                )

            for field_key, pattern_config in fields_config.items():
                # pattern_config is always [pattern, type]
                pattern_str = pattern_config[0]
                field_type = pattern_config[1]

                value = self._extract_value(payload, pattern_str, field_key, f"field:{field_key}")
                if value is not None:
                    try:
                        add_field_with_type(line, field_key, value, field_type)
                    except (ValueError, TypeError) as e:
                        self.influxdb3_local.error(
                            f"[{self.task_id}] Failed to convert field '{field_key}' value '{value}' to type '{field_type}': {str(e)}"
                        )
                        raise
                    field_count += 1

            if field_count == 0:
                raise ValueError("No fields were extracted from text message")

            # Add timestamp
            timestamp_ns: int = self._get_timestamp(payload)
            line.time_ns(timestamp_ns)

            return line

        except Exception as e:
            self.influxdb3_local.error(f"[{self.task_id}] Error parsing text: {str(e)}")
            raise

    def _extract_value(
        self, text: str, pattern_str: str, field_name: str, cache_key: str | None = None
    ) -> str | None:
        """Extract value from text using regex pattern

        Args:
            text: Text to search in
            pattern_str: Regex pattern (should contain one capturing group)
            field_name: Name of field (for error messages)
            cache_key: Optional key to use cached compiled pattern

        Returns:
            Extracted value or None if not found
        """
        try:
            # Use cached pattern if available
            if cache_key and cache_key in self._compiled_patterns:
                pattern = self._compiled_patterns[cache_key]
            else:
                # Fall back to compiling on-demand (for dynamic patterns)
                pattern = re.compile(pattern_str)

            match = pattern.search(text)

            if not match:
                self.influxdb3_local.warn(
                    f"[{self.task_id}] Pattern for '{field_name}' did not match: {pattern_str}"
                )
                return None

            # Try to get first capturing group
            if match.groups():
                return match.group(1)
            else:
                # If no capturing group, return the whole match
                return match.group(0)

        except re.error as e:
            self.influxdb3_local.error(
                f"[{self.task_id}] Invalid regex pattern for '{field_name}': {pattern_str} - {e}"
            )
            return None

    def _get_timestamp(self, payload: str) -> int:
        """Extract and convert timestamp from text payload

        Checks for timestamp configuration in mapping:
        - timestamp_config.field: regex pattern to extract timestamp value
        - timestamp_config.format: format (ns, ms, s, datetime)

        If no timestamp found or configured, returns current time.
        """
        timestamp_config: dict = self.mapping_config.get("timestamp_config")
        if not timestamp_config:
            return time.time_ns()

        # Extract timestamp using pattern (stored in 'field' key for consistency with JSON)
        pattern_str: str = timestamp_config.get("field")
        if not pattern_str:
            return time.time_ns()

        timestamp_value: Any = self._extract_value(payload, pattern_str, "timestamp", "timestamp")
        if timestamp_value is None:
            return time.time_ns()

        # Convert timestamp based on format
        time_format = timestamp_config.get("format", "ns")
        try:
            return convert_timestamp(timestamp_value, time_format)
        except Exception as e:
            self.influxdb3_local.error(
                f"[{self.task_id}] Failed to convert timestamp '{timestamp_value}' with format '{time_format}': {str(e)}"
            )
            return time.time_ns()


class MQTTStats:
    """Track and persist MQTT plugin statistics"""

    def __init__(self):
        self.reset()

    def reset(self):
        """Reset all statistics"""
        self.messages_received: int = 0
        self.messages_processed: int = 0
        self.messages_failed: int = 0
        # Track detailed stats per topic: {topic: {received, processed, failed}}
        self.stats_by_topic: dict = {}
        self.last_message_time: int | None = None
        self.current_topic: str | None = None  # Track current topic being processed

    def record_message_received(self, topic: str, count: int = 1):
        """Record received message(s)"""
        self.messages_received += count
        self.last_message_time = time.time_ns()
        self.current_topic = topic

        # Initialize topic stats if needed
        if topic not in self.stats_by_topic:
            self.stats_by_topic[topic] = {"received": 0, "processed": 0, "failed": 0}

        self.stats_by_topic[topic]["received"] += count

    def record_message_processed(self, count: int = 1):
        """Record successfully processed message(s)"""
        self.messages_processed += count

        # Update current topic stats
        if self.current_topic and self.current_topic in self.stats_by_topic:
            self.stats_by_topic[self.current_topic]["processed"] += count

    def record_message_failed(self, count: int = 1):
        """Record failed message(s)"""
        self.messages_failed += count

        # Update current topic stats
        if self.current_topic and self.current_topic in self.stats_by_topic:
            self.stats_by_topic[self.current_topic]["failed"] += count

    def get_topic_stats(self) -> dict[str, dict[str, Any]]:
        """Get statistics by topic with calculated success rates"""
        result: dict = {}
        for topic, stats in self.stats_by_topic.items():
            total: int = stats["processed"] + stats["failed"]
            success_rate: float = (
                (stats["processed"] / total * 100) if total > 0 else 0.0
            )

            result[topic] = {
                "received": stats["received"],
                "processed": stats["processed"],
                "failed": stats["failed"],
                "success_rate": round(success_rate, 2),
            }
        return result


def write_stats(influxdb3_local, stats: MQTTStats, broker_host: str, task_id: str):
    """Write per-topic statistics to mqtt_stats table.

    Args:
        influxdb3_local: InfluxDB local API
        stats: MQTTStats instance with collected statistics
        broker_host: MQTT broker address (host:port format)
        task_id: Task identifier for logging
    """
    try:
        topic_stats: dict = stats.get_topic_stats()
        lines: list = []

        # Build statistics for each topic
        for topic, topic_data in topic_stats.items():
            line = LineBuilder("mqtt_stats")

            # Add tags
            line.tag("topic", topic)
            line.tag("broker_host", broker_host)

            # Add fields
            line.int64_field("messages_received", topic_data["received"])
            line.int64_field("messages_processed", topic_data["processed"])
            line.int64_field("messages_failed", topic_data["failed"])
            line.float64_field("success_rate", topic_data["success_rate"])

            line.time_ns(time.time_ns())
            lines.append(line)

        if lines:
            influxdb3_local.write_sync(_BatchLines(lines), no_sync=True)

        influxdb3_local.info(
            f"[{task_id}] Wrote statistics for {len(topic_stats)} topics to mqtt_stats table"
        )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to write statistics: {str(e)}")


def write_exception(
    influxdb3_local,
    topic: str,
    error_type: str,
    error_message: str,
    raw_message: str,
    task_id: str,
):
    """Write exception to mqtt_exceptions table.

    Args:
        influxdb3_local: InfluxDB local API
        topic: MQTT topic where error occurred
        error_type: Type of exception (e.g., JSONDecodeError)
        error_message: Detailed error message
        raw_message: Original message payload (truncated to 1KB by caller)
        task_id: Task identifier for logging
    """
    try:
        line = LineBuilder("mqtt_exceptions")
        line.tag("topic", topic)
        line.tag("error_type", error_type)
        line.string_field("error_message", error_message)
        line.string_field("raw_message", raw_message)
        line.time_ns(time.time_ns())

        influxdb3_local.write_sync(line, no_sync=True)
        influxdb3_local.info(
            f"[{task_id}] Wrote exception to mqtt_exceptions table: {error_type}"
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
    mqtt_client: MQTTConnectionManager | None = None

    if not args:
        influxdb3_local.error(f"[{task_id}] No arguments provided")
        return

    try:
        # Load configuration from cache or parse fresh
        cached_config: dict | None = influxdb3_local.cache.get("mqtt_config")
        if cached_config is None:
            config_loader: MQTTConfig = MQTTConfig(influxdb3_local, args, task_id)
            cached_config = {
                "mqtt": config_loader.get_mqtt_config(),
                "mapping": {
                    "json": config_loader.get_mapping_config("json"),
                    "text": config_loader.get_mapping_config("text"),
                },
            }
            influxdb3_local.cache.put("mqtt_config", cached_config)
            influxdb3_local.info(
                f"[{task_id}] MQTT Plugin initialized, format: {cached_config['mqtt']['format']}"
            )

        mqtt_config: dict = cached_config["mqtt"]
        message_format: str = mqtt_config["format"]

        # Get or create stats tracker from cache
        stats: MQTTStats | None = influxdb3_local.cache.get("mqtt_stats")
        if stats is None:
            stats = MQTTStats()
            influxdb3_local.cache.put("mqtt_stats", stats)

        # Create new MQTT connection (don't use cache)
        influxdb3_local.info(f"[{task_id}] Creating new MQTT connection")
        mqtt_client = MQTTConnectionManager(mqtt_config, influxdb3_local, task_id)

        if not mqtt_client.connect():
            influxdb3_local.error(f"[{task_id}] Failed to connect to MQTT broker")
            return

        # Wait for messages to arrive after subscription (happens in _on_connect callback)
        time.sleep(0.5)

        # Retrieve messages from queue
        messages: list = mqtt_client.get_messages()

        # Write stats every 10 calls (even if no messages)
        call_count: int = influxdb3_local.cache.get("mqtt_call_count")
        if call_count is None:
            call_count = 0

        call_count += 1

        broker_str: str = (
            f"{mqtt_config.get('broker_host')}:{mqtt_config.get('broker_port')}"
        )
        if call_count >= 10:
            write_stats(influxdb3_local, stats, broker_str, task_id)
            call_count = 0

        influxdb3_local.cache.put("mqtt_call_count", call_count)

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
        # Per-message parse results: (msg, "ok", line_count) or (msg, "fail", exception)
        parse_results: list[tuple] = []

        for msg in messages:
            topic: str = msg.get("topic", "unknown")
            payload: str = msg.get("payload", "")
            stats.record_message_received(topic)

            try:
                if message_format == "json":
                    line_builders: list = parser.parse(payload)
                    all_line_builders.extend(line_builders)
                    parse_results.append((msg, "ok", len(line_builders)))
                else:
                    line_builder = parser.parse(payload)
                    if line_builder:
                        all_line_builders.append(line_builder)
                        parse_results.append((msg, "ok", 1))
                    else:
                        parse_results.append((msg, "ok", 0))

            except Exception as e:
                parse_results.append((msg, "fail", e))

        # Phase 2: Batch write all parsed lines
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

        # Phase 3: Update stats based on results
        success_count: int = 0
        error_count: int = 0

        for msg, status, result in parse_results:
            topic = msg.get("topic", "unknown")
            payload = msg.get("payload", "")

            if status == "ok" and not write_failed:
                success_count += result
                stats.record_message_processed(1)
            else:
                error_count += 1
                stats.record_message_failed()

                if status == "fail":
                    error_type: str = type(result).__name__
                    error_msg: str = str(result)
                else:
                    error_type = "BatchWriteError"
                    error_msg = "Batch write to InfluxDB failed"

                influxdb3_local.error(
                    f"[{task_id}] Error processing message from {topic}: {error_msg}"
                )

                write_exception(
                    influxdb3_local,
                    topic,
                    error_type,
                    error_msg,
                    payload[:1000],
                    task_id,
                )

        influxdb3_local.info(
            f"[{task_id}] Data write complete: {success_count} records inserted into DB, {error_count} errors"
        )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Error in MQTT plugin: {str(e)}")
        # Clean up cached state on fatal error
        influxdb3_local.cache.delete("mqtt_config")

    finally:
        # Always disconnect from broker
        if mqtt_client is not None:
            mqtt_client.disconnect()
