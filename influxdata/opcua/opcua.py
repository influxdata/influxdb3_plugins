"""
{
    "plugin_type": ["scheduled"],
    "scheduled_args_config": [
        {
            "name": "config_file_path",
            "example": "opcua_config.toml",
            "description": "Path to TOML configuration file (absolute or relative to PLUGIN_DIR).",
            "required": false
        },
        {
            "name": "server_url",
            "example": "opc.tcp://localhost:4840",
            "description": "OPC UA server endpoint URL.",
            "required": true
        },
        {
            "name": "table_name",
            "example": "opcua_data",
            "description": "InfluxDB table name (measurement) for storing data.",
            "required": true
        },
        {
            "name": "namespaces",
            "example": "siemens=urn:vendor:s7 beckhoff=urn:beckhoff:ua",
            "description": "Space-separated namespace alias mappings. Format: 'alias=namespace_uri'. Use aliases instead of numeric namespace indexes in 'nodes' and 'tag_nodes' for stable configuration that survives server restarts. The plugin resolves URIs to numeric indexes at connection time.",
            "required": false
        },
        {
            "name": "nodes",
            "example": "temperature:2:s=SpindleTemp pressure:siemens:s=CoolantPressure:float rpm:2:i=1234:uint",
            "description": "Space-separated node mappings. Format: 'field_name:namespace:identifier[:type]'. Namespace can be a numeric index (e.g. '2') or an alias defined in 'namespaces' parameter. Node ID is built as 'ns=<index>;<identifier>'. Types: int, uint, float, string, bool. Type is auto-detected from OPC UA DataType if not specified. Required unless 'browse_root' is set.",
            "required": false
        },
        {
            "name": "browse_root",
            "example": "ns=2;s=Devices",
            "description": "Root node ID for auto-discovery mode. The plugin browses the OPC UA address space from this node, discovers all Variable nodes grouped by parent Object (device), and reads their values. Required unless 'nodes' is set.",
            "required": false
        },
        {
            "name": "browse_depth",
            "example": "2",
            "description": "Maximum browse depth for auto-discovery (default: 2). Depth 1 = direct children of browse_root, depth 2 = children of children, etc.",
            "required": false
        },
        {
            "name": "path_tags",
            "example": "source zone",
            "description": "Space-separated list of tag names mapping Object hierarchy levels to InfluxDB tags in browse mode. Order corresponds to depth: first name maps to depth-1 Objects, second to depth-2, etc. Objects beyond path_tags length become field name prefixes. Length must be strictly less than browse_depth. Required when browse_root is set (use empty value for no hierarchy tags).",
            "required": false
        },
        {
            "name": "filter",
            "example": "Temperature|Pressure|Status",
            "description": "Regex pattern to filter discovered variable names in browse mode. Only variables with names matching the pattern are included.",
            "required": false
        },
        {
            "name": "exclude_branches",
            "example": "debug_device|test_bench",
            "description": "Regex pattern to exclude Object nodes (branches) by browse name during auto-discovery. Matched Objects and their entire subtree are skipped. Works at all browse depths (both path_tags and field-prefix levels).",
            "required": false
        },
        {
            "name": "browse_tags",
            "example": "room room2",
            "description": "Space-separated variable names that should be stored as InfluxDB tags instead of fields in browse mode. Values are converted to strings.",
            "required": false
        },
        {
            "name": "name_separator",
            "example": ".",
            "description": "Separator for splitting Variable browse names into segments for tag extraction. Required when 'name_tags' is set.",
            "required": false
        },
        {
            "name": "name_tags",
            "example": "source zone",
            "description": "Space-separated list of tag names extracted from leading segments of Variable browse names split by 'name_separator'. Remaining segments form the field name (joined with '_'). Requires 'name_separator'.",
            "required": false
        },
        {
            "name": "default_tags",
            "example": "location=factory_1 line=A",
            "description": "Space-separated static tags added to every data point. Format: 'key=value'.",
            "required": false
        },
        {
            "name": "tag_nodes",
            "example": "machine_id:2:s=MachineID serial:2:s=SerialNumber",
            "description": "Space-separated tag node mappings. Format: 'tag_name:namespace:identifier'. Namespace can be a numeric index or an alias defined in 'namespaces'. Values read from OPC UA nodes are used as InfluxDB tags (converted to string). Used alongside static 'default_tags'.",
            "required": false
        },
        {
            "name": "security_policy",
            "example": "Basic256Sha256",
            "description": "OPC UA security policy: 'Basic128Rsa15', 'Basic256', 'Basic256Sha256', 'Aes128Sha256RsaOaep', 'Aes256Sha256RsaPss'. Not set by default (no security).",
            "required": false
        },
        {
            "name": "security_mode",
            "example": "SignAndEncrypt",
            "description": "OPC UA security mode: 'Sign', 'SignAndEncrypt'. Default: 'SignAndEncrypt' (when security_policy is set).",
            "required": false
        },
        {
            "name": "username",
            "example": "opcua_user",
            "description": "Username for UserToken authentication. Both username and password must be provided together.",
            "required": false
        },
        {
            "name": "password",
            "example": "opcua_pass",
            "description": "Password for UserToken authentication. Both username and password must be provided together.",
            "required": false
        },
        {
            "name": "certificate",
            "example": "certs/client.der",
            "description": "Path to client certificate for security policy (absolute or relative to PLUGIN_DIR). Required when security_policy is set.",
            "required": false
        },
        {
            "name": "private_key",
            "example": "certs/client.pem",
            "description": "Path to client private key for security policy (absolute or relative to PLUGIN_DIR). Required when security_policy is set.",
            "required": false
        },
        {
            "name": "quality_filter",
            "example": "good uncertain",
            "description": "Space-separated list of OPC UA quality categories to accept: 'good', 'uncertain', 'bad'. Values with non-matching quality are skipped and logged. Default: 'good'.",
            "required": false
        },
        {
            "name": "disable_config_cache",
            "example": "true",
            "description": "Disable configuration caching. When set to 'true', the configuration is reloaded from file/arguments on every scheduled call instead of being cached for 1 hour. Useful during development or when the config file changes frequently. Default: false.",
            "required": false
        }
    ]
}
"""

import asyncio
import os
import re
import time
import tomllib
import uuid
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from asyncua import Client, ua

# Internal constants
_CONNECTION_TIMEOUT = 10  # seconds
_SESSION_TIMEOUT = 3600000  # milliseconds (1 hour)
_UNCERTAIN_STATUS_THRESHOLD = 0x40000000  # OPC UA Uncertain status codes (bits 31-30 = 01)
_BAD_STATUS_THRESHOLD = 0x80000000  # OPC UA Bad status codes (bit 31 set)

# Browse mode defaults
_DEFAULT_BROWSE_DEPTH = 2

# OPC UA VariantType sets for type detection
_INT_VARIANT_TYPES = {
    ua.VariantType.SByte,
    ua.VariantType.Int16,
    ua.VariantType.Int32,
    ua.VariantType.Int64,
}
_UINT_VARIANT_TYPES = {
    ua.VariantType.Byte,
    ua.VariantType.UInt16,
    ua.VariantType.UInt32,
    ua.VariantType.UInt64,
}
_FLOAT_VARIANT_TYPES = {
    ua.VariantType.Float,
    ua.VariantType.Double,
}

# Valid configuration values
_VALID_FIELD_TYPES = {"int", "uint", "float", "string", "bool"}
_VALID_SECURITY_POLICIES = {
    "Basic128Rsa15",
    "Basic256",
    "Basic256Sha256",
    "Aes128Sha256RsaOaep",
    "Aes256Sha256RsaPss",
}
_VALID_SECURITY_MODES = {"Sign", "SignAndEncrypt"}
_VALID_QUALITY_CATEGORIES = {"good", "uncertain", "bad"}
_DEFAULT_QUALITY_FILTER = {"good"}

_CONFIG_CACHE_TTL = 60 * 60  # seconds — TTL for config and browse structure cache


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


def _classify_quality(sc_value: int) -> str:
    """Classify OPC UA StatusCode into quality category."""
    if sc_value >= _BAD_STATUS_THRESHOLD:
        return "bad"
    elif sc_value >= _UNCERTAIN_STATUS_THRESHOLD:
        return "uncertain"
    return "good"


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


def detect_field_type(variant_type) -> str:
    """Detect InfluxDB field type from OPC UA VariantType."""
    if variant_type is None:
        return "string"
    if variant_type == ua.VariantType.Boolean:
        return "bool"
    elif variant_type in _INT_VARIANT_TYPES:
        return "int"
    elif variant_type in _UINT_VARIANT_TYPES:
        return "uint"
    elif variant_type in _FLOAT_VARIANT_TYPES:
        return "float"
    else:
        return "string"


class OPCUAConfig:
    """Configuration loader and validator for OPC UA plugin"""

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

    def _load_toml_config(self, config_file: str) -> dict[str, Any]:
        """Load configuration from TOML file"""
        config_path: str = _resolve_path(config_file, "configuration file")

        if not os.path.exists(config_path):
            raise FileNotFoundError("Configuration file not found or not accessible.")

        try:
            with open(config_path, "rb") as f:
                config: dict[str, Any] = tomllib.load(f)
        except OSError:
            raise OSError("Configuration file not found or not accessible.") from None

        self._validate_toml_config(config)

        # Normalize quality_filter from list to set
        opcua = config.get("opcua", {})
        qf = opcua.get("quality_filter")
        if qf is not None:
            opcua["quality_filter"] = {q.strip().lower() for q in qf}

        return config

    def _validate_toml_config(self, config: dict[str, Any]):
        """Validate TOML configuration"""
        if "opcua" not in config:
            raise ValueError("Missing required 'opcua' section in configuration")

        opcua_config: dict[str, Any] = config["opcua"]

        if "server_url" not in opcua_config:
            raise ValueError(
                "Missing required parameter 'opcua.server_url' in configuration"
            )

        if "table_name" not in opcua_config:
            raise ValueError(
                "Missing required parameter 'opcua.table_name' in configuration"
            )

        has_nodes = "nodes" in opcua_config
        has_browse = "browse" in opcua_config

        if not has_nodes and not has_browse:
            raise ValueError(
                "Either 'opcua.nodes' or 'opcua.browse' section is required"
            )

        if has_nodes and has_browse:
            raise ValueError(
                "Cannot specify both 'opcua.nodes' and 'opcua.browse' sections"
            )

        # Validate nodes section
        if has_nodes:
            self._validate_toml_nodes(opcua_config["nodes"])

        # Validate browse section
        if has_browse:
            self._validate_toml_browse(opcua_config["browse"])

        # Validate default_tags if present
        default_tags = opcua_config.get("default_tags")
        if default_tags is not None:
            if not isinstance(default_tags, dict):
                raise ValueError(
                    "Parameter 'opcua.default_tags' must be a table of key = \"value\" pairs"
                )
            for key, value in default_tags.items():
                if not isinstance(key, str) or not key.strip():
                    raise ValueError(
                        f"Invalid tag key in 'opcua.default_tags': keys must be non-empty strings"
                    )
                if not isinstance(value, str):
                    raise ValueError(
                        f"Invalid tag value for '{key}' in 'opcua.default_tags': "
                        f"values must be strings, got {type(value).__name__}"
                    )

        # Validate namespaces if present
        namespaces = opcua_config.get("namespaces")
        if namespaces is not None:
            if not isinstance(namespaces, dict):
                raise ValueError(
                    "Parameter 'opcua.namespaces' must be a table of alias = \"uri\" pairs"
                )
            for alias, uri in namespaces.items():
                if not isinstance(alias, str) or not alias.strip():
                    raise ValueError(
                        "Invalid alias in 'opcua.namespaces': aliases must be non-empty strings"
                    )
                if not isinstance(uri, str) or not uri.strip():
                    raise ValueError(
                        f"Invalid URI for alias '{alias}' in 'opcua.namespaces': "
                        f"values must be non-empty strings"
                    )

        # Validate tag_nodes if present (same format as nodes)
        if "tag_nodes" in opcua_config:
            self._validate_toml_nodes(opcua_config["tag_nodes"], "opcua.tag_nodes")

        # Validate security if present
        security: dict = opcua_config.get("security", {})
        if security:
            policy = security.get("security_policy")
            if policy and policy not in _VALID_SECURITY_POLICIES:
                raise ValueError(
                    f"Invalid security_policy: {policy}. "
                    f"Supported: {', '.join(sorted(_VALID_SECURITY_POLICIES))}"
                )
            mode = security.get("security_mode", "SignAndEncrypt")
            if mode not in _VALID_SECURITY_MODES:
                raise ValueError(
                    f"Invalid security_mode: {mode}. "
                    f"Supported: {', '.join(sorted(_VALID_SECURITY_MODES))}"
                )
            if policy:
                if not security.get("certificate") or not security.get("private_key"):
                    raise ValueError(
                        "Certificate and private_key required when security_policy is set"
                    )

        # Validate auth if present
        auth: dict = opcua_config.get("auth", {})
        if auth:
            has_username = "username" in auth
            has_password = "password" in auth
            if has_username != has_password:
                raise ValueError(
                    "Both username and password must be provided for authentication"
                )

        # Validate quality_filter if present
        quality_filter = opcua_config.get("quality_filter")
        if quality_filter is not None:
            if not isinstance(quality_filter, list) or not all(
                isinstance(q, str) for q in quality_filter
            ):
                raise ValueError(
                    "Parameter 'opcua.quality_filter' must be a list of strings "
                    "(e.g. ['good', 'uncertain'])"
                )
            categories = {q.strip().lower() for q in quality_filter}
            invalid = categories - _VALID_QUALITY_CATEGORIES
            if invalid:
                raise ValueError(
                    f"Invalid quality categories in 'opcua.quality_filter': "
                    f"{', '.join(sorted(invalid))}. "
                    f"Valid: {', '.join(sorted(_VALID_QUALITY_CATEGORIES))}"
                )

        # Validate disable_config_cache if present (accepts bool or string)
        disable_cache = opcua_config.get("disable_config_cache")
        if disable_cache is not None and not isinstance(disable_cache, (bool, str)):
            raise ValueError(
                "Parameter 'opcua.disable_config_cache' must be a boolean or string (true/false)"
            )

    @staticmethod
    def _validate_toml_nodes(nodes: Any, section_name: str = "opcua.nodes"):
        """Validate nodes section in TOML config"""
        if not isinstance(nodes, dict) or len(nodes) == 0:
            raise ValueError(f"Parameter '{section_name}' must be a non-empty table")

        for field_name, node_spec in nodes.items():
            if isinstance(node_spec, str):
                if not node_spec.strip():
                    raise ValueError(f"Empty node_id for field '{field_name}'")
            elif isinstance(node_spec, list):
                if len(node_spec) != 2:
                    raise ValueError(
                        f"Invalid node specification for '{field_name}'. "
                        f'Expected format: ["node_id", "type"]'
                    )
                node_id, field_type = node_spec
                if not isinstance(node_id, str) or not node_id.strip():
                    raise ValueError(f"Invalid node_id for field '{field_name}'")
                if field_type not in _VALID_FIELD_TYPES:
                    raise ValueError(
                        f"Invalid field type '{field_type}' for '{field_name}'. "
                        f"Supported types: {', '.join(sorted(_VALID_FIELD_TYPES))}"
                    )
            else:
                raise ValueError(
                    f"Invalid node specification for '{field_name}'. "
                    f"Expected string or [node_id, type] list."
                )

    @staticmethod
    def _validate_toml_browse(browse: Any):
        """Validate browse section in TOML config"""
        if not isinstance(browse, dict):
            raise ValueError("Parameter 'opcua.browse' must be a table")

        if "browse_root" not in browse:
            raise ValueError(
                "Missing required parameter 'opcua.browse.browse_root'"
            )

        browse_root = browse["browse_root"]
        if not isinstance(browse_root, str) or not browse_root.strip():
            raise ValueError("Parameter 'opcua.browse.browse_root' must be a non-empty string")

        browse_depth = browse.get("browse_depth", _DEFAULT_BROWSE_DEPTH)
        if not isinstance(browse_depth, int) or browse_depth < 1:
            raise ValueError(
                f"Parameter 'opcua.browse.browse_depth' must be a positive integer, got: {browse_depth}"
            )

        # path_tags: required list of tag names for Object hierarchy levels
        if "path_tags" not in browse:
            raise ValueError(
                "Missing required parameter 'opcua.browse.path_tags'"
            )
        path_tags = browse["path_tags"]
        if not isinstance(path_tags, list) or not all(
            isinstance(t, str) and t.strip() for t in path_tags
        ):
            raise ValueError(
                "Parameter 'opcua.browse.path_tags' must be a list of non-empty strings"
            )
        if len(path_tags) >= browse_depth:
            raise ValueError(
                f"len(path_tags)={len(path_tags)} must be strictly less than "
                f"browse_depth={browse_depth}. Variables need at least one "
                f"deeper level beyond the tagged Object levels."
            )

        filter_pattern = browse.get("filter")
        if filter_pattern is not None:
            if not isinstance(filter_pattern, str) or not filter_pattern.strip():
                raise ValueError(
                    "Parameter 'opcua.browse.filter' must be a non-empty string"
                )
            try:
                re.compile(filter_pattern)
            except re.error as e:
                raise ValueError(
                    f"Invalid regex pattern in 'opcua.browse.filter': {e}"
                )

        exclude_branches = browse.get("exclude_branches")
        if exclude_branches is not None:
            if not isinstance(exclude_branches, str) or not exclude_branches.strip():
                raise ValueError(
                    "Parameter 'opcua.browse.exclude_branches' must be a non-empty string"
                )
            try:
                re.compile(exclude_branches)
            except re.error as e:
                raise ValueError(
                    f"Invalid regex pattern in 'opcua.browse.exclude_branches': {e}"
                )

        browse_tags = browse.get("browse_tags")
        if browse_tags is not None:
            if not isinstance(browse_tags, list) or not all(
                isinstance(t, str) and t.strip() for t in browse_tags
            ):
                raise ValueError(
                    "Parameter 'opcua.browse.browse_tags' must be a list of non-empty strings"
                )

        # name_tags / name_separator: parse Variable browse names into tags
        name_separator = browse.get("name_separator")
        name_tags = browse.get("name_tags")

        if name_separator is not None:
            if not isinstance(name_separator, str) or not name_separator:
                raise ValueError(
                    "Parameter 'opcua.browse.name_separator' must be a non-empty string"
                )

        if name_tags is not None:
            if not isinstance(name_tags, list) or not name_tags or not all(
                isinstance(t, str) and t.strip() for t in name_tags
            ):
                raise ValueError(
                    "Parameter 'opcua.browse.name_tags' must be a non-empty list of non-empty strings"
                )
            if name_separator is None:
                raise ValueError(
                    "Parameter 'opcua.browse.name_separator' is required when 'name_tags' is set"
                )

        # Cross-validate: no overlapping tag names between path_tags and name_tags
        if path_tags and name_tags:
            overlap = set(path_tags) & set(name_tags)
            if overlap:
                raise ValueError(
                    f"Tag names must be unique across 'path_tags' and 'name_tags'. "
                    f"Duplicates: {', '.join(sorted(overlap))}"
                )

    def _build_config_from_args(self) -> dict[str, Any]:
        """Build configuration from command-line arguments"""
        if not self.args:
            raise ValueError("No arguments provided")

        if "server_url" not in self.args:
            raise ValueError("Missing required argument: server_url")
        if "table_name" not in self.args:
            raise ValueError("Missing required argument: table_name")

        has_nodes = "nodes" in self.args
        has_browse = "browse_root" in self.args

        if not has_nodes and not has_browse:
            raise ValueError(
                "Either 'nodes' or 'browse_root' argument is required"
            )
        if has_nodes and has_browse:
            raise ValueError(
                "Cannot specify both 'nodes' and 'browse_root' arguments"
            )

        # Parse namespaces mapping (alias=uri) — must be parsed before nodes
        namespaces_config: dict = {}
        namespaces_arg: str | None = self.args.get("namespaces")
        if namespaces_arg:
            for ns_spec in namespaces_arg.split():
                ns_spec = ns_spec.strip()
                if not ns_spec:
                    continue
                if "=" not in ns_spec:
                    raise ValueError(
                        f"Invalid namespace mapping: '{ns_spec}'. "
                        f"Expected format: 'alias=namespace_uri'"
                    )
                alias, uri = ns_spec.split("=", 1)
                if not alias.strip() or not uri.strip():
                    raise ValueError(
                        f"Invalid namespace mapping: '{ns_spec}'. "
                        f"Both alias and URI must be non-empty"
                    )
                namespaces_config[alias.strip()] = uri.strip()

        # Parse nodes (explicit mode)
        nodes_config: dict = {}
        if has_nodes:
            nodes_config = self._parse_nodes_from_args(self.args.get("nodes"), namespaces_config)

        # Parse browse config (browse mode)
        browse_config: dict = {}
        if has_browse:
            browse_root = self.args.get("browse_root", "").strip()
            if not browse_root:
                raise ValueError("Parameter 'browse_root' must be non-empty")

            browse_config["browse_root"] = browse_root

            browse_depth = self.args.get("browse_depth")
            if browse_depth is not None:
                try:
                    depth_val = int(browse_depth)
                    if depth_val < 1:
                        raise ValueError
                    browse_config["browse_depth"] = depth_val
                except ValueError:
                    raise ValueError(
                        f"Parameter 'browse_depth' must be a positive integer, got: {browse_depth}"
                    )

            # path_tags: required list of tag names for hierarchy levels
            path_tags_arg = self.args.get("path_tags")
            if path_tags_arg is not None:
                path_tags_list = [t.strip() for t in path_tags_arg.split() if t.strip()]
            else:
                path_tags_list = []
            browse_config["path_tags"] = path_tags_list

            # Validate path_tags vs browse_depth
            depth_val = browse_config.get("browse_depth", _DEFAULT_BROWSE_DEPTH)
            if len(path_tags_list) >= depth_val:
                raise ValueError(
                    f"len(path_tags)={len(path_tags_list)} must be strictly less than "
                    f"browse_depth={depth_val}. Variables need at least one "
                    f"deeper level beyond the tagged Object levels."
                )

            filter_arg = self.args.get("filter")
            if filter_arg is not None:
                if not filter_arg.strip():
                    raise ValueError("Parameter 'filter' must be non-empty")
                try:
                    re.compile(filter_arg)
                except re.error as e:
                    raise ValueError(f"Invalid regex in 'filter': {e}")
                browse_config["filter"] = filter_arg.strip()

            exclude_branches_arg = self.args.get("exclude_branches")
            if exclude_branches_arg is not None:
                if not exclude_branches_arg.strip():
                    raise ValueError("Parameter 'exclude_branches' must be non-empty")
                try:
                    re.compile(exclude_branches_arg)
                except re.error as e:
                    raise ValueError(f"Invalid regex in 'exclude_branches': {e}")
                browse_config["exclude_branches"] = exclude_branches_arg.strip()

            browse_tags_arg = self.args.get("browse_tags")
            if browse_tags_arg is not None:
                browse_tag_names = [t.strip() for t in browse_tags_arg.split() if t.strip()]
                if not browse_tag_names:
                    raise ValueError("Parameter 'browse_tags' must be non-empty")
                browse_config["browse_tags"] = browse_tag_names

            # name_separator + name_tags: parse Variable browse names
            name_separator = self.args.get("name_separator")
            if name_separator is not None:
                if not name_separator.strip():
                    raise ValueError("Parameter 'name_separator' must be non-empty")
                browse_config["name_separator"] = name_separator.strip()

            name_tags_arg = self.args.get("name_tags")
            if name_tags_arg is not None:
                name_tags_list = [t.strip() for t in name_tags_arg.split() if t.strip()]
                if not name_tags_list:
                    raise ValueError("Parameter 'name_tags' must be non-empty")
                browse_config["name_tags"] = name_tags_list

            if "name_tags" in browse_config and "name_separator" not in browse_config:
                raise ValueError(
                    "Parameter 'name_separator' is required when 'name_tags' is set"
                )

            # Cross-validate: no overlapping tag names
            if path_tags_list and "name_tags" in browse_config:
                overlap = set(path_tags_list) & set(browse_config["name_tags"])
                if overlap:
                    raise ValueError(
                        f"Tag names must be unique across 'path_tags' and 'name_tags'. "
                        f"Duplicates: {', '.join(sorted(overlap))}"
                    )

        # Parse default_tags
        tags_config: dict = {}
        tags_arg: str | None = self.args.get("default_tags")
        if tags_arg:
            for tag_spec in tags_arg.split():
                tag_spec = tag_spec.strip()
                if not tag_spec:
                    continue
                if "=" not in tag_spec:
                    raise ValueError(
                        f"Invalid tag specification: '{tag_spec}'. "
                        f"Expected format: 'key=value'"
                    )
                key, value = tag_spec.split("=", 1)
                if key.strip() and value.strip():
                    tags_config[key.strip()] = value.strip()

        # Parse tag nodes (dynamic tags from OPC UA nodes)
        tag_nodes_config: dict = {}
        tag_nodes_arg: str | None = self.args.get("tag_nodes")
        if tag_nodes_arg:
            tag_nodes_config = self._parse_nodes_from_args(tag_nodes_arg, namespaces_config)

        # Build security config
        security_config: dict = {}
        security_policy = self.args.get("security_policy")
        security_mode = self.args.get("security_mode")
        certificate = self.args.get("certificate")
        private_key = self.args.get("private_key")

        if security_policy:
            if security_policy not in _VALID_SECURITY_POLICIES:
                raise ValueError(
                    f"Invalid security_policy: {security_policy}. "
                    f"Supported: {', '.join(sorted(_VALID_SECURITY_POLICIES))}"
                )
            security_config["security_policy"] = security_policy

            if security_mode:
                if security_mode not in _VALID_SECURITY_MODES:
                    raise ValueError(
                        f"Invalid security_mode: {security_mode}. "
                        f"Supported: {', '.join(sorted(_VALID_SECURITY_MODES))}"
                    )
                security_config["security_mode"] = security_mode
            else:
                security_config["security_mode"] = "SignAndEncrypt"

            if not certificate or not private_key:
                raise ValueError(
                    "Both certificate and private_key required "
                    "when security_policy is set"
                )
            security_config["certificate"] = certificate
            security_config["private_key"] = private_key

        # Build auth config
        auth_config: dict = {}
        username = self.args.get("username")
        password = self.args.get("password")
        if username and password:
            auth_config["username"] = username
            auth_config["password"] = password
        elif username or password:
            raise ValueError(
                "Both username and password must be provided for authentication"
            )

        # Parse quality_filter
        quality_filter_arg = self.args.get("quality_filter", "good")
        quality_filter = {q.strip().lower() for q in quality_filter_arg.split()}
        invalid_categories = quality_filter - _VALID_QUALITY_CATEGORIES
        if invalid_categories:
            raise ValueError(
                f"Invalid quality categories: {', '.join(sorted(invalid_categories))}. "
                f"Valid: {', '.join(sorted(_VALID_QUALITY_CATEGORIES))}"
            )

        disable_config_cache: bool = (
            str(self.args.get("disable_config_cache", "false")).lower() == "true"
        )

        result = {
            "opcua": {
                "server_url": self.args.get("server_url"),
                "table_name": self.args.get("table_name"),
                "default_tags": tags_config,
                "quality_filter": quality_filter,
                "namespaces": namespaces_config,
                "tag_nodes": tag_nodes_config,
                "security": security_config,
                "auth": auth_config,
                "disable_config_cache": disable_config_cache,
            }
        }

        if nodes_config:
            result["opcua"]["nodes"] = nodes_config
        if browse_config:
            result["opcua"]["browse"] = browse_config

        return result

    def _parse_nodes_from_args(
        self, nodes_arg: str, namespaces: dict[str, str] | None = None
    ) -> dict[str, Any]:
        """Parse nodes from args.

        Format: 'field_name:namespace:identifier[:type]' (space-separated)
        Namespace can be a numeric index or an alias defined in 'namespaces' mapping.
        Node ID is built as 'ns=<namespace>;<identifier>' for numeric namespaces,
        or 'nsu=<uri>;<identifier>' for namespace aliases.
        Type is optional - auto-detected from OPC UA DataType if not specified.

        Examples:
            temperature:2:s=SpindleTemp           -> ns=2;s=SpindleTemp, auto type
            pressure:siemens:s=CoolantPressure:float -> nsu=urn:vendor:s7;s=CoolantPressure, float
            rpm:2:i=1234:uint                      -> ns=2;i=1234, uint
        """
        nodes_config: dict = {}

        for node_spec in nodes_arg.split():
            node_spec = node_spec.strip()
            if not node_spec:
                continue

            parts = node_spec.split(":")
            if len(parts) < 3:
                raise ValueError(
                    f"Invalid node specification: '{node_spec}'. "
                    f"Expected format: 'field_name:namespace:identifier[:type]'"
                )

            field_name = parts[0].strip()
            namespace = parts[1].strip()

            # Check if last part is a type override
            if len(parts) > 3 and parts[-1].strip() in _VALID_FIELD_TYPES:
                field_type = parts[-1].strip()
                identifier = ":".join(parts[2:-1])
            else:
                field_type = None
                identifier = ":".join(parts[2:])

            if not field_name:
                raise ValueError(
                    f"Empty field_name in node specification: '{node_spec}'"
                )
            if not identifier:
                raise ValueError(
                    f"Empty identifier in node specification: '{node_spec}'"
                )

            # Resolve namespace: numeric index or alias from namespaces mapping
            if namespace.isdigit():
                node_id = f"ns={namespace};{identifier}"
            elif namespaces and namespace in namespaces:
                node_id = f"nsu={namespaces[namespace]};{identifier}"
            else:
                if namespaces:
                    raise ValueError(
                        f"Unknown namespace alias '{namespace}' in node specification: "
                        f"'{node_spec}'. Available aliases: {', '.join(sorted(namespaces))}"
                    )
                raise ValueError(
                    f"Invalid namespace '{namespace}' in node specification: "
                    f"'{node_spec}'. Must be a numeric index or an alias "
                    f"defined in 'namespaces' parameter."
                )

            if field_type:
                nodes_config[field_name] = [node_id, field_type]
            else:
                nodes_config[field_name] = node_id

        if not nodes_config:
            raise ValueError("No valid node specifications found")

        return nodes_config

    def get_opcua_config(self) -> dict[str, Any]:
        """Get OPC UA configuration"""
        return self.config.get("opcua")


class OPCUAConnectionManager:
    """Manages OPC UA client connection and node reading"""

    def __init__(self, config: dict[str, Any], influxdb3_local, task_id: str):
        self.config: dict[str, Any] = config
        self.influxdb3_local = influxdb3_local
        self.task_id: str = task_id
        self.client: Client | None = None
        self.connected: bool = False

    async def connect(self) -> bool:
        """Establish connection to OPC UA server"""
        try:
            server_url: str = self.config.get("server_url")
            self.influxdb3_local.info(
                f"[{self.task_id}] Connecting to OPC UA server: {server_url}"
            )

            self.client = Client(server_url, timeout=_CONNECTION_TIMEOUT)
            self.client.session_timeout = _SESSION_TIMEOUT

            # Configure security
            security: dict = self.config.get("security", {})
            policy = security.get("security_policy")

            if policy:
                mode = security.get("security_mode", "SignAndEncrypt")
                cert_path = _resolve_path(
                    security["certificate"], "client certificate"
                )
                key_path = _resolve_path(
                    security["private_key"], "client private key"
                )

                if not os.path.exists(cert_path):
                    raise FileNotFoundError(
                        "Security configuration failed. Check certificate and key files."
                    )
                if not os.path.exists(key_path):
                    raise FileNotFoundError(
                        "Security configuration failed. Check certificate and key files."
                    )

                security_string = f"{policy},{mode},{cert_path},{key_path}"
                try:
                    await self.client.set_security_string(security_string)
                except Exception:
                    raise OSError("Security configuration failed. Check certificate and key files.") from None

            # Configure authentication
            auth: dict = self.config.get("auth", {})
            username = auth.get("username")
            password = auth.get("password")
            if username and password:
                self.client.set_user(username)
                self.client.set_password(password)

            await self.client.connect()
            self.connected = True
            self.influxdb3_local.info(
                f"[{self.task_id}] Connected to OPC UA server successfully"
            )

            # Resolve namespace URIs to numeric indexes
            await self._resolve_namespace_uris()

            return True

        except Exception as e:
            if self.config.get("security", {}).get("security_policy"):
                self.influxdb3_local.error(
                    f"[{self.task_id}] Error connecting to OPC UA server: "
                    f"connection failed (security configured). "
                    f"Check server address, certificates, and key files."
                )
            else:
                self.influxdb3_local.error(
                    f"[{self.task_id}] Error connecting to OPC UA server: {str(e)}"
                )
            return False

    async def _resolve_namespace_uris(self):
        """Resolve nsu= prefixed node IDs to ns= using server's namespace array.

        Scans all node IDs in 'nodes' and 'tag_nodes' config sections.
        Replaces 'nsu=<uri>;...' with 'ns=<index>;...' using the server's
        namespace array lookup.
        """
        if not self.client:
            return

        # Collect unique URIs to resolve
        uris_to_resolve: set[str] = set()
        for config_key in ("nodes", "tag_nodes"):
            nodes_config = self.config.get(config_key, {})
            for node_spec in nodes_config.values():
                node_id = node_spec[0] if isinstance(node_spec, list) else node_spec
                if node_id.startswith("nsu="):
                    uri = node_id.split(";", 1)[0][4:]  # strip "nsu="
                    uris_to_resolve.add(uri)

        if not uris_to_resolve:
            return

        # Resolve URIs to indexes
        uri_to_index: dict[str, int] = {}
        for uri in uris_to_resolve:
            try:
                idx = await self.client.get_namespace_index(uri)
                uri_to_index[uri] = idx
                self.influxdb3_local.info(
                    f"[{self.task_id}] Resolved namespace URI '{uri}' -> ns={idx}"
                )
            except ValueError:
                raise ValueError(
                    f"Namespace URI '{uri}' not found on server. "
                    f"Available namespaces: {await self.client.get_namespace_array()}"
                )

        # Replace nsu= with ns= in config
        for config_key in ("nodes", "tag_nodes"):
            nodes_config = self.config.get(config_key, {})
            for field_name, node_spec in nodes_config.items():
                if isinstance(node_spec, list):
                    node_id = node_spec[0]
                else:
                    node_id = node_spec

                if node_id.startswith("nsu="):
                    uri, rest = node_id[4:].split(";", 1)
                    idx = uri_to_index[uri]
                    resolved = f"ns={idx};{rest}"
                    if isinstance(node_spec, list):
                        nodes_config[field_name] = [resolved, node_spec[1]]
                    else:
                        nodes_config[field_name] = resolved

    async def read_nodes(self, config_key: str = "nodes") -> list[dict[str, Any]]:
        """Batch-read all configured nodes in a single OPC UA Read request.

        Args:
            config_key: Configuration key to read nodes from ('nodes' or 'tag_nodes').
        """
        nodes_config: dict = self.config.get(config_key, {})
        if not nodes_config:
            return []

        node_specs: list[tuple[str, str, str | None]] = []
        for field_name, node_spec in nodes_config.items():
            if isinstance(node_spec, list):
                node_specs.append((field_name, node_spec[0], node_spec[1]))
            else:
                node_specs.append((field_name, node_spec, None))

        return await self._batch_read(node_specs)

    async def browse_nodes(self) -> list[tuple[dict[str, str], dict[str, str]]]:
        """Browse from root node and discover Variable nodes grouped by path tags.

        Uses path_tags config to map Object hierarchy levels to InfluxDB tags.
        Objects at depths within path_tags become tag values; Objects beyond
        path_tags length become field name prefixes.

        Returns: list of (path_tags_dict, {field_name: node_id_str})
        """
        if not self.client:
            return []

        browse_config: dict = self.config.get("browse", {})
        root_node_id: str = browse_config["browse_root"]
        max_depth: int = browse_config.get("browse_depth", _DEFAULT_BROWSE_DEPTH)
        filter_pattern: str | None = browse_config.get("filter")
        path_tags: list[str] = browse_config.get("path_tags", [])

        compiled_filter = re.compile(filter_pattern) if filter_pattern else None

        exclude_branches_pattern: str | None = browse_config.get("exclude_branches")
        compiled_exclude = re.compile(exclude_branches_pattern) if exclude_branches_pattern else None

        root = self.client.get_node(root_node_id)

        self.influxdb3_local.info(
            f"[{self.task_id}] Browsing OPC UA address space from: {root_node_id}, "
            f"browse_depth={max_depth}, path_tags={path_tags}"
        )

        result: list[tuple[dict[str, str], dict[str, str]]] = []

        try:
            await self._browse_recursive(
                root, max_depth, compiled_filter, compiled_exclude, path_tags,
                current_depth=0, current_path_tags={},
                result=result,
            )
        except Exception as e:
            self.influxdb3_local.error(
                f"[{self.task_id}] Error browsing from {root_node_id}: {str(e)}"
            )
            return []

        total_groups = len(result)
        total_vars = sum(len(fields) for _, fields in result)
        self.influxdb3_local.info(
            f"[{self.task_id}] Browse complete: {total_groups} groups, "
            f"{total_vars} variables discovered"
        )

        return result

    async def _browse_recursive(
        self,
        node,
        remaining_depth: int,
        filter_re: re.Pattern | None,
        exclude_re: re.Pattern | None,
        path_tags: list[str],
        current_depth: int,
        current_path_tags: dict[str, str],
        result: list[tuple[dict[str, str], dict[str, str]]],
    ):
        """Recursively browse OPC UA address space collecting variables.

        Objects at depths within path_tags range become tag values.
        Objects beyond path_tags range become field name prefixes
        (collected into a single variables dict per tag group).
        Variables are collected as fields.
        Objects matching exclude_re are skipped with their entire subtree.
        """
        if remaining_depth <= 0:
            return

        try:
            children = await node.get_children()
        except Exception:
            return

        variables: dict[str, str] = {}

        for child in children:
            try:
                node_class = await child.read_node_class()
                browse_name = (await child.read_browse_name()).Name

                if node_class == ua.NodeClass.Variable:
                    if filter_re and not filter_re.search(browse_name):
                        continue
                    variables[browse_name] = child.nodeid.to_string()

                elif node_class == ua.NodeClass.Object and remaining_depth > 1:
                    if exclude_re and exclude_re.search(browse_name):
                        continue
                    if current_depth < len(path_tags):
                        # This Object maps to a path tag
                        new_tags = dict(current_path_tags)
                        new_tags[path_tags[current_depth]] = browse_name
                        await self._browse_recursive(
                            child, remaining_depth - 1, filter_re, exclude_re,
                            path_tags, current_depth + 1, new_tags,
                            result,
                        )
                    else:
                        # Beyond path_tags — collect into same variables dict
                        await self._collect_prefixed_variables(
                            child, remaining_depth - 1, filter_re, exclude_re,
                            f"{browse_name}_", variables,
                        )
            except Exception:
                continue

        if variables:
            result.append((dict(current_path_tags), variables))

    async def _collect_prefixed_variables(
        self,
        node,
        remaining_depth: int,
        filter_re: re.Pattern | None,
        exclude_re: re.Pattern | None,
        prefix: str,
        variables: dict[str, str],
    ):
        """Collect Variable nodes under an Object into the caller's variables dict.

        Used for Objects beyond path_tags depth — the Object name becomes a
        field name prefix. All variables at any sub-level are accumulated
        into the same dict, producing a single data point per tag group.
        Objects matching exclude_re are skipped with their entire subtree.
        """
        if remaining_depth <= 0:
            return

        try:
            children = await node.get_children()
        except Exception:
            return

        for child in children:
            try:
                node_class = await child.read_node_class()
                browse_name = (await child.read_browse_name()).Name

                if node_class == ua.NodeClass.Variable:
                    if filter_re and not filter_re.search(browse_name):
                        continue
                    variables[f"{prefix}{browse_name}"] = child.nodeid.to_string()

                elif node_class == ua.NodeClass.Object and remaining_depth > 1:
                    if exclude_re and exclude_re.search(browse_name):
                        continue
                    await self._collect_prefixed_variables(
                        child, remaining_depth - 1, filter_re, exclude_re,
                        f"{prefix}{browse_name}_", variables,
                    )
            except Exception:
                continue

    async def read_browsed_nodes(
        self,
        browse_structure: list[tuple[dict[str, str], dict[str, str]]],
    ) -> list[tuple[dict[str, str], list[dict[str, Any]]]]:
        """Batch-read all browsed nodes across all groups in a single OPC UA request.

        Args:
            browse_structure: list of (path_tags_dict, {field_name: node_id_str})

        Returns: list of (path_tags_dict, [node_results])
        """
        if not browse_structure:
            return []

        # Flatten all groups into one ordered list for a single batch request
        ordered: list[tuple[int, str, str]] = [
            (group_idx, field_name, node_id_str)
            for group_idx, (_, fields) in enumerate(browse_structure)
            for field_name, node_id_str in fields.items()
        ]

        batch_specs = [(fn, nid, None) for _, fn, nid in ordered]
        flat_results = await self._batch_read(batch_specs)

        # Reassemble per group preserving order
        group_results: list[list[dict[str, Any]]] = [[] for _ in browse_structure]
        for i, (group_idx, _, _) in enumerate(ordered):
            group_results[group_idx].append(flat_results[i])

        return [
            (browse_structure[idx][0], results)
            for idx, results in enumerate(group_results)
        ]

    async def _batch_read(
        self,
        node_specs: list[tuple[str, str, str | None]],
    ) -> list[dict[str, Any]]:
        """Execute a single OPC UA batch Read request for all given node specs.

        All nodes are read in one network round-trip using ReadRequest.

        Args:
            node_specs: list of (field_name, node_id_str, explicit_type_or_None)

        Returns:
            list of result dicts (same length and order as node_specs)
        """
        if not self.client or not node_specs:
            return []

        params = ua.ReadParameters()
        params.TimestampsToReturn = ua.TimestampsToReturn.Both
        params.NodesToRead = []
        for _, node_id_str, _ in node_specs:
            rvid = ua.ReadValueId()
            rvid.NodeId = self.client.get_node(node_id_str).nodeid
            rvid.AttributeId = ua.AttributeIds.Value
            params.NodesToRead.append(rvid)

        data_values = await self.client.uaclient.read(params)

        results: list[dict[str, Any]] = []
        for i, (field_name, node_id_str, explicit_type) in enumerate(node_specs):
            result: dict[str, Any] = {
                "field_name": field_name,
                "node_id": node_id_str,
                "value": None,
                "variant_type": None,
                "field_type": explicit_type,
                "timestamp_ns": None,
                "quality": "good",
                "status": "error",
                "error": None,
            }

            dv = data_values[i]

            # Classify quality from StatusCode
            status_code = dv.StatusCode
            if status_code is not None:
                sc_value = (
                    status_code
                    if isinstance(status_code, int)
                    else getattr(status_code, "value", 0)
                )
                quality = _classify_quality(sc_value)
                result["quality"] = quality
                if quality == "bad":
                    status_name = getattr(status_code, "name", hex(sc_value))
                    result["status"] = "bad"
                    result["error"] = f"Bad status: {status_name}"
                    results.append(result)
                    continue

            # Extract value
            variant = dv.Value
            if variant is None or variant.Value is None:
                result["quality"] = "bad"
                result["status"] = "bad"
                result["error"] = "Null value"
                results.append(result)
                continue

            # Extract OPC UA timestamps (prefer SourceTimestamp)
            source_ts = getattr(dv, "SourceTimestamp", None)
            server_ts = getattr(dv, "ServerTimestamp", None)
            if source_ts is not None:
                result["timestamp_ns"] = int(source_ts.timestamp() * 1_000_000_000)
            elif server_ts is not None:
                result["timestamp_ns"] = int(server_ts.timestamp() * 1_000_000_000)

            result["value"] = variant.Value
            result["variant_type"] = variant.VariantType
            result["status"] = result["quality"]
            results.append(result)

        return results

    async def disconnect_silent(self):
        """Disconnect without raising exceptions, used internally before reconnect."""
        try:
            if self.client and self.connected:
                await self.client.disconnect()
        except Exception:
            pass
        finally:
            self.connected = False

    async def reconnect(self) -> bool:
        """Force disconnect and reconnect to OPC UA server."""
        self.influxdb3_local.info(
            f"[{self.task_id}] Reconnecting to OPC UA server..."
        )
        await self.disconnect_silent()
        return await self.connect()

    async def disconnect(self):
        """Disconnect from OPC UA server."""
        try:
            if self.client and self.connected:
                await self.client.disconnect()
                self.connected = False
                self.influxdb3_local.info(
                    f"[{self.task_id}] Disconnected from OPC UA server"
                )
        except Exception as e:
            self.influxdb3_local.error(
                f"[{self.task_id}] Error disconnecting from OPC UA server: {str(e)}"
            )


class OPCUAStats:
    """Track and persist OPC UA plugin statistics"""

    def __init__(self):
        self.reset()

    def reset(self):
        """Reset all statistics"""
        self.total_nodes_read: int = 0
        self.total_nodes_failed: int = 0
        self.total_points_written: int = 0
        self.period_nodes_read: int = 0
        self.period_nodes_failed: int = 0
        self.period_points_written: int = 0
        self.last_read_time: int | None = None

    def record_nodes_read(self, count: int = 1):
        """Record successfully read nodes"""
        self.total_nodes_read += count
        self.period_nodes_read += count
        self.last_read_time = time.time_ns()

    def record_nodes_failed(self, count: int = 1):
        """Record failed node reads"""
        self.total_nodes_failed += count
        self.period_nodes_failed += count

    def record_point_written(self, count: int = 1):
        """Record successfully written points"""
        self.total_points_written += count
        self.period_points_written += count

    def reset_period_stats(self):
        """Reset period statistics after writing to database"""
        self.period_nodes_read = 0
        self.period_nodes_failed = 0
        self.period_points_written = 0

    def get_stats(self) -> dict[str, Any]:
        """Get statistics with calculated success rates"""
        period_total = self.period_nodes_read + self.period_nodes_failed
        period_success_rate = (
            (self.period_nodes_read / period_total * 100)
            if period_total > 0
            else 0.0
        )

        total_total = self.total_nodes_read + self.total_nodes_failed
        total_success_rate = (
            (self.total_nodes_read / total_total * 100)
            if total_total > 0
            else 0.0
        )

        return {
            "nodes_read": self.period_nodes_read,
            "nodes_failed": self.period_nodes_failed,
            "points_written": self.period_points_written,
            "success_rate": round(period_success_rate, 2),
            "total_nodes_read": self.total_nodes_read,
            "total_nodes_failed": self.total_nodes_failed,
            "total_points_written": self.total_points_written,
            "total_success_rate": round(total_success_rate, 2),
        }


def write_stats(
    influxdb3_local,
    stats: OPCUAStats,
    server_url: str,
    table_name: str,
    task_id: str,
):
    """Write statistics to opcua_stats table."""
    try:
        data: dict = stats.get_stats()

        line = LineBuilder("opcua_stats")
        line.tag("server_url", server_url)
        line.tag("table_name", table_name)

        # Period stats (current interval since last write)
        line.int64_field("nodes_read", data["nodes_read"])
        line.int64_field("nodes_failed", data["nodes_failed"])
        line.int64_field("points_written", data["points_written"])
        if data["nodes_read"] > 0 or data["nodes_failed"] > 0:
            line.float64_field("success_rate", data["success_rate"])

        # Total stats (accumulated over entire plugin lifetime)
        line.int64_field("total_nodes_read", data["total_nodes_read"])
        line.int64_field("total_nodes_failed", data["total_nodes_failed"])
        line.int64_field("total_points_written", data["total_points_written"])
        if data["total_nodes_read"] > 0 or data["total_nodes_failed"] > 0:
            line.float64_field("total_success_rate", data["total_success_rate"])

        line.time_ns(time.time_ns())
        influxdb3_local.write_sync(line, no_sync=True)

        # Reset period stats after writing
        stats.reset_period_stats()

        influxdb3_local.info(
            f"[{task_id}] Wrote statistics to opcua_stats table"
        )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to write statistics: {str(e)}")


def write_exception(
    influxdb3_local,
    node_id: str,
    field_name: str,
    error_type: str,
    error_message: str,
    task_id: str,
):
    """Write exception to opcua_exceptions table."""
    try:
        line = LineBuilder("opcua_exceptions")
        line.tag("node_id", node_id)
        line.tag("field_name", field_name)
        line.tag("error_type", error_type)
        line.string_field("error_message", error_message)
        line.time_ns(time.time_ns())

        influxdb3_local.write_sync(line, no_sync=True)
        influxdb3_local.info(
            f"[{task_id}] Wrote exception to opcua_exceptions table: "
            f"{error_type} for {field_name}"
        )

    except Exception as e:
        influxdb3_local.error(
            f"[{task_id}] Failed to write exception to table: {str(e)}"
        )


def _build_point(
    influxdb3_local,
    table_name: str,
    tags: dict[str, str],
    node_results: list[dict[str, Any]],
    stats: OPCUAStats,
    task_id: str,
    browse_tag_names: set[str] | None = None,
    quality_filter: set[str] | None = None,
) -> tuple[Any, int, int]:
    """Build a single data point from node results.

    Returns: (line_builder_or_None, field_count, error_count)
    """
    if quality_filter is None:
        quality_filter = _DEFAULT_QUALITY_FILTER

    line = LineBuilder(table_name)

    for tag_key, tag_value in tags.items():
        line.tag(tag_key, str(tag_value))

    field_count: int = 0
    error_count: int = 0

    def _record_error(node_id_: str, field_name_: str, error_type: str, error_msg: str):
        nonlocal error_count
        error_count += 1
        stats.record_nodes_failed()
        influxdb3_local.warn(f"[{task_id}] Node {node_id_} ({field_name_}): {error_msg}")
        write_exception(influxdb3_local, node_id_, field_name_, error_type, error_msg, task_id)

    for result in node_results:
        field_name: str = result["field_name"]
        node_id: str = result["node_id"]
        quality: str = result.get("quality", "good")

        # Filter by quality category
        if quality not in quality_filter:
            _record_error(
                node_id, field_name, "QualityFilterError",
                result.get("error") or f"Quality '{quality}' not in filter",
            )
            continue

        # Skip nodes with no value (e.g. bad status with null value)
        if result["value"] is None:
            _record_error(
                node_id, field_name, "NodeReadError",
                result.get("error", "Unknown error"),
            )
            continue

        value = result["value"]

        # If this variable is marked as a browse tag, store as tag
        if browse_tag_names and field_name in browse_tag_names:
            line.tag(field_name, str(value))
            stats.record_nodes_read()
            continue

        # Determine field type
        explicit_type = result.get("field_type")

        if explicit_type:
            field_type = explicit_type
        else:
            variant_type = result.get("variant_type")
            field_type = detect_field_type(variant_type)

        try:
            add_field_with_type(line, field_name, value, field_type)
            field_count += 1
            stats.record_nodes_read()
        except (ValueError, TypeError) as e:
            _record_error(node_id, field_name, "TypeConversionError", f"Type conversion error: {e}")

    # Return the line builder only if we have at least one field
    if field_count > 0:
        # Use the latest OPC UA timestamp from successful node reads,
        # fall back to system time if no OPC UA timestamps available
        node_timestamps = [
            r["timestamp_ns"] for r in node_results
            if r.get("timestamp_ns") is not None and r.get("quality", "good") in quality_filter
        ]
        line.time_ns(max(node_timestamps) if node_timestamps else time.time_ns())
        return line, field_count, error_count

    return None, field_count, error_count


async def _read_with_reconnect(opcua_client, operation, label, influxdb3_local, task_id):
    """Try an async read; on failure reconnect once and retry.

    Returns the operation result on success, or None if reconnect failed
    (caller should abort the current scheduled call).
    """
    try:
        return await operation()
    except Exception as e:
        influxdb3_local.warn(f"[{task_id}] {label} failed ({e}), reconnecting...")
        influxdb3_local.cache.delete("opcua_browse_structure")
        if not await opcua_client.reconnect():
            influxdb3_local.cache.delete("opcua_connection")
            influxdb3_local.error(f"[{task_id}] Reconnect failed")
            return None
        return await operation()


async def _async_scheduled_call(
    influxdb3_local, task_id: str, args: dict | None = None
):
    """Async implementation of the scheduled call logic."""
    opcua_client: OPCUAConnectionManager | None = None

    if not args:
        influxdb3_local.error(f"[{task_id}] No arguments provided")
        return

    try:
        config_from_cache: bool = True
        cached_config: dict | None = influxdb3_local.cache.get("opcua_config")

        if cached_config is None:
            config_from_cache = False
            config_loader: OPCUAConfig = OPCUAConfig(influxdb3_local, args, task_id)
            cached_config = config_loader.get_opcua_config()

            if not str(cached_config.get("disable_config_cache", False)).lower() == "true":
                influxdb3_local.cache.put(
                    "opcua_config", cached_config, _CONFIG_CACHE_TTL
                )

            # Config changed — invalidate cached browse structure
            influxdb3_local.cache.delete("opcua_browse_structure")

            browse_config_log = cached_config.get("browse")
            if browse_config_log:
                influxdb3_local.info(
                    f"[{task_id}] OPC UA Plugin config loaded (browse mode), "
                    f"server: {cached_config['server_url']}, "
                    f"root: {browse_config_log['browse_root']}"
                )
            else:
                influxdb3_local.info(
                    f"[{task_id}] OPC UA Plugin config loaded (explicit mode), "
                    f"server: {cached_config['server_url']}, "
                    f"nodes: {len(cached_config.get('nodes', {}))}"
                )
        else:
            influxdb3_local.info(f"[{task_id}] Using cached configuration")

        server_url: str = cached_config["server_url"]
        table_name: str = cached_config["table_name"]
        tags_config: dict = cached_config.get("default_tags", {})
        quality_filter: set[str] = cached_config.get("quality_filter", _DEFAULT_QUALITY_FILTER)
        browse_config: dict | None = cached_config.get("browse")

        stats: OPCUAStats | None = influxdb3_local.cache.get("opcua_stats")
        if stats is None:
            stats = OPCUAStats()
            influxdb3_local.cache.put("opcua_stats", stats)

        opcua_client: OPCUAConnectionManager | None = influxdb3_local.cache.get(
            "opcua_connection"
        )

        if opcua_client is not None:
            # If config was freshly loaded and the server URL changed, drop old connection
            if not config_from_cache and opcua_client.config.get("server_url") != server_url:
                influxdb3_local.info(
                    f"[{task_id}] Server URL changed, dropping cached connection"
                )
                await opcua_client.disconnect()
                influxdb3_local.cache.delete("opcua_connection")
                opcua_client = None

        if opcua_client is None:
            opcua_client = OPCUAConnectionManager(cached_config, influxdb3_local, task_id)
            if not await opcua_client.connect():
                influxdb3_local.error(f"[{task_id}] Failed to connect to OPC UA server")
                opcua_client = None
                return
            influxdb3_local.cache.put("opcua_connection", opcua_client)
        else:
            # Reuse existing connection — update task_id and config for this call
            opcua_client.task_id = task_id
            if not config_from_cache:
                opcua_client.config = cached_config

        dynamic_tags: dict[str, str] = {}
        if cached_config.get("tag_nodes"):
            tag_results = await _read_with_reconnect(
                opcua_client,
                lambda: opcua_client.read_nodes("tag_nodes"),
                "Tag-nodes read",
                influxdb3_local, task_id,
            )
            if tag_results is None:
                return

            for r in tag_results:
                if r["status"] == "good" and r["value"] is not None:
                    dynamic_tags[r["field_name"]] = str(r["value"])

        merged_tags: dict = {**tags_config, **dynamic_tags}

        all_lines: list = []
        total_fields: int = 0
        total_errors: int = 0

        if browse_config:
            # === Browse mode ===
            browse_structure: list | None = influxdb3_local.cache.get(
                "opcua_browse_structure"
            )
            if browse_structure is None:
                browse_structure = await opcua_client.browse_nodes()
                if not browse_structure:
                    influxdb3_local.warn(
                        f"[{task_id}] Browse discovered no variables, reconnecting..."
                    )
                    if not await opcua_client.reconnect():
                        influxdb3_local.cache.delete("opcua_connection")
                        influxdb3_local.error(f"[{task_id}] Reconnect failed")
                        return
                    browse_structure = await opcua_client.browse_nodes()
                    if not browse_structure:
                        influxdb3_local.warn(
                            f"[{task_id}] Browse still empty after reconnect"
                        )
                        return
                influxdb3_local.cache.put(
                    "opcua_browse_structure", browse_structure, _CONFIG_CACHE_TTL
                )

            group_results = await _read_with_reconnect(
                opcua_client,
                lambda: opcua_client.read_browsed_nodes(browse_structure),
                "Browse read",
                influxdb3_local, task_id,
            )
            if group_results is None:
                return

            browse_tag_names: set[str] | None = (
                set(browse_config["browse_tags"]) if browse_config.get("browse_tags") else None
            )
            name_separator: str | None = browse_config.get("name_separator")
            name_tags_list: list[str] | None = browse_config.get("name_tags")

            for path_tags_dict, node_results in group_results:
                base_tags: dict = {**merged_tags, **path_tags_dict}

                if name_separator and name_tags_list:
                    # Split field names by separator, extract leading segments
                    # as tags, group results by extracted tag combinations
                    sub_groups: dict[tuple[str, ...], list[dict[str, Any]]] = {}
                    n_tags = len(name_tags_list)
                    for nr in node_results:
                        parts = nr["field_name"].split(name_separator)
                        if len(parts) <= n_tags:
                            influxdb3_local.warn(
                                f"[{task_id}] Variable '{nr['field_name']}' has "
                                f"{len(parts)} segment(s) after split by "
                                f"'{name_separator}', need at least {n_tags + 1} "
                                f"for name_tags; skipping"
                            )
                            total_errors += 1
                            continue
                        tag_vals = tuple(parts[:n_tags])
                        new_field = "_".join(parts[n_tags:])
                        modified = dict(nr)
                        modified["field_name"] = new_field
                        sub_groups.setdefault(tag_vals, []).append(modified)

                    for tag_vals, sub_results in sub_groups.items():
                        point_tags = dict(base_tags)
                        for tag_name, tag_val in zip(name_tags_list, tag_vals):
                            if tag_val:
                                point_tags[tag_name] = tag_val
                        built_line, fc, ec = _build_point(
                            influxdb3_local, table_name, point_tags,
                            sub_results, stats, task_id,
                            browse_tag_names=browse_tag_names,
                            quality_filter=quality_filter,
                        )
                        if built_line is not None:
                            all_lines.append(built_line)
                        total_fields += fc
                        total_errors += ec
                else:
                    # No name_tags — write one point per browse group
                    built_line, fc, ec = _build_point(
                        influxdb3_local, table_name, base_tags,
                        node_results, stats, task_id,
                        browse_tag_names=browse_tag_names,
                        quality_filter=quality_filter,
                    )
                    if built_line is not None:
                        all_lines.append(built_line)
                    total_fields += fc
                    total_errors += ec

        else:
            # === Explicit nodes mode ===
            node_results = await _read_with_reconnect(
                opcua_client,
                lambda: opcua_client.read_nodes(),
                "Nodes read",
                influxdb3_local, task_id,
            )
            if node_results is None:
                return

            if node_results:
                built_line, total_fields, total_errors = _build_point(
                    influxdb3_local, table_name, merged_tags, node_results, stats, task_id,
                    quality_filter=quality_filter,
                )
                if built_line is not None:
                    all_lines.append(built_line)

        # ── 6. Batch write all collected data points ─────────────────────────
        if all_lines:
            try:
                influxdb3_local.write_sync(_BatchLines(all_lines), no_sync=True)
                stats.record_point_written(len(all_lines))
            except Exception as e:
                influxdb3_local.error(
                    f"[{task_id}] Batch write failed: {str(e)}"
                )
                total_errors += total_fields
                total_fields = 0

        # ── 7. Stats every 10 calls ──────────────────────────────────────────
        call_count: int = influxdb3_local.cache.get("opcua_call_count") or 0
        call_count += 1
        if call_count >= 10:
            write_stats(influxdb3_local, stats, server_url, table_name, task_id)
            call_count = 0
        influxdb3_local.cache.put("opcua_call_count", call_count)

        # ── 7. Summary log ───────────────────────────────────────────────────
        if total_fields > 0 or total_errors > 0:
            mode = "browse" if browse_config else "explicit"
            influxdb3_local.info(
                f"[{task_id}] Data write complete ({mode} mode): "
                f"{total_fields} fields written, {total_errors} errors"
            )
        elif not browse_config:
            influxdb3_local.warn(f"[{task_id}] No fields to write - all nodes failed")

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Error in OPC UA plugin: {str(e)}")
        # Close and drop connection so next call starts fresh
        if opcua_client is not None:
            await opcua_client.disconnect_silent()
        influxdb3_local.cache.delete("opcua_config")
        influxdb3_local.cache.delete("opcua_connection")
        influxdb3_local.cache.delete("opcua_browse_structure")


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
    # Reuse a persistent event loop so that the cached asyncua Client
    # (and its underlying transport) remains valid across calls.
    # asyncio.run() creates AND closes a new loop each time, which would
    # invalidate any cached async connection objects.
    task_id: str = str(uuid.uuid4())
    influxdb3_local.info(f"[{task_id}] Starting opcua plugin")

    loop: asyncio.AbstractEventLoop | None = influxdb3_local.cache.get(
        "opcua_event_loop"
    )
    if loop is None or loop.is_closed():
        loop = asyncio.new_event_loop()
        influxdb3_local.info(f"[{task_id}] Creating a new event loop")
        influxdb3_local.cache.put("opcua_event_loop", loop)

    loop.run_until_complete(_async_scheduled_call(influxdb3_local, task_id, args))
