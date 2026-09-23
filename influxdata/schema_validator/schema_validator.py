"""
{
    "plugin_type": ["onwrite"],
    "onwrite_args_config": [
        {
            "name": "schema_file",
            "example": "schema_validator_config.json",
            "description": "Path to the JSON schema configuration file, absolute or relative to the plugin directory.",
            "required": true
        },
        {
            "name": "target_database",
            "example": "clean_db",
            "description": "Target database to write validated data to. Defaults to the trigger's own database.",
            "required": false
        },
        {
            "name": "target_table_prefix",
            "example": "validated_",
            "description": "Prefix added to measurement names in the target. Ignored for tables that define 'target_table'. Defaults to an empty string.",
            "required": false
        },
        {
            "name": "target_table_suffix",
            "example": "_clean",
            "description": "Suffix added to measurement names in the target. Ignored for tables that define 'target_table'. Defaults to an empty string.",
            "required": false
        },
        {
            "name": "log_rejected",
            "example": "true",
            "description": "Log one warning per rejected row. Accepts true/false, 1/0, yes/no, on/off. Defaults to true.",
            "required": false
        },
        {
            "name": "log_accepted",
            "example": "false",
            "description": "Log one message per accepted row. Accepts true/false, 1/0, yes/no, on/off. Defaults to false.",
            "required": false
        },
        {
            "name": "write_rejection_log",
            "example": "true",
            "description": "Write rejected row details to the '_schema_rejections' measurement in the target database. Accepts true/false, 1/0, yes/no, on/off. Defaults to false.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "schema_validator_trigger_config.toml",
            "description": "Path to a TOML config file, absolute or relative to the plugin directory. Its values override the inline arguments.",
            "required": false
        }
    ]
}
"""

import json
import time
import uuid

from influxdata_plugin_utils.cache import cached
from influxdata_plugin_utils.config import Config, load_config, resolve_path
from influxdata_plugin_utils.parsing import parse_bool
from influxdata_plugin_utils.sources import (
    KeySpec,
    parse_env,
    parse_toml,
    parse_trigger_args,
)
from influxdata_plugin_utils.validation import Validator
from influxdata_plugin_utils.write import build_line_typed, infer_type, write_data

REJECTION_MEASUREMENT = "_schema_rejections"
SCHEMA_CACHE_TTL_SECONDS = 300
ROW_DATA_LIMIT = 1024

# Schema type names accepted in a field definition, mapped to write types.
FIELD_TYPES = {
    "float": "float",
    "float64": "float",
    "double": "float",
    "integer": "int",
    "int": "int",
    "int64": "int",
    "uint64": "uint",
    "unsigned": "uint",
    "uint": "uint",
    "string": "string",
    "str": "string",
    "boolean": "bool",
    "bool": "bool",
}
SUPPORTED_TYPES = ", ".join(sorted(FIELD_TYPES))


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def trimmed(value) -> str:
    """A setting as trimmed text; TOML may deliver it as a number."""
    return str(value).strip()


CONFIG_VALIDATORS = [
    Validator("schema_file", required=True, cast=trimmed, endswith=".json"),
    Validator("target_database", default="", cast=str),
    Validator("target_table_prefix", default="", cast=str),
    Validator("target_table_suffix", default="", cast=str),
    Validator("log_rejected", default=True, cast=parse_bool),
    Validator("log_accepted", default=False, cast=parse_bool),
    Validator("write_rejection_log", default=False, cast=parse_bool),
]

SETTING_NAMES = [name for validator in CONFIG_VALIDATORS for name in validator.names]

# a key outside this list is named in the error rather than silently dropped
CONFIG_KEYS = KeySpec(
    allowlist=SETTING_NAMES + ["config_file_path"],
    unknown="reject",
)

ENV_PREFIX = "INFLUXDB3_SCHEMA_VALIDATOR_"


def env_spec(*names: str) -> KeySpec:
    """Read the named settings from ``INFLUXDB3_SCHEMA_VALIDATOR_<SETTING>``.

    The prefix is stripped again, so a variable merges with the same setting
    coming from a trigger argument or the TOML file.
    """
    rename = {f"{ENV_PREFIX}{name.upper()}": name for name in names}
    return KeySpec(allowlist=tuple(rename), rename=rename)


ENV_SETTINGS = env_spec(*SETTING_NAMES)


# ---------------------------------------------------------------------------
# Schema loading
# ---------------------------------------------------------------------------


def resolve_field_type(field_def) -> str | None:
    """Return the write type of a field definition, or None when it is untyped."""
    if not isinstance(field_def, dict):
        return None
    raw_type = field_def.get("type")
    if raw_type is None:
        return None
    return FIELD_TYPES.get(str(raw_type).strip().lower())


def validate_schema(schema: dict) -> dict:
    """Check the schema structure and field types, returning it unchanged."""
    allowed = schema.get("allowed_measurements")
    if allowed is not None and not isinstance(allowed, list):
        raise ValueError(
            f"'allowed_measurements' must be a list, got {type(allowed).__name__}"
        )

    tables = schema.get("tables")
    if not isinstance(tables, dict) or not tables:
        raise ValueError("'tables' must be a non-empty dict of measurement definitions")

    for table_name, table_def in tables.items():
        if not isinstance(table_def, dict):
            raise ValueError(
                f"Table '{table_name}' definition must be a dict, got {type(table_def).__name__}"
            )

        tags = table_def.get("tags")
        if tags is not None and not isinstance(tags, dict):
            raise ValueError(
                f"Table '{table_name}' tags must be a dict, got {type(tags).__name__}"
            )

        fields = table_def.get("fields")
        if not isinstance(fields, dict) or not fields:
            raise ValueError(f"Table '{table_name}' fields must be a non-empty dict")

        for field_name, field_def in fields.items():
            if isinstance(field_def, dict) and field_def.get("type") is not None:
                if resolve_field_type(field_def) is None:
                    raise ValueError(
                        f"Table '{table_name}' field '{field_name}' has unknown type "
                        f"'{field_def['type']}' (supported: {SUPPORTED_TYPES})"
                    )

    return schema


def load_schema(influxdb3_local, schema_file: str, task_id: str) -> dict:
    """Read the JSON schema file and cache the validated result."""

    def read_schema() -> dict:
        schema_path = resolve_path(schema_file)
        with open(schema_path) as schema_fh:
            schema = validate_schema(json.load(schema_fh))
        influxdb3_local.info(f"[{task_id}] Loaded schema from {schema_path}")
        return schema

    return cached(
        influxdb3_local,
        f"schema_validator:{schema_file}",
        read_schema,
        ttl_seconds=SCHEMA_CACHE_TTL_SECONDS,
    )


# ---------------------------------------------------------------------------
# Target resolution
# ---------------------------------------------------------------------------


def target_measurement(table_schema: dict, table_name: str, config: dict) -> str:
    """Measurement name validated rows of a table are written to."""
    if target_table := table_schema.get("target_table"):
        return str(target_table)
    return f"{config['target_table_prefix']}{table_name}{config['target_table_suffix']}"


def validate_targets(schema: dict, config: dict, table_names: set[str]) -> None:
    """Reject a configuration that writes one of the given tables back into itself."""
    if config["target_database"]:
        return

    for table_name in table_names:
        table_schema = schema["tables"].get(table_name)
        if table_schema is None:
            continue
        if target_measurement(table_schema, table_name, config) == table_name:
            raise ValueError(
                f"Table '{table_name}' would be written back into itself; set "
                "target_database, target_table_prefix, target_table_suffix, or the "
                "table's 'target_table'"
            )


# ---------------------------------------------------------------------------
# Row validation
# ---------------------------------------------------------------------------


def check_field_type(value, field_type: str) -> bool:
    """Check a value against a write type from FIELD_TYPES."""
    if field_type == "float":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if field_type == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    if field_type == "uint":
        return isinstance(value, int) and not isinstance(value, bool) and value >= 0
    if field_type == "string":
        return isinstance(value, str)
    return isinstance(value, bool)


def validate_tags(table_schema: dict, row: dict) -> tuple[bool, str]:
    """Check required tags and allowed tag values. Extra tags are ignored."""
    tags_schema = table_schema.get("tags", {})

    for tag_name, tag_def in tags_schema.items():
        if isinstance(tag_def, dict):
            required = tag_def.get("required", False)
            allowed_values = tag_def.get("allowed_values")
        else:
            # a bare tag name means the tag is required
            required = True
            allowed_values = None

        value = row.get(tag_name)

        if required and value is None:
            return False, f"Required tag '{tag_name}' is missing"

        if value is not None and allowed_values is not None:
            if str(value) not in [str(allowed) for allowed in allowed_values]:
                return (
                    False,
                    f"Tag '{tag_name}' value '{value}' is not in allowed values: {allowed_values}",
                )

    return True, ""


def validate_fields(table_schema: dict, row: dict) -> tuple[bool, str]:
    """Check required fields, field types and allowed field values."""
    fields_schema = table_schema["fields"]

    for field_name, field_def in fields_schema.items():
        if isinstance(field_def, dict):
            required = field_def.get("required", False)
            allowed_values = field_def.get("allowed_values")
        else:
            # a bare field name means the field is required
            required = True
            allowed_values = None

        value = row.get(field_name)

        if required and value is None:
            return False, f"Required field '{field_name}' is missing"

        if value is None:
            continue

        field_type = resolve_field_type(field_def)
        if field_type is not None and not check_field_type(value, field_type):
            return (
                False,
                f"Field '{field_name}' expected type '{field_def['type']}', "
                f"got {type(value).__name__} (value: {value})",
            )

        if allowed_values is not None:
            if value not in allowed_values and str(value) not in [
                str(allowed) for allowed in allowed_values
            ]:
                return (
                    False,
                    f"Field '{field_name}' value '{value}' is not in allowed values: {allowed_values}",
                )

    return True, ""


def validate_row(table_schema: dict, row: dict) -> tuple[bool, str]:
    """Validate one row, returning (is_valid, rejection_reason)."""
    is_valid, reason = validate_tags(table_schema, row)
    if not is_valid:
        return False, reason

    return validate_fields(table_schema, row)


# ---------------------------------------------------------------------------
# Line building
# ---------------------------------------------------------------------------


def build_line_from_row(table_schema: dict, measurement: str, row: dict):
    """Build a line from a validated row, keeping only schema-defined columns.

    Returns None when the row carries no schema-defined field. Raises ValueError
    when a value cannot be written as its type.
    """
    tags = {tag_name: row.get(tag_name) for tag_name in table_schema.get("tags", {})}

    typed_fields = {}
    for field_name, field_def in table_schema["fields"].items():
        value = row.get(field_name)
        if value is None:
            continue
        field_type = resolve_field_type(field_def) or infer_type(value)
        typed_fields[field_name] = (value, field_type)

    if not typed_fields:
        return None

    return build_line_typed(
        LineBuilder,
        measurement,
        tags=tags,
        typed_fields=typed_fields,
        time_ns=row.get("time"),
    )


class RejectionClock:
    """Hands out strictly increasing timestamps so rejections stay distinct."""

    def __init__(self):
        self._last_ns = 0

    def next_ns(self) -> int:
        self._last_ns = max(time.time_ns(), self._last_ns + 1)
        return self._last_ns


def build_rejection_line(table_name: str, reason: str, row: dict, time_ns: int):
    """Build a rejection log entry for one rejected row."""
    line = LineBuilder(REJECTION_MEASUREMENT)
    line.tag("source_table", table_name)
    line.string_field("reason", reason)
    line.string_field("row_data", str(row)[:ROW_DATA_LIMIT])
    line.time_ns(time_ns)
    return line


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def process_writes(influxdb3_local, table_batches: list, args: dict | None = None):
    """Validate incoming rows against a JSON schema and forward the valid ones.

    For every row of every table batch the plugin checks the measurement name,
    required tags and their allowed values, then required fields, their types and
    their allowed values. Valid rows are stripped down to the schema-defined
    columns and written to the target database or table; rejected rows are
    optionally logged and recorded in the '_schema_rejections' measurement.

    Args:
        influxdb3_local: The InfluxDB 3 local API object.
        table_batches (list): Table batch dicts with 'table_name' and 'rows'.
        args (dict | None): Trigger arguments dictionary.
    """
    task_id = str(uuid.uuid4())[:8]
    args = args or {}
    influxdb3_local.info(f"[{task_id}] Schema Validator plugin triggered")

    try:
        config_file_path = args.get("config_file_path") or parse_env(
            env_spec("config_file_path")
        ).get("config_file_path")
        config: Config = load_config(
            parse_env(ENV_SETTINGS),
            parse_trigger_args(args, CONFIG_KEYS),
            parse_toml(config_file_path, CONFIG_KEYS),
            validators=CONFIG_VALIDATORS,
        )
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to load configuration: {e}")
        return

    try:
        schema = load_schema(influxdb3_local, config["schema_file"], task_id)
        validate_targets(
            schema, config, {batch["table_name"] for batch in table_batches}
        )
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to load schema: {e}")
        return

    target_database = config["target_database"] or None
    log_rejected = config["log_rejected"]
    log_accepted = config["log_accepted"]
    write_rejections = config["write_rejection_log"]
    allowed_measurements = schema.get("allowed_measurements")
    rejection_clock = RejectionClock()

    total_accepted = 0
    total_rejected = 0
    total_dropped = 0

    for table_batch in table_batches:
        table_name = table_batch["table_name"]

        if allowed_measurements and table_name not in allowed_measurements:
            if log_rejected:
                influxdb3_local.info(
                    f"[{task_id}] Skipping table '{table_name}' - not in allowed_measurements"
                )
            continue

        table_schema = schema["tables"].get(table_name)
        if table_schema is None:
            if log_rejected:
                influxdb3_local.info(
                    f"[{task_id}] No schema defined for table '{table_name}', skipping"
                )
            continue

        measurement = target_measurement(table_schema, table_name, config)
        valid_lines = []
        rejection_lines = []
        batch_accepted = 0
        batch_rejected = 0

        for row in table_batch["rows"]:
            is_valid, reason = validate_row(table_schema, row)

            if is_valid:
                try:
                    line = build_line_from_row(table_schema, measurement, row)
                except ValueError as e:
                    is_valid, reason = False, str(e)
                else:
                    if line is None:
                        is_valid = False
                        reason = "No fields matched the schema definition"
                    else:
                        valid_lines.append(line)
                        batch_accepted += 1
                        if log_accepted:
                            influxdb3_local.info(
                                f"[{task_id}] ACCEPTED: {table_name} -> {measurement}"
                            )

            if not is_valid:
                batch_rejected += 1
                if log_rejected:
                    influxdb3_local.warn(
                        f"[{task_id}] REJECTED: {table_name} - {reason}"
                    )
                if write_rejections:
                    rejection_lines.append(
                        build_rejection_line(
                            table_name, reason, row, rejection_clock.next_ns()
                        )
                    )

        try:
            write_data(
                influxdb3_local,
                valid_lines,
                retries=0,
                no_sync=True,
                database=target_database,
            )
        except Exception as e:
            influxdb3_local.error(
                f"[{task_id}] Failed to write {batch_accepted} validated rows of "
                f"table '{table_name}': {e}"
            )
            total_dropped += batch_accepted
            batch_accepted = 0

        try:
            write_data(
                influxdb3_local,
                rejection_lines,
                retries=0,
                no_sync=True,
                database=target_database,
            )
        except Exception as e:
            influxdb3_local.error(
                f"[{task_id}] Failed to write rejection log of table '{table_name}': {e}"
            )

        total_accepted += batch_accepted
        total_rejected += batch_rejected

        influxdb3_local.info(
            f"[{task_id}] Table '{table_name}': {batch_accepted} accepted, {batch_rejected} rejected"
        )

    influxdb3_local.info(
        f"[{task_id}] Schema Validator complete: {total_accepted} total accepted, "
        f"{total_rejected} total rejected, {total_dropped} total dropped"
    )