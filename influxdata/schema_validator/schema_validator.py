"""
{
    "plugin_type": ["onwrite"],
    "onwrite_args_config": [
        {
            "name": "schema_file",
            "example": "schema_validator_config.json",
            "description": "Path to the JSON schema configuration file (relative to PLUGIN_DIR).",
            "required": true
        },
        {
            "name": "target_database",
            "example": "clean_db",
            "description": "Target database to write validated data to. If omitted, writes to the same database.",
            "required": false
        },
        {
            "name": "target_table_prefix",
            "example": "validated_",
            "description": "Prefix to add to measurement names when writing to the target. If omitted, uses the original measurement name.",
            "required": false
        },
        {
            "name": "target_table_suffix",
            "example": "_clean",
            "description": "Suffix to add to measurement names when writing to the target. If omitted, no suffix is added.",
            "required": false
        },
        {
            "name": "log_rejected",
            "example": "true",
            "description": "If 'true', logs details about rejected rows. Defaults to 'true'.",
            "required": false
        },
        {
            "name": "log_accepted",
            "example": "false",
            "description": "If 'true', logs details about accepted rows. Defaults to 'false'.",
            "required": false
        },
        {
            "name": "write_rejection_log",
            "example": "true",
            "description": "If 'true', writes rejected row details to a '_schema_rejections' measurement. Defaults to 'false'.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "schema_validator_trigger_config.toml",
            "description": "Path to TOML config file to override trigger arguments.",
            "required": false
        }
    ]
}
"""

import json
import os
import tomllib
import uuid
from pathlib import Path
from typing import Iterable, Optional, runtime_checkable, Protocol


# ---------------------------------------------------------------------------
# Batch write helper
# ---------------------------------------------------------------------------

@runtime_checkable
class _LineBuilderInterface(Protocol):
    def build(self) -> str: ...


class _BatchLines:
    """
    Wraps multiple LineBuilder objects into a single object with a build()
    method that returns a newline-separated string. This allows batched writes
    through the write_sync / write_sync_to_db APIs without changing the Rust code.
    """
    def __init__(self, line_builders: Iterable[_LineBuilderInterface]):
        self._line_builders = list(line_builders)
        self._built: Optional[str] = None

    def _coerce_builder(self, builder: _LineBuilderInterface) -> str:
        build_fn = getattr(builder, "build", None)
        if not callable(build_fn):
            raise TypeError("line_builder is missing a callable build()")
        return str(build_fn())

    def build(self) -> str:
        if self._built is None:
            lines = [self._coerce_builder(builder) for builder in self._line_builders]
            if not lines:
                raise ValueError("batch_write received no lines to build")
            self._built = "\n".join(lines)
        return self._built


# ---------------------------------------------------------------------------
# Schema loading
# ---------------------------------------------------------------------------

def load_schema(influxdb3_local, schema_file_path: str, task_id: str) -> dict | None:
    """
    Loads the JSON schema configuration file from the plugin directory.
    Uses the influxdb3_local cache to avoid re-reading the file on every WAL flush.
    """
    cache_key = f"schema_validator:{schema_file_path}"
    cached = influxdb3_local.cache.get(cache_key)
    if cached is not None:
        return cached

    plugin_dir_var = os.getenv("PLUGIN_DIR", None)
    if not plugin_dir_var:
        influxdb3_local.error(f"[{task_id}] PLUGIN_DIR environment variable is not set")
        return None

    plugin_dir = Path(plugin_dir_var)
    file_path = plugin_dir / schema_file_path

    try:
        with open(file_path, "r") as f:
            schema = json.load(f)
        influxdb3_local.info(f"[{task_id}] Loaded schema from {file_path}")
        # Cache for 300 seconds (5 minutes) so config changes are picked up reasonably fast
        influxdb3_local.cache.put(cache_key, schema, ttl=300)
        return schema
    except FileNotFoundError:
        influxdb3_local.error(f"[{task_id}] Schema file not found: {file_path}")
        return None
    except json.JSONDecodeError as e:
        influxdb3_local.error(f"[{task_id}] Invalid JSON in schema file: {e}")
        return None
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to load schema file: {e}")
        return None


def validate_schema_structure(influxdb3_local, schema: dict, task_id: str) -> bool:
    """
    Validates the basic structure of a loaded schema to catch malformed configs
    before they cause AttributeError crashes during row processing.
    """
    allowed = schema.get("allowed_measurements", None)
    if allowed is not None and not isinstance(allowed, list):
        influxdb3_local.error(f"[{task_id}] 'allowed_measurements' must be a list, got {type(allowed).__name__}")
        return False

    tables = schema.get("tables", None)
    if tables is not None and not isinstance(tables, dict):
        influxdb3_local.error(f"[{task_id}] 'tables' must be a dict, got {type(tables).__name__}")
        return False

    if not tables:
        influxdb3_local.error(f"[{task_id}] 'tables' is empty or missing — no measurements to validate")
        return False

    if isinstance(tables, dict):
        for table_name, table_def in tables.items():
            if not isinstance(table_def, dict):
                influxdb3_local.error(f"[{task_id}] Table '{table_name}' definition must be a dict, got {type(table_def).__name__}")
                return False
            tags = table_def.get("tags", None)
            if tags is not None and not isinstance(tags, dict):
                influxdb3_local.error(f"[{task_id}] Table '{table_name}' tags must be a dict, got {type(tags).__name__}")
                return False
            fields = table_def.get("fields", None)
            if fields is not None and not isinstance(fields, dict):
                influxdb3_local.error(f"[{task_id}] Table '{table_name}' fields must be a dict, got {type(fields).__name__}")
                return False

    return True


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def get_table_schema(schema: dict, table_name: str) -> dict | None:
    """
    Finds the schema definition for a given table/measurement name.
    Returns the table schema dict, or None if the table is not in the schema.
    """
    tables = schema.get("tables", {})
    return tables.get(table_name, None)


def validate_measurement(schema: dict, table_name: str) -> tuple[bool, str]:
    """
    Validates that the measurement/table name is allowed by the schema.
    If 'allowed_measurements' is a non-empty list, the table_name must be in that list.
    If omitted (None) or empty ([]), no measurement filter is applied — validation
    falls through to the table-level 'tables' rules.
    """
    allowed = schema.get("allowed_measurements", None)
    if allowed:
        if table_name not in allowed:
            return False, f"Measurement '{table_name}' is not in the allowed measurements list"
    return True, ""


def validate_tags(table_schema: dict, row: dict) -> tuple[bool, str]:
    """
    Validates the tags in a row against the table schema.

    Checks:
    1. All required tags are present and non-None
    2. If 'allowed_values' is specified for a tag, the value must be in that list

    Extra tags not defined in the schema are silently stripped during output.
    """
    tags_schema = table_schema.get("tags", {})
    if not tags_schema:
        return True, ""

    # Check required tags
    for tag_name, tag_def in tags_schema.items():
        if isinstance(tag_def, dict):
            required = tag_def.get("required", False)
            allowed_values = tag_def.get("allowed_values", None)
        else:
            # Simple definition: just the tag name listed means it's required
            required = True
            allowed_values = None

        value = row.get(tag_name, None)

        if required and value is None:
            return False, f"Required tag '{tag_name}' is missing"

        if value is not None and allowed_values is not None:
            if str(value) not in [str(v) for v in allowed_values]:
                return False, f"Tag '{tag_name}' value '{value}' is not in allowed values: {allowed_values}"

    return True, ""


def validate_fields(table_schema: dict, row: dict) -> tuple[bool, str]:
    """
    Validates the fields in a row against the table schema.

    Checks:
    1. All required fields are present and non-None
    2. Type validation if 'type' is specified
    3. Allowed values validation if 'allowed_values' is specified

    Extra fields not defined in the schema are silently stripped during output.
    """
    fields_schema = table_schema.get("fields", {})
    if not fields_schema:
        return True, ""

    for field_name, field_def in fields_schema.items():
        if isinstance(field_def, dict):
            required = field_def.get("required", False)
            field_type = field_def.get("type", None)
            allowed_values = field_def.get("allowed_values", None)
        else:
            required = True
            field_type = None
            allowed_values = None

        value = row.get(field_name, None)

        if required and value is None:
            return False, f"Required field '{field_name}' is missing"

        if value is not None:
            # Type validation
            if field_type is not None:
                if not check_field_type(value, field_type):
                    return False, f"Field '{field_name}' expected type '{field_type}', got {type(value).__name__} (value: {value})"

            # Allowed values validation
            if allowed_values is not None:
                if value not in allowed_values and str(value) not in [str(v) for v in allowed_values]:
                    return False, f"Field '{field_name}' value '{value}' is not in allowed values: {allowed_values}"

    return True, ""


def check_field_type(value, expected_type: str) -> bool:
    """
    Checks if a value matches the expected InfluxDB field type.
    Supported types: 'float', 'integer', 'string', 'boolean', 'uint64'
    Returns False for unknown/unrecognized types.
    """
    expected_type_lower = expected_type.lower()

    if expected_type_lower in ("float", "float64", "double"):
        return isinstance(value, (int, float))
    elif expected_type_lower in ("integer", "int", "int64"):
        return isinstance(value, int) and not isinstance(value, bool)
    elif expected_type_lower in ("string", "str"):
        return isinstance(value, str)
    elif expected_type_lower in ("boolean", "bool"):
        return isinstance(value, bool)
    elif expected_type_lower in ("uint64", "unsigned", "uint"):
        return isinstance(value, int) and not isinstance(value, bool) and value >= 0
    else:
        # Unknown type — reject to catch schema config typos
        return False


def validate_row(schema: dict, table_schema: dict, table_name: str, row: dict) -> tuple[bool, str]:
    """
    Validates a single row against the full schema.
    Returns (is_valid, rejection_reason).
    """
    # 1. Validate measurement name
    valid, reason = validate_measurement(schema, table_name)
    if not valid:
        return False, reason

    # 2. Validate tags
    valid, reason = validate_tags(table_schema, row)
    if not valid:
        return False, reason

    # 3. Validate fields
    valid, reason = validate_fields(table_schema, row)
    if not valid:
        return False, reason

    return True, ""


# ---------------------------------------------------------------------------
# Row writing helpers
# ---------------------------------------------------------------------------

def build_line_from_row(table_schema: dict, target_measurement: str, row: dict) -> "LineBuilder | None":
    """
    Builds a LineBuilder object from a validated row.
    Separates tags from fields based on the schema definition.
    Returns None if no fields were added (line protocol requires at least one field).
    """
    builder = LineBuilder(target_measurement)

    tags_schema = table_schema.get("tags", {})
    fields_schema = table_schema.get("fields", {})

    # Set timestamp if present
    ts = row.get("time", None)
    if ts is not None:
        builder.time_ns(ts)

    # Add tags in deterministic order (dict preserves insertion order in Python 3.7+)
    for tag_name in tags_schema:
        value = row.get(tag_name, None)
        if value is not None:
            builder.tag(tag_name, str(value))

    # Add fields with proper typing in deterministic order
    field_count = 0
    for field_name in fields_schema:
        value = row.get(field_name, None)
        if value is None:
            continue

        # Determine the correct field method based on schema type or actual value type
        field_def = fields_schema.get(field_name, {})
        if isinstance(field_def, dict):
            field_type = field_def.get("type", None)
        else:
            field_type = None

        if field_type:
            _add_typed_field(builder, field_name, value, field_type)
        else:
            _add_inferred_field(builder, field_name, value)
        field_count += 1

    # Line protocol requires at least one field
    if field_count == 0:
        return None

    return builder


def _add_typed_field(builder, field_name: str, value, field_type: str):
    """Adds a field to the LineBuilder using the schema-defined type."""
    ft = field_type.lower()
    if ft in ("float", "float64", "double"):
        builder.float64_field(field_name, float(value))
    elif ft in ("integer", "int", "int64"):
        builder.int64_field(field_name, int(value))
    elif ft in ("uint64", "unsigned", "uint"):
        builder.uint64_field(field_name, int(value))
    elif ft in ("string", "str"):
        builder.string_field(field_name, str(value))
    elif ft in ("boolean", "bool"):
        builder.bool_field(field_name, bool(value))
    else:
        _add_inferred_field(builder, field_name, value)


def _add_inferred_field(builder, field_name: str, value):
    """Adds a field to the LineBuilder by inferring its type from the Python value."""
    if isinstance(value, bool):
        builder.bool_field(field_name, value)
    elif isinstance(value, int):
        builder.int64_field(field_name, value)
    elif isinstance(value, float):
        builder.float64_field(field_name, value)
    else:
        builder.string_field(field_name, str(value))


def write_rejection_log_entry(influxdb3_local, target_database: str | None, table_name: str, reason: str, row: dict):
    """
    Writes a rejection log entry to the _schema_rejections measurement.
    """
    builder = LineBuilder("_schema_rejections")
    builder.tag("source_table", table_name)
    builder.string_field("reason", reason)
    builder.string_field("row_data", str(row)[:1024])  # Truncate to avoid huge writes

    if target_database:
        influxdb3_local.write_sync_to_db(target_database, builder, no_sync=True)
    else:
        influxdb3_local.write_sync(builder, no_sync=True)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def process_writes(influxdb3_local, table_batches: list, args: dict | None = None):
    """
    Schema Validator Plugin - Validates incoming data against a JSON schema definition
    and writes only conforming rows to a target database/table.

    This plugin is triggered on each WAL flush. For every row in the incoming data:
    1. Checks if the measurement/table name is allowed
    2. Validates that required tags are present and have allowed values
    3. Validates that required fields are present, have correct types, and allowed values
    4. Strips any extra tags/fields not defined in the schema
    5. Writes valid rows to the target (same or different database/table)
    6. Optionally logs and records rejected rows

    Args:
        influxdb3_local: The InfluxDB 3 local API object.
        table_batches (list): List of table batch dicts, each with 'table_name' and 'rows'.
        args (dict | None): Trigger arguments dictionary.
    """
    task_id = str(uuid.uuid4())[:8]
    influxdb3_local.info(f"[{task_id}] Schema Validator plugin triggered")

    try:
        # ---- Load args from config file if specified ----
        if args:
            if path := args.get("config_file_path", None):
                try:
                    plugin_dir_var = os.getenv("PLUGIN_DIR", None)
                    if not plugin_dir_var:
                        influxdb3_local.error(f"[{task_id}] PLUGIN_DIR env var not set")
                        return
                    plugin_dir = Path(plugin_dir_var)
                    file_path = plugin_dir / path
                    influxdb3_local.info(f"[{task_id}] Reading trigger config from {file_path}")
                    with open(file_path, "rb") as f:
                        args = tomllib.load(f)
                except Exception as e:
                    influxdb3_local.error(f"[{task_id}] Failed to read config file: {e}")
                    return

        # ---- Validate required args ----
        if not args or "schema_file" not in args:
            influxdb3_local.error(f"[{task_id}] Missing required argument: 'schema_file'")
            return

        # ---- Parse configuration ----
        schema_file = args["schema_file"]
        target_database = args.get("target_database", None)
        target_table_prefix = args.get("target_table_prefix", "")
        target_table_suffix = args.get("target_table_suffix", "")
        log_rejected = str(args.get("log_rejected", "true")).lower() == "true"
        log_accepted = str(args.get("log_accepted", "false")).lower() == "true"
        write_rejections = str(args.get("write_rejection_log", "false")).lower() == "true"

        # ---- Load schema ----
        schema = load_schema(influxdb3_local, schema_file, task_id)
        if schema is None:
            influxdb3_local.error(f"[{task_id}] Cannot proceed without a valid schema")
            return

        # ---- Validate schema structure ----
        if not validate_schema_structure(influxdb3_local, schema, task_id):
            return

        # ---- Process each table batch ----
        total_accepted = 0
        total_rejected = 0

        for table_batch in table_batches:
            table_name = table_batch["table_name"]
            rows = table_batch["rows"]

            # Check if this measurement is even relevant to our schema
            # If allowed_measurements is a non-empty list, skip tables not in it
            allowed_measurements = schema.get("allowed_measurements", None)
            if allowed_measurements and table_name not in allowed_measurements:
                if log_rejected:
                    influxdb3_local.info(
                        f"[{task_id}] Skipping table '{table_name}' - not in allowed_measurements"
                    )
                continue

            # Get table-specific schema; if not defined, skip
            table_schema = get_table_schema(schema, table_name)
            if table_schema is None:
                if log_rejected:
                    influxdb3_local.info(
                        f"[{task_id}] No schema defined for table '{table_name}', skipping"
                    )
                continue

            # Determine target measurement name
            target_table = table_schema.get("target_table", None)
            if target_table:
                target_measurement = target_table
            else:
                target_measurement = f"{target_table_prefix}{table_name}{target_table_suffix}"

            # Validate each row, collect valid LineBuilders for batched write
            batch_accepted = 0
            batch_rejected = 0
            valid_line_builders = []

            for row in rows:
                is_valid, reason = validate_row(schema, table_schema, table_name, row)

                if is_valid:
                    line = build_line_from_row(table_schema, target_measurement, row)
                    if line is None:
                        reason = "No fields matched the schema definition"
                        is_valid = False
                    else:
                        valid_line_builders.append(line)
                        batch_accepted += 1

                        if log_accepted:
                            influxdb3_local.info(
                                f"[{task_id}] ACCEPTED: {table_name} -> {target_measurement}"
                            )

                if not is_valid:
                    batch_rejected += 1

                    if log_rejected:
                        influxdb3_local.warn(
                            f"[{task_id}] REJECTED: {table_name} - {reason}"
                        )

                    if write_rejections:
                        try:
                            write_rejection_log_entry(
                                influxdb3_local, target_database, table_name, reason, row
                            )
                        except Exception as e:
                            influxdb3_local.error(
                                f"[{task_id}] Failed to write rejection log: {e}"
                            )

            # Batched write of all valid rows for this table
            if valid_line_builders:
                batch = _BatchLines(valid_line_builders)
                if target_database:
                    influxdb3_local.write_sync_to_db(target_database, batch, no_sync=True)
                else:
                    influxdb3_local.write_sync(batch, no_sync=True)

            total_accepted += batch_accepted
            total_rejected += batch_rejected

            influxdb3_local.info(
                f"[{task_id}] Table '{table_name}': {batch_accepted} accepted, {batch_rejected} rejected"
            )

        influxdb3_local.info(
            f"[{task_id}] Schema Validator complete: {total_accepted} total accepted, {total_rejected} total rejected"
        )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Unexpected error in process_writes: {e}")
