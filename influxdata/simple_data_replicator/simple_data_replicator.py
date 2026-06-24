"""
{
    "plugin_type": ["scheduled", "onwrite"],
    "scheduled_args_config": [
        {
            "name": "host",
            "example": "https://remote-influxdb.com:8181",
            "description": "Remote InfluxDB host URL.",
            "required": true
        },
        {
            "name": "remote_token",
            "example": "remote_api_token",
            "description": "Remote InfluxDB API token. Falls back to the REMOTE_INFLUXDB_TOKEN environment variable if not provided.",
            "required": false
        },
        {
            "name": "database",
            "example": "remote_db",
            "description": "Remote database name.",
            "required": true
        },
        {
            "name": "source_measurement",
            "example": "table1",
            "description": "The name of the measurement to replicate.",
            "required": true
        },
        {
            "name": "window",
            "example": "10s",
            "description": "Time window for each replication job (e.g., '1h', '1d'). Units: 's', 'min', 'h', 'd', 'w'.",
            "required": true
        },
        {
            "name": "unique_file_suffix",
            "example": "suffix123",
            "description": "Unique suffix for the queue file to avoid conflicts.",
            "required": true
        },
        {
            "name": "max_size",
            "example": "512",
            "description": "Maximum size for the queue file in MB.",
            "required": false
        },
        {
            "name": "verify_ssl",
            "example": "true",
            "description": "Whether to verify SSL certificates when connecting via HTTPS.",
            "required": false
        },
        {
            "name": "max_retries",
            "example": "3",
            "description": "Maximum number of retries for write operations.",
            "required": false
        },
        {
            "name": "queue_flush_chunk_size",
            "example": "500",
            "description": "Number of line-protocol entries sent per remote flush request.",
            "required": false
        },
        {
            "name": "excluded_fields",
            "example": "field1 field2 tag1",
            "description": "Space-separated list of fields and tags to exclude from the source measurement (e.g., 'field1 field2 tag1').",
            "required": false
        },
        {
            "name": "target_table",
            "example": "new_table_name",
            "description": "New name for the measurement in the remote database. Defaults to source_measurement.",
            "required": false
        },
        {
            "name": "field_renames",
            "example": "hum:humidity temp:temperature",
            "description": "Field renames for the source measurement in the format 'old1:new1 old2:new2'.",
            "required": false
        },
        {
            "name": "offset",
            "example": "10min",
            "description": "Time offset to apply to the window (e.g., '10min', '1h'). Units: 's', 'min', 'h', 'd', 'w'.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "config.toml",
            "description": "Path to config file to override args. Format: 'config.toml'.",
            "required": false
        }
    ],
    "onwrite_args_config": [
        {
            "name": "host",
            "example": "https://remote-influxdb.com:8181",
            "description": "Remote InfluxDB host URL.",
            "required": true
        },
        {
            "name": "remote_token",
            "example": "remote_api_token",
            "description": "Remote InfluxDB API token. Falls back to the REMOTE_INFLUXDB_TOKEN environment variable if not provided.",
            "required": false
        },
        {
            "name": "database",
            "example": "remote_db",
            "description": "Remote database name.",
            "required": true
        },
        {
            "name": "unique_file_suffix",
            "example": "suffix123",
            "description": "Unique suffix for the queue file to avoid conflicts.",
            "required": true
        },
        {
            "name": "tables",
            "example": "table1 table2",
            "description": "Space-separated list of tables to replicate. If not provided, all tables are replicated.",
            "required": false
        },
        {
            "name": "verify_ssl",
            "example": "true",
            "description": "Whether to verify SSL certificates when connecting via HTTPS.",
            "required": false
        },
        {
            "name": "max_size",
            "example": "512",
            "description": "Maximum size for the queue file in MB.",
            "required": false
        },
        {
            "name": "queue_flush_chunk_size",
            "example": "500",
            "description": "Number of line-protocol entries sent per remote flush request.",
            "required": false
        },
        {
            "name": "excluded_fields",
            "example": "table1:field1|field2|tag1 table2:field3|tag2",
            "description": "String defining fields and tags to exclude per table (e.g., '<table1>:<field1>|<field2>|<tag1> <table2>:<field3>|<tag2>').",
            "required": false
        },
        {
            "name": "tables_rename",
            "example": "table1:new_table1 table2:new_table2",
            "description": "String defining table renames (e.g., '<old_table1>:<new_table1> <old_table2>:<new_table2>').",
            "required": false
        },
        {
            "name": "field_renames",
            "example": "table1:hum=humidity|temp=temperature table2:oldX=newX",
            "description": "String defining field renames per table (e.g., 'table1:oldA=newA|oldB=newB table2:oldX=newX').",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "config.toml",
            "description": "Path to config file to override args. Format: 'config.toml'.",
            "required": false
        }
    ]
}
"""

import gzip
import json
import os
import re
import time
import tomllib
import urllib.parse
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from influxdb_client_3 import InfluxDBClient3, InfluxDBError, Point

# Cache key prefix for the remote client reused across invocations.
CACHE_CLIENT = "sdr_remote_client"

# Default number of line-protocol entries written per remote flush request.
DEFAULT_QUEUE_FLUSH_CHUNK_SIZE = 500


def resolve_plugin_path(path: str, description: str) -> str:
    """Resolve path - absolute paths used as-is, relative paths resolved from PLUGIN_DIR, falling back to server-derivable locations."""
    if os.path.isabs(path):
        return path

    plugin_dir: str | None = os.environ.get("PLUGIN_DIR")
    if plugin_dir:
        return os.path.join(plugin_dir, path)

    # Fallbacks for servers where the operator has not exported PLUGIN_DIR:
    #  - INFLUXDB3_PLUGIN_DIR: set when the server is configured via env var
    #  - VIRTUAL_ENV: exported by the processing engine; default venv is <plugin-dir>/.venv
    candidates: list[str] = []
    if influxdb3_plugin_dir := os.environ.get("INFLUXDB3_PLUGIN_DIR"):
        candidates.append(influxdb3_plugin_dir)
    if virtual_env := os.environ.get("VIRTUAL_ENV"):
        candidates.append(str(Path(virtual_env).parent))

    for base in candidates:
        candidate = os.path.join(base, path)
        if os.path.exists(candidate):
            return candidate

    raise ValueError(
        f"PLUGIN_DIR environment variable not set and {description} path "
        f"'{path}' was not found via fallbacks "
        f"(tried: {', '.join(candidates) or 'none available'})."
    )


def resolve_plugin_dir() -> Path:
    """Resolve the plugin directory used for queue file storage.

    Prefers PLUGIN_DIR, then INFLUXDB3_PLUGIN_DIR, then the parent of
    VIRTUAL_ENV (the processing engine's default venv lives at
    <plugin-dir>/.venv).
    """
    if plugin_dir := os.environ.get("PLUGIN_DIR"):
        return Path(plugin_dir)
    if influxdb3_plugin_dir := os.environ.get("INFLUXDB3_PLUGIN_DIR"):
        return Path(influxdb3_plugin_dir)
    if virtual_env := os.environ.get("VIRTUAL_ENV"):
        return Path(virtual_env).parent
    raise ValueError(
        "Could not resolve plugin directory: none of PLUGIN_DIR, "
        "INFLUXDB3_PLUGIN_DIR, or VIRTUAL_ENV environment variables are set."
    )


def load_toml_config(config_file: str) -> dict:
    """Load configuration from a TOML file, resolving its path via PLUGIN_DIR."""
    if not config_file.endswith(".toml"):
        raise ValueError("Invalid config file format: expected a .toml file")

    config_path: str = resolve_plugin_path(config_file, "configuration file")
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def ensure_queue_file(queue_file: Path) -> None:
    """Ensure the queue file directory exists."""
    if not queue_file.parent.exists():
        queue_file.parent.mkdir(parents=True)


def append_to_queue(queue_file: Path, entries: list, max_size: int) -> bool:
    """
    Append serialized entries to a compressed JSONL queue file.

    Args:
        queue_file (Path): Path to the queue file.
        entries (list): List of dictionaries to append to the queue.
        max_size (int): Maximum size in MB for the queue file.

    Returns:
        bool: True if the entries were appended, False if the queue already
              reached max_size and the append was skipped.
    """
    ensure_queue_file(queue_file)
    max_size_bytes: int = max_size * 1024 * 1024  # Convert MB to bytes
    if os.path.exists(queue_file) and os.path.getsize(queue_file) >= max_size_bytes:
        return False
    with gzip.open(queue_file, "at", encoding="utf-8") as f:
        for entry in entries:
            # Only store serializable fields (table, line)
            queue_entry = {"table": entry["table"], "line": entry["line"]}
            f.write(json.dumps(queue_entry) + "\n")
    return True


def read_queue(queue_file: Path) -> list:
    """Read all lines from the queue file."""
    ensure_queue_file(queue_file)
    if not queue_file.exists():
        return []
    with gzip.open(queue_file, "rt", encoding="utf-8") as f:
        return [json.loads(line.strip()) for line in f if line.strip()]


def rewrite_queue(remaining_entries: list, queue_file: Path) -> None:
    """Rewrite the queue file with the entries that were not flushed."""
    with gzip.open(queue_file, "wt", encoding="utf-8") as f:
        for entry in remaining_entries:
            f.write(json.dumps(entry) + "\n")


def row_to_point(
    influxdb3_local,
    old_table_name: str,
    new_table_name: str,
    row: dict,
    task_id: str,
    excluded_fields: list,
    table_fields_renames: dict,
    all_tags: list,
):
    """
    Convert a row dictionary into InfluxDB Point object.

    Args:
        influxdb3_local: Logger or cache client for info logging.
        old_table_name (str): Original table name in the local database.
        new_table_name (str): Mapped table name for remote DB.
        row (dict): Row data to convert.
        task_id (str): Unique ID used for logging.
        excluded_fields (list): Fields and tags to exclude from replication.
        table_fields_renames (dict, optional): Mapping of field renames for this table.
        all_tags (list, optional): List of tags.

    Returns:
        Point | None: Point object or None if row is invalid.
    """
    if not row:
        influxdb3_local.info(
            f"[{task_id}] Skipping row in table {old_table_name}: row is empty"
        )
        return None

    # Create Point with measurement name
    point = Point(new_table_name)

    # Add timestamp
    if "time" in row and row["time"] is not None:
        point.time(row["time"])

    # Tags are columns known to be tags; everything else is a field.
    # The client serializes and validates field types (int/float/bool/str) itself.
    tags: dict = {
        table_fields_renames.get(k, k): str(v)
        for k, v in row.items()
        if k != "time" and v is not None and k not in excluded_fields and k in all_tags
    }

    fields: dict = {
        table_fields_renames.get(k, k): v
        for k, v in row.items()
        if k != "time"
        and v is not None
        and k not in excluded_fields
        and k not in all_tags
    }

    # Add tags to point
    for tag_key, tag_value in tags.items():
        point.tag(tag_key, tag_value)

    # Add fields to point
    has_fields = False
    for field_key, field_value in fields.items():
        point.field(field_key, field_value)
        has_fields = True

    if not has_fields:
        influxdb3_local.info(
            f"[{task_id}] Skipping row in table {old_table_name}: no fields provided - row: {row}"
        )
        return None

    return point


def parse_exclusions_for_data_writes(args: dict, task_id: str) -> dict[str, list[str]]:
    """
    Parse a string defining fields and tags to exclude from replication per table or use values from config file.

    Args:
        args (dict): Dictionary of runtime arguments. Should contain the 'excluded_fields' key
                     in the format "<table1>:<field1>|<field2>|<tag1> <table2>:<field3>|<tag2> ..."
        task_id (str): Task identifier for logging.

    Returns:
        dict[str, list[str]]: Mapping of table names to lists of fields and tags to exclude.

    Raises:
        Exception: If the string format is invalid (a block is missing ':').
    """
    input_val: str | dict | None = args.get("excluded_fields", None)
    exclusions: dict = {}
    if not input_val:
        return exclusions

    if args["use_config_file"]:
        if isinstance(input_val, dict):
            return input_val
        else:
            raise Exception(
                f"[{task_id}] excluded_fields must be a dict when using config file"
            )

    for block in input_val.split(" "):
        block = block.strip()
        if not block:
            continue

        if ":" not in block:
            raise Exception(f"[{task_id}] Invalid segment '{block}': missing ':'")

        table, fields_raw = block.split(":", 1)

        if not fields_raw:
            exclusions[table] = []
            continue

        fields: list = [field for field in fields_raw.split("|") if field]
        exclusions[table] = fields

    return exclusions


def parse_exclusions_for_schedule(
    influxdb3_local, args: dict, task_id: str
) -> list[str]:
    """
    Parse a string defining fields and tags to exclude or use values from config file.

    Args:
        influxdb3_local: Logger or cache client for info logging.
        args (dict): Dictionary of runtime arguments. Should contain the 'excluded_fields' key
                     in the format "<field1> <field2> <tag1> ..." (space-separated)
        task_id (str): Task identifier for logging.

    Returns:
        list[str]: list of field and tag names to exclude.
    """
    input_val: str | list | None = args.get("excluded_fields", None)
    if not input_val:
        return []

    if args["use_config_file"]:
        if isinstance(input_val, list):
            return input_val
        else:
            influxdb3_local.warn(
                f"[{task_id}] excluded_fields must be a list when using config file, skipping..."
            )
            return []
    else:
        return [field for field in input_val.split(" ") if field]


def parse_table_renames(args: dict, task_id: str) -> dict[str, str]:
    """
    Parse table renaming rules from a space-separated string or use values from config file.

    Args:
        args (dict): Dictionary of runtime arguments. Must contain the 'tables_rename' key with format:
                     "<old_table1>:<new_table1> <old_table2>:<new_table2> ..."
        task_id (str): Task identifier used for log messages.

    Returns:
        dict[str, str]: Mapping of old table names to new names.

    Raises:
        Exception: If format is invalid (a pair is missing ':').
    """
    mapping_input: str | dict | None = args.get("tables_rename", None)
    if mapping_input is None:
        return {}

    if args["use_config_file"]:
        if isinstance(mapping_input, dict):
            return mapping_input
        else:
            raise Exception(
                f"[{task_id}] tables_rename must be a dict when using config file"
            )

    renames: dict = {}

    pairs: list = mapping_input.split(" ")
    for pair in pairs:
        if not pair:
            continue

        if ":" not in pair:
            raise Exception(f"[{task_id}] Invalid mapping pair: '{pair}' (missing ':')")

        old, new = pair.split(":", 1)
        renames[old] = new

    return renames


def parse_field_renames_for_data_writes(
    args: dict, task_id: str
) -> dict[str, dict[str, str]]:
    """
    Parse a complex mapping string for renaming fields in specific tables or use values from config file.

    Format example:
        "table1:oldA=newA|oldB=newB table2:oldX=newX|oldY=newY"

    Args:
        args (dict): Arguments containing the 'field_renames' key.
        task_id (str): Task identifier for logs.

    Returns:
        dict[str, dict[str, str]]: Nested dict mapping table names to old-new field name pairs.

    Raises:
        Exception: If a segment is missing ':' or a mapping is missing '='.
    """
    mapping_input: str | dict | None = args.get("field_renames", None)
    if not mapping_input:
        return {}

    if args["use_config_file"]:
        if isinstance(mapping_input, dict):
            return mapping_input
        else:
            raise Exception(
                f"[{task_id}] field_renames must be a dict when using config file"
            )

    table_renames: dict = {}
    for table_block in mapping_input.split(" "):
        table_block = table_block.strip()
        if not table_block:
            continue  # skip empty blocks

        if ":" not in table_block:
            raise Exception(f"[{task_id}] Invalid segment '{table_block}': missing ':'")

        table, fields_part = table_block.split(":", 1)

        # initialize inner mapping
        renames: dict = {}

        if fields_part:
            # split individual old=new mappings
            for mapping in fields_part.split("|"):
                if "=" not in mapping:
                    raise Exception(
                        f"[{task_id}] Invalid field mapping '{mapping}' in table '{table}': missing '='"
                    )
                old_field, new_field = mapping.split("=", 1)
                renames[old_field] = new_field

        table_renames[table] = renames

    return table_renames


def parse_field_renames_for_schedule(args: dict, task_id: str) -> dict[str, str]:
    """
    Parse a complex mapping string for renaming fields or use values from config file.

    Format example:
        "oldA:newA oldB:newB"

    Args:
        args (dict): Arguments containing the 'field_renames' key.
        task_id (str): Task identifier for logs.

    Returns:
        dict[str, str]: Dict mapping old to new name.
    """
    mapping_input: str | dict = args.get("field_renames", "")
    field_renames: dict = {}

    if args["use_config_file"]:
        if isinstance(mapping_input, dict):
            return mapping_input
        else:
            raise Exception(
                f"[{task_id}] field_renames must be a dict when using config file"
            )

    for table_block in mapping_input.split(" "):
        if not table_block:
            continue  # skip empty blocks

        if ":" not in table_block:
            raise Exception(f"[{task_id}] Invalid segment '{table_block}': missing ':'")

        old_field, new_field = table_block.split(":", 1)
        field_renames[old_field] = new_field

    return field_renames


def parse_max_retries(args: dict, task_id: str) -> int:
    """
    Parse and validate the 'max_retries' argument, converting it from string to int.

    Args:
        args (dict): Runtime arguments containing 'max_retries'.
        task_id (str): Unique task identifier for logging context.

    Returns:
        int: Parsed number of retries (>= 1).

    Raises:
        Exception: If 'max_retries' is missing, not an integer, or less than 1.
    """
    raw: str | int = args.get("max_retries", 3)
    try:
        max_retries = int(raw)
    except (TypeError, ValueError):
        raise Exception(f"[{task_id}] Invalid max_retries, not an integer: {raw!r}")

    if max_retries < 1:
        raise Exception(f"[{task_id}] Invalid max_retries, must be >= 1: {max_retries}")

    return max_retries


def parse_chunk_size(args: dict, task_id: str) -> int:
    """
    Parse and validate the 'queue_flush_chunk_size' argument.
    """
    raw: str | int = args.get("queue_flush_chunk_size", DEFAULT_QUEUE_FLUSH_CHUNK_SIZE)
    try:
        chunk_size = int(raw)
    except (TypeError, ValueError):
        raise Exception(
            f"[{task_id}] Invalid queue_flush_chunk_size, not an integer: {raw!r}"
        )

    if chunk_size < 1:
        raise Exception(
            f"[{task_id}] Invalid queue_flush_chunk_size, must be >= 1: {chunk_size}"
        )

    return chunk_size


def resolve_remote_token(args: dict, task_id: str) -> str:
    """
    Resolve the remote InfluxDB token from args/config or the environment.
    """
    token: str | None = args.get("remote_token") or os.getenv("REMOTE_INFLUXDB_TOKEN")
    if not token:
        raise Exception(
            f"[{task_id}] Missing remote token: set the 'remote_token' argument "
            f"or the REMOTE_INFLUXDB_TOKEN environment variable"
        )
    return token


def get_tag_names(influxdb3_local, measurement: str, task_id: str) -> list[str]:
    """
    Retrieves the list of tag names for a measurement from cache or the database.

    Args:
        influxdb3_local: InfluxDB client instance.
        measurement (str): Name of the measurement to query.
        task_id (str): The task ID.

    Returns:
        list[str]: List of tag names with 'Dictionary(Int32, Utf8)' data type.
    """
    # check cache first
    tags: list = influxdb3_local.cache.get(f"{measurement}_tags")
    if tags:
        return tags

    # if not in cache, query the database
    query: str = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $measurement
        AND data_type = 'Dictionary(Int32, Utf8)'
    """
    res: list[dict] = influxdb3_local.query(query, {"measurement": measurement})

    if not res:
        influxdb3_local.info(
            f"[{task_id}] No tags found for measurement '{measurement}'."
        )
        return []

    tag_names: list[str] = [tag["column_name"] for tag in res]

    # cache the result for 1 hour
    influxdb3_local.cache.put(f"{measurement}_tags", tag_names, 60 * 60)

    return tag_names


def parse_host(raw: str, task_id: str) -> tuple[str, int]:
    """
    Parses a host string and returns a tuple (base_url, port).

    Args:
        raw (str): The host string, potentially quoted or missing scheme/port.
        task_id (str): Unique task identifier for logging context.

    Returns:
        tuple[str, int]: A tuple where the first element is the base URL
                         (e.g., "http://123.12.12.1" or "https://example.com")
                         and the second element is the port as an integer.

    Behavior:
        - Strips surrounding quotes (single or double) if present.
        - If no scheme (http or https) is specified, defaults to "http".
        - If no port is specified, defaults to 8181.
        - Supports IPv4, domain names, and IPv6 (with or without brackets).
    Raises:
        ValueError: If the hostname cannot be determined.
    """
    host = raw.strip()
    # Remove surrounding quotes if present
    if len(host) >= 2 and host[0] == host[-1] and host[0] in ('"', "'"):
        host = host[1:-1].strip()

    # Prepend default scheme if missing
    if not host.startswith(("http://", "https://")):
        host_with_scheme: str = "http://" + host
    else:
        host_with_scheme = host

    parsed = urllib.parse.urlparse(host_with_scheme)
    scheme: str = parsed.scheme or "http"
    hostname: str = parsed.hostname
    if not hostname:
        raise ValueError(
            f"[{task_id}] Invalid host string: '{raw}'. No hostname found."
        )

    # Determine port, defaulting to 8181 if not specified
    port: int = parsed.port or 8181

    # Reconstruct base URL, handling IPv6 hostnames with brackets if needed
    if ":" in hostname:  # crude check for IPv6
        base = f"{scheme}://[{hostname}]"
    else:
        base = f"{scheme}://{hostname}"

    return base, port


def get_remote_client(
    influxdb3_local,
    cache_key: str,
    host: str,
    port: int,
    token: str,
    database: str,
    verify_ssl: bool,
    task_id: str,
) -> InfluxDBClient3:
    """
    Return a remote InfluxDBClient3, reusing a cached one when the connection
    settings are unchanged. The client is rebuilt (and the old one closed) when
    any connection setting changes.

    Args:
        influxdb3_local: Local InfluxDB client providing the cache.
        cache_key (str): Cache key for the client, unique per trigger.
        host (str): Remote base URL.
        port (int): Remote port.
        token (str): Remote API token.
        database (str): Remote database name.
        verify_ssl (bool): Whether to verify SSL certificates.
        task_id (str): Unique task identifier for logging.

    Returns:
        InfluxDBClient3: A ready-to-use remote client.
    """
    signature: tuple = (host, port, database, verify_ssl, token)
    cached: dict | None = influxdb3_local.cache.get(cache_key)
    if cached and cached["signature"] == signature:
        influxdb3_local.info(f"[{task_id}] Reusing cached remote client")
        return cached["client"]

    if cached:
        try:
            cached["client"].close()
        except Exception:
            pass

    client = InfluxDBClient3(
        write_port_overwrite=port,
        host=host,
        token=token,
        database=database,
        verify_ssl=verify_ssl,
    )
    influxdb3_local.cache.put(cache_key, {"signature": signature, "client": client})
    influxdb3_local.info(f"[{task_id}] Created new remote client")
    return client


def invalidate_remote_client(influxdb3_local, cache_key: str) -> None:
    """Close and evict the cached remote client so it is rebuilt next cycle."""
    cached: dict | None = influxdb3_local.cache.get(cache_key)
    if cached:
        try:
            cached["client"].close()
        except Exception:
            pass
        influxdb3_local.cache.delete(cache_key)


def _write_chunk_with_retries(
    influxdb3_local,
    client,
    chunk: list,
    task_id: str,
    max_retries: int,
    remote_host: str,
    remote_port: int,
    cache_key: str,
) -> bool:
    """
    Write a single chunk of line-protocol entries to remote InfluxDB with retries.

    On exhausted retries due to a connection/transport failure, the cached
    remote client is evicted so the next cycle rebuilds a fresh one.

    Returns:
        bool: True if the chunk was written, False if retries were exhausted.
    """
    lines: list = [entry["line"] for entry in chunk]
    for attempt in range(max_retries):
        try:
            client.write(lines)
            return True

        except InfluxDBError as e:
            if e.response and e.response.status == 429:
                try:
                    retry_after = int(e.response.headers.get("retry-after", 2**attempt))
                except (TypeError, ValueError):
                    retry_after = 2**attempt
                if attempt < max_retries - 1:
                    influxdb3_local.info(
                        f"[{task_id}] Rate limit hit, retrying after {retry_after}s"
                    )
                    time.sleep(retry_after)
                else:
                    influxdb3_local.error(
                        f"[{task_id}] Rate limit hit and max retries reached; data remains in queue"
                    )
                    return False
            else:
                influxdb3_local.error(
                    f"[{task_id}] Replication attempt {attempt + 1} to {remote_host}:{remote_port} failed: {e}"
                )
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)
                else:
                    influxdb3_local.error(
                        f"[{task_id}] Max retries reached; data remains in queue"
                    )
                    invalidate_remote_client(influxdb3_local, cache_key)
                    return False

        except Exception as e:
            influxdb3_local.error(
                f"[{task_id}] Replication attempt {attempt + 1} to {remote_host}:{remote_port} failed: {e}"
            )
            if attempt < max_retries - 1:
                time.sleep(2**attempt)
            else:
                influxdb3_local.error(
                    f"[{task_id}] Max retries reached; data remains in queue"
                )
                invalidate_remote_client(influxdb3_local, cache_key)
                return False

    return False


def _flush_queue_with_retries(
    influxdb3_local,
    client,
    queue_file: Path,
    queued_entries: list,
    task_id: str,
    max_retries: int,
    remote_host: str,
    remote_port: int,
    cache_key: str,
    chunk_size: int,
) -> None:
    """
    Flush queued line-protocol entries to remote InfluxDB in chunks of
    chunk_size. Chunks are sent in order, so the flushed entries are always a
    prefix of the queue; on a chunk failure the flush stops and the remaining
    suffix is rewritten back to the queue file.
    """
    flushed_count: int = 0
    for start in range(0, len(queued_entries), chunk_size):
        chunk: list = queued_entries[start : start + chunk_size]
        if not _write_chunk_with_retries(
            influxdb3_local,
            client,
            chunk,
            task_id,
            max_retries,
            remote_host,
            remote_port,
            cache_key,
        ):
            break
        flushed_count += len(chunk)
        influxdb3_local.info(
            f"[{task_id}] Replicated {flushed_count}/{len(queued_entries)} lines to remote instance"
        )

    if flushed_count:
        rewrite_queue(queued_entries[flushed_count:], queue_file)


def process_writes(influxdb3_local, table_batches: list, args: dict | None = None):
    """
    Replicate incoming data rows to a remote InfluxDB 3 instance.

    Performs table filtering, field renaming, timestamp handling and manages a local queue to ensure reliable delivery.

    Args:
        influxdb3_local: Local InfluxDB 3-compatible client or logger.
        table_batches (list): List of dictionaries containing rows from WAL flush.
        args (dict, optional): Runtime config including remote host, remote_token, database and options.

    Raises:
        Exception: Only internally, caught and logged; this method handles its own fault tolerance.
    """
    task_id = str(uuid.uuid4())
    influxdb3_local.info(f"[{task_id}] Starting remote replication process")

    # Override args with config file if specified
    if args:
        if path := args.get("config_file_path", None):
            try:
                influxdb3_local.info(f"[{task_id}] Reading config file {path}")
                args = load_toml_config(path)
                args["use_config_file"] = True
                influxdb3_local.info(f"[{task_id}] New args loaded from config file")
            except Exception:
                influxdb3_local.error(f"[{task_id}] Failed to read config file")
                return
        else:
            args["use_config_file"] = False

    # Validate required arguments
    required_keys: list = [
        "host",
        "database",
        "unique_file_suffix",
    ]
    if not args or any(key not in args for key in required_keys):
        influxdb3_local.error(
            f"[{task_id}] Missing some of the required arguments: {', '.join(required_keys)}"
        )
        return

    try:
        # Configuration
        plugin_dir = resolve_plugin_dir()
        unique_file_suffix: str = args["unique_file_suffix"]
        queue_file = (
            plugin_dir
            / "sdr_queues"
            / f"sdr_queue_writes_{unique_file_suffix}.jsonl.gz"
        )
        influxdb3_local.info(
            f"[{task_id}] Starting data replication process, queue file path={queue_file}"
        )

        remote_host, remote_port = parse_host(args["host"], task_id)
        influxdb3_local.info(f"[{task_id}] Target host: {remote_host}:{remote_port}")

        remote_token: str = resolve_remote_token(args, task_id)
        remote_db: str = args["database"]
        verify_ssl: bool = str(args.get("verify_ssl", "true")).lower() == "true"

        if not args.get("use_config_file"):
            tables_to_replicate: list | None = (
                args.get("tables").split(" ") if args.get("tables") else None
            )
        else:
            if tables_to_replicate := args.get("tables"):
                if not isinstance(args.get("tables"), list):
                    influxdb3_local.warn(
                        f"[{task_id}] 'tables' is not a list in config file, ignoring"
                    )
                    tables_to_replicate = None

        max_size: int = int(args.get("max_size", 512))
        chunk_size: int = parse_chunk_size(args, task_id)
        excluded_fields: dict = parse_exclusions_for_data_writes(args, task_id)
        tables_renames: dict = parse_table_renames(args, task_id)
        field_renames: dict = parse_field_renames_for_data_writes(args, task_id)

        try:
            client = get_remote_client(
                influxdb3_local,
                f"{CACHE_CLIENT}_{unique_file_suffix}",
                remote_host,
                remote_port,
                remote_token,
                remote_db,
                verify_ssl,
                task_id,
            )
        except Exception as e:
            influxdb3_local.error(
                f"[{task_id}] Failed to initialize remote client to {remote_host}:{remote_port} with error: {str(e)}"
            )
            return

        lines_to_replicate: list = []

        for table_batch in table_batches:
            table_name: str = table_batch["table_name"]

            if tables_to_replicate and table_name not in tables_to_replicate:
                continue

            table_excluded_fields: list = excluded_fields.get(table_name, [])
            all_tags: list = get_tag_names(influxdb3_local, table_name, task_id)
            tables_fields_renames: dict = field_renames.get(table_name, {})
            table_name_: str = tables_renames.get(table_name, table_name)

            for row in table_batch["rows"]:
                point = row_to_point(
                    influxdb3_local,
                    table_name,
                    table_name_,
                    row,
                    task_id,
                    table_excluded_fields,
                    tables_fields_renames,
                    all_tags,
                )
                if point:
                    # Convert Point to line protocol for storage in queue
                    line = point.to_line_protocol()
                    lines_to_replicate.append({"table": table_name, "line": line})

        if lines_to_replicate:
            appended: bool = append_to_queue(queue_file, lines_to_replicate, max_size)
            if appended:
                influxdb3_local.info(
                    f"[{task_id}] Queued {len(lines_to_replicate)} lines from {', '.join(set(p['table'] for p in lines_to_replicate))}"
                )
            else:
                influxdb3_local.warn(
                    f"[{task_id}] Queue file reached the maximum size of {max_size}MB; "
                    f"skipped {len(lines_to_replicate)} new lines and flushing the existing queue"
                )

        queued_entries: list = read_queue(queue_file)
        if not queued_entries:
            influxdb3_local.info(f"[{task_id}] No data to replicate")
            return
        influxdb3_local.info(
            f"[{task_id}] Read {len(queued_entries)} entries from queue for flushing"
        )

        # Single attempt on write triggers: failed data stays queued and is
        # retried on the next WAL flush, so no in-call retries/backoff are used.
        _flush_queue_with_retries(
            influxdb3_local,
            client,
            queue_file,
            queued_entries,
            task_id,
            1,
            remote_host,
            remote_port,
            f"{CACHE_CLIENT}_{unique_file_suffix}",
            chunk_size,
        )

    except Exception as e:
        influxdb3_local.error(str(e))


def get_all_tables(influxdb3_local) -> list[str]:
    """
    Retrieves a list of all tables of type 'BASE TABLE' from cache or the current InfluxDB database.

    Args:
        influxdb3_local: InfluxDB client instance.

    Returns:
        list[str]: List of table names (e.g., ["cpu", "memory", "disk"]).
    """
    # check cache first
    measurements: list = influxdb3_local.cache.get("measurements")
    if measurements:
        return measurements

    # if not in cache, query the database
    result: list = influxdb3_local.query("SHOW TABLES")
    measurements = [
        row["table_name"] for row in result if row.get("table_type") == "BASE TABLE"
    ]

    # cache the result for 1 hour
    influxdb3_local.cache.put("measurements", measurements, 60 * 60)

    return measurements


def parse_offset(args: dict, task_id: str) -> timedelta:
    """
    Parses the 'offset' argument from args and converts it into a timedelta object.
    Used to shift the query time window backwards.

    Args:
        args (dict): Dictionary with the 'offset' key (e.g., {"offset": "1h"}).
        task_id (str): Unique identifier for the current task, used for logging.

    Returns:
        timedelta: Parsed time delta. Returns zero if no offset is provided.

    Raises:
        Exception: If the format is invalid or the unit is unsupported.

    Example input:
        args = {"offset": "15min"}  # valid units: 's', 'min', 'h', 'd', 'w'
    """
    valid_units = {
        "s": "seconds",
        "min": "minutes",
        "h": "hours",
        "d": "days",
        "w": "weeks",
    }

    offset: str | None = args.get("offset", None)

    if offset is None:
        return timedelta(0)

    match = re.fullmatch(r"(\d+)([a-zA-Z]+)", offset)
    if match:
        number, unit = match.groups()
        number = int(number)

        if number >= 1 and unit in valid_units:
            return timedelta(**{valid_units[unit]: number})

    raise Exception(f"[{task_id}] Invalid offset format: {offset}.")


def parse_window(args: dict, task_id: str) -> timedelta:
    """
    Parses the 'window' argument from args and converts it into a timedelta object.
    Represents the size of the query window.

    Args:
        args (dict): Dictionary with the 'window' key (e.g., {"window": "2h"}).
        task_id (str): Unique identifier for the current task, used for logging.

    Returns:
        timedelta: Parsed time delta for the window.

    Raises:
        Exception: If the window has an invalid format or unit.

    Example input:
        args = {"window": "3d"}  # valid units: 's', 'min', 'h', 'd', 'w'
    """
    valid_units: dict = {
        "s": "seconds",
        "min": "minutes",
        "h": "hours",
        "d": "days",
        "w": "weeks",
    }

    window: str = args["window"]
    match = re.fullmatch(r"(\d+)([a-zA-Z]+)", window)

    if match:
        number, unit = match.groups()
        number = int(number)

        if number >= 1 and unit in valid_units:
            return timedelta(**{valid_units[unit]: number})

    raise Exception(f"[{task_id}] Invalid interval format: {window}.")


def build_query(measurement: str, time_from: datetime, time_to: datetime) -> str:
    """
    Builds a SQL query to select all data from a measurement between two timestamps.

    Args:
        measurement (str): Name of the measurement/table.
        time_from (datetime): Start time (inclusive).
        time_to (datetime): End time (exclusive).

    Returns:
        str: SQL query string selecting data between time_from and time_to.

    Example output:
        SELECT * FROM "cpu" WHERE time >= '2025-05-16T12:00:00Z' AND time < '2025-05-16T13:00:00Z'
    """
    # ISO timestamps with microsecond precision to avoid window gaps/overlaps
    start_iso = time_from.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    end_iso = time_to.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    query = f"""
        SELECT
            *
        FROM
            "{measurement}"
        WHERE
            time >= '{start_iso}'
        AND 
            time < '{end_iso}'
    """
    return query


def process_scheduled_call(
    influxdb3_local, call_time: datetime, args: dict | None = None
):
    """
    Handles a scheduled data replication process from a local InfluxDB database
    to a remote InfluxDB3-compatible instance using parameters provided via `args`.

    Args:
        influxdb3_local: An object providing logging and querying capabilities to the local InfluxDB.
        call_time (datetime): The scheduled time of execution (UTC-aware will be enforced).
        args (dict | None): A dictionary of required and optional parameters, including:
            Required:
                - host (str): Remote host address, possibly with port (e.g., "example.com:8086").
                - database (str): Remote database name.
                - window (str or int): Duration of time window to pull data for (e.g., "5m", 300).
                - source_measurement (str): Measurement name in local DB to replicate from.
                - unique_file_suffix (str): Unique suffix used to construct the queue file name.
            Optional:
                - remote_token (str): Authorization token for remote InfluxDB. Falls back to the REMOTE_INFLUXDB_TOKEN env var.
                - verify_ssl (str): "true" or "false", whether to verify SSL certificates (default: "true").
                - max_retries (int): Maximum number of retry attempts when flushing queue (default handled separately).
                - max_size (int): Maximum size of queue file before flushing (default: 512).
                - queue_flush_chunk_size (int): Line-protocol entries sent per remote flush request (default: 500).
                - excluded_fields (list): List of fields and tags to exclude during replication.
                - target_table (str): Target measurement name in remote DB (defaults to `source_measurement`).
                - field_renames (dict): Mapping of field names to rename during replication.
                - offset (str or int): How far back from `call_time` the window should end (e.g., "1m", 60).
                - config_file_path (str): Path to a TOML config file to override args.

    Raises:
        No exceptions are propagated; all exceptions are caught and logged internally.

    Notes:
        - The function uses a queueing mechanism to ensure data durability in case of temporary failures.
        - The data is only flushed from the queue once a threshold is reached or retries are exhausted.
        - Queue is stored as a compressed JSONL file using the `unique_file_suffix` to differentiate tasks.
    """
    task_id: str = str(uuid.uuid4())

    # Override args with config file if specified
    if args:
        if path := args.get("config_file_path", None):
            try:
                influxdb3_local.info(f"[{task_id}] Reading config file {path}")
                args = load_toml_config(path)
                args["use_config_file"] = True
                influxdb3_local.info(f"[{task_id}] New args loaded from config file")
            except Exception:
                influxdb3_local.error(f"[{task_id}] Failed to read config file")
                return
        else:
            args["use_config_file"] = False

    # Validate required arguments
    required_keys: list = [
        "host",
        "database",
        "window",
        "source_measurement",
        "unique_file_suffix",
    ]
    if not args or any(key not in args for key in required_keys):
        influxdb3_local.error(
            f"[{task_id}] Missing some of the required arguments: {', '.join(required_keys)}"
        )
        return

    try:
        # Configuration
        plugin_dir = resolve_plugin_dir()
        unique_file_suffix: str = args["unique_file_suffix"]
        queue_file = (
            plugin_dir
            / "sdr_queues"
            / f"sdr_queue_schedule_{unique_file_suffix}.jsonl.gz"
        )
        influxdb3_local.info(
            f"[{task_id}] Starting data replication process, queue file path={queue_file}"
        )

        remote_host, remote_port = parse_host(args["host"], task_id)
        influxdb3_local.info(
            f"[{task_id}] Parsed remote host: {remote_host}, port: {remote_port}"
        )
        remote_token: str = resolve_remote_token(args, task_id)
        remote_db: str = args["database"]
        measurement: str = args["source_measurement"]
        verify_ssl: bool = str(args.get("verify_ssl", "true")).lower() == "true"
        max_retries: int = parse_max_retries(args, task_id)

        if measurement not in get_all_tables(influxdb3_local):
            influxdb3_local.error(
                f"[{task_id}] Measurement {measurement} not found in current database"
            )
            return

        max_size: int = int(args.get("max_size", 512))
        chunk_size: int = parse_chunk_size(args, task_id)
        excluded_fields: list = parse_exclusions_for_schedule(
            influxdb3_local, args, task_id
        )
        target_table: str = args.get("target_table", measurement)
        field_renames: dict = parse_field_renames_for_schedule(args, task_id)
        offset: timedelta = parse_offset(args, task_id)
        window: timedelta = parse_window(args, task_id)
        call_time_: datetime = call_time.replace(tzinfo=timezone.utc)

        time_to: datetime = call_time_ - offset
        time_from: datetime = time_to - window
        influxdb3_local.info(f"[{task_id}] Time window: from {time_from} to {time_to}")

        query: str = build_query(measurement, time_from, time_to)
        data: list = influxdb3_local.query(query)
        influxdb3_local.info(f"[{task_id}] Retrieved {len(data)} rows")

        all_tags: list = get_tag_names(influxdb3_local, measurement, task_id)

        lines_to_replicate: list = []
        for row in data:
            point = row_to_point(
                influxdb3_local,
                measurement,
                target_table,
                row,
                task_id,
                excluded_fields,
                field_renames,
                all_tags,
            )
            if point:
                # Convert Point to line protocol for storage in queue
                line = point.to_line_protocol()
                lines_to_replicate.append({"table": measurement, "line": line})

        try:
            client = get_remote_client(
                influxdb3_local,
                f"{CACHE_CLIENT}_{unique_file_suffix}",
                remote_host,
                remote_port,
                remote_token,
                remote_db,
                verify_ssl,
                task_id,
            )
        except Exception as e:
            influxdb3_local.error(
                f"[{task_id}] Failed to initialize remote client to {remote_host}:{remote_port} with error: {str(e)}"
            )
            return

        if lines_to_replicate:
            appended: bool = append_to_queue(queue_file, lines_to_replicate, max_size)
            if appended:
                influxdb3_local.info(
                    f"[{task_id}] Queued {len(lines_to_replicate)} lines from {', '.join(set(p['table'] for p in lines_to_replicate))}"
                )
            else:
                influxdb3_local.warn(
                    f"[{task_id}] Queue file reached the maximum size of {max_size}MB; "
                    f"skipped {len(lines_to_replicate)} new lines and flushing the existing queue"
                )

        queued_entries: list = read_queue(queue_file)
        if not queued_entries:
            influxdb3_local.info(f"[{task_id}] No data to replicate")
            return
        influxdb3_local.info(
            f"[{task_id}] Read {len(queued_entries)} queued entries for flushing"
        )

        _flush_queue_with_retries(
            influxdb3_local,
            client,
            queue_file,
            queued_entries,
            task_id,
            max_retries,
            remote_host,
            remote_port,
            f"{CACHE_CLIENT}_{unique_file_suffix}",
            chunk_size,
        )

    except Exception as e:
        influxdb3_local.error(str(e))
