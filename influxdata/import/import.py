"""
{
    "plugin_type": ["http"],
    "http_args_config": [
        {
            "name": "source_url",
            "example": "http://localhost:8086",
            "description": "Source InfluxDB URL (include port if non-standard).",
            "required": true
        },
        {
            "name": "influxdb_version",
            "example": 1,
            "description": "Source InfluxDB version: 1, 2, or 3.",
            "required": true
        },
        {
            "name": "source_database",
            "example": "telegraf",
            "description": "Source database name.",
            "required": true
        },
        {
            "name": "dest_database",
            "example": "imported_data",
            "description": "Destination database name.",
            "required": false
        },
        {
            "name": "start_timestamp",
            "example": "2024-01-01T00:00:00Z",
            "description": "Import start timestamp (RFC3339/Unix/date format).",
            "required": false
        },
        {
            "name": "end_timestamp",
            "example": "2024-12-31T23:59:59Z",
            "description": "Import end timestamp (RFC3339/Unix/date format).",
            "required": false
        },
        {
            "name": "query_interval_ms",
            "example": "100",
            "description": "Delay between queries in milliseconds (0 or greater, default: 100).",
            "required": false
        },
        {
            "name": "import_direction",
            "example": "oldest_first",
            "description": "Import direction: 'oldest_first' or 'newest_first' (default: 'oldest_first').",
            "required": false
        },
        {
            "name": "target_batch_size",
            "example": "2000",
            "description": "Target rows per query batch (1 or greater, default: 2000).",
            "required": false
        },
        {
            "name": "table_filter",
            "example": "cpu.mem.disk",
            "description": "Dot-separated list of specific tables to import (or all if not specified).",
            "required": false
        },
        {
            "name": "dry_run",
            "example": "false",
            "description": "Estimate the import and return a plan without writing data (default: false).",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "import_config.toml",
            "description": "TOML config file path, absolute or relative to the plugin directory. Also read from INFLUXDB3_IMPORT_CONFIG_FILE_PATH; its keys override the same keys passed inline.",
            "required": false
        }
    ],
    "http_body_config": [
        {
            "name": "source_url",
            "example": "http://localhost:8086",
            "description": "Source InfluxDB URL (include port if non-standard). Required unless set in the trigger arguments, the TOML file or the environment.",
            "required": false
        },
        {
            "name": "influxdb_version",
            "example": 1,
            "description": "Source InfluxDB version: 1, 2, or 3. Required unless set in the trigger arguments, the TOML file or the environment.",
            "required": false
        },
        {
            "name": "source_database",
            "example": "telegraf",
            "description": "Source database name. Required unless set in the trigger arguments, the TOML file or the environment.",
            "required": false
        },
        {
            "name": "dest_database",
            "example": "imported_data",
            "description": "Destination database name.",
            "required": false
        },
        {
            "name": "start_timestamp",
            "example": "2024-01-01T00:00:00Z",
            "description": "Import start timestamp (RFC3339/Unix/date format).",
            "required": false
        },
        {
            "name": "end_timestamp",
            "example": "2024-12-31T23:59:59Z",
            "description": "Import end timestamp (RFC3339/Unix/date format).",
            "required": false
        },
        {
            "name": "query_interval_ms",
            "example": "100",
            "description": "Delay between queries in milliseconds (0 or greater, default: 100).",
            "required": false
        },
        {
            "name": "import_direction",
            "example": "oldest_first",
            "description": "Import direction: 'oldest_first' or 'newest_first' (default: 'oldest_first').",
            "required": false
        },
        {
            "name": "target_batch_size",
            "example": "2000",
            "description": "Target rows per query batch (1 or greater, default: 2000).",
            "required": false
        },
        {
            "name": "table_filter",
            "example": "cpu.mem.disk",
            "description": "Dot-separated list of specific tables to import (or all if not specified).",
            "required": false
        },
        {
            "name": "dry_run",
            "example": "false",
            "description": "Estimate the import and return a plan without writing data (default: false).",
            "required": false
        }
    ],
    "http_headers_config": [
        {
            "name": "Source-Token",
            "example": "<your-source-token>",
            "description": "Authentication token for the source InfluxDB, sent as Bearer for v1 and v3 and as Token for v2. On a v1 source it is ignored when Source-Username and Source-Password are both present.",
            "required": false
        },
        {
            "name": "Source-Username",
            "example": "admin",
            "description": "Username for InfluxDB v1 basic authentication. Applies only to a v1 source, and only together with Source-Password.",
            "required": false
        },
        {
            "name": "Source-Password",
            "example": "<your-source-password>",
            "description": "Password for InfluxDB v1 basic authentication. Applies only to a v1 source, and only together with Source-Username.",
            "required": false
        }
    ]
}
"""

import base64
import json
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import requests
from influxdata_plugin_utils.config import Config, load_config
from influxdata_plugin_utils.parsing import (
    parse_bool,
    parse_delimited_list,
    parse_int,
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
from influxdata_plugin_utils.validation import Validator, validate
from influxdata_plugin_utils.write import add_field_with_type, write_data

# Per-thread HTTP session for connection pooling
_thread_state = threading.local()

# Configuration constants
MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 1
MAX_BACKOFF_SECONDS = 16
REQUEST_TIMEOUT_SECONDS = 30
STALE_IMPORT_THRESHOLD_SECONDS = 300  # 5 minutes — if last import_state update is older, import is considered stale
STORED_ERRORS = 50  # how many failed windows a table records in import_state
PROGRESS_ERRORS = 3  # how many of them an in_progress row samples, to stay small

# Timestamp offset constants (for boundary adjustments)
MICROSECOND_OFFSET = 1

SUPPORTED_VERSIONS = (1, 2, 3)
IMPORT_DIRECTIONS = ("oldest_first", "newest_first")
STOP_NOUNS = {"cancelled": "cancellation", "paused": "pause"}

ENV_PREFIX = "INFLUXDB3_IMPORT_"
HEADER_PREFIX = "X-Influxdb3-Import-"

# Credentials are sent as request headers, never through the config layers
CREDENTIAL_NAMES = ("source-token", "source-username", "source-password")

# the query string names the action and the import it acts on, not a setting
CONTROL_NAMES = ("action", "import_id")


# --- how a raw setting becomes its value, for the validators below ---


def trimmed(value) -> str:
    """A setting as trimmed text; TOML may deliver it as a number."""
    return str(value).strip()


def table_list(value) -> Optional[List[str]]:
    """
    A dot-separated table filter, or a list as TOML delivers one.

    An empty filter is no filter, and reads as None so that leaving the setting
    out and giving it an empty value end up the same.
    """
    return parse_delimited_list(value, sep=".") or None


# --- what each setting has to be ---

# An optional setting with no default stays out of the validated config, so
# ImportConfig supplies its own default.
CONFIG_VALIDATORS = [
    Validator("source_url", required=True, cast=trimmed),
    Validator("source_database", required=True, cast=trimmed),
    Validator(
        "influxdb_version", required=True, cast=parse_int, is_in=SUPPORTED_VERSIONS
    ),
    Validator("dest_database", cast=trimmed),
    Validator("start_timestamp", cast=trimmed),
    Validator("end_timestamp", cast=trimmed),
    Validator("query_interval_ms", default=100, cast=parse_int, gte=0),
    Validator(
        "import_direction",
        default="oldest_first",
        cast=trimmed,
        is_in=IMPORT_DIRECTIONS,
    ),
    Validator("target_batch_size", default=2000, cast=parse_int, gte=1),
    Validator("table_filter", cast=table_list),
    Validator("dry_run", default=False, cast=parse_bool),
    Validator("config_file_path", cast=trimmed),
]

CONNECTION_VALIDATORS = [Validator("source_url", required=True, cast=trimmed)]

SOURCE_VALIDATORS = CONNECTION_VALIDATORS + [
    Validator(
        "influxdb_version", required=True, cast=parse_int, is_in=SUPPORTED_VERSIONS
    ),
]


SETTING_NAMES = [
    name for validator in CONFIG_VALIDATORS for name in validator.names
]

# the config file path names a layer rather than setting a value
VALUE_NAMES = [name for name in SETTING_NAMES if name != "config_file_path"]

# a key outside these lists is named in the error rather than silently dropped
ARG_KEYS = KeySpec(allowlist=SETTING_NAMES, unknown="reject")

# the config file path is a trigger argument only: neither the file itself nor
# the request body may point at another one
SETTING_KEYS = KeySpec(allowlist=VALUE_NAMES, unknown="reject")


def env_spec(*names: str) -> KeySpec:
    """
    Read the named settings from ``INFLUXDB3_IMPORT_<SETTING>``.

    The prefix is stripped again, so a variable merges with the same setting
    coming from a trigger argument, the TOML file or the request body.
    """
    rename = {f"{ENV_PREFIX}{name.upper()}": name for name in names}
    return KeySpec(allowlist=tuple(rename), rename=rename)


ENV_SETTINGS = env_spec(*VALUE_NAMES)

# a second set of names for five of the settings; INFLUXDB3_IMPORT_* wins when
# a setting is given under both
LEGACY_ENV_KEYS = KeySpec(
    allowlist=[
        "IMPORT_SOURCE_URL",
        "IMPORT_SOURCE_DATABASE",
        "IMPORT_DEST_DATABASE",
        "IMPORT_START_TIMESTAMP",
        "IMPORT_END_TIMESTAMP",
    ],
    rename={
        "IMPORT_SOURCE_URL": "source_url",
        "IMPORT_SOURCE_DATABASE": "source_database",
        "IMPORT_DEST_DATABASE": "dest_database",
        "IMPORT_START_TIMESTAMP": "start_timestamp",
        "IMPORT_END_TIMESTAMP": "end_timestamp",
    },
)

CREDENTIAL_HEADERS = KeySpec(
    allowlist=CREDENTIAL_NAMES,
    rename={name: name.replace("-", "_") for name in CREDENTIAL_NAMES},
)


def header_name(name: str) -> str:
    """The header a setting is spelled as, prefixed and hyphenated."""
    return HEADER_PREFIX + name.replace("_", "-").title()


# a header outside this list is dropped rather than refused: a client sends
# headers of its own on every request, and refusing them would refuse the request
SETTING_HEADERS = KeySpec(
    allowlist=tuple(header_name(name) for name in VALUE_NAMES),
    rename={header_name(name): name for name in VALUE_NAMES},
)

# the read-only actions read a few keys of the body directly and ignore the rest
SOURCE_KEYS = KeySpec(
    allowlist=["source_url", "influxdb_version", "source_database"]
)

# an action accepts only the query parameters it reads, so an unknown one is
# named alongside what that action does accept
QUERY_ACTION_ONLY = KeySpec(allowlist=("action",), unknown="reject")
QUERY_WITH_IMPORT_ID = KeySpec(allowlist=CONTROL_NAMES, unknown="reject")
QUERY_WITH_SETTINGS = KeySpec(
    allowlist=("action",) + tuple(VALUE_NAMES), unknown="reject"
)

# resume takes no settings: it reads the configuration the import was started
# with, and the read-only actions take their source from the request body
QUERY_KEYS_BY_ACTION = {
    "start": QUERY_WITH_SETTINGS,
    "status": QUERY_WITH_IMPORT_ID,
    "pause": QUERY_WITH_IMPORT_ID,
    "resume": QUERY_WITH_IMPORT_ID,
    "cancel": QUERY_WITH_IMPORT_ID,
    "test_connection": QUERY_ACTION_ONLY,
    "databases": QUERY_ACTION_ONLY,
    "tables": QUERY_ACTION_ONLY,
}


class ImportPauseState(Enum):
    """Possible states returned from querying the import_pause_state table."""

    NOT_FOUND = "not_found"
    CANCELLED = "cancelled"
    PAUSED = "paused"
    RUNNING = "running"
    COMPLETED = "completed"


@dataclass
class ImportConfig:
    """Configuration for import plugin"""

    source_url: str
    source_database: str
    influxdb_version: int
    dest_database: Optional[str] = None
    start_timestamp: Optional[str] = None
    end_timestamp: Optional[str] = None
    query_interval_ms: int = 100
    import_direction: str = "oldest_first"
    target_batch_size: int = 2000
    table_filter: Optional[List[str]] = None
    config_file_path: Optional[str] = None
    dry_run: bool = False


# source field types (SHOW FIELD KEYS) -> line protocol field types
LINE_FIELD_TYPES = {
    "boolean": "bool",
    "integer": "int",
    "unsigned": "uint",
    "float": "float",
    "string": "string",
}


class SourceQueryError(Exception):
    """The source accepted the request but reported a failed statement."""


def load_import_settings(
    influxdb3_local,
    task_id: str,
    args: Optional[Dict[str, Any]] = None,
    request_body=None,
    request_headers=None,
    query_settings: Optional[Dict[str, Any]] = None,
) -> ImportConfig:
    """
    Load configuration from every layer a start request can carry.

    Priority: query parameters > headers > request body > config file > args >
    environment variables. Within the environment, INFLUXDB3_IMPORT_* wins when
    a setting is given under both names.
    The TOML file path is never read from the request body.

    Args:
        influxdb3_local: InfluxDB client instance.
        task_id: Identifier written into the log lines.
        args: Trigger arguments, and the TOML file they name.
        request_body: JSON body, parsed here as the other layers are.
        request_headers: Headers spelled X-Influxdb3-Import-<SETTING>.
        query_settings: Query parameters already selected by the action's spec,
            since the action and the import id come off that same parse.
    """
    args = args or {}
    # trimmed, because a trigger argument arrives exactly as it was written and
    # a stray space would only fail later, when the file is opened
    config_file_path = trimmed(
        args.get("config_file_path")
        or parse_env(env_spec("config_file_path")).get("config_file_path")
        or ""
    )

    settings: Config = load_config(
        parse_env(LEGACY_ENV_KEYS),
        parse_env(ENV_SETTINGS),
        parse_trigger_args(args, ARG_KEYS),
        parse_toml(config_file_path, SETTING_KEYS),
        parse_json_body(request_body, SETTING_KEYS),
        parse_request_headers(request_headers, SETTING_HEADERS),
        query_settings or {},
        validators=CONFIG_VALIDATORS,
    )
    if config_file_path:
        influxdb3_local.info(
            f"[{task_id}] Loaded configuration from {config_file_path}"
        )
    return ImportConfig(**settings)


def get_http_session() -> requests.Session:
    """
    Return this thread's HTTP session, creating it on first use.

    The engine runs every plugin invocation on its own thread, and a trigger
    created with --run-asynchronous runs several of them at once. A
    requests.Session is not safe to share between threads, so each one keeps its
    own; connections are still reused across the many queries of one import.
    """
    session = getattr(_thread_state, "http_session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({"Connection": "keep-alive"})
        _thread_state.http_session = session
    return session


def check_query_result(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Raise when the source reports a statement error, otherwise return the payload.

    InfluxDB answers a failed statement with HTTP 200 and the reason inside
    'results', on v1, v2 and v3 alike, so the status code says nothing. On v3
    even a malformed query comes back this way. Left unchecked, a failure is
    indistinguishable from an empty result.
    """
    statements = payload.get("results") or []
    if statements and statements[0].get("error"):
        raise SourceQueryError(f"Source query failed: {statements[0]['error']}")
    return payload


def query_source_influxdb(
    influxdb3_local,
    config: ImportConfig,
    credentials: Dict[str, Optional[str]],
    query: str,
    task_id: str,
) -> Dict[str, Any]:
    """
    Execute InfluxQL query against source InfluxDB API
    Supports InfluxDB v1 and v2 with appropriate authentication methods
    Returns parsed JSON response
    """
    session = get_http_session()

    base_url = f"{_parse_url_with_port_inference(config.source_url)}/query"

    # Build query parameters
    params = {"db": config.source_database, "q": query}

    headers = {"Content-Type": "application/vnd.influxql"}

    # Build auth headers based on version
    if config.influxdb_version == 1:
        auth_headers = _build_v1_headers(credentials)
        headers.update(auth_headers)
    elif config.influxdb_version == 2:
        auth_headers = _build_v2_headers(credentials)
        headers.update(auth_headers)
    elif config.influxdb_version == 3:
        auth_headers = _build_v3_headers(credentials)
        headers.update(auth_headers)

    retry_count = 0
    backoff = INITIAL_BACKOFF_SECONDS

    while retry_count < MAX_RETRIES:
        try:
            response = session.get(
                base_url,
                params=params,
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return check_query_result(response.json())
        except SourceQueryError as e:
            influxdb3_local.error(f"[{task_id}] {e}")
            raise
        except requests.exceptions.RequestException as e:
            retry_count += 1
            if retry_count >= MAX_RETRIES:
                influxdb3_local.error(
                    f"[{task_id}] Query failed after {MAX_RETRIES} retries: {e}"
                )
                raise
            influxdb3_local.warn(
                f"[{task_id}] Query failed (attempt {retry_count}/{MAX_RETRIES}), retrying in {backoff}s: {e}"
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
        except Exception as e:
            influxdb3_local.error(f"[{task_id}] Unexpected error querying source: {e}")
            raise

    raise Exception(f"[{task_id}] Query failed after all retries")


def get_source_measurements(
    influxdb3_local, config: ImportConfig, credentials: Dict[str, Optional[str]], task_id: str
) -> List[str]:
    """Get list of measurements (tables) from source database"""
    result = query_source_influxdb(
        influxdb3_local, config, credentials, "SHOW MEASUREMENTS", task_id
    )

    measurements = []
    if "results" in result and len(result["results"]) > 0:
        series = result["results"][0].get("series", [])
        if series and "values" in series[0]:
            measurements = [row[0] for row in series[0]["values"]]

    # Apply table filter if specified
    if config.table_filter:
        measurements = [m for m in measurements if m in config.table_filter]

    return sorted(measurements)


def get_field_keys(
    influxdb3_local, config: ImportConfig, credentials: Dict[str, Optional[str]], measurement: str, task_id: str
) -> Dict[str, str]:
    """Get field keys and their types for a measurement"""
    query = f'SHOW FIELD KEYS FROM {quote_influxql_identifier(measurement)}'
    result = query_source_influxdb(influxdb3_local, config, credentials, query, task_id)

    fields = {}
    if "results" in result and len(result["results"]) > 0:
        series = result["results"][0].get("series", [])
        if series and "values" in series[0]:
            for row in series[0]["values"]:
                field_name, field_type = row[0], row[1]
                fields[field_name] = field_type

    return fields


def get_tag_keys(
    influxdb3_local, config: ImportConfig, credentials: Dict[str, Optional[str]], measurement: str, task_id: str
) -> List[str]:
    """Get tag keys for a measurement"""
    query = f'SHOW TAG KEYS FROM {quote_influxql_identifier(measurement)}'
    result = query_source_influxdb(influxdb3_local, config, credentials, query, task_id)

    tags = []
    if "results" in result and len(result["results"]) > 0:
        series = result["results"][0].get("series", [])
        if series and "values" in series[0]:
            tags = [row[0] for row in series[0]["values"]]

    return tags


def check_tag_field_conflicts(tags: List[str], fields: Dict[str, str]) -> List[str]:
    """Identify tags that conflict with field names"""
    conflicts = []
    for tag in tags:
        if tag in fields:
            conflicts.append(tag)
    return conflicts


def count_rows_in_result(result: Dict[str, Any]) -> int:
    """
    Read a row count out of a SELECT COUNT(*) answer.

    InfluxQL counts each field separately and answers with one 'count_<field>'
    column per field, so no column holds the number of rows. A row is returned
    when any one of its fields is set, which makes the largest per-field count
    the closest lower bound; summing them would count a row once per field it
    fills.
    """
    statements = result.get("results") or []
    if not statements or not statements[0].get("series"):
        return 0
    series = statements[0]["series"][0]
    if not series.get("values"):
        return 0
    counts = [
        value
        for column, value in zip(series["columns"], series["values"][0])
        if column.startswith("count_") and value is not None
    ]
    return max(counts) if counts else 0


def estimate_import_time(
    influxdb3_local,
    config: ImportConfig,
    credentials: Dict[str, Optional[str]],
    measurements: List[str],
    start_dt: datetime,
    end_dt: datetime,
    task_id: str,
) -> Dict[str, Any]:
    """
    Estimate total import time based on data sampling
    Returns: {
        'estimated_total_rows': int,
        'estimated_duration_seconds': float,
        'estimated_duration_human': str,
        'per_table_estimates': [...]
    }
    """
    total_estimated_rows = 0
    per_table_estimates = []

    # Rough benchmark: assume we can process ~1000 rows/second
    # This is conservative and accounts for network, parsing, writing
    ROWS_PER_SECOND = 1000

    # Add overhead for each table (connection, schema checks, etc.)
    TABLE_OVERHEAD_SECONDS = 2

    for measurement in measurements:
        try:
            # Get actual data boundaries
            actual_start, actual_end = find_actual_data_boundaries(
                influxdb3_local, config, credentials, measurement, start_dt, end_dt, task_id
            )

            if not actual_start or not actual_end:
                per_table_estimates.append(
                    {
                        "measurement": measurement,
                        "estimated_rows": 0,
                        "estimated_seconds": 0,
                    }
                )
                continue

            # Sample data to estimate row count
            # Use COUNT(*) for quick estimation
            count_query = f"""
            SELECT COUNT(*) FROM {quote_influxql_identifier(measurement)}
            WHERE time >= '{actual_start.isoformat()}' AND time <= '{actual_end.isoformat()}'
            """

            result = query_source_influxdb(
                influxdb3_local, config, credentials, count_query, task_id
            )

            row_count = count_rows_in_result(result)

            # Estimate time for this table
            table_seconds = (row_count / ROWS_PER_SECOND) + TABLE_OVERHEAD_SECONDS

            per_table_estimates.append(
                {
                    "measurement": measurement,
                    "estimated_rows": row_count,
                    "estimated_seconds": table_seconds,
                }
            )

            total_estimated_rows += row_count

        except Exception as e:
            influxdb3_local.warn(
                f"[{task_id}] Could not estimate for '{measurement}': {e}"
            )
            per_table_estimates.append(
                {
                    "measurement": measurement,
                    "estimated_rows": 0,
                    "estimated_seconds": 0,
                    "error": str(e),
                }
            )

    # Calculate total duration
    total_seconds = sum(t["estimated_seconds"] for t in per_table_estimates)

    # Add query interval delays between batches
    # Rough estimate: total_rows / batch_size * (query_interval_ms / 1000)
    if total_estimated_rows > 0:
        estimated_batches = total_estimated_rows / config.target_batch_size
        delay_seconds = estimated_batches * (config.query_interval_ms / 1000.0)
        total_seconds += delay_seconds

    # Format human-readable duration
    if total_seconds < 60:
        human_duration = f"{total_seconds:.1f} seconds"
    elif total_seconds < 3600:
        minutes = total_seconds / 60
        human_duration = f"{minutes:.1f} minutes"
    elif total_seconds < 86400:
        hours = total_seconds / 3600
        human_duration = f"{hours:.1f} hours"
    else:
        days = total_seconds / 86400
        human_duration = f"{days:.1f} days"

    return {
        "estimated_total_rows": total_estimated_rows,
        "estimated_duration_seconds": round(total_seconds, 1),
        "estimated_duration_human": human_duration,
        "per_table_estimates": per_table_estimates,
    }


def perform_preflight_checks(
    influxdb3_local, config: ImportConfig, credentials: Dict[str, Optional[str]], task_id: str
) -> Tuple[bool, List[str], Dict[str, Any]]:
    """
    Perform pre-flight validation checks
    Returns: (success, error_messages, metadata)
    """
    errors = []
    metadata = {
        "measurements": [],
        "total_tables": 0,
        "schema_issues": [],
        "time_estimate": None,
    }

    influxdb3_local.info(f"[{task_id}] Starting pre-flight checks...")

    # Check source connectivity
    try:
        measurements = get_source_measurements(influxdb3_local, config, credentials, task_id)
        metadata["measurements"] = measurements
        metadata["total_tables"] = len(measurements)
        influxdb3_local.info(
            f"[{task_id}] Found {len(measurements)} measurements in source database"
        )
    except Exception as e:
        errors.append(f"Failed to connect to source database: {e}")
        return False, errors, metadata

    # If we have errors, fail
    if errors:
        return False, errors, metadata

    influxdb3_local.info(f"[{task_id}] Pre-flight checks completed successfully")
    return True, [], metadata


def as_utc(moment: datetime) -> datetime:
    """Read a moment that names no zone as UTC."""
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def quote_influxql_identifier(name) -> str:
    """
    Quote a measurement name for InfluxQL.

    Named apart from the `quote_identifier` other plugins define because
    InfluxQL escapes with a backslash inside a quoted identifier, where SQL
    doubles the quote.
    """
    escaped = str(name).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def escape_string_literal(value) -> str:
    """Escape a value for a single-quoted SQL literal, as other plugins do."""
    return str(value).replace("'", "''")


def parse_timestamp(ts_str: str) -> datetime:
    """
    Parse various timestamp formats to datetime

    A value that names no zone is read as UTC: InfluxQL refuses a bound without
    an offset, so a naive result would fail the query rather than the parse.
    """
    # Try RFC3339 format first
    try:
        return as_utc(datetime.fromisoformat(ts_str.replace("Z", "+00:00")))
    except:
        pass

    # Try Unix timestamp (seconds)
    try:
        return datetime.fromtimestamp(float(ts_str), tz=timezone.utc)
    except:
        pass

    # Try Unix timestamp (nanoseconds)
    try:
        return datetime.fromtimestamp(float(ts_str) / 1e9, tz=timezone.utc)
    except:
        pass

    # Try common date formats
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
    ]

    for fmt in formats:
        try:
            return as_utc(datetime.strptime(ts_str, fmt))
        except:
            continue

    raise ValueError(f"Unable to parse timestamp: {ts_str}")


def find_actual_data_boundaries(
    influxdb3_local,
    config: ImportConfig,
    credentials: Dict[str, Optional[str]],
    measurement: str,
    user_start: Optional[Any],
    user_end: Optional[Any],
    task_id: str,
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Find actual data boundaries within user-specified range.
    Returns: (actual_start, actual_end) or (None, None) if no data.

    A bound is a datetime, or an RFC3339 string when the caller needs
    nanosecond precision that a datetime cannot carry.

    Behavior:
    - If both user_start and user_end are None → use the entire dataset.
    - If only user_start is provided → find newest record from that time to the end.
    - If only user_end is provided → find oldest record from the beginning up to that time.
    - If both provided → restrict queries within that range.
    """
    start_bound = None if user_start is None else influxql_time_literal(user_start)
    end_bound = None if user_end is None else influxql_time_literal(user_end)

    # --- Build start query ---
    if user_start is None and user_end is None:
        start_query = f'SELECT * FROM {quote_influxql_identifier(measurement)} ORDER BY time ASC LIMIT 1'
    elif user_start is None:
        start_query = f"""
        SELECT * FROM {quote_influxql_identifier(measurement)}
        WHERE time <= {end_bound}
        ORDER BY time ASC LIMIT 1
        """
    elif user_end is None:
        start_query = f"""
        SELECT * FROM {quote_influxql_identifier(measurement)}
        WHERE time >= {start_bound}
        ORDER BY time ASC LIMIT 1
        """
    else:
        start_query = f"""
        SELECT * FROM {quote_influxql_identifier(measurement)}
        WHERE time >= {start_bound} AND time <= {end_bound}
        ORDER BY time ASC LIMIT 1
        """

    # --- Build end query ---
    if user_start is None and user_end is None:
        end_query = f'SELECT * FROM {quote_influxql_identifier(measurement)} ORDER BY time DESC LIMIT 1'
    elif user_start is None:
        end_query = f"""
        SELECT * FROM {quote_influxql_identifier(measurement)}
        WHERE time <= {end_bound}
        ORDER BY time DESC LIMIT 1
        """
    elif user_end is None:
        end_query = f"""
        SELECT * FROM {quote_influxql_identifier(measurement)}
        WHERE time >= {start_bound}
        ORDER BY time DESC LIMIT 1
        """
    else:
        end_query = f"""
        SELECT * FROM {quote_influxql_identifier(measurement)}
        WHERE time >= {start_bound} AND time <= {end_bound}
        ORDER BY time DESC LIMIT 1
        """

    actual_start = None
    actual_end = None

    # A failure here is not an empty range: letting it through would mark the
    # table completed with nothing imported
    # --- Query for actual_start ---
    result = query_source_influxdb(influxdb3_local, config, credentials, start_query, task_id)
    if result.get("results") and result["results"][0].get("series"):
        series = result["results"][0]["series"][0]
        if "values" in series and series["values"]:
            time_col_idx = series["columns"].index("time")
            actual_start = datetime.fromisoformat(
                series["values"][0][time_col_idx].replace("Z", "+00:00")
            )

    # --- Query for actual_end ---
    result = query_source_influxdb(influxdb3_local, config, credentials, end_query, task_id)
    if result.get("results") and result["results"][0].get("series"):
        series = result["results"][0]["series"][0]
        if "values" in series and series["values"]:
            time_col_idx = series["columns"].index("time")
            actual_end = datetime.fromisoformat(
                series["values"][0][time_col_idx].replace("Z", "+00:00")
            )
            # Add 1 microsecond to make the upper boundary inclusive
            actual_end = actual_end + timedelta(microseconds=MICROSECOND_OFFSET)

    return actual_start, actual_end


def sample_data_density(
    influxdb3_local,
    config: ImportConfig,
    credentials: Dict[str, Optional[str]],
    measurement: str,
    start: datetime,
    end: datetime,
    task_id: str,
) -> int:
    """
    Sample data to determine optimal time window for target batch size
    Returns: optimal window size in seconds
    """
    # Calculate total time range
    total_duration = (end - start).total_seconds()

    # Sample intervals to test: 1 hour, 10 hours, 1 day, 5 days
    # Only test intervals that fit within the time range
    test_intervals = [
        ("1h", 3600),
        ("10h", 36000),
        ("1d", 86400),
        ("5d", 432000),
    ]

    # Filter intervals that are smaller than total duration
    viable_intervals = [
        (name, secs) for name, secs in test_intervals if secs <= total_duration
    ]

    if not viable_intervals:
        # If even 1 second is too long, use the entire duration
        influxdb3_local.warn(
            f"[{task_id}] Time range too short for sampling, using entire duration: {total_duration:.1f}s"
        )
        return max(1, int(total_duration))

    samples = []
    time_delta = end - start

    for interval_name, interval_seconds in viable_intervals:
        # Take 3 samples at different points in time range
        sample_points = [start, start + time_delta / 3, start + (time_delta * 2) / 3]

        for sample_start in sample_points:
            sample_end = sample_start + timedelta(seconds=interval_seconds)
            if sample_end > end:
                influxdb3_local.info(
                    f"[{task_id}] Skipping sample at {sample_start.isoformat()} "
                    f"(would exceed end time)"
                )
                continue

            query = f"""
            SELECT COUNT(*) FROM {quote_influxql_identifier(measurement)}
            WHERE time >= '{sample_start.isoformat()}'
            AND time < '{sample_end.isoformat()}'
            """

            try:
                result = query_source_influxdb(influxdb3_local, config, credentials, query, task_id)
                count = count_rows_in_result(result)
                if count > 0:
                    # Calculate rows per second
                    rows_per_second = count / interval_seconds
                    samples.append(rows_per_second)
                    influxdb3_local.info(
                        f"[{task_id}] Sample {interval_name}: {count} rows, "
                        f"{rows_per_second:.2f} rows/sec"
                    )
            except Exception as e:
                influxdb3_local.warn(f"[{task_id}] Error sampling {interval_name}: {e}")

    if not samples:
        # Default to 1 hour if no samples
        influxdb3_local.warn(
            f"[{task_id}] No samples obtained for '{measurement}', defaulting to 1-hour windows"
        )
        return 3600

    # Calculate average rows per second
    avg_rows_per_second = sum(samples) / len(samples)

    if avg_rows_per_second == 0:
        influxdb3_local.warn(
            f"[{task_id}] Average rows per second is 0, defaulting to 1-day window"
        )
        return 86400

    # Calculate window size to get target batch size
    optimal_window = int(config.target_batch_size / avg_rows_per_second)

    # Clamp between 1 second and 1 month (30.5 days)
    optimal_window = max(1, min(optimal_window, int(86400 * 30.5)))

    influxdb3_local.info(
        f"[{task_id}] Measurement '{measurement}': {avg_rows_per_second:.2f} rows/sec "
        f"({len(samples)} samples), optimal window: {optimal_window}s"
    )

    return optimal_window


def check_influx_type_to_python_type(influx_type: str, value) -> bool:
    if influx_type == "string":
        return isinstance(value, str)
    elif influx_type == "boolean":
        return isinstance(value, bool)
    elif influx_type == "integer":
        return isinstance(value, int)
    elif influx_type == "unsigned":
        return isinstance(value, int)
    elif influx_type == "float":
        if not isinstance(value, float) and isinstance(value, int):
            return True
        return isinstance(value, float)
    else:
        return False


def get_actual_influx_type(value) -> str:
    """
    Determine actual InfluxDB type from Python value
    Returns: 'boolean', 'integer', 'float', or 'string'
    """
    # Check bool first because bool is a subclass of int in Python
    if isinstance(value, bool):
        return "boolean"
    elif isinstance(value, int):
        return "integer"
    elif isinstance(value, float):
        return "float"
    else:
        return "string"


def sanitize_field_name(field_name: str) -> str:
    """
    Sanitize field name to be compatible with InfluxDB v3
    InfluxDB v3 does not allow spaces in field names
    Replace spaces with underscores
    """
    # Replace spaces with underscores
    sanitized = field_name.replace(" ", "_")
    return sanitized


def parse_timestamp_to_nanoseconds(timestamp) -> int:
    """
    Parse timestamp to nanoseconds with full precision support.

    Handles multiple timestamp formats:
    - RFC3339 strings with nanosecond precision (e.g., "2023-01-01T12:00:00.123456789Z")
    - RFC3339 strings with timezone offsets (e.g., "2023-01-01T12:00:00.123456789+05:00")
    - RFC3339 strings without fractional seconds
    - Integer timestamps (assumed to be nanoseconds)
    - Float timestamps (assumed to be seconds with fractional part)

    Python's datetime only supports microsecond precision (6 digits), so for nanosecond
    precision we manually extract the extra 3 digits and combine them using integer arithmetic.

    Args:
        timestamp: Timestamp in one of the supported formats

    Returns:
        int: Timestamp in nanoseconds since epoch
    """
    if isinstance(timestamp, str):
        # Parse timestamp preserving nanosecond precision
        # Python datetime only supports microseconds, so we need to extract nanoseconds manually
        timestamp_str = timestamp.replace("Z", "+00:00")

        # Check if timestamp has fractional seconds with nanosecond precision
        if "." in timestamp_str:
            try:
                # Split into datetime part and fractional seconds (split only on first '.')
                parts = timestamp_str.split(".", 1)
                datetime_part = parts[0]

                # Extract fractional seconds and timezone
                fractional_part = parts[1]

                # Remove timezone info from fractional part
                # Handle both +HH:MM and -HH:MM timezones
                tz_str = ""
                if "+" in fractional_part:
                    fractional_seconds, tz_part = fractional_part.split("+", 1)
                    tz_str = "+" + tz_part
                elif fractional_part.count("-") > 0:
                    # Be careful: datetime might have dashes in date part
                    # Timezone offset appears at the end after fractional seconds
                    # Example: 123456789-05:00
                    parts_tz = fractional_part.rsplit("-", 1)
                    if len(parts_tz) == 2 and ":" in parts_tz[1]:
                        fractional_seconds = parts_tz[0]
                        tz_str = "-" + parts_tz[1]
                    else:
                        fractional_seconds = fractional_part
                else:
                    fractional_seconds = fractional_part

                # Parse datetime with microsecond precision (first 6 digits of fractional seconds)
                # Pad or truncate to exactly 6 digits
                microseconds_str = fractional_seconds[:6].ljust(6, "0")
                dt = datetime.fromisoformat(
                    f"{datetime_part}.{microseconds_str}{tz_str}"
                )

                # Extract nanoseconds (digits 7-9 of fractional seconds, or 0 if not present)
                # Only take up to 9 digits total (nanosecond precision)
                if len(fractional_seconds) > 6:
                    # Get digits 7-9 and pad with zeros if needed
                    nanoseconds_str = fractional_seconds[6:9].ljust(3, "0")
                    extra_nanoseconds = int(nanoseconds_str)
                else:
                    extra_nanoseconds = 0

                # Calculate timestamp in nanoseconds using integer arithmetic
                epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
                delta = dt - epoch
                timestamp_ns = (delta.days * 86400 * 1_000_000_000 +
                               delta.seconds * 1_000_000_000 +
                               delta.microseconds * 1000 +
                               extra_nanoseconds)
            except Exception:
                # Fallback to simple parsing if nanosecond extraction fails
                dt = datetime.fromisoformat(timestamp_str)
                epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
                delta = dt - epoch
                timestamp_ns = delta.days * 86400 * 1_000_000_000 + delta.seconds * 1_000_000_000 + delta.microseconds * 1000
        else:
            # No fractional seconds, parse as-is
            dt = datetime.fromisoformat(timestamp_str)
            epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
            delta = dt - epoch
            timestamp_ns = delta.days * 86400 * 1_000_000_000 + delta.seconds * 1_000_000_000 + delta.microseconds * 1000
    elif isinstance(timestamp, int):
        # Already in nanoseconds (or assume it is)
        timestamp_ns = timestamp
    else:
        # Float or other numeric type - assume seconds with fractional part
        timestamp_ns = int(timestamp * 1e9)

    return timestamp_ns


def format_nanoseconds_iso(timestamp_ns) -> str:
    """
    Render integer nanoseconds as an RFC3339 timestamp, keeping all nine digits.

    Built from whole seconds, since a datetime holds no more than microseconds.
    InfluxDB v1, v2 and v3 all accept a literal of this shape in a WHERE clause.
    """
    seconds, nanoseconds = divmod(int(timestamp_ns), 1_000_000_000)
    whole_seconds = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return f"{whole_seconds.strftime('%Y-%m-%dT%H:%M:%S')}.{nanoseconds:09d}+00:00"


def influxql_time_literal(value) -> str:
    """
    Quote a time bound for an InfluxQL WHERE clause.

    A string is already an RFC3339 literal and is used as written, so a
    nanosecond-precision bound survives; a datetime carries microseconds at most.
    """
    return f"'{value if isinstance(value, str) else value.isoformat()}'"


def write_field_to_builder(builder, field_name: str, value, field_type: str) -> bool:
    """
    Write a field to LineBuilder with the specified type
    Returns True if field was written successfully, False otherwise
    """
    try:
        # Sanitize field name to ensure compatibility with InfluxDB v3
        add_field_with_type(
            builder,
            sanitize_field_name(field_name),
            value,
            LINE_FIELD_TYPES[field_type],
        )
        return True
    except (ValueError, TypeError, KeyError):
        return False


def analyze_column_schema(
    columns: List[str],
    values: List[List],
    tag_keys: List[str],
    field_types: Dict[str, str],
    tag_renames: Dict[str, str],
) -> Tuple[Dict[int, Tuple[str, str]], Dict[int, Tuple[str, str]]]:
    """
    Analyze columns to determine which are tags and which are fields

    Returns:
        Tuple of (tag_columns, field_columns)
        - tag_columns: {column_index: (original_tag_name, renamed_tag_name)}
        - field_columns: {column_index: (column_name, field_type)}
    """
    tag_columns = {}
    field_columns = {}

    for i, col in enumerate(columns):
        if col == "time":
            continue

        # Check if column is a tag (appears in tag_keys)
        is_tag = False
        original_tag_name = None

        # Direct tag match (no conflict with field)
        if col in tag_keys and col not in field_types:
            is_tag = True
            original_tag_name = col
        elif col in tag_keys and col in field_types:
            if f"{col}_1" not in columns:
                field_type = field_types[col]
                if check_influx_type_to_python_type(field_type, values[0][i]):
                    is_tag = False
                else:
                    is_tag = True
                    original_tag_name = col

        # Renamed tag due to conflict (e.g., "room_1" for conflicting tag "room")
        elif col.endswith("_1"):
            potential_tag_name = col[:-2]
            if potential_tag_name in tag_keys and potential_tag_name in field_types:
                is_tag = True
                original_tag_name = potential_tag_name

        if is_tag and original_tag_name:
            renamed_tag_name = tag_renames.get(original_tag_name, original_tag_name)
            tag_columns[i] = (original_tag_name, renamed_tag_name)

        # Check if column is a field
        # Skip if it's a renamed tag column (e.g., "room_1")
        if col.endswith("_1"):
            potential_tag_name = col[:-2]
            if potential_tag_name in tag_keys and potential_tag_name in field_types:
                # This is a renamed tag, not a real field
                continue

        # Determine field type
        field_type = field_types.get(col)

        # For conflicting columns (both tag and field), only add as field if it's in field_types
        if col in tag_keys and col in field_types:
            if f"{col}_1" in columns:
                # This is a conflicting column, add as field
                field_columns[i] = (col, field_type)
        elif col not in tag_keys and field_type is not None:
            # Regular field (not a tag)
            field_columns[i] = (col, field_type)

    return tag_columns, field_columns


def build_line_protocol_row(
    influxdb3_local,
    measurement: str,
    row: List,
    time_idx: int,
    tags_dict: Dict[str, Any],
    tag_columns: Dict[int, Tuple[str, str]],
    field_columns: Dict[int, Tuple[str, str]],
    tag_renames: Dict[str, str],
    task_id: str,
) -> Optional[LineBuilder]:
    """
    Build a single LineBuilder from a row of data

    Returns:
        LineBuilder if successful, None if row should be skipped
    """
    builder = LineBuilder(measurement)
    timestamp = row[time_idx]

    # Add tags from tags_dict (these are GROUP BY tags in the query result)
    for tag_key, tag_value in tags_dict.items():
        renamed_key = tag_renames.get(tag_key, tag_key)
        builder.tag(renamed_key, str(tag_value))

    # Add tags from columns
    for i, (original_tag_name, renamed_tag_name) in tag_columns.items():
        value = row[i]
        if value is not None:
            builder.tag(renamed_tag_name, str(value))

    # Add fields
    has_fields = False
    for i, (col, field_type) in field_columns.items():
        value = row[i]
        if value is None:
            continue

        # Check if value type matches expected field type
        type_matches = check_influx_type_to_python_type(field_type, value)

        if not type_matches:
            # Type mismatch: use actual type and create field with suffix
            actual_type = get_actual_influx_type(value)
            field_name = f"{col}_{actual_type}"

            influxdb3_local.warn(
                f"[{task_id}] Type mismatch for '{col}': expected {field_type}, got {actual_type}. "
                f"Creating field '{field_name}'"
            )
        else:
            # Type matches: use original field name and type
            field_name = col
            actual_type = field_type

        # Write field using helper function
        if write_field_to_builder(builder, field_name, value, actual_type):
            has_fields = True
        else:
            influxdb3_local.error(
                f"[{task_id}] Failed to write field '{field_name}' (type {actual_type}), skipping"
            )

    # Skip if no fields
    if not has_fields:
        return None

    # Convert timestamp to nanoseconds using the dedicated parsing function
    timestamp_ns = parse_timestamp_to_nanoseconds(timestamp)
    builder.time_ns(timestamp_ns)
    return builder


def convert_influxql_to_line_protocol(
    influxdb3_local,
    measurement: str,
    series_data: Dict[str, Any],
    tag_keys: List[str],
    field_types: Dict[str, str],
    task_id: str,
    tag_renames: Dict[str, str] | None = None,
) -> List[LineBuilder]:
    """
    Convert InfluxQL query result to LineBuilder objects

    Args:
        influxdb3_local: InfluxDB3Local instance
        measurement: Measurement name
        series_data: Query result series data
        tag_keys: List of tag keys from source (from SHOW TAG KEYS)
        field_types: Dict mapping field names to their types (from SHOW FIELD KEYS)
        tag_renames: Optional dict to rename conflicting tags
        task_id: Task ID for logging

    Returns:
        List of LineBuilder objects ready for writing
    """
    if tag_renames is None:
        tag_renames = {}

    builders = []

    columns = series_data.get("columns", [])
    values = series_data.get("values", [])
    tags_dict = series_data.get("tags", {})

    # Early return if no data
    if not values or len(values) == 0:
        return []

    # Analyze column schema
    tag_columns, field_columns = analyze_column_schema(
        columns, values, tag_keys, field_types, tag_renames
    )

    # Find time column index
    time_idx = columns.index("time") if "time" in columns else 0

    # Process each row
    skipped = 0
    for row in values:
        try:
            builder = build_line_protocol_row(
                influxdb3_local,
                measurement,
                row,
                time_idx,
                tags_dict,
                tag_columns,
                field_columns,
                tag_renames,
                task_id,
            )
            if builder:
                builders.append(builder)
        except Exception as e:
            skipped += 1
            influxdb3_local.warn(
                f"[{task_id}] Skipping invalid row in '{measurement}': {e}"
            )

    if skipped > 0:
        influxdb3_local.warn(
            f"[{task_id}] Skipped {skipped} invalid rows in '{measurement}'"
        )

    return builders


def write_to_destination(
    influxdb3_local, database: str, line_builders: List[LineBuilder], task_id: str
) -> Tuple[bool, Optional[str]]:
    """
    Write LineBuilder objects to destination database
    Returns: (success, error_message)
    """
    if not line_builders:
        return True, None

    try:
        write_data(
            influxdb3_local,
            line_builders,
            retries=1,
            no_sync=True,
            database=database or None,
        )
        return True, None
    except Exception as e:
        error_msg = str(e)
        influxdb3_local.error(f"[{task_id}] Write failed: {error_msg}")
        return False, error_msg


def save_import_config(
    influxdb3_local, import_id: str, config: ImportConfig, task_id: str
) -> None:
    """
    Save import configuration to database for later resumption
    Token, username and password are not saved for security reasons and must be provided when resuming
    """
    try:
        # Convert table_filter list to comma-separated string
        table_filter_str = ".".join(config.table_filter) if config.table_filter else ""

        # Build LineBuilder for config storage
        builder = LineBuilder("import_config")
        builder.tag("import_id", import_id)
        builder.string_field("source_url", config.source_url)
        builder.string_field("source_database", config.source_database)
        builder.string_field(
            "dest_database", config.dest_database if config.dest_database else ""
        )
        builder.int64_field("influxdb_version", config.influxdb_version)
        builder.string_field(
            "start_timestamp", config.start_timestamp if config.start_timestamp else ""
        )
        builder.string_field(
            "end_timestamp", config.end_timestamp if config.end_timestamp else ""
        )
        builder.int64_field("query_interval_ms", config.query_interval_ms)
        builder.string_field("import_direction", config.import_direction)
        builder.int64_field("target_batch_size", config.target_batch_size)
        builder.string_field("table_filter", table_filter_str)
        builder.time_ns(int(time.time() * 1_000_000_000))

        influxdb3_local.write_sync(builder, no_sync=False)
        influxdb3_local.info(f"[{task_id}] Saved import config for {import_id}")
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to save import config: {e}")
        raise


def load_import_config(
    influxdb3_local,
    import_id: str,
    task_id: str,
) -> Optional[ImportConfig]:
    """
    Load import configuration from database
    Returns ImportConfig or None if not found
    """
    try:
        query = f"""
        SELECT *
        FROM import_config
        WHERE import_id = '{escape_string_literal(import_id)}'
        ORDER BY time DESC
        LIMIT 1
        """
        result = influxdb3_local.query(query)

        if not result or len(result) == 0:
            influxdb3_local.warn(
                f"[{task_id}] No saved config found for import {import_id}"
            )
            return None

        row = result[0]

        # Convert table_filter from dot-separated string back to list
        table_filter_str = row.get("table_filter", "")
        table_filter = (
            [t.strip() for t in table_filter_str.split(".") if t.strip()]
            if table_filter_str
            else None
        )

        # Reconstruct ImportConfig from saved data
        config = ImportConfig(
            source_url=row.get("source_url"),
            source_database=row.get("source_database"),
            dest_database=row.get("dest_database"),
            influxdb_version=int(row.get("influxdb_version", 1)),
            start_timestamp=row.get("start_timestamp"),
            end_timestamp=row.get("end_timestamp"),
            query_interval_ms=int(row.get("query_interval_ms", 100)),
            import_direction=row.get("import_direction", "oldest_first"),
            target_batch_size=int(row.get("target_batch_size", 2000)),
            table_filter=table_filter,
        )

        influxdb3_local.info(
            f"[{task_id}] Loaded saved config for import {import_id}"
        )
        return config

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to load import config: {e}")
        return None


def errors_as_json(
    errors: Optional[List[Dict[str, Any]]] = None,
    failed_windows: Optional[int] = None,
    limit: int = STORED_ERRORS,
) -> str:
    """
    Render the windows a table failed to write, for its import_state row.

    Only the first `limit` of them are kept. A table that fails on every window
    repeats one reason, and the whole list would grow past what a single point
    can carry; failed_windows still gives the true scale. A table that failed
    nothing records an empty list, so the column is always there.

    Args:
        failed_windows: The true count, which a resume carries over and which is
            therefore larger than the sample. Defaults to the length of errors.
    """
    failed = errors or []
    return json.dumps(
        {
            "failed_windows": len(failed) if failed_windows is None else failed_windows,
            "errors": failed[:limit],
        }
    )


def write_import_state(
    influxdb3_local,
    import_id: str,
    table_name: str,
    status: str,
    rows_imported: int,
    task_id: str,
    paused_at_time: Optional[str] = None,
    no_sync: bool = False,
    errors: Optional[List[Dict[str, Any]]] = None,
    failed_windows: Optional[int] = None,
    error_limit: int = STORED_ERRORS,
) -> None:
    """
    Write import state to tracking table using LineBuilder

    Args:
        paused_at_time: ISO timestamp of data time where import was paused (only for 'paused' status)
        no_sync: If True, don't wait for WAL flush (faster but data may not be immediately queryable)
        errors: The windows this table failed to write, if any
        failed_windows: How many failed in total, when that is more than the sample
        error_limit: How many of them this row records
    """
    try:
        # Build LineBuilder for state tracking
        builder = LineBuilder("import_state")
        builder.tag("import_id", import_id)
        builder.tag("table_name", table_name)
        builder.string_field("status", status)
        builder.int64_field("rows_imported", rows_imported)
        builder.string_field(
            "errors", errors_as_json(errors, failed_windows, error_limit)
        )

        # Save paused_at_time if provided (for resume functionality)
        if paused_at_time:
            builder.string_field("paused_at_time", paused_at_time)
        else:
            builder.string_field("paused_at_time", "")

        builder.time_ns(int(time.time() * 1_000_000_000))

        influxdb3_local.write_sync(builder, no_sync=no_sync)
        influxdb3_local.info(
            f"[{task_id}] Wrote import state for {import_id} for table {table_name}"
        )
    except Exception as e:
        influxdb3_local.warn(f"[{task_id}] Failed to write import state: {e}")



def prepare_table_import(
    influxdb3_local,
    config: ImportConfig,
    credentials: Dict[str, Optional[str]],
    measurement: str,
    start_time: Optional[Any],
    end_time: Optional[Any],
    task_id: str,
) -> Tuple[
    Optional[datetime], Optional[datetime], int, Dict[str, str], List[str], List[str]
]:
    """
    Everything a table needs before its first window: the range it holds, the
    window size to walk it with, and the schema the rows are built against.

    Returns (actual_start, actual_end, window_seconds, fields, tags, conflicts);
    actual_start is None when the range holds no data.
    """
    actual_start, actual_end = find_actual_data_boundaries(
        influxdb3_local, config, credentials, measurement, start_time, end_time, task_id
    )
    if not actual_start or not actual_end:
        return None, None, 0, {}, [], []

    influxdb3_local.info(
        f"[{task_id}] Actual data range for '{measurement}': {actual_start} to {actual_end}"
    )

    window_seconds = sample_data_density(
        influxdb3_local, config, credentials, measurement, actual_start, actual_end, task_id
    )

    fields = get_field_keys(influxdb3_local, config, credentials, measurement, task_id)
    tags = get_tag_keys(influxdb3_local, config, credentials, measurement, task_id)
    conflicts = check_tag_field_conflicts(tags, fields)
    return actual_start, actual_end, window_seconds, fields, tags, conflicts


def checkpoint_of(frontier_ns: Optional[int]) -> str:
    """
    The checkpoint stored for a table, or empty when it has imported nothing.

    An empty checkpoint asks a resume to start the table over, which is what a
    table with no written rows needs.
    """
    return "" if frontier_ns is None else format_nanoseconds_iso(frontier_ns)


def frontier_of_series(series: Dict[str, Any], direction: int) -> Optional[int]:
    """
    The timestamp a queried window reaches in the direction of the import.

    The newest row for oldest_first, the oldest row for newest_first: the point
    the table is imported up to once the window is written.
    """
    columns = series.get("columns", [])
    values = series.get("values", [])
    if "time" not in columns or not values:
        return None
    time_index = columns.index("time")
    times = [row[time_index] for row in values if row[time_index] is not None]
    if not times:
        return None
    furthest = min if direction < 0 else max
    return furthest(parse_timestamp_to_nanoseconds(value) for value in times)


def import_table(
    influxdb3_local,
    config: ImportConfig,
    credentials: Dict[str, Optional[str]],
    import_id: str,
    measurement: str,
    start_time: Optional[Any],
    end_time: Optional[Any],
    task_id: str,
    metadata: Optional[Dict[str, Any]] = None,
    rows_already_imported: int = 0,
    imported_up_to_ns: Optional[int] = None,
    windows_already_failed: int = 0,
    errors_already_recorded: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Import a single table from source to destination
    Returns import statistics for this table

    Args:
        start_time: Lower bound, a datetime or an RFC3339 string when resuming
            from a nanosecond checkpoint
        end_time: Upper bound, same forms as start_time
        metadata: Optional metadata dict to update with schema issues
        rows_already_imported: What an earlier attempt at this table wrote, so
            the count it reports stays the table's total
        imported_up_to_ns: The checkpoint that attempt reached, so a stop before
            the first window of this attempt keeps the position
        windows_already_failed: How many windows it failed, which is more than
            errors_already_recorded once the sample is full
        errors_already_recorded: The sample of those failures kept in import_state
    """
    influxdb3_local.info(f"[{task_id}] Starting import for table: {measurement}")

    # carried across a resume so the table keeps its running total, its position
    # and the failures of earlier attempts
    rows_imported = rows_already_imported
    frontier_ns: Optional[int] = imported_up_to_ns
    failed_windows = windows_already_failed
    errors = list(errors_already_recorded or [])

    try:
        actual_start, actual_end, optimal_window_seconds, fields, tags, conflicts = (
            prepare_table_import(
                influxdb3_local,
                config,
                credentials,
                measurement,
                start_time,
                end_time,
                task_id,
            )
        )
    except Exception as e:
        # Pause rather than stay in 'pending', which a resume would not see,
        # and keep whatever an earlier attempt reached
        influxdb3_local.error(
            f"[{task_id}] Failed to prepare import of '{measurement}': {e}"
        )
        write_import_state(
            influxdb3_local,
            import_id,
            measurement,
            "paused",
            rows_imported,
            task_id,
            checkpoint_of(frontier_ns),
            no_sync=True,
            errors=errors,
            failed_windows=failed_windows,
        )
        raise

    if not actual_start or not actual_end:
        influxdb3_local.info(
            f"[{task_id}] No data found in specified range for '{measurement}'"
        )
        write_import_state(
            influxdb3_local,
            import_id,
            measurement,
            "completed",
            rows_imported,
            task_id,
            no_sync=True,
            errors=errors,
            failed_windows=failed_windows,
        )
        return {
            "measurement": measurement,
            "status": "completed",
            "rows_imported": rows_imported,
            "errors": errors,
            "failed_windows": failed_windows,
        }

    # Add schema issues to metadata if conflicts found
    if conflicts and metadata is not None:
        metadata["schema_issues"].append(
            {
                "measurement": measurement,
                "type": "tag_field_conflict",
                "conflicts": conflicts,
            }
        )
        influxdb3_local.warn(
            f"[{task_id}] Measurement '{measurement}' has tag/field conflicts: {conflicts}. "
            "Will rename tags with '_tag' suffix."
        )

    # Create tag rename map
    tag_renames = {conflict: f"{conflict}_tag" for conflict in conflicts}

    # Initialize tracking
    current_time = (
        actual_start if config.import_direction == "oldest_first" else actual_end
    )
    direction = 1 if config.import_direction == "oldest_first" else -1
    first_window = True

    # Import loop
    while True:
        # Check for pause/cancel state
        pause_state = get_import_pause_state(
            influxdb3_local, import_id, task_id
        )

        if pause_state == ImportPauseState.CANCELLED:
            influxdb3_local.info(
                f"[{task_id}] Import cancelled by user for '{measurement}'"
            )
            # Write cancelled state for this table
            write_import_state(
                influxdb3_local,
                import_id,
                measurement,
                "cancelled",
                rows_imported,
                task_id,
                no_sync=True,
                errors=errors,
                failed_windows=failed_windows,
            )
            # Return immediately with cancelled status
            return {
                "measurement": measurement,
                "status": "cancelled",
                "rows_imported": rows_imported,
                "errors": errors,
                "failed_windows": failed_windows,
                "cancelled_at_time": current_time.isoformat(),
            }
        elif pause_state == ImportPauseState.PAUSED:
            influxdb3_local.info(
                f"[{task_id}] Import paused by user for '{measurement}'"
            )

            paused_at_time = checkpoint_of(frontier_ns)

            write_import_state(
                influxdb3_local,
                import_id,
                measurement,
                "paused",
                rows_imported,
                task_id,
                paused_at_time,  # Save data time where we paused
                no_sync=True,
                errors=errors,
                failed_windows=failed_windows,
            )
            # Return immediately with paused status
            return {
                "measurement": measurement,
                "status": "paused",
                "rows_imported": rows_imported,
                "errors": errors,
                "failed_windows": failed_windows,
                "paused_at_time": paused_at_time,
            }

        # Calculate window
        if direction > 0:
            window_start = current_time
            window_end = current_time + timedelta(seconds=optimal_window_seconds)
            if window_end > actual_end:
                window_end = actual_end
        else:
            window_end = current_time
            window_start = current_time - timedelta(seconds=optimal_window_seconds)
            if window_start < actual_start:
                window_start = actual_start

        lower_bound = window_start.isoformat()
        upper_bound = window_end.isoformat()
        # Boundaries come back as datetimes, which hold no nanoseconds, so the
        # first window of a resume states the checkpoint exactly instead of
        # reaching back up to a microsecond past it and importing it twice
        if first_window and imported_up_to_ns is not None:
            if direction > 0:
                lower_bound = format_nanoseconds_iso(imported_up_to_ns + 1)
            else:
                upper_bound = format_nanoseconds_iso(imported_up_to_ns)
        first_window = False

        # Query data
        query = f"""
        SELECT * FROM {quote_influxql_identifier(measurement)}
        WHERE time >= '{lower_bound}' AND time < '{upper_bound}'
        ORDER BY time {"ASC" if direction > 0 else "DESC"}
        """
        try:
            influxdb3_local.info(
                f"[{task_id}] Querying data for '{measurement}' from {window_start} to {window_end}"
            )
            result = query_source_influxdb(influxdb3_local, config, credentials, query, task_id)

            if result.get("results") and result["results"][0].get("series"):
                series = result["results"][0]["series"][0]

                # Convert to line protocol with proper tag/field type information
                line_protocol = convert_influxql_to_line_protocol(
                    influxdb3_local, measurement, series, tags, fields, task_id, tag_renames
                )

                # Write to destination
                success, error = write_to_destination(
                    influxdb3_local, config.dest_database, line_protocol, task_id
                )

                if success:
                    rows_imported += len(line_protocol)
                    window_frontier = frontier_of_series(series, direction)
                    if window_frontier is not None:
                        frontier_ns = window_frontier

                    influxdb3_local.info(
                        f"[{task_id}] {measurement}: Imported {len(line_protocol)} rows "
                        f"({rows_imported} total)"
                    )

                    write_import_state(
                        influxdb3_local,
                        import_id,
                        measurement,
                        "in_progress",
                        rows_imported,
                        task_id,
                        # so a crash resumes from here instead of starting over
                        checkpoint_of(frontier_ns),
                        no_sync=True,
                        errors=errors,
                        failed_windows=failed_windows,
                        error_limit=PROGRESS_ERRORS,
                    )
                else:
                    failed_windows += 1
                    errors.append(
                        {
                            "time_range": f"{window_start} to {window_end}",
                            "error": error,
                        }
                    )
            else:
                influxdb3_local.info(
                    f"[{task_id}] No data found in specified range for '{measurement}'"
                )

            # Move to next window
            if direction > 0:
                current_time = window_end
                if current_time >= actual_end:
                    break
            else:
                current_time = window_start
                if current_time <= actual_start:
                    break

            # Rate limiting
            time.sleep(config.query_interval_ms / 1000.0)

        except Exception as e:
            influxdb3_local.error(
                f"[{task_id}] Error during import of '{measurement}': {e}"
            )
            # Write paused state for this table so it can be resumed from this point
            write_import_state(
                influxdb3_local,
                import_id,
                measurement,
                "paused",
                rows_imported,
                task_id,
                checkpoint_of(frontier_ns),
                no_sync=True,
                errors=errors,
                failed_windows=failed_windows,
            )
            raise

    influxdb3_local.info(
        f"[{task_id}] Completed import for '{measurement}': {rows_imported} rows imported"
    )

    # Write final completion status
    write_import_state(
        influxdb3_local,
        import_id,
        measurement,
        "completed",
        rows_imported,
        task_id,
        no_sync=True,
        errors=errors,
        failed_windows=failed_windows,
    )

    return {
        "measurement": measurement,
        "status": "completed",
        "rows_imported": rows_imported,
        "errors": errors,
        "failed_windows": failed_windows,
    }


def _stopped_import_report(
    influxdb3_local,
    import_id: str,
    status: str,
    table_result: Dict[str, Any],
    completed_tables: int,
    total_tables: int,
    total_rows: int,
    task_id: str,
) -> Dict[str, Any]:
    """
    Build the report for an import the user paused or cancelled mid-run.

    Shared by a first run and a resumed one so both answer in the same shape.
    """
    measurement = table_result["measurement"]
    noun = STOP_NOUNS[status]

    influxdb3_local.info(
        f"[{task_id}] Import {status} by user on table '{measurement}'"
    )
    influxdb3_local.info(
        f"[{task_id}] Tables completed before {noun}: {completed_tables}/{total_tables}"
    )
    influxdb3_local.info(f"[{task_id}] Rows imported before {noun}: {total_rows}")

    return {
        "import_id": import_id,
        "status": status,
        f"{status}_on_table": measurement,
        "tables_completed": completed_tables,
        "total_tables": total_tables,
        "rows_imported": total_rows,
        f"{status}_at_time": table_result.get(f"{status}_at_time"),
        "message": (
            f"Import {status} by user. Completed {completed_tables}/{total_tables} "
            f"tables, {total_rows} rows imported."
        ),
    }


def resume_incomplete_import(
    influxdb3_local,
    config: ImportConfig,
    credentials: Dict[str, Optional[str]],
    import_id: str,
    incomplete_tables: List[Dict[str, Any]],
    task_id: str,
) -> Dict[str, Any]:
    """
    Resume an incomplete import from last checkpoint
    """
    influxdb3_local.info(f"[{task_id}] Resuming incomplete import {import_id}")

    # Parse timestamps
    start_dt = (
        parse_timestamp(config.start_timestamp) if config.start_timestamp else None
    )
    end_dt = parse_timestamp(config.end_timestamp) if config.end_timestamp else None

    # Get all measurements to import
    all_measurements = get_source_measurements(influxdb3_local, config, credentials, task_id)

    # Determine which tables still need import
    tables_to_resume = {}
    tables_to_restart = set()

    for table_info in incomplete_tables:
        table_name = table_info["table_name"]
        paused_at_time_str = table_info.get("paused_at_time", "")

        # No paused_at_time means no window of the table is written yet, whether
        # it never started or stopped before finishing its first one
        if not paused_at_time_str or paused_at_time_str.strip() == "":
            influxdb3_local.warn(
                f"[{task_id}] Table '{table_name}' has status '{table_info.get('status')}' "
                f"and no paused_at_time, so nothing of it is imported yet. "
                f"Importing from the beginning."
            )
            tables_to_restart.add(table_name)
        else:
            # Valid paused_at_time - can resume from checkpoint
            stored_errors = table_info.get("errors") or {}
            tables_to_resume[table_name] = {
                "resume_from_timestamp": paused_at_time_str,
                "rows_imported": table_info.get("rows_imported", 0),
                "failed_windows": stored_errors.get("failed_windows", 0),
                "errors": stored_errors.get("errors", []),
            }

    # Import remaining tables
    import_start = time.time()
    started_at = datetime.now(timezone.utc)
    total_rows = 0
    completed_tables = 0
    total_failed_windows = 0

    for idx, measurement in enumerate(all_measurements, 1):
        # Check if table needs restart from beginning (DB crash scenario)
        if measurement in tables_to_restart:
            influxdb3_local.info(
                f"[{task_id}] Restarting table {idx}/{len(all_measurements)}: {measurement} "
                f"from beginning (no valid checkpoint)"
            )
            table_result = import_table(
                influxdb3_local,
                config,
                credentials,
                import_id,
                measurement,
                start_dt,
                end_dt,
                task_id,
            )
        # Check if table needs resumption from checkpoint
        elif measurement in tables_to_resume:
            influxdb3_local.info(
                f"[{task_id}] Resuming table {idx}/{len(all_measurements)}: {measurement} "
                f"from timestamp {tables_to_resume[measurement]['resume_from_timestamp']}"
            )
            # Resume from checkpoint (using data timestamp, not record timestamp)
            checkpoint_ns = parse_timestamp_to_nanoseconds(
                tables_to_resume[measurement]["resume_from_timestamp"]
            )
            # The checkpoint bounds whichever side the import was moving
            # towards, one nanosecond past it so it is not imported twice
            if config.import_direction == "newest_first":
                resume_start = start_dt
                resume_end = format_nanoseconds_iso(checkpoint_ns - 1)
            else:
                resume_start = format_nanoseconds_iso(checkpoint_ns + 1)
                resume_end = end_dt

            table_result = import_table(
                influxdb3_local,
                config,
                credentials,
                import_id,
                measurement,
                resume_start,
                resume_end,
                task_id,
                rows_already_imported=tables_to_resume[measurement]["rows_imported"],
                imported_up_to_ns=checkpoint_ns,
                windows_already_failed=tables_to_resume[measurement]["failed_windows"],
                errors_already_recorded=tables_to_resume[measurement]["errors"],
            )
        else:
            # Check if already completed
            try:
                check_query = f"""
                SELECT *
                FROM 'import_state'
                WHERE import_id = '{escape_string_literal(import_id)}' AND table_name = '{escape_string_literal(measurement)}'
                ORDER BY time DESC
                LIMIT 1
                """
                check_result = influxdb3_local.query(check_query)
                if check_result and check_result[0].get("status") == "completed":
                    influxdb3_local.info(
                        f"[{task_id}] Table {measurement} already completed, skipping"
                    )
                    completed_tables += 1
                    # counted here because the table is skipped, so what it
                    # imported and what it failed would be missing from the report
                    row = check_result[0]
                    total_rows += row.get("rows_imported", 0)
                    stored_errors = json.loads(row["errors"]) if row.get("errors") else {}
                    total_failed_windows += stored_errors.get("failed_windows", 0)
                    continue
            except Exception:
                pass

            # Import from beginning
            influxdb3_local.info(
                f"[{task_id}] Importing table {idx}/{len(all_measurements)}: {measurement}"
            )
            table_result = import_table(
                influxdb3_local,
                config,
                credentials,
                import_id,
                measurement,
                start_dt,
                end_dt,
                task_id,
            )

        if table_result["status"] in ["completed"]:
            completed_tables += 1
            total_rows += table_result.get("rows_imported", 0)
        elif table_result["status"] in STOP_NOUNS:
            total_rows += table_result.get("rows_imported", 0)
            return _stopped_import_report(
                influxdb3_local,
                import_id,
                table_result["status"],
                table_result,
                completed_tables,
                len(all_measurements),
                total_rows,
                task_id,
            )

        total_failed_windows += table_result.get("failed_windows", 0)

        influxdb3_local.info(
            f"[{task_id}] Progress: {completed_tables}/{len(all_measurements)} tables completed"
        )

    import_duration = time.time() - import_start

    # Write completed state to import_pause_state
    try:
        _write_import_pause_state(influxdb3_local, import_id, paused=False, canceled=False, completed=True)
    except Exception as e:
        influxdb3_local.warn(f"[{task_id}] Failed to write completed state: {e}")

    # Generate final report
    report = {
        "import_id": import_id,
        "status": "resumed_and_completed",
        "start_time": started_at.isoformat(),
        "duration_seconds": import_duration,
        "time_range": {"start": config.start_timestamp, "end": config.end_timestamp},
        "tables": {"total": len(all_measurements), "completed": completed_tables},
        "rows_imported": total_rows,
        "errors": total_failed_windows,
    }

    influxdb3_local.info(
        f"[{task_id}] ============================================================"
    )
    influxdb3_local.info(f"[{task_id}] RESUMED IMPORT COMPLETED")
    influxdb3_local.info(f"[{task_id}] Import ID: {import_id}")
    influxdb3_local.info(f"[{task_id}] Duration: {import_duration:.2f} seconds")
    influxdb3_local.info(
        f"[{task_id}] Tables imported: {completed_tables}/{len(all_measurements)}"
    )
    influxdb3_local.info(f"[{task_id}] Total rows: {total_rows}")
    influxdb3_local.info(f"[{task_id}] Errors encountered: {total_failed_windows}")
    influxdb3_local.info(
        f"[{task_id}] ============================================================"
    )

    return report


def generate_import_plan(
    influxdb3_local,
    config: ImportConfig,
    credentials: Dict[str, Optional[str]],
    import_id: str,
    measurements: List[str],
    time_estimate: Dict[str, Any],
    task_id: str,
) -> Dict[str, Any]:
    """
    Generate a dry-run import plan with schema conflicts and estimates

    Args:
        influxdb3_local: InfluxDB3Local instance
        config: Import configuration
        import_id: UUID of the import
        measurements: List of measurements to import
        time_estimate: Time estimation data
        task_id: Task ID for logging

    Returns:
        Dictionary with import plan details
    """
    influxdb3_local.info(
        f"[{task_id}] DRY RUN MODE: Collecting schema information for import plan..."
    )

    # Collect schema conflicts for all tables
    schema_conflicts = []
    for measurement in measurements:
        try:
            fields = get_field_keys(influxdb3_local, config, credentials, measurement, task_id)
            tags = get_tag_keys(influxdb3_local, config, credentials, measurement, task_id)
            conflicts = check_tag_field_conflicts(tags, fields)

            if conflicts:
                schema_conflicts.append(
                    {
                        "measurement": measurement,
                        "type": "tag_field_conflict",
                        "conflicts": conflicts,
                        "resolution": f"Tags will be renamed with '_tag' suffix: {', '.join([f'{c} -> {c}_tag' for c in conflicts])}",
                    }
                )
        except Exception as e:
            influxdb3_local.warn(
                f"[{task_id}] Failed to check schema for '{measurement}': {e}"
            )

    # Build import plan
    import_plan = {
        "import_id": import_id,
        "status": "dry_run_plan",
        "source": {
            "url": config.source_url,
            "database": config.source_database,
            "influxdb_version": config.influxdb_version
        },
        "destination": {
            "database": config.dest_database
        },
        "time_range": {
            "start": config.start_timestamp if config.start_timestamp else "all data",
            "end": config.end_timestamp if config.end_timestamp else "all data"
        },
        "import_settings": {
            "direction": config.import_direction,
            "target_batch_size": config.target_batch_size,
            "query_interval_ms": config.query_interval_ms
        },
        "tables": {
            "total": len(measurements),
            "list": measurements,
            "filtered": config.table_filter if config.table_filter else "all tables"
        },
        "estimated_import": {
            "total_rows": time_estimate["estimated_total_rows"],
            "estimated_duration": time_estimate["estimated_duration_human"],
            "estimated_duration_seconds": time_estimate["estimated_duration_seconds"],
            "per_table_estimates": time_estimate["per_table_estimates"]
        },
        "schema_conflicts": {
            "total": len(schema_conflicts),
            "details": schema_conflicts
        }
    }

    influxdb3_local.info(
        f"[{task_id}] ============================================================"
    )
    influxdb3_local.info(f"[{task_id}] DRY RUN IMPORT PLAN")
    influxdb3_local.info(f"[{task_id}] Import ID: {import_id}")
    influxdb3_local.info(f"[{task_id}] Tables to import: {len(measurements)}")
    influxdb3_local.info(f"[{task_id}] Estimated rows: {time_estimate['estimated_total_rows']:,}")
    influxdb3_local.info(f"[{task_id}] Estimated duration: {time_estimate['estimated_duration_human']}")
    influxdb3_local.info(f"[{task_id}] Schema conflicts: {len(schema_conflicts)}")
    if schema_conflicts:
        for conflict in schema_conflicts:
            influxdb3_local.info(
                f"[{task_id}]   - {conflict['measurement']}: {', '.join(conflict['conflicts'])}"
            )
    influxdb3_local.info(
        f"[{task_id}] ============================================================"
    )

    return import_plan


def start_import(influxdb3_local, config: ImportConfig, credentials: Dict[str, Optional[str]], task_id: str) -> Dict[str, Any]:
    """
    Start a new import process
    Returns import_id and initial status
    """
    import_id = str(uuid.uuid4())

    influxdb3_local.info(f"[{task_id}] Starting import {import_id}")
    influxdb3_local.info(
        f"[{task_id}] Source: {config.source_url}/{config.source_database}"
    )
    influxdb3_local.info(f"[{task_id}] Destination: {config.dest_database}")
    influxdb3_local.info(
        f"[{task_id}] Time range: {config.start_timestamp} to {config.end_timestamp}"
    )
    influxdb3_local.info(f"[{task_id}] Direction: {config.import_direction}")
    if config.dry_run:
        influxdb3_local.info(
            f"[{task_id}] DRY RUN MODE IS SET - No data will be written"
        )

    if not config.dry_run:
        # Save import configuration for potential resumption
        try:
            save_import_config(influxdb3_local, import_id, config, task_id)
        except Exception as e:
            influxdb3_local.error(f"[{task_id}] Failed to save import config: {e}")
            return {
                "import_id": import_id,
                "status": "failed",
                "errors": [f"Failed to save import config: {e}"],
            }

        # Create default pause state record (not paused, not canceled, not completed)
        try:
            _write_import_pause_state(influxdb3_local, import_id, paused=False, canceled=False, completed=False)
            influxdb3_local.info(
                f"[{task_id}] Created default pause state for import {import_id}"
            )
        except Exception as e:
            return {
                "import_id": import_id,
                "status": "failed",
                "errors": [
                    f"Failed to create default pause state record in import_pause_state table: {e}"
                ],
            }

    # Run the import with error handling — on any unhandled error, set state to paused
    try:
        return _run_import(influxdb3_local, config, credentials, import_id, task_id)
    except Exception as e:
        influxdb3_local.error(
            f"[{task_id}] Import failed with error: {e}. Setting state to paused for resumption."
        )
        _write_pause_state_on_error(influxdb3_local, import_id, task_id)
        return {
            "import_id": import_id,
            "status": "error",
            "error": f"Import failed: {e}. State set to paused — resume after fixing the issue.",
        }


def _write_import_pause_state(
    influxdb3_local,
    import_id: str,
    paused: bool,
    canceled: bool,
    completed: bool,
) -> None:
    """Write a record to the import_pause_state table."""
    builder = LineBuilder("import_pause_state")
    builder.tag("import_id", import_id)
    builder.bool_field("paused", paused)
    builder.bool_field("canceled", canceled)
    builder.bool_field("completed", completed)
    builder.time_ns(int(time.time() * 1_000_000_000))
    influxdb3_local.write_sync(builder, no_sync=False)


def _write_pause_state_on_error(
    influxdb3_local, import_id: str, task_id: str
) -> None:
    """Write paused state to import_pause_state so the import can be resumed after fixing the issue."""
    try:
        _write_import_pause_state(influxdb3_local, import_id, paused=True, canceled=False, completed=False)
        influxdb3_local.info(
            f"[{task_id}] Wrote paused state for import {import_id} due to error"
        )
    except Exception as write_err:
        influxdb3_local.error(
            f"[{task_id}] Failed to write paused state after error: {write_err}"
        )


def _run_import(
    influxdb3_local, config: ImportConfig, credentials: Dict[str, Optional[str]], import_id: str, task_id: str
) -> Dict[str, Any]:
    """
    Internal import execution logic.
    Exceptions propagate to the caller (start_import) for state cleanup.
    """
    # Perform pre-flight checks
    success, errors, metadata = perform_preflight_checks(
        influxdb3_local, config, credentials, task_id
    )

    if not success:
        influxdb3_local.error(f"[{task_id}] Pre-flight checks failed:")
        for error in errors:
            influxdb3_local.error(f"[{task_id}]   - {error}")
        raise Exception(f"Pre-flight checks failed: {'; '.join(errors)}")

    measurements = metadata["measurements"]
    total_tables = len(measurements)

    influxdb3_local.info(
        f"[{task_id}] Pre-flight checks passed. {total_tables} tables to import."
    )

    # Parse timestamps (if provided, otherwise set to None for full table copy)
    start_dt = (
        parse_timestamp(config.start_timestamp) if config.start_timestamp else None
    )
    end_dt = parse_timestamp(config.end_timestamp) if config.end_timestamp else None

    if start_dt is None or end_dt is None:
        influxdb3_local.info(
            f"[{task_id}] No time range specified - will import all data from tables"
        )

    # Estimate import time
    influxdb3_local.info(
        f"[{task_id}] Estimating import time based on data sampling..."
    )
    time_estimate = estimate_import_time(
        influxdb3_local, config, credentials, measurements, start_dt, end_dt, task_id
    )
    metadata["time_estimate"] = time_estimate

    influxdb3_local.info(
        f"[{task_id}] Estimated import time: {time_estimate['estimated_duration_human']} "
        f"({time_estimate['estimated_total_rows']:,} rows total)"
    )

    # Log per-table estimates for large imports
    if total_tables <= 10:
        for table_est in time_estimate["per_table_estimates"]:
            if table_est.get("estimated_rows", 0) > 0:
                influxdb3_local.info(
                    f"[{task_id}]   - {table_est['measurement']}: "
                    f"{table_est['estimated_rows']:,} rows (~{table_est['estimated_seconds']:.1f}s)"
                )

    # If dry_run mode, generate import plan and return immediately
    if config.dry_run:
        return generate_import_plan(
            influxdb3_local,
            config,
            credentials,
            import_id,
            measurements,
            time_estimate,
            task_id,
        )

    # Write initial import state for status tracking
    try:
        for measurement in measurements:
            write_import_state(
                influxdb3_local, import_id, measurement, "pending", 0, task_id
            )
        influxdb3_local.info(
            f"[{task_id}] Initialized import state for {len(measurements)} tables"
        )
    except Exception as e:
        influxdb3_local.warn(f"[{task_id}] Failed to initialize import state: {e}")

    # Import each table
    import_start = time.time()
    started_at = datetime.now(timezone.utc)
    total_rows = 0
    completed_tables = 0
    all_errors = []

    for idx, measurement in enumerate(measurements, 1):
        influxdb3_local.info(
            f"[{task_id}] Importing table {idx}/{total_tables}: {measurement}"
        )

        table_result = import_table(
            influxdb3_local,
            config,
            credentials,
            import_id,
            measurement,
            start_dt,
            end_dt,
            task_id,
            metadata=metadata,
        )

        if table_result["status"] in ["completed"]:
            completed_tables += 1
            total_rows += table_result.get("rows_imported", 0)
        elif table_result["status"] in STOP_NOUNS:
            total_rows += table_result.get("rows_imported", 0)
            return _stopped_import_report(
                influxdb3_local,
                import_id,
                table_result["status"],
                table_result,
                completed_tables,
                total_tables,
                total_rows,
                task_id,
            )

        if "errors" in table_result:
            all_errors.extend(table_result["errors"])

        influxdb3_local.info(
            f"[{task_id}] Progress: {completed_tables}/{total_tables} tables completed"
        )

    import_duration = time.time() - import_start

    # Write completed state to import_pause_state
    try:
        _write_import_pause_state(influxdb3_local, import_id, paused=False, canceled=False, completed=True)
    except Exception as e:
        influxdb3_local.warn(f"[{task_id}] Failed to write completed state: {e}")

    # Generate final report
    report = {
        "import_id": import_id,
        "status": "completed",
        "start_time": started_at.isoformat(),
        "duration_seconds": import_duration,
        "time_range": {"start": config.start_timestamp, "end": config.end_timestamp},
        "tables": {"total": total_tables, "completed": completed_tables},
        "rows_imported": total_rows,
        "schema_issues": metadata.get("schema_issues", []),
        "errors": all_errors,
        "time_estimate": metadata.get("time_estimate"),
    }

    influxdb3_local.info(
        f"[{task_id}] ============================================================"
    )
    influxdb3_local.info(f"[{task_id}] IMPORT COMPLETED")
    influxdb3_local.info(f"[{task_id}] Import ID: {import_id}")
    influxdb3_local.info(f"[{task_id}] Duration: {import_duration:.2f} seconds")
    influxdb3_local.info(
        f"[{task_id}] Tables imported: {completed_tables}/{total_tables}"
    )
    influxdb3_local.info(f"[{task_id}] Total rows: {total_rows}")
    influxdb3_local.info(
        f"[{task_id}] Schema issues handled: {metadata.get('schema_issues', [])}"
    )
    influxdb3_local.info(f"[{task_id}] Errors encountered: {all_errors}")
    influxdb3_local.info(
        f"[{task_id}] ============================================================"
    )

    return report


def get_import_pause_state(
    influxdb3_local, import_id: str, task_id: str
) -> ImportPauseState:
    """
    Get the current state of an import from the import_pause_state table.

    Returns an ImportPauseState enum value:
        NOT_FOUND  - no record exists in import_pause_state for this import_id
        CANCELLED  - the import has been canceled
        COMPLETED  - the import has completed successfully
        PAUSED     - the import is paused
        RUNNING    - the import exists but is neither paused, canceled, nor completed
    """
    try:
        status_query = f"""
        SELECT paused, canceled, completed
        FROM 'import_pause_state'
        WHERE import_id = '{escape_string_literal(import_id)}'
        ORDER BY time DESC
        LIMIT 1
        """
        result = influxdb3_local.query(status_query)

        if not result or len(result) == 0:
            return ImportPauseState.NOT_FOUND

        row = result[0]

        canceled_value = row.get("canceled", False)
        if str(canceled_value).lower() == "true":
            return ImportPauseState.CANCELLED

        completed_value = row.get("completed", False)
        if str(completed_value).lower() == "true":
            return ImportPauseState.COMPLETED

        paused_value = row.get("paused", False)
        if str(paused_value).lower() == "true":
            return ImportPauseState.PAUSED

        return ImportPauseState.RUNNING
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to get import pause state: {e}")
        return ImportPauseState.NOT_FOUND


def pause_import(influxdb3_local, import_id: str, task_id: str) -> Dict[str, Any]:
    """Pause an in-progress import by writing pause state using LineBuilder"""
    try:
        # Check import state using get_import_pause_state
        pause_state = get_import_pause_state(influxdb3_local, import_id, task_id)

        if pause_state == ImportPauseState.NOT_FOUND:
            return {"status": "error", "error": f"Import {import_id} not found"}

        if pause_state == ImportPauseState.CANCELLED:
            return {"status": "error", "error": f"Import {import_id} is already cancelled and cannot be paused"}

        if pause_state == ImportPauseState.COMPLETED:
            return {"status": "error", "error": f"Import {import_id} is already completed and cannot be paused"}

        if pause_state == ImportPauseState.PAUSED:
            return {"status": "error", "error": f"Import {import_id} is already paused"}

        _write_import_pause_state(influxdb3_local, import_id, paused=True, canceled=False, completed=False)
        influxdb3_local.info(f"[{task_id}] Import {import_id} paused")
        return {"status": "paused", "import_id": import_id}
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to pause import: {e}")
        return {"status": "error", "error": str(e)}


def resume_import(
    influxdb3_local,
    import_id: str,
    credentials: Dict[str, Optional[str]],
    task_id: str,
) -> Dict[str, Any]:
    """
    Resume a paused or incomplete import
    Checks import status and continues from where it stopped
    Requires either source_token OR (source_username AND source_password) in credentials dict
    """
    try:
        influxdb3_local.info(
            f"[{task_id}] Attempting to resume import {import_id}"
        )

        # Check import pause state first
        pause_state = get_import_pause_state(influxdb3_local, import_id, task_id)

        if pause_state == ImportPauseState.NOT_FOUND:
            return {"status": "error", "error": f"Import {import_id} not found"}

        if pause_state == ImportPauseState.CANCELLED:
            return {"status": "error", "error": f"Import {import_id} was cancelled and cannot be resumed"}

        if pause_state == ImportPauseState.COMPLETED:
            return {"status": "error", "error": f"Import {import_id} is already completed"}

        if pause_state == ImportPauseState.RUNNING:
            # Check if the import is actually running or just stale (crashed without writing paused state)
            try:
                stale_query = f"""
                SELECT time
                FROM 'import_state'
                WHERE import_id = '{escape_string_literal(import_id)}'
                ORDER BY time DESC
                LIMIT 1
                """
                stale_result = influxdb3_local.query(stale_query)
            except Exception:
                stale_result = None

            if stale_result:
                last_update_ns = stale_result[0].get("time")
                if last_update_ns is not None:
                    age_seconds = time.time() - (last_update_ns / 1e9)
                    if age_seconds > STALE_IMPORT_THRESHOLD_SECONDS:
                        influxdb3_local.warn(
                            f"[{task_id}] Import {import_id} has stale in_progress state "
                            f"(last update {age_seconds:.0f}s ago), treating as crashed. Allowing resume."
                        )
                    else:
                        return {"status": "error", "error": f"Import {import_id} is already running"}
                else:
                    return {"status": "error", "error": f"Import {import_id} is already running"}
            else:
                # No import_state records at all — import crashed before processing any tables
                influxdb3_local.warn(
                    f"[{task_id}] Import {import_id} is in running state but has no import_state records. "
                    f"Treating as crashed. Allowing resume."
                )

        # Load import configuration
        config = load_import_config(
            influxdb3_local,
            import_id,
            task_id,
        )
        if not config:
            return {
                "status": "error",
                "error": f"Import config not found for {import_id}. Cannot resume import.",
            }

        # The latest row of each table, taken whole because rows written by an
        # earlier version of the plugin have no 'errors' column
        try:
            status_query = f"""
            SELECT DISTINCT ON (table_name) *
            FROM 'import_state'
            WHERE import_id = '{escape_string_literal(import_id)}'
            ORDER BY table_name, time DESC
            """
            status_result = influxdb3_local.query(status_query)
        except Exception:
            status_result = None

        # If no import_state records exist, the import failed before any tables were processed.
        # Restart it from the beginning.
        if not status_result:
            influxdb3_local.info(
                f"[{task_id}] No import_state records found for {import_id}. Restarting import from the beginning."
            )
            # Write resume state (unpause the import)
            _write_import_pause_state(influxdb3_local, import_id, paused=False, canceled=False, completed=False)

            return _run_import(influxdb3_local, config, credentials, import_id, task_id)

        # The query above already returns one row per table
        latest_states = {
            row.get("table_name"): {
                "table_name": row.get("table_name"),
                "status": row.get("status"),
                "rows_imported": row.get("rows_imported", 0),
                "paused_at_time": row.get("paused_at_time", ""),
                "errors": json.loads(row["errors"]) if row.get("errors") else {},
            }
            for row in status_result
        }

        # Every table with work left, whatever stage it stopped at. A table that
        # never started is 'pending' and resume_incomplete_import imports it
        # from the beginning, so leaving it out would strand the import
        incomplete_tables = [
            state
            for table_name, state in latest_states.items()
            if table_name != "all"
            and state["status"] not in ["completed", "cancelled"]
        ]

        if not incomplete_tables:
            return {
                "status": "error",
                "error": f"Import {import_id} is already completed",
            }

        # Write resume state (unpause the import)
        _write_import_pause_state(influxdb3_local, import_id, paused=False, canceled=False, completed=False)
        influxdb3_local.info(f"[{task_id}] Wrote resume state for import {import_id}")

        influxdb3_local.info(
            f"[{task_id}] Found {len(incomplete_tables)} incomplete tables to resume for import {import_id}: {incomplete_tables}"
        )

        # Resume the import using resume_incomplete_import
        return resume_incomplete_import(
            influxdb3_local,
            config,
            credentials,
            import_id,
            incomplete_tables,
            task_id,
        )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to resume import: {e}. Setting state to paused for resumption.")
        _write_pause_state_on_error(influxdb3_local, import_id, task_id)
        return {"import_id": import_id, "status": "error", "error": f"Resume failed: {e}. State set to paused — resume after fixing the issue."}


def get_import_stats(influxdb3_local, import_id: str, task_id: str) -> Dict[str, Any]:
    """
    Get comprehensive statistics for a import

    Returns:
        Dictionary with import statistics including:
        - Overall status (running, paused, cancelled, completed)
        - Total tables and their statuses
        - Total rows imported
        - Per-table progress
        - Import config
        - Time information
    """
    try:
        # 1. The latest state record of each table, and the span of all of them.
        # Taken whole because rows written by an earlier version of the plugin
        # have no 'errors' column, and naming a column the table does not carry
        # would fail the query.
        state_query = f"""
        SELECT DISTINCT ON (table_name) *
        FROM 'import_state'
        WHERE import_id = '{escape_string_literal(import_id)}'
        ORDER BY table_name, time DESC
        """
        state_result = influxdb3_local.query(state_query)

        if not state_result or len(state_result) == 0:
            return {
                "status": "not_found",
                "import_id": import_id,
                "error": "No import records found",
            }

        span_query = f"""
        SELECT min(time) AS earliest, max(time) AS latest
        FROM 'import_state'
        WHERE import_id = '{escape_string_literal(import_id)}'
        """
        span_result = influxdb3_local.query(span_query)

        # 2. Get pause/cancel/completed state
        pause_query = f"""
        SELECT paused, canceled, completed, time
        FROM 'import_pause_state'
        WHERE import_id = '{escape_string_literal(import_id)}'
        ORDER BY time DESC
        LIMIT 1
        """
        pause_result = influxdb3_local.query(pause_query)

        # 3. Get import config
        config_query = f"""
        SELECT *
        FROM 'import_config'
        WHERE import_id = '{escape_string_literal(import_id)}'
        ORDER BY time DESC
        LIMIT 1
        """
        config_result = influxdb3_local.query(config_query)

        span = span_result[0] if span_result else {}
        earliest_time = span.get("earliest")
        latest_time = span.get("latest")

        # The query above already returns one row per table
        latest_table_states = {
            row.get("table_name"): {
                "table_name": row.get("table_name"),
                "status": row.get("status"),
                "rows_imported": row.get("rows_imported", 0),
                "last_update": row.get("time"),
                "paused_at_time": row.get("paused_at_time", ""),
                "errors": json.loads(row["errors"]) if row.get("errors") else None,
            }
            for row in state_result
        }

        # Calculate statistics
        total_tables = len([t for t in latest_table_states.keys() if t != "all"])
        completed_tables = len(
            [
                t
                for t, s in latest_table_states.items()
                if t != "all" and s["status"] == "completed"
            ]
        )
        in_progress_tables = len(
            [
                t
                for t, s in latest_table_states.items()
                if t != "all" and s["status"] == "in_progress"
            ]
        )
        paused_tables = len(
            [
                t
                for t, s in latest_table_states.items()
                if t != "all" and s["status"] == "paused"
            ]
        )
        cancelled_tables = len(
            [
                t
                for t, s in latest_table_states.items()
                if t != "all" and s["status"] == "cancelled"
            ]
        )
        pending_tables = len(
            [
                t
                for t, s in latest_table_states.items()
                if t != "all" and s["status"] == "pending"
            ]
        )

        total_rows_imported = sum(
            s["rows_imported"] for t, s in latest_table_states.items() if t != "all"
        )

        tables_with_errors = len(
            [
                t
                for t, s in latest_table_states.items()
                if t != "all" and (s["errors"] or {}).get("failed_windows")
            ]
        )

        # Determine overall import status
        overall_status = "unknown"
        is_paused = False
        is_cancelled = False
        is_completed = False

        if pause_result and len(pause_result) > 0:
            pause_state = pause_result[0]
            is_cancelled = str(pause_state.get("canceled", False)).lower() == "true"
            is_completed = str(pause_state.get("completed", False)).lower() == "true"
            is_paused = str(pause_state.get("paused", False)).lower() == "true"

        if (
            is_cancelled
            or "all" in latest_table_states
            and latest_table_states["all"]["status"] == "cancelled"
        ):
            overall_status = "cancelled"
        elif is_completed or (completed_tables == total_tables and total_tables > 0):
            overall_status = "completed"
        elif is_paused:
            overall_status = "paused"
        elif in_progress_tables > 0 or pending_tables > 0:
            overall_status = "running"
        else:
            overall_status = "unknown"

        # Build per-table details (exclude 'all' marker)
        table_details = [
            {
                "table_name": s["table_name"],
                "status": s["status"],
                "rows_imported": s["rows_imported"],
                "last_update": s["last_update"],
                "paused_at_time": s["paused_at_time"] if s["paused_at_time"] else None,
                "errors": s["errors"],
            }
            for t, s in latest_table_states.items()
            if t != "all"
        ]

        # Sort by table name
        table_details.sort(key=lambda x: x["table_name"])

        # Build config summary
        config_summary = None
        if config_result and len(config_result) > 0:
            config_row = config_result[0]
            config_summary = {
                "source_url": config_row.get("source_url"),
                "source_database": config_row.get("source_database"),
                "dest_database": config_row.get("dest_database"),
                "start_timestamp": config_row.get("start_timestamp"),
                "end_timestamp": config_row.get("end_timestamp"),
                "import_direction": config_row.get("import_direction"),
                "target_batch_size": config_row.get("target_batch_size"),
                "query_interval_ms": config_row.get("query_interval_ms"),
                "table_filter": config_row.get("table_filter"),
            }

        # Calculate duration if possible
        duration_seconds = None
        if earliest_time and latest_time:
            # Convert nanoseconds to seconds
            duration_seconds = (latest_time - earliest_time) / 1_000_000_000

        # Calculate progress percentage
        progress_percentage = 0.0
        if total_tables > 0:
            progress_percentage = (completed_tables / total_tables) * 100

        # Build final statistics
        stats = {
            "import_id": import_id,
            "overall_status": overall_status,
            "summary": {
                "total_tables": total_tables,
                "completed_tables": completed_tables,
                "in_progress_tables": in_progress_tables,
                "paused_tables": paused_tables,
                "cancelled_tables": cancelled_tables,
                "pending_tables": pending_tables,
                "tables_with_errors": tables_with_errors,
                "total_rows_imported": total_rows_imported,
                "progress_percentage": round(progress_percentage, 2),
            },
            "timing": {
                "started_at": earliest_time,
                "last_updated_at": latest_time,
                "duration_seconds": (
                    round(duration_seconds, 2) if duration_seconds else None
                ),
            },
            "config": config_summary,
            "pause_state": (
                {"is_paused": is_paused, "is_cancelled": is_cancelled, "is_completed": is_completed}
                if pause_result
                else None
            ),
            "table_details": table_details,
        }

        influxdb3_local.info(
            f"[{task_id}] Retrieved stats for import {import_id}: "
            f"{completed_tables}/{total_tables} tables, {total_rows_imported} rows, "
            f"status: {overall_status}"
        )

        return stats

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to get import stats: {e}")
        return {"status": "error", "import_id": import_id, "error": str(e)}


def cancel_import(influxdb3_local, import_id: str, task_id: str) -> Dict[str, Any]:
    """Cancel a import by writing cancel state using LineBuilder"""
    try:
        # Check import state using get_import_pause_state
        pause_state = get_import_pause_state(influxdb3_local, import_id, task_id)

        if pause_state == ImportPauseState.NOT_FOUND:
            return {"status": "error", "error": f"Import {import_id} not found"}

        if pause_state == ImportPauseState.CANCELLED:
            return {"status": "error", "error": f"Import {import_id} is already cancelled"}

        if pause_state == ImportPauseState.COMPLETED:
            return {"status": "error", "error": f"Import {import_id} is already completed and cannot be cancelled"}

        # Write pause state with canceled flag
        _write_import_pause_state(influxdb3_local, import_id, paused=True, canceled=True, completed=False)

        # Write cancelled status using LineBuilder
        status_builder = LineBuilder("import_state")
        status_builder.tag("import_id", import_id)
        status_builder.tag("table_name", "all")
        status_builder.string_field("status", "cancelled")
        status_builder.int64_field("rows_imported", 0)
        status_builder.time_ns(int(time.time() * 1_000_000_000))
        influxdb3_local.write_sync(status_builder, no_sync=False)

        influxdb3_local.info(f"[{task_id}] Import {import_id} cancelled")
        return {"status": "cancelled", "import_id": import_id}
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to cancel import: {e}")
        return {"status": "error", "error": str(e)}


def _validate_test_connection_params(body_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Validate test_connection parameters.

    Args:
        body_data: Dict containing source_url

    Returns:
        Error dict if validation fails, None if valid
    """
    try:
        body_data.update(validate(body_data, CONNECTION_VALIDATORS))
    except ValueError as e:
        return {"message": str(e)}
    return None


def _validate_source_params(body_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Validate common source connection parameters.

    Args:
        body_data: Dict containing source_url, influxdb_version

    Returns:
        Error dict if validation fails, None if valid
    """
    try:
        body_data.update(validate(body_data, SOURCE_VALIDATORS))
    except ValueError as e:
        return {"error": str(e)}
    return None


def _parse_url_with_port_inference(source_url: str) -> str:
    """Parse URL and infer port from scheme if not specified.

    Args:
        source_url: URL that may or may not include port

    Returns:
        URL with port included (inferred from scheme if missing)
    """
    from urllib.parse import urlparse, urlunparse

    source_url = source_url.rstrip("/")
    try:
        parsed = urlparse(source_url)

        if parsed.port is not None:
            return source_url

        if parsed.hostname is None:
            return source_url

        default_ports = {"http": 80, "https": 443}
        port = default_ports.get(parsed.scheme, 80)
        netloc_with_port = f"{parsed.hostname}:{port}"
        return urlunparse((parsed.scheme, netloc_with_port, parsed.path, "", "", ""))
    except Exception:
        return source_url


def _build_v1_headers(credentials: Dict[str, Optional[str]]) -> Dict[str, str]:
    """Build headers for InfluxDB v1 API requests."""
    headers = {"Content-Type": "application/json"}
    username = credentials.get("source_username")
    password = credentials.get("source_password")
    token = credentials.get("source_token")

    if username and password:
        creds = f"{username}:{password}"
        encoded = base64.b64encode(creds.encode()).decode()
        headers["Authorization"] = f"Basic {encoded}"
    elif token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _build_v2_headers(
    credentials: Dict[str, Optional[str]],
    extra_headers: Dict[str, str] | None = None,
) -> Dict[str, str]:
    """Build headers for InfluxDB v2 API requests."""
    headers = {}
    if extra_headers:
        headers.update(extra_headers)
    token = credentials.get("source_token")
    if token:
        headers["Authorization"] = f"Token {token}"
    return headers


def _build_v3_headers(credentials: Dict[str, Optional[str]]) -> Dict[str, str]:
    """Build headers for InfluxDB v3 API requests."""
    headers = {"Content-Type": "application/json"}
    token = credentials.get("source_token")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _parse_v1_series_values(result: Dict[str, Any]) -> List[str]:
    """Extract first column values from InfluxDB v1 query result.

    Args:
        result: JSON response from v1 /query endpoint

    Returns:
        List of values from first column of first series
    """
    if "results" not in result or len(result["results"]) == 0:
        return []
    series = result["results"][0].get("series", [])
    if not series or "values" not in series[0]:
        return []
    return [row[0] for row in series[0]["values"]]


def _parse_v3_databases(result: List[Dict[str, Any]]) -> List[str]:
    """Extract database names from v3 response, excluding _internal.

    Args:
        result: JSON response from v3 /api/v3/configure/database endpoint

    Returns:
        List of database names, excluding _internal
    """
    databases = []
    for row in result:
        db_name = row.get("iox::database")
        if db_name and db_name != "_internal":
            databases.append(db_name)
    return databases


def _parse_v3_tables(result: List[Dict[str, Any]]) -> List[str]:
    """Extract table names from v3 response, excluding system/information_schema.

    Args:
        result: JSON response from v3 /api/v3/query_sql?q=SHOW TABLES endpoint

    Returns:
        List of table names from iox schema only
    """
    excluded_schemas = {"system", "information_schema"}
    tables = []
    for row in result:
        schema = row.get("table_schema")
        table_name = row.get("table_name")
        if schema not in excluded_schemas and table_name:
            tables.append(table_name)
    return tables


def check_source_connection(
    body_data: Dict[str, Any],
    session: requests.Session = None,
) -> Dict[str, Any]:
    """Test connection to a URL and identify if it's an InfluxDB instance.

    Args:
        body_data: Dict containing source_url
        session: Optional requests.Session for dependency injection (testing)

    Returns:
        Dict with success=True and version/build if InfluxDB detected,
        or success=False with message if not InfluxDB or unreachable.
    """
    if session is None:
        session = get_http_session()

    validation_error = _validate_test_connection_params(body_data)
    if validation_error:
        return {"success": False, **validation_error}

    source_url = body_data.get("source_url")
    base_url = _parse_url_with_port_inference(source_url)

    try:
        response = session.get(f"{base_url}/ping", timeout=5)

        version = response.headers.get("X-Influxdb-Version")
        build = response.headers.get("X-Influxdb-Build")

        if version is not None or build is not None:
            return {"success": True, "version": version or "", "build": build or ""}

        # Detect InfluxDB v3 via cluster-uuid header (v3 doesn't expose version headers without auth)
        if response.headers.get("cluster-uuid"):
            return {"success": True, "version": "3.x.x", "build": ""}

        if response.status_code in (401, 403):
            return {"success": False, "message": "Unable to determine InfluxDB version"}

        return {"success": False, "message": "Not an InfluxDB instance"}

    except requests.exceptions.RequestException as e:
        return {"success": False, "message": str(e)}


def get_source_databases_list(
    body_data: Dict[str, Any],
    credentials: Dict[str, Optional[str]],
    session: requests.Session = None,
) -> Dict[str, Any]:
    """Get list of databases from source InfluxDB instance.

    Args:
        body_data: Dict containing source_url and influxdb_version
        credentials: Dict containing auth credentials (source_token, source_username, source_password)
        session: Optional requests.Session for dependency injection (testing)

    Returns:
        Dict with databases list or error
    """
    if session is None:
        session = get_http_session()

    validation_error = _validate_source_params(body_data)
    if validation_error:
        return validation_error

    source_url = body_data.get("source_url")
    influxdb_version = body_data.get("influxdb_version")

    base_url = _parse_url_with_port_inference(source_url)

    try:

        if influxdb_version == 1:
            headers = _build_v1_headers(credentials)

            response = session.get(
                f"{base_url}/query",
                params={"q": "SHOW DATABASES"},
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

            databases = _parse_v1_series_values(response.json())
            databases = [db for db in databases if db != "_internal"]
            return {"databases": sorted(databases)}

        elif influxdb_version == 2:
            headers = _build_v2_headers(credentials)
            headers["Content-Type"] = "application/json"

            response = session.get(
                f"{base_url}/query",
                params={"q": "SHOW DATABASES"},
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

            databases = _parse_v1_series_values(response.json())
            # InfluxDB 2 reserves the underscore prefix for its own buckets and
            # refuses to create a user bucket with it
            databases = [db for db in databases if not db.startswith("_")]
            return {"databases": sorted(databases)}

        elif influxdb_version == 3:
            headers = _build_v3_headers(credentials)

            response = session.get(
                f"{base_url}/api/v3/configure/database",
                params={"format": "json"},
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

            databases = _parse_v3_databases(response.json())
            return {"databases": sorted(databases)}
        else:
            return {"error": f"Unsupported version: {influxdb_version}"}

    except requests.exceptions.RequestException as e:
        return {"error": str(e)}


def get_source_tables_list(
    body_data: Dict[str, Any],
    credentials: Dict[str, Optional[str]],
    session: requests.Session = None,
) -> Dict[str, Any]:
    """Get list of tables/measurements from source database.

    Args:
        body_data: Dict containing source_url, influxdb_version, and source_database
        credentials: Dict containing auth credentials (source_token, source_username, source_password)
        session: Optional requests.Session for dependency injection (testing)

    Returns:
        Dict with tables list or error
    """
    if session is None:
        session = get_http_session()

    validation_error = _validate_source_params(body_data)
    if validation_error:
        return validation_error

    source_url = body_data.get("source_url")
    influxdb_version = body_data.get("influxdb_version")
    source_database = body_data.get("source_database")

    if not source_database:
        return {"error": "source_database is required"}

    base_url = _parse_url_with_port_inference(source_url)

    try:
        if influxdb_version == 1:
            headers = _build_v1_headers(credentials)

            response = session.get(
                f"{base_url}/query",
                params={"db": source_database, "q": "SHOW MEASUREMENTS"},
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

            tables = _parse_v1_series_values(response.json())
            return {"tables": sorted(tables)}

        elif influxdb_version == 2:
            headers = _build_v2_headers(credentials)
            headers["Content-Type"] = "application/json"

            response = session.get(
                f"{base_url}/query",
                params={"db": source_database, "q": "SHOW MEASUREMENTS"},
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

            tables = _parse_v1_series_values(response.json())
            return {"tables": sorted(tables)}

        elif influxdb_version == 3:
            headers = _build_v3_headers(credentials)

            response = session.get(
                f"{base_url}/api/v3/query_sql",
                params={"db": source_database, "q": "SHOW TABLES", "format": "json"},
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

            tables = _parse_v3_tables(response.json())
            return {"tables": sorted(tables)}
        else:
            return {"error": f"Unsupported version: {influxdb_version}"}

    except requests.exceptions.RequestException as e:
        return {"error": str(e)}


def process_request(
    influxdb3_local, query_parameters, request_headers, request_body, args=None
):
    """
    HTTP request handler for import plugin

    Endpoints:
    - POST /api/v3/engine/import?action=start - Start new import
    - GET /api/v3/engine/import?action=status&import_id=<id> - Get import status
    - POST /api/v3/engine/import?action=pause&import_id=<id> - Pause import
    - POST /api/v3/engine/import?action=resume&import_id=<id> - Resume import
    - POST /api/v3/engine/import?action=cancel&import_id=<id> - Cancel import

    Each action accepts only the query parameters it reads; an unknown one is
    refused and named. On start, a setting may also arrive as a query parameter
    or an X-Influxdb3-Import-<SETTING> header, both above the request body.
    Credentials are read from the Source-Token, Source-Username and
    Source-Password headers.
    """
    task_id: str = str(uuid.uuid4())
    influxdb3_local.info(f"[{task_id}] Import plugin invoked")

    try:
        action = query_parameters.get("action", "start")
        spec = QUERY_KEYS_BY_ACTION.get(action)
        if spec is None:
            return {
                "status": "error",
                "error": f"Unknown action: {action}",
                "available_actions": list(QUERY_KEYS_BY_ACTION),
            }

        # what is left once the control keys are taken off is the top settings layer
        query_settings = parse_query_parameters(query_parameters, spec)
        query_settings.pop("action", None)
        import_id = query_settings.pop("import_id", None)

        credentials = parse_request_headers(request_headers, CREDENTIAL_HEADERS)

        # Handle different actions
        if action == "start":
            try:
                config = load_import_settings(
                    influxdb3_local,
                    task_id,
                    args,
                    request_body,
                    request_headers,
                    query_settings,
                )
            except Exception as e:
                influxdb3_local.error(f"[{task_id}] Configuration error: {e}")
                return {"status": "error", "error": f"Configuration error: {e}"}

            # Start import
            return start_import(influxdb3_local, config, credentials, task_id)

        elif action == "status":
            if not import_id:
                return {"status": "error", "error": "import_id required"}
            return get_import_stats(influxdb3_local, import_id, task_id)

        elif action == "pause":
            if not import_id:
                return {"status": "error", "error": "import_id required"}
            return pause_import(influxdb3_local, import_id, task_id)

        elif action == "resume":
            if not import_id:
                return {"status": "error", "error": "import_id required"}

            return resume_import(
                influxdb3_local,
                import_id,
                credentials,
                task_id,
            )

        elif action == "cancel":
            if not import_id:
                return {"status": "error", "error": "import_id required"}
            return cancel_import(influxdb3_local, import_id, task_id)

        elif action == "test_connection":
            body_data = parse_json_body(request_body, SOURCE_KEYS)
            result = check_source_connection(body_data)
            if not result.get("success"):
                influxdb3_local.error(f"[{task_id}] test_connection failed: {result.get('message')}")
            return result

        elif action == "databases":
            body_data = parse_json_body(request_body, SOURCE_KEYS)
            result = get_source_databases_list(body_data, credentials)
            if result.get("error"):
                influxdb3_local.error(f"[{task_id}] databases failed: {result.get('error')}")
            return result

        elif action == "tables":
            body_data = parse_json_body(request_body, SOURCE_KEYS)
            result = get_source_tables_list(body_data, credentials)
            if result.get("error"):
                influxdb3_local.error(f"[{task_id}] tables failed: {result.get('error')}")
            return result

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to process request: {e}")
        return {"status": "error", "error": str(e)}
