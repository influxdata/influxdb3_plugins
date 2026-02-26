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
            "name": "source_token",
            "example": "my-token",
            "description": "Source InfluxDB authentication token.",
            "required": false
        },
        {
            "name": "source_username",
            "example": "admin",
            "description": "Source InfluxDB authentication username.",
            "required": false
        },
        {
            "name": "source_password",
            "example": "my-password",
            "description": "Source InfluxDB authentication password.",
            "required": false
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
            "description": "Delay between queries in milliseconds.",
            "required": false
        },
        {
            "name": "import_direction",
            "example": "oldest_first",
            "description": "Import direction: 'oldest_first' or 'newest_first'.",
            "required": false
        },
        {
            "name": "target_batch_size",
            "example": "2000",
            "description": "Target rows per query batch.",
            "required": false
        },
        {
            "name": "table_filter",
            "example": "cpu.mem.disk",
            "description": "Dot-separated list of specific tables to import (or all if not specified).",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "import_config.toml",
            "description": "Path to TOML config file to override args.",
            "required": false
        }
    ]
}
"""

import base64
import json
import os
import time
import tomllib
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Protocol,
    Tuple,
    runtime_checkable,
)

import requests

# Global HTTP session for connection pooling
_http_session = None

# Configuration constants
MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 1
MAX_BACKOFF_SECONDS = 16
REQUEST_TIMEOUT_SECONDS = 30

# Timestamp offset constants (for boundary adjustments)
MICROSECOND_OFFSET = 1


class ImportPauseState(Enum):
    """Possible states returned from querying the import_pause_state table."""

    NOT_FOUND = "not_found"
    CANCELLED = "cancelled"
    PAUSED = "paused"
    RUNNING = "running"


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
    source_token: Optional[str] = None
    source_username: Optional[str] = None
    source_password: Optional[str] = None
    dry_run: bool = False


"""
Helper for batching multiple line protocol builders into a single write.

The Rust API only requires an object with a ``build()`` method that returns a
string. By wrapping several builders into one object that returns a newline-
separated string, we can perform batched writes without changing the Rust
code. Works for both the default database (`write`) and a specific database
(`write_to_db`).
"""
@runtime_checkable
class _LineBuilderInterface(Protocol):
    def build(self) -> str: ...


class _BatchLines:
    def __init__(self, line_builders: Iterable[_LineBuilderInterface]):
        # Convert eagerly so repeated build() calls are stable.
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


def load_config(
    influxdb3_local,
    task_id: str,
    args: Optional[Dict[str, Any]] = None,
    body_args: Optional[Dict[str, Any]] = None,
) -> ImportConfig:
    """
    Load configuration from TOML file or args
    Priority: body_args > config file > args > environment variables
    """
    config_data = {}

    # 1. Start with environment variables (lowest priority)
    env_mappings = {
        "IMPORT_SOURCE_URL": "source_url",
        "IMPORT_SOURCE_TOKEN": "source_token",
        "IMPORT_SOURCE_USERNAME": "source_username",
        "IMPORT_SOURCE_PASSWORD": "source_password",
        "IMPORT_SOURCE_DATABASE": "source_database",
        "IMPORT_DEST_DATABASE": "dest_database",
        "IMPORT_START_TIMESTAMP": "start_timestamp",
        "IMPORT_END_TIMESTAMP": "end_timestamp",
    }

    for env_var, config_key in env_mappings.items():
        if env_var in os.environ:
            value = os.environ[env_var]
            config_data[config_key] = value

    # 2. Override with args
    if args:
        for key, value in args.items():
            if key != "config_file_path" and value is not None:
                config_data[key] = value

    # 3. Override with config file if specified
    if (args and "config_file_path" in args) or (
        body_args and "config_file_path" in body_args
    ):
        config_file_path = (
            args.get("config_file_path") if args else body_args.get("config_file_path")
        )
        try:
            plugin_dir_var: str | None = os.getenv("PLUGIN_DIR", None)
            if not plugin_dir_var:
                influxdb3_local.error(f"[{task_id}] Failed to get PLUGIN_DIR env var")
                raise Exception(f"[{task_id}] PLUGIN_DIR environment variable not set")

            plugin_dir: Path = Path(plugin_dir_var)
            config_file = plugin_dir / config_file_path

            if config_file.exists():
                with open(config_file, "rb") as f:
                    file_config = tomllib.load(f)
                # Override config_data with values from file
                config_data.update(file_config)
                influxdb3_local.info(
                    f"[{task_id}] Loaded configuration from {config_file}"
                )
        except Exception as e:
            influxdb3_local.error(f"[{task_id}] Failed to load config file: {e}")
            raise

    # 4. Override with body_args (highest priority)
    if body_args:
        for key, value in body_args.items():
            if value is not None:
                config_data[key] = value

    # Convert table_filter from dot-separated string to list if needed
    if "table_filter" in config_data and isinstance(config_data["table_filter"], str):
        config_data["table_filter"] = [
            t.strip() for t in config_data["table_filter"].split(".")
        ]

    # Validate authentication parameters
    has_username = config_data.get("source_username")
    has_password = config_data.get("source_password")
    has_token = config_data.get("source_token")

    # Check that either (username AND password together) OR (only token) is provided
    if has_username or has_password:
        # If username or password is provided, both must be provided
        if not (has_username and has_password):
            raise ValueError(
                "Authentication error: source_username and source_password must be provided together"
            )
        # If both username and password are provided, token should NOT be provided
        if has_token:
            raise ValueError(
                "Authentication error: Cannot use both (source_username/source_password) and source_token. "
                "Please provide either (source_username and source_password) OR (source_token only)"
            )
    elif not has_token:
        # Neither username/password nor token is provided
        raise ValueError(
            "Authentication error: Must provide either (source_username and source_password) OR (source_token)"
        )

    return ImportConfig(**config_data)


def get_http_session() -> requests.Session:
    global _http_session
    if _http_session is None:
        _http_session = requests.Session()
        _http_session.headers.update({"Connection": "keep-alive"})
    return _http_session


def query_source_influxdb(
    influxdb3_local,
    config: ImportConfig,
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

    # Determine authentication method based on InfluxDB version
    if config.influxdb_version == 1:
        # InfluxDB v1 authentication
        if config.source_username and config.source_password:
            # Basic Authentication with username:password
            credentials = f"{config.source_username}:{config.source_password}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            headers["Authorization"] = f"Basic {encoded_credentials}"
        elif config.source_token:
            # Bearer token authentication for v1
            headers["Authorization"] = f"Bearer {config.source_token}"
    elif config.influxdb_version == 2:
        # InfluxDB v2 authentication (requires token)
        if config.source_token:
            headers["Authorization"] = f"Token {config.source_token}"
        else:
            raise ValueError("InfluxDB v2 requires source_token for authentication")
    elif config.influxdb_version == 3:
        # InfluxDB v3 authentication (Bearer token)
        if config.source_token:
            headers["Authorization"] = f"Bearer {config.source_token}"
        else:
            raise ValueError("InfluxDB v3 requires source_token for authentication")

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
            return response.json()
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
    influxdb3_local, config: ImportConfig, task_id: str
) -> List[str]:
    """Get list of measurements (tables) from source database"""
    result = query_source_influxdb(
        influxdb3_local, config, "SHOW MEASUREMENTS", task_id
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
    influxdb3_local, config: ImportConfig, measurement: str, task_id: str
) -> Dict[str, str]:
    """Get field keys and their types for a measurement"""
    query = f'SHOW FIELD KEYS FROM "{measurement}"'
    result = query_source_influxdb(influxdb3_local, config, query, task_id)

    fields = {}
    if "results" in result and len(result["results"]) > 0:
        series = result["results"][0].get("series", [])
        if series and "values" in series[0]:
            for row in series[0]["values"]:
                field_name, field_type = row[0], row[1]
                fields[field_name] = field_type

    return fields


def get_tag_keys(
    influxdb3_local, config: ImportConfig, measurement: str, task_id: str
) -> List[str]:
    """Get tag keys for a measurement"""
    query = f'SHOW TAG KEYS FROM "{measurement}"'
    result = query_source_influxdb(influxdb3_local, config, query, task_id)

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


def estimate_import_time(
    influxdb3_local,
    config: ImportConfig,
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
                influxdb3_local, config, measurement, start_dt, end_dt, task_id
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

            if actual_start == actual_end:
                influxdb3_local.info(
                    f"[{task_id}] Single timestamp detected for '{measurement}': {actual_start}. Adding buffer to end time."
                )
                actual_end = actual_end + timedelta(microseconds=MICROSECOND_OFFSET)

            # Sample data to estimate row count
            # Use COUNT(*) for quick estimation
            count_query = f"""
            SELECT COUNT(*) FROM "{measurement}"
            WHERE time >= '{actual_start.isoformat()}' AND time <= '{actual_end.isoformat()}'
            """

            result = query_source_influxdb(
                influxdb3_local, config, count_query, task_id
            )

            row_count = 0
            if "results" in result and result["results"][0].get("series"):
                series = result["results"][0]["series"][0]
                if "values" in series and series["values"]:
                    row_count = series["values"][0][1]

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
    influxdb3_local, config: ImportConfig, task_id: str
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
        measurements = get_source_measurements(influxdb3_local, config, task_id)
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


def parse_timestamp(ts_str: str) -> datetime:
    """Parse various timestamp formats to datetime"""
    # Try RFC3339 format first
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
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
            dt = datetime.strptime(ts_str, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except:
            continue

    raise ValueError(f"Unable to parse timestamp: {ts_str}")


def find_actual_data_boundaries(
    influxdb3_local,
    config: ImportConfig,
    measurement: str,
    user_start: Optional[datetime],
    user_end: Optional[datetime],
    task_id: str,
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Find actual data boundaries within user-specified range.
    Returns: (actual_start, actual_end) or (None, None) if no data.

    Behavior:
    - If both user_start and user_end are None → use the entire dataset.
    - If only user_start is provided → find newest record from that time to the end.
    - If only user_end is provided → find oldest record from the beginning up to that time.
    - If both provided → restrict queries within that range.
    """
    # --- Build start query ---
    if user_start is None and user_end is None:
        start_query = f'SELECT * FROM "{measurement}" ORDER BY time ASC LIMIT 1'
    elif user_start is None:
        start_query = f"""
        SELECT * FROM "{measurement}"
        WHERE time <= '{user_end.isoformat()}'
        ORDER BY time ASC LIMIT 1
        """
    elif user_end is None:
        start_query = f"""
        SELECT * FROM "{measurement}"
        WHERE time >= '{user_start.isoformat()}'
        ORDER BY time ASC LIMIT 1
        """
    else:
        start_query = f"""
        SELECT * FROM "{measurement}"
        WHERE time >= '{user_start.isoformat()}' AND time <= '{user_end.isoformat()}'
        ORDER BY time ASC LIMIT 1
        """

    # --- Build end query ---
    if user_start is None and user_end is None:
        end_query = f'SELECT * FROM "{measurement}" ORDER BY time DESC LIMIT 1'
    elif user_start is None:
        end_query = f"""
        SELECT * FROM "{measurement}"
        WHERE time <= '{user_end.isoformat()}'
        ORDER BY time DESC LIMIT 1
        """
    elif user_end is None:
        end_query = f"""
        SELECT * FROM "{measurement}"
        WHERE time >= '{user_start.isoformat()}'
        ORDER BY time DESC LIMIT 1
        """
    else:
        end_query = f"""
        SELECT * FROM "{measurement}"
        WHERE time >= '{user_start.isoformat()}' AND time <= '{user_end.isoformat()}'
        ORDER BY time DESC LIMIT 1
        """

    actual_start = None
    actual_end = None

    try:
        # --- Query for actual_start ---
        result = query_source_influxdb(influxdb3_local, config, start_query, task_id)
        if "results" in result and result["results"][0].get("series"):
            series = result["results"][0]["series"][0]
            if "values" in series and series["values"]:
                time_col_idx = series["columns"].index("time")
                actual_start = datetime.fromisoformat(
                    series["values"][0][time_col_idx].replace("Z", "+00:00")
                )

        # --- Query for actual_end ---
        result = query_source_influxdb(influxdb3_local, config, end_query, task_id)
        if "results" in result and result["results"][0].get("series"):
            series = result["results"][0]["series"][0]
            if "values" in series and series["values"]:
                time_col_idx = series["columns"].index("time")
                actual_end = datetime.fromisoformat(
                    series["values"][0][time_col_idx].replace("Z", "+00:00")
                )
                # Add 1 ms to make the upper boundary inclusive
                actual_end = actual_end + timedelta(microseconds=MICROSECOND_OFFSET)

    except Exception as e:
        influxdb3_local.warn(
            f"[{task_id}] Error finding data boundaries for '{measurement}': {e}"
        )

    return actual_start, actual_end


def sample_data_density(
    influxdb3_local,
    config: ImportConfig,
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
            SELECT COUNT(*) FROM "{measurement}"
            WHERE time >= '{sample_start.isoformat()}'
            AND time < '{sample_end.isoformat()}'
            """

            try:
                result = query_source_influxdb(influxdb3_local, config, query, task_id)
                if "results" in result and result["results"][0].get("series"):
                    series = result["results"][0]["series"][0]
                    if "values" in series and series["values"]:
                        count = series["values"][0][1]
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


def write_field_to_builder(builder, field_name: str, value, field_type: str) -> bool:
    """
    Write a field to LineBuilder with the specified type
    Returns True if field was written successfully, False otherwise
    """
    try:
        # Sanitize field name to ensure compatibility with InfluxDB v3
        sanitized_field_name = sanitize_field_name(field_name)

        if field_type == "boolean":
            builder.bool_field(sanitized_field_name, bool(value))
        elif field_type == "integer":
            builder.int64_field(sanitized_field_name, int(value))
        elif field_type == "float":
            builder.float64_field(sanitized_field_name, float(value))
        elif field_type == "unsigned":
            builder.uint64_field(sanitized_field_name, int(value))
        elif field_type == "string":
            builder.string_field(sanitized_field_name, str(value))
        else:
            # Fallback to type inference for unknown types
            actual_type = get_actual_influx_type(value)
            return write_field_to_builder(builder, field_name, value, actual_type)
        return True
    except (ValueError, TypeError):
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
    tag_renames: Dict[str, str] = None,
    task_id: str = None,
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
    for row in values:
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
        # Write each LineBuilder object individually
        if database:
            influxdb3_local.write_sync_to_db(database, _BatchLines(line_builders), no_sync=True)
        else:
            influxdb3_local.write_sync(_BatchLines(line_builders), no_sync=True)
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
    source_token: str = None,
    source_username: str = None,
    source_password: str = None,
) -> Optional[ImportConfig]:
    """
    Load import configuration from database
    Returns ImportConfig or None if not found
    Authentication credentials must be provided as parameters since they're not stored in DB for security
    """
    try:
        query = f"""
        SELECT *
        FROM import_config
        WHERE import_id = '{import_id}'
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
            source_token=source_token,
            source_username=source_username,
            source_password=source_password,
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


def write_import_state(
    influxdb3_local,
    import_id: str,
    table_name: str,
    status: str,
    rows_imported: int,
    task_id: str,
    paused_at_time: Optional[str] = None,
    no_sync: bool = False,
) -> None:
    """
    Write import state to tracking table using LineBuilder

    Args:
        paused_at_time: ISO timestamp of data time where import was paused (only for 'paused' status)
        no_sync: If True, don't wait for WAL flush (faster but data may not be immediately queryable)
    """
    try:
        # Build LineBuilder for state tracking
        builder = LineBuilder("import_state")
        builder.tag("import_id", import_id)
        builder.tag("table_name", table_name)
        builder.string_field("status", status)
        builder.int64_field("rows_imported", rows_imported)

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


def check_pause_state(
    influxdb3_local,
    import_id: str,
    task_id: str,
) -> str:
    """
    Check if import is paused or canceled by querying the import_pause_state measurement

    Returns:
        'cancelled' if import is canceled
        'paused' if import is paused
        'running' if import is running normally
    """
    try:
        # Use database parameter in query
        query = f"""
        SELECT paused, canceled, time
        FROM 'import_pause_state'
        WHERE import_id = '{import_id}'
        ORDER BY time DESC
        LIMIT 1
        """

        result = influxdb3_local.query(query)

        # query() returns a list of dicts
        if result and len(result) > 0:
            # Get the first row (dict)
            row = result[0]

            # Check if import is canceled (higher priority than paused)
            canceled_value = row.get("canceled", False)
            if str(canceled_value).lower() == "true":
                influxdb3_local.info(
                    f"[{task_id}] Import {import_id} has been canceled"
                )
                return "cancelled"

            # Check if import is paused
            paused_value = row.get("paused", False)
            # Convert to bool in case it's an integer or string
            if str(paused_value).lower() == "true":
                return "paused"

        return "running"
    except Exception:
        # For errors, assume import is running
        return "running"


def import_table(
    influxdb3_local,
    config: ImportConfig,
    import_id: str,
    measurement: str,
    start_time: datetime,
    end_time: datetime,
    task_id: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Import a single table from source to destination
    Returns import statistics for this table

    Args:
        metadata: Optional metadata dict to update with schema issues
    """
    influxdb3_local.info(f"[{task_id}] Starting import for table: {measurement}")

    # Find actual data boundaries
    actual_start, actual_end = find_actual_data_boundaries(
        influxdb3_local, config, measurement, start_time, end_time, task_id
    )

    if not actual_start or not actual_end:
        influxdb3_local.info(
            f"[{task_id}] No data found in specified range for '{measurement}'"
        )
        return {
            "measurement": measurement,
            "status": "completed",
            "rows_imported": 0,
            "errors": [],
        }

    # If start and end are the same (single point), add a small buffer to ensure we capture it
    if actual_start == actual_end:
        influxdb3_local.info(
            f"[{task_id}] Single timestamp detected for '{measurement}': {actual_start}. Adding buffer to end time."
        )
        actual_end = actual_end + timedelta(microseconds=MICROSECOND_OFFSET)

    influxdb3_local.info(
        f"[{task_id}] Actual data range for '{measurement}': {actual_start} to {actual_end}"
    )

    # Calculate optimal batch window
    optimal_window_seconds = sample_data_density(
        influxdb3_local, config, measurement, actual_start, actual_end, task_id
    )

    # Get schema info for conflict detection
    fields = get_field_keys(influxdb3_local, config, measurement, task_id)
    tags = get_tag_keys(influxdb3_local, config, measurement, task_id)
    conflicts = check_tag_field_conflicts(tags, fields)

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

    rows_imported = 0
    errors = []

    # Import loop
    result = None
    while True:
        # Check for pause/cancel state
        pause_state = check_pause_state(
            influxdb3_local, import_id, task_id
        )

        if pause_state == "cancelled":
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
            )
            # Return immediately with cancelled status
            return {
                "measurement": measurement,
                "status": "cancelled",
                "rows_imported": rows_imported,
                "errors": errors,
                "cancelled_at_time": current_time.isoformat(),
            }
        elif pause_state == "paused":
            influxdb3_local.info(
                f"[{task_id}] Import paused by user for '{measurement}'"
            )

            paused_at_time = (
                current_time.isoformat()
            )  # default value if no valid timestamp found

            # Try to extract the maximum time value (nanoseconds) from the current series
            if result and "results" in result:
                series_list = result["results"][0].get("series", [])
                if series_list:
                    series = series_list[0]
                    columns = series.get("columns", [])
                    values = series.get("values", [])

                    if "time" in columns and values:
                        time_idx = columns.index("time")
                        try:
                            # Collect all valid timestamps from rows (expected as int nanoseconds)
                            times = [
                                row[time_idx]
                                for row in values
                                if row[time_idx] is not None
                            ]
                            if times:
                                # Find the maximum timestamp (nanoseconds)
                                max_time_ns = max(times)
                                # Convert nanoseconds to seconds and produce an aware UTC ISO8601 string
                                paused_at_time = datetime.fromtimestamp(
                                    max_time_ns / 1e9, tz=timezone.utc
                                ).isoformat()
                        except Exception as e:
                            influxdb3_local.warn(
                                f"[{task_id}] Failed to extract paused_at_time from series: {e}"
                            )

            write_import_state(
                influxdb3_local,
                import_id,
                measurement,
                "paused",
                rows_imported,
                task_id,
                paused_at_time,  # Save data time where we paused
                no_sync=True,
            )
            # Return immediately with paused status
            return {
                "measurement": measurement,
                "status": "paused",
                "rows_imported": rows_imported,
                "errors": errors,
                "paused_at_time": current_time.isoformat(),
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

        # Query data
        query = f"""
        SELECT * FROM "{measurement}"
        WHERE time >= '{window_start.isoformat()}' AND time <= '{window_end.isoformat()}'
        ORDER BY time {"ASC" if direction > 0 else "DESC"}
        """
        try:
            influxdb3_local.info(
                f"[{task_id}] Querying data for '{measurement}' from {window_start} to {window_end}"
            )
            result = query_source_influxdb(influxdb3_local, config, query, task_id)

            if "results" in result and result["results"][0].get("series"):
                series = result["results"][0]["series"][0]

                # Convert to line protocol with proper tag/field type information
                line_protocol = convert_influxql_to_line_protocol(
                    influxdb3_local, measurement, series, tags, fields, tag_renames
                )

                # Write to destination
                success, error = write_to_destination(
                    influxdb3_local, config.dest_database, line_protocol, task_id
                )

                if success:
                    rows_imported += len(line_protocol)

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
                        no_sync=True,
                    )
                else:
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
            errors.append(
                {"time_range": f"{window_start} to {window_end}", "error": str(e)}
            )
            # Continue with next window
            if direction > 0:
                current_time = window_end
            else:
                current_time = window_start

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
    )

    return {
        "measurement": measurement,
        "status": "completed",
        "rows_imported": rows_imported,
        "errors": errors,
    }


def resume_incomplete_import(
    influxdb3_local,
    config: ImportConfig,
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
    all_measurements = get_source_measurements(influxdb3_local, config, task_id)

    # Determine which tables still need import
    tables_to_resume = {}
    tables_to_restart = set()

    for table_info in incomplete_tables:
        table_name = table_info["table_name"]
        paused_at_time_str = table_info.get("paused_at_time", "")

        # If paused_at_time is empty (e.g., DB crashed during in_progress),
        # import from beginning
        if not paused_at_time_str or paused_at_time_str.strip() == "":
            influxdb3_local.warn(
                f"[{task_id}] Table '{table_name}' has status '{table_info.get('status')}' "
                f"but no paused_at_time (likely DB crash). Will restart from beginning."
            )
            tables_to_restart.add(table_name)
        else:
            # Valid paused_at_time - can resume from checkpoint
            tables_to_resume[table_name] = {
                "resume_from_timestamp": paused_at_time_str,
                "rows_imported": table_info.get("rows_imported", 0),
            }

    # Import remaining tables
    import_start = time.time()
    total_rows = 0
    completed_tables = 0
    all_errors = []

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
            resume_start = parse_timestamp(
                tables_to_resume[measurement]["resume_from_timestamp"]
            )
            # Add small offset to avoid re-importing the last record
            resume_start = resume_start + timedelta(microseconds=MICROSECOND_OFFSET)
            total_rows += tables_to_resume[measurement]["rows_imported"]

            table_result = import_table(
                influxdb3_local,
                config,
                import_id,
                measurement,
                resume_start,
                end_dt,
                task_id,
            )
        else:
            # Check if already completed
            try:
                check_query = f"""
                SELECT status
                FROM 'import_state'
                WHERE import_id = '{import_id}' AND table_name = '{measurement}'
                ORDER BY time DESC
                LIMIT 1
                """
                check_result = influxdb3_local.query(check_query)
                if check_result and check_result[0].get("status") == "completed":
                    influxdb3_local.info(
                        f"[{task_id}] Table {measurement} already completed, skipping"
                    )
                    completed_tables += 1
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
                import_id,
                measurement,
                start_dt,
                end_dt,
                task_id,
            )

        if table_result["status"] in ["completed"]:
            completed_tables += 1
            total_rows += table_result.get("rows_imported", 0)

        if "errors" in table_result:
            all_errors.extend(table_result["errors"])

        influxdb3_local.info(
            f"[{task_id}] Progress: {completed_tables}/{len(all_measurements)} tables completed"
        )

    import_duration = time.time() - import_start

    # Generate final report
    report = {
        "import_id": import_id,
        "status": "resumed_and_completed",
        "start_time": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": import_duration,
        "time_range": {"start": config.start_timestamp, "end": config.end_timestamp},
        "tables": {"total": len(all_measurements), "completed": completed_tables},
        "rows_imported": total_rows,
        "errors": len(all_errors),
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
    influxdb3_local.info(f"[{task_id}] Errors encountered: {len(all_errors)}")
    influxdb3_local.info(
        f"[{task_id}] ============================================================"
    )

    return report


def generate_import_plan(
    influxdb3_local,
    config: ImportConfig,
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
            fields = get_field_keys(influxdb3_local, config, measurement, task_id)
            tags = get_tag_keys(influxdb3_local, config, measurement, task_id)
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


def start_import(influxdb3_local, config: ImportConfig, task_id: str) -> Dict[str, Any]:
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

        # Create default pause state record (not paused, not canceled)
        try:
            pause_builder = LineBuilder("import_pause_state")
            pause_builder.tag("import_id", import_id)
            pause_builder.bool_field("paused", False)
            pause_builder.bool_field("canceled", False)
            pause_builder.time_ns(int(time.time() * 1_000_000_000))

            influxdb3_local.write_sync(pause_builder, no_sync=False)
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

    # Perform pre-flight checks
    success, errors, metadata = perform_preflight_checks(
        influxdb3_local, config, task_id
    )

    if not success:
        influxdb3_local.error(f"[{task_id}] Pre-flight checks failed:")
        for error in errors:
            influxdb3_local.error(f"[{task_id}]   - {error}")
        return {"import_id": import_id, "status": "failed", "errors": errors}

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
        influxdb3_local, config, measurements, start_dt, end_dt, task_id
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
        elif table_result["status"] == "cancelled":
            # Import was cancelled by user, stop immediately and return report
            total_rows += table_result.get("rows_imported", 0)
            influxdb3_local.info(
                f"[{task_id}] Import cancelled by user on table '{table_result['measurement']}'"
            )
            influxdb3_local.info(
                f"[{task_id}] Tables completed before cancellation: {completed_tables}/{total_tables}"
            )
            influxdb3_local.info(
                f"[{task_id}] Rows imported before cancellation: {total_rows}"
            )

            return {
                "import_id": import_id,
                "status": "cancelled",
                "cancelled_on_table": table_result["measurement"],
                "tables_completed": completed_tables,
                "total_tables": total_tables,
                "rows_imported": total_rows,
                "cancelled_at_time": table_result.get("cancelled_at_time"),
                "message": f"Import cancelled by user. Completed {completed_tables}/{total_tables} tables, {total_rows} rows imported.",
            }
        elif table_result["status"] == "paused":
            # Import was paused by user, stop immediately and return report
            total_rows += table_result.get("rows_imported", 0)
            influxdb3_local.info(
                f"[{task_id}] Import paused by user on table '{table_result['measurement']}'"
            )
            influxdb3_local.info(
                f"[{task_id}] Tables completed before pause: {completed_tables}/{total_tables}"
            )
            influxdb3_local.info(
                f"[{task_id}] Rows imported before pause: {total_rows}"
            )

            return {
                "import_id": import_id,
                "status": "paused",
                "paused_on_table": table_result["measurement"],
                "tables_completed": completed_tables,
                "total_tables": total_tables,
                "rows_imported": total_rows,
                "paused_at_time": table_result.get("paused_at_time"),
                "message": f"Import paused by user. Completed {completed_tables}/{total_tables} tables, {total_rows} rows imported.",
            }

        if "errors" in table_result:
            all_errors.extend(table_result["errors"])

        influxdb3_local.info(
            f"[{task_id}] Progress: {completed_tables}/{total_tables} tables completed"
        )

    import_duration = time.time() - import_start

    # Generate final report
    report = {
        "import_id": import_id,
        "status": "completed",
        "start_time": datetime.now(timezone.utc).isoformat(),
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
        PAUSED     - the import is paused
        RUNNING    - the import exists but is neither paused nor canceled
    """
    try:
        status_query = f"""
        SELECT paused, canceled
        FROM 'import_pause_state'
        WHERE import_id = '{import_id}'
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

        if pause_state == ImportPauseState.PAUSED:
            return {"status": "error", "error": f"Import {import_id} is already paused"}

        builder = LineBuilder("import_pause_state")
        builder.tag("import_id", import_id)
        builder.bool_field("paused", True)
        builder.bool_field("canceled", False)
        builder.time_ns(int(time.time() * 1_000_000_000))

        influxdb3_local.write_sync(builder, no_sync=False)
        influxdb3_local.info(f"[{task_id}] Import {import_id} paused")
        return {"status": "paused", "import_id": import_id}
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to pause import: {e}")
        return {"status": "error", "error": str(e)}


def resume_import(
    influxdb3_local,
    import_id: str,
    task_id: str,
    source_token: str = None,
    source_username: str = None,
    source_password: str = None,
) -> Dict[str, Any]:
    """
    Resume a paused or incomplete import
    Checks import status and continues from where it stopped
    Requires either source_token OR (source_username AND source_password)
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

        if pause_state == ImportPauseState.RUNNING:
            return {"status": "error", "error": f"Import {import_id} is already running"}

        # Check if import exists and is not completed
        status_query = f"""
        SELECT status, table_name
        FROM 'import_state'
        WHERE import_id = '{import_id}'
        ORDER BY time DESC
        LIMIT 100
        """
        status_result = influxdb3_local.query(status_query)

        if not status_result:
            return {"status": "error", "error": f"Import {import_id} not found"}

        # Check latest states to determine if import can be resumed
        latest_states = {}
        for row in status_result:
            table_name = row.get("table_name")
            if table_name not in latest_states:
                latest_states[table_name] = row.get("status")

        # Check if all tables are completed
        non_completed_tables = [
            table
            for table, status in latest_states.items()
            if table != "all" and status not in ["completed", "cancelled"]
        ]

        if not non_completed_tables:
            return {
                "status": "error",
                "error": f"Import {import_id} is already completed",
            }

        # Write resume state (unpause the import)
        builder = LineBuilder("import_pause_state")
        builder.tag("import_id", import_id)
        builder.bool_field("paused", False)
        builder.bool_field("canceled", False)
        influxdb3_local.write_sync(builder, no_sync=False)
        influxdb3_local.info(f"[{task_id}] Wrote resume state for import {import_id}")

        # 2. Load import configuration with provided credentials
        config = load_import_config(
            influxdb3_local,
            import_id,
            task_id,
            source_token,
            source_username,
            source_password,
        )
        if not config:
            return {
                "status": "error",
                "error": f"Import config not found for {import_id}. Cannot resume import.",
            }

        # 3. Find paused and in_progress tables for this specific import
        query = f"""
        SELECT import_id, table_name, status, rows_imported, time, paused_at_time
        FROM 'import_state'
        WHERE import_id = '{import_id}'
        ORDER BY time DESC
        """
        result = influxdb3_local.query(query)

        if not result:
            return {
                "status": "error",
                "error": f"No import state found for {import_id}",
            }

        # Get the latest status for each table
        latest_states = {}
        for row in result:
            table_name = row.get("table_name")
            if table_name not in latest_states:
                latest_states[table_name] = {
                    "table_name": table_name,
                    "status": row.get("status"),
                    "rows_imported": row.get("rows_imported", 0),
                    "paused_at_time": row.get("paused_at_time", ""),
                }

        # Find tables that need to be resumed (paused or in_progress)
        incomplete_tables = []
        for table_name, state in latest_states.items():
            # Skip special marker entries
            if table_name == "all":
                continue

            # Include paused and in_progress tables
            if state["status"] in ["paused", "in_progress"]:
                incomplete_tables.append(state)

        if not incomplete_tables:
            influxdb3_local.info(
                f"[{task_id}] No incomplete tables found for import {import_id}"
            )
            return {
                "status": "error",
                "error": f"No paused or incomplete tables found for import {import_id}",
            }

        influxdb3_local.info(
            f"[{task_id}] Found {incomplete_tables} incomplete tables to resume for import {import_id}: {incomplete_tables}"
        )

        # 4. Resume the import using resume_incomplete_import
        return resume_incomplete_import(
            influxdb3_local,
            config,
            import_id,
            incomplete_tables,
            task_id,
        )

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to resume import: {e}")
        return {"status": "error", "error": str(e)}


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
        # 1. Get all import state records
        state_query = f"""
        SELECT table_name, status, rows_imported, time, paused_at_time
        FROM 'import_state'
        WHERE import_id = '{import_id}'
        ORDER BY time DESC
        """
        state_result = influxdb3_local.query(state_query)

        if not state_result or len(state_result) == 0:
            return {
                "status": "not_found",
                "import_id": import_id,
                "error": "No import records found",
            }

        # 2. Get pause/cancel state
        pause_query = f"""
        SELECT paused, canceled, time
        FROM 'import_pause_state'
        WHERE import_id = '{import_id}'
        ORDER BY time DESC
        LIMIT 1
        """
        pause_result = influxdb3_local.query(pause_query)

        # 3. Get import config
        config_query = f"""
        SELECT *
        FROM 'import_config'
        WHERE import_id = '{import_id}'
        ORDER BY time DESC
        LIMIT 1
        """
        config_result = influxdb3_local.query(config_query)

        # Process state records - get latest state for each table
        latest_table_states = {}
        earliest_time = None
        latest_time = None

        for row in state_result:
            table_name = row.get("table_name")
            record_time = row.get("time")

            # Track time boundaries
            if earliest_time is None or record_time < earliest_time:
                earliest_time = record_time
            if latest_time is None or record_time > latest_time:
                latest_time = record_time

            # Keep only the latest state for each table
            if table_name not in latest_table_states:
                latest_table_states[table_name] = {
                    "table_name": table_name,
                    "status": row.get("status"),
                    "rows_imported": row.get("rows_imported", 0),
                    "last_update": record_time,
                    "paused_at_time": row.get("paused_at_time", ""),
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

        # Determine overall import status
        overall_status = "unknown"
        is_paused = False
        is_cancelled = False

        if pause_result and len(pause_result) > 0:
            pause_state = pause_result[0]
            is_cancelled = str(pause_state.get("canceled", False)).lower() == "true"
            is_paused = str(pause_state.get("paused", False)).lower() == "true"

        if (
            is_cancelled
            or "all" in latest_table_states
            and latest_table_states["all"]["status"] == "cancelled"
        ):
            overall_status = "cancelled"
        elif is_paused:
            overall_status = "paused"
        elif completed_tables == total_tables and total_tables > 0:
            overall_status = "completed"
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
                {"is_paused": is_paused, "is_cancelled": is_cancelled}
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

        # Write pause state with canceled flag using LineBuilder
        pause_builder = LineBuilder("import_pause_state")
        pause_builder.tag("import_id", import_id)
        pause_builder.bool_field("paused", True)
        pause_builder.bool_field("canceled", True)
        pause_builder.time_ns(int(time.time() * 1_000_000_000))
        influxdb3_local.write_sync(pause_builder, no_sync=False)

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
    source_url = body_data.get("source_url")
    if not source_url or not str(source_url).strip():
        return {"message": "source_url is required"}
    return None


def _validate_source_params(body_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Validate common source connection parameters.

    Args:
        body_data: Dict containing source_url, influxdb_version

    Returns:
        Error dict if validation fails, None if valid
    """
    source_url = body_data.get("source_url")
    influxdb_version = body_data.get("influxdb_version")

    if not source_url or not source_url.strip():
        return {"error": "source_url is required"}
    if influxdb_version is None:
        return {"error": "influxdb_version is required"}

    try:
        if int(influxdb_version) not in {1, 2, 3}:
            raise ValueError
    except (TypeError, ValueError):
        return {
            "error": f"Unsupported influxdb_version: {influxdb_version}. Must be 1, 2, or 3."
        }

    body_data["influxdb_version"] = int(influxdb_version)

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


def _build_v1_headers(
    username: str = None,
    password: str = None,
    token: str = None,
) -> Dict[str, str]:
    """Build headers for InfluxDB v1 API requests.

    Args:
        username: Optional username for Basic auth
        password: Optional password for Basic auth
        token: Optional token for Bearer auth

    Returns:
        Headers dict with Content-Type and optional Authorization
    """
    headers = {"Content-Type": "application/json"}
    if username and password:
        credentials = f"{username}:{password}"
        encoded = base64.b64encode(credentials.encode()).decode()
        headers["Authorization"] = f"Basic {encoded}"
    elif token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _build_v2_headers(
    token: str = None,
    extra_headers: Dict[str, str] = None,
) -> Dict[str, str]:
    """Build headers for InfluxDB v2 API requests.

    Args:
        token: Optional token for Token auth
        extra_headers: Optional additional headers to include

    Returns:
        Headers dict with optional Authorization and extra headers
    """
    headers = {}
    if extra_headers:
        headers.update(extra_headers)
    if token:
        headers["Authorization"] = f"Token {token}"
    return headers


def _build_v3_headers(token: str = None) -> Dict[str, str]:
    """Build headers for InfluxDB v3 API requests.

    Args:
        token: Optional token for Bearer auth

    Returns:
        Headers dict with Content-Type and optional Authorization
    """
    headers = {"Content-Type": "application/json"}
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
    session: requests.Session = None,
) -> Dict[str, Any]:
    """Get list of databases from source InfluxDB instance.

    Args:
        body_data: Dict containing source_url, influxdb_version, and auth credentials
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
    source_token = body_data.get("source_token")
    source_username = body_data.get("source_username")
    source_password = body_data.get("source_password")

    base_url = _parse_url_with_port_inference(source_url)

    try:
        if influxdb_version == 1:
            headers = _build_v1_headers(source_username, source_password, source_token)

            response = session.get(
                f"{base_url}/query",
                params={"q": "SHOW DATABASES"},
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

            databases = _parse_v1_series_values(response.json())
            databases = [db for db in databases if db not in ["_internal"]]
            return {"databases": sorted(databases)}

        elif influxdb_version == 2:
            headers = _build_v2_headers(source_token)

            response = session.get(
                f"{base_url}/api/v2/buckets",
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

            result = response.json()
            databases = []
            if "buckets" in result:
                databases = [bucket["name"] for bucket in result["buckets"]]

            databases = [db for db in databases if not db.startswith("_")]
            return {"databases": sorted(databases)}

        elif influxdb_version == 3:
            headers = _build_v3_headers(source_token)

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
    session: requests.Session = None,
) -> Dict[str, Any]:
    """Get list of tables/measurements from source database.

    Args:
        body_data: Dict containing source_url, influxdb_version, source_database, and auth credentials
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
    source_token = body_data.get("source_token")
    source_username = body_data.get("source_username")
    source_password = body_data.get("source_password")
    source_org = body_data.get("source_org")

    if not source_database:
        return {"error": "source_database is required"}

    base_url = _parse_url_with_port_inference(source_url)

    try:
        if influxdb_version == 1:
            headers = _build_v1_headers(source_username, source_password, source_token)

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
            if not source_org:
                return {"error": "source_org is required for InfluxDB v2"}

            headers = _build_v2_headers(
                source_token,
                extra_headers={
                    "Content-Type": "application/vnd.flux",
                    "Accept": "application/csv",
                }
            )

            escaped_bucket = source_database.replace('"', '\\"')
            flux_query = f'''import "influxdata/influxdb/schema" schema.measurements(bucket: "{escaped_bucket}")'''

            response = session.post(
                f"{base_url}/api/v2/query",
                params={"org": source_org},
                headers=headers,
                data=flux_query,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

            tables = []
            lines = response.text.strip().split("\n")
            for line in lines[1:]:
                if line.strip() and "," in line:
                    parts = line.split(",")
                    if len(parts) >= 4 and parts[3]:
                        tables.append(parts[3].strip())

            return {"tables": sorted(tables)}

        elif influxdb_version == 3:
            headers = _build_v3_headers(source_token)

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
    - POST /api/v3/import?action=start - Start new import
    - GET /api/v3/import?action=status&import_id=<id> - Get import status
    - POST /api/v3/import?action=pause&import_id=<id> - Pause import
    - POST /api/v3/import?action=resume&import_id=<id> - Resume import
    - POST /api/v3/import?action=cancel&import_id=<id> - Cancel import
    """
    task_id: str = str(uuid.uuid4())
    influxdb3_local.info(f"[{task_id}] Import plugin invoked")

    # Determine action from query parameters
    action = query_parameters.get("action", "start")
    import_id = query_parameters.get("import_id")

    try:
        # Handle different actions
        if action == "start":
            # Parse request body if JSON
            body_data: dict = json.loads(request_body) if request_body else {}

            try:
                config = load_config(influxdb3_local, task_id, args, body_data)
            except Exception as e:
                return {"status": "error", "error": f"Configuration error: {e}"}

            # Start import
            return start_import(influxdb3_local, config, task_id)

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

            # Get authentication credentials from query parameters or request body for resume
            # Parse request body if JSON
            body_data: dict = json.loads(request_body) if request_body else {}

            # Try to get credentials from body first, then from query parameters
            source_token = body_data.get("source_token") or query_parameters.get(
                "source_token"
            )
            source_username = body_data.get("source_username") or query_parameters.get(
                "source_username"
            )
            source_password = body_data.get("source_password") or query_parameters.get(
                "source_password"
            )

            # Validate that either (username AND password) OR (token) is provided
            if source_username or source_password:
                if not (source_username and source_password):
                    return {
                        "status": "error",
                        "error": "source_username and source_password must be provided together",
                    }
                if source_token:
                    return {
                        "status": "error",
                        "error": "Cannot use both (source_username/source_password) and source_token. "
                        "Please provide either (source_username and source_password) OR (source_token only)",
                    }
            elif not source_token:
                return {
                    "status": "error",
                    "error": "Must provide either (source_username and source_password) OR (source_token) for resume action",
                }

            return resume_import(
                influxdb3_local,
                import_id,
                task_id,
                source_token,
                source_username,
                source_password,
            )

        elif action == "cancel":
            if not import_id:
                return {"status": "error", "error": "import_id required"}
            return cancel_import(influxdb3_local, import_id, task_id)

        elif action == "test_connection":
            body_data = json.loads(request_body) if request_body else {}
            result = check_source_connection(body_data)
            if not result.get("success"):
                influxdb3_local.error(f"[{task_id}] test_connection failed: {result.get('message')}")
            return result

        elif action == "databases":
            body_data = json.loads(request_body) if request_body else {}
            result = get_source_databases_list(body_data)
            if result.get("error"):
                influxdb3_local.error(f"[{task_id}] databases failed: {result.get('error')}")
            return result

        elif action == "tables":
            body_data = json.loads(request_body) if request_body else {}
            result = get_source_tables_list(body_data)
            if result.get("error"):
                influxdb3_local.error(f"[{task_id}] tables failed: {result.get('error')}")
            return result

        else:
            return {
                "status": "error",
                "error": f"Unknown action: {action}",
                "available_actions": [
                    "start",
                    "status",
                    "pause",
                    "resume",
                    "cancel",
                    "test_connection",
                    "databases",
                    "tables",
                ],
            }

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to process request: {e}")
        return {"status": "error", "error": str(e)}
