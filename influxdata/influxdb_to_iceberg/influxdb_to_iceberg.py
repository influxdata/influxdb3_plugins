"""
{
    "plugin_type": ["scheduled", "http"],
    "scheduled_args_config": [
        {
            "name": "measurement",
            "example": "cpu",
            "description": "The InfluxDB measurement to query.",
            "required": true
        },
        {
            "name": "window",
            "example": "1h",
            "description": "Time window for data analysis (e.g., '1h' for 1 hour). Units: 's', 'min', 'h', 'd', 'w'.",
            "required": true
        },
        {
            "name": "catalog_configs",
            "example": "eyJ1cmkiOiAiaHR0cDovL25lc3NpZTo5MDAwIn0=",
            "description": "Base64-encoded JSON string containing Iceberg catalog configuration.",
            "required": true
        },
        {
            "name": "included_fields",
            "example": "usage_user.usage_idle",
            "description": "Dot-separated list of field names to include in the query (optional).",
            "required": false
        },
        {
            "name": "excluded_fields",
            "example": "usage_system",
            "description": "Dot-separated list of field names to exclude from the query (optional).",
            "required": false
        },
        {
            "name": "namespace",
            "example": "production",
            "description": "Iceberg namespace for the table (optional, default: 'default').",
            "required": false
        },
        {
            "name": "table_name",
            "example": "cpu_metrics",
            "description": "Iceberg table name (optional, default: same as measurement).",
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

import base64
import json
import os
import time
import tomllib
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    BooleanType,
    FloatType,
    IntegerType,
    NestedField,
    PrimitiveType,
    StringType,
    TimestampType,
)


def get_all_measurements(influxdb3_local) -> list[str]:
    """
    Retrieves a list of all tables of type 'BASE TABLE' from the current InfluxDB database.

    Args:
        influxdb3_local: InfluxDB client instance.

    Returns:
        list[str]: List of table names (e.g., ["cpu", "memory", "disk"]).
    """
    result: list = influxdb3_local.query("SHOW TABLES")
    return [
        row["table_name"] for row in result if row.get("table_type") == "BASE TABLE"
    ]


def parse_time_duration(raw: str, task_id: str) -> timedelta:
    """
    Convert a duration string (e.g., "5m", "2h", "1d") into a timedelta.

    Args:
        raw (str): Duration with unit suffix. Supported suffixes: s, min, h, d, w.
        task_id (str): Unique task identifier (for error messages).

    Returns:
        timedelta.

    Raises:
        Exception: if format is invalid or number conversion fails.
    """
    units: dict = {
        "s": "seconds",
        "min": "minutes",
        "h": "hours",
        "d": "days",
        "w": "weeks",
    }
    num_part, unit_part = "", ""
    for u in sorted(units.keys(), key=len, reverse=True):
        if raw.endswith(u):
            num_part = raw[: -len(u)]
            unit_part = u
            break
    if not num_part or unit_part not in units:
        raise Exception(f"[{task_id}] Invalid duration '{raw}'")
    try:
        val: int = int(num_part)
    except ValueError:
        raise Exception(f"[{task_id}] Invalid number in duration '{raw}'")
    return timedelta(**{units[unit_part]: val})


def parse_fields(args: dict, key: str, task_id: str) -> list[str]:
    """Splits a dot-separated string into a list of strings or use config file."""
    input_val: str | list | None = args.get(key, None)
    if not input_val:
        return []

    if args["use_config_file"]:
        if isinstance(input_val, list):
            return input_val
        else:
            raise Exception(f"[{task_id}] {key} must be a list when using config file")

    return input_val.split(".")


def parse_catalog_configs(args: dict, task_id: str) -> dict:
    """Decode and parse a base64-encoded JSON string or use config file."""
    input_params: str | dict = args["catalog_configs"]

    if args["use_config_file"]:
        if isinstance(input_params, dict):
            return input_params
        else:
            raise Exception(
                f"[{task_id}] catalog_configs must be a dict when using config file"
            )

    try:
        # Decode base64-encoded string
        decoded_bytes: bytes = base64.b64decode(input_params)
        decoded_str: str = decoded_bytes.decode("utf-8")
    except Exception:
        raise Exception(
            f"[{task_id}] Invalid base64 encoding in catalog_configs: {input_params}"
        )

    try:
        # Parse JSON from decoded string
        params: dict = json.loads(decoded_str)
    except json.JSONDecodeError:
        raise Exception(
            f"[{task_id}] Invalid JSON in decoded catalog_configs: {decoded_str}"
        )

    return params


def get_tag_names(influxdb3_local, measurement: str, task_id: str) -> list[str]:
    """
    Retrieves the list of tag names for a measurement.

    Args:
        influxdb3_local: InfluxDB client instance.
        measurement (str): Name of the measurement to query.
        task_id (str): Unique task identifier.

    Returns:
        list[str]: List of tag names with 'Dictionary(Int32, Utf8)' data type.
    """
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
    return tag_names


def get_fields_names(influxdb3_local, measurement: str, task_id: str) -> list[str]:
    """
    Retrieves the list of field names for a measurement.

    Args:
        influxdb3_local: InfluxDB client instance.
        measurement (str): Name of the measurement to query.
        task_id (str): Unique task identifier.

    Returns:
        list[str]: List of field names.
    """
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $measurement
        AND data_type != 'Dictionary(Int32, Utf8)'
    """
    res: list[dict] = influxdb3_local.query(query, {"measurement": measurement})

    if not res:
        raise Exception(
            f"[{task_id}] No fields found for measurement '{measurement}'."
        )

    field_names: list[str] = [field["column_name"] for field in res]
    return field_names


def generate_fields_string(tags: list[str], fields: list[str]) -> str:
    """
    Generates a formatted SELECT clause with tags and fields.

    Args:
        tags (list[str]): List of tag names.
        fields (list[str]): List of field names.

    Returns:
        str: Formatted string with quoted field names separated by commas and newlines.
    """
    all_fields: list = tags + fields
    return ",\n\t".join(f'"{field}"' for field in all_fields)


def generate_query(
    measurement: str,
    tag_names: list[str],
    field_names: list[str],
    start_time: datetime,
    end_time: datetime,
) -> str:
    """
    Builds an SQL query.

    Args:
        measurement: source measurement name
        tag_names: list of tags
        field_names: list of field names
        start_time: UTC datetime for WHERE time > ...
        end_time:   UTC datetime for WHERE time < ...

    Returns:
        A complete SQL query string.
    """
    # SELECT clause
    fields_clause: str = generate_fields_string(tag_names, field_names)

    query: str = f"""
            SELECT
                {fields_clause}
            FROM
                '{measurement}'
            WHERE
                time >= '{start_time}'
            AND 
                time < '{end_time}'
            ORDER BY time
        """
    return query


def pandas_dtype_to_iceberg_type(dtype) -> PrimitiveType:
    """Converts a Pandas dtype to an Iceberg type."""
    if pd.api.types.is_integer_dtype(dtype):
        return IntegerType()
    elif pd.api.types.is_float_dtype(dtype):
        return FloatType()
    elif pd.api.types.is_bool_dtype(dtype):
        return BooleanType()
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return TimestampType()
    elif pd.api.types.is_string_dtype(dtype):
        return StringType()
    else:
        raise TypeError(f"Unsupported dtype: {dtype}")


def df_to_iceberg_schema(df: pd.DataFrame) -> Schema:
    """Generates an Iceberg schema from a Pandas DataFrame."""
    fields: list = []
    for idx, (col_name, dtype) in enumerate(df.dtypes.items(), start=1):
        iceberg_type = pandas_dtype_to_iceberg_type(dtype)
        required: bool = not df[col_name].isnull().any()
        field: NestedField = NestedField(
            field_id=idx, name=col_name, field_type=iceberg_type, required=required
        )
        fields.append(field)
    return Schema(*fields)


def process_scheduled_call(
    influxdb3_local, call_time: datetime, args: dict | None = None
):
    """
    Main scheduled task for querying InfluxDB and appending data into an Iceberg table.

    Args:
        influxdb3_local: InfluxDB client instance.
        call_time (datetime): the timestamp when this task is run.
        args (dict): parameters for this call:
            - "measurement": str, InfluxDB measurement name (required).
            - "window": str, time window (e.g., "5m", "1h") (required).
            - "catalog_configs": str, base64-encoded JSON for load_catalog (required).
            - "included_fields": str, dot-separated field names to include (optional).
            - "excluded_fields": str, dot-separated field names to exclude (optional).
            - "namespace": str, Iceberg namespace (optional; default "default").
            - "table_name": str, Iceberg table name (optional; default = measurement).
            - "config_file_path": str, path to config file to override args (optional).

    Returns:
        None. All outcomes and errors are logged via influxdb3_local.

    Notes:
      - If the Iceberg table does not exist, it is created with a schema inferred from the DataFrame.
      - If the table exists but schema differs, append may fail; handling schema evolution is not implemented here.
      - Each append writes a new file in Iceberg (normal behavior).
    """
    task_id = str(uuid.uuid4())
    influxdb3_local.info(f"[{task_id}] Starting scheduled call with args: {args} and call_time: {call_time}")

    # Override args with config file if specified
    if args:
        if path := args.get("config_file_path", None):
            try:
                plugin_dir_var: str | None = os.getenv("PLUGIN_DIR", None)
                if not plugin_dir_var:
                    influxdb3_local.error(
                        f"[{task_id}] Failed to get PLUGIN_DIR env var"
                    )
                    return
                plugin_dir: Path = Path(plugin_dir_var)
                file_path = plugin_dir / path
                influxdb3_local.info(f"[{task_id}] Reading config file {file_path}")
                with open(file_path, "rb") as f:
                    args = tomllib.load(f)
                    args["use_config_file"] = True
                influxdb3_local.info(f"[{task_id}] New args content: {args}")
            except Exception:
                influxdb3_local.error(f"[{task_id}] Failed to read config file")
                return
        else:
            args["use_config_file"] = False

    # Validate required arguments
    required_keys: list = ["measurement", "window", "catalog_configs"]
    if not args or any(key not in args for key in required_keys):
        influxdb3_local.error(
            f"[{task_id}] Missing some of the required arguments: {', '.join(required_keys)}"
        )
        return

    measurement: str = args["measurement"]
    all_measurements: list = get_all_measurements(influxdb3_local)
    if measurement not in all_measurements:
        influxdb3_local.error(
            f"[{task_id}] Measurement '{measurement}' not found in database"
        )
        return

    try:
        # Parse config
        window: timedelta = parse_time_duration(args["window"], task_id)
        catalog_configs: dict = parse_catalog_configs(args, task_id)
        included_fields: list = parse_fields(args, "included_fields", task_id)
        excluded_fields: list = parse_fields(args, "excluded_fields", task_id)
        namespace: str = args.get("namespace", "default")
        table_name: str = args.get("table_name", measurement)
        full_table_name: str = f"{namespace}.{table_name}"
        influxdb3_local.info(f"[{task_id}] Target Iceberg table: {full_table_name}")

        # Determine time window
        end_time: datetime = call_time.replace(tzinfo=timezone.utc)
        start_time: datetime = end_time - window
        influxdb3_local.info(f"[{task_id}] Querying data from {start_time} to {end_time}")

        # Get data
        tags: list = get_tag_names(influxdb3_local, measurement, task_id)
        fields: list = get_fields_names(influxdb3_local, measurement, task_id)

        # Recognize fields to query
        if included_fields:
            fields_to_query: list = [
                field for field in fields if field in included_fields or field == "time"
            ]
        elif excluded_fields:
            fields_to_query = [
                field for field in fields if field not in excluded_fields
            ]
        else:
            fields_to_query = fields
        influxdb3_local.info(f"[{task_id}] Fields to query: {fields_to_query}")

        query: str = generate_query(
            measurement, tags, fields_to_query, start_time, end_time
        )
        results: list = influxdb3_local.query(query)
        if not results:
            influxdb3_local.info(
                f"[{task_id}] No data returned from {start_time} to {end_time}"
            )
            return
        influxdb3_local.info(f"[{task_id}] Retrieved {len(results)} records from {measurement}")

        # Convert to DataFrame and convert 'time' to datetime
        df: pd.DataFrame = pd.DataFrame.from_records(results)
        try:
            df["time"] = pd.to_datetime(
                df["time"], unit="ns"
            )  # Assuming 'time' column exists and is the timestamp
            influxdb3_local.info(
                f"[{task_id}] Successfully converted 'time' column to datetime."
            )
        except Exception as e:
            influxdb3_local.error(
                f"[{task_id}] Error while converting 'time' to datetime: {e}"
            )
            return
        df["time"] = df["time"].dt.tz_localize(None)
        df["time"] = df["time"].astype(
            "datetime64[us]"
        )  # Ensure time is microsecond datetime for Iceberg

        # Load catalog
        influxdb3_local.info(f"[{task_id}] Loading Iceberg catalog")
        try:
            catalog = load_catalog("iceberg", **catalog_configs)
            influxdb3_local.info(f"[{task_id}] Catalog loaded successfully.")
        except Exception as e:
            influxdb3_local.error(f"[{task_id}] Error while loading catalog: {e}")
            return

        # Create the namespace if it doesn't exist
        catalog.create_namespace_if_not_exists(namespace)

        # Create the table if it doesn't exist
        if not catalog.table_exists(full_table_name):
            schema: Schema = df_to_iceberg_schema(df)
            catalog.create_table(full_table_name, schema)
            influxdb3_local.info(f"[{task_id}] Table created successfully.")

        table: Table = catalog.load_table(full_table_name)
        pa_schema: pa.Schema = table.schema().as_arrow()
        result_arrows: pa.Table = pa.Table.from_pandas(df, schema=pa_schema)
        table.append(result_arrows)
        influxdb3_local.info(
            f"[{task_id}] Data appended to table successfully ({len(result_arrows)} rows)."
        )

    except Exception as e:
        influxdb3_local.error(f"Error: {e}")
        return


def parse_backfill_window(data: dict, task_id: str) -> tuple[datetime | None, datetime]:
    """
    Parses and validates the backfill window from input data.

    Extracts 'backfill_start' and 'backfill_end' values from the given dictionary,
    converts them from ISO 8601 strings to timezone-aware UTC datetimes, and
    ensures that the window is valid.

    If 'backfill_start' is not provided, returns (None, backfill_end), using the
    current UTC time as the end time if 'backfill_end' is also not provided.

    Args:
        data (dict): A dictionary expected to contain 'backfill_start' and/or 'backfill_end' keys.
        task_id (str): An identifier used for error context in exception messages.

    Returns:
        tuple[datetime | None, datetime]: A tuple containing the parsed start and end datetimes.
            The start can be None if not provided, but the end is always a datetime.

    Raises:
        Exception: If the provided datetime strings are invalid, lack timezone info,
                   or if the start is not earlier than the end.
    """

    def parse_iso_datetime(name: str, value: str) -> datetime:
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            raise Exception(
                f"[{task_id}] Invalid ISO 8601 datetime for {name}: '{value}'."
            )
        if dt.tzinfo is None:
            raise Exception(
                f"[{task_id}] {name} must include timezone info (e.g., '+00:00')."
            )
        return dt.astimezone(timezone.utc)

    start_str = data.get("backfill_start")
    end_str = data.get("backfill_end")

    if end_str:
        backfill_end: datetime = parse_iso_datetime("backfill_end", end_str)
    else:
        backfill_end = datetime.now(timezone.utc)

    if start_str is None:
        return None, backfill_end

    backfill_start = parse_iso_datetime("backfill_start", start_str)

    if backfill_start >= backfill_end:
        raise Exception(
            f"[{task_id}] backfill_start must be earlier than backfill_end."
        )

    return backfill_start, backfill_end


def process_request(
    influxdb3_local, query_parameters, request_headers, request_body, args=None
):
    """
    Process a data replication request from InfluxDB to Iceberg catalog.

    Parses and validates input JSON, retrieves measurement data in batches,
    converts timestamps, manages catalog and table creation, appends data
    in batches, and logs progress and errors.

    Args:
        influxdb3_local: InfluxDB client instance with logging methods.
        query_parameters: Query parameters from the request (unused internally).
        request_headers: Request headers (unused internally).
        request_body: JSON string containing replication parameters with the following structure:
            {
                "measurement": str,               # Required. Name of the measurement to replicate.
                "catalog_configs": dict,          # Required. Configuration for catalog loading, e.g.:
                                                 #   {
                                                 #       "type": "sql",
                                                 #       "uri": "...",
                                                 #       "warehouse": "...",
                                                 #       "s3.endpoint": "...",
                                                 #       "s3.region": "...",
                                                 #       "s3.path-style-access": "true",
                                                 #       "s3.access-key-id": "...",
                                                 #       "s3.secret-access-key": "...",
                                                 #       "supportedAPIVersion": "2"
                                                 #   }
                "included_fields": list[str],     # Optional. List of field names to include in replication.
                "excluded_fields": list[str],     # Optional. List of field names to exclude from replication.
                "namespace": str,                 # Optional. Target namespace for the Iceberg catalog (default: "default").
                "table_name": str,                # Optional. Target table name in the Iceberg catalog (default: measurement name).
                "batch_size": str,                # Optional. Batch size duration for processing, e.g. "1d", "12h" (default: "1d").
                "backfill_start": str,            # Optional. ISO 8601 datetime string with timezone for start of backfill window.
                "backfill_end": str               # Optional. ISO 8601 datetime string with timezone for end of backfill window.
            }
        args: Optional additional arguments.

    Returns:
        dict: Result message with success or error information.
    """
    task_id = str(uuid.uuid4())
    influxdb3_local.info(f"[{task_id}] Received request for data replication to Iceberg.")

    if request_body:
        data: dict = json.loads(request_body)
    else:
        influxdb3_local.error(f"[{task_id}] No request body provided.")
        return {"message": f"[{task_id}] Error: No request body provided."}

    # Validate required arguments
    required_keys: list = ["measurement", "catalog_configs"]
    if any(key not in data for key in required_keys):
        influxdb3_local.error(
            f"[{task_id}] Missing some of the required arguments: {', '.join(required_keys)}"
        )
        return {
            "message": f"[{task_id}] Missing some of the required arguments: {', '.join(required_keys)}"
        }

    measurement: str = data["measurement"]
    all_measurements: list = get_all_measurements(influxdb3_local)
    if measurement not in all_measurements:
        influxdb3_local.error(
            f"[{task_id}] Measurement '{measurement}' not found in database"
        )
        return {
            "message": f"[{task_id}] Measurement '{measurement}' not found in database"
        }

    try:
        influxdb3_local.info(f"[{task_id}] Starting data replication process.")
        start_process_time: float = time.time()

        catalog_configs: dict = data["catalog_configs"]
        influxdb3_local.info(f"[{task_id}] Catalog configs received: {catalog_configs}")
        included_fields: list | None = data.get("included_fields", None)
        excluded_fields: list = data.get("excluded_fields", [])
        namespace: str = data.get("namespace", "default")
        table_name: str = data.get("table_name", measurement)
        full_table_name: str = f"{namespace}.{table_name}"
        influxdb3_local.info(f"[{task_id}] Target Iceberg table: {full_table_name}")

        # Get data
        tags: list = get_tag_names(influxdb3_local, measurement, task_id)
        fields: list = get_fields_names(influxdb3_local, measurement, task_id)

        # Recognize fields to query
        if included_fields:
            fields_to_query: list = [
                field for field in fields if field in included_fields or field == "time"
            ]
        elif excluded_fields:
            fields_to_query = [
                field for field in fields if field not in excluded_fields
            ]
        else:
            fields_to_query = fields
        influxdb3_local.info(f"[{task_id}] Fields to query: {fields_to_query}")

        batch_size: timedelta = parse_time_duration(
            data.get("batch_size", "1d"), task_id
        )
        backfill_start, backfill_end = parse_backfill_window(data, task_id)

        if backfill_start is None:
            q: str = f"SELECT MIN(time) as _t FROM {measurement}"
            res: list = influxdb3_local.query(q)
            oldest: int = res[0].get("_t")
            backfill_start: datetime = datetime.fromtimestamp(
                oldest / 1e9, tz=timezone.utc
            )

        influxdb3_local.info(
            f"[{task_id}] Data will be replicated from {backfill_start} to {backfill_end}."
        )

        # Load catalog
        try:
            influxdb3_local.info(f"[{task_id}] Loading Iceberg catalog...")
            catalog = load_catalog("iceberg", **catalog_configs)
            influxdb3_local.info(f"[{task_id}] Catalog loaded successfully.")
        except Exception as e:
            influxdb3_local.error(f"[{task_id}] Error while loading catalog: {e}")
            return {"message": f"[{task_id}] Error while loading catalog: {e}"}

        # Create the namespace if it doesn't exist
        catalog.create_namespace_if_not_exists(namespace)

        # Process data
        cursor: datetime = backfill_start
        total_source_records: int = 0
        total_written_records: int = 0
        batch_count: int = 0
        is_first_valid_batch: bool = True
        table: Table | None = None
        pa_schema: pa.Schema | None = None

        while cursor < backfill_end:
            batch_count += 1
            batch_end = min(cursor + batch_size, backfill_end)

            query: str = generate_query(
                measurement,
                tags,
                fields_to_query,
                cursor,
                batch_end,
            )

            batch_data: list = influxdb3_local.query(query)
            batch_source_count = len(batch_data)
            total_source_records += batch_source_count

            # Log batch source data metrics
            source_columns = (
                list(batch_data[0].keys()) if batch_source_count > 0 else []
            )
            batch_source_log: dict = {
                "batch": batch_count,
                "time_range": f"{cursor.isoformat()} to {batch_end.isoformat()}",
                "source_records": batch_source_count,
                "source_columns": source_columns[
                    :10
                ],  # Limit to first 10 columns to avoid huge logs
                "source_measurement": measurement,
            }
            influxdb3_local.info(
                f"[{task_id}] Batch source data retrieved", batch_source_log
            )
            if batch_source_count == 0:
                influxdb3_local.info(
                    f"[{task_id}] No data in batch {batch_count}, skipping"
                )
                cursor = batch_end
                continue

            success: bool = False
            error: str | None = None

            # Transform and replicate data
            try:
                # Convert to DataFrame and convert 'time' to datetime
                batch_df: pd.DataFrame = pd.DataFrame.from_records(batch_data)
                batch_df["time"] = pd.to_datetime(
                    batch_df["time"], unit="ns"
                )  # Assuming 'time' column exists and is the timestamp
                influxdb3_local.info(
                    f"[{task_id}] Successfully converted 'time' column to datetime on batch {batch_count}."
                )

                batch_df["time"] = batch_df["time"].dt.tz_localize(None)
                batch_df["time"] = batch_df["time"].astype(
                    "datetime64[us]"
                )  # Ensure time is microsecond datetime for Iceberg

                if is_first_valid_batch:
                    # Create the table if it doesn't exist
                    if not catalog.table_exists(full_table_name):
                        schema: Schema = df_to_iceberg_schema(batch_df)
                        catalog.create_table(full_table_name, schema)
                        influxdb3_local.info(
                            f"[{task_id}] Table {full_table_name} created successfully."
                        )

                    table = catalog.load_table(full_table_name)
                    pa_schema = table.schema().as_arrow()
                    is_first_valid_batch = False

                result_arrows: pa.Table = pa.Table.from_pandas(
                    batch_df, schema=pa_schema
                )
                table.append(result_arrows)
                success = True
                total_written_records += batch_source_count
                influxdb3_local.info(
                    f"[{task_id}] Data from {cursor.isoformat()} to {batch_end.isoformat()} on batch {batch_count} appended to table {full_table_name} successfully ({batch_source_count} rows)."
                )
            except Exception as e:
                error = str(e)
                influxdb3_local.error(
                    f"[{task_id}] Error while appending data from {cursor.isoformat()} to {batch_end.isoformat()} on batch {batch_count} to table {full_table_name}: {e}"
                )

            cursor = batch_end

            batch_result_log = {
                "batch": batch_count,
                "success": success,
                "source_records": batch_source_count,
                "written_records": batch_source_count if success else 0,
                "error": error or None,
            }
            influxdb3_local.info(f"[{task_id}] Batch processed", batch_result_log)

        duration: float = time.time() - start_process_time

        # Final summary log
        final_summary: dict = {
            "total_batches": batch_count,
            "execution_time_seconds": round(duration, 2),
            "total_source_records": total_source_records,
            "total_written_records": total_written_records,
            "source_measurement": measurement,
            "target_table": full_table_name,
            "time_range": f"{backfill_start.isoformat()} to {backfill_end.isoformat()}",
        }
        influxdb3_local.info(
            f"[{task_id}] Replication to Iceberg completed", final_summary
        )

        return {
            "message": f"[{task_id}] Replication to Iceberg completed with summary: {final_summary}"
        }

    except Exception as e:
        influxdb3_local.error(str(e))
        return {"message": str(e)}
