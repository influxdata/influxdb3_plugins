"""
{
    "plugin_type": ["http"],
    "http_args_config": [
        {
            "name": "measurement",
            "example": "temperature",
            "description": "InfluxDB measurement name to read from",
            "required": true
        },
        {
            "name": "field",
            "example": "value",
            "description": "Field name containing the time series values",
            "required": false
        },
        {
            "name": "tags",
            "example": "location=NYC,device=sensor1",
            "description": "Tag filters (e.g., 'location=NYC,device=sensor1')",
            "required": false
        },
        {
            "name": "time_range",
            "example": "30d",
            "description": "Time range to query (e.g., '30d' for last 30 days)",
            "required": false
        },
        {
            "name": "forecast_horizon",
            "example": "7d",
            "description": "Forecast horizon (e.g., '7d' for 7 days, '30 points' for 30 points)",
            "required": false
        },
        {
            "name": "model",
            "example": "sfm-tabular",
            "description": "Synthefy model to use (e.g., 'sfm-tabular', 'Migas-latest'). See README for supported models.",
            "required": false
        },
        {
            "name": "api_key",
            "example": "your-synthefy-api-key-here",
            "description": "Synthefy API key",
            "required": true
        },
        {
            "name": "output_measurement",
            "example": "temperature_forecast",
            "description": "Output measurement name (default: '{measurement}_forecast')",
            "required": false
        },
        {
            "name": "metadata_fields",
            "example": "humidity,pressure",
            "description": "Comma-separated list of metadata field names",
            "required": false
        },
        {
            "name": "database",
            "example": "mydb",
            "description": "Database name for reading and writing data",
            "required": false
        }
    ]
}
"""

import json
import logging
import os
import random
import time
from datetime import timedelta
from typing import Any, Dict, List, Optional

try:
    import pandas as pd
except ImportError as e:
    raise ImportError(
        f"Required dependencies not installed: {e}. Please install: pandas"
    )

try:
    import httpx
except ImportError:
    try:
        import requests as httpx
    except ImportError:
        raise ImportError(
            "Either 'httpx' or 'requests' must be installed for API calls"
        )

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Note: LineBuilder is provided by the InfluxDB 3 plugin framework
# It's injected into the plugin's namespace at runtime, so no import is needed.
# LineBuilder is used to construct line protocol data for writing to InfluxDB.

# Synthefy Forecasting API base URL (hardcoded)
SYNTHEFY_API_BASE_URL = "https://forecast.synthefy.com"


def _parse_args(args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse and validate plugin arguments.

    Extracts and validates configuration arguments from the plugin trigger.
    Sets default values for optional parameters and ensures required parameters
    are present.

    Args:
        args (Dict[str, Any]): Dictionary containing plugin configuration arguments.
            Expected keys:
            - measurement (str, required): InfluxDB measurement name to read from
            - field (str, optional): Field name containing time series values (default: "value")
            - tags (str, optional): Tag filters as comma-separated key=value pairs (default: "")
            - time_range (str, optional): Time range to query, e.g., "30d" (default: "30d")
            - forecast_horizon (str, optional): Forecast horizon, e.g., "7d" or "30 points" (default: "7d")
            - model (str, optional): Synthefy model identifier (default: "sfm-tabular")
            - api_key (str, required): Synthefy API key (can also be set via SYNTHEFY_API_KEY env var)
            - output_measurement (str, optional): Output measurement name (default: "{measurement}_forecast")
            - metadata_fields (str, optional): Comma-separated list of metadata field names (default: "")

    Returns:
        Dict[str, Any]: Parsed and validated arguments dictionary with all keys present.
            Includes default values for optional parameters. The output_measurement will be
            automatically set to "{measurement}_forecast" if not provided.

    Raises:
        ValueError: If 'measurement' argument is missing or empty.
        ValueError: If 'api_key' argument is missing and SYNTHEFY_API_KEY environment
            variable is not set.
    """
    parsed = {
        "measurement": args.get("measurement"),
        "field": args.get("field", "value"),
        "tags": args.get("tags", ""),
        "time_range": args.get("time_range", "30d"),
        "forecast_horizon": args.get("forecast_horizon", "7d"),
        "model": args.get("model", "sfm-tabular"),
        "api_key": args.get("api_key") or os.getenv("SYNTHEFY_API_KEY"),
        "output_measurement": args.get("output_measurement"),
        "metadata_fields": args.get("metadata_fields", ""),
    }

    if not parsed["measurement"]:
        raise ValueError("'measurement' argument is required")
    if not parsed["api_key"]:
        raise ValueError(
            "'api_key' argument is required or set SYNTHEFY_API_KEY environment variable"
        )

    if not parsed["output_measurement"]:
        parsed["output_measurement"] = f"{parsed['measurement']}_forecast"

    return parsed


def _parse_tags(tags_str: str) -> Dict[str, str]:
    """
    Parse tag filter string into dictionary.

    Converts a comma-separated string of key=value pairs into a dictionary
    suitable for use in SQL WHERE clauses.

    Args:
        tags_str (str): Comma-separated tag filters in the format "key1=value1,key2=value2".
            Whitespace around keys and values is automatically stripped.
            Empty string or None returns an empty dictionary.

    Returns:
        Dict[str, str]: Dictionary mapping tag keys to tag values. Empty dictionary
            if tags_str is empty or None. Only pairs containing "=" are parsed;
            malformed pairs are silently skipped.

    Example:
        >>> _parse_tags("location=NYC,device=sensor1")
        {'location': 'NYC', 'device': 'sensor1'}
        >>> _parse_tags("")
        {}
    """
    if not tags_str:
        return {}
    tags = {}
    for pair in tags_str.split(","):
        if "=" in pair:
            key, value = pair.split("=", 1)
            tags[key.strip()] = value.strip()
    return tags


def _build_query(
    measurement: str,
    field: str,
    tags: Dict[str, str],
    time_range: str,
    metadata_fields: List[str],
) -> str:
    """
    Build SQL query to extract time series data from InfluxDB.

    Constructs a SQL SELECT query that retrieves historical time series data
    with optional tag filtering and metadata fields.

    Args:
        measurement (str): InfluxDB measurement/table name to query from.
        field (str): Field name containing the time series values to forecast.
        tags (Dict[str, str]): Dictionary of tag key-value pairs for filtering.
            Tags are added as WHERE conditions only. Tags are metadata in InfluxDB 3
            and are automatically included in query results, so they should not be
            explicitly selected.
        time_range (str): Time range for the query in InfluxDB interval format
            (e.g., "30d", "7d", "1h"). Used in WHERE clause as:
            `time >= now() - INTERVAL '{time_range}'`.
        metadata_fields (List[str]): List of additional field names to include
            in the SELECT clause. These are used as covariates in forecasting.

    Returns:
        str: SQL query string that:
            - SELECTs: time, field, and metadata_fields
            - FROM: the specified measurement
            - WHERE: time >= now() - INTERVAL '{time_range}' AND tag conditions (if tags provided)
            - ORDER BY: time (ascending)
            Note: Tags are used for filtering in WHERE clause but not selected (they're metadata)

    Example:
        >>> query = _build_query("temperature", "value", {"location": "NYC"}, "7d", ["humidity"])
        >>> "SELECT time, value, humidity" in query
        True
        >>> "location = 'NYC'" in query
        True
        >>> "FROM temperature" in query
        True
    """
    # Build SELECT clause
    # Note: Tags are NOT columns in InfluxDB 3 - they're metadata used for filtering only
    # Tags are automatically included in query results, so we don't SELECT them
    select_fields = ["time", field]
    if metadata_fields:
        select_fields.extend(metadata_fields)

    select_clause = ", ".join(select_fields)

    # Build WHERE clause
    where_parts = [f"time >= now() - INTERVAL '{time_range}'"]

    if tags:
        tag_conditions = [f"{k} = '{v}'" for k, v in tags.items()]
        where_parts.extend(tag_conditions)

    where_clause = " AND ".join(where_parts)

    query = f"""
    SELECT {select_clause}
    FROM {measurement}
    WHERE {where_clause}
    ORDER BY time
    """

    return query


def _dataframe_to_synthefy_request(
    df: pd.DataFrame,
    field: str,
    forecast_horizon: str,
    metadata_fields: List[str],
    model: str,
) -> Dict[str, Any]:
    """
    Transform InfluxDB DataFrame to Synthefy ForecastV2Request format.

    Converts a pandas DataFrame containing time series data from InfluxDB into
    the request format expected by the Synthefy Forecasting API v2. Handles both
    univariate (single field) and multivariate (with metadata fields) forecasting.

    The function:
    - Extracts historical timestamps and values for the target field
    - Generates target timestamps based on forecast_horizon
    - Creates metadata samples for additional fields (covariates)
    - Builds the request structure matching ForecastV2Request model

    Args:
        df (pd.DataFrame): DataFrame containing time series data from InfluxDB.
            Must include a 'time' column and the specified field column.
            May include additional columns for metadata fields.
        field (str): Name of the field/column to forecast (the target variable).
        forecast_horizon (str): Duration for forecast horizon. Supported formats:
            - "Nd" for N days (e.g., "7d" for 7 days)
            - "Nh" for N hours (e.g., "24h" for 24 hours)
            - "N points" for N data points (e.g., "30 points")
            Defaults to 7 days if format is unrecognized.
        metadata_fields (List[str]): List of field names to use as covariates.
            These fields will be included as metadata samples (not forecasted,
            but used to improve forecast accuracy). Empty list for univariate.
        model (str): Synthefy model identifier to use (e.g., "sfm-tabular", "Migas-latest").

    Returns:
        Dict[str, Any]: Request dictionary matching ForecastV2Request format with:
            - "samples": List containing one sample row with:
                - Main forecast sample (forecast=True, metadata=False) for the target field
                - Metadata samples (forecast=False, metadata=True) for each metadata field
            - "model": Model identifier string
            Each sample contains:
                - "sample_id": Field name
                - "history_timestamps": List of ISO 8601 timestamp strings
                - "history_values": List of historical values (None for missing)
                - "target_timestamps": List of ISO 8601 timestamp strings for forecast period
                - "target_values": List of None (to be filled by API)
                - "forecast": Boolean indicating if this sample should be forecasted
                - "metadata": Boolean indicating if this is a covariate
                - "leak_target": Boolean (always False)
                - "column_name": Field name

    Raises:
        ValueError: If DataFrame is empty or missing required 'time' column.

    Example:
        >>> df = pd.DataFrame({
        ...     'time': pd.date_range('2024-01-01', periods=100, freq='H'),
        ...     'temperature': [20 + i*0.1 for i in range(100)],
        ...     'humidity': [50 + i*0.05 for i in range(100)]
        ... })
        >>> request = _dataframe_to_synthefy_request(
        ...     df, 'temperature', '7d', ['humidity'], 'sfm-tabular'
        ... )
        >>> 'samples' in request
        True
        >>> 'model' in request
        True
    """
    if df.empty:
        raise ValueError("No data found in query result")

    # Ensure time column is datetime
    if "time" not in df.columns:
        raise ValueError("Query result must include 'time' column")
    df["time"] = pd.to_datetime(df["time"])

    # Sort by time
    df = df.sort_values("time").reset_index(drop=True)

    # Extract history data
    history_timestamps = df["time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ").tolist()
    # Convert NaN to None - pandas fillna doesn't accept None as value in newer versions
    history_values = [None if pd.isna(val) else val for val in df[field].tolist()]

    # Calculate forecast horizon
    if forecast_horizon.endswith("d"):
        days = int(forecast_horizon[:-1])
        forecast_timedelta = timedelta(days=days)
    elif forecast_horizon.endswith("h"):
        hours = int(forecast_horizon[:-1])
        forecast_timedelta = timedelta(hours=hours)
    elif forecast_horizon.endswith(" points"):
        num_points = int(forecast_horizon.replace(" points", ""))
        # Estimate time delta from last two points
        if len(df) >= 2:
            time_delta = df["time"].iloc[-1] - df["time"].iloc[-2]
            forecast_timedelta = time_delta * num_points
        else:
            forecast_timedelta = timedelta(days=7)  # Default
    else:
        # Default to 7 days
        forecast_timedelta = timedelta(days=7)

    # Generate target timestamps
    last_timestamp = df["time"].iloc[-1]
    target_timestamps = []

    # Calculate time step from data
    if len(df) >= 2:
        time_step = df["time"].iloc[-1] - df["time"].iloc[-2]
        num_points = max(1, int(forecast_timedelta / time_step))
    else:
        # Default to hourly if only one point
        time_step = timedelta(hours=1)
        num_points = max(1, int(forecast_timedelta / time_step))

    current_time = last_timestamp + time_step
    for _ in range(num_points):
        target_timestamps.append(current_time.strftime("%Y-%m-%dT%H:%M:%SZ"))
        current_time += time_step

    target_values = [None] * len(target_timestamps)

    # Build samples
    samples = []

    # Main forecast sample
    forecast_sample = {
        "sample_id": field,
        "history_timestamps": history_timestamps,
        "history_values": history_values,
        "target_timestamps": target_timestamps,
        "target_values": target_values,
        "forecast": True,
        "metadata": False,
        "leak_target": False,
        "column_name": field,
    }

    # Metadata samples
    metadata_samples = []
    for metadata_field in metadata_fields:
        if metadata_field in df.columns:
            metadata_sample = {
                "sample_id": metadata_field,
                "history_timestamps": history_timestamps,
                # Convert NaN to None - pandas fillna doesn't accept None as value in newer versions
                "history_values": [
                    None if pd.isna(val) else val for val in df[metadata_field].tolist()
                ],
                "target_timestamps": target_timestamps,
                "target_values": [None] * len(target_timestamps),
                "forecast": False,
                "metadata": True,
                "leak_target": False,
                "column_name": metadata_field,
            }
            metadata_samples.append(metadata_sample)

    # Combine all samples
    sample_row = [forecast_sample] + metadata_samples
    samples.append(sample_row)

    request = {"samples": samples, "model": model}

    return request


def _call_synthefy_api(request_data: Dict[str, Any], api_key: str) -> Dict[str, Any]:
    """
    Call Synthefy Forecasting API v2 endpoint.

    Sends a POST request to the Synthefy Forecasting API with the forecast request
    data and returns the forecast response. Handles both httpx and requests libraries.

    Args:
        request_data (Dict[str, Any]): Forecast request dictionary matching
            ForecastV2Request format. Must contain:
            - "samples": List of sample rows (each row contains forecast and metadata samples)
            - "model": Model identifier string
        api_key (str): Synthefy API key for authentication. Sent in X-API-Key header.

    Returns:
        Dict[str, Any]: Forecast response dictionary matching ForecastV2Response format.
            Contains:
            - "forecasts": List of forecast rows, where each row contains
              SingleSampleForecastPayload objects with timestamps, values, and quantiles

    Raises:
        Exception: If the HTTP request fails (network error, timeout, etc.).
        HTTPError: If the API returns a non-2xx status code (raised by raise_for_status()).
        JSONDecodeError: If the response body cannot be parsed as JSON.

    Note:
        - Uses endpoint: https://forecast.synthefy.com/v2/forecast
        - Request timeout is set to 300 seconds
        - Supports both httpx and requests libraries (checks for httpx.post attribute)
    """
    endpoint = f"{SYNTHEFY_API_BASE_URL.rstrip('/')}/v2/forecast"
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": api_key,
    }

    logger.info(f"Calling Synthefy API: {endpoint}")
    logger.debug(f"Request data: {json.dumps(request_data, indent=2)}")

    try:
        if hasattr(httpx, "post"):
            # Using requests library
            response = httpx.post(
                endpoint, json=request_data, headers=headers, timeout=300
            )
        else:
            # Using httpx library
            response = httpx.post(
                endpoint, json=request_data, headers=headers, timeout=300.0
            )

        response.raise_for_status()
        result = response.json()
        logger.info("Synthefy API call successful")
        return result
    except Exception as e:
        logger.error(f"Synthefy API call failed: {e}")
        raise


def _forecast_response_to_line_builders(
    forecast_response: Dict[str, Any],
    output_measurement: str,
    tags: Dict[str, str],
    model: str,
    field_name: Optional[str] = None,
) -> List[Any]:
    """
    Transform Synthefy forecast response to LineBuilder objects for InfluxDB.

    Converts the JSON response from Synthefy API into LineBuilder objects that can
    be written to InfluxDB. Each forecast point becomes a separate LineBuilder
    with appropriate tags and fields.

    Args:
        forecast_response (Dict[str, Any]): Forecast response from Synthefy API.
            Must contain:
            - "forecasts": List of forecast rows, where each row contains
              SingleSampleForecastPayload objects (as dicts) with:
              - "timestamps": List of ISO 8601 timestamp strings
              - "values": List of forecasted values
              - "quantiles": Optional dict mapping quantile levels to value lists
              - "sample_id": Field name identifier
        output_measurement (str): Measurement name for the output forecast data.
        tags (Dict[str, str]): Dictionary of tag key-value pairs to add to each
            forecast point. These are typically the same tags used to filter
            the input data.
        model (str): Synthefy model identifier. Added as a "model" tag to each point.
        field_name (Optional[str]): Field name for the forecast values in InfluxDB.
            If None, uses the "sample_id" from the forecast payload, or "value" as fallback.

    Returns:
        List[Any]: List of LineBuilder objects, one for each forecast point.
            Each LineBuilder contains:
            - Measurement: output_measurement
            - Time: Parsed timestamp in nanoseconds
            - Tags: All tags from tags dict + "model" tag
            - Fields: Main forecast value (as int64, float64, or string) +
              quantile fields (if available) as "value_{quantile_level}"

    Raises:
        ValueError: If forecast_response is missing the "forecasts" field.
        ValueError: If forecasts list is empty or contains no forecast data.
        ValueError: If no forecast payload can be found in the response.

    Note:
        - Skips forecast points with None values
        - Handles both dict and object-like forecast payloads
        - Quantiles are added as separate fields (e.g., "value_0.1", "value_0.9")
        - Timestamps are converted from ISO 8601 strings to nanoseconds
    """
    builders = []

    if "forecasts" not in forecast_response:
        raise ValueError("Invalid forecast response: missing 'forecasts' field")

    forecasts = forecast_response["forecasts"]
    if not forecasts or not forecasts[0]:
        raise ValueError("No forecasts in response")

    # Get the first forecast (we support single time series for now)
    forecast_row = forecasts[0]

    # The API returns SingleSampleForecastPayload objects as dicts in JSON
    # Find the forecast sample (not metadata)
    forecast_payload = None
    for f in forecast_row:
        # API response is JSON, so f will be a dict
        if isinstance(f, dict):
            # SingleSampleForecastPayload doesn't have "forecast" field
            # It's identified by having timestamps and values
            if "timestamps" in f and "values" in f:
                forecast_payload = f
                break
        else:
            # Handle object-like structures (shouldn't happen with JSON API)
            if hasattr(f, "timestamps") and hasattr(f, "values"):
                forecast_payload = {
                    "sample_id": getattr(f, "sample_id", "value"),
                    "timestamps": getattr(f, "timestamps", []),
                    "values": getattr(f, "values", []),
                    "quantiles": getattr(f, "quantiles", None),
                }
                break

    if not forecast_payload:
        # Fallback: use first item if available
        if forecast_row:
            first_item = forecast_row[0]
            if isinstance(first_item, dict):
                forecast_payload = first_item
            else:
                forecast_payload = {
                    "sample_id": getattr(first_item, "sample_id", "value"),
                    "timestamps": getattr(first_item, "timestamps", []),
                    "values": getattr(first_item, "values", []),
                    "quantiles": getattr(first_item, "quantiles", None),
                }
        else:
            raise ValueError("No forecast payload found in response")

    timestamps = forecast_payload.get("timestamps", [])
    values = forecast_payload.get("values", [])
    quantiles = forecast_payload.get("quantiles", {})

    # Get output field name
    output_field_name = field_name or forecast_payload.get("sample_id", "value")

    # Convert timestamps to nanoseconds and create LineBuilder objects
    for i, (ts_str, value) in enumerate(zip(timestamps, values)):
        if value is None:
            continue

        # Parse timestamp
        try:
            ts = pd.to_datetime(ts_str)
            ts_ns = int(ts.timestamp() * 1e9)
        except Exception:
            logger.warning(f"Could not parse timestamp: {ts_str}")
            continue

        # Create LineBuilder
        builder = LineBuilder(output_measurement)
        builder.time_ns(ts_ns)

        # Add tags
        for tag_key, tag_value in tags.items():
            builder.tag(tag_key, tag_value)
        builder.tag("model", model)

        # Add main field value
        if isinstance(value, int):
            builder.int64_field(output_field_name, value)
        elif isinstance(value, float):
            builder.float64_field(output_field_name, value)
        else:
            builder.string_field(output_field_name, str(value))

        # Add quantiles if available
        if quantiles:
            for q_level, q_values in quantiles.items():
                if i < len(q_values) and q_values[i] is not None:
                    q_value = q_values[i]
                    if isinstance(q_value, int):
                        builder.int64_field(f"value_{q_level}", q_value)
                    elif isinstance(q_value, float):
                        builder.float64_field(f"value_{q_level}", q_value)
                    else:
                        builder.string_field(f"value_{q_level}", str(q_value))

        builders.append(builder)

    return builders


def _write_forecasts_to_influxdb(
    influxdb3_local: Any,
    builders: List[Any],
    database: str,
    max_retries: int = 3,
) -> None:
    """
    Write forecast data to InfluxDB using LineBuilder objects.

    Writes forecast points to InfluxDB with automatic retry logic using exponential
    backoff. Each LineBuilder object is written individually to the specified database.

    Args:
        influxdb3_local (Any): InfluxDB 3 local client instance provided by the
            plugin framework. Must have a write_to_db(database, builder) method.
        builders (List[Any]): List of LineBuilder objects containing forecast data
            to write. Each builder represents one forecast point.
        database (str): Database name where forecast data should be written.
        max_retries (int, optional): Maximum number of retry attempts if write fails.
            Defaults to 3. Uses exponential backoff with jitter between retries.

    Returns:
        None: Function returns successfully if all writes complete without error.

    Raises:
        Exception: Re-raises the last exception if all retry attempts fail.
            The original exception from the write operation is preserved.

    Note:
        - Retry logic uses exponential backoff: wait_time = (2^attempt) + random(0,1) seconds
        - Logs warnings for each failed attempt
        - Logs error and re-raises exception after max_retries exhausted
        - All builders are written in a single batch; if any write fails, the
          entire batch is retried
    """
    logger.info(f"Writing {len(builders)} forecast points to InfluxDB")

    retry_count = 0
    for attempt in range(max_retries):
        try:
            for builder in builders:
                influxdb3_local.write_to_db(database, builder)
            logger.info(
                f"Forecasts written successfully to InfluxDB (attempt {attempt + 1})"
            )
            return
        except Exception as e:
            retry_count += 1
            logger.warning(
                f"Error writing forecast attempt {attempt + 1}/{max_retries}: {e}"
            )
            if attempt < max_retries - 1:
                wait_time = (2**attempt) + random.random()
                time.sleep(wait_time)
            else:
                logger.error(
                    f"Failed to write forecasts to InfluxDB after {max_retries} attempts: {e}"
                )
                raise


def process_request(
    influxdb3_local: Any,
    query_parameters: Dict[str, Any],
    request_headers: Dict[str, str],
    request_body: Any,
    args: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Process HTTP request forecasting trigger.

    This function is called by InfluxDB 3 when an HTTP request is made to the plugin
    endpoint. It queries historical time series data, generates a forecast using
    the Synthefy API, and writes the forecast results back to InfluxDB.

    Args:
        influxdb3_local (Any): InfluxDB 3 local client instance provided by the
            plugin framework. Must have:
            - query(query_str): Execute SQL query and return list of dicts
            - write_to_db(database, builder): Write LineBuilder to database
            - database (attribute): Database name from trigger context (may be None)
        query_parameters (Dict[str, Any]): HTTP query parameters from the request URL.
            These are merged with request_body and args (lowest priority).
        request_headers (Dict[str, str]): HTTP request headers. Currently not used
            but provided by the framework for authentication and other purposes.
        request_body (Any): HTTP request body. Can be:
            - bytes: Decoded as UTF-8 and parsed as JSON
            - str: Parsed as JSON
            - dict: Used directly
            - None: Treated as empty dict
            Parameters in request_body override trigger args (highest priority).
        args (Optional[Dict[str, Any]]): Plugin configuration arguments from the
            trigger configuration (defaults). These are merged with request_body
            and query_parameters. Priority: request_body > query_parameters > args.
            See _parse_args() for expected keys.

    Returns:
        Dict[str, Any]: Response dictionary with a "message" key containing:
            - Success: "Forecast generated and written to InfluxDB. {N} forecast points written."
            - No data: "No data found"
            - Error: "Error: {error_message}"

    Raises:
        Exception: Any exception raised during processing is caught, logged, and
            returned as an error message in the response dictionary. The function
            never raises exceptions directly; all errors are returned as error messages.

    Workflow:
        1. Parse request body (handle bytes, str, dict, or None)
        2. Merge args, request_body, and query_parameters (request_body has highest priority)
        3. Parse and validate merged arguments
        4. Determine database name (priority: trigger context > request_body > args)
        5. Parse tags and metadata fields
        6. Build and execute SQL query to retrieve historical data
        7. Convert query results to DataFrame
        8. Transform DataFrame to Synthefy API request format
        9. Call Synthefy Forecasting API
        10. Transform API response to LineBuilder objects
        11. Write forecast points to InfluxDB
        12. Return success message with count of forecast points

    Note:
        - Database name is required. If not found in trigger context, it must be
          provided in request_body or trigger args. Returns error message if missing.
        - All parameters can be provided via request body, query parameters, or
          trigger configuration. Request body parameters override others.
        - If query returns no data, returns {"message": "No data found"} (not an error)
        - All exceptions are caught and returned as error messages; function never raises
        - Authentication is handled automatically by the framework via influxdb3_local
    """
    logger.info("Starting HTTP request forecast")

    if args is None:
        args = {}

    try:
        # Parse request body (may be bytes, JSON string, or already a dict)
        if isinstance(request_body, bytes):
            try:
                body_str = request_body.decode("utf-8")
                body_dict = json.loads(body_str)
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                logger.warning(f"Failed to decode/parse request body: {e}")
                body_dict = {}
        elif isinstance(request_body, str):
            try:
                body_dict = json.loads(request_body)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse JSON string: {e}")
                body_dict = {}
        elif isinstance(request_body, dict):
            body_dict = request_body
        elif request_body is None:
            body_dict = {}
        else:
            logger.warning(f"Unexpected request_body type: {type(request_body)}")
            body_dict = {}

        # Merge query parameters, request body, and args (request body can override args)
        merged_args = {**args, **body_dict, **query_parameters}
        parsed_args = _parse_args(merged_args)
        # Get database from trigger context, request body, or args
        # Priority: trigger context > request body > trigger args
        database = (
            getattr(influxdb3_local, "database", None)
            or merged_args.get("database")
            or args.get("database")
        )
        if not database:
            error_msg = (
                "Database name not found. The database should be set automatically by the trigger context. "
                "For HTTP requests, you can specify 'database' in the request body or trigger arguments."
            )
            logger.error(error_msg)
            return {"message": error_msg}
        logger.info(
            f"Using database: {database} (from trigger context: {getattr(influxdb3_local, 'database', None)}, from request: {merged_args.get('database')}, from args: {args.get('database') if args else None})"
        )

        tags = _parse_tags(parsed_args.get("tags", ""))
        metadata_fields = [
            f.strip()
            for f in parsed_args.get("metadata_fields", "").split(",")
            if f.strip()
        ]

        # Query data using influxdb3_local (which has authentication from HTTP request)
        query = _build_query(
            parsed_args["measurement"],
            parsed_args["field"],
            tags,
            parsed_args["time_range"],
            metadata_fields,
        )

        logger.info(f"Executing query: {query}")
        # Use influxdb3_local.query() which automatically uses the authenticated context from HTTP request
        # Returns a list of dictionaries (rows)
        result_rows = influxdb3_local.query(query)

        # Convert list of dicts to DataFrame
        if not result_rows:
            df = pd.DataFrame()
        else:
            df = pd.DataFrame(result_rows)

        if df.empty:
            return {"message": "No data found"}

        # Transform and call API
        synthefy_request = _dataframe_to_synthefy_request(
            df,
            parsed_args["field"],
            parsed_args["forecast_horizon"],
            metadata_fields,
            parsed_args["model"],
        )

        forecast_response = _call_synthefy_api(
            synthefy_request,
            parsed_args["api_key"],
        )

        builders = _forecast_response_to_line_builders(
            forecast_response,
            parsed_args["output_measurement"],
            tags,
            parsed_args["model"],
            parsed_args["field"],
        )

        # Write using influxdb3_local (authentication handled by framework)
        _write_forecasts_to_influxdb(influxdb3_local, builders, database)

        return {
            "message": f"Forecast generated and written to InfluxDB. {len(builders)} forecast points written."
        }

    except Exception as e:
        logger.error(f"HTTP request forecast failed: {e}", exc_info=True)
        return {"message": f"Error: {str(e)}"}
