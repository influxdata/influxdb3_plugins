"""
{
    "plugin_type": ["scheduled"],
    "scheduled_args_config": [
        {
            "name": "stations",
            "example": "KSEA.KSFO.KORD",
            "description": "Dot-separated list of NWS station IDs. Defaults to 10 major US airports.",
            "required": false
        },
        {
            "name": "measurement",
            "example": "weather_observations",
            "description": "Name of the measurement to write weather data to. Defaults to 'weather_observations'.",
            "required": false
        },
        {
            "name": "user_agent",
            "example": "InfluxDB3-NWS-Plugin/1.0",
            "description": "Custom User-Agent header for NWS API requests. Defaults to 'InfluxDB3-NWS-Plugin/1.0'.",
            "required": false
        },
        {
            "name": "use_data_timestamp",
            "example": "true",
            "description": "Use timestamp from weather data instead of current time. Defaults to 'true'.",
            "required": false
        }
    ]
}
"""

import json
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def process_scheduled_call(
    influxdb3_local, call_time: datetime, args: Optional[Dict[str, Any]] = None
) -> None:
    """
    Scheduled plugin that fetches weather data from NWS stations.

    Args:
        influxdb3_local: InfluxDB API object for writing data and logging
        call_time: Timestamp when the trigger was called
        args: Optional trigger arguments:
            - stations: Dot-separated list of station IDs (default: pre-configured list)
            - measurement: Measurement name to write to (default: 'weather_observations')
            - user_agent: Custom User-Agent for API requests (default: 'InfluxDB3-NWS-Plugin')
            - use_data_timestamp: Use timestamp from weather data instead of current time (default: 'true')
    """
    task_id = str(uuid.uuid4())

    # Default configuration
    default_stations = [
        "KSEA",  # Seattle-Tacoma International Airport, WA
        "KORD",  # Chicago O'Hare International Airport, IL
        "KJFK",  # John F. Kennedy International Airport, NY
        "KDEN",  # Denver International Airport, CO
        "KATL",  # Hartsfield-Jackson Atlanta International Airport, GA
        "KDFW",  # Dallas/Fort Worth International Airport, TX
        "KLAX",  # Los Angeles International Airport, CA
        "KMIA",  # Miami International Airport, FL
        "KPHX",  # Phoenix Sky Harbor International Airport, AZ
        "KBOS",  # Boston Logan International Airport, MA
    ]

    # Parse trigger arguments
    measurement_name = "weather_observations"
    user_agent = "InfluxDB3-NWS-Plugin/1.0"
    stations = default_stations
    use_data_timestamp = True

    if args:
        if "measurement" in args:
            measurement_name = args["measurement"]
        if "user_agent" in args:
            user_agent = args["user_agent"]
        if "stations" in args:
            # Parse comma-separated station list
            stations = [s.strip() for s in args["stations"].split(".") if s.strip()]
        if "use_data_timestamp" in args:
            use_data_timestamp = args["use_data_timestamp"].lower() == "true"

    influxdb3_local.info(
        f"[{task_id}] NWS Plugin started at {call_time} with args {args}"
    )
    influxdb3_local.info(
        f"[{task_id}] Fetching data from {len(stations)} weather stations"
    )

    # Track statistics
    success_count = 0
    error_count = 0

    try:
        # Fetch data from stations in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(len(stations), 5)) as executor:
            # Submit all fetch tasks
            futures = {
                executor.submit(
                    fetch_station_data, influxdb3_local, station_id, user_agent, task_id
                ): station_id
                for station_id in stations
            }

            # Process results as they complete
            for future in as_completed(futures):
                station_id = futures[future]
                try:
                    data = future.result()

                    if data:
                        write_weather_data(
                            influxdb3_local,
                            measurement_name,
                            station_id,
                            data,
                            task_id,
                            use_data_timestamp,
                        )
                        success_count += 1
                    else:
                        error_count += 1

                except Exception as e:
                    influxdb3_local.error(
                        f"[{task_id}] Error processing station {station_id}: {str(e)}"
                    )
                    error_count += 1

        # Write plugin statistics
        write_plugin_stats(
            influxdb3_local, success_count, error_count, len(stations), task_id
        )

        # Log summary
        influxdb3_local.info(
            f"[{task_id}] NWS Plugin completed: {success_count} successful, {error_count} errors"
        )

    except Exception as e:
        influxdb3_local.error(
            f"[{task_id}] Unexpected error while processing NWS data: {str(e)}"
        )


def fetch_station_data(
    influxdb3_local, station_id: str, user_agent: str, task_id: str
) -> Optional[Dict[str, Any]]:
    """
    Fetch latest weather observations from a NWS station.

    Args:
        influxdb3_local: InfluxDB API for logging
        station_id: NWS station identifier (e.g., 'KSEA')
        user_agent: User-Agent header for the request
        task_id: String identifier for logging context

    Returns:
        dict: Weather observation data or None if fetch failed
    """
    url = f"https://api.weather.gov/stations/{station_id}/observations/latest"

    try:
        # Create request with User-Agent header (required by NWS API)
        req = Request(url)
        req.add_header("User-Agent", user_agent)
        req.add_header("Accept", "application/json")

        # Fetch data with timeout
        with urlopen(req, timeout=10) as response:
            if response.status == 200:
                data = json.loads(response.read().decode("utf-8"))
                influxdb3_local.info(
                    f"[{task_id}] Successfully fetched data from {station_id}"
                )
                return data
            else:
                influxdb3_local.warn(
                    f"[{task_id}] Station {station_id} returned status {response.status}"
                )
                return None

    except HTTPError as e:
        if e.code == 404:
            influxdb3_local.warn(f"[{task_id}] Station {station_id} not found (404)")
        elif e.code == 500:
            influxdb3_local.warn(f"[{task_id}] NWS API error for {station_id} (500)")
        else:
            influxdb3_local.error(
                f"[{task_id}] HTTP error {e.code} for station {station_id}"
            )
        return None

    except URLError as e:
        influxdb3_local.error(
            f"[{task_id}] Network error fetching {station_id}: {str(e)}"
        )
        return None

    except json.JSONDecodeError as e:
        influxdb3_local.error(
            f"[{task_id}] JSON decode error for {station_id}: {str(e)}"
        )
        return None

    except Exception as e:
        influxdb3_local.error(
            f"[{task_id}] Unexpected error for {station_id}: {str(e)}"
        )
        return None


def write_weather_data(
    influxdb3_local,
    measurement_name: str,
    station_id: str,
    data: Dict[str, Any],
    task_id: str,
    use_data_timestamp: bool = True,
) -> None:
    """
    Parse NWS API response and write to InfluxDB.

    Args:
        influxdb3_local: InfluxDB API object
        measurement_name: Name of the measurement to write to
        station_id: Station identifier for tagging
        data: Raw API response data
        task_id: String identifier for logging context
        use_data_timestamp: If True, use timestamp from weather data; if False, use current time
    """
    try:
        properties = data.get("properties", {})

        # Extract location information
        geometry = data.get("geometry", {})
        coordinates = geometry.get("coordinates", [])

        # Extract weather values
        temp_c = extract_value(properties.get("temperature"))
        dewpoint_c = extract_value(properties.get("dewpoint"))
        wind_speed_kmh = extract_value(properties.get("windSpeed"))
        wind_direction = extract_value(properties.get("windDirection"))
        wind_gust_kmh = extract_value(properties.get("windGust"))
        barometric_pressure_pa = extract_value(properties.get("barometricPressure"))
        visibility_m = extract_value(properties.get("visibility"))
        relative_humidity = extract_value(properties.get("relativeHumidity"))
        text_description = properties.get("textDescription", "N/A")

        # Build line protocol
        line = LineBuilder(measurement_name)

        # Set timestamp if using data timestamp
        if use_data_timestamp:
            # Extract and parse timestamp
            timestamp_str = properties.get("timestamp")
            if not timestamp_str:
                influxdb3_local.warn(
                    f"[{task_id}] No timestamp for {station_id}, skipping"
                )
                return

            timestamp_ns = parse_timestamp_to_nanoseconds(timestamp_str)
            if timestamp_ns is None:
                influxdb3_local.warn(
                    f"[{task_id}] Failed to parse timestamp for {station_id}, skipping"
                )
                return

            line.time_ns(timestamp_ns)

        # Add tags
        line.tag("station_id", station_id)
        line.tag("station", properties.get("station", "").split("/")[-1])

        if text_description and text_description != "N/A":
            # Sanitize description for use as tag (remove special characters)
            safe_description = (
                text_description.replace(",", "").replace("=", "").strip()
            )
            if safe_description:
                line.tag("conditions", safe_description[:50])  # Limit length

        # Add location tags if available
        if len(coordinates) >= 2:
            line.tag("longitude", f"{coordinates[0]:.4f}")
            line.tag("latitude", f"{coordinates[1]:.4f}")

        # Add elevation if available
        elevation = properties.get("elevation", {}).get("value")
        if elevation is not None:
            line.float64_field("elevation_m", float(elevation))

        # Add fields
        if temp_c is not None:
            line.float64_field("temperature_c", temp_c)

        if dewpoint_c is not None:
            line.float64_field("dewpoint_c", dewpoint_c)

        if wind_speed_kmh is not None:
            line.float64_field("wind_speed_kmh", wind_speed_kmh)

        if wind_direction is not None:
            line.float64_field("wind_direction_degrees", wind_direction)

        if wind_gust_kmh is not None:
            line.float64_field("wind_gust_kmh", wind_gust_kmh)

        if barometric_pressure_pa is not None:
            line.float64_field("barometric_pressure_pa", barometric_pressure_pa)

        if visibility_m is not None:
            line.float64_field("visibility_m", visibility_m)

        if relative_humidity is not None:
            line.float64_field("relative_humidity_percent", relative_humidity)

        # Write to InfluxDB
        influxdb3_local.write(line)

        influxdb3_local.info(
            f"[{task_id}] Wrote weather data for {station_id}: "
            f"temp={temp_c}°C, humidity={relative_humidity}%"
        )

    except Exception as e:
        influxdb3_local.error(
            f"[{task_id}] Error writing data for {station_id}: {str(e)}"
        )


def extract_value(value_obj: Optional[Dict[str, Any]]) -> Optional[float]:
    """
    Extract numeric value from NWS API value object.

    NWS API returns values in format: {"value": 23.3, "unitCode": "..."}

    Args:
        value_obj: Value object from NWS API

    Returns:
        float: Extracted value or None
    """
    if not isinstance(value_obj, dict):
        return None

    value = value_obj.get("value")
    if value is None:
        return None

    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_timestamp_to_nanoseconds(timestamp_str: str) -> Optional[int]:
    """
    Parse ISO 8601 timestamp string to nanoseconds.

    Args:
        timestamp_str: ISO 8601 formatted timestamp (e.g., "2025-11-23T14:00:00+00:00")

    Returns:
        int: Unix timestamp in nanoseconds or None if parsing fails
    """
    try:
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        # Convert to Unix timestamp in seconds, then to nanoseconds
        timestamp_ns = int(dt.timestamp() * 1_000_000_000)
        return timestamp_ns
    except (ValueError, AttributeError):
        return None


def write_plugin_stats(
    influxdb3_local,
    success_count: int,
    error_count: int,
    total_count: int,
    task_id: str,
) -> None:
    """
    Write plugin execution statistics to a separate measurement.

    Args:
        influxdb3_local: InfluxDB API object
        success_count: Number of successful station fetches
        error_count: Number of failed station fetches
        total_count: Total number of stations attempted
        task_id: String identifier for logging context
    """
    try:
        line = LineBuilder("nws_plugin_stats")
        line.tag("plugin", "nws_weather_sampler")
        line.int64_field("success_count", success_count)
        line.int64_field("error_count", error_count)
        line.int64_field("total_count", total_count)
        line.float64_field(
            "success_rate",
            (success_count / total_count * 100) if total_count > 0 else 0,
        )
        line.string_field("task_id", task_id)

        influxdb3_local.write(line)

    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Error writing plugin stats: {str(e)}")
