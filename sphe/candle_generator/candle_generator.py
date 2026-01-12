"""
{
    "plugin_type": ["scheduled", "http"],
    "scheduled_args_config": [

        {
            "name": "target_measurement",
            "example": "prochain_candles",
            "description": "Name of the target measurement to write candle data.",
            "required": true
        },
        {
            "name": "timeframe",
            "example": "30s",
            "description": "Candle timeframe. Supported: '30s', '60s', '120s', '300s', '600s', '900s'. Note: All timeframes are processed automatically.",
            "required": false
        },

        {
            "name": "offset",
            "example": "10s",
            "description": "Time offset to apply to the window (e.g., '10s', '1h'). Units: 's', 'min', 'h', 'd', 'w'.",
            "required": false
        },

        {
            "name": "max_retries",
            "example": "5",
            "description": "Maximum number of retries for write operations.",
            "required": false
        },
        {
            "name": "target_database",
            "example": "candles_db",
            "description": "Target database for writing candle data.",
            "required": true
        },
        {
            "name": "source_measurement",
            "example": "raw_trades",
            "description": "Source measurement/table to read trade data from.",
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

import json
import os
import re
import time
import tomllib
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def get_all_timeframes() -> List[timedelta]:
    """
    Get all supported timeframes as timedelta objects.
    
    Returns:
        List of timedelta objects for all supported timeframes
    """
    supported_timeframes = {'30s', '60s', '120s', '300s', '600s', '900s'}
    timeframes = []
    
    for timeframe_str in supported_timeframes:
        if timeframe_str.endswith('s'):
            seconds = int(timeframe_str[:-1])
            timeframes.append(timedelta(seconds=seconds))
    
    return timeframes


def parse_timeframe(timeframe: str) -> timedelta:
    """
    Parse timeframe string into timedelta.
    
    Args:
        timeframe: Timeframe string (e.g., '30s', '60s', '300s')
        
    Returns:
        timedelta representing the timeframe
        
    Raises:
        ValueError: If timeframe is not supported
    """
    supported_timeframes = {'30s', '60s', '120s', '300s', '600s', '900s'}
    if timeframe not in supported_timeframes:
        raise ValueError(f"Unsupported timeframe: {timeframe}. Supported: {supported_timeframes}")
    
    # Parse timeframe
    if timeframe.endswith('s'):
        seconds = int(timeframe[:-1])
        return timedelta(seconds=seconds)
    else:
        raise ValueError(f"Invalid timeframe format: {timeframe}")


def calculate_window() -> timedelta:
    """
    Calculate the time window based on the plugin trigger interval.
    Since the plugin runs every 30 seconds, we use a 1-hour window
    to ensure we capture enough data for all timeframes.
    
    Returns:
        timedelta representing the window (1 hour)
    """
    return timedelta(hours=1)


def parse_offset(args: dict, task_id: str) -> timedelta:
    """
    Parse offset string into timedelta.
    
    Args:
        args: Arguments dictionary
        task_id: Task ID for logging
        
    Returns:
        timedelta representing the offset (default: 0)
    """
    offset_str = args.get('offset', '0s')
    
    # Parse offset string (e.g., "10s", "1h")
    match = re.match(r'^(\d+)(s|min|h|d|w)$', offset_str)
    if not match:
        raise ValueError(f"[{task_id}] Invalid offset format: {offset_str}")
    
    value, unit = match.groups()
    value = int(value)
    
    if unit == 's':
        return timedelta(seconds=value)
    elif unit == 'min':
        return timedelta(minutes=value)
    elif unit == 'h':
        return timedelta(hours=value)
    elif unit == 'd':
        return timedelta(days=value)
    elif unit == 'w':
        return timedelta(weeks=value)
    else:
        raise ValueError(f"[{task_id}] Unsupported offset unit: {unit}")


def build_multi_timeframe_candle_query(
    timeframes: List[timedelta],
    start_time: datetime,
    end_time: datetime,
    source_measurement: str = "raw_trades"
) -> str:
    """
    Build SQL query for multi-timeframe candle generation from source measurement table.
    
    Args:
        timeframes: List of candle timeframes
        start_time: Start time for query
        end_time: End time for query
        source_measurement: Source measurement/table name
        
    Returns:
        SQL query string
    """
    # Format timestamps for SQL
    start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
    
    # Build UNION ALL queries for each timeframe
    timeframe_queries = []
    
    for timeframe in timeframes:
        timeframe_seconds = int(timeframe.total_seconds())
        
        timeframe_query = f"""
        SELECT 
            date_bin(INTERVAL '{timeframe_seconds} seconds', time, TIMESTAMP '1970-01-01 00:00:00') AS bucket_time,
            mint,
            platform,
            price,
            quote_volume,
            time as original_time,
            {timeframe_seconds} as timeframe_seconds
        FROM "{source_measurement}"
        WHERE time >= '{start_time_str}' 
        AND time < '{end_time_str}'
        """
        timeframe_queries.append(timeframe_query)
    
    # Combine all timeframe queries
    combined_query = f"""
    WITH all_timeframes AS (
        {' UNION ALL '.join(timeframe_queries)}
    ),
    ranked_data AS (
        SELECT 
            bucket_time,
            mint,
            platform,
            price,
            quote_volume,
            timeframe_seconds,
            ROW_NUMBER() OVER (PARTITION BY bucket_time, mint, platform, timeframe_seconds ORDER BY original_time ASC) as rn_asc,
            ROW_NUMBER() OVER (PARTITION BY bucket_time, mint, platform, timeframe_seconds ORDER BY original_time DESC) as rn_desc
        FROM all_timeframes
    ),
    candle_data AS (
        SELECT 
            bucket_time,
            mint,
            platform,
            timeframe_seconds,
            MAX(CASE WHEN rn_asc = 1 THEN price END) AS open,
            MAX(price) AS high,
            MIN(price) AS low,
            MAX(CASE WHEN rn_desc = 1 THEN price END) AS close,
            SUM(quote_volume) AS volume
        FROM ranked_data
        GROUP BY bucket_time, mint, platform, timeframe_seconds
        ORDER BY bucket_time, timeframe_seconds
    )
    SELECT * FROM candle_data
    """
    
    return combined_query


def write_candle_data(
    influxdb3_local,
    data: List[Dict],
    max_retries: int,
    target_measurement: str,
    target_database: str,
    task_id: str,
):
    """
    Write candle data to InfluxDB.
    
    Args:
        influxdb3_local: InfluxDB client
        data: List of candle data dictionaries (each with timeframe field)
        max_retries: Maximum number of retries
        target_measurement: Target measurement name
        target_database: Target database name
        task_id: Task ID for logging
    """
    if not data:
        influxdb3_local.info(f"[{task_id}] No candle data to write")
        return 0
    
    influxdb3_local.info(f"[{task_id}] Writing {len(data)} candle records to {target_measurement}")
    
    # Use target database (now mandatory)
    database = target_database
    
    # Transform data to InfluxDB line protocol format using LineBuilder
    builders: list = []
    for candle in data:
        # Get timeframe from candle data
        timeframe_seconds = candle.get('timeframe_seconds')
        if timeframe_seconds is None:
            influxdb3_local.info(f"[{task_id}] Warning: candle missing timeframe_seconds, skipping")
            continue
            
        # Create tags including timeframe - only add non-empty, valid values
        tags = {}
        mint = candle.get('mint', '')
        platform = candle.get('platform', '')

        tags['mint'] = str(mint).strip()
        tags['platform'] = str(platform).strip()
        tags['timeframe'] = f"{timeframe_seconds}s"
        
        # Create fields - make OHLCV mandatory even if zero
        fields = {}
        open_price = candle.get('open')
        if open_price is not None:
            fields['open'] = float(open_price)
        else:
            fields['open'] = 0.0
        
        high_price = candle.get('high')
        if high_price is not None:
            fields['high'] = float(high_price)
        else:
            fields['high'] = 0.0
        
        low_price = candle.get('low')
        if low_price is not None:
            fields['low'] = float(low_price)
        else:
            fields['low'] = 0.0
        
        close_price = candle.get('close')
        if close_price is not None:
            fields['close'] = float(close_price)
        else:
            fields['close'] = 0.0
        
        volume = candle.get('volume')
        if volume is not None:
            fields['volume'] = float(volume)
        else:
            fields['volume'] = 0.0
        
        # Set timestamp
        timestamp = candle.get('_time', datetime.now(timezone.utc))
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        elif isinstance(timestamp, int):
            # Convert nanoseconds to datetime
            timestamp = datetime.fromtimestamp(timestamp / 1e9, tz=timezone.utc)
        
        # Create LineBuilder object
        line = LineBuilder(target_measurement)
        
        # Set timestamp
        line.time_ns(int(timestamp.timestamp() * 1e9))
        
        # Add tags - only add non-empty tags
        for key, value in tags.items():
            line.tag(key, str(value).strip())
        
        # Add fields - all OHLCV fields are mandatory
        for key, value in fields.items():
            if value is not None:
                line.float64_field(key, value)
        
        builders.append(line)

    # Write data with retries
    for attempt in range(max_retries + 1):
        try:
            for row in builders:
                influxdb3_local.write_to_db(database, row)
            influxdb3_local.info(f"[{task_id}] Successfully wrote {len(builders)} candles to {target_measurement}")
            break
        except Exception as e:
            if attempt == max_retries:
                influxdb3_local.info(f"[{task_id}] Failed to write candle data to {target_measurement} after {max_retries} retries: {e}")
                raise
            else:
                influxdb3_local.info(f"[{task_id}] Write attempt {attempt + 1} failed, retrying: {e}")
                time.sleep(2 ** attempt)  # Exponential backoff

    return len(builders)


def process_scheduled_call(
    influxdb3_local, call_time: datetime, args: Optional[Dict] = None
):
    """
    Process scheduled candle generation call.
    
    Args:
        influxdb3_local: InfluxDB client
        call_time: Current call time
        args: Arguments dictionary
    """
    task_id = str(uuid.uuid4())[:8]
    influxdb3_local.info(f"[{task_id}] Starting scheduled candle generation at {call_time}")
    
    try:
        # Load config from file if specified
        if args and args.get('config_file_path'):
            config_path = Path(args['config_file_path'])
            if not config_path.is_absolute():
                plugin_dir = os.environ.get('PLUGIN_DIR', '.')
                config_path = Path(plugin_dir) / config_path
            
            with open(config_path, 'rb') as f:
                config = tomllib.load(f)
            args = {**args, **config}
        
        # Parse arguments
        target_measurement = args.get('target_measurement')
        if not target_measurement:
            raise ValueError(f"[{task_id}] target_measurement is required")
        
        # Get all timeframes to process
        timeframes = get_all_timeframes()
        window = calculate_window()
        offset = parse_offset(args, task_id)
        max_retries = int(args.get('max_retries', 5))
        target_database = args.get('target_database')
        if not target_database:
            raise ValueError(f"[{task_id}] target_database is required")
        source_measurement = args.get('source_measurement', 'raw_trades')
        
        # Calculate time window
        end_time = call_time - offset
        start_time = end_time - window
        
        influxdb3_local.info(f"[{task_id}] Processing window: {start_time} to {end_time}")
        influxdb3_local.info(f"[{task_id}] Source: {source_measurement}")
        influxdb3_local.info(f"[{task_id}] Target: {target_measurement}")
        influxdb3_local.info(f"[{task_id}] Processing {len(timeframes)} timeframes: {[f'{int(tf.total_seconds())}s' for tf in timeframes]}")
        
        # Build and execute single query for all timeframes
        influxdb3_local.info(f"[{task_id}] Building multi-timeframe query for {len(timeframes)} timeframes...")
        query = build_multi_timeframe_candle_query(
            timeframes=timeframes,
            start_time=start_time,
            end_time=end_time,
            source_measurement=source_measurement
        )
        
        influxdb3_local.info(f"[{task_id}] Executing multi-timeframe query...")
        result = influxdb3_local.query(query)
        
        # Process results from single query
        all_candles = []
        timeframe_counts = {}
        
        for row in result:
            candle = {
                '_time': row['bucket_time'],
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                'close': row['close'],
                'volume': row['volume'],
                'mint': row['mint'],
                'platform': row['platform'],
                'timeframe_seconds': row['timeframe_seconds']
            }
            all_candles.append(candle)
            
            # Count candles per timeframe
            timeframe_str = f"{row['timeframe_seconds']}s"
            timeframe_counts[timeframe_str] = timeframe_counts.get(timeframe_str, 0) + 1
        
        # Log results per timeframe
        for timeframe_str, count in timeframe_counts.items():
            influxdb3_local.info(f"[{task_id}] Generated {count} candles for {timeframe_str}")
        
        # Write all candles to database in a single call
        if all_candles:
            total_candles_generated = write_candle_data(
                influxdb3_local=influxdb3_local,
                data=all_candles,
                max_retries=max_retries,
                target_measurement=target_measurement,
                target_database=target_database,
                task_id=task_id
            )
        else:
            total_candles_generated = 0
        
        influxdb3_local.info(f"[{task_id}] Total candles generated across all timeframes: {total_candles_generated}")
        
        influxdb3_local.info(f"[{task_id}] Candle generation completed successfully")
        
    except Exception as e:
        influxdb3_local.info(f"[{task_id}] Error in scheduled candle generation: {e}")
        raise  

        
def process_request(
    influxdb3_local, query_parameters, request_headers, request_body, args=None
):
    """
    Process HTTP request for candle generation.
    
    Args:
        influxdb3_local: InfluxDB client
        query_parameters: Query parameters from request
        request_headers: Request headers
        request_body: Request body
        args: Arguments dictionary
    """
    task_id = str(uuid.uuid4())[:8]
    influxdb3_local.info(f"[{task_id}] Processing HTTP candle generation request")
    
    try:
        Parse request data
        if request_body:
            data = json.loads(request_body)
        else:
            data = {}
        
        Merge query parameters and request body
        request_args = {**query_parameters, **data}
        
        Load config from file if specified
        if request_args.get('config_file_path'):
            config_path = Path(request_args['config_file_path'])
            if not config_path.is_absolute():
                plugin_dir = os.environ.get('PLUGIN_DIR', '.')
                config_path = Path(plugin_dir) / config_path
            
            with open(config_path, 'rb') as f:
                config = tomllib.load(f)
            request_args = {**request_args, **config}
        
        Parse arguments
        target_measurement = request_args.get('target_measurement')
        if not target_measurement:
            raise ValueError(f"[{task_id}] target_measurement is required")
        
        Check if specific timeframe is requested, otherwise use all timeframes
        timeframe_str = request_args.get('timeframe')
        if timeframe_str:
            Single timeframe requested
            timeframes = [parse_timeframe(timeframe_str)]
            influxdb3_local.info(f"[{task_id}] Single timeframe requested: {timeframe_str}")
        else:
            All timeframes
            timeframes = get_all_timeframes()
            influxdb3_local.info(f"[{task_id}] All timeframes requested")
        
        max_retries = int(request_args.get('max_retries', 5))
        target_database = request_args.get('target_database')
        if not target_database:
            raise ValueError(f"[{task_id}] target_database is required")
        source_measurement = request_args.get('source_measurement', 'raw_trades')
        
        Parse time range from request
        start_time_str = request_args.get('start_time')
        end_time_str = request_args.get('end_time')
        
        if not start_time_str or not end_time_str:
            raise ValueError(f"[{task_id}] start_time and end_time are required for HTTP requests")
        
        start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
        end_time = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
        
        influxdb3_local.info(f"[{task_id}] Processing time range: {start_time} to {end_time}")
        influxdb3_local.info(f"[{task_id}] Source: {source_measurement}")
        influxdb3_local.info(f"[{task_id}] Target: {target_measurement}")
        influxdb3_local.info(f"[{task_id}] Processing {len(timeframes)} timeframes: {[f'{int(tf.total_seconds())}s' for tf in timeframes]}")
        
        Build and execute single query for all timeframes
        influxdb3_local.info(f"[{task_id}] Building multi-timeframe query for {len(timeframes)} timeframes...")
        query = build_multi_timeframe_candle_query(
            timeframes=timeframes,
            start_time=start_time,
            end_time=end_time,
            source_measurement=source_measurement
        )
        
        influxdb3_local.info(f"[{task_id}] Executing multi-timeframe query...")
        result = influxdb3_local.query(query)
        
        Process results from single query
        all_candles = []
        timeframe_results = {}
        
        for row in result:
            candle = {
                '_time': row['bucket_time'],
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                'close': row['close'],
                'volume': row['volume'],
                'mint': row['mint'],
                'platform': row['platform'],
                'timeframe_seconds': row['timeframe_seconds']
            }
            all_candles.append(candle)
            
            Count candles per timeframe
            timeframe_str = f"{row['timeframe_seconds']}s"
            timeframe_results[timeframe_str] = timeframe_results.get(timeframe_str, 0) + 1
        
        Log results per timeframe
        for timeframe_str, count in timeframe_results.items():
            influxdb3_local.info(f"[{task_id}] Generated {count} candles for {timeframe_str}")
        
        Write all candles to database in a single call
        if all_candles:
            total_candles_generated = write_candle_data(
                influxdb3_local=influxdb3_local,
                data=all_candles,
                max_retries=max_retries,
                target_measurement=target_measurement,
                target_database=target_database,
                task_id=task_id
            )
        else:
            total_candles_generated = 0
        
        influxdb3_local.info(f"[{task_id}] Total candles generated across all timeframes: {total_candles_generated}")
        
        Return success response
        response = {
            'status': 'success',
            'task_id': task_id,
            'candles_generated': total_candles_generated,
            'timeframes_processed': len(timeframes),
            'timeframe_results': timeframe_results,
            'target_measurement': target_measurement,
            'time_range': {
                'start': start_time.isoformat(),
                'end': end_time.isoformat()
            }
        }
        
        return json.dumps(response), 200, {'Content-Type': 'application/json'}
        
    except Exception as e:
        influxdb3_local.info(f"[{task_id}] Error in HTTP candle generation: {e}")
        error_response = {
            'status': 'error',
            'task_id': task_id,
            'error': str(e)
        }
        
        return json.dumps(error_response), 500, {'Content-Type': 'application/json'}
