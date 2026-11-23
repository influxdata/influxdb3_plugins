# Candle Generator Plugin

⚡ scheduled, http 🏷️ candles, OHLCV, trading, aggregation 🔧 InfluxDB 3 Core, InfluxDB 3 Enterprise

## Description

The Candle Generator Plugin enables automatic generation of OHLCV (Open, High, Low, Close, Volume) candles from InfluxProchainDataPoint data in InfluxDB 3. This plugin is specifically designed to work with Prochain trading data and supports multiple timeframes (30s, 60s, 300s). It can process both scheduled batch operations and on-demand HTTP requests to generate candlestick data for trading analysis.

## Features

- **Multiple Timeframes**: Support for 30-second, 1-minute, and 5-minute candles
- **OHLCV Generation**: Complete candlestick data with open, high, low, close, and volume
- **Dynamic Table Naming**: Candles are automatically saved to tables named after the mint (e.g., `candles_TOKEN1`)
- **Essential Tags Only**: Preserves only essential tags (mint, is_buy, platform) for cleaner data
- **Mint Filtering**: Optional filtering by specific mint addresses
- **Scheduled Processing**: Automatic candle generation on configurable intervals
- **HTTP API**: On-demand candle generation via HTTP requests
- **Retry Logic**: Robust error handling with configurable retry attempts
- **TOML Configuration**: Support for configuration files

## Candle Structure

Each generated candle contains the following fields:

### OHLCV Fields
- `open`: Opening price for the timeframe
- `high`: Highest price during the timeframe
- `low`: Lowest price during the timeframe
- `close`: Closing price for the timeframe
- `volume`: Total quote volume during the timeframe

### Tags (Essential Only)
- `mint`: Token mint address
- `is_buy`: Boolean indicating if it's a buy transaction
- `platform`: Platform identifier

### Table Naming
Candles are automatically saved to tables named after the mint address. For example:
- `candles_EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v` for USDC
- `candles_So11111111111111111111111111111111111111112` for SOL

## Configuration

Plugin parameters may be specified as key-value pairs in the `--trigger-arguments` flag (CLI) or in the `trigger_arguments` field (API) when creating a trigger. The plugin also supports TOML configuration files.

### Plugin metadata

This plugin includes a JSON metadata schema in its docstring that defines supported trigger types and configuration parameters. This metadata enables the [InfluxDB 3 Explorer](https://docs.influxdata.com/influxdb3/explorer/) UI to display and configure the plugin.

### Required parameters

| Parameter            | Type   | Default | Description                                                                        |
|----------------------|--------|---------|------------------------------------------------------------------------------------|
| `source_measurement` | string | required | Source measurement containing InfluxProchainDataPoint data                        |
| `target_measurement` | string | required | Destination measurement for candle data                                          |
| `timeframe`          | string | required | Candle timeframe. Supported: '30s', '60s', '300s'                                |
| `window`             | string | required | Time window for each candle generation job. Format: `<number><unit>` (e.g., "1h") |

### Optional parameters

| Parameter         | Type   | Default | Description                                                         |
|-------------------|--------|---------|---------------------------------------------------------------------|
| `offset`          | string | "0s"    | Time offset to apply to the window                                  |
| `mint_filter`     | string | none    | Specific mint address to filter for candle generation               |
| `max_retries`     | integer| 5       | Maximum number of retries for write operations                      |
| `target_database` | string | default | Database for storing candle data                                    |

### TOML configuration

| Parameter          | Type   | Default | Description                                                                      |
|--------------------|--------|---------|----------------------------------------------------------------------------------|
| `config_file_path` | string | none    | TOML config file path relative to `PLUGIN_DIR` (required for TOML configuration) |

*To use a TOML configuration file, set the `PLUGIN_DIR` environment variable and specify the `config_file_path` in the trigger arguments.*

#### Example TOML configuration

[candle_config_scheduler.toml](candle_config_scheduler.toml)

## Installation steps

1. Start InfluxDB 3 with the Processing Engine enabled (`--plugin-dir /path/to/plugins`):

   ```bash
   influxdb3 serve \
     --node-id node0 \
     --object-store file \
     --data-dir ~/.influxdb3 \
     --plugin-dir ~/.plugins
   ```

2. No additional Python packages required for this plugin.

## Trigger setup

### Scheduled candle generation

Run candle generation periodically on historical data:

```bash
# 30-second candles - every 30 minutes
influxdb3 create trigger \
  --database prochain_db \
  --plugin-filename candle_generator.py \
  --trigger-spec "every:30min" \
  --trigger-arguments 'source_measurement=prochain_data,target_measurement=candles_30s,timeframe=30s,window=30min' \
  prochain_candles_30s

# 1-minute candles - every hour
influxdb3 create trigger \
  --database prochain_db \
  --plugin-filename candle_generator.py \
  --trigger-spec "every:1h" \
  --trigger-arguments 'source_measurement=prochain_data,target_measurement=candles_60s,timeframe=60s,window=1h' \
  prochain_candles_60s

# 5-minute candles - every 6 hours
influxdb3 create trigger \
  --database prochain_db \
  --plugin-filename candle_generator.py \
  --trigger-spec "every:6h" \
  --trigger-arguments 'source_measurement=prochain_data,target_measurement=candles_300s,timeframe=300s,window=6h' \
  prochain_candles_300s
```

### Using TOML configuration

```bash
influxdb3 create trigger \
  --database prochain_db \
  --plugin-filename candle_generator.py \
  --trigger-spec "every:1h" \
  --trigger-arguments config_file_path=candle_config_scheduler.toml \
  prochain_candles_30s
```

### HTTP candle generation

Generate candles on-demand via HTTP requests:

```bash
# Create HTTP trigger
influxdb3 create trigger \
  --database prochain_db \
  --plugin-filename candle_generator.py \
  --trigger-spec "http" \
  --trigger-arguments 'source_measurement=prochain_data,target_measurement=candles_30s,timeframe=30s' \
  prochain_candles_http
```

#### HTTP API Usage

**Endpoint**: `POST /api/v2/triggers/{trigger_id}/execute`

**Request Body**:
```json
{
  "start_time": "2024-01-01T00:00:00Z",
  "end_time": "2024-01-01T01:00:00Z",
  "mint_filter": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
}
```

**Response**:
```json
{
  "status": "success",
  "task_id": "a1b2c3d4",
  "candles_generated": 120,
  "timeframe": "30s",
  "source_measurement": "prochain_data",
  "target_measurement": "candles_30s",
  "time_range": {
    "start": "2024-01-01T00:00:00Z",
    "end": "2024-01-01T01:00:00Z"
  }
}
```

## Data Flow

1. **Source Data**: InfluxProchainDataPoint records with price, volume, and metadata
2. **Aggregation**: Data is grouped by timeframe and aggregated into OHLCV candles
3. **Tag Preservation**: All original tags are preserved in the candle records
4. **Output**: Candle data written to target measurement with OHLCV fields

## Example Queries

### View generated candles
```sql
SELECT * FROM "candles_30s"
WHERE time >= NOW() - INTERVAL '1 hour'
ORDER BY time
```

### Compare different timeframes
```sql
SELECT 
    time,
    '30s' as timeframe,
    close
FROM "candles_30s"
WHERE time >= NOW() - INTERVAL '1 day'

UNION ALL

SELECT 
    time,
    '60s' as timeframe,
    close
FROM "candles_60s"
WHERE time >= NOW() - INTERVAL '1 day'

UNION ALL

SELECT 
    time,
    '300s' as timeframe,
    close
FROM "candles_300s"
WHERE time >= NOW() - INTERVAL '1 day'

ORDER BY time
```

### Filter by specific mint
```sql
SELECT time, open, high, low, close, volume
FROM "candles_30s"
WHERE time >= NOW() - INTERVAL '1 hour'
AND mint = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
ORDER BY time
```

## Troubleshooting

### Common Issues

1. **No candles generated**: Check that source measurement contains data in the specified time range
2. **Invalid timeframe**: Ensure timeframe is one of: '30s', '60s', '300s'
3. **Permission errors**: Verify database permissions for read/write operations
4. **Memory issues**: Reduce window size for large datasets

### Logs

The plugin provides detailed logging with task IDs for tracking:
```
[a1b2c3d4] Starting scheduled candle generation at 2024-01-01 12:00:00
[a1b2c3d4] Processing window: 2024-01-01 11:00:00 to 2024-01-01 12:00:00
[a1b2c3d4] Timeframe: 30s
[a1b2c3d4] Source: prochain_data -> Target: candles_30s
[a1b2c3d4] Generated 120 candles
[a1b2c3d4] Successfully wrote 120 candle records to candles_30s
[a1b2c3d4] Candle generation completed successfully
```

## Performance Considerations

- **Window Size**: Larger windows process more data but use more memory
- **Timeframe**: Shorter timeframes generate more candles but require more processing
- **Mint Filtering**: Filtering by mint can significantly reduce processing time
- **Batch Processing**: Consider using multiple triggers with different schedules for optimal performance

## Contributing

This plugin is designed to work with InfluxProchainDataPoint structures. If you need to modify it for different data structures, update the `build_candle_query` function and field mappings accordingly.
