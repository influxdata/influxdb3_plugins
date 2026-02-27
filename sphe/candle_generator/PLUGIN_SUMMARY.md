# Candle Generator Plugin - Complete Overview

## Plugin Purpose

This InfluxDB 3 plugin is specifically designed to generate OHLCV (Open, High, Low, Close, Volume) candles from `InfluxProchainDataPoint` data structures. It automatically processes **all supported timeframes** (30s, 60s, 120s, 300s, 600s, 900s) in a single trigger, eliminating the need for multiple plugin instances. It can be used for both scheduled batch processing and on-demand HTTP requests.

## File Structure

```
influxdb3/candle_generator/
├── candle_generator.py           # Main plugin implementation
├── candle_config_scheduler.toml  # TOML configuration template
├── README.md                     # Comprehensive documentation
├── setup_example.sh             # Setup script with examples
├── test_plugin.py               # Test script for validation
└── PLUGIN_SUMMARY.md            # This overview file
```

## How It Works

### 1. Data Source: InfluxProchainDataPoint

The plugin reads from measurements containing data with this structure:
```rust
pub struct InfluxProchainDataPoint {
    pub time: DateTime<Utc>,
    pub price: f64,
    pub quote_volume: f64,
    pub base_volume: f64,
    pub key: String,           // tag
    pub object_type: String,   // tag
    pub mint: String,          // tag
    pub is_buy: bool,          // tag
    pub payer: String,         // tag
    pub trx: String,           // tag
    pub platform: String,      // tag
}
```

### 2. Candle Generation Process

1. **Data Source**: Reads from the specified source measurement/table (default: `raw_trades`)
2. **Automatic Window Calculation**: Uses a 1-hour data window (automatically calculated based on 30-second trigger interval)
3. **Multi-Timeframe Processing**: Automatically processes **all supported timeframes** in a single execution
4. **Data Aggregation**: Groups data points by each timeframe (30s, 60s, 120s, 300s, 600s, 900s) from the source table
5. **OHLCV Calculation**:
   - **Open**: First price in the timeframe
   - **High**: Highest price in the timeframe
   - **Low**: Lowest price in the timeframe
   - **Close**: Last price in the timeframe
   - **Volume**: Sum of quote_volume in the timeframe
6. **Essential Tag Preservation**: Preserves only essential tags (mint, platform, timeframe)
7. **Single Measurement**: All candles are written to a single measurement with timeframe as a tag
8. **Output**: Writes candles to a single measurement with timeframe differentiation via tags

### 3. Generated Candle Structure

Each candle contains:
- **Fields**: `open`, `high`, `low`, `close`, `volume`
- **Tags**: Essential tags only (`mint`, `platform`, `timeframe`)
- **Measurement**: All candles are saved to a single measurement (e.g., `candles`) with timeframe as a tag

## Supported Timeframes

- **30s**: 30-second candles
- **60s**: 1-minute candles  
- **120s**: 2-minute candles
- **300s**: 5-minute candles
- **600s**: 10-minute candles
- **900s**: 15-minute candles

## Usage Modes

### Scheduled Mode
Automatically generates candles for **all timeframes** on configurable intervals:
```bash
# Generate all timeframes (30s, 60s, 120s, 300s, 600s, 900s) every 30 seconds
influxdb3 create trigger \
  --database prochain_db \
  --plugin-filename candle_generator.py \
  --trigger-spec "every:30s" \
  --trigger-arguments 'target_measurement=candles,target_database=candles_db,source_measurement=raw_trades' \
  prochain_candles_all_timeframes
```

### HTTP Mode
On-demand candle generation via HTTP API (supports single or all timeframes):
```bash
# Create HTTP trigger for all timeframes
influxdb3 create trigger \
  --database prochain_db \
  --plugin-filename candle_generator.py \
  --trigger-spec "http" \
  --trigger-arguments 'target_measurement=candles,target_database=candles_db,source_measurement=raw_trades' \
  prochain_candles_http

# Or specify a single timeframe for HTTP requests
influxdb3 create trigger \
  --database prochain_db \
  --plugin-filename candle_generator.py \
  --trigger-spec "http" \
  --trigger-arguments 'target_measurement=candles,timeframe=30s,target_database=candles_db,source_measurement=raw_trades' \
  prochain_candles_http_single
```

## Key Features

### 1. Multi-Timeframe Processing
- **Single Trigger, All Timeframes**: One plugin instance processes all 6 timeframes (30s, 60s, 120s, 300s, 600s, 900s)
- **Efficient Resource Usage**: Eliminates the need for 6 separate plugin instances
- **Consistent Data**: All timeframes are processed from the same data window, ensuring consistency
- **Flexible HTTP Mode**: HTTP requests can target all timeframes or specify a single timeframe

### 2. Automatic Window Calculation
- **Smart Window Sizing**: Automatically uses a 1-hour data window based on the 30-second trigger interval
- **Simplified Configuration**: No need to specify window size - it's automatically optimized
- **Optimal Coverage**: 1-hour window ensures sufficient data for all timeframes (up to 15-minute candles)
- **Consistent Processing**: Every execution processes the same amount of historical data

### 3. Single Measurement Architecture
- **Unified Storage**: All candles are stored in a single measurement regardless of timeframe
- **Timeframe Tagging**: Each candle includes a `timeframe` tag (30s, 60s, 120s, 300s, 600s, 900s)
- **Simplified Queries**: Easy to query all timeframes or filter by specific timeframe
- **Efficient Storage**: Reduces measurement proliferation and simplifies data management

### 4. Flexible Configuration
- TOML configuration file support
- Command-line argument support
- Environment variable support

### 5. Robust Error Handling
- Configurable retry logic
- Detailed logging with task IDs
- Graceful error recovery

### 6. Performance Optimizations
- Efficient SQL queries for aggregation using time_bucket functions
- Batch processing capabilities
- Memory-conscious data handling

### 7. Filtering Options
- Mint address filtering
- Time range filtering
- Tag-based filtering

## Example Data Flow

```
Raw InfluxProchainDataPoint Data:
┌─────────────────┬─────────┬─────────────┬─────────┬─────────┬─────────┐
│ time            │ price   │ quote_vol   │ mint    │ is_buy  │ platform│
├─────────────────┼─────────┼─────────────┼─────────┼─────────┼─────────┤
│ 12:00:00        │ 100.0   │ 1000.0      │ TOKEN1  │ true    │ pumpfun │
│ 12:00:15        │ 101.0   │ 1500.0      │ TOKEN1  │ false   │ pumpfun │
│ 12:00:25        │ 99.5    │ 800.0       │ TOKEN1  │ true    │ pumpfun │
│ 12:00:45        │ 102.0   │ 2000.0      │ TOKEN1  │ true    │ pumpfun │
└─────────────────┴─────────┴─────────────┴─────────┴─────────┴─────────┘

Generated 30s Candle (saved to `candles` measurement with timeframe tag):
┌─────────────────┬──────┬──────┬──────┬───────┬─────────┬─────────┬───────────┐
│ time            │ open │ high │ low  │ close │ volume  │ mint    │ timeframe │
├─────────────────┼──────┼──────┼──────┼───────┼─────────┼─────────┼───────────┤
│ 12:00:00        │ 100.0│ 101.0│ 99.5 │ 99.5  │ 3300.0  │ TOKEN1  │ 30s       │
└─────────────────┴──────┴──────┴──────┴───────┴─────────┴─────────┴───────────┘
```

## Integration with Your System

### 1. Prerequisites
- InfluxDB 3 with Processing Engine enabled
- Python 3.8+ (for plugin execution)
- `influxdb_client_3` package

### 2. Setup Steps
1. Copy plugin files to your plugin directory
2. Start InfluxDB 3 with `--plugin-dir` flag
3. Create database and measurements
4. Set up triggers using provided scripts
5. Monitor candle generation

### 3. Monitoring
- Check plugin logs for execution status
- Query generated candles for validation
- Monitor performance metrics

## Customization

The plugin can be customized for different data structures by modifying:
- `build_candle_query()` function for different field names
- `write_candle_data()` function for different output formats
- `parse_timeframe()` function for additional timeframes

## Troubleshooting

Common issues and solutions:
1. **No candles generated**: Check source data exists and timeframes match
2. **Memory issues**: Reduce window size or increase processing intervals
3. **Permission errors**: Verify database and measurement permissions
4. **Invalid timeframes**: Ensure timeframe is one of supported values

## Performance Considerations

- **Window Size**: Balance between data coverage and memory usage
- **Timeframe**: Shorter timeframes = more candles = more processing
- **Filtering**: Use mint filters to reduce data volume
- **Scheduling**: Adjust trigger frequencies based on data volume

This plugin provides a complete solution for generating trading candles from Prochain data, enabling real-time and historical analysis of market movements.
