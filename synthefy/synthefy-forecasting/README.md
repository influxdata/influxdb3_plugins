# Synthefy Forecasting Plugin

⚡ http  
🏷️ forecasting, time-series, predictive-analytics  
🔧 InfluxDB 3 Core, InfluxDB 3 Enterprise

## Description

The Synthefy Forecasting Plugin integrates Synthefy Forecasting API with InfluxDB 3 to enable on-demand time series forecasting via HTTP requests. It reads time series data from InfluxDB, generates forecasts using Synthefy's advanced forecasting models and writes the results back to InfluxDB for visualization and alerting.

**Key Features:**

- **On-Demand Forecasting**: Generate forecasts on-demand via HTTP requests
- **Multiple Models**: Support for various Synthefy models
- **Metadata Support**: Use additional fields as covariates for improved forecasting accuracy
- **Tag Filtering**: Filter data by tags (e.g., location, device) for targeted forecasting
- **Line Protocol Writes**: Reliable data writing using InfluxDB Line Protocol

## Configuration

Plugin parameters may be specified as key-value pairs in the `--trigger-arguments` flag (CLI) or in the `trigger_arguments` field (API) when creating a trigger.

### Plugin metadata

This plugin includes a JSON metadata schema in its docstring that defines supported trigger types and configuration parameters. This metadata enables the [InfluxDB 3 Explorer](https://docs.influxdata.com/influxdb3/explorer/) UI to display and configure the plugin.

### HTTP trigger parameters

| Parameter            | Type   | Default  | Description                                                       |
|----------------------|--------|----------|-------------------------------------------------------------------|
| `measurement`        | string | required | Source measurement containing historical data                     |
| `field`              | string | "value"  | Field name to forecast                                            |
| `tags`               | string | ""       | Tag filters, comma-separated (e.g., "location=NYC,device=sensor1") |
| `time_range`         | string | "30d"    | Historical data window. Format: `<number><unit>` (e.g., "30d")    |
| `forecast_horizon`   | string | "7d"     | Forecast duration. Format: `<number><unit>` or "<number> points"  |
| `model`              | string | "sfm-tabular"| Synthefy model to use (e.g., "sfm-tabular", "Migas-latest") |
| `api_key`            | string | required | Synthefy API key (create at [console.synthefy.com/api-keys](https://console.synthefy.com/api-keys) or set SYNTHEFY_API_KEY environment variable)   |
| `output_measurement` | string | "{measurement}_forecast" | Destination measurement for forecast results          |
| `metadata_fields`    | string | ""       | Comma-separated list of metadata field names to use as covariates |
| `database`           | string | ""       | Database name for reading and writing data (optional)             |

## Requirements

### Dependencies

- Python 3.7 or higher
- `pandas` - Data manipulation
- `httpx` or `requests` - HTTP client for API calls

### Installation

```bash
pip install pandas httpx
```

Or using InfluxDB 3 package manager:

```bash
influxdb3 install package pandas
influxdb3 install package httpx
```

### Prerequisites

- InfluxDB 3 Core or Enterprise installed and running
- Synthefy API key: Create one at [https://console.synthefy.com/api-keys](https://console.synthefy.com/api-keys)

## Quick Start / Testing Setup

Before using the plugin, you need to create a database and write some sample data:

```bash
# Create database
influxdb3 create database mydb

# Write sample time series data (7 days of hourly data)
NOW=$(date +%s)
for i in {0..168}; do
  TIMESTAMP=$((NOW - (168 - i) * 3600))000000000
  influxdb3 write --database mydb "temperature,location=NYC value=$((70 + RANDOM % 10)),humidity=$((60 + RANDOM % 15)),pressure=$((1000 + RANDOM % 20)) ${TIMESTAMP}"
done
```

**Note**: In InfluxDB 3, tables/measurements are created automatically when you first write data to them. Make sure you have data in your measurement before running forecasts.

**Quick check if data exists:**
```bash
# Check if measurement has data
influxdb3 query --database mydb "SELECT COUNT(*) FROM temperature"

# Or see a few sample rows
influxdb3 query --database mydb "SELECT * FROM temperature ORDER BY time DESC LIMIT 5"
```

## Trigger Setup

### HTTP trigger

Generate forecasts on-demand via HTTP request:

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename synthefy/synthefy-forecasting/synthefy_forecasting.py \
  --trigger-spec "request:forecast" \
  --trigger-arguments measurement=temperature,field=value,api_key=YOUR_API_KEY \
  temperature_forecast_http
```

Then call via HTTP:

```bash
# Get your token first
TOKEN=$(influxdb3 create token --admin --offline | grep token | cut -d'=' -f2)

# IMPORTANT: Make sure you have written data to the measurement first!
# Tables are created when you first write data. See "Quick Start / Testing Setup" section above.
# Example: influxdb3 write --database mydb "temperature,location=NYC value=72.5"

curl -X POST http://localhost:8181/api/v3/engine/forecast \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "temperature",
    "field": "value",
    "time_range": "30d",
    "forecast_horizon": "7d",
    "model": "sfm-tabular",
    "api_key": "YOUR_SYNTHEFY_API_KEY",
    "database": "mydb"
  }'
```

**Important Notes:**
- Endpoint is `/api/v3/engine/forecast` (matches `request:forecast` trigger spec)
- Include `"database"` in request body for HTTP triggers (trigger context may not be available)
- Authentication is handled automatically by the framework via the Authorization header

## Example Usage

### Basic forecast

Forecast temperature data with default settings:

```bash
# First, ensure you have data in the measurement (see Quick Start section above)
# Example: influxdb3 write --database mydb "temperature,location=NYC value=72.5"

# Create HTTP trigger
influxdb3 create trigger \
  --database mydb \
  --plugin-filename synthefy/synthefy-forecasting/synthefy_forecasting.py \
  --trigger-spec "request:forecast" \
  --trigger-arguments measurement=temperature,field=value,api_key=YOUR_API_KEY \
  temperature_forecast_trigger

# Enable trigger
influxdb3 enable trigger --database mydb temperature_forecast_trigger

# Make HTTP request
TOKEN=$(influxdb3 create token --admin --offline | grep token | cut -d'=' -f2)
curl -X POST http://localhost:8181/api/v3/engine/forecast \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "temperature",
    "field": "value",
    "database": "mydb"
  }'
```

### Forecast with tag filtering

Forecast only for specific location:

```bash
curl -X POST http://localhost:8181/api/v3/engine/forecast \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "temperature",
    "field": "value",
    "tags": "location=NYC",
    "time_range": "30d",
    "database": "mydb"
  }'
```

### Forecast with metadata

Use humidity as a covariate for improved accuracy:

```bash
curl -X POST http://localhost:8181/api/v3/engine/forecast \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "temperature",
    "field": "value",
    "metadata_fields": "humidity,pressure",
    "time_range": "30d",
    "database": "mydb"
  }'
```

### Advanced model

Use advanced foundation models for more accurate forecasts:

```bash
# First, ensure you have data in the sales measurement
# Example: Write sample sales data (90 days of daily data)
# Note: Using 90d instead of 730d to avoid InfluxDB 3 Core file limit (see Troubleshooting)
NOW=$(date +%s)
for i in {0..90}; do
  TIMESTAMP=$((NOW - (90 - i) * 86400))000000000
  influxdb3 write --database mydb "sales,location=NYC revenue=$((1000 + RANDOM % 500)) ${TIMESTAMP}"
done

# Check if data exists
influxdb3 query --database mydb "SELECT COUNT(*) FROM sales"

# Using sfm-tabular (multivariate foundation model)
curl -X POST http://localhost:8181/api/v3/engine/forecast \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "sales",
    "field": "revenue",
    "model": "sfm-tabular",
    "forecast_horizon": "30d",
    "time_range": "90d",
    "database": "mydb"
  }'

# Using Migas-latest (foundation model)
curl -X POST http://localhost:8181/api/v3/engine/forecast \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "sales",
    "field": "revenue",
    "model": "Migas-latest",
    "forecast_horizon": "30d",
    "time_range": "90d",
    "database": "mydb"
  }'
```

## Output Format

Forecasts are written to a new measurement (default: `{measurement}_forecast`) with the following structure:

- **Measurement**: `{measurement}_forecast` (configurable via `output_measurement`)
- **Tags**: Original tags + `model={model_name}`
- **Fields**:
  - `{field_name}`: Forecasted values
  - `value_{quantile}`: Quantile forecasts if available (e.g., `value_0.1`, `value_0.9`)

Example Line Protocol output:

```
temperature_forecast,location=NYC,model=sfm-tabular value=72.5,value_0.1=71.2,value_0.9=73.8 1704672000000000000
```

## Querying Forecasts

Query forecast results:

```sql
SELECT * FROM temperature_forecast 
WHERE time >= now() - INTERVAL '7 days'
ORDER BY time
```

Compare historical and forecasted data:

```sql
SELECT time, value as actual 
FROM temperature 
WHERE time >= now() - INTERVAL '30 days'

UNION ALL

SELECT time, value as forecast 
FROM temperature_forecast 
WHERE time >= now() - INTERVAL '7 days'
ORDER BY time
```

## Supported Models

The plugin supports models available through the Synthefy Forecasting API. Key supported models include:

- `sfm-tabular`: Synthefy Foundation Model for tabular/multivariate time series
- `Migas-latest`: Latest Migas foundation model for time series forecasting

**Note**: Additional models may be available depending on your Synthefy API configuration. Check with [Synthefy documentation](https://docs.synthefy.com) for the most up-to-date model list and availability.

## Troubleshooting

### No data found / Table not found

If you see "No data found" or "table not found" errors:

- **Tables are created when you first write data**: In InfluxDB 3, measurements/tables don't exist until you write data to them. Write some sample data first (see Quick Start section above)
- Check that the measurement exists and has data
- Verify the time range includes data (use longer range like `"730d"` for older data)
- Check tag filters match your data
- Ensure data timestamps are within the specified `time_range` window

### Database name not found

If you see "Database name not found" errors:

- **For HTTP triggers**: Include `"database": "your_db_name"` in the request body
- The database may also be set automatically by the trigger context
- If error persists, specify `database=your_db_name` in trigger arguments

### API errors

If Synthefy API calls fail:

- Verify your API key is correct (create one at [console.synthefy.com/api-keys](https://console.synthefy.com/api-keys))
- Check API URL is accessible
- Review API rate limits
- Check network connectivity

### Write errors

If writes fail:

- Ensure database exists
- Check plugin has write permissions
- Verify Line Protocol format is correct
- Check InfluxDB connection
- Verify the correct database is being used (check logs for "Using database: ...")

### Forecast data not queryable

If you can't query forecast data after successful write:

- Wait a few seconds for table to be created (first write creates the table)
- Verify you're querying the correct database
- Check that forecast was written to `{measurement}_forecast` measurement
- Use: `SELECT * FROM temperature_forecast ORDER BY time LIMIT 10`

### Query file limit exceeded (InfluxDB 3 Core)

If you see "Query would scan X Parquet files, exceeding the file limit" errors:

This is a limitation of **InfluxDB 3 Core** when querying large time ranges with many data files.

**Solutions:**

1. **Use a narrower time range** (recommended):
   ```bash
   # Instead of 730d, use 90d or 180d to stay within file limits
   curl -X POST http://localhost:8181/api/v3/engine/forecast \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '{
       "measurement": "sales",
       "field": "revenue",
       "time_range": "90d",
       "database": "mydb"
     }'
   ```

2. **Increase the file limit** (may cause slower queries):
   ```bash
   influxdb3 query --database mydb --query-file-limit 1000 "SELECT * FROM sales LIMIT 10"
   ```
   Note: This only affects the CLI query command, not plugin queries.

3. **Write data in batches** to reduce file count:
   ```bash
   # Write multiple points per command to reduce file count
   influxdb3 write --database mydb "sales,location=NYC revenue=1000 1700000000000000000\nsales,location=NYC revenue=1100 1700086400000000000"
   ```

4. **Upgrade to InfluxDB 3 Enterprise** (free for non-commercial use):
   - Enterprise automatically compacts files for efficient querying
   - No file limit restrictions
   - See: https://www.influxdata.com/downloads

**Note**: The file limit is a Core limitation to prevent performance degradation. For production use with large datasets, consider InfluxDB 3 Enterprise.

## Limitations

- Currently supports single time series per trigger execution
- Forecast horizon calculation assumes regular time intervals

## License

Apache 2.0

## Support

For issues and questions:

- Plugin issues: Open an issue in the [influxdb3_plugins repository](https://github.com/influxdata/influxdb3_plugins)
- Synthefy API: Contact [Synthefy support](https://synthefy.com) or see [Synthefy documentation](https://docs.synthefy.com)
- InfluxDB: See [InfluxDB documentation](https://docs.influxdata.com/influxdb3/)
