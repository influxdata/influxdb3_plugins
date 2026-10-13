#!/bin/bash

# Candle Generator Plugin Setup Example
# This script demonstrates how to set up the candle generator plugin for InfluxDB 3

echo "Setting up Candle Generator Plugin for InfluxDB 3..."

# Set your database name
DATABASE_NAME="prochain_db"
PLUGIN_DIR="$(pwd)"

echo "Using database: $DATABASE_NAME"
echo "Plugin directory: $PLUGIN_DIR"

# Create database if it doesn't exist
echo "Creating database if it doesn't exist..."
influxdb3 database create --name "$DATABASE_NAME" 2>/dev/null || echo "Database already exists"

# Set up triggers for different timeframes

echo "Setting up 30-second candle trigger..."
influxdb3 create trigger \
  --database "$DATABASE_NAME" \
  --plugin-filename "$PLUGIN_DIR/candle_generator.py" \
  --trigger-spec "every:30min" \
  --trigger-arguments "source_measurement=prochain_data,target_measurement=candles_30s,timeframe=30s,window=30min" \
  prochain_candles_30s

echo "Setting up 1-minute candle trigger..."
influxdb3 create trigger \
  --database "$DATABASE_NAME" \
  --plugin-filename "$PLUGIN_DIR/candle_generator.py" \
  --trigger-spec "every:1h" \
  --trigger-arguments "source_measurement=prochain_data,target_measurement=candles_60s,timeframe=60s,window=1h" \
  prochain_candles_60s

echo "Setting up 5-minute candle trigger..."
influxdb3 create trigger \
  --database "$DATABASE_NAME" \
  --plugin-filename "$PLUGIN_DIR/candle_generator.py" \
  --trigger-spec "every:6h" \
  --trigger-arguments "source_measurement=prochain_data,target_measurement=candles_300s,timeframe=300s,window=6h" \
  prochain_candles_300s

echo "Setting up HTTP trigger for on-demand candle generation..."
influxdb3 create trigger \
  --database "$DATABASE_NAME" \
  --plugin-filename "$PLUGIN_DIR/candle_generator.py" \
  --trigger-spec "http" \
  --trigger-arguments "source_measurement=prochain_data,target_measurement=candles_30s,timeframe=30s" \
  prochain_candles_http

echo ""
echo "Setup complete! Created triggers:"
echo "- prochain_candles_30s (30-second candles, every 30 minutes)"
echo "- prochain_candles_60s (1-minute candles, every hour)"
echo "- prochain_candles_300s (5-minute candles, every 6 hours)"
echo "- prochain_candles_http (HTTP endpoint for on-demand generation)"
echo ""
echo "To list all triggers:"
echo "influxdb3 trigger list --database $DATABASE_NAME"
echo ""
echo "To test HTTP generation, use:"
echo "curl -X POST http://localhost:8086/api/v2/triggers/{trigger_id}/execute \\"
echo "  -H 'Content-Type: application/json' \\"
echo "  -d '{\"start_time\": \"2024-01-01T00:00:00Z\", \"end_time\": \"2024-01-01T01:00:00Z\"}'"
