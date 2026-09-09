#!/usr/bin/env python3
"""
Test script for the Candle Generator Plugin
This script creates sample raw_trades data and tests the candle generation functionality.
"""

import json
import time
from datetime import datetime, timedelta, timezone
from influxdb_client_3 import InfluxDBClient3

def create_sample_data(client, database_name):
    """Create sample raw_trades data for testing."""
    
    # Sample data points over the last hour
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)
    
    # Generate sample data points every 10 seconds
    current_time = start_time
    data_points = []
    
    base_price = 100.0
    price_variation = 5.0
    
    # Sample mints for testing
    mints = [
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "So11111111111111111111111111111111111111112",
        "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So"
    ]
    
    platforms = ["raydium", "orca", "serum"]
    
    while current_time <= end_time:
        # Simulate price movement
        price = base_price + (price_variation * (current_time.minute % 10) / 10.0)
        volume = 1000.0 + (current_time.second * 10)
        
        # Alternate between different mints and platforms
        mint = mints[current_time.second % len(mints)]
        platform = platforms[current_time.second % len(platforms)]
        
        data_point = {
            "measurement": "raw_trades",
            "time": current_time,
            "tags": {
                "mint": mint,
                "platform": platform
            },
            "fields": {
                "price": price,
                "quote_volume": volume
            }
        }
        data_points.append(data_point)
        current_time += timedelta(seconds=10)
    
    # Write data to InfluxDB
    print(f"Writing {len(data_points)} sample raw_trades data points...")
    client.write(data_points, database=database_name)
    print("Sample data written successfully!")
    
    return start_time, end_time

def test_candle_generation(client, database_name, target_measurement):
    """Test candle generation for all timeframes."""
    
    print(f"\nTesting candle generation for {target_measurement}...")
    
    # Query to check if candles were generated
    query = f"""
    SELECT COUNT(*) as candle_count,
           COUNT(DISTINCT timeframe) as timeframe_count,
           timeframe
    FROM "{target_measurement}"
    WHERE time >= NOW() - INTERVAL '1 hour'
    GROUP BY timeframe
    ORDER BY timeframe
    """
    
    try:
        result = client.query(query)
        total_candles = 0
        timeframes_found = []
        
        print("\nCandle generation results:")
        print("-" * 50)
        
        for row in result:
            timeframe = row['timeframe']
            count = row['candle_count']
            total_candles += count
            timeframes_found.append(timeframe)
            print(f"  {timeframe}: {count} candles")
        
        print("-" * 50)
        print(f"Total candles: {total_candles}")
        print(f"Timeframes found: {len(timeframes_found)}")
        
        if total_candles > 0:
            print("✅ Candle generation test PASSED")
            print(f"✅ Single measurement working: {target_measurement}")
            print(f"✅ Timeframe tags working: {timeframes_found}")
            return True
        else:
            print("❌ Candle generation test FAILED - No candles found")
            return False
            
    except Exception as e:
        print(f"❌ Candle generation test FAILED - Error: {e}")
        return False

def test_candle_data_quality(client, database_name, target_measurement):
    """Test the quality and structure of generated candles."""
    
    print(f"\nTesting candle data quality for {target_measurement}...")
    
    # Query to check candle structure
    query = f"""
    SELECT open, high, low, close, volume, mint, platform, timeframe
    FROM "{target_measurement}"
    WHERE time >= NOW() - INTERVAL '1 hour'
    LIMIT 5
    """
    
    try:
        result = client.query(query)
        candles = list(result)
        
        if not candles:
            print("❌ No candles found for quality testing")
            return False
        
        print(f"\nSample candle data (first {len(candles)} candles):")
        print("-" * 80)
        
        for i, candle in enumerate(candles, 1):
            print(f"Candle {i}:")
            print(f"  OHLCV: O={candle['open']:.4f}, H={candle['high']:.4f}, L={candle['low']:.4f}, C={candle['close']:.4f}, V={candle['volume']:.2f}")
            print(f"  Tags: mint={candle['mint']}, platform={candle['platform']}, timeframe={candle['timeframe']}")
            print()
        
        # Validate OHLCV logic
        valid_candles = 0
        for candle in candles:
            high = candle['high']
            low = candle['low']
            open_price = candle['open']
            close_price = candle['close']
            
            # Basic OHLCV validation
            if (high >= low and 
                high >= open_price and 
                high >= close_price and
                low <= open_price and 
                low <= close_price and
                candle['volume'] >= 0):
                valid_candles += 1
        
        print(f"✅ Valid candles: {valid_candles}/{len(candles)}")
        
        if valid_candles == len(candles):
            print("✅ Candle data quality test PASSED")
            return True
        else:
            print("❌ Candle data quality test FAILED - Invalid OHLCV data")
            return False
            
    except Exception as e:
        print(f"❌ Candle data quality test FAILED - Error: {e}")
        return False

def main():
    """Main test function."""
    
    # Configuration
    database_name = "test_prochain_db"
    target_measurement = "candles"  # Single measurement with timeframe tags
    
    # Connect to InfluxDB (adjust connection details as needed)
    try:
        client = InfluxDBClient3(
            host="localhost",
            port=8086,
            database=database_name
        )
        print("Connected to InfluxDB successfully!")
    except Exception as e:
        print(f"Failed to connect to InfluxDB: {e}")
        print("Make sure InfluxDB 3 is running and accessible")
        return
    
    # Create database if it doesn't exist
    try:
        client.database = database_name
        print(f"Using database: {database_name}")
    except Exception as e:
        print(f"Database error: {e}")
        return
    
    # Create sample data
    start_time, end_time = create_sample_data(client, database_name)
    
    print("\n" + "="*60)
    print("TESTING CANDLE GENERATION")
    print("="*60)
    
    # Note: In a real scenario, you would need to trigger the plugin
    print("\nNote: This test assumes the candle generator plugin has been executed manually.")
    print("To test the plugin, you need to:")
    print("1. Set up the plugin triggers using the setup script")
    print("2. Wait for scheduled execution or trigger manually")
    print("3. Run this test script to verify results")
    
    # Test candle generation
    generation_success = test_candle_generation(client, database_name, target_measurement)
    
    # Test candle data quality
    quality_success = test_candle_data_quality(client, database_name, target_measurement)
    
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"Candle Generation: {'✅ PASSED' if generation_success else '❌ FAILED'}")
    print(f"Data Quality: {'✅ PASSED' if quality_success else '❌ FAILED'}")
    
    if generation_success and quality_success:
        print("\n🎉 All tests PASSED!")
    else:
        print("\n⚠️  Some tests FAILED. Check the plugin configuration and execution.")
    
    print("\n" + "="*60)
    print("TEST COMPLETE")
    print("="*60)
    print("\nTo manually test the plugin, you can:")
    print("1. Use the HTTP trigger to generate candles on-demand")
    print("2. Check the generated candles with queries like:")
    print(f"   SELECT * FROM \"{target_measurement}\" WHERE time >= NOW() - INTERVAL '1 hour'")
    print(f"   SELECT * FROM \"{target_measurement}\" WHERE timeframe = '30s' LIMIT 10")
    print("\nSample data time range:")
    print(f"   Start: {start_time}")
    print(f"   End: {end_time}")

if __name__ == "__main__":
    main()
