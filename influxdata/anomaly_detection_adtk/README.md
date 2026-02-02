# Anomaly Detection ADTK Plugin

⚡ scheduled 🏷️ anomaly-detection, machine-learning, time-series 🔧 InfluxDB 3 Core, InfluxDB 3 Enterprise

## Description

The Anomaly Detection ADTK Plugin provides machine learning-based anomaly detection for time series data in InfluxDB 3 using the ADTK (Anomaly Detection Toolkit) library. Detect outliers using algorithms like Isolation Forest, Local Outlier Factor, and One-Class SVM, and write detection results to a separate output table for analysis and alerting.

## Configuration

Plugin parameters may be specified as key-value pairs in the `--trigger-arguments` flag (CLI) or in the `trigger_arguments` field (API) when creating a trigger.

### Plugin metadata

This plugin includes a JSON metadata schema in its docstring that defines supported trigger types and configuration parameters. This metadata enables the [InfluxDB 3 Explorer](https://docs.influxdata.com/influxdb3/explorer/) UI to display and configure the plugin.

### Required parameters

| Parameter      | Type   | Default  | Description                                           |
|----------------|--------|----------|-------------------------------------------------------|
| `database`     | string | required | Target database name for anomaly detection results    |
| `table`        | string | required | Source table name containing time series data         |
| `field`        | string | required | Numeric field name to analyze for anomalies           |
| `output_table` | string | required | Destination table name for anomaly detection results  |

### Optional parameters

| Parameter       | Type    | Default            | Description                                                                           |
|-----------------|---------|--------------------|---------------------------------------------------------------------------------------|
| `detector_type` | string  | `IsolationForestAD`| Anomaly detection algorithm (IsolationForestAD, LocalOutlierFactorAD, OneClassSVMAD) |
| `contamination` | float   | `0.1`              | Expected proportion of anomalies in the dataset (0.0 to 0.5)                          |
| `window_size`   | integer | `10`               | Number of data points to include in detection window                                  |
| `time_column`   | string  | `time`             | Column name containing timestamp values                                               |

## Software Requirements

- **InfluxDB 3 Core/Enterprise**: with the Processing Engine enabled.
- **Python packages**:
	- `adtk` (for anomaly detection)
	- `pandas` (for data manipulation)
	- `scikit-learn` (for machine learning algorithms)

### Installation steps

1. Start InfluxDB 3 with the Processing Engine enabled (`--plugin-dir /path/to/plugins`):

   ```bash
   influxdb3 serve \
     --node-id node0 \
     --object-store file \
     --data-dir ~/.influxdb3 \
     --plugin-dir ~/.plugins
   ```

2. Install required Python packages:

   ```bash
   influxdb3 install package adtk
   influxdb3 install package pandas
   influxdb3 install package scikit-learn
   ```

## Trigger setup

### Scheduled trigger

Create a scheduled trigger for periodic anomaly detection:

```bash
influxdb3 create trigger \
  --database mydb \
  --path "gh:influxdata/anomaly_detection_adtk/anomaly_detection_adtk.py" \
  --trigger-spec "every:5m" \
  --trigger-arguments "database=mydb,table=sensor_data,field=temperature,output_table=anomalies,detector_type=IsolationForestAD,contamination=0.1,window_size=20" \
  anomaly_detector
```

### Enable trigger

```bash
influxdb3 enable trigger --database mydb anomaly_detector
```

## Example usage

### Example 1: Basic anomaly detection with Isolation Forest

Write test data and detect anomalies:

```bash
# Write normal sensor data
influxdb3 write \
  --database mydb \
  "sensor_data,location=factory temperature=22.5"

influxdb3 write \
  --database mydb \
  "sensor_data,location=factory temperature=23.1"

influxdb3 write \
  --database mydb \
  "sensor_data,location=factory temperature=85.0"  # Anomaly

# Create and enable the trigger
influxdb3 create trigger \
  --database mydb \
  --path "gh:influxdata/anomaly_detection_adtk/anomaly_detection_adtk.py" \
  --trigger-spec "every:5m" \
  --trigger-arguments "database=mydb,table=sensor_data,field=temperature,output_table=temperature_anomalies" \
  temp_anomaly_detector

influxdb3 enable trigger --database mydb temp_anomaly_detector

# Query anomaly detection results (after trigger runs)
influxdb3 query \
  --database mydb \
  "SELECT * FROM temperature_anomalies ORDER BY time DESC LIMIT 5"
```

**Expected output**

```
+----------------------+-------------+-------+----------+
| time                 | temperature | score | is_anomaly|
+----------------------+-------------+-------+----------+
| 2025-06-01T10:02:00Z | 85.0        | -0.95 | true     |
| 2025-06-01T10:01:00Z | 23.1        | 0.12  | false    |
| 2025-06-01T10:00:00Z | 22.5        | 0.08  | false    |
+----------------------+-------------+-------+----------+
```

### Example 2: Local Outlier Factor detection

Use Local Outlier Factor for density-based anomaly detection:

```bash
influxdb3 create trigger \
  --database monitoring \
  --path "gh:influxdata/anomaly_detection_adtk/anomaly_detection_adtk.py" \
  --trigger-spec "every:10m" \
  --trigger-arguments "database=monitoring,table=cpu_metrics,field=usage,output_table=cpu_anomalies,detector_type=LocalOutlierFactorAD,contamination=0.05,window_size=30" \
  cpu_lof_detector
```

### Example 3: One-Class SVM detection

Use One-Class SVM for novelty detection:

```bash
influxdb3 create trigger \
  --database production \
  --path "gh:influxdata/anomaly_detection_adtk/anomaly_detection_adtk.py" \
  --trigger-spec "every:15m" \
  --trigger-arguments "database=production,table=network_traffic,field=bytes_in,output_table=traffic_anomalies,detector_type=OneClassSVMAD,contamination=0.02,window_size=50" \
  network_svm_detector
```

## Code overview

### Files

- `anomaly_detection_adtk.py`: The main plugin code containing the scheduled handler for anomaly detection

### Logging

Logs are stored in the `_internal` database in the `system.processing_engine_logs` table. To view logs:

```bash
influxdb3 query --database _internal "SELECT * FROM system.processing_engine_logs WHERE trigger_name = 'anomaly_detector'"
```

### Main functions

#### `process_scheduled_call(influxdb3_local, call_time, args)`

Handles scheduled anomaly detection tasks. Queries data from the source table, applies the configured detection algorithm, and writes results to the output table.

Key operations:

1. Parses configuration from trigger arguments
2. Queries source data within the detection window
3. Applies the specified anomaly detection algorithm
4. Writes detection results with anomaly scores to output table

## Troubleshooting

### Common issues

#### Issue: Insufficient data for detection

**Solution**: Ensure the source table has enough data points. The `window_size` parameter determines the minimum data required. Increase the time window or reduce window_size.

#### Issue: Too many false positives

**Solution**: Decrease the `contamination` parameter (for example, from 0.1 to 0.05). Increase `window_size` for more stable detection. Try a different `detector_type` suited to your data characteristics.

#### Issue: Missing dependencies

**Solution**: Install required packages:

```bash
influxdb3 install package adtk
influxdb3 install package pandas
influxdb3 install package scikit-learn
```

#### Issue: No anomalies detected

**Solution**: Increase the `contamination` parameter. Verify the source data contains actual anomalies. Check that the `field` parameter matches an existing numeric field.

### Debugging tips

1. **Check source data availability**:
   ```bash
   influxdb3 query --database mydb "SELECT COUNT(*) FROM sensor_data WHERE time >= now() - interval '1 hour'"
   ```

2. **Verify output table**:
   ```bash
   influxdb3 query --database mydb "SELECT * FROM anomalies ORDER BY time DESC LIMIT 10"
   ```

3. **Review plugin logs**:
   ```bash
   influxdb3 query --database _internal "SELECT * FROM system.processing_engine_logs WHERE trigger_name = 'anomaly_detector' ORDER BY time DESC LIMIT 10"
   ```

## Questions/Comments

For additional support, see the [Support section](../README.md#support).
