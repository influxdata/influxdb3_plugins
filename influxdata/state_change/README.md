# State Change Plugin

⚡ scheduled, data-write  
🏷️ monitoring, alerting, threshold-detection, state-tracking
🔧 InfluxDB 3 Core, InfluxDB 3 Enterprise

## Description

The State Change Plugin provides comprehensive field monitoring and threshold detection for InfluxDB 3 data streams.
Detect field value changes, monitor threshold conditions, and trigger notifications when specified criteria are met.
Supports both scheduled batch monitoring and real-time data write monitoring with configurable stability checks and multi-channel alerts.

### Plugin metadata

This plugin includes a JSON metadata schema in its docstring that defines supported trigger types and configuration parameters. This metadata enables the InfluxDB 3 Explorer UI to display and configure the plugin.

## Configuration

### Scheduled trigger parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `measurement` | string | required | Measurement to monitor for field changes |
| `field_change_count` | string | required | Dot-separated field thresholds (e.g., "temp:3.load:2") |
| `senders` | string | required | Dot-separated notification channels |
| `window` | string | required | Time window for analysis. Format: `<number><unit>` |

### Data write trigger parameters  

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `measurement` | string | required | Measurement to monitor for threshold conditions |
| `field_thresholds` | string | required | Threshold conditions (e.g., "temp:30:10@status:ok:1h") |
| `senders` | string | required | Dot-separated notification channels |

### Notification parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `influxdb3_auth_token` | string | env var | InfluxDB 3 API token |
| `notification_text` | string | template | Message template for scheduled notifications |
| `notification_count_text` | string | template | Message template for count-based notifications |
| `notification_time_text` | string | template | Message template for time-based notifications |
| `notification_path` | string | "notify" | Notification endpoint path |
| `port_override` | number | 8181 | InfluxDB port override |

### Advanced parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `state_change_window` | number | 1 | Recent values to check for stability |
| `state_change_count` | number | 1 | Max changes allowed within stability window |
| `config_file_path` | string | none | TOML config file path relative to PLUGIN_DIR |

### Channel-specific configuration

Notification channels require additional parameters based on the sender type (same as the [Notifier Plugin](../notifier/README.md)).

## Installation

### Install dependencies

Install required Python packages:

```bash
influxdb3 install package requests
```

### Create scheduled trigger

Create a trigger for periodic field change monitoring:

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename state_change_check_plugin.py \
  --trigger-spec "every:10m" \
  --trigger-arguments "measurement=cpu,field_change_count=temp:3.load:2,window=10m,senders=slack,slack_webhook_url=https://hooks.slack.com/services/..." \
  state_change_scheduler
```

### Create data write trigger

Create a trigger for real-time threshold monitoring:

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename state_change_check_plugin.py \
  --trigger-spec "all_tables" \
  --trigger-arguments "measurement=cpu,field_thresholds=temp:30:10@status:ok:1h,senders=slack,slack_webhook_url=https://hooks.slack.com/services/..." \
  state_change_datawrite
```

### Enable triggers

```bash
influxdb3 enable trigger --database mydb state_change_scheduler
influxdb3 enable trigger --database mydb state_change_datawrite
```

## Examples

### Scheduled field change monitoring

Monitor field changes over a time window and alert when thresholds are exceeded:

```bash
influxdb3 create trigger \
  --database sensors \
  --plugin-filename state_change_check_plugin.py \
  --trigger-spec "every:15m" \
  --trigger-arguments "measurement=temperature,field_change_count=value:5,window=1h,senders=slack,slack_webhook_url=https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX,notification_text=Temperature sensor $field changed $changes times in $window for tags $tags" \
  temp_change_monitor
```

### Real-time threshold detection

Monitor data writes for threshold conditions:

```bash
influxdb3 create trigger \
  --database monitoring \
  --plugin-filename state_change_check_plugin.py \
  --trigger-spec "all_tables" \
  --trigger-arguments "measurement=system_metrics,field_thresholds=cpu_usage:80:5@memory_usage:90:10min,senders=discord,discord_webhook_url=https://discord.com/api/webhooks/..." \
  system_threshold_monitor
```

### Multi-condition monitoring

Monitor multiple fields with different threshold types:

```bash
influxdb3 create trigger \
  --database application \
  --plugin-filename state_change_check_plugin.py \
  --trigger-spec "all_tables" \
  --trigger-arguments "measurement=app_health,field_thresholds=error_rate:0.05:3@response_time:500:30s@status:down:1,senders=slack.sms,slack_webhook_url=https://hooks.slack.com/services/...,twilio_from_number=+1234567890,twilio_to_number=+0987654321" \
  app_health_monitor
```

## Features

- **Dual monitoring modes**: Scheduled batch monitoring and real-time data write monitoring
- **Flexible thresholds**: Support for count-based and duration-based conditions
- **Stability checks**: Configurable state change detection to reduce noise
- **Multi-channel alerts**: Integration with Slack, Discord, HTTP, SMS, and WhatsApp
- **Template notifications**: Customizable message templates with dynamic variables
- **Caching optimization**: Measurement and tag name caching for improved performance
- **Environment variable support**: Credential management via environment variables

## Troubleshooting

### Common issues

**No notifications triggered**
- Verify notification channel configuration (webhook URLs, credentials)
- Check threshold values are appropriate for your data
- Ensure the Notifier Plugin is installed and configured
- Review plugin logs for error messages

**Too many notifications**
- Adjust `state_change_window` and `state_change_count` for stability filtering
- Increase threshold values to reduce sensitivity
- Consider longer monitoring windows for scheduled triggers

**Authentication errors**
- Set `INFLUXDB3_AUTH_TOKEN` environment variable
- Verify token has appropriate database permissions
- Check Twilio credentials for SMS/WhatsApp notifications

### Field threshold formats

**Count-based thresholds**
- Format: `field_name:"value":count`
- Example: `temp:"30.5":10` (10 occurrences of temperature = 30.5)

**Time-based thresholds**  
- Format: `field_name:"value":duration`
- Example: `status:"error":5min` (status = error for 5 minutes)
- Supported units: `s`, `min`, `h`, `d`, `w`

**Multiple conditions**
- Separate with `@`: `temp:"30":5@humidity:"high":10min`

### Message template variables

**Scheduled notifications**
- `$table`: Measurement name
- `$field`: Field name
- `$changes`: Number of changes detected
- `$window`: Time window
- `$tags`: Tag values

**Data write notifications**
- `$table`: Measurement name
- `$field`: Field name  
- `$value`: Threshold value
- `$duration`: Time duration or count
- `$row`: Unique row identifier

## Questions/Comments
For support, open a GitHub issue or contact us via [Discord](https://discord.com/invite/vZe2w2Ds8B) in the `#influxdb3_core` channel, [Slack](https://influxcommunity.slack.com/) in the `#influxdb3_core` channel, or the [Community Forums](https://community.influxdata.com/).