# Alert Plugin for InfluxDB 3

A flexible notification plugin that supports sending alerts to Slack and Discord when data points exceed specified thresholds.

## Features
- Supports Slack and Discord webhooks
- Configurable thresholds and field monitoring
- Customizable notification messages
- Retries with exponential backoff
- Environment variable support for webhook URLs

## Prerequisites

Before using the plugin, ensure you have installed the required httpx package.  This is a prerequisite for the InfluxDB plugin to function correctly. You can install it using:
```bash
influxdb3 install package httpx
```

## Setup

### Slack Setup
You can either:

1. Use the public testing webhook (for development only):
```bash
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/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"
```
This webhook posts to the #notifications-testing channel in the InfluxData Slack community.

2. Or use your own webhook:
```bash
# For Bash/ZSH
export SLACK_WEBHOOK_URL="your-slack-webhook-url"

# For Windows CMD
set SLACK_WEBHOOK_URL=your-slack-webhook-url

# For Windows PowerShell
$env:SLACK_WEBHOOK_URL="your-slack-webhook-url"
```

### Discord Setup
Set your Discord webhook URL as an environment variable:
```bash
# For Bash/ZSH
export DISCORD_WEBHOOK_URL="your-discord-webhook-url"

# For Windows CMD
set DISCORD_WEBHOOK_URL=your-discord-webhook-url

# For Windows PowerShell
$env:DISCORD_WEBHOOK_URL="your-discord-webhook-url"
```

## Testing

Test the plugin using sample data with the InfluxDB 3 CLI:

```bash
influxdb3 test wal_plugin \
  --database my_database \
  --lp="sensor_data,sensor=TempSensor1,location=living_room temperature=25 123456789" \
  --input-arguments "name=temp_alert,endpoint_type=slack,threshold=20,field_name=temperature,notification_text=Alert Temperature too high,webhook_url=$SLACK_WEBHOOK_URL" \
  alert.py
```

## Configuration Parameters

Required:
- `name`: Unique identifier for the alert plugin
- `endpoint_type`: Service to send alerts to (`slack`, `discord`, or `http`)
- `field_name`: Name of the field to monitor
- `threshold`: Numeric threshold value

Optional:
- `notification_text`: Custom alert message (supports ${variable} interpolation)
- `webhook_url`: Override webhook URL from environment variable
- `headers`: Base64 encoded headers for custom HTTP endpoints

## Example Use Cases

1. Slack Temperature Alert:
```bash
influxdb3 test wal_plugin \
  --database my_database \
  --lp="sensor_data,sensor=TempSensor1,location=living_room temperature=25 123456789" \
  --input-arguments "name=temp_alert_slack,endpoint_type=slack,threshold=25,field_name=temperature,notification_text=Temperature ${temperature} exceeds threshold,webhook_url=$SLACK_WEBHOOK_URL" \
  alert.py
```

2. Discord CPU Usage Alert with Alert History:
```bash
influxdb3 test wal_plugin \
  --database my_database \
  --lp="sensor_data,sensor=CPU1,location=server_room cpu_usage=85 123456789" \
  --input-arguments "name=cpu_alert_discord,endpoint_type=discord,threshold=80,field_name=cpu_usage,notification_text=High CPU usage: ${cpu_usage},alert_db=alerts_history,webhook_url=$DISCORD_WEBHOOK_URL" \
  alert.py
```

3. HTTP Endpoint with Custom Headers:
```bash
# First, create base64 encoded headers
HEADERS_JSON=$(echo -n '{"header1":"example1","header2":"example2"}' | base64)

# Then run the test
influxdb3 test wal_plugin \
  --database my_database \
  --lp="sensor_data,sensor=TempSensor1,location=living_room temperature=25 123456789" \
  --input-arguments "name=test_http,endpoint_type=http,threshold=20,field_name=temperature,notification_text=Test,webhook_url=http://host.docker.internal:8000,headers=${HEADERS_JSON}" \
  alert.py
```

When an alert triggers, it writes to the alerts_history database.

### Full Example with Setting up Alert History Database

The alert history database can be used to monitor and track all triggered alerts. You can name this database anything you want (e.g., alert_logs, notification_history, etc.).

1. Install required package:
```bash
influxdb3 install package httpx
```

2. Create your alert history database:
```bash
influxdb3 create database alerts_history
```

3. Create and enable a trigger:
```bash
# Create trigger
influxdb3 create trigger \
  --database my_database \
  --plugin-filename alert.py \
  --trigger-spec "table:sensor_data" \
  --trigger-arguments "name=temp_alert_trigger,endpoint_type=discord,threshold=20,field_name=temperature,notification_text=Value is \${temperature},webhook_url=https://discord.com/api/webhooks/YOUR_WEBHOOK_URL,alert_db=alerts_history" \
  temperature_alert

# Enable trigger
influxdb3 enable trigger --database my_database temperature_alert
```

4. Write test data and query results:
```bash
# Write data
influxdb3 write --database my_database "sensor_data,sensor=TempSensor1,location=living_room temperature=25"

# Query alerts history
influxdb3 query --database alerts_history "select * from sensor_data"
```

The alerts_history output will be formatted as follows:

| Field         | Example Value                    | Description                        |
|---------------|----------------------------------|------------------------------------|
| alert_message | "Value is 25.0"                  | The formatted notification message |
| location      | "living_room"                    | Original location tag              |
| plugin_name   | "temp_alert"                     | Name of the alert plugin          |
| processed_at  | "2025-02-25T00:25:45.497805"    | When alert was processed (UTC)    |
| sensor        | "TempSensor1"                    | Original sensor tag               |
| temperature   | 25.0                             | Original field value              |
| time          | "2025-02-25T00:25:43.885895466" | Original timestamp                |