# Notifier Plugin

⚡ http, onwrite
🏷️ notifications, webhooks, messaging, alerts 🔧 InfluxDB 3 Core, InfluxDB 3 Enterprise

## Description

The Notifier Plugin provides multi-channel notification capabilities for InfluxDB 3, enabling real-time alert delivery through various communication channels. Send notifications via Slack, Discord, HTTP webhooks, SMS, or WhatsApp — either via incoming HTTP requests or when rows are written to a table (WAL trigger). Acts as a centralized notification dispatcher that receives data from other plugins or external systems and routes notifications to the appropriate channels.

## Configuration

### HTTP trigger (legacy mode)

This HTTP plugin receives all configuration via the request body. No trigger arguments are required.

### WAL trigger (onwrite mode)

The WAL trigger receives `sender_type` and credential env var names via trigger arguments. No secrets are stored in the table or trigger args — all credentials live in environment variables, referenced by name.

### Plugin metadata

This plugin includes a JSON metadata schema in its docstring that defines supported trigger types and configuration parameters. This metadata enables the [InfluxDB 3 Explorer](https://docs.influxdata.com/influxdb3/explorer/) UI to display and configure the plugin.

### Request body parameters (HTTP trigger)

Send these parameters as JSON in the HTTP POST request body:

| Parameter           | Type   | Default  | Description                                 |
|---------------------|--------|----------|---------------------------------------------|
| `notification_text` | string | required | Text content of the notification message    |
| `senders_config`    | object | required | Configuration for each notification channel |

### Sender-specific configuration (in request body)

The `senders_config` object accepts channel configurations where keys are sender names and values contain channel-specific settings:

#### Slack notifications

| Parameter           | Type   | Default  | Description                 |
|---------------------|--------|----------|-----------------------------|
| `slack_webhook_url` | string | required | Slack webhook URL           |
| `slack_headers`     | string | none     | Base64-encoded JSON headers |

#### Discord notifications

| Parameter             | Type   | Default  | Description                 |
|-----------------------|--------|----------|-----------------------------|
| `discord_webhook_url` | string | required | Discord webhook URL         |
| `discord_headers`     | string | none     | Base64-encoded JSON headers |

#### HTTP webhook notifications

| Parameter          | Type   | Default  | Description                      |
|--------------------|--------|----------|----------------------------------|
| `http_webhook_url` | string | required | Custom webhook URL for HTTP POST |
| `http_headers`     | string | none     | Base64-encoded JSON headers      |

#### SMS notifications (via Twilio)

| Parameter            | Type   | Default  | Description                                       |
|----------------------|--------|----------|---------------------------------------------------|
| `twilio_sid`         | string | required | Twilio Account SID (or use `TWILIO_SID` env var)  |
| `twilio_token`       | string | required | Twilio Auth Token (or use `TWILIO_TOKEN` env var) |
| `twilio_from_number` | string | required | Sender phone number in E.164 format               |
| `twilio_to_number`   | string | required | Recipient phone number in E.164 format            |

#### WhatsApp notifications (via Twilio)

| Parameter            | Type   | Default  | Description                                       |
|----------------------|--------|----------|---------------------------------------------------|
| `twilio_sid`         | string | required | Twilio Account SID (or use `TWILIO_SID` env var)  |
| `twilio_token`       | string | required | Twilio Auth Token (or use `TWILIO_TOKEN` env var) |
| `twilio_from_number` | string | required | Sender WhatsApp number in E.164 format            |
| `twilio_to_number`   | string | required | Recipient WhatsApp number in E.164 format         |

## Software Requirements

- **InfluxDB 3 Core/Enterprise**: with the Processing Engine enabled.
- **Python packages**:
 	- `httpx` (for HTTP requests)
 	- `twilio` (for SMS/WhatsApp notifications)

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
   influxdb3 install package httpx
   influxdb3 install package twilio
   ```

## Trigger setup

### HTTP trigger

Create an HTTP trigger to handle notification requests:

```bash
influxdb3 create trigger \
  --database mydb \
  --path "gh:influxdata/notifier/notifier_plugin.py" \
  --trigger-spec "request:notify" \
  notification_trigger
```

This registers an HTTP endpoint at `/api/v3/engine/notify`.

### WAL trigger

Create a WAL trigger to fire notifications when rows are written to a table. Each trigger is configured for a single sender type; use multiple triggers on the same table to fan out to multiple channels.

**Slack example:**

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin "gh:influxdata/notifier/notifier_plugin.py" \
  --trigger-spec "table:notifications" \
  --trigger-arguments sender_type="slack",webhook_url_env="INFLUXDB3_SLACK_WEBHOOK_URL",headers_env="INFLUXDB3_SLACK_HEADERS" \
  slack_notifier
```

This fires whenever a row is written to the `notifications` table and dispatches a Slack notification using the webhook URL stored in the `INFLUXDB3_SLACK_WEBHOOK_URL` environment variable.

### Enable trigger

```bash
influxdb3 enable trigger --database mydb notification_trigger
```

## Table schema (WAL trigger contract)

When using the WAL trigger, alert plugins write rows to a shared table. The notifier reads these columns from each row:

| Column              | Type         | Required | Description                                                              |
|---------------------|--------------|----------|--------------------------------------------------------------------------|
| `time`              | timestamp    | yes      | When the alert fired (set automatically by InfluxDB on write)            |
| `notification_text` | string field | yes      | The notification message to dispatch                                     |
| `id`                | string field | no       | Unique ID for this alert instance (set by the alert plugin)              |
| `alert_name`        | tag          | no       | Identifies the source, e.g. `"cpu_threshold:prod_monitor"`               |

One row = one notification event. All notifier triggers attached to the table fire on every row. Rows missing `notification_text` are skipped with a warning.

The table name is user-configured when creating the WAL trigger. The plugin processes rows from any table it is attached to.

## WAL trigger arguments

Each WAL trigger is configured with `sender_type` and the names of the environment variables that hold the credentials. The `*_env` keys tell the plugin which env vars to read — the trigger args contain env var **names**, not secret values.

### Slack

| Argument          | Required | Description                                              |
|-------------------|----------|----------------------------------------------------------|
| `sender_type`     | yes      | Must be `"slack"`                                        |
| `webhook_url_env` | yes      | Name of env var holding the Slack webhook URL            |
| `headers_env`     | no       | Name of env var holding Base64-encoded JSON headers      |

### Discord

| Argument          | Required | Description                                              |
|-------------------|----------|----------------------------------------------------------|
| `sender_type`     | yes      | Must be `"discord"`                                      |
| `webhook_url_env` | yes      | Name of env var holding the Discord webhook URL          |
| `headers_env`     | no       | Name of env var holding Base64-encoded JSON headers      |

### HTTP (generic webhook)

| Argument          | Required | Description                                              |
|-------------------|----------|----------------------------------------------------------|
| `sender_type`     | yes      | Must be `"http"`                                         |
| `webhook_url_env` | yes      | Name of env var holding the custom webhook URL           |
| `headers_env`     | no       | Name of env var holding Base64-encoded JSON headers      |

### SMS (via Twilio)

| Argument                | Required | Description                                              |
|-------------------------|----------|----------------------------------------------------------|
| `sender_type`           | yes      | Must be `"sms"`                                          |
| `twilio_sid_env`        | yes      | Name of env var holding the Twilio Account SID           |
| `twilio_token_env`      | yes      | Name of env var holding the Twilio Auth Token            |
| `twilio_from_number_env`| yes      | Name of env var holding the sender phone number (E.164)  |
| `twilio_to_number_env`  | yes      | Name of env var holding the recipient phone number (E.164)|

### WhatsApp (via Twilio)

| Argument                | Required | Description                                                 |
|-------------------------|----------|-------------------------------------------------------------|
| `sender_type`           | yes      | Must be `"whatsapp"`                                        |
| `twilio_sid_env`        | yes      | Name of env var holding the Twilio Account SID              |
| `twilio_token_env`      | yes      | Name of env var holding the Twilio Auth Token               |
| `twilio_from_number_env`| yes      | Name of env var holding the sender WhatsApp number (E.164)  |
| `twilio_to_number_env`  | yes      | Name of env var holding the recipient WhatsApp number (E.164)|

## Pub-sub architecture

The WAL trigger enables a pub-sub notification pattern where the table acts as a message bus:

```
              PUBLISHERS                          SUBSCRIBERS
              (alert plugins write rows)          (notifier triggers fire on writes)

 ┌──────────────┐  ┌──────────────┐       ┌──────────────┐  ┌──────────────┐
 │  threshold   │  │   deadman    │       │    slack     │  │     sms      │
 │  alert       │  │   alert      │       │  notifier    │  │  notifier    │
 │  plugin      │  │   plugin     │       │  (trigger)   │  │  (trigger)   │
 └──────┬───────┘  └──────┬───────┘       └──────┬───────┘  └──────┬───────┘
        │                 │                      │                 │
        └────────┬────────┘                      └────────┬────────┘
                 │                                        │
                 ▼                                        ▼
        ┌────────────────────────────────────────────────────────┐
        │              notifications table                       │
        │              (the contract)                             │
        └────────────────────────────────────────────────────────┘
```

- **Publishers** (alert plugins) write rows containing the notification message and optional metadata
- **Subscribers** (notifier triggers) fire on every write and dispatch via their configured sender
- **Fan-out** is structural: attach N triggers to the same table and every row triggers N notifications
- **Fan-in** is natural: M alert plugins write to the same table and all messages reach all subscribers
- **Secrets never touch the table.** Each notifier trigger owns its credentials via env var names in trigger args and secret values in environment variables

## Example usage

### Example 1: Slack notification (HTTP trigger)

Send a notification to Slack:

```bash
curl -X POST http://localhost:8181/api/v3/engine/notify \
  -H "Authorization: Bearer $INFLUXDB3_AUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "notification_text": "Alert: High CPU usage detected on server1",
    "senders_config": {
      "slack": {
        "slack_webhook_url": "'"$SLACK_WEBHOOK_URL"'"
      }
    }
  }'
```

Set `INFLUXDB3_AUTH_TOKEN` and `SLACK_WEBHOOK_URL` to your credentials.

**Expected output**

Notification sent to Slack channel with message: "Alert: High CPU usage detected on server1"

### Example 2: SMS notification (HTTP trigger)

Send an SMS via Twilio:

```bash
curl -X POST http://localhost:8181/api/v3/engine/notify \
  -H "Authorization: Bearer $INFLUXDB3_AUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "notification_text": "Critical alert: System down",
    "senders_config": {
      "sms": {
        "twilio_from_number": "'"$TWILIO_FROM_NUMBER"'",
        "twilio_to_number": "'"$TWILIO_TO_NUMBER"'"
      }
    }
  }'
```

Set `TWILIO_FROM_NUMBER` and `TWILIO_TO_NUMBER` to your phone numbers. Twilio credentials can be set via `TWILIO_SID` and `TWILIO_TOKEN` environment variables.

### Example 3: Multi-channel notification (HTTP trigger)

Send notifications via multiple channels simultaneously:

```bash
curl -X POST http://localhost:8181/api/v3/engine/notify \
  -H "Authorization: Bearer $INFLUXDB3_AUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "notification_text": "Performance warning: Memory usage above threshold",
    "senders_config": {
      "slack": {
        "slack_webhook_url": "'"$SLACK_WEBHOOK_URL"'"
      },
      "discord": {
        "discord_webhook_url": "'"$DISCORD_WEBHOOK_URL"'"
      }
    }
  }'
```

Set `SLACK_WEBHOOK_URL` and `DISCORD_WEBHOOK_URL` to your webhook URLs.

### Example 4: End-to-end WAL trigger (alert plugin → notifier)

This example shows an alert plugin writing a row to the `notifications` table and the notifier WAL trigger dispatching the notification to Slack.

**Step 1: Set up credentials**

```bash
export INFLUXDB3_SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."
```

**Step 2: Create the WAL trigger**

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin "gh:influxdata/notifier/notifier_plugin.py" \
  --trigger-spec "table:notifications" \
  --trigger-arguments sender_type="slack",webhook_url_env="INFLUXDB3_SLACK_WEBHOOK_URL" \
  slack_notifier

influxdb3 enable trigger --database mydb slack_notifier
```

**Step 3: Alert plugin writes a row (publisher)**

An alert plugin (or any code with write access) writes a row to the `notifications` table:

```python
# Inside an alert plugin's process_scheduled_call or process_writes
influxdb3_local.write_line_protocol(
    "notifications,alert_name=cpu_threshold:prod_monitor "
    "notification_text=\"CPU usage at 95% on server1\","
    "id=\"alert-20260401-001\""
)
```

**Step 4: Notifier fires automatically (subscriber)**

The `slack_notifier` WAL trigger fires on the write, reads `notification_text` from the row, resolves the Slack webhook URL from the `INFLUXDB3_SLACK_WEBHOOK_URL` env var, and dispatches the notification to Slack — no further action required.

## Code overview

### Files

- `notifier_plugin.py`: The main plugin code containing both the WAL trigger and HTTP handler for notification dispatch

### Logging

Logs are stored in the trigger's database in the `system.processing_engine_logs` table. To view logs:

```bash
influxdb3 query --database YOUR_DATABASE "SELECT * FROM system.processing_engine_logs WHERE trigger_name = 'notification_trigger'"
```

### Main functions

#### `process_writes(influxdb3_local, table_batches, args)`

WAL trigger entry point. Called by the InfluxDB Processing Engine each time rows are written to a table the trigger is attached to.

Key operations:

1. Validates `sender_type` is present in `args`; logs error and returns early if missing
2. Resolves credentials once from env vars (reads each `*_env` arg, looks up the corresponding env var)
3. Iterates `table_batches` and rows, skipping any row missing `notification_text`
4. Dispatches all notifications in a single batched call (async senders run concurrently)
5. Logs summary results per sender

Returns `None` (WAL trigger API contract; return value is ignored by the engine).

#### `process_request(influxdb3_local, query_parameters, request_headers, request_body, args)`

HTTP trigger entry point. Routes between two modes based on the presence of `sender_type` in `args`:

**New mode** (`sender_type` present in `args`):

Credentials come from environment variables, referenced by `*_env` trigger args. The request body need only contain `notification_text`.

1. Parses and validates JSON body for `notification_text`; returns error dict if missing or invalid
2. Resolves credentials from env vars using the `*_env` trigger args; returns error dict if resolution fails
3. Builds a single notification request and dispatches it
4. Returns dispatch results

**Legacy mode** (no `sender_type` in `args`):

All credentials and channel configuration come from the request body in `senders_config`. This code path is unchanged — existing integrations continue to work without modification.

Key operations:

1. Validates request body for required `notification_text` and `senders_config`
2. Iterates through sender configurations (Slack, Discord, HTTP, SMS, WhatsApp)
3. Dispatches notifications with built-in retry logic and error handling
4. Returns success/failure status for each channel

## Troubleshooting

### Common issues

#### Issue: Notification not delivered

**Solution**: Verify webhook URLs are correct and accessible. Check Twilio credentials and phone number formats. Review logs for specific error messages.

#### Issue: Authentication errors

**Solution**: Ensure Twilio credentials are set via environment variables or request parameters. Verify webhook URLs have proper authentication if required.

#### Issue: Rate limiting

**Solution**: Plugin includes built-in retry logic with exponential backoff. Consider implementing client-side rate limiting for high-frequency notifications.

#### Issue: WAL trigger rows skipped

**Solution**: Ensure rows written to the notifications table include the `notification_text` field. Rows missing this field are skipped with a warning logged to `system.processing_engine_logs`.

#### Issue: WAL trigger missing `sender_type`

**Solution**: Verify `--trigger-arguments` includes `sender_type=` when creating the WAL trigger. The trigger will log an error and return early if `sender_type` is absent.

### Environment variables

For security, set credentials as environment variables rather than passing them in requests:

```bash
# Slack
export INFLUXDB3_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...

# Discord
export INFLUXDB3_DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...

# Twilio (SMS/WhatsApp)
export INFLUXDB3_TWILIO_SID=your_account_sid
export INFLUXDB3_TWILIO_TOKEN=your_auth_token

# Legacy HTTP trigger (Twilio fallback)
export TWILIO_SID=your_account_sid
export TWILIO_TOKEN=your_auth_token
```

### Viewing logs

Check processing logs in the InfluxDB system tables:

```bash
influxdb3 query --database YOUR_DATABASE "SELECT * FROM system.processing_engine_logs WHERE log_text LIKE '%notifier%' ORDER BY event_time DESC LIMIT 10"
```

## Questions/Comments

For support, open a GitHub issue or contact us via [Discord](https://discord.com/invite/vZe2w2Ds8B) in the `#influxdb3_core` channel, [Slack](https://influxcommunity.slack.com/) in the `#influxdb3_core` channel, or the [Community Forums](https://community.influxdata.com/).
