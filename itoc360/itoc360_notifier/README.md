# ITOC360 Notifier Plugin

Evaluates a threshold check on a scheduled interval and sends alert and resolve events to
[ITOC360](https://itoc360.com/), an on-call and incident management platform. Each tag
combination is tracked separately, so one trigger can watch many hosts or regions and each
one opens and closes its own incident.

## Prerequisites

- InfluxDB 3 Core or Enterprise with the Processing Engine enabled
- An ITOC360 account with an InfluxDB integration source, which provides the endpoint URL
  and source token

## Installation

1. Copy the plugin into your plugin directory:

   ```bash
   influxdb3 install plugin gh:itoc360/itoc360_notifier/itoc360_notifier.py
   ```

2. Install the Python dependency:

   ```bash
   influxdb3 install package requests
   ```

3. Create the trigger:

   ```bash
   influxdb3 create trigger \
     --database mydb \
     --plugin-filename itoc360_notifier.py \
     --trigger-spec "every:1m" \
     --trigger-arguments measurement=cpu,field=usage_percent,check_name="CPU Threshold",window=5min,crit_threshold=90,warn_threshold=75,group_by_tags=host.region,itoc360_url="https://api.itoc360.app/functions/v1/events?token=YOUR_SOURCE_TOKEN" \
     cpu_threshold
   ```

4. Enable it:

   ```bash
   influxdb3 enable trigger --database mydb cpu_threshold
   ```

## Configuration

| Argument | Required | Default | Description |
|---|---|---|---|
| `measurement` | yes | | Measurement (table) to evaluate |
| `field` | yes | | Numeric field to aggregate |
| `itoc360_url` | yes | | ITOC360 endpoint including the `token` query parameter |
| `check_name` | yes | | Display name; also forms the check identity |
| `window` | yes | | Look-back window, `<number><unit>` with unit `s`, `min`, `h` or `d` |
| `crit_threshold` | yes | | Threshold raising a `crit` level alert |
| `warn_threshold` | no | disabled | Threshold raising a `warn` level alert |
| `operator` | no | `gt` | `gt` or `lt` |
| `aggregation` | no | `avg` | `avg`, `max`, `min`, `sum` or `count` |
| `group_by_tags` | no | none | Dot separated tag keys, for example `host.region` |
| `dry_run` | no | `false` | Evaluate and log without sending |
| `max_retries` | no | `3` | HTTP delivery attempts |
| `request_timeout` | no | `10` | Request timeout in seconds |

## Event format

The plugin posts the body that the ITOC360 InfluxDB provider expects, which is the same
schema used by InfluxDB v2 Checks and Notifications:

```json
{
  "_check_id": "cpu_threshold:cpu:host=server-01,region=eu",
  "_check_name": "CPU Threshold",
  "_type": "threshold",
  "_level": "crit",
  "_message": "avg(usage_percent) > 90 (actual: 94.2)",
  "_time": "2026-08-31T14:30:00Z",
  "_source_measurement": "cpu"
}
```

When the series returns below the threshold, the same `_check_id` is sent with
`"_level": "ok"`, which resolves the incident in ITOC360.

`_level` maps to incident priority as follows: `crit` to critical, `warn` to medium, `info`
and `ok` to low.

### Check identity

`_check_id` is built as `<check_slug>:<measurement>:<sorted tag key=value list>`. ITOC360
fingerprints the incident from this string, so it must be identical between an alert and its
resolve. Tags are sorted for that reason, and no timestamp or measured value is included.

An event is sent only when a series changes level, so a sustained breach does not re-notify
on every interval.

## Testing

Validate without creating a trigger:

```bash
influxdb3 test schedule_plugin \
  --database mydb \
  --input-arguments measurement=cpu,field=usage_percent,check_name="CPU Threshold",window=5min,crit_threshold=90,dry_run=true,itoc360_url="https://api.itoc360.app/functions/v1/events?token=YOUR_SOURCE_TOKEN" \
  itoc360_notifier.py
```

With `dry_run=true` the plugin logs the payload it would send instead of sending it.

## Logging

The plugin writes to `system.processing_engine_logs`:

```bash
influxdb3 query --database mydb \
  "SELECT * FROM system.processing_engine_logs WHERE trigger_name = 'cpu_threshold' ORDER BY time DESC LIMIT 20"
```

The source token is stripped from every log line, since these logs are queryable by anyone
with query access to the database.

## Known limitations

- Level state is held in the plugin cache, which does not survive a server restart. If a
  series is breaching before a restart and healthy afterwards, the resolve event is not sent
  and the incident stays open in ITOC360 until the check breaches and recovers again.
- One field per trigger. Watch several fields by creating several triggers.
- Retries are safe to repeat because ITOC360 deduplicates on the check identity, but a
  delivery that fails all attempts is dropped rather than queued.

## License

MIT or Apache 2.0, at the user's choosing.
