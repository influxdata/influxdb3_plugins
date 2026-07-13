# Nori Regression Plugin

⚡ scheduled, http 🏷️ regression, tabular, machine-learning 🔧 InfluxDB 3 Core, InfluxDB 3 Enterprise

Predict a numeric field in an InfluxDB 3 measurement from other columns with **Nori**, Synthefy's
in-context-learning tabular regression model, called through the Synthefy inference gateway. The
plugin reads a window of rows, trains on the rows where the target field is present, predicts the
rows where it is null (imputation / backfill), and writes the predicted values back into InfluxDB.

## Description

Nori is a tabular regression foundation model: you give it labeled feature rows (`X_train`,
`y_train`) and query rows (`X_test`) in a single request, and it predicts a value for each query row
in one forward pass, with no training or fine-tuning.

This plugin applies Nori to an InfluxDB measurement. You choose a target field and a set of feature
columns; the plugin uses the rows where the target is present as the in-context training set and
predicts the target for the rows where it is null, writing each prediction back at its own row's
timestamp. It is plain tabular regression: Nori sees only the feature columns you name, with no time
or ordering assumptions.

Typical uses:

- Backfill a field that dropped out (a sensor went offline while its neighbors kept reporting).
- Impute a missing metric from correlated ones (for example, predict `pressure` from `temperature`
  and `humidity`).
- Derive a field that is expensive to measure directly from cheaper ones recorded alongside it.

Key features:

- **In-context tabular regression**: no training step; the recent labeled rows are the context.
- **Imputation / backfill**: predicts the rows where the target is null and writes them back.
- **Scheduled or on-demand**: run on an interval (scheduled trigger) or via an HTTP endpoint.
- **Tag filtering**: operate on a single series selected by tags.
- **Writes results back** to InfluxDB as a new measurement using line protocol.

## Configuration

### Plugin metadata

This plugin includes a JSON metadata schema in its docstring that declares the supported trigger
types (`scheduled`, `http`) and their arguments, so the InfluxDB 3 Explorer UI can render a
configuration form.

### Authentication for the Nori gateway

The Nori gateway API key is a secret and is **never** read from trigger arguments or the request
body (both are logged). It is resolved, in order:

1. an incoming `X-Nori-Api-Key: <key>` request header (HTTP trigger only), then
2. the `NORI_API_KEY` environment variable set on the InfluxDB host (required for the scheduled
   trigger).

The key is intentionally **not** accepted in the `Authorization` header: InfluxDB parses
`Authorization` for its own request authorization, so a key placed there never reaches the plugin.
Use the custom `X-Nori-Api-Key` header instead.

Get a Nori API key from the [Synthefy console](https://console.synthefy.com/). A single key works
for all Nori model variants (see [Supported models](#supported-models)), so you do not need a
separate key per variant. This plugin does not create keys.

### Scheduled trigger parameters

| Parameter | Default | Description |
|---|---|---|
| `measurement` | (required) | Source measurement (table) to read from. |
| `field` | (required) | The numeric field to predict (the regression target). The plugin trains on rows where it is present and predicts the rows where it is null. |
| `feature_fields` | (required) | Numeric feature columns (X) used to predict `field`, **space-separated** (e.g. `temp humidity`). Use spaces, not commas (`--trigger-arguments` splits argument pairs on commas) and not dots (field names may contain a `.`). In the HTTP JSON body it may also be a list. |
| `window` | `30d` | Time window of rows to read. Units: `s,min,h,d,w`. |
| `tags` | (none) | Filter to a single series. Format: `key:val key2:val2` (space-separated pairs). Required if the measurement holds more than one series (see [Notes](#notes)). |
| `model` | `synthefy/nori` | The Nori gateway slug to call. Your API key must be granted this slug. |
| `gateway_url` | `https://inference.baseten.co/predict` | Nori gateway endpoint. |
| `output_measurement` | `<measurement>_regressed` | Measurement to write predictions to. |
| `target_database` | (trigger db) | Write predictions to a different database. |
| `dry_run` | `false` | If `true`, log predictions but do not write them. |
| `min_history` | `50` | Minimum labeled rows (target present) required to train; skip below this. |

### HTTP trigger parameters

All scheduled parameters are accepted (in the JSON request body or as trigger arguments), plus:

| Parameter | Default | Description |
|---|---|---|
| `start_time` | (none) | ISO start of the window. Overrides `window`. |
| `end_time` | (none) | ISO end of the window. |

Scalar values in the JSON body override trigger arguments. A `tags` object may be sent as JSON
(`{"tags": {"site": "A"}}`), and `feature_fields` may be sent as a JSON list
(`{"feature_fields": ["temp", "humidity"]}`) or a space-separated string.

## Requirements

### Dependencies

- `requests`
- `pandas`
- `numpy`

### Installation steps

1. Install the Python dependencies into the InfluxDB 3 Processing Engine environment:

   ```bash
   influxdb3 install package requests pandas numpy
   ```

2. Reference the plugin directly from this repository with the `gh:` prefix (the form used in the
   examples below): `--path "gh:influxdata/nori_regression/nori_regression.py"`. Alternatively, copy
   `nori_regression.py` into your plugin directory (the one passed to `influxdb3 serve
   --plugin-dir`) and use `--path nori_regression.py`.

3. Set the Nori gateway key on the InfluxDB host. Get your key from the
   [Synthefy console](https://console.synthefy.com/) (one key works for all Nori variants):

   ```bash
   export NORI_API_KEY="<your Nori API key>"
   ```

### Prerequisites

- InfluxDB 3 Core or Enterprise (>= 3.8.2) with the Processing Engine enabled.
- A Nori API key from the [Synthefy console](https://console.synthefy.com/).
- A measurement with a numeric target field, one or more numeric feature columns, and enough labeled
  rows (`>= min_history`) where the target is present.

## Trigger setup

### Scheduled trigger

Every 15 minutes, fill any rows of `sensors` (for `site=A`) that are missing `pressure`, predicting
it from `temp` and `humidity`:

```bash
influxdb3 create trigger \
  --database mydb \
  --path "gh:influxdata/nori_regression/nori_regression.py" \
  --trigger-spec "every:15m" \
  --trigger-arguments measurement=sensors,field=pressure,feature_fields="temp humidity",tags=site:A,model=synthefy/nori \
  nori_sensors_pressure
```

### HTTP trigger

```bash
influxdb3 create trigger \
  --database mydb \
  --path "gh:influxdata/nori_regression/nori_regression.py" \
  --trigger-spec "request:nori_regress" \
  nori_http
```

## Example usage

### On-demand HTTP regression

Call the HTTP endpoint (exposed at `/api/v3/engine/<path>`), passing the Nori key in the header:

```bash
curl -X POST http://localhost:8181/api/v3/engine/nori_regress \
  -H "X-Nori-Api-Key: $NORI_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"measurement":"sensors","field":"pressure","feature_fields":["temp","humidity"],"tags":{"site":"A"}}'
```

**Expected output:**

```json
{"status": "success", "task_id": "...", "result": {"status": "success", "written": 24}}
```

The predicted values are written to the `sensors_regressed` measurement.

### Backfill a specific window

```bash
curl -X POST http://localhost:8181/api/v3/engine/nori_regress \
  -H "X-Nori-Api-Key: $NORI_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"measurement":"sensors","field":"pressure","feature_fields":["temp","humidity"],"tags":{"site":"A"},"start_time":"2026-01-01T00:00:00Z","end_time":"2026-02-01T00:00:00Z"}'
```

### Dry run (preview without writing)

Set `dry_run=true` to log the predictions without writing them back:

```bash
curl -X POST http://localhost:8181/api/v3/engine/nori_regress \
  -H "X-Nori-Api-Key: $NORI_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"measurement":"sensors","field":"pressure","feature_fields":["temp","humidity"],"tags":{"site":"A"},"dry_run":true}'
```

## Output format

Each prediction is written as a point:

- **Measurement:** `output_measurement` (default `<measurement>_regressed`).
- **Tags:** `model` (the slug), `source` (the input measurement), `target` (the predicted field),
  and any single-value filter tags.
- **Field:** `value` (float): the predicted target value.
- **Timestamp:** the predicted row's own timestamp (nanoseconds).

Example line protocol:

```
sensors_regressed,model=synthefy/nori,source=sensors,target=pressure value=1001.2 1704672000000000000
```

## Querying predictions

```sql
SELECT time, value, model, target FROM sensors_regressed ORDER BY time DESC LIMIT 24;
```

## Notes

- **What it predicts:** rows in the window where the target `field` is null but every
  `feature_fields` column is present. Rows where the target is already present become the training
  set (`X_train`/`y_train`). If no rows are missing the target, the run is a no-op (`skipped`). It
  does not overwrite existing target values.
- **One series per run:** predictions are written back at each row's own timestamp, so a run must
  resolve to a single series. If the measurement holds several series (for example one per `site`)
  and your `tags` filter does not isolate one, two rows can share a timestamp and their output
  points would collide, so the plugin **fails with a clear message** rather than silently dropping
  values. Add a `tags` filter, or run one trigger per series.
- **Features only:** Nori sees just the columns you name in `feature_fields`. Row order does not
  matter, and no time-derived features are added.

## Supported models

The `model` argument is the Nori gateway slug your API key is granted:

- `synthefy/nori`: the base Nori in-context tabular regression model (default).
- `synthefy/nori-30m`: a 30M-parameter Nori variant.

A single API key from the [Synthefy console](https://console.synthefy.com/) works for all of these
variants.

## Code overview

### Files

- `nori_regression.py`: the plugin (metadata docstring and implementation).
- `requirements.txt`: Python dependencies.
- `manifest.toml`: packaging metadata.

### Key functions

- `process_scheduled_call(influxdb3_local, call_time, args)`: scheduled entry point.
- `process_request(influxdb3_local, query_parameters, request_headers, request_body, args)`: HTTP entry point.
- `_query(influxdb3_local, cfg, task_id)`: reads the target and feature columns for the window into a DataFrame.
- `_regress(influxdb3_local, cfg, api_key, task_id)`: splits labeled and null-target rows and builds `X_train`/`y_train`/`X_test`.
- `_call_nori(...)`: sends the in-context regression request (`task="regression"`) to the gateway.
- `_write_predictions(...)`: writes the predictions back with `write_sync`.

## Troubleshooting

### Common issues

### Missing API key

The plugin cannot find a Nori gateway key.

**Solution:** set `NORI_API_KEY` on the InfluxDB host, or pass an `X-Nori-Api-Key: <key>` header
when calling the HTTP trigger (see [Authentication](#authentication-for-the-nori-gateway)).

### Model API not found (404)

The gateway rejects the `model` slug for your key.

**Solution:** confirm the `model` slug is spelled correctly (for example `synthefy/nori` or
`synthefy/nori-30m`) and that you passed a valid API key from the
[Synthefy console](https://console.synthefy.com/).

### Not enough labeled rows, or nothing to predict

- **`only N labeled rows (< min_history)`:** fewer than `min_history` rows have the target present.
  Widen `window`, lower `min_history`, or check that `measurement`/`field`/`feature_fields`/`tags`
  select data.
- **`no rows to predict`:** every target value in the window is already present. The plugin only
  fills rows where the target is null; there is nothing to impute.

### Multiple series sharing timestamps

The measurement holds more than one series and your `tags` filter did not isolate one, so
predictions would collide on write.

**Solution:** add a `tags` filter that selects a single series, or run one trigger per series.

### Feature field not found

A `feature_fields` column does not exist in the measurement, or it clashes with the target field or
the reserved names `time`/`y` (not allowed).

**Solution:** fix the column names in `feature_fields`.

### Cold-start latency on the first call

If the model has scaled to zero, the first request after idle can be slow (about 60-70 seconds) or
return a 500 once.

**Solution:** retry; the scheduled trigger recovers on the next tick. Pre-warm with a direct call
before time-sensitive use.

## Limitations

- One series per run; run multiple triggers for multiple series (the plugin fails loud if a run
  resolves to more than one series). Multi-series imputation in a single run is a possible
  enhancement.
- Imputes only rows where the target is null; it does not overwrite existing values.
- Prediction quality depends on how well `feature_fields` explain the target.
- Each call is a billed gateway request; a larger `window` sends more rows and tokens.

## License

Apache 2.0.

## Questions/Comments

Please open an issue or discussion in the
[influxdb3_plugins](https://github.com/influxdata/influxdb3_plugins) repository.
