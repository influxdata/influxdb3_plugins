# Forecasting plugin interface

Forecasting plugins share one interface: the same request and response formats, error codes and stored rows, and a
manifest that describes what each plugin supports. An app that connects several of these plugins can work with them the
same way.

Implementing the interface doesn't make a plugin available in an app. Each app, including InfluxDB 3 Explorer, decides
which plugins it connects.

This document describes version 1 of the interface.

- [Overview](#overview)
- [Capabilities manifest](#capabilities-manifest)
- [On-demand requests](#on-demand-requests)
- [How data is selected](#how-data-is-selected)
- [On-demand response](#on-demand-response)
- [Errors](#errors)
- [Scheduled trigger](#scheduled-trigger)
- [On-write trigger](#on-write-trigger)
- [Stored forecasts](#stored-forecasts)
- [Settings](#settings)

## Overview

A plugin can run in three modes:

| Mode      | Trigger spec                        | When it runs              | Result                                                      |
|-----------|-------------------------------------|---------------------------|-------------------------------------------------------------|
| On demand | `request:<path>`                    | An app sends a request    | A JSON response, and stored rows with `write_results: true` |
| Scheduled | `every:<duration>` or `cron:<spec>` | On the trigger's schedule | Stored rows; errors go to the plugin log                    |
| On write  | `table:<source_measurement>`        | As writes arrive          | Stored rows; errors go to the plugin log                    |

On demand and scheduled, a plugin selects the data in one of two ways:

- **Measurement mode:** the request names a measurement and a field, and can add filters and tags to split by.
- **Query mode:** the request gives an SQL query, and the query's result defines the series.

A request uses exactly one of them.

These plugins implement the interface:

| Plugin                | Modes                | Manifest                                                                               | Documentation                                         |
|-----------------------|----------------------|----------------------------------------------------------------------------------------|-------------------------------------------------------|
| `chronos_forecasting` | On demand, scheduled | [`forecast_interface.json`](../influxdata/chronos_forecasting/forecast_interface.json) | [README](../influxdata/chronos_forecasting/README.md) |
| `prophet_forecasting` | On demand, scheduled | [`forecast_interface.json`](../influxdata/prophet_forecasting/forecast_interface.json) | [README](../influxdata/prophet_forecasting/README.md) |

No plugin implements on-write mode yet.

### Example

A request to a trigger with the spec `request:forecast`:

```bash
curl -X POST "http://localhost:8181/api/v3/engine/forecast" \
  -H "Authorization: Bearer $INFLUXDB3_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "source_measurement": "cpu",
    "field": "usage",
    "tag_filter": {"host": "a"},
    "window": "6h",
    "horizon_steps": 2
  }'
```

The response:

```json
{
  "status": "ok",
  "model": "amazon/chronos-bolt-tiny",
  "levels": [
    0.5,
    0.8
  ],
  "series_total": 1,
  "results": [
    {
      "field": "usage",
      "tags": {
        "host": "a"
      },
      "points_read": 360,
      "step_ms": 60000,
      "forecast": [
        {
          "timestamp": 1700003600000,
          "forecast": 1.30,
          "lower_50": 1.10,
          "upper_50": 1.50,
          "lower_80": 0.90,
          "upper_80": 1.70
        },
        {
          "timestamp": 1700003660000,
          "forecast": 1.32,
          "lower_50": 1.11,
          "upper_50": 1.53,
          "lower_80": 0.88,
          "upper_80": 1.76
        }
      ]
    }
  ]
}
```

## Capabilities manifest

Each plugin ships a manifest, `forecast_interface.json`, in its directory next to `manifest.toml`. If the `exclude` list
in `manifest.toml` would remove the file from the package, add `!forecast_interface.json` to the list. An app that
connects the plugin can read the manifest from the plugin's package.

The manifest is descriptive. It tells an app what to offer: which controls to show, which models to list, and which
bounds to suggest. The plugin doesn't read the manifest and validates every request on its own. It refuses a key it
doesn't support with `bad_request`, and accepts any model it can run, listed or not.

| Field                       | Required | Type          | Description                                                                                                      | Example                     |
|-----------------------------|----------|---------------|------------------------------------------------------------------------------------------------------------------|-----------------------------|
| `forecast_contract_version` | Yes      | integer       | Version of this interface the plugin implements. The current version is `1`.                                     | `1`                         |
| `name`                      | No       | string        | Plugin name to show. Without it, the app names the plugin itself.                                                | `"Chronos"`                 |
| `description`               | No       | string        | Plain text, one or two sentences: what the plugin is, where it runs, and what its first run does.                | See the example             |
| `modes`                     | Yes      | array(string) | Modes the plugin offers. A non-empty subset of `on_demand`, `scheduled`, `on_write`.                             | `["on_demand","scheduled"]` |
| `accepts_query`             | Yes      | boolean       | Whether the plugin accepts [query mode](#query-mode).                                                            | `true`                      |
| `filter`                    | No       | array(string) | Filter keys the plugin accepts in measurement mode. A subset of `tag_filter`, `sql_filter`.                      | `["tag_filter"]`            |
| `group_by`                  | Yes      | boolean       | Whether the plugin accepts `group_by` in measurement mode.                                                       | `true`                      |
| `history`                   | Yes      | array(string) | Ways to bound history. A subset of `window`, `point_limit`, non-empty if `modes` has `on_demand` or `scheduled`. | `["window","point_limit"]`  |
| `sampling`                  | Yes      | boolean       | Whether the plugin accepts `sampling_interval`.                                                                  | `true`                      |
| `horizon`                   | Yes      | array(string) | Ways to set the horizon. A non-empty subset of `steps` (`horizon_steps`), `duration` (`horizon_duration`).       | `["steps","duration"]`      |
| `backtest`                  | Yes      | boolean       | Whether an on-demand request accepts `backtest_points`.                                                          | `true`                      |
| `data_leaves_server`        | Yes      | boolean       | Whether a forecast sends data to an external service, so the app can warn first.                                 | `false`                     |
| `credentials`               | No       | array(object) | Secrets the plugin needs. Absent or `[]` means none. See [Credentials](#credentials).                            | See below                   |
| `models`                    | Yes      | array(object) | Models to offer, each with its own limits. Non-empty. The plugin may accept others.                              | See below                   |
| `advanced_params`           | No       | array(object) | Plugin-specific parameters, for an "advanced settings" section.                                                  | See below                   |

An object in `credentials`, one for each secret. It has `header`, `env_var`, or both:

| Field     | Required | Type   | Description                                                                                             | Example               |
|-----------|----------|--------|---------------------------------------------------------------------------------------------------------|-----------------------|
| `label`   | Yes      | string | Name of the secret, shown when the app asks for it.                                                     | `"Service API key"`   |
| `header`  | No       | string | Request header the app sends the secret in.                                                             | `"X-Service-Api-Key"` |
| `env_var` | No       | string | Environment variable of the InfluxDB server that holds the secret. The app can name it, but not set it. | `"SERVICE_API_KEY"`   |

An object in `models`:

| Field                | Required | Type            | Description                                                                               | Example                      |
|----------------------|----------|-----------------|-------------------------------------------------------------------------------------------|------------------------------|
| `id`                 | Yes      | string          | Model identifier: the value of the request key `model`.                                   | `"amazon/chronos-bolt-tiny"` |
| `label`              | No       | string          | Model name to show. Without it, the app shows `id`.                                       | `"Chronos-Bolt (tiny)"`      |
| `description`        | No       | string          | Plain text, one line: what the model suits, such as speed, accuracy, or download size.    | `"Fastest; 35 MB download"`  |
| `default`            | Yes      | boolean         | The model to preselect. Exactly one model sets `true`.                                    | `true`                       |
| `levels`             | Yes      | array(number)   | Interval levels the model can produce, each between 0 and 1. `[]` means no intervals.     | `[0.5, 0.8]`                 |
| `min_points`         | Yes      | integer         | Fewest history points the model needs, at least 1. Fewer gives `too_few_points`.          | `10`                         |
| `max_history_points` | Yes      | integer or null | Most history points the model reads; longer history is cut. `null` means unbounded.       | `2048`                       |
| `max_horizon_steps`  | Yes      | integer or null | Native horizon; past it, the model extrapolates at lower quality. `null` means unbounded. | `64`                         |

An object in `advanced_params`:

| Field     | Required | Type   | Description                                                                                   | Example                         |
|-----------|----------|--------|-----------------------------------------------------------------------------------------------|---------------------------------|
| `name`    | Yes      | string | Request key, sent at the top level of the body. Must not be a [reserved key](#reserved-keys). | `"covariate_fields"`            |
| `type`    | Yes      | string | Value type, so the app renders the right input: `string`, `number`, `bool`, `list`, `object`. | `"list"`                        |
| `default` | No       | any    | Default value to show.                                                                        | `[]`                            |
| `values`  | No       | array  | Allowed values, so the app renders a dropdown instead of free input.                          | `["additive","multiplicative"]` |

Example, shortened to one model:

```json
{
  "forecast_contract_version": 1,
  "name": "Chronos",
  "description": "Amazon's pretrained Chronos models, run inside InfluxDB: no training, and data stays on the server. The first forecast downloads the model.",
  "modes": [
    "on_demand",
    "scheduled"
  ],
  "accepts_query": true,
  "filter": [
    "tag_filter",
    "sql_filter"
  ],
  "group_by": true,
  "history": [
    "window",
    "point_limit"
  ],
  "sampling": true,
  "horizon": [
    "steps",
    "duration"
  ],
  "backtest": true,
  "data_leaves_server": false,
  "credentials": [],
  "models": [
    {
      "id": "amazon/chronos-bolt-tiny",
      "label": "Chronos-Bolt (tiny)",
      "description": "Fastest; 35 MB download",
      "default": true,
      "levels": [
        0.5,
        0.8
      ],
      "min_points": 10,
      "max_history_points": 2048,
      "max_horizon_steps": 64
    }
  ],
  "advanced_params": [
    {
      "name": "covariate_fields",
      "type": "list",
      "default": []
    }
  ]
}
```

## On-demand requests

An app sends a request as a JSON body in a `POST` to `/api/v3/engine/<path>`. Keys can also come from
[other sources](#sources-and-precedence). By default, a request only returns the forecast and writes nothing.

The plugin's [manifest](#capabilities-manifest) says which optional keys it offers. A key the plugin doesn't support
gives `bad_request`.

### Measurement mode

| Key                  | Required       | Type              | Description                                                                                                                                                                                         | Example                        |
|----------------------|----------------|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------|
| `source_measurement` | Yes            | string            | Measurement to read.                                                                                                                                                                                | `"cpu"`                        |
| `field`              | Yes            | string            | Numeric field to forecast.                                                                                                                                                                          | `"usage"`                      |
| `source_database`    | No             | string            | Database to read from. Default: the trigger's database. An unknown database gives `bad_request`.                                                                                                    | `"demo"`                       |
| `tag_filter`         | No             | object            | Keeps rows by tag equality: `{tag: value}`, or `{tag: [values]}` for several values. Applies together with `sql_filter`.                                                                            | `{"region": "eu"}`             |
| `sql_filter`         | No             | string            | SQL condition on tags and fields, without the `WHERE` keyword, inserted into the query as is. Applies together with `tag_filter`.                                                                   | `"usage > 0"`                  |
| `group_by`           | No             | array(string)     | Tags to split by; each distinct combination of their values is one series. Default: one series of all matching rows.                                                                                | `["host"]`                     |
| `series_limit`       | No             | integer           | At least 1 and at most the plugin's series cap. Forecasts only the first N forecasts, and skips the rest. See [Series order and limit](#series-order-and-limit).                                    | `5`                            |
| `window`             | No             | string (duration) | History as a time window ending now. See [History](#history).                                                                                                                                       | `"6h"`                         |
| `point_limit`        | No             | integer           | History as the most recent points of each series, at least the model's `min_points`. See [History](#history).                                                                                       | `512`                          |
| `sampling_interval`  | No             | string (duration) | Resamples history to a grid of this step before the model: the mean of each bucket, and an empty bucket stays a gap. The response's `step_ms` equals it.                                            | `"30s"`                        |
| `horizon_steps`      | One of the two | integer           | Horizon as a number of future points, at least 1. See [Horizon](#horizon).                                                                                                                          | `64`                           |
| `horizon_duration`   | One of the two | string (duration) | Horizon as a length of time. See [Horizon](#horizon).                                                                                                                                               | `"1h"`                         |
| `levels`             | No             | array(number)     | Interval levels, each between 0 and 1. Level `L` is the interval between the quantiles `(1 - L) / 2` and `(1 + L) / 2`. Default: the plugin's. A level the model can't produce gives `null` bounds. | `[0.5, 0.8]`                   |
| `model`              | No             | string            | Model to run: any model the plugin can run. Default: the plugin's.                                                                                                                                  | `"amazon/chronos-bolt-tiny"`   |
| `write_results`      | No             | boolean           | Also writes the forecast. Default: `false`. See [Write the forecast](#write-the-forecast).                                                                                                          | `true`                         |
| `target_measurement` | No             | string            | Measurement to write to. Default: `_forecasts.<source_measurement>`.                                                                                                                                | `"_forecasts.cpu"`             |
| `target_database`    | No             | string            | Database to write to. Default: the trigger's database.                                                                                                                                              | `"demo"`                       |
| `forecast_id`        | No             | string            | Identifies this forecast's stored rows, written as the tag `forecast_id`. See [Stored forecasts](#stored-forecasts).                                                                                | `"ops-1h"`                     |
| `backtest_points`    | No             | integer           | At least 1. Checks the forecast on the latest N points of history. See [Backtest](#backtest).                                                                                                       | `12`                           |
| *advanced keys*      | No             | per manifest      | Plugin-specific parameters, at the top level of the body, declared in the manifest `advanced_params`.                                                                                               | `"covariate_fields": ["temp"]` |

Example:

```json
{
  "source_measurement": "cpu",
  "field": "usage",
  "source_database": "demo",
  "tag_filter": {
    "region": "eu"
  },
  "sql_filter": "usage > 0",
  "group_by": [
    "host"
  ],
  "window": "6h",
  "sampling_interval": "30s",
  "horizon_steps": 64,
  "levels": [
    0.5,
    0.8
  ],
  "model": "amazon/chronos-bolt-tiny",
  "covariate_fields": [
    "temp"
  ]
}
```

### Query mode

For plugins whose manifest `accepts_query` is `true`. The query defines the series, so `tag_filter`, `sql_filter` and
`group_by` give `bad_request`.

| Key                  | Required                  | Type              | Description                                                                                                                                                                                         | Example                          |
|----------------------|---------------------------|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------|
| `query`              | Yes                       | string            | SQL query the plugin runs. Each series of its result is forecast. See [Column roles](#column-roles).                                                                                                | See the example                  |
| `source_database`    | No                        | string            | Database to run the query against. Default: the trigger's database. An unknown database gives `bad_request`.                                                                                        | `"demo"`                         |
| `series_limit`       | No                        | integer           | At least 1 and at most the plugin's series cap. Forecasts only the first N forecasts, and skips the rest. See [Series order and limit](#series-order-and-limit).                                    | `5`                              |
| `window`             | No                        | string (duration) | Keeps the result's rows inside this time window. Usually omitted, as the query defines its history.                                                                                                 | `"6h"`                           |
| `point_limit`        | No                        | integer           | How many of the most recent points of each series reach the model.                                                                                                                                  | `512`                            |
| `sampling_interval`  | No                        | string (duration) | Resamples the result to a grid of this step before the model. Usually omitted, as the query defines the aggregation.                                                                                | `"30s"`                          |
| `horizon_steps`      | One of the two            | integer           | Horizon as a number of future points, at least 1. See [Horizon](#horizon).                                                                                                                          | `64`                             |
| `horizon_duration`   | One of the two            | string (duration) | Horizon as a length of time. See [Horizon](#horizon).                                                                                                                                               | `"1h"`                           |
| `levels`             | No                        | array(number)     | Interval levels, each between 0 and 1. Level `L` is the interval between the quantiles `(1 - L) / 2` and `(1 + L) / 2`. Default: the plugin's. A level the model can't produce gives `null` bounds. | `[0.5, 0.8]`                     |
| `model`              | No                        | string            | Model to run: any model the plugin can run. Default: the plugin's.                                                                                                                                  | `"amazon/chronos-bolt-tiny"`     |
| `write_results`      | No                        | boolean           | Also writes the forecast. Default: `false`. See [Write the forecast](#write-the-forecast).                                                                                                          | `true`                           |
| `target_measurement` | With several measurements | string            | Measurement to write to. Default: `_forecasts.<measurement>` if the query reads one measurement. Required if it reads several.                                                                      | `"_forecasts.cpu"`               |
| `target_database`    | No                        | string            | Database to write to. Default: the trigger's database.                                                                                                                                              | `"demo"`                         |
| `forecast_id`        | No                        | string            | Identifies this forecast's stored rows, written as the tag `forecast_id`. See [Stored forecasts](#stored-forecasts).                                                                                | `"ops-1h"`                       |
| `backtest_points`    | No                        | integer           | At least 1. Checks the forecast on the latest N points of history. See [Backtest](#backtest).                                                                                                       | `12`                             |
| *advanced keys*      | No                        | per manifest      | Plugin-specific parameters that apply to a query, at the top level of the body, declared in the manifest `advanced_params`.                                                                         | `"seasonality_mode": "additive"` |

Example:

```json
{
  "query": "SELECT date_bin(INTERVAL '5 minutes', time) AS time, host, avg(usage) AS usage FROM cpu WHERE time > now() - INTERVAL '6 hours' GROUP BY 1, host ORDER BY time",
  "source_database": "demo",
  "horizon_duration": "1h",
  "levels": [
    0.5,
    0.8
  ]
}
```

The plugin reads the whole result before `point_limit` cuts it and sets no default window, so the query should bound
its own time range.

The Processing Engine passes only `BIGINT`, `BIGINT UNSIGNED`, `DOUBLE`, `BOOLEAN`, text and timestamp columns to a
plugin. A query that returns another type (`INT`, `REAL`, `DECIMAL`, arrays, structs) gives `internal`, so cast such
columns in the query.

#### Column roles

The plugin assigns a role to each column of the result, by name for the time and by type for the rest:

| Column              | Role                                                                                                 |
|---------------------|------------------------------------------------------------------------------------------------------|
| The column `time`   | The timestamp. Alias your timestamp `AS time`. Without this column, the request gives `bad_request`. |
| Non-numeric columns | Series keys. Each distinct combination of their values is one series.                                |
| Numeric columns     | Values. Each one is forecast on its own for each series; extra numeric columns aren't covariates.    |

A query needs at least one numeric column, and a query that doesn't plan gives `bad_request`.

A timestamp column other than `time` reaches the plugin as an integer, so the plugin forecasts it as a number. Leave
such columns out of the query.

The plugin finds the measurements the query reads from the query's plan (`EXPLAIN`), so aliases, CTEs and subqueries
resolve:

- One measurement is the `source_measurement` tag of stored rows and the base of the default `target_measurement`.
- With several measurements (joins, unions, `IN (SELECT ...)`), the tag is omitted, and writing requires
  `target_measurement`.

### Write the forecast

With `write_results: true`, the plugin also writes the [stored rows](#stored-forecasts) and still returns the response.
If the write fails, the whole request fails with `internal`.

### Backtest

With `backtest_points: N`, the plugin also checks how the forecast would have done on the latest history, in the same
call. For each series, it holds out the last `N` points of history, after its own resampling, forecasts them from the
points before them, and compares the forecast with the measured points. The result goes to the series' `backtest`
object in the [response](#on-demand-response). The forecast itself doesn't change.

- The check runs like the forecast: the same model, levels and history length, only ending `N` points earlier.
- It uses one window, the last `N` points, so the result is noisy when `N` is small. Show `backtest.points` with it. `N`
  equal to the horizon measures accuracy over the same distance as the forecast.
- Only measured points count. An empty bucket in the window is skipped.
- With fewer than the model's `min_points` points before the cut, or no measured point among the last `N`, the series
  gets `backtest: null` and its forecast as usual.
- A scheduled trigger doesn't accept the key, and stored rows don't hold the check.
- It costs about one more forecast per series. A plugin that trains a model per series trains it again.

## How data is selected

### History

A request selects its history with `window`, `point_limit`, or both.

| Keys set      | History the plugin reads                                              |
|---------------|-----------------------------------------------------------------------|
| `window`      | The points inside the window                                          |
| `point_limit` | The most recent N points of each series                               |
| Both          | The most recent N points of each series inside the window             |
| Neither       | The plugin's default history, or `bad_request` if the plugin has none |

With `sampling_interval`, `point_limit` counts buckets instead of points.

In measurement mode, history ends now, so the plugin doesn't read a row with a later timestamp until that time comes.
With `sampling_interval`, history ends at the last full bucket, so a bucket still filling doesn't reach the model.

In query mode, the query defines its own history and keeps later rows. Only `sampling_interval` ends the result at the
last full bucket.

### Source database

`source_database` names the database the plugin reads, in both modes. The default is the trigger's database. An unknown
database gives `bad_request`.

The plugin reads with the engine's rights, not the caller's. Only an admin token can call a request trigger, so the
caller can read every database anyway.

### Filters

These keys apply in measurement mode. With both set, a row must match both.

- `tag_filter` matches tags by equality.
- `sql_filter` is an SQL condition on tags and fields, inserted into the query as is. It can also reference other tables
  of the database, so a plugin's documentation states this risk.

### Series

In measurement mode, without `group_by`, all rows that match the filters form one series. With `group_by`, each distinct
combination of the listed tags is its own series.

In query mode, the query defines the series. In on-write mode, each tag set is its own series.

Rows of a series that share a timestamp are averaged, as the rows of a `sampling_interval` bucket are, so a series has
one value per timestamp.

### Mixed tag sets

In measurement mode without `sampling_interval`, a series can't mix rows from several tag sets that write at the same
time within the history the model reads, as interleaved rows form a saw-tooth. Such a request gives `bad_request`. To
forecast the series, add the tags to `group_by`, pin them with `tag_filter`, or set `sampling_interval` to forecast
their mean.

A tag set that follows another in time, such as a tag that changed its value, is fine.

### Series tags

Series tags appear in the response's `tags` and in stored rows.

| Mode        | Series tags                                                                             |
|-------------|-----------------------------------------------------------------------------------------|
| Measurement | The measurement's tags that every row of the series carries with one and the same value |
| Query       | The series key columns                                                                  |
| On write    | The tags of the series' tag set                                                         |

So in measurement mode, the `group_by` tags and any tag a filter pins to one value are series tags, whichever filter
does it. A tag with several values, or one missing from some rows, isn't.

### Series order and limit

A forecast is one series and one field. Forecasts are ordered by the series' key values as text, with a missing value
last, then by field name. The key is the `group_by` tags in measurement mode and the key columns in query mode, in their
order. The response's `results` follow this order.

`series_limit: N` keeps the first `N` forecasts in this order and skips the rest without an error. The response's
`series_total` counts them all. `N` above the plugin's series cap gives `bad_request`.

### Series cap

A plugin can cap the forecasts one request or run produces, in measurement and query modes. The cap counts forecasts, so
in query mode each numeric column counts. More forecasts than the cap give `bad_request`, with the cap in the message,
unless `series_limit` is set.

An on-write trigger has nothing to refuse, so its cap evicts the least recently updated series.

The cap is a plugin setting, not part of the manifest.

### Horizon

A request sets exactly one of `horizon_steps` and `horizon_duration`. Neither or both give `bad_request`.

`horizon_steps` counts future points at the series' spacing. `horizon_duration` is rounded up to whole steps of that
spacing, at least one.

The forecast starts one step after the last point of history, or the last cell for a plugin that lays history on a grid.
The horizon counts from there, so the forecast continues the chart's line.

## On-demand response

A request can resolve to many forecasts, so the response is a list of results. With no matching history, a request in
measurement mode without `group_by` answers its one series with `too_few_points`. With `group_by` or in query mode there
are no series, so `results` is empty.

Top level:

| Field          | Required  | Type          | Description                                                                                     | Example                      |
|----------------|-----------|---------------|-------------------------------------------------------------------------------------------------|------------------------------|
| `status`       | Yes       | string        | `"ok"`, or `"error"` for a [request-level error](#errors).                                      | `"ok"`                       |
| `model`        | When `ok` | string        | ID of the model that ran, as in the manifest. The default model's ID if the request names none. | `"amazon/chronos-bolt-tiny"` |
| `levels`       | When `ok` | array(number) | The returned levels, which are the `lower_*` and `upper_*` keys of each point.                  | `[0.5, 0.8]`                 |
| `series_total` | When `ok` | integer       | Forecasts the request selects before `series_limit`. Without it, the number of `results`.       | `12`                         |
| `results`      | When `ok` | array(object) | One element per forecast, in the [order above](#series-order-and-limit).                        | See below                    |

An object in `results`:

| Field         | Required              | Type           | Description                                                                                                          | Example                      |
|---------------|-----------------------|----------------|----------------------------------------------------------------------------------------------------------------------|------------------------------|
| `field`       | Yes                   | string         | Forecast field, or the numeric column in query mode.                                                                 | `"usage"`                    |
| `tags`        | Yes                   | object         | [Series tags](#series-tags). `{}` if none.                                                                           | `{"host": "a"}`              |
| `points_read` | Unless a series error | integer        | Observed history points the model used.                                                                              | `512`                        |
| `step_ms`     | Unless a series error | integer        | Spacing between points, in milliseconds.                                                                             | `30000`                      |
| `forecast`    | Unless a series error | array(object)  | The forecast points.                                                                                                 | See below                    |
| `backtest`    | No                    | object or null | The [check](#backtest) on the latest history, only with `backtest_points`. `null` if the series is too short for it. | See below                    |
| `status`      | On a series error     | string         | `"error"`.                                                                                                           | `"error"`                    |
| `code`        | On a series error     | string         | [Error code](#errors).                                                                                               | `"too_few_points"`           |
| `message`     | On a series error     | string         | What went wrong.                                                                                                     | `"need >= 10 points, got 4"` |

An object in `forecast`:

| Field                             | Required | Type           | Description                                                                   | Example         |
|-----------------------------------|----------|----------------|-------------------------------------------------------------------------------|-----------------|
| `timestamp`                       | Yes      | integer        | The point's time, in epoch milliseconds.                                      | `1700003600000` |
| `forecast`                        | Yes      | number or null | Central forecast.                                                             | `1.30`          |
| `lower_<level>` / `upper_<level>` | Yes      | number or null | Bounds of each requested level. `null` if the model can't produce that level. | `1.10` / `1.50` |

`<level>` is the level as a percent in its shortest form: `0.8` gives `lower_80`, and `0.975` gives `lower_97.5`.

A lower bound never exceeds the forecast, an upper bound is never below it, and a wider level is never narrower.

The `backtest` object:

| Field      | Required | Type    | Description                                                                                                                         | Example                      |
|------------|----------|---------|-------------------------------------------------------------------------------------------------------------------------------------|------------------------------|
| `points`   | Yes      | integer | Measured points the check compared, from 1 to `backtest_points`.                                                                    | `11`                         |
| `coverage` | Yes      | object  | For each requested level, the share of those points inside its interval, from 0 to 1. `null` if the model can't produce that level. | `{"50": 0.545, "80": 0.818}` |
| `mae`      | Yes      | number  | Mean absolute difference between the central forecast and the measured points, in the units of the field.                           | `2.21`                       |

Coverage close to the level means the intervals are calibrated. Coverage far below it means the forecast is
overconfident. Coverage alone rewards wide intervals, so `mae` comes with it.

Example:

```json
{
  "status": "ok",
  "model": "amazon/chronos-bolt-tiny",
  "levels": [
    0.5,
    0.8
  ],
  "series_total": 2,
  "results": [
    {
      "field": "usage",
      "tags": {
        "host": "a"
      },
      "points_read": 512,
      "step_ms": 30000,
      "forecast": [
        {
          "timestamp": 1700003600000,
          "forecast": 1.30,
          "lower_50": 1.10,
          "upper_50": 1.50,
          "lower_80": 0.90,
          "upper_80": 1.70
        }
      ],
      "backtest": {
        "points": 11,
        "coverage": {
          "50": 0.545,
          "80": 0.818
        },
        "mae": 0.21
      }
    },
    {
      "field": "usage",
      "tags": {
        "host": "c"
      },
      "status": "error",
      "code": "too_few_points",
      "message": "need >= 10 points, got 4"
    }
  ]
}
```

## Errors

Errors have two levels, so one bad series doesn't blank a whole chart:

- **Request level:** the top-level `status` is `"error"`, with `code` and `message` and no `results`. The whole request
  failed.
- **Series level:** the top-level `status` is `"ok"`, and an element of `results` has `status: "error"`, `code` and
  `message`, and no `forecast`.

A response with the top-level `status` of `"ok"` has the HTTP status `200`, series errors included. A request-level
error
has the HTTP status of its code, with the same JSON body.

| Code                | Level   | HTTP status | When                                                                                                            |
|---------------------|---------|-------------|-----------------------------------------------------------------------------------------------------------------|
| `bad_request`       | Request | `400`       | A malformed body, a missing or unsupported key, no `time` column in a query result, more forecasts than the cap |
| `unauthorized`      | Request | `401`       | A secret from the manifest `credentials` is missing or rejected                                                 |
| `too_few_points`    | Series  | None        | Fewer history points than the model's `min_points`                                                              |
| `model_unavailable` | Either  | `503`       | The model failed to load or download, or the external service that runs it is unreachable                       |
| `timeout`           | Request | `504`       | The run, or a call to an external service, exceeded its time budget                                             |
| `internal`          | Either  | `500`       | A failure no other code covers: a query, the inference, or the write failed                                     |
| `other`             | Either  | `400`       | A plugin-specific condition, such as a failed validation or a service's rate limit. `message` explains it.      |

A request-level error:

```json
{
  "status": "error",
  "code": "bad_request",
  "message": "no time column in query result"
}
```

## Scheduled trigger

A scheduled trigger runs on its spec and writes the forecast to `target_measurement`. Its spec is `every:<duration>`, or
`cron:<spec>` with six fields, seconds first.

Arguments are strings. A list is space-separated, and `tag_filter` is space-separated `key=value` pairs, with one value
for each tag. The TOML config takes native lists and tables. See [Trigger arguments](#trigger-arguments).

The arguments select the data in measurement mode or in query mode, as a request does. `write_results` and
`backtest_points` give `bad_request`.

| Argument             | Required                  | Type              | Description                                                                                                                                                       | Example                    |
|----------------------|---------------------------|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------|
| `source_measurement` | In measurement mode       | string            | Measurement to read.                                                                                                                                              | `cpu`                      |
| `field`              | In measurement mode       | string            | Numeric field to forecast.                                                                                                                                        | `usage`                    |
| `query`              | In query mode             | string            | SQL query the plugin runs. Each series of its result is forecast. See [Column roles](#column-roles). A query with a comma goes in the TOML config.                | See the example            |
| `source_database`    | No                        | string            | Database to read from. Default: the trigger's database. An unknown database gives `bad_request`.                                                                  | `demo`                     |
| `tag_filter`         | No                        | string or table   | Measurement mode. Keeps rows by tag equality. Applies together with `sql_filter`.                                                                                 | `host=a region=eu`         |
| `sql_filter`         | No                        | string            | Measurement mode. SQL condition on tags and fields, without the `WHERE` keyword, inserted into the query as is. A filter with a comma goes in the TOML config.    | `usage > 0`                |
| `group_by`           | No                        | string or list    | Measurement mode. Tags to split by; each distinct combination of their values is one series. Default: one series of all matching rows.                            | `host region`              |
| `series_limit`       | No                        | integer           | At least 1 and at most the plugin's series cap. Forecasts only the first N forecasts. See [Series order and limit](#series-order-and-limit).                      | `5`                        |
| `window`             | No                        | string (duration) | History as a time window, rolling with each run. See [History](#history).                                                                                         | `6h`                       |
| `point_limit`        | No                        | integer           | History as the most recent points of each series, at least the model's `min_points`. See [History](#history).                                                     | `512`                      |
| `sampling_interval`  | No                        | string (duration) | Resamples history to a grid of this step before the model: the mean of each bucket, and an empty bucket stays a gap.                                              | `30s`                      |
| `horizon_steps`      | One of the two            | integer           | Horizon as a number of future points, at least 1. See [Horizon](#horizon).                                                                                        | `64`                       |
| `horizon_duration`   | One of the two            | string (duration) | Horizon as a length of time. See [Horizon](#horizon).                                                                                                             | `1h`                       |
| `levels`             | No                        | string or list    | Interval levels, each between 0 and 1. Default: the plugin's. A level the model can't produce isn't written.                                                      | `0.5 0.8`                  |
| `model`              | No                        | string            | Model to run: any model the plugin can run. Default: the plugin's.                                                                                                | `amazon/chronos-bolt-tiny` |
| `target_measurement` | With several measurements | string            | Measurement to write to. Default: `_forecasts.<source_measurement>`. Required if a query reads several measurements.                                              | `_forecasts.cpu`           |
| `target_database`    | No                        | string            | Database to write to. Default: the trigger's database.                                                                                                            | `demo`                     |
| `forecast_id`        | No                        | string            | Identifies this forecast's stored rows, so forecasts of one series with different settings don't overwrite each other. See [Stored forecasts](#stored-forecasts). | `ops-1h`                   |
| *advanced keys*      | No                        | per manifest      | Plugin-specific parameters, declared in the manifest `advanced_params`.                                                                                           | `covariate_fields=temp`    |

A trigger in measurement mode:

```bash
influxdb3 create trigger \
  --database mydb \
  --path chronos_forecasting.py \
  --trigger-spec "every:5m" \
  --trigger-arguments "source_measurement=cpu,field=usage,group_by=host,window=6h,sampling_interval=1min,horizon_steps=60" \
  cpu_forecast
```

A trigger in query mode, with the query in a TOML file because it has commas:

```toml
query = "SELECT date_bin(INTERVAL '5 minutes', time) AS time, host, avg(usage) AS usage FROM cpu WHERE time > now() - INTERVAL '6 hours' GROUP BY 1, host"
horizon_steps = 12
```

```bash
influxdb3 create trigger \
  --database mydb \
  --path chronos_forecasting.py \
  --trigger-spec "every:5m" \
  --trigger-arguments config_file_path=cpu_forecast.toml \
  cpu_forecast_query
```

A run returns nothing. It writes the [stored rows](#stored-forecasts) and logs its errors with the
[error codes](#errors):

- A failed run logs an `ERROR` line with the code. To detect one, look for `ERROR` in the trigger's rows in
  `system.processing_engine_logs`.
- A series with `too_few_points` logs a `WARN` line, and the other series of the run are still written.

## On-write trigger

No plugin implements this mode yet.

An on-write trigger learns from the writes to one measurement as they arrive and writes forecasts on its own. There is
no request and no response. An app creates a data-write trigger with the spec `table:<source_measurement>` and the
arguments below, then reads the forecasts from `target_measurement` by the latest `run_time`.

Errors go to the plugin log with the [error codes](#errors).

| Argument             | Required       | Type              | Description                                                                                                  | Example          |
|----------------------|----------------|-------------------|--------------------------------------------------------------------------------------------------------------|------------------|
| `source_measurement` | Yes            | string            | Measurement whose writes the model learns from: the measurement of the trigger spec.                         | `cpu`            |
| `field`              | No             | string            | Numeric field to forecast. Default: every numeric field.                                                     | `usage`          |
| `tag_filter`         | No             | string or table   | Learns only from rows whose tags match.                                                                      | `region=eu`      |
| `horizon_steps`      | One of the two | integer           | Horizon as a number of future points, at least 1. See [Horizon](#horizon).                                   | `64`             |
| `horizon_duration`   | One of the two | string (duration) | Horizon as a length of time. See [Horizon](#horizon).                                                        | `1h`             |
| `levels`             | No             | string or list    | Interval levels, each between 0 and 1. Default: the plugin's. A level the model can't produce isn't written. | `0.5 0.8`        |
| `model`              | No             | string            | Model to run: any model the plugin can run. Default: the plugin's.                                           | `my-model`       |
| `target_measurement` | No             | string            | Measurement to write to. Default: `_forecasts.<source_measurement>`.                                         | `_forecasts.cpu` |
| `target_database`    | No             | string            | Database to write to. Default: the trigger's database.                                                       | `demo`           |
| `forecast_id`        | No             | string            | Identifies this forecast's stored rows. See [Stored forecasts](#stored-forecasts).                           | `ops-1h`         |
| *advanced keys*      | No             | per manifest      | Plugin-specific parameters, declared in the manifest `advanced_params`.                                      | `my_parameter=1` |

`query`, `source_database`, `sql_filter`, `group_by`, `series_limit`, `window`, `point_limit`, `sampling_interval` and
`write_results` give `bad_request`: the trigger sees only new writes to its own database, row by row.

- History is what the trigger has seen since it started, plus any state the plugin restores. It doesn't read older data.
- The plugin decides when to forecast. Each forecast is a run with its own `run_time`.
- A series with fewer than `min_points` points is warming up, not failing: it gets no forecast and no `too_few_points`.
  Warn the user that the first forecast comes only after the warm-up.

## Stored forecasts

Three things write forecasts: a scheduled trigger on every run, an on-demand request with `write_results: true`, and an
on-write trigger whenever it forecasts. All of them write one row per forecast point to `target_measurement`, by default
`_forecasts.<source_measurement>`:

| Element | Name                              | Type    | Description                                                                              |
|---------|-----------------------------------|---------|------------------------------------------------------------------------------------------|
| Tag     | `field`                           | string  | Forecast field name.                                                                     |
| Tag     | `model`                           | string  | Model ID, as in the response's `model`.                                                  |
| Tag     | `source_measurement`              | string  | Source measurement. In query mode, only if the query reads one measurement.              |
| Tag     | `forecast_id`                     | string  | The writer's `forecast_id`. Absent if not set.                                           |
| Tag     | *series tags*                     | string  | See [Series tags](#series-tags).                                                         |
| Field   | `forecast`                        | float64 | Central forecast.                                                                        |
| Field   | `lower_<level>` / `upper_<level>` | float64 | Bounds of each level, such as `lower_80`. A level the model can't produce isn't written. |
| Field   | `step`                            | int64   | Number of the forecast step. `1` is the first point after the data.                      |
| Field   | `points_read`                     | int64   | Observed history points, as in the response's `points_read`.                             |
| Field   | `run_time`                        | string  | Time of the run, in ISO 8601 UTC, such as `2026-10-26T12:00:00Z`.                        |
| Time    | The point's time                  | ns      | The forecast point's time, in nanoseconds.                                               |

- **Column types are fixed.** InfluxDB pins a column's type on the first write, so a writer with another type fails.
  `run_time` is always a string in one format, UTC to the second with `Z`, so `max(run_time)` sorts correctly.
- **A series tag never takes a column's name.** A series tag named like a column above is left out of the row. A
  `group_by` tag or a query key column with such a name gives `bad_request` when the forecast is written, as it would
  break the row or merge series.
- **A run overwrites the same rows.** Rows with the same tags and time are overwritten. A longer tail from an earlier
  run
  stays, so read by the latest `run_time`.
- **`forecast_id` keeps forecasts apart.** Two schedules of one series with different settings overwrite each other
  unless each sets its own `forecast_id`. The `model` tag tells plugins apart, and `forecast_id` tells configurations
  apart. It's a tag because a row is identified by its tags and time, so a field wouldn't keep the rows apart.

To read the latest forecast of each series, partition by `field` and the series tags:

```sql
SELECT time, field, host, forecast, lower_80, upper_80, run_time
FROM (
    SELECT *, max (run_time) OVER (PARTITION BY field, host) AS latest
    FROM "_forecasts.cpu"
    WHERE forecast_id = 'ops-1h'
    ) AS f
WHERE run_time = latest
ORDER BY host, time
```

## Settings

### Sources and precedence

Any key, shared or plugin-specific, can come from any of these sources. Each one overrides the ones above it.

| Source               | Form                                                                                       | Modes     |
|----------------------|--------------------------------------------------------------------------------------------|-----------|
| Environment variable | `INFLUXDB3_<PLUGIN>_<KEY>`: `INFLUXDB3_CHRONOS_FORECASTING_HORIZON_STEPS`                  | All       |
| Trigger argument     | `key=value`, as in [Trigger arguments](#trigger-arguments)                                 | All       |
| TOML config          | The file named by `config_file_path`                                                       | All       |
| Request body         | A JSON object                                                                              | On demand |
| Request header       | `X-Influxdb3-<Plugin>-<key>`, `-` for `_`: `X-Influxdb3-Chronos-Forecasting-horizon-steps` | On demand |
| Query string         | `?horizon_steps=12`                                                                        | On demand |

An app sends settings only in the JSON body.

There are two exceptions: [credentials](#credentials), and `config_file_path`, which comes only from a trigger argument
or an environment variable.

A plugin's limits, such as `max_series` and `max_forecast_points`, are settings like any other, not part of the
manifest.

### Choices between keys

Two choices are made as a whole, by the highest source that sets either side. The other side from lower sources is
dropped:

- The series mode: `query` against the measurement-mode keys.
- The horizon: `horizon_steps` against `horizon_duration`.

So a request body with `query` overrides `source_measurement` and `field` from trigger arguments. Both sides in one
source give `bad_request`. In this choice, empty text, lists and objects don't count as set.

A plugin can add its own choices between keys and states them in its documentation.

### Empty values

Outside these choices, empty text and `null` keep the value from a lower source, and an empty list or object replaces
it.
For example, `group_by: []` means no split for this request.

### Trigger arguments

Trigger arguments are strings:

| Value                 | Form                                                      | Example                       |
|-----------------------|-----------------------------------------------------------|-------------------------------|
| A list                | Space-separated                                           | `group_by=host region`        |
| `tag_filter`          | Space-separated `key=value` pairs, one value for each tag | `tag_filter=host=a region=eu` |
| `sql_filter`, `query` | Passed as is                                              | `sql_filter=usage > 0`        |

For names with spaces or several values for a tag, use the TOML config or the JSON body, where lists and tables are
native.

The CLI splits `--trigger-arguments` at every comma, so a `query` or `sql_filter` with a comma also goes in the TOML
config. Quote the whole list when a value has a space:

```bash
--trigger-arguments "source_measurement=cpu,field=usage,tag_filter=host=a region=eu,horizon_steps=12"
```

### Durations

A duration is a whole number and a unit. Every plugin accepts these units:

| Unit  | Meaning | Example |
|-------|---------|---------|
| `s`   | Seconds | `30s`   |
| `min` | Minutes | `5min`  |
| `h`   | Hours   | `6h`    |
| `d`   | Days    | `7d`    |
| `w`   | Weeks   | `1w`    |

An app sends only these. A plugin can accept more units and says so in its documentation. A unit it doesn't accept gives
`bad_request`. `m` is never minutes: where a plugin accepts it, it's a month.

### Credentials

Secrets listed in the manifest `credentials`, such as a forecasting service's API key, never come from the request body,
the query string, trigger arguments or the TOML config.

On demand, an app sends them in request headers. A plugin can also read them from environment variables of the InfluxDB
server, which is the only way for a scheduled or on-write trigger. A header wins over an environment variable.

A missing or rejected secret gives `unauthorized`. An app asks the user for a secret only after this answer, so it never
asks for a secret already set on the server.

### Reserved keys

An advanced parameter can't reuse these keys:

`source_measurement`, `field`, `tag_filter`, `sql_filter`, `group_by`, `series_limit`, `query`, `source_database`,
`window`, `point_limit`, `sampling_interval`, `horizon_steps`, `horizon_duration`, `levels`, `model`, `write_results`,
`target_measurement`, `target_database`, `forecast_id`, `backtest_points`.
