# influxdata-plugin-utils

Shared helpers for InfluxDB 3 plugins.

## Install

```bash
pip install influxdata-plugin-utils
```

Editable, for local development:

```bash
pip install -e influxdata-plugin-utils
```

## Modules

| Module          | What it provides                                                                                                                                     |
|-----------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| `config`        | `load_plugin_config(args, validators)` (dynaconf-backed), `merge_config_layers()`, `resolve_plugin_dir()`, `resolve_path()`, re-exported `Validator` |
| `introspection` | `get_table_names()`, `get_tag_names()`, `get_field_names()`, `get_schema()`, `query_window()` with optional `database=`                              |
| `parsing`       | `parse_timedelta()`, `parse_timestamp_ns()`, `parse_int()`, `parse_bool()`, `parse_delimited_list()`, `parse_key_value()`                            |
| `request`       | `parse_json_body()`, `parse_request_headers()`, `parse_query_parameters()`                                                                           |
| `cache`         | `cached(influxdb3_local, key, producer, ttl_seconds=3600, refresh=False, cache_empty=True)`                                                          |
| `write`         | `build_line()`, `build_line_typed()`, `add_field_with_type()`, `write_data()`, `BatchLines`                                                          |

Every module raises `ValueError` on bad input.

## Config: precedence

`load_plugin_config` merges sources low → high: **env vars → engine `args` → TOML file**. A provided TOML config file overrides everything. Environment variables are read only when their exact names are passed via `env_keys=[...]`; nothing is read from the environment by default.

Keys are stored literally, dots included, so read a nested value as `cfg.section["key"]` rather than `cfg.get("section.key")`.

Keys that name a dynaconf option are dropped from every layer — they would configure the loader instead of the plugin. Those are any key ending in `_FOR_DYNACONF`, any key starting with `DYNACONF`, and `DEFAULT_SETTINGS_PATHS`, `DYNABOXIFY`, `PROJECT_ROOT`, `RENAMED_VARS`, `SETTINGS_MODULE`. Do not use them as plugin parameters.

```python
from influxdata_plugin_utils.config import load_plugin_config, Validator
from influxdata_plugin_utils.parsing import parse_timedelta

def process_scheduled_call(influxdb3_local, call_time, args):
    cfg = load_plugin_config(
        args,
        validators=[
            Validator("source_table", must_exist=True),
            Validator("batch_size", default=1000, gte=1, lte=10000, cast=int),
            Validator("window", default="5min", cast=parse_timedelta),
        ],
    )
    influxdb3_local.info(f"{cfg.source_table} window={cfg.window}")
```

TOML becomes native — no manual string parsing:

```toml
source_table = "cpu"
batch_size = 2000
excluded_fields = ["usage_idle", "usage_guest"]
```

## HTTP request layers

`process_request` plugins receive configuration from the request itself. Each
parser turns one raw input into a dict ready for `load_plugin_config`: `names`
selects the keys a layer may contribute, and for the body and query string
`unknown` decides what happens to the rest — dropped by default, or named back
with `unknown="reject"`. A top-level
value that arrives empty — a blank string, a JSON `null` — counts as "not
provided" and is dropped, so the validator's default applies. Nested values are
passed through untouched.

```python
from influxdata_plugin_utils.config import (
    load_plugin_config,
    merge_config_layers,
)
from influxdata_plugin_utils.request import (
    parse_json_body,
    parse_query_parameters,
    parse_request_headers,
)

BODY_KEYS = {"measurement", "field", "window"}

def process_request(
    influxdb3_local, query_parameters, request_headers, request_body, args=None
):
    body = parse_json_body(request_body, BODY_KEYS)
    query = parse_query_parameters(query_parameters, ["window"])
    creds = parse_request_headers(
        request_headers, {"source-token": "source_token"}
    )
    cfg = load_plugin_config(
        merge_config_layers(args, body, creds, query),
        validators=VALIDATORS,
        source="args",
    )
```

`names` accepts one name, a sequence of names, or a `{source: config_key}` dict
that renames. It is required for headers, since every request also carries proxy
and content headers that do not belong in a config object — those are always
dropped. The engine consumes the `Authorization` header itself and does not pass
it on, so a plugin that takes a token over HTTP needs a header of its own.

Only header names are normalized into config keys (`X-Api-Key` → `x_api_key`),
because their casing and hyphenation come from the protocol rather than from
you. Body and query names are matched and kept exactly as written, so rename
them yourself when you need to: `{"max-rows": "max_rows"}`.

Headers and query parameters may arrive as a mapping or as name/value pairs. A
repeated name reads as its first value, or as every value with `multi=True`.

`merge_config_layers` takes the layers in increasing precedence, so by default a
request overrides the trigger arguments. Values that arrive empty are dropped
from every layer, and `load_plugin_config` drops them too, so a blank trigger
argument lets a validator default apply instead of shadowing it.

To keep one key out of a caller's reach, name it in `pinned`:

```python
merged = merge_config_layers(args, body, pinned=["measurement"])
```

An overlay that sets a pinned key raises; pass `on_conflict="ignore"` to keep
the `base` value silently instead. A pinned key the trigger never set stays
open, so the same plugin can be deployed with or without a fixed measurement.
The pin covers the layers passed here and not a TOML file: under the default
`source="merge"` the TOML layer outranks everything, so a plugin that accepts
`config_file_path` from a request should load with `source="args"`.

## Write helpers

`LineBuilder` is a runtime global injected into the plugin, so builders take the
class as their first argument:

```python
from influxdata_plugin_utils.write import build_line, write_data

lines = [
    build_line(LineBuilder, "cpu", tags={"host": "a"}, fields={"usage": 12.5}, time_ns=ts)
]
write_data(influxdb3_local, lines)            # batched + retried by default
# write_data(influxdb3_local, lines, batch=False, retries=0)  # opt out
# write_data(influxdb3_local, lines, database="other_db")     # another database
# write_data(influxdb3_local, lines, no_sync=True)            # write_sync API (3.8+)
```

## Cross-database queries

On InfluxDB versions that support processing-engine cross-database queries,
the introspection helpers accept `database=` and pass it through to
`influxdb3_local.query`.
Cached schema results are separated per database.

```python
from influxdata_plugin_utils.introspection import get_field_names, query_window

fields = get_field_names(influxdb3_local, "cpu", database="source_db")
rows = query_window(
    influxdb3_local,
    "cpu",
    start=start,
    end=end,
    columns=fields,
    database="source_db",
)
```

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
