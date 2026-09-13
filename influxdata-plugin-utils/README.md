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

| Module          | What it provides                                                                                                                             |
|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `sources`       | `KeySpec`, `parse_trigger_args()`, `parse_toml()`, `parse_env()`, `parse_json_body()`, `parse_request_headers()`, `parse_query_parameters()` |
| `config`        | `load_config()`, `load_plugin_config()`, `merge_config_layers()`, `Config`, `resolve_plugin_dir()`, `resolve_path()`                         |
| `validation`    | `Validator`, `validate()`                                                                                                                    |
| `introspection` | `get_table_names()`, `get_tag_names()`, `get_field_names()`, `get_schema()`, `query_window()` with optional `database=`                      |
| `parsing`       | `parse_timedelta()`, `parse_timestamp_ns()`, `parse_int()`, `parse_bool()`, `parse_delimited_list()`, `parse_key_value()`                    |
| `cache`         | `cached(influxdb3_local, key, producer, ttl_seconds=3600, refresh=False, cache_empty=True)`                                                  |
| `write`         | `build_line()`, `build_line_typed()`, `add_field_with_type()`, `write_data()`, `BatchLines`                                                  |

The package has no dependencies, and every module raises `ValueError` on bad
input, so a plugin answers a bad configuration from one `except` clause.

## Configuration

Configuration reaches a plugin from several places: the trigger arguments, a
TOML file, environment variables, and — for `process_request` plugins — the
request body, its headers and its query string. Each of those is a **source**
with its own parser, and each parser returns a plain dict.

`load_config` merges the dicts you give it and validates the result. The
argument order is the precedence, lowest first.

```python
from influxdata_plugin_utils.config import load_config
from influxdata_plugin_utils.parsing import parse_timedelta
from influxdata_plugin_utils.sources import (
    KeySpec,
    parse_env,
    parse_json_body,
    parse_query_parameters,
    parse_request_headers,
    parse_toml,
    parse_trigger_args,
)
from influxdata_plugin_utils.validation import Validator

BODY = KeySpec(allowlist=["measurement", "field", "window"], unknown="reject")
QUERY = KeySpec(allowlist=["window"])
HEADERS = KeySpec(allowlist=["x-api-key"], rename={"x-api-key": "api_key"})
ENV = KeySpec(allowlist=["PLUGIN_API_KEY"], rename={"PLUGIN_API_KEY": "api_key"})

VALIDATORS = [
    Validator("measurement", required=True),
    Validator("api_key", required=True),
    Validator("window", default="1h", cast=parse_timedelta),
    Validator("limit", default=1000, cast=int, gte=1, lte=10_000),
]

def process_request(
    influxdb3_local, query_parameters, request_headers, request_body, args=None
):
    cfg = load_config(
        parse_env(ENV),
        parse_trigger_args(args),
        parse_toml(args.get("config_file_path") if args else None),
        parse_json_body(request_body, BODY),
        parse_request_headers(request_headers, HEADERS),
        parse_query_parameters(query_parameters, QUERY),
        validators=VALIDATORS,
    )
    influxdb3_local.info(f"{cfg.measurement} window={cfg['window']}")
```

`load_plugin_config` covers three layers in a fixed order — the named
environment variables, the trigger arguments, and the file at
`config_file_path`:

```python
from influxdata_plugin_utils.config import load_plugin_config

def process_scheduled_call(influxdb3_local, call_time, args):
    cfg = load_plugin_config(args, validators=VALIDATORS, env_keys=["PLUGIN_API_KEY"])
```

Pass `source="args"` or `source="toml"` to use only one of the last two. Prefer
`load_config` in new plugins: there the layers are ordinary arguments, so a
plugin adds, reorders or drops any of them.

### What a source contributes

A `KeySpec` says which keys of a source become config values and under what
names:

```python
KeySpec(allowlist=["measurement"], rename={"measurement": "table"}, unknown="reject")
```

- `allowlist` names the keys that pass, `denylist` the ones that do not;
- `rename` maps a source key onto the config key it becomes;
- `unknown` decides what happens to a refused key — `"ignore"` drops it,
  `"reject"` names it in the error so the sender learns what was wrong.

On a layer the caller controls, prefer `allowlist`: a parameter added to the
plugin later stays unreachable until it is listed, where a `denylist` would let
it through unnoticed.

Header names are matched regardless of casing and hyphenation and become config
keys (`X-Api-Key` → `x_api_key`). Everywhere else names are matched and kept
exactly as written. `parse_env` requires an allowlist: the process environment
belongs to the host and holds credentials, so nothing is read without being
named. `parse_toml` refuses a path that does not name a `.toml` file before
opening it; pass `require_suffix=False` for a config file named some other way,
and `is_toml_path()` answers the same question without reading anything. `Authorization` never reaches a plugin — the engine authenticates with
it — so a token needs a header of your own.

A value that arrives empty — a blank string, a JSON `null`, an unset variable —
is left out of its layer, so a validator default applies instead and a blank in
one layer does not erase the layer below it. `0`, `False` and `[]` are real
values and are kept.

### Holding a key against the request

`merge_config_layers` merges without validating, and can hold chosen keys
against the layers above them:

```python
merged = merge_config_layers(args, body, pinned=["measurement"])
```

A later layer that sets a pinned key raises; `on_conflict="ignore"` keeps the
value already set instead. A pinned key nobody set stays open, so the same
plugin works with or without a fixed measurement.

### Validating

A `Validator` describes one config key: the default it falls back to, the cast
that turns it into a usable type, and the checks it must then pass.

```python
Validator("window", default="1h", cast=parse_timedelta, gt=timedelta(0), lte=timedelta(days=30))
Validator("aggregate", default="mean", is_in=("mean", "min", "max", "count"))
Validator("ripple", required=True, when=Validator("prototype", eq="cheby1"))
```

`required` asks for a usable value, so a key that arrives blank or `null`
counts as unset. `when` applies a rule only while another one holds — and holds
means the key is there and passes — while `condition` takes any predicate. The
checks are `eq`, `ne`, `gt`, `gte`, `ge`, `lt`, `lte`, `le`,
`identity`, `is_type_of`, `is_in`, `is_not_in`, `contains`, `cont`,
`not_contains`, `len_eq`, `len_ne`, `len_min`, `len_max`, `startswith`,
`endswith`, `not_startswith`, `not_endswith`, `regex` and `not_regex`. They are
named explicitly, so a misspelled one is a `TypeError` where the rule is
written.

Validation runs once, over the merged values: defaults fill what no layer set,
`cast` runs next, and the checks see the cast value.

TOML becomes native — no manual string parsing:

```toml
source_table = "cpu"
batch_size = 2000
excluded_fields = ["usage_idle", "usage_guest"]
```

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
