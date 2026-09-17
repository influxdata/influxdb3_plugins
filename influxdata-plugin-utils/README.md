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
| `introspection` | `get_table_names()`, `get_tag_names()`, `get_field_names()`, `get_schema()`, `get_line_schema()`, `query_window()` with optional `database=`; `TAG_DATA_TYPE`, `NUMERIC_TYPES`, `LINE_TYPES`, `NUMERIC_LINE_TYPES` |
| `parsing`       | `parse_timedelta()`, `parse_timestamp_ns()`, `parse_int()`, `parse_bool()`, `parse_delimited_list()`, `parse_key_value()`                    |
| `cache`         | `cached(influxdb3_local, key, producer, ttl_seconds=3600, refresh=False, cache_empty=True)`                                                  |
| `write`         | `build_line()`, `build_line_typed()`, `split_row()`, `infer_type()`, `add_field_with_type()`, `write_data()`, `BatchLines`                  |

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
from influxdata_plugin_utils.parsing import parse_int, parse_timedelta
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
    Validator("limit", default=1000, cast=parse_int, gte=1, lte=10_000),
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

Header names are matched regardless of casing — RFC 9110 makes it meaningless —
and become config keys spelled in lower case (`X-Api-Key` → `x-api-key`); use
`rename` for a name of your own. Everywhere else names are matched and kept
exactly as written.

Within one spec, spell a `rename` key the way the `allowlist` spells it. A
`KeySpec` does not know which source will read it, so it checks the two against
each other as written, and `allowlist=["X-Api-Key"]` with
`rename={"x-api-key": "api_key"}` is refused at construction. Either spelling
works as long as both use it.

A header or query parameter the plugin asked for that arrives more than once is
refused, since which value it would otherwise get is the order the runtime
delivers them in; `multi=True` reads every value as a list instead. InfluxDB 3
hands the plugin a plain dict, which holds one value per name, so a repeat never
reaches a plugin there and neither the refusal nor `multi` fires; both are for a
runtime that delivers name/value pairs.

`parse_env` requires an allowlist: the process environment belongs to the host
and holds credentials, so nothing is read without being named. `Authorization`
never reaches a plugin — the engine authenticates with it — so a token needs a
header of your own.

`parse_toml` refuses a path that does not name a `.toml` file before opening it;
pass `require_suffix=False` for a config file named some other way, and
`is_toml_path()` answers the same question without reading anything. Otherwise
it reads whatever path it is given: a relative one resolves under the plugin
directory, an absolute one is used as is. Take that path from a layer the
operator controls — the trigger arguments, or the file itself. A path that
arrives in the request body, a header or the query string lets the caller name
any file the engine can read, and whatever parses as TOML becomes this plugin's
configuration, another plugin's credentials included.

A value that arrives empty — a blank string, a JSON `null`, an unset variable —
is left out of its layer, so a validator default applies instead and a blank in
one layer does not erase the layer below it. `0`, `False` and `[]` are real
values and are kept.

Headers, query-string parameters and environment variables arrive as text, so
surrounding whitespace is trimmed before anything else sees the value: a header
sent as `  secret  ` becomes `secret`. A trigger argument, a body field and a
TOML value keep exactly what was written, whitespace included.

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
means the key is there and passes. A `when` rule that cannot judge at all, say
a `cast` of its own that fails, gives no answer, and that is reported against
the rule which asked rather than quietly leaving it out. `condition` takes any
predicate and runs after the checks. The checks are `eq`, `ne`, `gt`, `gte`,
`ge`, `lt`, `lte`, `le`, `identity`, `is_type_of`, `is_in`, `is_not_in`,
`contains`, `cont`, `not_contains`, `len_eq`, `len_ne`, `len_min`, `len_max`,
`startswith`, `endswith`, `not_startswith`, `not_endswith`, `regex` and
`not_regex`. They are named explicitly, so a misspelled one is a `TypeError`
where the rule is written.

`regex` and `not_regex` match from the start of the value, so `regex="b"`
rejects `"abc"`, and a pattern that may appear anywhere needs `.*` in front.
`is_type_of` reads a parameterized generic through to the items, so
`list[int]`, `dict[str, int]` and `tuple[int, ...]` say what they look like.

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

## Rebuilding a line from a row

A row from `process_writes` and a row from `influxdb3_local.query()` are the
same flat dict keyed by column name, and neither says which keys are tags or
what type each field column has. `get_line_schema` reads that from the catalog
and `split_row` applies it:

```python
from influxdata_plugin_utils.introspection import get_line_schema
from influxdata_plugin_utils.write import build_line_typed, split_row

def process_writes(influxdb3_local, table_batches, args=None):
    for batch in table_batches:
        table = batch["table_name"]
        schema = get_line_schema(influxdb3_local, table)
        known = set(schema["tags"]) | set(schema["fields"]) | {"time"}
        if any(key not in known for row in batch["rows"] for key in row):
            # a column this batch created is not in the cached schema yet
            schema = get_line_schema(influxdb3_local, table, refresh=True)
        for row in batch["rows"]:
            tags, typed_fields, time_ns = split_row(row, schema)
            typed_fields["enriched"] = (True, "bool")
            line = build_line_typed(
                LineBuilder, "cpu_enriched", tags=tags, typed_fields=typed_fields, time_ns=time_ns
            )
```

A key the schema does not know becomes a field typed from its value, so the
refresh matters for rows the plugin did not select itself: without it a tag
added since the schema was cached would be written as a string field.

`get_line_schema` raises `ValueError` for a table the catalog does not know,
as `get_schema` does. Catch it where the plugin wants its task id in the message.

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
