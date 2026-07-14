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

| Module          | What it provides                                                                                                            |
|-----------------|-----------------------------------------------------------------------------------------------------------------------------|
| `config`        | `load_plugin_config(args, validators)` (dynaconf-backed), `resolve_plugin_dir()`, `resolve_path()`, re-exported `Validator` |
| `introspection` | `get_table_names()`, `get_tag_names()`, `get_field_names()`, `query_window()`                                               |
| `parsing`       | `parse_timedelta()`, `parse_timestamp_ns()`, `parse_int()`, `parse_bool()`, `parse_delimited_list()`, `parse_key_value()`   |
| `cache`         | `cached(influxdb3_local, key, producer, ttl_seconds=3600)`                                                                  |
| `write`         | `build_line()`, `build_line_typed()`, `add_field_with_type()`, `write_data()`, `BatchLines`                                 |

## Config: precedence

`load_plugin_config` merges sources low → high: **env vars → engine `args` → TOML file**. A provided TOML config file overrides everything. Environment variables are read only when their exact names are passed via `env_keys=[...]`; nothing is read from the environment by default.

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

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.