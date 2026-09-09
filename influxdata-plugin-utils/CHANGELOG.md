# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-09-08

### Added

- `request` module for `process_request` plugins: `parse_json_body()`,
  `parse_request_headers()` and `parse_query_parameters()` turn one raw runtime
  input into a dict ready for `load_plugin_config`.
- All three take the same `names` selection (one name, a sequence, or a
  `{source: config_key}` dict to rename; `None` reads every key) and the same
  `unknown` policy: `"ignore"` drops the rest, `"reject"` names a bounded sample
  of them back to the caller. Headers are worth naming explicitly, since a
  client sends `host`, `user-agent` and friends on every request. Only header
  names become config keys (`X-Api-Key` -> `x_api_key`); body and query names
  are kept as written. A top-level value that arrives empty is dropped, so a
  validator default applies.
- Headers and query parameters are read from a mapping or from a sequence of
  name/value pairs, as byte-level and ASGI runtimes deliver them; a repeated
  name reads as its first value, or as every value with `multi=True`. Two header
  spellings that fold onto one config key are refused: `X-Api-Key` and
  `x_api_key` are separate headers on the wire, so whichever the runtime listed
  first would otherwise win silently.
- `parse_json_body()` caps the body at 10 MiB, accepts a leading byte order
  mark, and rejects a body that is not JSON text, bytes or a dict, is not a
  JSON object, or is nested too deeply. Every function raises `ValueError`, so a
  plugin can answer a bad request from one `except` clause.
- `config.merge_config_layers(base, *overlays)` merges layers in increasing
  precedence, dropping values that arrive empty; `0`, `False` and `[]` are
  kept. `pinned=[...]` names keys an overlay may not change once `base` sets
  them, compared as the settings store keeps them; `on_conflict` chooses
  between raising and keeping the `base` value.
- `introspection.get_schema(influxdb3_local, table)` returns
  `{column_name: data_type}` from one `information_schema` query.
- `cache.cached()` gains two parameters: `refresh` replaces a stored entry, and
  `cache_empty=False` leaves a falsy result unstored. Combined, a refresh that
  produces an empty value drops the entry, so a table dropped between reads
  leaves neither its old schema cached nor an empty one that would never be
  retried. Every introspection lookup passes `cache_empty=False`, so an empty
  answer is retried rather than remembered; `get_schema()` also forwards
  `refresh`, letting a caller re-read a schema on seeing an unknown column.

### Changed

- `config.load_plugin_config` — bad input always raises `ValueError`, in line
  with the rest of the package. A validator rejection surfaced as dynaconf's
  `ValidationError` and an unreadable TOML file as `OSError`; both are now
  `ValueError`, keeping the original message.

### Security

- `config.load_plugin_config` — drop layer keys that name a dynaconf option:
  they share the settings store with config keys, so a request body could
  reach them. `AUTO_CAST_FOR_DYNACONF` switched `@` token substitution back on
  and reopened
  [#134](https://github.com/influxdata/influxdb3_plugins/issues/134), letting
  `@format {env[...]}` read the host's environment and `@read_file` its
  filesystem; `dynaconf_include` and `default_settings_paths` made the loader
  read a file of the sender's choosing; `dynaboxify` turned every nested table
  into a plain dict.
- `config.load_plugin_config` — pin `AUTO_CAST_FOR_DYNACONF`,
  `DOTTED_LOOKUP_FOR_DYNACONF` and `MERGE_ENABLED_FOR_DYNACONF` to `False`,
  after the settings object is built as well: dynaconf reads its options from
  the process environment too, and that value wins over a constructor
  argument. With dotted lookup off, a key such as `measurement.sub` is stored
  literally instead of replacing `measurement`; read a nested value as
  `cfg.section["key"]` rather than `cfg.get("section.key")`.
- `config.load_plugin_config` — each layer drops its own blank values, matching
  `merge_config_layers`, so a blank means "not set here": a blank trigger
  argument lets a validator default apply instead of shadowing it, and a blank
  in the TOML file no longer erases the argument underneath it.

## [0.3.1] - 2026-08-03

### Changed

- No functional changes. Released to exercise the tag-triggered release
  automation added in
  [#137](https://github.com/influxdata/influxdb3_plugins/pull/137), which
  builds release notes from this file and publishes a GitHub release
  alongside the PyPI upload.

## [0.3.0] - 2026-07-31

### Security

- `config.load_plugin_config` — disable dynaconf's `@` token substitution
  (`@read_file`, `@format`, `@jinja`, `@get`, and ~30 others) by constructing
  the settings object with `AUTO_CAST_FOR_DYNACONF=False`. Previously any
  string value beginning with `@` was evaluated, so an untrusted value from an
  HTTP request body could read the server's files or environment variables
  (for example `@read_file /etc/passwd` or `@format {env[SECRET]}`). Values are
  now always treated as literal data. See
  [#134](https://github.com/influxdata/influxdb3_plugins/issues/134).

### Changed

- Pin `dynaconf>=3.2,<4` so a future major release cannot silently re-enable
  token substitution.

## [0.2.0] - 2026-07-12

### Added

- `write.write_data` — optional `database` parameter for writing to another
  database.
- `introspection` — optional `database` parameter for schema helpers and
  `query_window`.
- `parsing.parse_timedelta` — `ms` (milliseconds) and `us` (microseconds)
  duration units.

### Changed

- `write.write_data` — `no_sync` now defaults to `None`: writes go through
  `write` / `write_to_db` (available on all InfluxDB 3 versions); passing a
  boolean switches to `write_sync` / `write_sync_to_db` (InfluxDB 3.8+).

## [0.1.0] - 2026-07-08

### Added

- `config` — dynaconf-backed config loading (`load_plugin_config`), plugin
  directory resolution (`resolve_plugin_dir`, `resolve_path`), re-exported
  `Validator`.
- `introspection` — schema helpers (`get_table_names`, `get_tag_names`,
  `get_field_names`) and `query_window`, with optional TTL caching.
- `parsing` — `parse_timedelta`, `parse_timestamp_ns`, `parse_int`,
  `parse_bool`, `parse_delimited_list`, `parse_key_value`.
- `cache` — `cached` TTL wrapper over `influxdb3_local.cache`.
- `write` — `build_line`, `build_line_typed`, `add_field_with_type`,
  `write_data` (batching + retry), `BatchLines`.

[Unreleased]: https://github.com/influxdata/influxdb3_plugins/compare/utils-v0.4.0...HEAD
[0.4.0]: https://github.com/influxdata/influxdb3_plugins/compare/utils-v0.3.1...utils-v0.4.0
[0.3.1]: https://github.com/influxdata/influxdb3_plugins/compare/utils-v0.3.0...utils-v0.3.1
[0.3.0]: https://github.com/influxdata/influxdb3_plugins/compare/utils-v0.2.0...utils-v0.3.0
[0.2.0]: https://github.com/influxdata/influxdb3_plugins/compare/utils-v0.1.0...utils-v0.2.0
[0.1.0]: https://github.com/influxdata/influxdb3_plugins/releases/tag/utils-v0.1.0
