# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-09-12

### Added

- `sources` module: one parser per place configuration comes from —
  `parse_trigger_args()`, `parse_toml()`, `parse_env()`, `parse_json_body()`,
  `parse_request_headers()` and `parse_query_parameters()`. Each reads one raw
  input and returns a plain dict, so a plugin composes the layers it needs.
- `sources.KeySpec` says which keys of a source become config values and under
  what names: `allowlist`, `denylist`, `rename`, and an `unknown` policy that
  either drops a refused key or names it back to whoever sent it. `parse_env`
  requires an allowlist, since the process environment belongs to the host.
- `sources.parse_toml` refuses a path that does not name a `.toml` file before
  opening it; `require_suffix=False` lifts that for a config file named some
  other way, and `sources.is_toml_path()` answers the same question on its own,
  for a plugin that would rather report the path itself than raise.
- `config.load_config(*layers, validators=...)` merges the layers in the order
  given — lowest precedence first — and validates the result once.
- `config.merge_config_layers(*layers, pinned=...)` merges without validating
  and can hold chosen keys against the layers above them, so a request cannot
  move what the operator fixed.
- `config.Config`, the validated configuration: a dict that also answers to
  attribute access.
- `validation` module: `Validator` and `validate()`. A rule carries a default,
  a `cast`, and checks — 25 of them, from `gte` and `is_in` to `regex` — plus
  `condition` for an arbitrary predicate and `when` to apply a rule only while
  another one holds. A list, dict or set `default` is copied for each use, so
  one rule's default cannot be changed through the values it fills in. Checks
  are named explicitly, so a misspelled one is a `TypeError` where the rule is
  written.
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

- `config.load_plugin_config` reads the same three layers as before — the named
  environment variables, the trigger arguments, the TOML file — and returns a
  `Config`. A value that arrives empty is left out of its own layer, so a blank
  trigger argument lets a validator default apply and a blank in the file no
  longer erases the argument underneath it. Every failure is a `ValueError`,
  including an unreadable file and a rejected value, and a `config_file_path`
  that does not name a `.toml` file is now refused before the file is opened.
  It stays supported, and `load_config` is the one to reach for in new plugins.
- Configuration keys are stored as they arrive. Nothing in a layer is
  interpreted, whatever a value spells, and a key is matched exactly as written
  — except header names, which become config keys (`X-Api-Key` -> `x_api_key`)
  because their spelling comes from the protocol.

### Removed

- The `dynaconf` dependency. The package now has none.

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
