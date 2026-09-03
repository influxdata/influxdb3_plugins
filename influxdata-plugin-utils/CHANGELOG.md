# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-09-03

### Added

- `introspection.get_schema(influxdb3_local, table)` returns
  `{column_name: data_type}` from one `information_schema` query.
- `cache.cached()` gains two parameters: `refresh` replaces a stored entry, and
  `cache_empty=False` leaves a falsy result unstored. Combined, a refresh that
  produces an empty value drops the entry, so a table dropped between reads
  leaves neither its old schema cached nor an empty one that would never be
  retried. Every introspection lookup passes `cache_empty=False`, so an empty
  answer is retried rather than remembered; `get_schema()` also forwards
  `refresh`, letting a caller re-read a schema on seeing an unknown column.

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

[Unreleased]: https://github.com/influxdata/influxdb3_plugins/compare/utils-v0.3.1...HEAD
[0.3.1]: https://github.com/influxdata/influxdb3_plugins/compare/utils-v0.3.0...utils-v0.3.1
[0.3.0]: https://github.com/influxdata/influxdb3_plugins/compare/utils-v0.2.0...utils-v0.3.0
[0.2.0]: https://github.com/influxdata/influxdb3_plugins/compare/utils-v0.1.0...utils-v0.2.0
[0.1.0]: https://github.com/influxdata/influxdb3_plugins/releases/tag/utils-v0.1.0
