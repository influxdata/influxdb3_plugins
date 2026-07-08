# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
- `examples/simple_downsampler` — scheduled plugin demonstrating every module.

[Unreleased]: https://github.com/influxdata/influxdb3_plugins/compare/pkg-v0.1.0...HEAD
[0.1.0]: https://github.com/influxdata/influxdb3_plugins/releases/tag/pkg-v0.1.0