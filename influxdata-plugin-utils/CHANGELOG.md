# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- `sources.parse_json_body` takes `max_depth` (default 100) and refuses a body
  whose objects and arrays nest deeper than that, counting the top-level
  object as the first level. The refusal used to rest on the interpreter's
  recursion guard alone, which sits near a thousand levels on Python 3.11,
  near ten thousand on 3.13, and past a hundred thousand on 3.14, so "nested
  too deeply" was a promise the parser could not keep on every interpreter.
  `max_depth=None` restores the old behaviour.

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
- `sources.parse_request_headers` folds only the casing of a header name, which
  RFC 9110 makes meaningless; a name is otherwise kept as written, and `rename`
  gives a key another name.
- A header or query parameter the plugin asked for that arrives more than once
  is refused rather than resolved by the order the runtime delivers them in —
  `multi=True` reads every value as a list instead.
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
  interpreted, whatever a value spells, and a key is matched exactly as
  written — except header names, which become lower-case config keys
  (`X-Api-Key` -> `x-api-key`) because their spelling comes from the protocol.
  A validator name is read the same way: dynaconf matched `Validator("Rows")`
  to a `rows` key and walked `Validator("a.b")` into a nested dict, where here
  `a.b` is the name of a flat key and a rule named `Rows` finds nothing while
  the layer carries `rows`.

### Removed

- The `dynaconf` dependency. The package now has none. `Validator` keeps the
  argument names for the subset the plugins use, so most rules port unchanged,
  but these habits from dynaconf no longer hold:
  - `required` is a plain "this key must carry a usable value", and `False` is
    simply no rule. In dynaconf `required` was an alias for `must_exist`, so
    both `required=False` and `must_exist=False` meant "this key must be
    absent" and raised when it was present. `must_exist` is gone; a rule that
    read `required=False` there says nothing here.
  - a string `default` is stored as written. dynaconf read it as TOML, so
    `default="5"` arrived as the number `5` and `default="5", gte=1` passed;
    now that rule fails, and `cast=int` is how a string default becomes a
    number.
  - a callable `default` is stored as the callable itself. dynaconf called it
    with `(settings, validator)` and kept what it returned.
  - a `when` rule does not hold while its key is unset. dynaconf read an absent
    key carrying no existence rule as passing, so
    `Validator("ripple", required=True, when=Validator("prototype", eq="cheby1"))`
    demanded `ripple` from a configuration that never mentioned `prototype`;
    here the rule waits until `prototype` is set.
  - a `when` rule judges a copy, so its own `default` and `cast` are not kept.
    dynaconf applied them to the settings on the way past, so
    `Validator("k", lte=10, when=Validator("k", cast=int))` compared a number
    there and compares the string here. Put the `cast` on the rule itself.
  - a `when` rule that cannot judge -- its own `cast` or predicate raising --
    is reported against the rule that asked, naming both ends: `window: its
    condition could not be checked: rows: invalid literal for int() with base
    10: 'abc'`. dynaconf let the guard's own error out, which named the guarded
    key and never mentioned the rule being written.
  - every failure is a `ValueError`. dynaconf raised `ValidationError`, which is
    not one, and let anything but a `TypeError` out of a cast, a condition or a
    check untouched: `AttributeError` from `startswith` on a number,
    `re.PatternError` from a bad pattern, `KeyError` from a cast.
  - `required=True` is not satisfied by `""`, whitespace or `None`, and it is
    read before `cast`. dynaconf cast first, so `cast=str` turned `None` into
    the string `"None"` and the rule passed.
  - values keep their Python types. dynaconf handed a rule containers of its
    own -- a tuple arrived as a list, a dict as a case-insensitive mapping --
    so `contains="A"` passed on `{"a": 1}` there and fails here.
  - a misspelled check is a `TypeError` where the rule is written. dynaconf
    took any unknown name as a check and raised `AttributeError` at validation
    time, or passed in silence while the key was absent.
  - `env`, `messages`, `description`, `items_validators`, the `|` and `&`
    combinators, `validate_all` and `only`/`exclude` are not carried over. Every
    rule in the list is applied, too: dynaconf's `register` dropped a rule equal
    to an earlier one, and compared everything except `default`.

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
