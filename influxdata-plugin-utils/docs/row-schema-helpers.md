# Design: row schema helpers for `influxdata_plugin_utils`

Status: accepted, not yet implemented

## Problem

A row from `process_writes` and a row from `influxdb3_local.query()` have the
same shape and the same value types: a flat dictionary keyed by column name,
native Python values, nanosecond-integer time, and `None` for null. Neither row
says which columns are tags or which Arrow type each field column has. That
information lives only in `information_schema.columns`.

Every plugin that rebuilds a line from a row therefore reads the catalog itself.
A survey of the repository found the same work done in incompatible ways:

- The tag data type string `Dictionary(Int32, Utf8)` is redeclared in seven
  plugins. The utils copy is private.
- The Arrow-to-line-protocol type map exists as an identical private copy in
  `geo_enrichment` and `gapfill`. It does not exist in utils.
- Value-type inference for fields with no known type is hand-rolled in seven
  plugins: `geo_enrichment`, `gapfill`, `basic_transformation`, `downsampler`,
  `sagemaker`, `import`, and `schema_validator`.
- `downsampler` uses a narrower numeric type set than utils, `gapfill`, and
  `nori_regression`, and writes UInt64 aggregates through `int64_field`.
- Only `geo_enrichment` re-reads the catalog when a row carries a new column.
  `resampler` and `synthefy_forecasting` disable caching to get the same
  effect.
- `kafka_subscriber` and `amqp_subscriber` share one local copy of
  `add_field_with_type`. `mqtt_subscriber` and `opcua` share another that lacks
  the range and finite checks.

## Goals

- One place that turns a table name into a tag list and a map of field name to
  line-protocol type.
- One place that turns a row plus that schema into tags, typed fields, and
  time, ready for the existing `build_line_typed`.
- Consistent handling of booleans, unknown types, `None` values, and columns
  that appear after the schema was cached.

## Non-goals

The API encodes the one correct path from a row and a table to a typed line.
A plugin that does something else keeps that logic itself. Specifically
declined:

- Per-field type overrides or column renames. `basic_transformation` changes
  types and names on purpose. It owns its output types.
- A configurable time key. Both documented row shapes use `time`.
  `downsampler` renames its `_time` alias before building the line.
- Include, exclude, or rename filters on the split. Callers filter the
  returned dictionaries.
- Type aliases such as `double` or `unsigned`, or inferring when a declared
  type is unrecognized. `schema_validator` normalizes before calling utils.
- A strict mode that raises on unsupported values. `sagemaker` keeps a local
  guard.
- Re-reading the catalog from inside the schema lookup based on the rows
  passed in. The lookup stays a pure lookup with `refresh`. The check stays in
  the caller.
- Deriving output types for computed columns. `downsampler` and `gapfill`
  build their own output schema.
- InfluxQL sources. `import` reads schema from a remote v1 or v2 server and
  stays as it is.

## Additions

### `introspection`

Make the private constants public and add the type map.

```python
TAG_DATA_TYPE: str
NUMERIC_TYPES: frozenset[str]
LINE_TYPES: dict[str, str]   # Arrow data_type -> int | uint | float | bool | string
NUMERIC_LINE_TYPES: frozenset   # {"int", "uint", "float"}
```

`NUMERIC_LINE_TYPES` is the set a numeric column's line type falls in, for a
plugin that has a line schema and wants the aggregatable columns. `LINE_TYPES`
is a read-only mapping.

Add one lookup built on `get_schema`.

```python
def get_line_schema(
    influxdb3_local,
    table: str,
    *,
    database: str | None = None,
    use_cache: bool = True,
    ttl_seconds: int | None = 3600,
    refresh: bool = False,
) -> dict
```

Returns `{"tags": list[str], "fields": dict[str, str | None]}`. A field maps
to `None` when its Arrow type is not in `LINE_TYPES`. An unknown table raises
`ValueError`, as `get_schema` now does; a plugin that wants its task id in the
message catches and re-raises. The TTL default matches `get_schema`.

### `write`

Make the private inference helper public.

```python
def infer_type(value) -> str
```

Add one split.

```python
def split_row(row: Mapping[str, Any], schema: dict) -> tuple[dict, dict, int | None]
```

Returns `(tags, typed_fields, time_ns)`. Iteration is row-driven: every key in
the row is placed by the schema, and a key the schema does not know becomes a
field typed by `infer_type`. `None` values are skipped. `time` is removed from
the fields and returned separately. The result feeds `build_line_typed`
unchanged.

Because unknown keys become fields, a caller that receives rows it did not
project must check for unknown keys and call `get_line_schema` with
`refresh=True` before splitting. Otherwise a newly added tag would be written
as a string field and the write would fail on a type conflict. That check is
three lines and stays in the caller.

## Adoption

### Full adopters, with the plugin-side change that enables it

| Plugin | Change | Behavior change |
|---|---|---|
| `geo_enrichment` | Delete `TAG_DATA_TYPE`, `LINE_TYPES`, `infer_line_type`, and `resolve_schema`. `schema_for` stays as the unknown-key check that passes `refresh=True`. In-place mode calls `split_row` and discards the typed fields before adding its own. | None |
| `gapfill` | `resolve_schema` becomes `get_line_schema` plus the local marker-collision check. Fill and copy sites build `{**tag_values, **values, "time": ts}` and call `split_row`. Report lines stay on `build_line`. | None |
| `downsampler` | Rename the `_time` key to `time` in Python, not in SQL, since the source `time` column shares the query. Replace `get_aggregatable_fields` with the schema lookup filtered to int, uint, and float. Build an output schema: `avg`, `median`, `stddev`, `var`, and `approx_median` map to float, `count` and `record_count` to int, `sum`, `min`, `max`, `first_value`, and `last_value` keep the source type. Pass it to `split_row`. | Int32 and Float32 columns become aggregatable. UInt64 aggregates are written as uint. |
| `basic_transformation` | Call `split_row` with the renamed tag list from `tags_mapping` and an empty field map, so every field goes through `infer_type`. Delete `transform_to_influx_line`. Name lookups move to `get_tag_names` and `get_field_names`. | None |
| `nori_regression` | Replace the raw query with `get_line_schema` and `use_cache=False`. The numeric check becomes a line-type membership test. Delete the constants. | Error messages show `int` rather than `Int64`. |
| `sagemaker` | Replace the column query with `get_line_schema` keys plus `time`. Replace `add_typed_field` with a local guard that rejects values other than bool, int, float, and str, then `add_field_with_type` with `infer_type`. | None |
| `schema_validator` | Keep a local alias map to the five utils type names, then call `add_field_with_type`. Unknown alias falls through to `infer_type`. Delete both local helpers. | None |

### Existing helpers only

| Plugin | Change |
|---|---|
| `resampler` | One `get_line_schema` with `use_cache=False` replaces three catalog queries. Field and numeric lists derive from the map. |
| `synthefy_forecasting` | One `get_line_schema` with `use_cache=False` replaces two catalog queries. |
| `simple_data_replicator` | `get_tag_names`. Keeps writing through the client `Point`, which infers types itself and has no unsigned type. |
| `influxdb_to_iceberg` | `get_schema` with `exclude_time=False`, since it keeps `time` in its field list. |
| `state_change`, `stateless_adtk_detector`, `threshold_deadman_checks` | Delete the second lookup on an empty result and its stale comment. `cached` never stores an empty list, so the retry only repeats the same query. |
| `kafka_subscriber`, `amqp_subscriber`, `mqtt_subscriber`, `opcua` | Delete the local `add_field_with_type` and import the utils one. See below. |

The four ingest plugins take two behavior changes. Bool strings outside the
recognized set currently become `False`. utils `parse_bool` raises, and all
four already wrap the call and re-raise as a conversion error, so a bad message
fails per record instead of silently writing `False`. The utils truthy set is
exactly the five strings the local copies accept, so no accepted input changes
meaning. Separately,
`mqtt_subscriber` and `opcua` gain the int64 and uint64 range checks and the
finite-float check they lack today.

### Unchanged

`import` keeps its InfluxQL schema path. `forecast_error_evaluator`,
`mad_check`, and `valuecounter` already use only utils helpers and need
nothing.

## Order of work

1. Add the constants, `get_line_schema`, `infer_type`, and `split_row` to
   utils with tests, and release.
2. Move `geo_enrichment` first. It is the only plugin that exercises every
   piece, including the refresh path.
3. Move `gapfill`, `downsampler`, and `basic_transformation`, which delete the
   most duplicated code.
4. The remaining rows are independent single-plugin changes and can land in
   any order.
