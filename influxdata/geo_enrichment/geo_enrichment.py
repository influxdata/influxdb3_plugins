"""
{
    "plugin_type": ["onwrite", "http"],
    "onwrite_args_config": [
        {
            "name": "source_measurements",
            "example": "gps fleet_pos",
            "description": "Space-separated source tables to enrich. Rows from other tables in the batch are ignored.",
            "required": true
        },
        {
            "name": "output_columns",
            "example": "country_code:geo_country city:geo_city",
            "description": "Space-separated 'attribute:column' pairs selecting which resolver attributes to write and under what column names. With a GeoJSON reference_file an attribute is a JSONPath into the feature properties, so nested values are reachable as owner.name; with a CSV one it is a column name.",
            "required": true
        },
        {
            "name": "output_mode",
            "example": "field",
            "description": "'field' writes geo attributes as fields, which merge into the source row. 'tag' writes them as tags and requires target_measurement. Defaults to 'field'.",
            "required": false
        },
        {
            "name": "target_measurement",
            "example": "gps_located",
            "description": "Destination table. Omit to enrich the source table in place, which is possible only with output_mode='field'.",
            "required": false
        },
        {
            "name": "strategy",
            "example": "polygon",
            "description": "Resolver: 'builtin' (offline place lookup), 'polygon' (point inside a zone), 'nearest' (closest site) or 'grid' (global grid cell). Defaults to 'builtin'.",
            "required": false
        },
        {
            "name": "lat_field",
            "example": "lat",
            "description": "Column holding latitude, as a number or a string. Defaults to 'lat'.",
            "required": false
        },
        {
            "name": "lon_field",
            "example": "lon",
            "description": "Column holding longitude, as a number or a string. Defaults to 'lon'.",
            "required": false
        },
        {
            "name": "coord_scale",
            "example": "1e7",
            "description": "Positive divisor applied to both coordinates after they are read, turning scaled integers into degrees: 557558000 with coord_scale=1e7 becomes 55.7558. Applies to every input mode. Defaults to 1.",
            "required": false
        },
        {
            "name": "point_field",
            "example": "position",
            "description": "Single column holding both coordinates, used instead of lat_field/lon_field.",
            "required": false
        },
        {
            "name": "point_format",
            "example": "wkt",
            "description": "How to read point_field: 'lat_lon', 'lon_lat', 'wkt' or 'geojson'. Defaults to 'lat_lon'.",
            "required": false
        },
        {
            "name": "geohash_field",
            "example": "gh",
            "description": "Column holding a geohash string, decoded to the cell center.",
            "required": false
        },
        {
            "name": "h3_field",
            "example": "h3",
            "description": "Column holding an H3 index, decoded to the cell center.",
            "required": false
        },
        {
            "name": "min_population",
            "example": "10000",
            "description": "strategy=builtin: consider only places at least this populous, 0 or more. Raising it coarsens the answer; set too high it returns a distant metropolis. Defaults to 0.",
            "required": false
        },
        {
            "name": "reference_file",
            "example": "/plugins/data/zones.geojson",
            "description": "strategy=polygon and strategy=nearest: the reference data, a .geojson, .json or .csv file under PLUGIN_DIR or an absolute path. Required for those strategies.",
            "required": false
        },
        {
            "name": "reference_encoding",
            "example": "cp1251",
            "description": "Python codec name for a CSV reference_file. Defaults to utf-8-sig, which also accepts plain UTF-8. GeoJSON is always UTF-8.",
            "required": false
        },
        {
            "name": "reference_lat_column",
            "example": "y_coord",
            "description": "Latitude column of a CSV reference_file. Detected from lat or latitude when omitted, ignoring case.",
            "required": false
        },
        {
            "name": "reference_lon_column",
            "example": "x_coord",
            "description": "Longitude column of a CSV reference_file. Detected from lon, lng, long or longitude when omitted, ignoring case.",
            "required": false
        },
        {
            "name": "reference_geometry_column",
            "example": "shape",
            "description": "Column of a CSV reference_file holding WKT geometry. Detected from geometry or wkt when omitted. Alternative to the latitude/longitude columns.",
            "required": false
        },
        {
            "name": "overlap_policy",
            "example": "smallest",
            "description": "strategy=polygon: which zone wins when a point is inside several. 'smallest' (most specific), 'largest' (most general), 'first' (file order) or 'priority'. Defaults to 'smallest'.",
            "required": false
        },
        {
            "name": "priority_attribute",
            "example": "rank",
            "description": "strategy=polygon: attribute ranked when overlap_policy='priority', and read only by that policy. Named like the entries of output_columns.",
            "required": false
        },
        {
            "name": "nearest_count",
            "example": "3",
            "description": "strategy=nearest: how many closest sites to describe, 1 or more. Above 1 every output column repeats per rank with a '_2', '_3' suffix. Defaults to 1.",
            "required": false
        },
        {
            "name": "max_radius_m",
            "example": "1000",
            "description": "Meters, above 0. Points farther than this from the resolved place are unresolved. Defaults to 1000 for strategy=nearest and to no limit for strategy=builtin, where distances are measured to a city center. There is no 'no limit' keyword: pass a value larger than half the Earth's circumference.",
            "required": false
        },
        {
            "name": "grid_type",
            "example": "h3",
            "description": "strategy=grid: 'h3' (hexagons), 'geohash' (rectangles) or 's2' (spherical quads). Defaults to 'h3'.",
            "required": false
        },
        {
            "name": "grid_precision",
            "example": "7",
            "description": "strategy=grid: cell size, ranged by grid_type: h3 0-15, geohash 1-12, s2 0-30. Defaults to 7 for h3, 6 for geohash, 9 for s2. Each finer step multiplies the distinct values the column can take.",
            "required": false
        },
        {
            "name": "unknown_value",
            "example": "UNKNOWN",
            "description": "Written when a coordinate cannot be resolved. Defaults to 'UNKNOWN'.",
            "required": false
        },
        {
            "name": "quantize_decimals",
            "example": "4",
            "description": "Decimal places a coordinate is rounded to before it becomes a cache key, 0 to 9. 4 is about 11 m. Defaults to 4.",
            "required": false
        },
        {
            "name": "cache_size",
            "example": "100000",
            "description": "How many distinct rounded coordinates are remembered before least-recently-used entries are evicted, 1 or more. Defaults to 100000.",
            "required": false
        },
        {
            "name": "target_database",
            "example": "analytics",
            "description": "Database for the target table. Defaults to the trigger's database.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "geo_enrichment_config_data_writes.toml",
            "description": "TOML config file under PLUGIN_DIR whose values override the trigger arguments.",
            "required": false
        }
    ],
    "http_args_config": [
        {
            "name": "source_measurements",
            "example": "gps fleet_pos",
            "description": "Space-separated source tables to enrich. Rows from other tables in the batch are ignored.",
            "required": true
        },
        {
            "name": "output_columns",
            "example": "country_code:geo_country city:geo_city",
            "description": "Space-separated 'attribute:column' pairs selecting which resolver attributes to write and under what column names. With a GeoJSON reference_file an attribute is a JSONPath into the feature properties, so nested values are reachable as owner.name; with a CSV one it is a column name.",
            "required": true
        },
        {
            "name": "output_mode",
            "example": "field",
            "description": "'field' writes geo attributes as fields, which merge into the source row. 'tag' writes them as tags and requires target_measurement. Defaults to 'field'.",
            "required": false
        },
        {
            "name": "target_measurement",
            "example": "gps_located",
            "description": "Destination table. Omit to enrich the source table in place, which is possible only with output_mode='field'.",
            "required": false
        },
        {
            "name": "strategy",
            "example": "polygon",
            "description": "Resolver: 'builtin' (offline place lookup), 'polygon' (point inside a zone), 'nearest' (closest site) or 'grid' (global grid cell). Defaults to 'builtin'.",
            "required": false
        },
        {
            "name": "lat_field",
            "example": "lat",
            "description": "Column holding latitude, as a number or a string. Defaults to 'lat'.",
            "required": false
        },
        {
            "name": "lon_field",
            "example": "lon",
            "description": "Column holding longitude, as a number or a string. Defaults to 'lon'.",
            "required": false
        },
        {
            "name": "coord_scale",
            "example": "1e7",
            "description": "Positive divisor applied to both coordinates after they are read, turning scaled integers into degrees: 557558000 with coord_scale=1e7 becomes 55.7558. Applies to every input mode. Defaults to 1.",
            "required": false
        },
        {
            "name": "point_field",
            "example": "position",
            "description": "Single column holding both coordinates, used instead of lat_field/lon_field.",
            "required": false
        },
        {
            "name": "point_format",
            "example": "wkt",
            "description": "How to read point_field: 'lat_lon', 'lon_lat', 'wkt' or 'geojson'. Defaults to 'lat_lon'.",
            "required": false
        },
        {
            "name": "geohash_field",
            "example": "gh",
            "description": "Column holding a geohash string, decoded to the cell center.",
            "required": false
        },
        {
            "name": "h3_field",
            "example": "h3",
            "description": "Column holding an H3 index, decoded to the cell center.",
            "required": false
        },
        {
            "name": "min_population",
            "example": "10000",
            "description": "strategy=builtin: consider only places at least this populous, 0 or more. Raising it coarsens the answer; set too high it returns a distant metropolis. Defaults to 0.",
            "required": false
        },
        {
            "name": "reference_file",
            "example": "/plugins/data/zones.geojson",
            "description": "strategy=polygon and strategy=nearest: the reference data, a .geojson, .json or .csv file under PLUGIN_DIR or an absolute path. Required for those strategies.",
            "required": false
        },
        {
            "name": "reference_encoding",
            "example": "cp1251",
            "description": "Python codec name for a CSV reference_file. Defaults to utf-8-sig, which also accepts plain UTF-8. GeoJSON is always UTF-8.",
            "required": false
        },
        {
            "name": "reference_lat_column",
            "example": "y_coord",
            "description": "Latitude column of a CSV reference_file. Detected from lat or latitude when omitted, ignoring case.",
            "required": false
        },
        {
            "name": "reference_lon_column",
            "example": "x_coord",
            "description": "Longitude column of a CSV reference_file. Detected from lon, lng, long or longitude when omitted, ignoring case.",
            "required": false
        },
        {
            "name": "reference_geometry_column",
            "example": "shape",
            "description": "Column of a CSV reference_file holding WKT geometry. Detected from geometry or wkt when omitted. Alternative to the latitude/longitude columns.",
            "required": false
        },
        {
            "name": "overlap_policy",
            "example": "smallest",
            "description": "strategy=polygon: which zone wins when a point is inside several. 'smallest' (most specific), 'largest' (most general), 'first' (file order) or 'priority'. Defaults to 'smallest'.",
            "required": false
        },
        {
            "name": "priority_attribute",
            "example": "rank",
            "description": "strategy=polygon: attribute ranked when overlap_policy='priority', and read only by that policy. Named like the entries of output_columns.",
            "required": false
        },
        {
            "name": "nearest_count",
            "example": "3",
            "description": "strategy=nearest: how many closest sites to describe, 1 or more. Above 1 every output column repeats per rank with a '_2', '_3' suffix. Defaults to 1.",
            "required": false
        },
        {
            "name": "max_radius_m",
            "example": "1000",
            "description": "Meters, above 0. Points farther than this from the resolved place are unresolved. Defaults to 1000 for strategy=nearest and to no limit for strategy=builtin, where distances are measured to a city center. There is no 'no limit' keyword: pass a value larger than half the Earth's circumference.",
            "required": false
        },
        {
            "name": "grid_type",
            "example": "h3",
            "description": "strategy=grid: 'h3' (hexagons), 'geohash' (rectangles) or 's2' (spherical quads). Defaults to 'h3'.",
            "required": false
        },
        {
            "name": "grid_precision",
            "example": "7",
            "description": "strategy=grid: cell size, ranged by grid_type: h3 0-15, geohash 1-12, s2 0-30. Defaults to 7 for h3, 6 for geohash, 9 for s2. Each finer step multiplies the distinct values the column can take.",
            "required": false
        },
        {
            "name": "unknown_value",
            "example": "UNKNOWN",
            "description": "Written when a coordinate cannot be resolved. Defaults to 'UNKNOWN'.",
            "required": false
        },
        {
            "name": "quantize_decimals",
            "example": "4",
            "description": "Decimal places a coordinate is rounded to before it becomes a cache key, 0 to 9. 4 is about 11 m. Defaults to 4.",
            "required": false
        },
        {
            "name": "cache_size",
            "example": "100000",
            "description": "How many distinct rounded coordinates are remembered before least-recently-used entries are evicted, 1 or more. Defaults to 100000.",
            "required": false
        },
        {
            "name": "target_database",
            "example": "analytics",
            "description": "Database for the target table. Defaults to the trigger's database.",
            "required": false
        },
        {
            "name": "start",
            "example": "2026-08-01T00:00:00Z",
            "description": "Default lower bound for every request, RFC 3339 inclusive; the request body overrides it. Given together with end.",
            "required": false
        },
        {
            "name": "end",
            "example": "2026-08-29T00:00:00Z",
            "description": "Default upper bound for every request, RFC 3339 exclusive; the request body overrides it. Given together with start.",
            "required": false
        },
        {
            "name": "batch_size",
            "example": "1000",
            "description": "Default rows read per page for every request, 1 or more; the request body overrides it. Defaults to 1000.",
            "required": false
        },
        {
            "name": "retry_unknown",
            "example": "true",
            "description": "Default for every request: re-resolve rows whose geo column equals unknown_value. The request body overrides it. Defaults to false.",
            "required": false
        },
        {
            "name": "force",
            "example": "true",
            "description": "Default for every request: re-resolve every row in range regardless of its current values. The request body overrides it. Defaults to false.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "geo_enrichment_config_data_writes.toml",
            "description": "TOML config file under PLUGIN_DIR whose values override the trigger arguments; the request body overrides both.",
            "required": false
        }
    ],
    "http_body_config": [
        {
            "name": "source_measurements",
            "example": "gps",
            "description": "Table to backfill. One call handles one table: if several are given, the first is used and the rest are ignored.",
            "required": true
        },
        {
            "name": "output_columns",
            "example": "country_code:geo_country city:geo_city",
            "description": "Space-separated 'attribute:column' pairs selecting which resolver attributes to write and under what column names. With a GeoJSON reference_file an attribute is a JSONPath into the feature properties, so nested values are reachable as owner.name; with a CSV one it is a column name.",
            "required": true
        },
        {
            "name": "output_mode",
            "example": "field",
            "description": "'field' writes geo attributes as fields, which merge into the source row. 'tag' writes them as tags and requires target_measurement. Defaults to 'field'.",
            "required": false
        },
        {
            "name": "target_measurement",
            "example": "gps_located",
            "description": "Destination table. Omit to enrich the source table in place, which is possible only with output_mode='field'.",
            "required": false
        },
        {
            "name": "strategy",
            "example": "polygon",
            "description": "Resolver: 'builtin' (offline place lookup), 'polygon' (point inside a zone), 'nearest' (closest site) or 'grid' (global grid cell). Defaults to 'builtin'.",
            "required": false
        },
        {
            "name": "lat_field",
            "example": "lat",
            "description": "Column holding latitude, as a number or a string. Defaults to 'lat'.",
            "required": false
        },
        {
            "name": "lon_field",
            "example": "lon",
            "description": "Column holding longitude, as a number or a string. Defaults to 'lon'.",
            "required": false
        },
        {
            "name": "coord_scale",
            "example": "1e7",
            "description": "Positive divisor applied to both coordinates after they are read, turning scaled integers into degrees: 557558000 with coord_scale=1e7 becomes 55.7558. Applies to every input mode. Defaults to 1.",
            "required": false
        },
        {
            "name": "point_field",
            "example": "position",
            "description": "Single column holding both coordinates, used instead of lat_field/lon_field.",
            "required": false
        },
        {
            "name": "point_format",
            "example": "wkt",
            "description": "How to read point_field: 'lat_lon', 'lon_lat', 'wkt' or 'geojson'. Defaults to 'lat_lon'.",
            "required": false
        },
        {
            "name": "geohash_field",
            "example": "gh",
            "description": "Column holding a geohash string, decoded to the cell center.",
            "required": false
        },
        {
            "name": "h3_field",
            "example": "h3",
            "description": "Column holding an H3 index, decoded to the cell center.",
            "required": false
        },
        {
            "name": "min_population",
            "example": "10000",
            "description": "strategy=builtin: consider only places at least this populous, 0 or more. Raising it coarsens the answer; set too high it returns a distant metropolis. Defaults to 0.",
            "required": false
        },
        {
            "name": "reference_file",
            "example": "/plugins/data/zones.geojson",
            "description": "strategy=polygon and strategy=nearest: the reference data, a .geojson, .json or .csv file under PLUGIN_DIR or an absolute path. Required for those strategies.",
            "required": false
        },
        {
            "name": "reference_encoding",
            "example": "cp1251",
            "description": "Python codec name for a CSV reference_file. Defaults to utf-8-sig, which also accepts plain UTF-8. GeoJSON is always UTF-8.",
            "required": false
        },
        {
            "name": "reference_lat_column",
            "example": "y_coord",
            "description": "Latitude column of a CSV reference_file. Detected from lat or latitude when omitted, ignoring case.",
            "required": false
        },
        {
            "name": "reference_lon_column",
            "example": "x_coord",
            "description": "Longitude column of a CSV reference_file. Detected from lon, lng, long or longitude when omitted, ignoring case.",
            "required": false
        },
        {
            "name": "reference_geometry_column",
            "example": "shape",
            "description": "Column of a CSV reference_file holding WKT geometry. Detected from geometry or wkt when omitted. Alternative to the latitude/longitude columns.",
            "required": false
        },
        {
            "name": "overlap_policy",
            "example": "smallest",
            "description": "strategy=polygon: which zone wins when a point is inside several. 'smallest' (most specific), 'largest' (most general), 'first' (file order) or 'priority'. Defaults to 'smallest'.",
            "required": false
        },
        {
            "name": "priority_attribute",
            "example": "rank",
            "description": "strategy=polygon: attribute ranked when overlap_policy='priority', and read only by that policy. Named like the entries of output_columns.",
            "required": false
        },
        {
            "name": "nearest_count",
            "example": "3",
            "description": "strategy=nearest: how many closest sites to describe, 1 or more. Above 1 every output column repeats per rank with a '_2', '_3' suffix. Defaults to 1.",
            "required": false
        },
        {
            "name": "max_radius_m",
            "example": "1000",
            "description": "Meters, above 0. Points farther than this from the resolved place are unresolved. Defaults to 1000 for strategy=nearest and to no limit for strategy=builtin, where distances are measured to a city center. There is no 'no limit' keyword: pass a value larger than half the Earth's circumference.",
            "required": false
        },
        {
            "name": "grid_type",
            "example": "h3",
            "description": "strategy=grid: 'h3' (hexagons), 'geohash' (rectangles) or 's2' (spherical quads). Defaults to 'h3'.",
            "required": false
        },
        {
            "name": "grid_precision",
            "example": "7",
            "description": "strategy=grid: cell size, ranged by grid_type: h3 0-15, geohash 1-12, s2 0-30. Defaults to 7 for h3, 6 for geohash, 9 for s2. Each finer step multiplies the distinct values the column can take.",
            "required": false
        },
        {
            "name": "unknown_value",
            "example": "UNKNOWN",
            "description": "Written when a coordinate cannot be resolved. Defaults to 'UNKNOWN'.",
            "required": false
        },
        {
            "name": "quantize_decimals",
            "example": "4",
            "description": "Decimal places a coordinate is rounded to before it becomes a cache key, 0 to 9. 4 is about 11 m. Defaults to 4.",
            "required": false
        },
        {
            "name": "cache_size",
            "example": "100000",
            "description": "How many distinct rounded coordinates are remembered before least-recently-used entries are evicted, 1 or more. Defaults to 100000.",
            "required": false
        },
        {
            "name": "target_database",
            "example": "analytics",
            "description": "Database for the target table. Defaults to the trigger's database.",
            "required": false
        },
        {
            "name": "start",
            "example": "2026-08-01T00:00:00Z",
            "description": "RFC 3339 lower bound, inclusive, nanosecond precision kept. Omit both start and end to backfill the whole table. A value set on the trigger or in its config file is the default.",
            "required": false
        },
        {
            "name": "end",
            "example": "2026-08-29T00:00:00Z",
            "description": "RFC 3339 upper bound, exclusive, nanosecond precision kept. A value set on the trigger or in its config file is the default.",
            "required": false
        },
        {
            "name": "batch_size",
            "example": "1000",
            "description": "Rows read per page, 1 or more; smaller values are raised to 1. Defaults to 1000. A value set on the trigger or in its config file is the default.",
            "required": false
        },
        {
            "name": "retry_unknown",
            "example": "true",
            "description": "Re-resolve rows whose geo column equals unknown_value instead of skipping them. A JSON boolean or true/false, yes/no, on/off, 1/0 as a string. Defaults to false. A value set on the trigger or in its config file is the default.",
            "required": false
        },
        {
            "name": "force",
            "example": "true",
            "description": "Re-resolve every row in range regardless of its current values, for applying a corrected reference file to history. A JSON boolean or true/false, yes/no, on/off, 1/0 as a string. Defaults to false. A value set on the trigger or in its config file is the default.",
            "required": false
        }
    ]
}
"""

import csv
import hashlib
import io
import json
import math
import uuid
from collections import OrderedDict
from datetime import datetime, timezone

from influxdata_plugin_utils.config import load_config, resolve_path
from influxdata_plugin_utils.introspection import get_schema
from influxdata_plugin_utils.parsing import (
    parse_bool,
    parse_delimited_list,
    parse_int,
    parse_key_value,
)
from influxdata_plugin_utils.sources import (
    KeySpec,
    parse_json_body,
    parse_toml,
    parse_trigger_args,
)
from influxdata_plugin_utils.validation import Validator
from influxdata_plugin_utils.write import build_line_typed, write_data

# information_schema data types.
TAG_DATA_TYPE: str = "Dictionary(Int32, Utf8)"
LINE_TYPES: dict = {
    "Int64": "int",
    "Int32": "int",
    "UInt64": "uint",
    "Float64": "float",
    "Float32": "float",
    "Boolean": "bool",
    "Utf8": "string",
}

STRATEGIES: tuple = ("builtin", "polygon", "nearest", "grid")
REFERENCE_STRATEGIES: tuple = ("polygon", "nearest")
RESOLVER_CONFIG_KEYS: tuple = (
    "strategy",
    "column_map",
    "min_population",
    "reference_file",
    "reference_encoding",
    "reference_lat_column",
    "reference_lon_column",
    "reference_geometry_column",
    "overlap_policy",
    "priority_attribute",
    "nearest_count",
    "max_radius_m",
    "grid_type",
    "grid_precision",
)
POINT_FORMATS: tuple = ("lat_lon", "lon_lat", "wkt", "geojson")
OVERLAP_POLICIES: tuple = ("smallest", "largest", "first", "priority")
GRID_TYPES: tuple = ("h3", "geohash", "s2")
DEFAULT_GRID_PRECISION: dict = {"h3": 7, "geohash": 6, "s2": 9}
GRID_PRECISION_RANGE: dict = {"h3": (0, 15), "geohash": (1, 12), "s2": (0, 30)}

BUILTIN_ATTRIBUTES: tuple = ("country_code", "country", "state", "city", "population")
DISTANCE_ATTRIBUTE: str = "distance_m"
# distance is a float column, so an unresolved row cannot carry unknown_value there
UNRESOLVED_DISTANCE: float = -1.0

EARTH_RADIUS_M: float = 6_371_008.8
DEFAULT_NEAREST_RADIUS_M: float = 1000.0
REFERENCE_TTL_SECONDS: int = 3600
MEMO_KEY: str = "geo:memo"
MEMO_MISS: object = object()
LAT_COLUMN_NAMES: tuple = ("lat", "latitude")
LON_COLUMN_NAMES: tuple = ("lon", "lng", "long", "longitude")
GEOMETRY_COLUMN_NAMES: tuple = ("geometry", "wkt")
REFERENCE_SUFFIXES: tuple = (".geojson", ".json", ".csv")
AREAL_GEOMETRIES: tuple = ("Polygon", "MultiPolygon")
CSV_DELIMITERS: str = ",;\t|"
# a ring wider than this in longitude crosses the antimeridian unsplit
WIDE_GEOMETRY_DEGREES: float = 180.0


def infer_line_type(value) -> str:
    """Line-protocol type for a field missing from the resolved schema."""
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    return "string"


def resolve_schema(
    influxdb3_local, measurement: str, task_id: str, refresh: bool = False
) -> dict:
    """Split the table's columns into tags and typed fields.

    A batch row is a flat dict, so the split has to come from the catalog.
    Cached without a TTL; schema_for() re-reads it when a column shows up.
    """
    columns = get_schema(
        influxdb3_local, measurement, ttl_seconds=None, refresh=refresh
    )
    if not columns:
        raise Exception(f"[{task_id}] Table '{measurement}' not found.")
    return {
        "tags": [name for name, dt in columns.items() if dt == TAG_DATA_TYPE],
        "fields": {
            name: LINE_TYPES.get(dt)
            for name, dt in columns.items()
            if dt != TAG_DATA_TYPE
        },
    }


def to_float(value):
    """Coerce a coordinate component to float; tags arrive as strings."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except ValueError:
        return None


def parse_point(raw, point_format: str):
    """Parse both coordinates out of a single column."""
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None

    if point_format == "geojson":
        try:
            coordinates = json.loads(text)["coordinates"]
            lon, lat = to_float(coordinates[0]), to_float(coordinates[1])
        except (ValueError, KeyError, TypeError, IndexError):
            return None
        return None if lat is None or lon is None else (lat, lon)

    if point_format == "wkt":
        upper = text.upper()
        if not upper.startswith("POINT"):
            return None
        inner = text[text.find("(") + 1 : text.rfind(")")]
        parts = inner.split()
        if len(parts) < 2:
            return None
        lon, lat = to_float(parts[0]), to_float(parts[1])
        return None if lat is None or lon is None else (lat, lon)

    parts = [part for part in text.replace(";", ",").split(",") if part.strip()]
    if len(parts) < 2:
        return None
    first, second = to_float(parts[0]), to_float(parts[1])
    if first is None or second is None:
        return None
    return (first, second) if point_format == "lat_lon" else (second, first)


def extract_coordinates(row: dict, cfg: dict):
    """Return (lat, lon) for a row, or None when it carries no usable point."""
    mode = cfg["coord_mode"]

    if mode == "geohash":
        raw = row.get(cfg["geohash_field"])
        if raw is None:
            return None
        pygeohash = require_package("pygeohash", "geohash_field")
        try:
            decoded = pygeohash.decode(str(raw))
        except Exception:
            return None
        return (to_float(decoded[0]), to_float(decoded[1]))

    if mode == "h3":
        raw = row.get(cfg["h3_field"])
        if raw is None:
            return None
        h3 = require_package("h3", "h3_field")
        try:
            lat, lon = h3.cell_to_latlng(str(raw))
        except Exception:
            return None
        return (lat, lon)

    if mode == "point":
        point = parse_point(row.get(cfg["point_field"]), cfg["point_format"])
        if point is None:
            return None
        lat, lon = point
    else:
        lat = to_float(row.get(cfg["lat_field"]))
        lon = to_float(row.get(cfg["lon_field"]))
        if lat is None or lon is None:
            return None

    scale = cfg["coord_scale"]
    if scale != 1:
        lat, lon = lat / scale, lon / scale
    return (lat, lon)


def coordinates_valid(lat: float, lon: float) -> bool:
    return (
        lat is not None
        and lon is not None
        and math.isfinite(lat)
        and math.isfinite(lon)
        and -90.0 <= lat <= 90.0
        and -180.0 <= lon <= 180.0
    )


def require_package(name: str, needed_by: str):
    """Import an optional dependency, naming what to install when it is absent."""
    try:
        return __import__(name)
    except ImportError as exc:
        raise Exception(
            f"'{needed_by}' needs the '{name}' package. Install it with "
            f"'influxdb3 install package {name}'."
        ) from exc


class BuiltinResolver:
    """Nearest populated place from the dataset bundled with reverse_geocode."""

    def __init__(self, min_population: int, max_radius_m: float):
        self._reverse_geocode = require_package("reverse_geocode", "strategy=builtin")
        self._min_population = min_population
        self._max_chord = chord_length(max_radius_m)
        self.warnings: list = []
        self.attributes = set(BUILTIN_ATTRIBUTES)
        self.attributes.add(DISTANCE_ATTRIBUTE)

    def resolve(self, lat: float, lon: float):
        place = self._reverse_geocode.get(
            (lat, lon), min_population=self._min_population
        )
        if not place:
            return None
        chord = math.dist(
            unit_vector(lat, lon),
            unit_vector(place["latitude"], place["longitude"]),
        )
        if chord > self._max_chord:
            return None
        attributes = {name: place.get(name) for name in BUILTIN_ATTRIBUTES}
        attributes[DISTANCE_ATTRIBUTE] = arc_length(chord)
        return attributes


def read_reference(cfg: dict, requested_attributes: dict) -> dict:
    """Read the reference file into geometry/attribute records.

    Returns 'records' with the geometry left in its source form, 'attributes'
    listing what the file can supply, and 'skipped' for unusable entries.
    """
    path = resolve_path(cfg["reference_file"])
    if str(path).lower().endswith(".csv"):
        return read_csv_reference(path, cfg)
    return read_geojson_reference(path, requested_attributes)


def read_geojson_reference(path, requested_attributes: dict) -> dict:
    try:
        with open(path, encoding="utf-8-sig") as handle:
            document = json.load(handle)
    except (OSError, ValueError) as exc:
        raise Exception(f"reference_file '{path}' cannot be read: {exc}") from exc

    if isinstance(document, dict) and document.get("type") == "Feature":
        features = [document]
    elif isinstance(document, dict):
        features = document.get("features") or []
    else:
        features = []

    geometries, properties, skipped = [], [], 0
    for feature in features:
        geometry = (feature or {}).get("geometry")
        if not geometry:
            skipped += 1
            continue
        geometries.append(geometry)
        properties.append(dict(feature.get("properties") or {}))

    if not geometries:
        raise Exception(f"reference_file '{path}' contains no usable features.")
    if requested_attributes and not any(properties):
        raise Exception(
            f"reference_file '{path}' features carry no properties to map."
        )

    bound = bind_paths(requested_attributes, properties)
    return {
        "records": [
            {"geometry": ("geojson", geometry), "attributes": attributes}
            for geometry, attributes in zip(geometries, bound)
        ],
        "attributes": set(requested_attributes),
        "skipped": skipped,
    }


def read_csv_reference(path, cfg: dict) -> dict:
    try:
        with open(path, encoding=cfg["reference_encoding"], newline="") as handle:
            text = handle.read()
    except LookupError as exc:
        raise Exception(f"'reference_encoding' is not a known encoding: {exc}") from exc
    except (OSError, UnicodeDecodeError) as exc:
        raise Exception(f"reference_file '{path}' cannot be read: {exc}") from exc

    delimiter = sniff_delimiter(text)
    rows = list(csv.DictReader(io.StringIO(text), delimiter=delimiter))
    if not rows:
        raise Exception(f"reference_file '{path}' contains no rows.")

    columns = [name for name in (rows[0].keys() or []) if name]
    geometry_column, lat_column, lon_column = reference_columns(
        columns, cfg, path, delimiter
    )
    # a comma inside a number can only be a decimal mark when it is not the delimiter
    decimal_comma = delimiter != ","

    records, skipped = [], 0
    for row in rows:
        geometry, consumed = csv_geometry(
            row, geometry_column, lat_column, lon_column, decimal_comma
        )
        if geometry is None:
            skipped += 1
            continue
        records.append({
            "geometry": geometry,
            "attributes": {
                name: value for name, value in row.items()
                if name and name not in consumed
            },
        })

    if not records:
        raise Exception(f"reference_file '{path}' has no rows with usable geometry.")

    return {
        "records": records,
        "attributes": {key for record in records for key in record["attributes"]},
        "skipped": skipped,
    }


def sniff_delimiter(text: str) -> str:
    try:
        return csv.Sniffer().sniff(text[:2048], delimiters=CSV_DELIMITERS).delimiter
    except csv.Error:
        return ","


def reference_columns(columns: list, cfg: dict, path, delimiter: str) -> tuple:
    """Locate the geometry source: a WKT column, or a latitude/longitude pair."""
    geometry_column = cfg["reference_geometry_column"]
    lat_column = cfg["reference_lat_column"]
    lon_column = cfg["reference_lon_column"]

    if geometry_column and (lat_column or lon_column):
        raise Exception(
            "'reference_geometry_column' and 'reference_lat_column'/"
            "'reference_lon_column' are alternatives; set only one of them."
        )

    if geometry_column:
        found = pick_column(columns, (geometry_column,))
        if found is None:
            raise Exception(
                f"reference_file '{path}' has no '{geometry_column}' column; "
                f"found: {', '.join(columns)}"
            )
        return found, None, None

    if lat_column or lon_column:
        return (
            None,
            site_column(columns, lat_column, LAT_COLUMN_NAMES, "latitude", path),
            site_column(columns, lon_column, LON_COLUMN_NAMES, "longitude", path),
        )

    found = pick_column(columns, GEOMETRY_COLUMN_NAMES)
    if found is not None:
        return found, None, None

    latitude = pick_column(columns, LAT_COLUMN_NAMES)
    longitude = pick_column(columns, LON_COLUMN_NAMES)
    if latitude and longitude:
        return None, latitude, longitude

    raise Exception(
        f"reference_file '{path}' carries no geometry: expected a "
        f"{' or '.join(GEOMETRY_COLUMN_NAMES)} column, or a latitude and a "
        f"longitude column. Read with delimiter '{delimiter}'; "
        f"found: {', '.join(columns)}"
    )


def csv_geometry(row: dict, geometry_column, lat_column, lon_column, decimal_comma):
    """Return the row's geometry and the columns it consumed."""
    if geometry_column:
        text = str(row.get(geometry_column) or "").strip()
        return (("wkt", text) if text else None), (geometry_column,)

    latitude = to_float(numeric_text(row.get(lat_column), decimal_comma))
    longitude = to_float(numeric_text(row.get(lon_column), decimal_comma))
    consumed = (lat_column, lon_column)
    if not coordinates_valid(latitude, longitude):
        return None, consumed
    return ("geojson", {"type": "Point", "coordinates": [longitude, latitude]}), consumed


def numeric_text(value, decimal_comma: bool) -> str:
    text = str(value or "").strip()
    return text.replace(",", ".") if decimal_comma else text


def geometry_point(geometry: tuple):
    """(lat, lon) for a point geometry, None for anything else."""
    kind, payload = geometry
    if kind == "wkt":
        return parse_point(payload, "wkt")
    if payload.get("type") != "Point":
        return None
    coordinates = payload.get("coordinates") or []
    if len(coordinates) < 2:
        return None
    latitude, longitude = to_float(coordinates[1]), to_float(coordinates[0])
    return None if latitude is None or longitude is None else (latitude, longitude)


def bind_paths(labeled_paths: dict, properties: list) -> list:
    """Read every requested JSONPath out of each feature's properties.

    Zone properties nest, so an attribute is a path rather than a key. Paths are
    resolved once here and the results stored flat, keeping resolve() a lookup.
    """
    require_package("jsonpath_ng", "a GeoJSON reference_file")
    from jsonpath_ng.ext import parse

    bound: list[dict] = [{} for _ in properties]
    for path, path_label in labeled_paths.items():
        try:
            expression = parse(path)
        except Exception as exc:
            raise Exception(f"{path_label} '{path}' is not a valid JSONPath: {exc}") from exc

        matched = False
        for index, props in enumerate(properties):
            found = [match.value for match in expression.find(props)]
            if not found:
                continue
            if len(found) > 1:
                raise Exception(
                    f"{path_label} '{path}' matches {len(found)} values in one "
                    f"feature; a column holds a single value."
                )
            value = found[0]
            if not isinstance(value, (str, int, float, bool, type(None))):
                raise Exception(
                    f"{path_label} '{path}' resolves to a "
                    f"{type(value).__name__}; only single values can be written. "
                    f"Point the path at a leaf, e.g. '{path}.<name>'."
                )
            bound[index][path] = value
            matched = True

        if not matched:
            raise Exception(f"{path_label} '{path}' matches no feature.")
    return bound


class PolygonResolver:
    """Point-in-polygon against the reference zones, indexed with an STRtree."""

    def __init__(self, reference: dict, overlap_policy: str, priority_attribute: str):
        require_package("shapely", "strategy=polygon")
        from shapely import STRtree
        from shapely.geometry import Point, shape
        from shapely.wkt import loads as load_wkt

        self._point_cls = Point
        self._overlap_policy = overlap_policy
        self._priority_attribute = priority_attribute
        self.warnings: list = []

        geometries, attributes = [], []
        unusable = reference["skipped"]
        for record in reference["records"]:
            kind, payload = record["geometry"]
            try:
                geometry = load_wkt(payload) if kind == "wkt" else shape(payload)
            except Exception:
                unusable += 1
                continue
            if geometry.is_empty or geometry.geom_type not in AREAL_GEOMETRIES:
                unusable += 1
                continue
            geometries.append(geometry)
            attributes.append(record["attributes"])

        if not geometries:
            raise Exception(
                f"strategy='polygon' needs {' or '.join(AREAL_GEOMETRIES)} "
                f"geometries; reference_file supplies none."
            )
        if unusable:
            self.warnings.append(
                f"reference_file: {unusable} entries skipped, they carry no usable "
                f"{' or '.join(AREAL_GEOMETRIES)} geometry."
            )

        wide = [
            index for index, geometry in enumerate(geometries)
            if crosses_antimeridian(geometry)
        ]
        if wide:
            self.warnings.append(
                f"reference_file: {len(wide)} geometries span more than "
                f"{WIDE_GEOMETRY_DEGREES:g} degrees of longitude. A zone crossing "
                f"the antimeridian must be split at it (RFC 7946), otherwise it "
                f"matches the opposite side of the globe."
            )

        self._attributes = attributes
        self._geometries = geometries
        self._tree = STRtree(geometries)
        self.attributes = set(reference["attributes"])

    def _choose(self, indexes) -> int:
        if len(indexes) == 1:
            return int(indexes[0])
        if self._overlap_policy == "first":
            return int(min(indexes))
        if self._overlap_policy == "priority":
            return int(
                max(indexes, key=lambda i: priority_key(self._attributes[i], self._priority_attribute))
            )
        if self._overlap_policy == "largest":
            return int(max(indexes, key=lambda i: self._geometries[i].area))
        return int(min(indexes, key=lambda i: self._geometries[i].area))

    def resolve(self, lat: float, lon: float):
        indexes = self._tree.query(self._point_cls(lon, lat), predicate="intersects")
        if len(indexes) == 0:
            return None
        return dict(self._attributes[self._choose(indexes)])


def priority_key(attributes: dict, attribute_name: str):
    """Sort key for overlap_policy='priority'; numbers rank above strings."""
    value = attributes.get(attribute_name)
    number = to_float(value)
    return (1, number) if number is not None else (0, 0.0)


def crosses_antimeridian(geometry) -> bool:
    """True when a single ring spans more longitude than a real zone can."""
    parts = getattr(geometry, "geoms", None) or [geometry]
    for part in parts:
        minimum_lon, _, maximum_lon, _ = part.bounds
        if maximum_lon - minimum_lon > WIDE_GEOMETRY_DEGREES:
            return True
    return False


def ranked_attribute(name: str, rank: int) -> str:
    """Name carrying the rank-th nearest site; rank 1 keeps the base name."""
    return name if rank == 1 else f"{name}_{rank}"


class NearestResolver:
    """Closest reference points, matched on the unit sphere so the nearest
    Euclidean neighbor is also the nearest great-circle one."""

    def __init__(self, reference: dict, max_radius_m: float, nearest_count: int = 1):
        require_package("scipy", "strategy=nearest")
        from scipy.spatial import cKDTree

        self.warnings: list = []

        points = []
        self._sites: list[dict] = []
        unusable = reference["skipped"]
        for record in reference["records"]:
            point = geometry_point(record["geometry"])
            if point is None or not coordinates_valid(*point):
                unusable += 1
                continue
            points.append(unit_vector(*point))
            self._sites.append(record["attributes"])

        if not points:
            raise Exception(
                "strategy='nearest' needs Point geometries; reference_file "
                "supplies none."
            )
        if unusable:
            self.warnings.append(
                f"reference_file: {unusable} entries skipped, they carry no usable "
                f"Point geometry."
            )

        self._tree = cKDTree(points)
        self._max_chord = chord_length(max_radius_m)
        self.site_count = len(points)
        self._neighbors = min(nearest_count, self.site_count)

        if nearest_count > self.site_count:
            self.warnings.append(
                f"reference_file supplies {self.site_count} point(s) but "
                f"nearest_count={nearest_count}; the ranks above "
                f"{self.site_count} stay unresolved."
            )

        base = set(reference["attributes"])
        base.add(DISTANCE_ATTRIBUTE)
        ranked = {
            ranked_attribute(name, rank)
            for name in base
            for rank in range(2, nearest_count + 1)
        }
        clashing = sorted(base & ranked)
        if clashing:
            raise Exception(
                f"reference_file supplies attributes {', '.join(clashing)} that "
                f"collide with the names generated for nearest_count={nearest_count}."
            )
        self.attributes = base | ranked

    def resolve(self, lat: float, lon: float):
        chords, indexes = self._tree.query(unit_vector(lat, lon), k=self._neighbors)
        if self._neighbors == 1:
            chords, indexes = [chords], [indexes]

        attributes: dict = {}
        for rank, (chord, index) in enumerate(zip(chords, indexes), start=1):
            # neighbors come back sorted, so the first one out of range ends it
            if not math.isfinite(chord) or chord > self._max_chord:
                break
            for key, value in self._sites[int(index)].items():
                attributes[ranked_attribute(key, rank)] = value
            attributes[ranked_attribute(DISTANCE_ATTRIBUTE, rank)] = arc_length(chord)

        return attributes or None


def pick_column(columns: list[str], candidates: tuple):
    lowered = {name.strip().lower(): name for name in columns}
    for candidate in candidates:
        key = str(candidate).strip().lower()
        if key in lowered:
            return lowered[key]
    return None


def site_column(columns: list[str], configured: str, defaults: tuple, kind: str, path):
    """Locate a coordinate column by name or by convention."""
    candidates = (configured,) if configured else defaults
    found = pick_column(columns, candidates)
    if found is None:
        raise Exception(
            f"reference_file '{path}' has no {kind} column (tried "
            f"{', '.join(candidates)}); found: {', '.join(columns)}"
        )
    return found


def unit_vector(lat: float, lon: float) -> tuple:
    lat_rad, lon_rad = math.radians(lat), math.radians(lon)
    cos_lat = math.cos(lat_rad)
    return (cos_lat * math.cos(lon_rad), cos_lat * math.sin(lon_rad), math.sin(lat_rad))


def chord_length(arc_meters: float) -> float:
    """Straight-line distance on the unit sphere for a given surface distance."""
    if arc_meters >= math.pi * EARTH_RADIUS_M:
        return 2.0
    return 2.0 * math.sin(arc_meters / (2.0 * EARTH_RADIUS_M))


def arc_length(chord: float) -> float:
    return 2.0 * EARTH_RADIUS_M * math.asin(min(1.0, chord / 2.0))


class GridResolver:
    """Identifier of the global-grid cell containing the point."""

    def __init__(self, grid_type: str, precision: int):
        self._grid_type = grid_type
        self._precision = precision
        self.warnings: list = []
        self.attributes = {"cell"}

        if grid_type == "h3":
            self._h3 = require_package("h3", "grid_type=h3")
        elif grid_type == "geohash":
            self._pygeohash = require_package("pygeohash", "grid_type=geohash")
        else:
            require_package("s2sphere", "grid_type=s2")
            import s2sphere

            self._s2 = s2sphere

    def resolve(self, lat: float, lon: float):
        if self._grid_type == "h3":
            return {"cell": self._h3.latlng_to_cell(lat, lon, self._precision)}
        if self._grid_type == "geohash":
            return {"cell": self._pygeohash.encode(lat, lon, precision=self._precision)}
        cell = self._s2.CellId.from_lat_lng(
            self._s2.LatLng.from_degrees(lat, lon)
        ).parent(self._precision)
        return {"cell": cell.to_token()}


def build_resolver(cfg: dict):
    strategy = cfg["strategy"]
    if strategy == "builtin":
        return BuiltinResolver(cfg["min_population"], cfg["max_radius_m"])
    if strategy == "grid":
        return GridResolver(cfg["grid_type"], cfg["grid_precision"])

    requested = {name: "output_columns attribute" for name in cfg["base_attributes"]}
    # only this policy reads the attribute, so only it may demand the file supply it
    if strategy == "polygon" and cfg["overlap_policy"] == "priority":
        requested.setdefault(cfg["priority_attribute"], "priority_attribute")
    reference = read_reference(cfg, requested)

    if strategy == "polygon":
        return PolygonResolver(
            reference, cfg["overlap_policy"], cfg["priority_attribute"]
        )
    return NearestResolver(reference, cfg["max_radius_m"], cfg["nearest_count"])


def resolver_cache_key(cfg: dict) -> str:
    """Key the resolver by every parameter build_resolver() reads."""
    signature = json.dumps(
        {name: cfg[name] for name in RESOLVER_CONFIG_KEYS}, sort_keys=True, default=str
    )
    return f"geo:resolver:{hashlib.sha1(signature.encode()).hexdigest()[:16]}"


def get_resolver(influxdb3_local, cfg: dict, rebuild: bool = False, task_id: str = ""):
    """Return the cached resolver, building the index on a miss.

    Module state does not survive an invocation; the cache holds live objects.
    """
    key = resolver_cache_key(cfg)
    if not rebuild:
        cached_resolver = influxdb3_local.cache.get(key)
        if cached_resolver is not None:
            return cached_resolver
    resolver = build_resolver(cfg)
    for message in resolver.warnings:
        influxdb3_local.warn(f"[{task_id}] {message}")
    influxdb3_local.cache.put(key, resolver, REFERENCE_TTL_SECONDS)
    # memoized results came from the previous index; they would outlive it
    influxdb3_local.cache.put(MEMO_KEY, OrderedDict(), REFERENCE_TTL_SECONDS)
    return resolver


def get_memo(influxdb3_local) -> OrderedDict:
    """Return the shared memo. Cached values are live objects, so mutating the
    returned mapping is enough — no put() is needed after a write."""
    memo = influxdb3_local.cache.get(MEMO_KEY)
    if memo is None:
        memo = OrderedDict()
        influxdb3_local.cache.put(MEMO_KEY, memo, REFERENCE_TTL_SECONDS)
    return memo


def memo_lookup(memo: OrderedDict, key: tuple):
    value = memo.get(key, MEMO_MISS)
    if value is MEMO_MISS:
        return None, False
    try:
        memo.move_to_end(key)
    except KeyError:
        pass
    return value, True


def memo_store(memo: OrderedDict, key: tuple, value, cache_size: int) -> None:
    memo[key] = value
    memo.move_to_end(key)
    while len(memo) > cache_size:
        memo.popitem(last=False)


# --- configuration ----------------------------------------------------------


def text(value) -> str:
    """A setting as trimmed text; TOML and JSON may deliver it as a number."""
    return str(value).strip()


def keyword(value) -> str:
    """A setting chosen from a fixed list: trimmed and lower-cased."""
    return text(value).lower()


def page_size(value) -> int:
    """Rows per backfill page; values below 1 are raised to 1."""
    return max(1, parse_int(value))


def output_column_map(value) -> dict:
    """attribute:column pairs from a string, a TOML table or a JSON object."""
    if isinstance(value, (list, tuple)):
        # a list of pairs would stringify into one mangled pair instead of failing
        raise ValueError(
            "cannot be a list. Write it as a table, "
            'output_columns = { attribute = "column" }, or as a string, '
            'output_columns = "attribute:column"'
        )
    try:
        return parse_key_value(value, kv_sep=":")
    except ValueError as exc:
        raise ValueError(
            f"must be space-separated 'attribute:column' pairs: {exc}"
        ) from exc


def reference_path(value) -> str:
    """A reference file name, checked for a readable format before it is opened."""
    path = text(value)
    if path and not path.lower().endswith(REFERENCE_SUFFIXES):
        raise ValueError(
            f"must be a {' or '.join(REFERENCE_SUFFIXES)} file, got '{path}'"
        )
    return path


VALIDATORS: list = [
    Validator("source_measurements", required=True, cast=parse_delimited_list, len_min=1),
    Validator("output_columns", required=True, cast=output_column_map, len_min=1),
    Validator("output_mode", default="field", cast=keyword, is_in=("field", "tag")),
    Validator("target_measurement", default="", cast=text),
    Validator("target_database", default="", cast=text),
    Validator("strategy", default="builtin", cast=keyword, is_in=STRATEGIES),
    Validator("lat_field", default="lat", cast=text),
    Validator("lon_field", default="lon", cast=text),
    Validator("coord_scale", default=1.0, cast=float, gt=0),
    Validator("point_field", default="", cast=text),
    Validator("point_format", default="lat_lon", cast=keyword, is_in=POINT_FORMATS),
    Validator("geohash_field", default="", cast=text),
    Validator("h3_field", default="", cast=text),
    Validator("min_population", default=0, cast=parse_int, gte=0),
    Validator("reference_file", default="", cast=reference_path),
    Validator("reference_encoding", default="utf-8-sig", cast=text),
    Validator("reference_lat_column", default="", cast=text),
    Validator("reference_lon_column", default="", cast=text),
    Validator("reference_geometry_column", default="", cast=text),
    Validator("overlap_policy", default="smallest", cast=keyword, is_in=OVERLAP_POLICIES),
    Validator("priority_attribute", default="", cast=text),
    Validator("nearest_count", default=1, cast=parse_int, gte=1),
    # unset: the strategy's own limit, 1000 m for nearest and none otherwise
    Validator("max_radius_m", cast=float, gt=0),
    Validator("grid_type", default="h3", cast=keyword, is_in=GRID_TYPES),
    # unset: the grid's own default; the range depends on the grid as well
    Validator("grid_precision", cast=parse_int, gte=0, lte=30),
    Validator("unknown_value", default="UNKNOWN", cast=str),
    Validator("quantize_decimals", default=4, cast=parse_int, gte=0, lte=9),
    Validator("cache_size", default=100_000, cast=parse_int, gte=1),
    # what one setting demands of another
    Validator("target_measurement", required=True, when=Validator("output_mode", eq="tag")),
    Validator("reference_file", required=True, when=Validator("strategy", is_in=REFERENCE_STRATEGIES)),
    Validator("priority_attribute", required=True, when=Validator("overlap_policy", eq="priority")),
    # backfill
    Validator("start", cast=text),
    Validator("end", cast=text),
    Validator("end", required=True, when=Validator("start", required=True)),
    Validator("start", required=True, when=Validator("end", required=True)),
    Validator("batch_size", default=1000, cast=page_size),
    Validator("retry_unknown", default=False, cast=parse_bool),
    Validator("force", default=False, cast=parse_bool),
]


def prepare_config(influxdb3_local, cfg, task_id: str):
    """Check the settings against each other and add what the pipeline derives.

    The refusals here are the ones whose message has to name a second setting.
    """
    if (
        cfg["output_mode"] == "tag"
        and cfg["target_measurement"] in cfg["source_measurements"]
    ):
        raise ValueError(
            "'target_measurement' must differ from the source tables when "
            "output_mode='tag'"
        )
    if cfg["nearest_count"] > 1 and cfg["strategy"] != "nearest":
        raise ValueError(
            f"'nearest_count' above 1 needs strategy='nearest', got '{cfg['strategy']}'"
        )

    cfg["coord_mode"] = resolve_coord_mode(cfg)
    cfg.setdefault("grid_precision", DEFAULT_GRID_PRECISION[cfg["grid_type"]])
    low, high = GRID_PRECISION_RANGE[cfg["grid_type"]]
    if not low <= cfg["grid_precision"] <= high:
        raise ValueError(
            f"grid_precision {cfg['grid_precision']} is out of range for "
            f"grid_type='{cfg['grid_type']}' ({low}-{high})"
        )
    cfg.setdefault(
        "max_radius_m",
        DEFAULT_NEAREST_RADIUS_M if cfg["strategy"] == "nearest" else math.inf,
    )

    # what the reference file must supply: ranks and distance are synthesized
    cfg["base_attributes"] = [
        name for name in cfg["output_columns"] if name != DISTANCE_ATTRIBUTE
    ]
    cfg["column_map"] = rank_columns(cfg["output_columns"], cfg["nearest_count"])
    cfg["output_column_names"] = list(cfg["column_map"].values())
    cfg["distance_attributes"] = {
        ranked_attribute(DISTANCE_ATTRIBUTE, rank)
        for rank in range(1, cfg["nearest_count"] + 1)
    }
    cfg["unresolved_markers"] = {
        column: value for column, (value, _) in output_values(cfg, None).items()
    }

    if cfg["output_mode"] == "tag" and cfg["nearest_count"] > 1:
        influxdb3_local.warn(
            f"[{task_id}] output_mode='tag' tags every one of the "
            f"{cfg['nearest_count']} ranks: the series key becomes the whole "
            f"combination, and two nearly equidistant sites swap ranks as the "
            f"point moves, opening a new series on every swap."
        )
    if (
        cfg["coord_mode"] == "h3"
        and cfg["strategy"] == "grid"
        and cfg["grid_type"] == "h3"
    ):
        influxdb3_local.warn(
            f"[{task_id}] Reading H3 cells and writing H3 cells; check that "
            f"grid_precision differs from the source resolution."
        )
    return cfg


def rank_columns(column_map: dict, nearest_count: int) -> dict:
    """Repeat every attribute:column pair once per rank, grouped rank by rank."""
    ranked: dict = {}
    claimed: dict = {}
    for rank in range(1, nearest_count + 1):
        for attribute, column in column_map.items():
            name = ranked_attribute(column, rank)
            source = ranked_attribute(attribute, rank)
            if name in claimed:
                raise ValueError(
                    f"'output_columns' maps both '{claimed[name]}' and "
                    f"'{source}' to column '{name}'"
                )
            claimed[name] = source
            ranked[source] = name
    return ranked


def resolve_coord_mode(cfg) -> str:
    """The single configured coordinate input mode."""
    inputs = (
        ("point", cfg["point_field"]),
        ("geohash", cfg["geohash_field"]),
        ("h3", cfg["h3_field"]),
    )
    configured = [mode for mode, field in inputs if field]
    if len(configured) > 1:
        raise ValueError(
            f"Configure exactly one coordinate input; got: {', '.join(configured)}"
        )
    return configured[0] if configured else "lat_lon"


def validate_attributes(cfg: dict, resolver, task_id: str) -> None:
    """Reject unknown attributes once, at load time, instead of per row."""
    unknown = [name for name in cfg["column_map"] if name not in resolver.attributes]
    if unknown:
        raise Exception(
            f"[{task_id}] strategy='{cfg['strategy']}' cannot produce "
            f"{', '.join(sorted(unknown))}. Available: "
            f"{', '.join(sorted(resolver.attributes))}."
        )


def already_enriched(row: dict, cfg: dict) -> bool:
    """True when the row carries a value in every output column.

    This stops the echo loop: an in-place write reproduces the source tags, so
    when coordinates are tags the echo is otherwise indistinguishable from a
    fresh point.
    """
    return all(row.get(column) is not None for column in cfg["output_column_names"])


def needs_reresolve(row: dict, cfg: dict, retry_unknown: bool) -> bool:
    if not retry_unknown:
        return False
    return any(
        row.get(column) == marker
        for column, marker in cfg["unresolved_markers"].items()
    )


def resolve_attributes(cfg: dict, resolver, memo: OrderedDict, lat: float, lon: float):
    """Resolve one point, reusing a memoized result for nearby coordinates."""
    decimals = cfg["quantize_decimals"]
    key = (round(lat, decimals), round(lon, decimals))
    value, hit = memo_lookup(memo, key)
    if hit:
        return value, True
    attributes = resolver.resolve(lat, lon)
    memo_store(memo, key, attributes, cfg["cache_size"])
    return attributes, False


def output_values(cfg: dict, attributes) -> dict:
    """Map resolver attributes onto output columns.

    Every column is always produced: a partial row would fail already_enriched()
    and be resolved again on the echo batch.
    """
    values: dict = {}
    for attribute, column in cfg["column_map"].items():
        raw = None if attributes is None else attributes.get(attribute)
        if attribute in cfg["distance_attributes"]:
            number = to_float(raw)
            values[column] = (
                UNRESOLVED_DISTANCE if number is None else float(number),
                "float",
            )
            continue
        values[column] = (scalar_text(raw, cfg["unknown_value"]), "string")
    return values


def scalar_text(value, unknown_value: str) -> str:
    """Render an attribute for a string column, keeping JSON booleans lowercase."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None or value == "":
        return unknown_value
    return str(value)


def build_enrichment_line(
    row: dict, table: str, values: dict, cfg: dict, schema: dict
):
    """Build the line carrying the resolved attributes.

    In place only the source tags are reproduced, so the write merges into the
    existing row; to a target table the whole row is copied.
    """
    in_place = not cfg["target_measurement"] and not cfg["target_database"]
    measurement = cfg["target_measurement"] or table
    tags = {name: row[name] for name in schema["tags"] if row.get(name) is not None}
    typed_fields: dict = {}

    if not in_place:
        for name, line_type in schema["fields"].items():
            value = row.get(name)
            if value is None:
                continue
            typed_fields[name] = (value, line_type or infer_line_type(value))

    for column, (value, line_type) in values.items():
        if cfg["output_mode"] == "tag" and line_type == "string":
            tags[column] = value
        else:
            typed_fields[column] = (value, line_type)

    if not typed_fields:
        raise Exception(
            f"Nothing to write for '{measurement}': output_mode='tag' produced no "
            f"fields. Map {DISTANCE_ATTRIBUTE} or use output_mode='field'."
        )

    return build_line_typed(
        LineBuilder,
        measurement,
        tags=tags,
        typed_fields=typed_fields,
        time_ns=int(row["time"]),
    )


def enrich_rows(
    influxdb3_local,
    rows: list,
    table: str,
    schema: dict,
    cfg: dict,
    resolver,
    stats: dict,
    task_id: str,
    retry_unknown: bool = False,
    force: bool = False,
) -> list:
    """Turn source rows into enrichment lines, counting what happened."""
    memo = get_memo(influxdb3_local)
    lines = []

    for row in rows:
        stats["rows"] += 1
        try:
            if not force and already_enriched(row, cfg):
                if not needs_reresolve(row, cfg, retry_unknown):
                    stats["skipped_enriched"] += 1
                    continue

            point = extract_coordinates(row, cfg)
            if point is None:
                stats["no_coordinates"] += 1
                continue

            lat, lon = point
            if not coordinates_valid(lat, lon):
                stats["invalid_coordinates"] += 1
                attributes = None
            else:
                attributes, hit = resolve_attributes(cfg, resolver, memo, lat, lon)
                stats["cache_hits" if hit else "cache_misses"] += 1
                if attributes is None:
                    stats["unresolved"] += 1
                else:
                    stats["resolved"] += 1

            lines.append(
                build_enrichment_line(
                    row, table, output_values(cfg, attributes), cfg, schema
                )
            )
        except Exception as exc:
            stats["errors"] += 1
            influxdb3_local.warn(f"[{task_id}] Row skipped: {exc}")

    return lines


def new_stats() -> dict:
    return {
        "rows": 0,
        "resolved": 0,
        "unresolved": 0,
        "no_coordinates": 0,
        "invalid_coordinates": 0,
        "skipped_enriched": 0,
        "cache_hits": 0,
        "cache_misses": 0,
        "errors": 0,
        "written": 0,
    }


def log_summary(influxdb3_local, stats: dict, task_id: str) -> None:
    influxdb3_local.info(
        f"[{task_id}] rows={stats['rows']} resolved={stats['resolved']} "
        f"unresolved={stats['unresolved']} no_coordinates={stats['no_coordinates']} "
        f"invalid={stats['invalid_coordinates']} "
        f"already_enriched={stats['skipped_enriched']} "
        f"cache_hits={stats['cache_hits']} cache_misses={stats['cache_misses']} "
        f"errors={stats['errors']} written={stats['written']}"
    )


def schema_for(
    influxdb3_local, table: str, rows: list, cache: dict, task_id: str
) -> dict:
    """Table schema, re-read when a row carries a column the cache does not know."""
    schema = cache.get(table)
    if schema is None:
        schema = cache[table] = resolve_schema(influxdb3_local, table, task_id)

    known = set(schema["tags"]) | set(schema["fields"]) | {"time"}
    if any(column not in known for row in rows for column in row):
        schema = cache[table] = resolve_schema(
            influxdb3_local, table, task_id, refresh=True
        )
    return schema


def process_writes(influxdb3_local, table_batches: list, args: dict | None = None):
    task_id: str = str(uuid.uuid4())[:8]
    args = args or {}

    try:
        cfg = load_config(
            parse_trigger_args(args),
            parse_toml(args.get("config_file_path")),
            validators=VALIDATORS,
        )
        prepare_config(influxdb3_local, cfg, task_id)
        resolver = get_resolver(influxdb3_local, cfg, task_id=task_id)
        validate_attributes(cfg, resolver, task_id)
    except Exception as exc:
        influxdb3_local.error(f"[{task_id}] Configuration error: {exc}")
        return

    stats = new_stats()
    schema_cache: dict = {}
    lines = []

    for batch in table_batches:
        table = batch["table_name"]
        if table not in cfg["source_measurements"]:
            continue
        try:
            schema = schema_for(
                influxdb3_local, table, batch["rows"], schema_cache, task_id
            )
        except Exception as exc:
            influxdb3_local.error(f"[{task_id}] {exc}")
            continue

        lines.extend(
            enrich_rows(
                influxdb3_local,
                batch["rows"],
                table,
                schema,
                cfg,
                resolver,
                stats,
                task_id,
            )
        )

    if not lines:
        if stats["rows"]:
            log_summary(influxdb3_local, stats, task_id)
        return

    try:
        write_data(
            influxdb3_local,
            lines,
            no_sync=True,
            retries=0,
            database=cfg["target_database"] or None,
        )
        stats["written"] = len(lines)
    except Exception as exc:
        influxdb3_local.error(f"[{task_id}] Write failed: {exc}")
        return

    log_summary(influxdb3_local, stats, task_id)


def process_request(
    influxdb3_local,
    query_parameters: dict | None,
    request_headers: dict | None,
    request_body,
    args: dict | None = None,
):
    task_id: str = str(uuid.uuid4())[:8]
    args = args or {}

    try:
        cfg = load_config(
            parse_trigger_args(args),
            parse_toml(args.get("config_file_path")),
            parse_json_body(
                request_body, KeySpec(denylist=["config_file_path"], unknown="reject")
            ),
            validators=VALIDATORS,
        )
        prepare_config(influxdb3_local, cfg, task_id)
        resolver = get_resolver(influxdb3_local, cfg, rebuild=True, task_id=task_id)
        validate_attributes(cfg, resolver, task_id)
    except Exception as exc:
        influxdb3_local.error(f"[{task_id}] Configuration error: {exc}")
        return {"error": str(exc)}, 400

    table = cfg["source_measurements"][0]
    if len(cfg["source_measurements"]) > 1:
        influxdb3_local.warn(
            f"[{task_id}] One call backfills one table; using '{table}' and "
            f"ignoring {', '.join(cfg['source_measurements'][1:])}."
        )

    try:
        resolve_schema(influxdb3_local, table, task_id)
    except Exception as exc:
        influxdb3_local.error(f"[{task_id}] {exc}")
        return {"error": str(exc)}, 400

    stats = new_stats()
    schema_cache: dict = {}
    try:
        cursor = cfg.get("start")
        while True:
            rows = read_page(
                influxdb3_local, table, cursor, cfg.get("end"), cfg["batch_size"]
            )
            if not rows:
                break
            rows, cursor = advance_cursor(
                influxdb3_local, table, rows, cfg["batch_size"]
            )
            schema = schema_for(influxdb3_local, table, rows, schema_cache, task_id)
            lines = enrich_rows(
                influxdb3_local,
                rows,
                table,
                schema,
                cfg,
                resolver,
                stats,
                task_id,
                retry_unknown=cfg["retry_unknown"],
                force=cfg["force"],
            )
            if lines:
                write_data(
                    influxdb3_local,
                    lines,
                    no_sync=True,
                    retries=3,
                    database=cfg["target_database"] or None,
                )
                stats["written"] += len(lines)
            if cursor is None:
                break
    except Exception as exc:
        influxdb3_local.error(f"[{task_id}] Backfill failed: {exc}")
        return {"error": str(exc), "stats": stats}, 500

    log_summary(influxdb3_local, stats, task_id)
    return {"status": "ok", "measurement": table, "stats": stats}, 200


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def ns_to_rfc3339(time_ns: int) -> str:
    """Nanosecond timestamp as an RFC3339 string, without losing precision."""
    seconds, nanos = divmod(int(time_ns), 1_000_000_000)
    stamp = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return f"{stamp.strftime('%Y-%m-%dT%H:%M:%S')}.{nanos:09d}Z"


def read_page(influxdb3_local, table: str, cursor, end, limit: int) -> list:
    """Read one page forward from ``cursor``, ordered by time."""
    clauses: list[str] = []
    params: dict = {}
    if cursor is not None:
        clauses.append("time >= $cursor")
        params["cursor"] = cursor
    if end is not None:
        clauses.append("time < $end")
        params["end"] = end
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    query = (
        f"SELECT * FROM {quote_identifier(table)}{where} ORDER BY time LIMIT {limit}"
    )
    if params:
        return influxdb3_local.query(query, params) or []
    return influxdb3_local.query(query) or []


def read_timestamp(influxdb3_local, table: str, time_ns: int) -> list:
    """Every row sharing one timestamp, for when they fill a whole page."""
    query = f"SELECT * FROM {quote_identifier(table)} WHERE time = $ts"
    return influxdb3_local.query(query, {"ts": ns_to_rfc3339(time_ns)}) or []


def advance_cursor(influxdb3_local, table: str, rows: list, limit: int) -> tuple:
    """Return the rows safe to process now and the next cursor (None when done).

    Paging by time rather than OFFSET: rows sharing a timestamp have no stable
    order across queries, and the plugin's own writes add chunks between pages,
    so an OFFSET could step over rows that were never enriched. The last
    timestamp of a full page is held back until its rows can be read together.
    """
    if len(rows) < limit:
        return rows, None
    last_ns = int(rows[-1]["time"])
    if int(rows[0]["time"]) == last_ns:
        return read_timestamp(influxdb3_local, table, last_ns), ns_to_rfc3339(
            last_ns + 1
        )
    return [row for row in rows if int(row["time"]) < last_ns], ns_to_rfc3339(last_ns)
