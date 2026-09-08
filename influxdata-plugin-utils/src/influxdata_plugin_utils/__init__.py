"""Shared helpers for InfluxDB 3 plugins.

Modules:
    config         - dynaconf-backed config loading, layer merging, validation
    introspection  - schema introspection and minimal time-window queries
    parsing        - duration / timestamp / int / bool / list / key=value parsers
    request        - JSON body / header / query parsing for HTTP plugins
    cache          - TTL cache over influxdb3_local.cache
    write          - LineBuilder builders and resilient write_data
"""

__version__ = "0.4.0"

from . import cache, config, introspection, parsing, request, write
from .cache import cached
from .config import (
    Validator,
    load_plugin_config,
    merge_config_layers,
    resolve_path,
    resolve_plugin_dir,
)
from .introspection import (
    get_field_names,
    get_schema,
    get_table_names,
    get_tag_names,
    query_window,
)
from .parsing import (
    parse_bool,
    parse_delimited_list,
    parse_int,
    parse_key_value,
    parse_timedelta,
    parse_timestamp_ns,
)
from .request import parse_json_body, parse_query_parameters, parse_request_headers
from .write import (
    BatchLines,
    add_field_with_type,
    build_line,
    build_line_typed,
    write_data,
)

__all__ = [
    "cache",
    "config",
    "introspection",
    "parsing",
    "request",
    "write",
    "cached",
    "Validator",
    "load_plugin_config",
    "merge_config_layers",
    "resolve_path",
    "resolve_plugin_dir",
    "get_field_names",
    "get_schema",
    "get_table_names",
    "get_tag_names",
    "query_window",
    "parse_bool",
    "parse_delimited_list",
    "parse_int",
    "parse_key_value",
    "parse_timedelta",
    "parse_timestamp_ns",
    "parse_json_body",
    "parse_query_parameters",
    "parse_request_headers",
    "BatchLines",
    "add_field_with_type",
    "build_line",
    "build_line_typed",
    "write_data",
]
