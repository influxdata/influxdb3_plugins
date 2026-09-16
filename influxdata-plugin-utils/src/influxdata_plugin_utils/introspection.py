"""Schema introspection and minimal query helpers.

Each helper takes ``influxdb3_local`` explicitly and may optionally use the
TTL cache. Queries mirror the patterns already used across the plugins.
Helpers accept an optional ``database`` argument for engines that support
cross-database plugin queries.
"""

from .cache import cached

__all__ = [
    "tag_data_type",
    "numeric_types",
    "line_types",
    "numeric_line_types",
    "get_table_names",
    "get_tag_names",
    "get_field_names",
    "get_schema",
    "get_line_schema",
    "query_window",
]

tag_data_type = "Dictionary(Int32, Utf8)"
"""The ``information_schema`` data type of a tag column."""

numeric_types = frozenset({"Int64", "UInt64", "Float64", "Int32", "Float32"})
"""The ``information_schema`` data types of the columns a numeric aggregate accepts."""

line_types = {
    "Int64": "int",
    "Int32": "int",
    "UInt64": "uint",
    "Float64": "float",
    "Float32": "float",
    "Boolean": "bool",
    "Utf8": "string",
}
"""``information_schema`` data type to the :func:`write.add_field_with_type` type."""

numeric_line_types = frozenset(line_types[data_type] for data_type in numeric_types)
"""The line types of :data:`numeric_types`: ``int``, ``uint`` and ``float``."""


def _quote_identifier(identifier: str) -> str:
    """Quote a SQL identifier, escaping embedded double quotes."""
    return '"' + identifier.replace('"', '""') + '"'


def _query(
    influxdb3_local,
    query: str,
    args: dict | None = None,
    database: str | None = None,
):
    """Run a plugin query, preserving the old call shape when no database is set."""
    if database is None:
        if args is None:
            return influxdb3_local.query(query)
        return influxdb3_local.query(query, args)
    if args is None:
        return influxdb3_local.query(query, database=database)
    return influxdb3_local.query(query, args, database=database)


def _cache_key(base: str, database: str | None) -> str:
    """Return a cache key, keeping existing default-database keys unchanged."""
    if database is None:
        return base
    return f"{base}:database:{database}"


def get_table_names(
    influxdb3_local,
    *,
    database: str | None = None,
    use_cache: bool = True,
    ttl_seconds: int = 3600,
) -> list[str]:
    """Return base table names via ``SHOW TABLES``."""

    def producer() -> list[str]:
        rows = _query(influxdb3_local, "SHOW TABLES", database=database)
        return [
            row["table_name"]
            for row in rows
            if row.get("table_type") == "BASE TABLE"
        ]

    if use_cache:
        return cached(
            influxdb3_local,
            _cache_key("shared:tables", database),
            producer,
            ttl_seconds=ttl_seconds,
            cache_empty=False,
        )
    return producer()


def get_tag_names(
    influxdb3_local,
    table: str,
    *,
    database: str | None = None,
    use_cache: bool = True,
    ttl_seconds: int = 3600,
) -> list[str]:
    """Return tag column names of a table (``Dictionary(Int32, Utf8)`` columns)."""

    def producer() -> list[str]:
        query = (
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = $table AND data_type = $data_type"
        )
        rows = _query(
            influxdb3_local,
            query,
            {"table": table, "data_type": tag_data_type},
            database=database,
        )
        return [row["column_name"] for row in rows]

    if use_cache:
        return cached(
            influxdb3_local,
            _cache_key(f"shared:tags:{table}", database),
            producer,
            ttl_seconds=ttl_seconds,
            cache_empty=False,
        )
    return producer()


def get_field_names(
    influxdb3_local,
    table: str,
    *,
    numeric_only: bool = False,
    database: str | None = None,
    use_cache: bool = True,
    ttl_seconds: int = 3600,
) -> list[str]:
    """Return field column names of a table (excludes tags and ``time``).

    With ``numeric_only=True`` only integer/float columns are returned.
    """

    def producer() -> list[str]:
        query = (
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = $table"
        )
        rows = _query(influxdb3_local, query, {"table": table}, database=database)
        names: list[str] = []
        for row in rows:
            name = row["column_name"]
            data_type = row.get("data_type", "")
            if name == "time" or data_type == tag_data_type:
                continue
            if numeric_only and data_type not in numeric_types:
                continue
            names.append(name)
        return names

    if use_cache:
        key = _cache_key(f"shared:fields:{table}:{int(numeric_only)}", database)
        return cached(
            influxdb3_local, key, producer, ttl_seconds=ttl_seconds, cache_empty=False
        )
    return producer()


def get_schema(
    influxdb3_local,
    table: str,
    *,
    exclude_time: bool = True,
    database: str | None = None,
    use_cache: bool = True,
    ttl_seconds: int | None = 3600,
    refresh: bool = False,
) -> dict:
    """Return ``{column_name: data_type}`` for a table.

    With ``exclude_time=False`` the ``time`` column is included. ``refresh=True``
    re-reads the catalog and replaces the cached entry, for callers that noticed
    a column the cache does not know.
    """

    def producer() -> dict:
        query = (
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = $table"
        )
        rows = _query(influxdb3_local, query, {"table": table}, database=database)
        return {
            row["column_name"]: row.get("data_type", "")
            for row in rows
            if not (exclude_time and row["column_name"] == "time")
        }

    if use_cache:
        key = _cache_key(f"shared:schema:{table}:{int(exclude_time)}", database)
        return cached(
            influxdb3_local,
            key,
            producer,
            ttl_seconds=ttl_seconds,
            refresh=refresh,
            cache_empty=False,
        )
    return producer()


def get_line_schema(
    influxdb3_local,
    table: str,
    *,
    database: str | None = None,
    use_cache: bool = True,
    ttl_seconds: int | None = 3600,
    refresh: bool = False,
) -> dict:
    """Return ``{"tags": [name, ...], "fields": {name: line_type}}`` for a table.

    Splits :func:`get_schema` into the tag names and a map of field name to
    the :func:`write.add_field_with_type` type of that column, or ``None`` for
    a data type :data:`line_types` does not know. ``time`` is left out. An
    unknown table gives empty tags and fields; whether that is an error is the
    caller's to decide. ``refresh=True`` re-reads the catalog, for a caller that
    saw a column the cached schema does not know. The cache entry is the one
    :func:`get_schema` keeps, so a refresh through either is seen by both.
    """
    columns = get_schema(
        influxdb3_local,
        table,
        database=database,
        use_cache=use_cache,
        ttl_seconds=ttl_seconds,
        refresh=refresh,
    )
    return {
        "tags": [name for name, dt in columns.items() if dt == tag_data_type],
        "fields": {
            name: line_types.get(dt)
            for name, dt in columns.items()
            if dt != tag_data_type
        },
    }


def query_window(
    influxdb3_local,
    table: str,
    *,
    start,
    end,
    columns: list[str] | None = None,
    database: str | None = None,
) -> list[dict]:
    """Run a basic ``time``-window query and return rows (``[]`` if none).

    ``start``/``end`` are passed as query parameters; provide values the engine
    accepts for ``time`` comparisons (e.g. RFC3339 strings).
    """
    selected = (
        ", ".join(_quote_identifier(column) for column in columns) if columns else "*"
    )
    query = (
        f"SELECT {selected} FROM {_quote_identifier(table)} "
        "WHERE time >= $start AND time < $end ORDER BY time"
    )
    rows = _query(
        influxdb3_local,
        query,
        {"start": start, "end": end},
        database=database,
    )
    return rows or []
