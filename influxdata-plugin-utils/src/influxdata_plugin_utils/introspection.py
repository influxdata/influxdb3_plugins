"""Schema introspection and minimal query helpers.

Each helper takes ``influxdb3_local`` explicitly and may optionally use the
TTL cache. Queries mirror the patterns already used across the plugins.
"""

from .cache import cached

__all__ = [
    "get_table_names",
    "get_tag_names",
    "get_field_names",
    "query_window",
]

_TAG_DATA_TYPE = "Dictionary(Int32, Utf8)"
_NUMERIC_TYPES = {"Int64", "UInt64", "Float64", "Int32", "Float32"}


def _quote_identifier(identifier: str) -> str:
    """Quote a SQL identifier, escaping embedded double quotes."""
    return '"' + identifier.replace('"', '""') + '"'


def get_table_names(
    influxdb3_local, *, use_cache: bool = True, ttl_seconds: int = 3600
) -> list[str]:
    """Return base table names via ``SHOW TABLES``."""

    def producer() -> list[str]:
        rows = influxdb3_local.query("SHOW TABLES")
        return [
            row["table_name"]
            for row in rows
            if row.get("table_type") == "BASE TABLE"
        ]

    if use_cache:
        return cached(influxdb3_local, "shared:tables", producer, ttl_seconds=ttl_seconds)
    return producer()


def get_tag_names(
    influxdb3_local, table: str, *, use_cache: bool = True, ttl_seconds: int = 3600
) -> list[str]:
    """Return tag column names of a table (``Dictionary(Int32, Utf8)`` columns)."""

    def producer() -> list[str]:
        query = (
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = $table AND data_type = $data_type"
        )
        rows = influxdb3_local.query(
            query, {"table": table, "data_type": _TAG_DATA_TYPE}
        )
        return [row["column_name"] for row in rows]

    if use_cache:
        return cached(
            influxdb3_local, f"shared:tags:{table}", producer, ttl_seconds=ttl_seconds
        )
    return producer()


def get_field_names(
    influxdb3_local,
    table: str,
    *,
    numeric_only: bool = False,
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
        rows = influxdb3_local.query(query, {"table": table})
        names: list[str] = []
        for row in rows:
            name = row["column_name"]
            data_type = row.get("data_type", "")
            if name == "time" or data_type == _TAG_DATA_TYPE:
                continue
            if numeric_only and data_type not in _NUMERIC_TYPES:
                continue
            names.append(name)
        return names

    if use_cache:
        key = f"shared:fields:{table}:{int(numeric_only)}"
        return cached(influxdb3_local, key, producer, ttl_seconds=ttl_seconds)
    return producer()


def query_window(
    influxdb3_local,
    table: str,
    *,
    start,
    end,
    columns: list[str] | None = None,
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
    rows = influxdb3_local.query(query, {"start": start, "end": end})
    return rows or []