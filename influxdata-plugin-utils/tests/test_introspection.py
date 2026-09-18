"""Tests for influxdata_plugin_utils.introspection."""

import pytest

from influxdata_plugin_utils.introspection import (
    LINE_TYPES,
    NUMERIC_LINE_TYPES,
    NUMERIC_TYPES,
    TAG_DATA_TYPE,
    get_field_names,
    get_line_schema,
    get_schema,
    get_table_names,
    get_tag_names,
    query_window,
)


class FakeCache:
    def __init__(self):
        self.values = {}
        self.ttls = {}

    def get(self, key):
        return self.values.get(key)

    def put(self, key, value, ttl_seconds):
        self.values[key] = value
        self.ttls[key] = ttl_seconds

    def delete(self, key):
        self.ttls.pop(key, None)
        return self.values.pop(key, None) is not None


class FakeInfluxDB:
    def __init__(self, responder):
        self.cache = FakeCache()
        self.calls = []
        self._responder = responder

    def query(self, query, args=None, *, database=None):
        self.calls.append({"query": query, "args": args, "database": database})
        return self._responder(query, args, database)


def test_get_table_names_passes_database_and_separates_cache():
    def responder(query, args, database):
        return [
            {"table_name": f"{database}_cpu", "table_type": "BASE TABLE"},
            {"table_name": f"{database}_view", "table_type": "VIEW"},
        ]

    influxdb3_local = FakeInfluxDB(responder)

    assert get_table_names(influxdb3_local, database="db_a") == ["db_a_cpu"]
    assert get_table_names(influxdb3_local, database="db_a") == ["db_a_cpu"]
    assert get_table_names(influxdb3_local, database="db_b") == ["db_b_cpu"]

    assert [call["database"] for call in influxdb3_local.calls] == ["db_a", "db_b"]
    assert "shared:tables:database:db_a" in influxdb3_local.cache.values
    assert "shared:tables:database:db_b" in influxdb3_local.cache.values


def test_default_database_keeps_existing_table_cache_key():
    influxdb3_local = FakeInfluxDB(
        lambda query, args, database: [
            {"table_name": "cpu", "table_type": "BASE TABLE"}
        ]
    )

    assert get_table_names(influxdb3_local) == ["cpu"]

    assert influxdb3_local.calls[0]["database"] is None
    assert "shared:tables" in influxdb3_local.cache.values


def test_get_tag_names_passes_database_and_separates_cache():
    def responder(query, args, database):
        assert args == {
            "table": "cpu",
            "data_type": "Dictionary(Int32, Utf8)",
        }
        return [{"column_name": f"{database}_host"}]

    influxdb3_local = FakeInfluxDB(responder)

    assert get_tag_names(influxdb3_local, "cpu", database="db_a") == ["db_a_host"]
    assert get_tag_names(influxdb3_local, "cpu", database="db_b") == ["db_b_host"]

    assert [call["database"] for call in influxdb3_local.calls] == ["db_a", "db_b"]
    assert "shared:tags:cpu:database:db_a" in influxdb3_local.cache.values
    assert "shared:tags:cpu:database:db_b" in influxdb3_local.cache.values


def test_get_field_names_passes_database_and_separates_cache():
    def responder(query, args, database):
        assert args == {"table": "cpu"}
        return [
            {"column_name": "time", "data_type": "Timestamp"},
            {"column_name": "host", "data_type": "Dictionary(Int32, Utf8)"},
            {"column_name": f"{database}_usage", "data_type": "Float64"},
            {"column_name": f"{database}_state", "data_type": "Utf8"},
        ]

    influxdb3_local = FakeInfluxDB(responder)

    assert get_field_names(influxdb3_local, "cpu", database="db_a") == [
        "db_a_usage",
        "db_a_state",
    ]
    assert get_field_names(
        influxdb3_local, "cpu", numeric_only=True, database="db_b"
    ) == ["db_b_usage"]

    assert [call["database"] for call in influxdb3_local.calls] == ["db_a", "db_b"]
    assert "shared:fields:cpu:0:database:db_a" in influxdb3_local.cache.values
    assert "shared:fields:cpu:1:database:db_b" in influxdb3_local.cache.values


def test_query_window_passes_database():
    def responder(query, args, database):
        assert '"cpu"' in query
        assert '"usage"' in query
        assert args == {
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-02T00:00:00Z",
        }
        assert database == "db_a"
        return [{"usage": 1.2}]

    influxdb3_local = FakeInfluxDB(responder)

    assert query_window(
        influxdb3_local,
        "cpu",
        start="2026-01-01T00:00:00Z",
        end="2026-01-02T00:00:00Z",
        columns=["usage"],
        database="db_a",
    ) == [{"usage": 1.2}]

    assert influxdb3_local.calls[0]["database"] == "db_a"


def test_get_schema_returns_column_types_and_leaves_time_out():
    def responder(query, args, database):
        return [
            {"column_name": "time", "data_type": "Timestamp(Nanosecond, None)"},
            {"column_name": "host", "data_type": "Dictionary(Int32, Utf8)"},
            {"column_name": "usage", "data_type": "Float64"},
        ]

    local = FakeInfluxDB(responder)
    assert get_schema(local, "cpu") == {
        "host": "Dictionary(Int32, Utf8)",
        "usage": "Float64",
    }
    assert "time" in get_schema(local, "cpu", exclude_time=False)


def test_a_schema_is_cached_until_a_caller_asks_for_a_re_read():
    """The recipe for "I just saw a column the cache does not know"."""
    columns = [[{"column_name": "usage", "data_type": "Float64"}]]

    def responder(query, args, database):
        return columns[-1]

    local = FakeInfluxDB(responder)
    assert get_schema(local, "cpu") == {"usage": "Float64"}

    columns.append(columns[-1] + [{"column_name": "temp", "data_type": "Float64"}])
    assert get_schema(local, "cpu") == {"usage": "Float64"}
    assert get_schema(local, "cpu", refresh=True) == {
        "usage": "Float64",
        "temp": "Float64",
    }


def test_a_missing_table_raises_and_is_asked_for_again_rather_than_remembered():
    columns = [[]]

    def responder(query, args, database):
        return columns[-1]

    local = FakeInfluxDB(responder)
    with pytest.raises(ValueError, match="Table 'cpu' not found"):
        get_schema(local, "cpu")
    assert local.cache.values == {}

    columns.append([{"column_name": "usage", "data_type": "Float64"}])
    assert get_schema(local, "cpu") == {"usage": "Float64"}


def test_a_table_dropped_between_reads_is_forgotten_on_refresh():
    columns = [[{"column_name": "usage", "data_type": "Float64"}]]
    local = FakeInfluxDB(lambda query, args, database: columns[-1])
    assert get_schema(local, "cpu") == {"usage": "Float64"}
    assert "shared:schema:cpu:1" in local.cache.values

    columns.append([])
    with pytest.raises(ValueError, match="Table 'cpu' not found"):
        get_schema(local, "cpu", refresh=True)
    assert "shared:schema:cpu:1" not in local.cache.values
    with pytest.raises(ValueError):
        get_schema(local, "cpu")  # no stale entry left to answer from


def test_get_field_names_raises_for_a_missing_table_but_not_for_no_numeric_fields():
    columns = [[]]
    local = FakeInfluxDB(lambda query, args, database: columns[-1])

    with pytest.raises(ValueError, match="Table 'cpu' not found"):
        get_field_names(local, "cpu")
    assert local.cache.values == {}

    columns.append(
        [
            {"column_name": "time", "data_type": "Timestamp(Nanosecond, None)"},
            {"column_name": "host", "data_type": "Dictionary(Int32, Utf8)"},
            {"column_name": "state", "data_type": "Utf8"},
        ]
    )
    assert get_field_names(local, "cpu") == ["state"]
    assert get_field_names(local, "cpu", numeric_only=True) == []


def test_get_tag_names_and_get_table_names_answer_empty_rather_than_raise():
    """A table without tags and a database without tables are ordinary."""
    local = FakeInfluxDB(lambda query, args, database: [])

    assert get_tag_names(local, "ghost") == []
    assert get_table_names(local) == []


def test_a_missing_table_names_its_database():
    local = FakeInfluxDB(lambda query, args, database: [])

    with pytest.raises(ValueError, match="Table 'cpu' not found in database 'db_a'"):
        get_schema(local, "cpu", database="db_a")


CPU_COLUMNS = [
    {"column_name": "time", "data_type": "Timestamp(Nanosecond, None)"},
    {"column_name": "host", "data_type": "Dictionary(Int32, Utf8)"},
    {"column_name": "region", "data_type": "Dictionary(Int32, Utf8)"},
    {"column_name": "usage", "data_type": "Float64"},
    {"column_name": "count", "data_type": "Int64"},
    {"column_name": "seq", "data_type": "UInt64"},
    {"column_name": "ok", "data_type": "Boolean"},
    {"column_name": "state", "data_type": "Utf8"},
    {"column_name": "odd", "data_type": "Decimal128(10, 2)"},
]


def test_get_line_schema_splits_tags_from_typed_fields_and_leaves_time_out():
    local = FakeInfluxDB(lambda query, args, database: CPU_COLUMNS)

    assert get_line_schema(local, "cpu") == {
        "tags": ["host", "region"],
        "fields": {
            "usage": "float",
            "count": "int",
            "seq": "uint",
            "ok": "bool",
            "state": "string",
            "odd": None,
        },
    }


def test_get_line_schema_of_an_unknown_table_raises_and_is_asked_again():
    columns = [[]]
    local = FakeInfluxDB(lambda query, args, database: columns[-1])

    with pytest.raises(ValueError, match="Table 'ghost' not found"):
        get_line_schema(local, "ghost")

    columns.append(CPU_COLUMNS[:2])
    assert get_line_schema(local, "ghost") == {"tags": ["host"], "fields": {}}
    assert len(local.calls) == 2


def test_get_line_schema_shares_the_get_schema_entry_and_its_refresh():
    columns = [CPU_COLUMNS[:4]]
    local = FakeInfluxDB(lambda query, args, database: columns[-1])

    assert get_line_schema(local, "cpu")["fields"] == {"usage": "float"}

    columns.append(CPU_COLUMNS[:5])
    # still the cached answer, through either helper
    assert get_line_schema(local, "cpu")["fields"] == {"usage": "float"}
    assert get_schema(local, "cpu") == {
        "host": TAG_DATA_TYPE,
        "region": TAG_DATA_TYPE,
        "usage": "Float64",
    }

    assert get_line_schema(local, "cpu", refresh=True)["fields"] == {
        "usage": "float",
        "count": "int",
    }
    assert "count" in get_schema(local, "cpu")
    assert len(local.calls) == 2
    assert local.cache.ttls["shared:schema:cpu:1"] == 3600


def test_get_line_schema_passes_database_and_can_skip_the_cache():
    local = FakeInfluxDB(lambda query, args, database: CPU_COLUMNS[:3])

    get_line_schema(local, "cpu", database="db_a", use_cache=False)
    get_line_schema(local, "cpu", database="db_a", use_cache=False)

    assert [call["database"] for call in local.calls] == ["db_a", "db_a"]
    assert local.cache.values == {}


def test_the_catalog_constants_agree_with_each_other():
    assert TAG_DATA_TYPE == "Dictionary(Int32, Utf8)"
    assert TAG_DATA_TYPE not in LINE_TYPES
    assert NUMERIC_TYPES == {"Int64", "UInt64", "Float64", "Int32", "Float32"}
    assert NUMERIC_TYPES <= set(LINE_TYPES)
    assert {LINE_TYPES[name] for name in NUMERIC_TYPES} == {"int", "uint", "float"}
    assert NUMERIC_LINE_TYPES == {"int", "uint", "float"}
    assert set(LINE_TYPES.values()) == {"int", "uint", "float", "bool", "string"}


def test_line_types_cannot_be_changed_by_a_caller():
    with pytest.raises(TypeError):
        LINE_TYPES["Int64"] = "float"
