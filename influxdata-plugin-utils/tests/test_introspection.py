"""Tests for influxdata_plugin_utils.introspection."""

from influxdata_plugin_utils.introspection import (
    get_field_names,
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
