"""Tests for influxdata_plugin_utils.write."""

import pytest

from influxdata_plugin_utils.write import (
    BatchLines,
    build_line_typed,
    infer_type,
    split_row,
    write_data,
)


class FakeLineBuilder:
    def __init__(self, built):
        self.built = built

    def build(self):
        return self.built


class RecordingLineBuilder:
    """Records every builder call, so a test can see which typed method ran."""

    def __init__(self, measurement):
        self.measurement = measurement
        self.calls = []

    def __getattr__(self, name):
        if name in {
            "tag",
            "int64_field",
            "uint64_field",
            "float64_field",
            "bool_field",
            "string_field",
            "time_ns",
        }:

            def record(*args):
                self.calls.append((name, *args))
                return self

            return record
        raise AttributeError(name)


CPU_SCHEMA = {
    "tags": ["host", "region"],
    "fields": {"usage": "float", "count": "int", "seq": "uint", "ok": "bool", "odd": None},
}


class FakeInfluxDB:
    def __init__(self, *, failures_before_success=0):
        self.calls = []
        self.failures_before_success = failures_before_success

    def _record(self, method, *args, **kwargs):
        self.calls.append((method, args, kwargs))
        if self.failures_before_success > 0:
            self.failures_before_success -= 1
            raise RuntimeError("temporary write failure")

    def write(self, payload):
        self._record("write", payload)

    def write_to_db(self, database, payload):
        self._record("write_to_db", database, payload)

    def write_sync(self, payload, *, no_sync):
        self._record("write_sync", payload, no_sync=no_sync)

    def write_sync_to_db(self, database, payload, *, no_sync):
        self._record("write_sync_to_db", database, payload, no_sync=no_sync)


def _lines(*built):
    return [FakeLineBuilder(line) for line in built]


def test_write_data_batches_to_default_buffered_write():
    influxdb3_local = FakeInfluxDB()

    write_data(influxdb3_local, _lines("cpu value=1", "cpu value=2"))

    assert len(influxdb3_local.calls) == 1
    method, args, kwargs = influxdb3_local.calls[0]
    assert method == "write"
    assert kwargs == {}
    payload = args[0]
    assert isinstance(payload, BatchLines)
    assert payload.build() == "cpu value=1\ncpu value=2"


def test_write_data_batches_to_cross_database_buffered_write():
    influxdb3_local = FakeInfluxDB()

    write_data(
        influxdb3_local,
        _lines("cpu value=1", "cpu value=2"),
        database="target_db",
    )

    assert len(influxdb3_local.calls) == 1
    method, args, kwargs = influxdb3_local.calls[0]
    assert method == "write_to_db"
    assert args[0] == "target_db"
    assert kwargs == {}
    payload = args[1]
    assert isinstance(payload, BatchLines)
    assert payload.build() == "cpu value=1\ncpu value=2"


def test_write_data_preserves_empty_database_for_engine_validation():
    influxdb3_local = FakeInfluxDB()

    write_data(influxdb3_local, _lines("cpu value=1"), database="")

    method, args, kwargs = influxdb3_local.calls[0]
    assert method == "write_to_db"
    assert args[0] == ""
    assert kwargs == {}


def test_write_data_unbatched_default_sync_write():
    influxdb3_local = FakeInfluxDB()

    write_data(
        influxdb3_local,
        _lines("cpu value=1", "cpu value=2"),
        batch=False,
        no_sync=False,
    )

    assert [call[0] for call in influxdb3_local.calls] == [
        "write_sync",
        "write_sync",
    ]
    assert [call[1][0].build() for call in influxdb3_local.calls] == [
        "cpu value=1",
        "cpu value=2",
    ]
    assert [call[2] for call in influxdb3_local.calls] == [
        {"no_sync": False},
        {"no_sync": False},
    ]


def test_write_data_unbatched_cross_database_sync_write():
    influxdb3_local = FakeInfluxDB()

    write_data(
        influxdb3_local,
        _lines("cpu value=1", "cpu value=2"),
        batch=False,
        no_sync=True,
        database="target_db",
    )

    assert [call[0] for call in influxdb3_local.calls] == [
        "write_sync_to_db",
        "write_sync_to_db",
    ]
    assert [call[1][0] for call in influxdb3_local.calls] == [
        "target_db",
        "target_db",
    ]
    assert [call[1][1].build() for call in influxdb3_local.calls] == [
        "cpu value=1",
        "cpu value=2",
    ]
    assert [call[2] for call in influxdb3_local.calls] == [
        {"no_sync": True},
        {"no_sync": True},
    ]


def test_write_data_retries_selected_cross_database_sync_writer(monkeypatch):
    influxdb3_local = FakeInfluxDB(failures_before_success=1)
    sleeps = []
    monkeypatch.setattr("influxdata_plugin_utils.write.random.uniform", lambda a, b: 0)
    monkeypatch.setattr("influxdata_plugin_utils.write.time.sleep", sleeps.append)

    write_data(
        influxdb3_local,
        _lines("cpu value=1"),
        no_sync=True,
        database="target_db",
        base_delay=0.5,
    )

    assert [call[0] for call in influxdb3_local.calls] == [
        "write_sync_to_db",
        "write_sync_to_db",
    ]
    assert [call[1][0] for call in influxdb3_local.calls] == [
        "target_db",
        "target_db",
    ]
    assert sleeps == [0.5]


def test_write_data_raises_after_sync_retries_exhausted(monkeypatch):
    influxdb3_local = FakeInfluxDB(failures_before_success=2)
    monkeypatch.setattr("influxdata_plugin_utils.write.random.uniform", lambda a, b: 0)
    monkeypatch.setattr("influxdata_plugin_utils.write.time.sleep", lambda delay: None)

    with pytest.raises(RuntimeError, match="temporary write failure"):
        write_data(influxdb3_local, _lines("cpu value=1"), retries=1, no_sync=True)

    assert [call[0] for call in influxdb3_local.calls] == [
        "write_sync",
        "write_sync",
    ]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (True, "bool"),
        (False, "bool"),
        (0, "int"),
        (-(2**63), "int"),
        (1.5, "float"),
        (float("nan"), "float"),
        ("x", "string"),
        (b"x", "string"),
        ([1], "string"),
        (object(), "string"),
    ],
)
def test_infer_type_reads_bool_before_int_and_strings_the_rest(value, expected):
    assert infer_type(value) == expected


def test_split_row_places_every_key_by_the_schema():
    row = {
        "time": 1_700_000_000_000_000_000,
        "host": "a",
        "region": "eu",
        "usage": 1.5,
        "count": 3,
        "seq": 4,
        "ok": True,
    }

    tags, typed_fields, time_ns = split_row(row, CPU_SCHEMA)

    assert tags == {"host": "a", "region": "eu"}
    assert typed_fields == {
        "usage": (1.5, "float"),
        "count": (3, "int"),
        "seq": (4, "uint"),
        "ok": (True, "bool"),
    }
    assert time_ns == 1_700_000_000_000_000_000


def test_split_row_skips_none_and_infers_what_the_schema_cannot_type():
    row = {
        "time": 5,
        "host": None,  # a tag the write did not set
        "usage": None,  # a field the write did not set
        "count": 3,
        "odd": 2.5,  # known column, data type outside LINE_TYPES
        "extra": 7,  # column the schema has never seen
        "flag": False,
    }

    tags, typed_fields, time_ns = split_row(row, CPU_SCHEMA)

    assert tags == {}
    assert typed_fields == {
        "count": (3, "int"),
        "odd": (2.5, "float"),
        "extra": (7, "int"),
        "flag": (False, "bool"),
    }
    assert time_ns == 5


def test_split_row_without_a_time_column_returns_none_for_it():
    tags, typed_fields, time_ns = split_row({"host": "a", "usage": 1.0}, CPU_SCHEMA)

    assert tags == {"host": "a"}
    assert typed_fields == {"usage": (1.0, "float")}
    assert time_ns is None


def test_split_row_keeps_the_schema_type_over_the_value_type():
    """A uint column holds a Python int; the schema, not the value, decides."""
    row = {"time": 1, "seq": 4, "usage": 2}  # usage arrives as an int here

    tags, typed_fields, time_ns = split_row(row, CPU_SCHEMA)
    line = build_line_typed(
        RecordingLineBuilder, "cpu", tags=tags, typed_fields=typed_fields, time_ns=time_ns
    )

    assert line.calls == [
        ("uint64_field", "seq", 4),
        ("float64_field", "usage", 2.0),
        ("time_ns", 1),
    ]


def test_split_row_result_is_a_starting_point_the_caller_can_extend():
    """The enrichment case: source tags and fields, then the plugin's own."""
    row = {"time": 9, "host": "a", "usage": 1.5}

    tags, typed_fields, time_ns = split_row(row, CPU_SCHEMA)
    tags["country"] = "NL"
    typed_fields["enriched"] = (True, "bool")
    line = build_line_typed(
        RecordingLineBuilder, "cpu_geo", tags=tags, typed_fields=typed_fields, time_ns=time_ns
    )

    assert line.calls == [
        ("tag", "host", "a"),
        ("tag", "country", "NL"),
        ("float64_field", "usage", 1.5),
        ("bool_field", "enriched", True),
        ("time_ns", 9),
    ]
