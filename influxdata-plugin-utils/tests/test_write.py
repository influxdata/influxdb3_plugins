"""Tests for influxdata_plugin_utils.write."""

import pytest

from influxdata_plugin_utils.write import BatchLines, write_data


class FakeLineBuilder:
    def __init__(self, built):
        self.built = built

    def build(self):
        return self.built


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
