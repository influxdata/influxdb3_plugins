"""Tests for influxdata_plugin_utils.cache."""

from influxdata_plugin_utils.cache import cached


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
    def __init__(self):
        self.cache = FakeCache()


class Producer:
    """Returns each value in turn, and counts how often it had to run."""

    def __init__(self, *values):
        self.values = values
        self.calls = 0

    def __call__(self):
        self.calls += 1
        return self.values[min(self.calls, len(self.values)) - 1]


def test_a_value_is_produced_once_and_then_served_from_the_cache():
    local = FakeInfluxDB()
    producer = Producer(["cpu"])

    assert cached(local, "tables", producer) == ["cpu"]
    assert cached(local, "tables", producer) == ["cpu"]
    assert producer.calls == 1
    assert local.cache.ttls["tables"] == 3600


def test_refresh_replaces_the_stored_value():
    local = FakeInfluxDB()
    producer = Producer(["cpu"], ["cpu", "mem"])

    assert cached(local, "tables", producer) == ["cpu"]
    assert cached(local, "tables", producer, refresh=True) == ["cpu", "mem"]
    assert cached(local, "tables", producer) == ["cpu", "mem"]
    assert producer.calls == 2


def test_an_empty_answer_can_be_retried_rather_than_remembered():
    """A catalog read too early means "not there yet", not "nothing"."""
    local = FakeInfluxDB()
    producer = Producer([], ["cpu"])

    assert cached(local, "tables", producer, cache_empty=False) == []
    assert "tables" not in local.cache.values
    assert cached(local, "tables", producer, cache_empty=False) == ["cpu"]


def test_a_refresh_that_comes_back_empty_drops_the_entry():
    """A table dropped between reads leaves neither its old value nor an empty one."""
    local = FakeInfluxDB()
    producer = Producer(["cpu"], [])

    cached(local, "tables", producer, cache_empty=False)
    assert cached(local, "tables", producer, refresh=True, cache_empty=False) == []
    assert "tables" not in local.cache.values
