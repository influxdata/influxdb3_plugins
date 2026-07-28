"""Unit tests for the opcua plugin: nsu= parsing, integer validation, and
browse-config normalization. Mock-based, no running OPC UA server.
"""

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))
import opcua
from opcua import OPCUAConfig, _browse_fingerprint, _split_nsu_node_id


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeCache:
    def __init__(self):
        self._d = {}
        self._ttls = {}

    def get(self, key, default=None, use_global=None):
        return self._d.get(key, default)

    def put(self, key, value, ttl=None, use_global=None):
        self._d[key] = value
        self._ttls[key] = ttl

    def delete(self, key, use_global=None):
        return self._d.pop(key, None) is not None


class FakeInfluxdb3Local:
    def __init__(self):
        self.cache = FakeCache()
        self.logs = []

    def info(self, m):
        self.logs.append(("info", m))

    def warn(self, m):
        self.logs.append(("warn", m))

    def error(self, m):
        self.logs.append(("error", m))


# ---------------------------------------------------------------------------
# _split_nsu_node_id
# ---------------------------------------------------------------------------


def test_split_nsu_node_id_valid():
    assert _split_nsu_node_id("nsu=urn:example;s=Devices") == ("urn:example", "s=Devices")


def test_split_nsu_node_id_percent_decodes_uri():
    # %3B is an escaped ';' inside the URI, decoded before namespace lookup.
    assert _split_nsu_node_id("nsu=urn:a%3Bb;s=X") == ("urn:a;b", "s=X")


@pytest.mark.parametrize(
    "bad",
    [
        "nsu=urn:example",      # missing ';identifier'
        "nsu=urn:example;",     # empty identifier
        "nsu=urn:example;  ",   # blank identifier
        "nsu=;s=Devices",       # empty URI
    ],
)
def test_split_nsu_node_id_rejects_malformed(bad):
    with pytest.raises(ValueError):
        _split_nsu_node_id(bad)


# ---------------------------------------------------------------------------
# _validate_positive_int
# ---------------------------------------------------------------------------


def test_validate_positive_int_accepts_int_and_numeric_string():
    assert OPCUAConfig._validate_positive_int(5, "p") == 5
    assert OPCUAConfig._validate_positive_int("300", "p") == 300


@pytest.mark.parametrize("bad", [0, -1, "0", "abc", 1.5, True, None])
def test_validate_positive_int_rejects_invalid(bad):
    with pytest.raises(ValueError):
        OPCUAConfig._validate_positive_int(bad, "p")


def test_validate_positive_int_enforces_max():
    assert OPCUAConfig._validate_positive_int(10, "p", max_value=10) == 10
    with pytest.raises(ValueError):
        OPCUAConfig._validate_positive_int(11, "p", max_value=10)


# ---------------------------------------------------------------------------
# _validate_toml_browse — normalization + nsu= root
# ---------------------------------------------------------------------------


def _browse(**overrides):
    browse = {"browse_root": "ns=2;s=Devices", "path_tags": []}
    browse.update(overrides)
    return browse


def test_validate_toml_browse_normalizes_ttl_default():
    browse = _browse()
    OPCUAConfig._validate_toml_browse(browse)
    assert browse["browse_cache_ttl"] == opcua._DEFAULT_BROWSE_CACHE_TTL


def test_validate_toml_browse_coerces_string_ttl():
    browse = _browse(browse_cache_ttl="120")
    OPCUAConfig._validate_toml_browse(browse)
    assert browse["browse_cache_ttl"] == 120


def test_validate_toml_browse_rejects_ttl_over_ceiling():
    with pytest.raises(ValueError):
        OPCUAConfig._validate_toml_browse(_browse(browse_cache_ttl=10**30))


def test_validate_toml_browse_rejects_malformed_nsu_root():
    with pytest.raises(ValueError):
        OPCUAConfig._validate_toml_browse(_browse(browse_root="nsu=urn:x"))


# ---------------------------------------------------------------------------
# _browse_fingerprint
# ---------------------------------------------------------------------------


def test_browse_fingerprint_stable_and_sensitive():
    base = {
        "server_url": "opc.tcp://h:4840",
        "browse": {"browse_root": "ns=2;s=A", "browse_depth": 2},
        "auth": {"password": "p1"},
    }
    # Credential/table changes must not invalidate the browse structure.
    creds_changed = {**base, "auth": {"password": "p2"}, "table_name": "other"}
    browse_changed = {**base, "browse": {"browse_root": "ns=2;s=B", "browse_depth": 2}}
    server_changed = {**base, "server_url": "opc.tcp://h2:4840"}

    assert _browse_fingerprint(base) == _browse_fingerprint(creds_changed)
    assert _browse_fingerprint(base) != _browse_fingerprint(browse_changed)
    assert _browse_fingerprint(base) != _browse_fingerprint(server_changed)


# ---------------------------------------------------------------------------
# Cache TTLs — config put runs before the (failed) connection
# ---------------------------------------------------------------------------


def test_config_and_browse_ttls_are_independent():
    fake = FakeInfluxdb3Local()
    args = {
        "server_url": "opc.tcp://127.0.0.1:1",  # refused → connect fails fast
        "table_name": "t",
        "browse_root": "ns=2;s=Devices",
        "path_tags": "",
        "config_cache_ttl": "1800",
        "browse_cache_ttl": "7200",
    }
    asyncio.run(opcua._async_scheduled_call(fake, "task", args))

    # Config TTL follows config_cache_ttl; browse fingerprint follows browse_cache_ttl.
    assert fake.cache._ttls.get("opcua_config") == 1800
    assert fake.cache._ttls.get("opcua_browse_fingerprint") == 7200


def test_browse_structure_survives_reload_unless_browse_config_changes():
    fake = FakeInfluxdb3Local()
    args = {
        "server_url": "opc.tcp://127.0.0.1:1",  # refused → connect fails fast
        "table_name": "t",
        "browse_root": "ns=2;s=Devices",
        "path_tags": "",
    }
    # First call records the browse fingerprint (before the failed connection).
    asyncio.run(opcua._async_scheduled_call(fake, "task", args))
    assert fake.cache._d.get("opcua_browse_fingerprint") is not None

    # Config-cache miss (expired) with the same browse config → structure kept.
    fake.cache.put("opcua_browse_structure", [("x", [])])
    fake.cache.delete("opcua_config")
    asyncio.run(opcua._async_scheduled_call(fake, "task", args))
    assert fake.cache._d.get("opcua_browse_structure") is not None

    # Browse root changed → structure invalidated on reload.
    fake.cache.delete("opcua_config")
    asyncio.run(opcua._async_scheduled_call(fake, "task", {**args, "browse_root": "ns=2;s=Other"}))
    assert fake.cache._d.get("opcua_browse_structure") is None