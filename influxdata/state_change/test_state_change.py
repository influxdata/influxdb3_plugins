import json
from collections import deque
from datetime import datetime, timedelta, timezone

import pytest

import state_change_check_plugin as plugin

TOKEN = "apiv3_secret_token_value"
WEBHOOK = "https://example.com/hook"


class FakeCache:
    def __init__(self):
        self.store = {}
        self.ttls = {}

    def get(self, key, default=None, use_global=None):
        return self.store.get(key, default)

    def put(self, key, value, ttl=None, use_global=None):
        self.store[key] = value
        self.ttls[key] = ttl

    def delete(self, key, use_global=None):
        return self.store.pop(key, None) is not None


class FakeInfluxdb3Local:
    """Stub of the runtime client: logging, trigger-local cache and queries."""

    def __init__(self, tables=("home",), tags=("host",), rows=None):
        self.cache = FakeCache()
        self.logs = []
        self.tables = list(tables)
        self.tags = list(tags)
        self.rows = rows or []
        self.queries = []

    def info(self, message):
        self.logs.append(("info", message))

    def warn(self, message):
        self.logs.append(("warn", message))

    def error(self, message):
        self.logs.append(("error", message))

    def query(self, query, params=None):
        self.queries.append(query)
        if "SHOW TABLES" in query:
            return [{"table_name": t, "table_type": "BASE TABLE"} for t in self.tables]
        if "information_schema" in query:
            return [{"column_name": tag} for tag in self.tags]
        return self.rows

    def messages(self, level=None):
        return [m for lvl, m in self.logs if level is None or lvl == level]


class FakeResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {"results": "recorded"}


@pytest.fixture
def sent(monkeypatch):
    """Collect notification payloads instead of posting them."""
    posts = []

    def fake_post(url, headers=None, data=None, timeout=None):
        posts.append({"url": url, "headers": headers, "payload": json.loads(data)})
        return FakeResponse()

    monkeypatch.setattr(plugin.requests, "post", fake_post)
    monkeypatch.setattr(plugin.time, "sleep", lambda seconds: None)
    return posts


@pytest.fixture
def plugin_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    monkeypatch.delenv("INFLUXDB3_AUTH_TOKEN", raising=False)
    return tmp_path


WRITES_ARGS = {
    "measurement": "home",
    "field_thresholds": "temp:30:2",
    "senders": "http",
    "http_webhook_url": WEBHOOK,
    "influxdb3_auth_token": TOKEN,
}
SCHEDULED_ARGS = {
    "measurement": "home",
    "field_change_count": "value:2",
    "senders": "http",
    "http_webhook_url": WEBHOOK,
    "influxdb3_auth_token": TOKEN,
    "window": "1h",
}


def batch(rows, table="home"):
    return [{"table_name": table, "rows": rows}]


def count_key(field="temp", value=30, host="a"):
    return plugin.generate_cache_key(
        "home", field, value, "count", ["host"], {"host": host}
    )


def time_key(field="temp", value=30, host="a"):
    return plugin.generate_cache_key(
        "home", field, value, "time", ["host"], {"host": host}
    )


# --- parsing ----------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("10", 10),
        (10, 10),
        ("2h", timedelta(hours=2)),
        ("500ms", timedelta(milliseconds=500)),
        ("5min", timedelta(minutes=5)),
    ],
)
def test_threshold_param_accepts_counts_and_durations(raw, expected):
    assert plugin._parse_threshold_param(FakeInfluxdb3Local(), raw, "tid") == expected


@pytest.mark.parametrize("raw", ["0", "-5", "0s", "abc", True, "2x"])
def test_threshold_param_rejects_invalid_values(raw):
    influxdb3_local = FakeInfluxdb3Local()

    assert plugin._parse_threshold_param(influxdb3_local, raw, "tid") is None
    assert influxdb3_local.messages("warn")


def test_field_thresholds_from_string():
    thresholds = plugin.parse_field_thresholds(
        FakeInfluxdb3Local(),
        {"field_thresholds": "temp:'30.1':10@humidity:'true':2h"},
        "tid",
    )

    assert thresholds == [
        ("temp", 30.1, 10),
        ("humidity", True, timedelta(hours=2)),
    ]


def test_field_thresholds_from_toml_entries():
    thresholds = plugin.parse_field_thresholds(
        FakeInfluxdb3Local(),
        {"field_thresholds": [["temp", 30, 1], ["status", "error", "10s"]]},
        "tid",
    )

    assert thresholds == [
        ("temp", 30, 1),
        ("status", "error", timedelta(seconds=10)),
    ]


def test_field_thresholds_skips_malformed_segments():
    influxdb3_local = FakeInfluxdb3Local()

    thresholds = plugin.parse_field_thresholds(
        influxdb3_local, {"field_thresholds": "temp:30@humidity:5:1"}, "tid"
    )

    assert thresholds == [("humidity", 5, 1)]
    assert any("must have exactly 2 colons" in m for m in influxdb3_local.messages("warn"))


def test_field_thresholds_without_valid_entries_raises():
    with pytest.raises(Exception, match="No valid field thresholds"):
        plugin.parse_field_thresholds(
            FakeInfluxdb3Local(), {"field_thresholds": "temp:30"}, "tid"
        )


def test_field_thresholds_rejects_unsupported_type():
    with pytest.raises(Exception, match="must be a list of entries or a string"):
        plugin.parse_field_thresholds(
            FakeInfluxdb3Local(), {"field_thresholds": 42}, "tid"
        )


def test_field_change_count_from_string():
    assert plugin.parse_field_change_count(
        FakeInfluxdb3Local(), {"field_change_count": "temp:3.load:2"}, "tid"
    ) == {"temp": 3, "load": 2}


def test_field_change_count_from_toml_mapping():
    assert plugin.parse_field_change_count(
        FakeInfluxdb3Local(), {"field_change_count": {"temp": 3}}, "tid"
    ) == {"temp": 3}


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("disk.used:2", {"disk.used": 2}),
        ("temp:3.disk.used:2.a.b.c:10", {"temp": 3, "disk.used": 2, "a.b.c": 10}),
        ("sensor.1.value:5", {"sensor.1.value": 5}),
        ("temp : 3", {"temp": 3}),
    ],
)
def test_field_change_count_accepts_dotted_field_names(raw, expected):
    """A pair ends at the dot after the count, so field names may contain dots."""
    influxdb3_local = FakeInfluxdb3Local()

    assert (
        plugin.parse_field_change_count(
            influxdb3_local, {"field_change_count": raw}, "tid"
        )
        == expected
    )
    assert influxdb3_local.messages("warn") == []


@pytest.mark.parametrize(
    "raw, expected",
    [("temp:abc.load:2", {"load": 2}), ("temp:0.load:2", {"load": 2})],
)
def test_field_change_count_skips_invalid_pairs(raw, expected):
    influxdb3_local = FakeInfluxdb3Local()

    assert (
        plugin.parse_field_change_count(
            influxdb3_local, {"field_change_count": raw}, "tid"
        )
        == expected
    )
    assert influxdb3_local.messages("warn")


def test_field_change_count_without_valid_entries_raises():
    with pytest.raises(Exception, match="No valid entries"):
        plugin.parse_field_change_count(
            FakeInfluxdb3Local(), {"field_change_count": "temp:0"}, "tid"
        )


@pytest.mark.parametrize(
    "raw, expected",
    [("1h", timedelta(hours=1)), ("30s", timedelta(seconds=30))],
)
def test_parse_window(raw, expected):
    assert plugin.parse_window(raw) == expected


@pytest.mark.parametrize("raw", ["0s", "10m", "abc"])
def test_parse_window_rejects_invalid(raw):
    with pytest.raises(ValueError):
        plugin.parse_window(raw)


# --- senders ----------------------------------------------------------------


def test_senders_from_string_and_list():
    config = {"senders": "http.slack", "http_webhook_url": WEBHOOK, "slack_webhook_url": WEBHOOK}

    from_string = plugin.parse_senders(FakeInfluxdb3Local(), config, "tid")
    from_list = plugin.parse_senders(
        FakeInfluxdb3Local(), {**config, "senders": ["http", "slack"]}, "tid"
    )

    assert set(from_string) == {"http", "slack"} == set(from_list)


def test_senders_skips_unknown_channel():
    influxdb3_local = FakeInfluxdb3Local()

    senders = plugin.parse_senders(
        influxdb3_local,
        {"senders": "telegram.http", "http_webhook_url": WEBHOOK},
        "tid",
    )

    assert set(senders) == {"http"}
    assert any("Invalid sender type: telegram" in m for m in influxdb3_local.messages("warn"))


def test_senders_requires_webhook_url():
    with pytest.raises(Exception, match="No valid senders"):
        plugin.parse_senders(FakeInfluxdb3Local(), {"senders": "http"}, "tid")


def test_senders_rejects_non_http_scheme():
    influxdb3_local = FakeInfluxdb3Local()

    with pytest.raises(Exception, match="No valid senders"):
        plugin.parse_senders(
            influxdb3_local,
            {"senders": "http", "http_webhook_url": "ftp://example.com/hook"},
            "tid",
        )
    assert any("must start with" in m for m in influxdb3_local.messages("error"))


# --- configuration ----------------------------------------------------------


def test_load_config_applies_defaults(plugin_dir):
    config = plugin._load_config(
        FakeInfluxdb3Local(), dict(WRITES_ARGS), plugin._WRITES_VALIDATORS, "tid"
    )

    assert config["port_override"] == 8181
    assert config["notification_path"] == "notify"
    assert config["state_change_window"] == 1
    assert config["state_change_count"] == 1


def test_load_config_reads_toml(plugin_dir):
    (plugin_dir / "writes.toml").write_text(
        "measurement = 'home'\n"
        "senders = ['http']\n"
        "http_webhook_url = 'https://example.com/hook'\n"
        "influxdb3_auth_token = 'tok'\n"
        "field_thresholds = [['temp', 30, 1]]\n"
        "state_change_window = 4\n"
    )

    config = plugin._load_config(
        FakeInfluxdb3Local(),
        {"config_file_path": "writes.toml"},
        plugin._WRITES_VALIDATORS,
        "tid",
    )

    assert config["senders"] == ["http"]
    assert config["field_thresholds"] == [["temp", 30, 1]]
    assert config["state_change_window"] == 4


def test_load_config_rejects_non_toml_path(plugin_dir):
    influxdb3_local = FakeInfluxdb3Local()

    assert (
        plugin._load_config(
            influxdb3_local,
            {"config_file_path": "config.yaml"},
            plugin._WRITES_VALIDATORS,
            "tid",
        )
        is None
    )
    assert any("expected a .toml file" in m for m in influxdb3_local.messages("error"))


@pytest.mark.parametrize(
    "override",
    [{"measurement": None}, {"state_change_window": "-1"}, {"port_override": "70000"}],
)
def test_load_config_reports_validation_failures(plugin_dir, override):
    args = {**WRITES_ARGS, **override}
    args = {key: value for key, value in args.items() if value is not None}
    influxdb3_local = FakeInfluxdb3Local()

    assert (
        plugin._load_config(
            influxdb3_local, args, plugin._WRITES_VALIDATORS, "tid"
        )
        is None
    )
    assert any("Failed to load configuration" in m for m in influxdb3_local.messages("error"))


def test_load_config_accepts_zero_stability_settings(plugin_dir):
    """Existing triggers may pass 0; it behaves like the default of 1."""
    args = {**WRITES_ARGS, "state_change_window": "0", "state_change_count": "0"}

    config = plugin._load_config(
        FakeInfluxdb3Local(), args, plugin._WRITES_VALIDATORS, "tid"
    )

    assert config["state_change_window"] == 0
    assert config["state_change_count"] == 0


def test_writes_with_zero_window_keeps_alerting(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local()
    args = {**WRITES_ARGS, "field_thresholds": "temp:30:1", "state_change_window": "0"}

    plugin.process_writes(influxdb3_local, batch([{"host": "a", "temp": 30}]), args)

    assert len(sent) == 1


def test_load_config_uses_token_from_environment(plugin_dir, monkeypatch):
    monkeypatch.setenv("INFLUXDB3_AUTH_TOKEN", "env-token")
    args = {key: value for key, value in WRITES_ARGS.items() if key != "influxdb3_auth_token"}

    config = plugin._load_config(
        FakeInfluxdb3Local(), args, plugin._WRITES_VALIDATORS, "tid"
    )

    assert config["influxdb3_auth_token"] == "env-token"


# --- stability and counters -------------------------------------------------


@pytest.mark.parametrize(
    "values, allowed, stable",
    [
        ([], 1, True),
        ([1], 1, True),
        ([1, 1, 1], 1, True),
        ([1, 2, 1], 2, False),
        ([1, 2, 2], 2, True),
    ],
)
def test_check_state_changes(values, allowed, stable):
    assert plugin.check_state_changes(deque(values), allowed) is stable


@pytest.mark.parametrize("stored, expected", [("3", 3), ("", 0), (None, 0), ("x", 0)])
def test_read_counter_tolerates_unusable_values(stored, expected):
    influxdb3_local = FakeInfluxdb3Local()
    if stored is not None:
        influxdb3_local.cache.put("key", stored)

    assert plugin.read_counter(influxdb3_local, "key") == expected


# --- process_writes ---------------------------------------------------------


def test_writes_count_threshold_alerts_after_enough_matches(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local()

    plugin.process_writes(
        influxdb3_local, batch([{"host": "a", "temp": 30}]), dict(WRITES_ARGS)
    )
    assert sent == []
    assert influxdb3_local.cache.get(count_key()) == "1"

    plugin.process_writes(
        influxdb3_local, batch([{"host": "a", "temp": 30}]), dict(WRITES_ARGS)
    )
    assert len(sent) == 1
    assert "changed to 30" in sent[0]["payload"]["notification_text"]
    assert sent[0]["payload"]["senders_config"] == {"http": {"http_webhook_url": WEBHOOK}}
    assert influxdb3_local.cache.get(count_key()) == "0"


def test_writes_count_resets_when_condition_fails(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local()

    plugin.process_writes(
        influxdb3_local,
        batch([{"host": "a", "temp": 30}, {"host": "a", "temp": 25}]),
        dict(WRITES_ARGS),
    )

    assert sent == []
    assert influxdb3_local.cache.get(count_key()) == "0"


def test_writes_missing_field_does_not_break_the_batch(plugin_dir, sent):
    """Regression: an absent field used to store '' and crash the next int() read."""
    influxdb3_local = FakeInfluxdb3Local()

    plugin.process_writes(
        influxdb3_local,
        batch([{"host": "a", "hum": 5}, {"host": "a", "temp": 30}]),
        dict(WRITES_ARGS),
    )

    assert influxdb3_local.messages("error") == []
    assert influxdb3_local.cache.get(count_key()) == "1"


def test_writes_duration_threshold_alerts_once_elapsed(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local()
    args = {**WRITES_ARGS, "field_thresholds": "temp:30:1h"}
    started = datetime.now(timezone.utc) - timedelta(hours=2)
    influxdb3_local.cache.put(time_key(), started.isoformat())

    plugin.process_writes(influxdb3_local, batch([{"host": "a", "temp": 30}]), args)

    assert len(sent) == 1
    assert influxdb3_local.cache.get(time_key()) == ""


def test_writes_duration_threshold_waits_and_keeps_start(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local()
    args = {**WRITES_ARGS, "field_thresholds": "temp:30:1h"}
    started = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    influxdb3_local.cache.put(time_key(), started)

    plugin.process_writes(influxdb3_local, batch([{"host": "a", "temp": 30}]), args)

    assert sent == []
    assert influxdb3_local.cache.get(time_key()) == started
    assert any("Condition still holding" in m for m in influxdb3_local.messages("warn"))


def test_writes_unstable_data_suppresses_notification(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local()
    args = {
        **WRITES_ARGS,
        "field_thresholds": "temp:30:1",
        "state_change_window": "3",
        "state_change_count": "2",
    }
    values_key = plugin.generate_cache_key(
        "home", "temp", 30, "values", ["host"], {"host": "a"}
    )
    influxdb3_local.cache.put(values_key, deque([25, 30, 25], maxlen=3))

    plugin.process_writes(influxdb3_local, batch([{"host": "a", "temp": 30}]), args)

    assert sent == []
    assert any("unstable data state" in m for m in influxdb3_local.messages("warn"))


def test_writes_ignores_batches_of_other_tables(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local()

    plugin.process_writes(
        influxdb3_local, batch([{"host": "a", "temp": 30}], table="cpu"), dict(WRITES_ARGS)
    )

    assert sent == []
    assert not any("Starting writes process" in m for m in influxdb3_local.messages())
    assert not any("information_schema" in q for q in influxdb3_local.queries)


def test_writes_reports_unknown_measurement(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local(tables=("cpu",))

    plugin.process_writes(
        influxdb3_local, batch([{"host": "a", "temp": 30}]), dict(WRITES_ARGS)
    )

    assert any("not found in database" in m for m in influxdb3_local.messages("error"))


def test_writes_caches_configuration(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local()

    plugin.process_writes(
        influxdb3_local, batch([{"host": "a", "temp": 30}]), dict(WRITES_ARGS)
    )

    assert influxdb3_local.cache.get(plugin._WRITES_CONFIG_CACHE_KEY) is not None
    assert (
        influxdb3_local.cache.ttls[plugin._WRITES_CONFIG_CACHE_KEY]
        == plugin._WRITES_CONFIG_TTL_SECONDS
    )


def test_writes_never_logs_credentials(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local()

    plugin.process_writes(
        influxdb3_local, batch([{"host": "a", "temp": 30}]), dict(WRITES_ARGS)
    )

    logged = " ".join(influxdb3_local.messages())
    assert TOKEN not in logged
    assert WEBHOOK not in logged


# --- process_scheduled_call -------------------------------------------------


def test_scheduled_alerts_when_changes_reach_threshold(plugin_dir, sent):
    rows = [
        {"host": "a", "value": 1},
        {"host": "a", "value": 2},
        {"host": "a", "value": 1},
    ]
    influxdb3_local = FakeInfluxdb3Local(rows=rows)

    plugin.process_scheduled_call(
        influxdb3_local, datetime(2026, 8, 16, 12, 0, 0), dict(SCHEDULED_ARGS)
    )

    assert len(sent) == 1
    assert "changed 2 times" in sent[0]["payload"]["notification_text"]
    assert any(
        "Found 2 changes (threshold 2)" in m for m in influxdb3_local.messages("error")
    )


def test_scheduled_stays_quiet_below_threshold(plugin_dir, sent):
    rows = [{"host": "a", "value": 1}, {"host": "a", "value": 2}]
    influxdb3_local = FakeInfluxdb3Local(rows=rows)
    args = {**SCHEDULED_ARGS, "field_change_count": "value:5"}

    plugin.process_scheduled_call(
        influxdb3_local, datetime(2026, 8, 16, 12, 0, 0), args
    )

    assert sent == []


def test_scheduled_counts_changes_per_tag_combination(plugin_dir, sent):
    rows = [
        {"host": "a", "value": 1},
        {"host": "a", "value": 2},
        {"host": "a", "value": 3},
        {"host": "b", "value": 7},
    ]
    influxdb3_local = FakeInfluxdb3Local(rows=rows)

    plugin.process_scheduled_call(
        influxdb3_local, datetime(2026, 8, 16, 12, 0, 0), dict(SCHEDULED_ARGS)
    )

    assert len(sent) == 1
    assert "host=a" in sent[0]["payload"]["notification_text"]


def test_scheduled_handles_empty_window(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local(rows=[])

    plugin.process_scheduled_call(
        influxdb3_local, datetime(2026, 8, 16, 12, 0, 0), dict(SCHEDULED_ARGS)
    )

    assert sent == []
    assert any("No data found" in m for m in influxdb3_local.messages("info"))


def test_scheduled_queries_the_configured_window(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local(rows=[])

    plugin.process_scheduled_call(
        influxdb3_local, datetime(2026, 8, 16, 12, 0, 0), dict(SCHEDULED_ARGS)
    )

    data_query = influxdb3_local.queries[-1]
    assert '"home"' in data_query
    assert "time >= $start AND time < $end" in data_query
    assert any("from 2026-08-16 11:00:00+00:00" in m for m in influxdb3_local.messages("info"))


def test_scheduled_never_logs_credentials(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local(rows=[{"host": "a", "value": 1}])

    plugin.process_scheduled_call(
        influxdb3_local, datetime(2026, 8, 16, 12, 0, 0), dict(SCHEDULED_ARGS)
    )

    logged = " ".join(influxdb3_local.messages())
    assert TOKEN not in logged
    assert WEBHOOK not in logged
