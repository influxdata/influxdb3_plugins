import json
from collections import deque
from datetime import datetime, timedelta, timezone

import pytest

import mad_check_plugin as plugin

TOKEN = "apiv3_secret_token_value"
WEBHOOK = "https://example.com/hook"

# Four calm values: the next written row completes a window of five
WARMUP = [20.0, 20.5, 21.0, 20.5]


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

    def __init__(self, tables=("home",), tags=("host",)):
        self.cache = FakeCache()
        self.logs = []
        self.tables = list(tables)
        self.tags = list(tags)
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
        return [{"column_name": tag} for tag in self.tags]

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
    return posts


@pytest.fixture
def plugin_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    monkeypatch.delenv("INFLUXDB3_AUTH_TOKEN", raising=False)
    return tmp_path


WRITES_ARGS = {
    "measurement": "home",
    "mad_thresholds": "temp:2:5:2",
    "senders": "http",
    "http_webhook_url": WEBHOOK,
    "influxdb3_auth_token": TOKEN,
}


def batch(rows, table="home"):
    return [{"table_name": table, "rows": rows}]


def rows(*values, host="a", field="temp"):
    return [{"host": host, field: value} for value in values]


def state_key(suffix, field="temp", k=2.0, window_count=5, threshold="2", host="a"):
    return plugin.generate_cache_key(
        "home",
        field,
        f"{k}-{window_count}-{threshold}",
        suffix,
        ["host"],
        {"host": host},
    )


def window_key(field="temp", window_count=5, host="a"):
    return plugin.generate_cache_key(
        "home", field, window_count, "deque", ["host"], {"host": host}
    )


def seed_window(influxdb3_local, values=WARMUP, field="temp", window_count=5, host="a"):
    """Pre-fill the MAD window so the next written row completes it."""
    influxdb3_local.cache.put(
        window_key(field, window_count, host), deque(values, maxlen=window_count)
    )


# --- parsing ----------------------------------------------------------------


def test_mad_thresholds_from_string():
    assert plugin.parse_mad_thresholds(
        FakeInfluxdb3Local(), {"mad_thresholds": "temp:2.5:20:5@load:3:10:2min"}, "tid"
    ) == [("temp", 2.5, 20, 5), ("load", 3.0, 10, timedelta(minutes=2))]


def test_mad_thresholds_from_toml_entries():
    assert plugin.parse_mad_thresholds(
        FakeInfluxdb3Local(),
        {"mad_thresholds": [["temp", 2.0, 5, 1], ["load", 3.5, 10, "500ms"]]},
        "tid",
    ) == [("temp", 2.0, 5, 1), ("load", 3.5, 10, timedelta(milliseconds=500))]


def test_mad_thresholds_accepts_quoted_k_and_dotted_field():
    assert plugin.parse_mad_thresholds(
        FakeInfluxdb3Local(), {"mad_thresholds": "disk.used:'2.5':20:5"}, "tid"
    ) == [("disk.used", 2.5, 20, 5)]


@pytest.mark.parametrize(
    "invalid",
    [
        "temp::20:5",  # regression: an empty k used to abort the whole flush
        "temp:abc:20:5",
        "temp:-2:20:5",
        "temp:2.5:0:5",
        "temp:2.5:1:5",
        "temp:2.5:-3:5",
        "temp:2.5:20:0",
        "temp:2.5:20:-4",
        "temp:2.5:20:2x",
        "temp:2.5:20",
        "temp:2.5:20000:5",  # window_count above _MAX_WINDOW_COUNT
    ],
)
def test_mad_thresholds_skips_invalid_segments(invalid):
    influxdb3_local = FakeInfluxdb3Local()

    thresholds = plugin.parse_mad_thresholds(
        influxdb3_local, {"mad_thresholds": f"{invalid}@load:3:10:2min"}, "tid"
    )

    assert thresholds == [("load", 3.0, 10, timedelta(minutes=2))]
    assert influxdb3_local.messages("warn")


def test_mad_thresholds_skips_entries_of_wrong_length():
    influxdb3_local = FakeInfluxdb3Local()

    thresholds = plugin.parse_mad_thresholds(
        influxdb3_local,
        {"mad_thresholds": [["temp", 2.0, 5], ["load", 3.0, 10, 2]]},
        "tid",
    )

    assert thresholds == [("load", 3.0, 10, 2)]
    assert any("expected [field, k" in m for m in influxdb3_local.messages("warn"))


def test_mad_thresholds_drops_duplicates():
    influxdb3_local = FakeInfluxdb3Local()

    thresholds = plugin.parse_mad_thresholds(
        influxdb3_local, {"mad_thresholds": "temp:2:5:4@temp:2:5:4"}, "tid"
    )

    assert thresholds == [("temp", 2.0, 5, 4)]
    assert any("duplicate threshold" in m for m in influxdb3_local.messages("warn"))


def test_mad_thresholds_without_valid_segments_raises():
    with pytest.raises(Exception, match="No valid MAD thresholds"):
        plugin.parse_mad_thresholds(
            FakeInfluxdb3Local(), {"mad_thresholds": "temp:2.5:20:0"}, "tid"
        )


def test_mad_thresholds_rejects_unsupported_type():
    with pytest.raises(Exception, match="must be a list of entries or a string"):
        plugin.parse_mad_thresholds(FakeInfluxdb3Local(), {"mad_thresholds": 42}, "tid")


@pytest.mark.parametrize(
    "senders, expected",
    [
        ("http", {"http": {"http_webhook_url": WEBHOOK}}),
        (["http"], {"http": {"http_webhook_url": WEBHOOK}}),
    ],
)
def test_parse_senders_accepts_string_and_list(senders, expected):
    config = {"senders": senders, "http_webhook_url": WEBHOOK}

    assert plugin.parse_senders(FakeInfluxdb3Local(), config, "tid") == expected


@pytest.mark.parametrize(
    "config",
    [
        {"senders": "telegram"},
        {"senders": "http"},
        {"senders": "http", "http_webhook_url": "ftp://example.com"},
    ],
)
def test_parse_senders_rejects_unusable_channels(config):
    with pytest.raises(Exception, match="No valid senders configured"):
        plugin.parse_senders(FakeInfluxdb3Local(), config, "tid")


# --- configuration ----------------------------------------------------------


def test_load_config_applies_defaults(plugin_dir):
    config = plugin._load_config(
        FakeInfluxdb3Local(), dict(WRITES_ARGS), plugin._WRITES_VALIDATORS, "tid"
    )

    assert config["port_override"] == 8181
    assert config["notification_path"] == "notify"
    assert config["state_change_count"] == 0


def test_load_config_reads_toml(plugin_dir):
    (plugin_dir / "writes.toml").write_text(
        "measurement = 'home'\n"
        "senders = ['http']\n"
        "http_webhook_url = 'https://example.com/hook'\n"
        "influxdb3_auth_token = 'tok'\n"
        "mad_thresholds = [['temp', 2.0, 5, '2min']]\n"
        "state_change_count = 3\n"
    )

    config = plugin._load_config(
        FakeInfluxdb3Local(),
        {"config_file_path": "writes.toml"},
        plugin._WRITES_VALIDATORS,
        "tid",
    )

    assert config["senders"] == ["http"]
    assert config["mad_thresholds"] == [["temp", 2.0, 5, "2min"]]
    assert config["state_change_count"] == 3


def test_load_config_rejects_non_toml_path(plugin_dir):
    influxdb3_local = FakeInfluxdb3Local()

    config = plugin._load_config(
        influxdb3_local,
        {"config_file_path": "writes.yaml"},
        plugin._WRITES_VALIDATORS,
        "tid",
    )

    assert config is None
    assert any("expected a .toml file" in m for m in influxdb3_local.messages("error"))


@pytest.mark.parametrize(
    "override",
    [
        {"measurement": None},
        {"port_override": "0"},
        {"port_override": "99999"},
        {"state_change_count": "-1"},
    ],
)
def test_load_config_reports_validation_failures(plugin_dir, override):
    influxdb3_local = FakeInfluxdb3Local()
    args = {**WRITES_ARGS, **override}
    args = {key: value for key, value in args.items() if value is not None}

    config = plugin._load_config(
        influxdb3_local, args, plugin._WRITES_VALIDATORS, "tid"
    )

    assert config is None
    assert any(
        "Failed to load configuration" in m for m in influxdb3_local.messages("error")
    )


def test_load_config_uses_token_from_environment(plugin_dir, monkeypatch):
    monkeypatch.setenv("INFLUXDB3_AUTH_TOKEN", "env-token")
    args = {
        key: value
        for key, value in WRITES_ARGS.items()
        if key != "influxdb3_auth_token"
    }

    config = plugin._load_config(
        FakeInfluxdb3Local(), args, plugin._WRITES_VALIDATORS, "tid"
    )

    assert config["influxdb3_auth_token"] == "env-token"


# --- flip suppression -------------------------------------------------------


@pytest.mark.parametrize(
    "flags, allowed, can_send",
    [
        ([False, True, True, True], 2, True),  # one sustained anomaly
        ([False, True, False, True], 2, False),  # flapping
        ([False, True, False, True], 0, True),  # suppression disabled
        ([True], 2, True),  # not enough history
    ],
)
def test_check_state_changes(flags, allowed, can_send):
    assert plugin.check_state_changes(deque(flags), allowed) is can_send


def test_inert_suppression_warns_for_narrow_count_windows_only():
    influxdb3_local = FakeInfluxdb3Local()
    thresholds = [
        ("temp", 2.0, 5, 5),  # no room for a transition
        ("load", 2.0, 10, 2),  # eight transitions fit
        ("rate", 2.0, 5, timedelta(minutes=2)),  # durations are not limited
    ]

    plugin.warn_on_inert_suppression(influxdb3_local, thresholds, 2, "tid")

    warns = influxdb3_local.messages("warn")
    assert len(warns) == 1
    assert "'temp'" in warns[0] and "window_count to 7" in warns[0]


@pytest.mark.parametrize("stored, expected", [(None, 0), ("", 0), ("x", 0), ("4", 4)])
def test_read_counter_tolerates_unusable_values(stored, expected):
    influxdb3_local = FakeInfluxdb3Local()
    influxdb3_local.cache.put("key", stored)

    assert plugin.read_counter(influxdb3_local, "key") == expected


# --- process_writes ---------------------------------------------------------


def test_writes_waits_until_the_window_is_full(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local()

    plugin.process_writes(influxdb3_local, batch(rows(20.0, 40.0)), dict(WRITES_ARGS))

    assert sent == []
    assert any("Waiting for 5 points" in m for m in influxdb3_local.messages("info"))


def test_writes_count_threshold_alerts_after_consecutive_outliers(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local()
    seed_window(influxdb3_local)

    plugin.process_writes(influxdb3_local, batch(rows(40.0)), dict(WRITES_ARGS))
    assert sent == []
    assert influxdb3_local.cache.get(state_key("count-count")) == "1"

    plugin.process_writes(influxdb3_local, batch(rows(41.0)), dict(WRITES_ARGS))
    assert len(sent) == 1
    assert "outlier for 2 consecutive points" in sent[0]["payload"]["notification_text"]
    assert sent[0]["payload"]["senders_config"] == {
        "http": {"http_webhook_url": WEBHOOK}
    }
    assert influxdb3_local.cache.get(state_key("count-count")) == "0"


def test_writes_count_resets_when_value_returns_to_normal(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local()
    seed_window(influxdb3_local)

    plugin.process_writes(influxdb3_local, batch(rows(40.0, 20.5)), dict(WRITES_ARGS))

    assert sent == []
    assert influxdb3_local.cache.get(state_key("count-count")) == "0"


def test_writes_duration_threshold_alerts_once_elapsed(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local()
    seed_window(influxdb3_local)
    started = datetime.now(timezone.utc) - timedelta(hours=2)
    influxdb3_local.cache.put(
        state_key("time-time", threshold="3600.0s"), started.isoformat()
    )
    args = {**WRITES_ARGS, "mad_thresholds": "temp:2:5:1h"}

    plugin.process_writes(influxdb3_local, batch(rows(40.0)), args)

    assert len(sent) == 1
    assert "outlier for 1:00:00" in sent[0]["payload"]["notification_text"]
    assert influxdb3_local.cache.get(state_key("time-time", threshold="3600.0s")) == ""


def test_writes_duration_threshold_keeps_the_start_while_waiting(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local()
    seed_window(influxdb3_local)
    started = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    influxdb3_local.cache.put(state_key("time-time", threshold="3600.0s"), started)
    args = {**WRITES_ARGS, "mad_thresholds": "temp:2:5:1h"}

    plugin.process_writes(influxdb3_local, batch(rows(40.0)), args)

    assert sent == []
    assert (
        influxdb3_local.cache.get(state_key("time-time", threshold="3600.0s"))
        == started
    )
    assert any("outlier ongoing" in m for m in influxdb3_local.messages("info"))


def test_writes_flapping_outlier_state_suppresses_notification(plugin_dir, sent):
    """Regression: flips were counted over raw values, which suppressed every alert."""
    influxdb3_local = FakeInfluxdb3Local()
    seed_window(influxdb3_local)
    args = {**WRITES_ARGS, "state_change_count": "2"}

    # outlier, normal, outlier, outlier: two transitions when the threshold is reached
    plugin.process_writes(influxdb3_local, batch(rows(40.0, 20.5, 40.0, 100.0)), args)

    assert sent == []
    assert any("outlier state flipped" in m for m in influxdb3_local.messages("warn"))


def test_writes_sustained_outlier_is_not_suppressed(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local()
    seed_window(influxdb3_local)
    args = {**WRITES_ARGS, "state_change_count": "2"}

    plugin.process_writes(influxdb3_local, batch(rows(40.0, 41.0)), args)

    assert len(sent) == 1


def test_writes_treats_state_change_count_of_one_as_disabled(plugin_dir, sent):
    """A sustained anomaly records one transition, so 1 would suppress every alert."""
    influxdb3_local = FakeInfluxdb3Local()
    seed_window(influxdb3_local)
    args = {**WRITES_ARGS, "state_change_count": "1"}

    plugin.process_writes(influxdb3_local, batch(rows(40.0, 20.5, 40.0, 100.0)), args)

    assert len(sent) == 1
    assert any("treating it as 0" in m for m in influxdb3_local.messages("warn"))
    assert not any("Suppressed" in m for m in influxdb3_local.messages("warn"))


def test_writes_shares_one_window_per_field_and_size(plugin_dir, sent):
    """Regression: thresholds on one field used to reset each other's window."""
    influxdb3_local = FakeInfluxdb3Local()
    seed_window(influxdb3_local)
    seed_window(influxdb3_local, values=WARMUP[1:], window_count=4)
    args = {**WRITES_ARGS, "mad_thresholds": "temp:2:5:2@temp:2:4:2"}

    plugin.process_writes(influxdb3_local, batch(rows(40.0)), args)

    assert len(influxdb3_local.cache.get(window_key(window_count=5))) == 5
    assert len(influxdb3_local.cache.get(window_key(window_count=4))) == 4
    assert not any("Waiting for" in m for m in influxdb3_local.messages("info"))
    assert influxdb3_local.cache.get(state_key("count-count", window_count=5)) == "1"
    assert influxdb3_local.cache.get(state_key("count-count", window_count=4)) == "1"


def test_writes_keeps_counters_of_thresholds_apart(plugin_dir, sent):
    """Regression: thresholds differing only in the count shared one counter."""
    influxdb3_local = FakeInfluxdb3Local()
    seed_window(influxdb3_local)
    args = {**WRITES_ARGS, "mad_thresholds": "temp:2:5:2@temp:2:5:10"}

    plugin.process_writes(influxdb3_local, batch(rows(40.0)), args)

    assert sent == []
    assert influxdb3_local.cache.get(state_key("count-count", threshold="2")) == "1"
    assert influxdb3_local.cache.get(state_key("count-count", threshold="10")) == "1"


def test_writes_sends_one_attempt_without_retrying(plugin_dir, monkeypatch):
    attempts = []

    def failing_post(url, headers=None, data=None, timeout=None):
        attempts.append(url)
        raise plugin.requests.ConnectionError("refused")

    monkeypatch.setattr(plugin.requests, "post", failing_post)
    influxdb3_local = FakeInfluxdb3Local()
    seed_window(influxdb3_local)

    plugin.process_writes(influxdb3_local, batch(rows(40.0, 41.0)), dict(WRITES_ARGS))

    assert len(attempts) == 1
    assert any("Failed to send alert" in m for m in influxdb3_local.messages("error"))


@pytest.mark.parametrize(
    "row", [{"host": "a", "hum": 5}, {"host": "a", "temp": "warm"}]
)
def test_writes_resets_state_when_the_field_is_unusable(plugin_dir, sent, row):
    influxdb3_local = FakeInfluxdb3Local()
    seed_window(influxdb3_local)
    influxdb3_local.cache.put(state_key("count-count"), "1")

    plugin.process_writes(influxdb3_local, batch([row]), dict(WRITES_ARGS))

    assert influxdb3_local.messages("error") == []
    assert influxdb3_local.cache.get(state_key("count-count")) == "0"


def test_writes_ignores_batches_of_other_tables(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local()

    plugin.process_writes(
        influxdb3_local, batch(rows(40.0), table="cpu"), dict(WRITES_ARGS)
    )

    assert sent == []
    assert not any("Starting writes process" in m for m in influxdb3_local.messages())
    assert not any("information_schema" in q for q in influxdb3_local.queries)


def test_writes_reports_unknown_measurement(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local(tables=("cpu",))

    plugin.process_writes(influxdb3_local, batch(rows(40.0)), dict(WRITES_ARGS))

    assert any("not found in database" in m for m in influxdb3_local.messages("error"))


def test_writes_caches_configuration(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local()

    plugin.process_writes(influxdb3_local, batch(rows(20.0)), dict(WRITES_ARGS))

    assert influxdb3_local.cache.get(plugin._WRITES_CONFIG_CACHE_KEY) is not None
    assert (
        influxdb3_local.cache.ttls[plugin._WRITES_CONFIG_CACHE_KEY]
        == plugin._WRITES_CONFIG_TTL_SECONDS
    )


def test_writes_never_logs_credentials(plugin_dir, sent):
    influxdb3_local = FakeInfluxdb3Local()
    seed_window(influxdb3_local)

    plugin.process_writes(influxdb3_local, batch(rows(40.0, 41.0)), dict(WRITES_ARGS))

    logged = " ".join(influxdb3_local.messages())
    assert TOKEN not in logged
    assert WEBHOOK not in logged
