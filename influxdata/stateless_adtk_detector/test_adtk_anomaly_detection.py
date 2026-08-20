import base64
import json
from datetime import datetime, timedelta, timezone
from itertools import chain

import pandas as pd
import pytest

import adtk_anomaly_detection_plugin as plugin


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

    def __init__(self, tables=("cpu",), tags=("host",), rows=None):
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
        self.queries.append((query, params))
        if "SHOW TABLES" in query:
            return [{"table_name": t, "table_type": "BASE TABLE"} for t in self.tables]
        if "information_schema" in query:
            return [{"column_name": tag} for tag in self.tags]
        return self.rows

    def messages(self, level=None):
        return [m for lvl, m in self.logs if level is None or lvl == level]

    def logged(self, fragment, level=None):
        return any(fragment in message for message in self.messages(level))

    def window_query(self):
        return next(
            (q, p)
            for q, p in self.queries
            if "information_schema" not in q and "SHOW" not in q
        )


class FakeResponse:
    def __init__(self, status_code=200):
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise plugin.requests.HTTPError(f"{self.status_code} Server Error")

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
    monkeypatch.delenv("INFLUXDB3_PLUGIN_DIR", raising=False)
    monkeypatch.delenv("INFLUXDB3_AUTH_TOKEN", raising=False)
    return tmp_path


START = datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)
CALL_TIME = START + timedelta(hours=1)


def encode(params):
    return base64.b64encode(json.dumps(params).encode()).decode()


ARGS = {
    "measurement": "cpu",
    "field": "usage",
    "detectors": "ThresholdAD",
    "detector_params": encode({"ThresholdAD": {"high": 200}}),
    "window": "2h",
    "senders": "http",
    "http_webhook_url": "https://example.com/hook",
    "influxdb3_auth_token": "tok",
}


def rows(values, host="server1", start=START, step=timedelta(minutes=1)):
    """Build query results: one row per value, one step apart."""
    return [
        {
            "usage": value,
            "time": int((start + step * index).timestamp() * 1_000_000_000),
            "host": host,
        }
        for index, value in enumerate(values)
    ]


def merged(*row_lists):
    """Interleave several series the way a time-ordered query returns them."""
    return sorted(chain(*row_lists), key=lambda row: row["time"])


def run(args=None, local=None, call_time=CALL_TIME):
    local = local or FakeInfluxdb3Local(rows=rows([10.0, 999.0, 10.0]))
    plugin.process_scheduled_call(local, call_time, {**ARGS, **(args or {})})
    return local


def series_of(values, start=START):
    index = pd.to_datetime(
        [
            int((start + timedelta(minutes=i)).timestamp() * 1e9)
            for i in range(len(values))
        ],
        unit="ns",
    )
    return pd.Series(values, index=index)


# --- configuration ----------------------------------------------------------


def test_config_applies_defaults(plugin_dir):
    config = plugin._load_config(FakeInfluxdb3Local(), dict(ARGS), "tid")

    assert config["min_consensus"] == 1
    assert config["group_by_tags"] is False
    assert config["max_notifications_per_run"] == 20
    assert config["min_condition_duration"] == timedelta(0)
    assert config["port_override"] == 8181
    assert config["notification_path"] == "notify"
    assert config["notification_text"] == plugin._DEFAULT_NOTIFICATION_TEXT
    assert config["window"] == timedelta(hours=2)
    assert config["detectors"] == ["ThresholdAD"]


def test_config_reports_missing_required_argument(plugin_dir):
    local = FakeInfluxdb3Local()
    args = {key: value for key, value in ARGS.items() if key != "field"}

    assert plugin._load_config(local, args, "tid") is None
    assert local.logged("field is required", "error")


@pytest.mark.parametrize(
    "override",
    [
        {"min_consensus": "0"},
        {"window": "0s"},
        {"window": "10m"},
        {"port_override": "70000"},
        {"group_by_tags": "maybe"},
        {"max_notifications_per_run": "0"},
        {"min_condition_duration": "-5min"},
    ],
)
def test_config_rejects_invalid_values(plugin_dir, override):
    local = FakeInfluxdb3Local()

    assert plugin._load_config(local, {**ARGS, **override}, "tid") is None
    assert local.logged("Failed to load configuration", "error")


def test_config_rejects_non_toml_path(plugin_dir):
    local = FakeInfluxdb3Local()

    assert (
        plugin._load_config(local, {"config_file_path": "config.yaml"}, "tid") is None
    )
    assert local.logged("expected a .toml file", "error")


def test_config_reports_missing_toml_file(plugin_dir):
    local = FakeInfluxdb3Local()

    assert (
        plugin._load_config(local, {"config_file_path": "absent.toml"}, "tid") is None
    )
    assert local.logged("Failed to load configuration", "error")


def test_config_from_toml_uses_native_structures(plugin_dir, sent):
    (plugin_dir / "adtk.toml").write_text(
        'measurement = "cpu"\n'
        'field = "usage"\n'
        'detectors = ["ThresholdAD"]\n'
        'window = "2h"\n'
        'senders = ["http"]\n'
        'http_webhook_url = "https://example.com/hook"\n'
        'influxdb3_auth_token = "from-toml"\n'
        "port_override = 8182\n"
        'notification_path = "custom/notify"\n'
        "\n[detector_params]\n"
        "ThresholdAD = { high = 200 }\n"
    )
    local = FakeInfluxdb3Local(rows=rows([10.0, 999.0]))

    plugin.process_scheduled_call(local, CALL_TIME, {"config_file_path": "adtk.toml"})

    assert len(sent) == 1
    assert sent[0]["url"] == "http://localhost:8182/api/v3/engine/custom/notify"
    assert sent[0]["headers"]["Authorization"] == "Bearer from-toml"


def test_config_from_toml_accepts_inline_spellings(plugin_dir, sent):
    (plugin_dir / "adtk.toml").write_text(
        'measurement = "cpu"\n'
        'field = "usage"\n'
        'detectors = "ThresholdAD"\n'
        f'detector_params = "{encode({"ThresholdAD": {"high": 200}})}"\n'
        'window = "2h"\n'
        'senders = "http.slack"\n'
        'http_webhook_url = "https://example.com/hook"\n'
        'slack_webhook_url = "https://hooks.slack.com/services/T"\n'
        'influxdb3_auth_token = "tok"\n'
    )
    local = FakeInfluxdb3Local(rows=rows([10.0, 999.0]))

    plugin.process_scheduled_call(local, CALL_TIME, {"config_file_path": "adtk.toml"})

    assert set(sent[0]["payload"]["senders_config"]) == {"http", "slack"}


def test_token_falls_back_to_environment(monkeypatch, plugin_dir, sent):
    monkeypatch.setenv("INFLUXDB3_AUTH_TOKEN", "from-env")
    args = {key: value for key, value in ARGS.items() if key != "influxdb3_auth_token"}
    local = FakeInfluxdb3Local(rows=rows([10.0, 999.0]))

    plugin.process_scheduled_call(local, CALL_TIME, args)

    assert sent[0]["headers"]["Authorization"] == "Bearer from-env"


def test_missing_token_stops_the_run(plugin_dir, sent):
    local = FakeInfluxdb3Local(rows=rows([999.0]))
    args = dict(ARGS)
    args["influxdb3_auth_token"] = ""

    plugin.process_scheduled_call(local, CALL_TIME, args)

    assert local.logged("Missing influxdb3_auth_token", "error")
    assert sent == []


# --- detector parameters ----------------------------------------------------


def test_decode_detector_params_accepts_mapping_and_base64():
    params = {"ThresholdAD": {"high": 200}}

    assert plugin.decode_detector_params(params) == params
    assert plugin.decode_detector_params(encode(params)) == params


@pytest.mark.parametrize(
    "raw, message",
    [
        ("!!!not base64!!!", "Invalid base64 encoding"),
        (base64.b64encode(b"{broken").decode(), "Invalid JSON"),
        (encode(["ThresholdAD"]), "must decode to a JSON object"),
    ],
)
def test_decode_detector_params_rejects_invalid(raw, message):
    with pytest.raises(Exception, match=message):
        plugin.decode_detector_params(raw)


def test_parse_detectors_skips_detectors_it_cannot_apply():
    local = FakeInfluxdb3Local()
    config = {
        "detectors": ["ThresholdAD", "Nonsense", "PersistAD", "LevelShiftAD"],
        "detector_params": {
            "ThresholdAD": {"high": 200},
            "LevelShiftAD": {},
            "Nonsense": {},
        },
    }

    detectors, params = plugin.parse_detectors(local, config, "tid")

    assert detectors == ["ThresholdAD"]
    assert set(params) == {"ThresholdAD"}
    assert local.logged("Unknown detector: Nonsense", "warn")
    assert local.logged("Missing parameters for detector: PersistAD", "warn")
    assert local.logged("LevelShiftAD requires the 'window' parameter", "warn")


def test_parse_detectors_rejects_non_mapping_parameters():
    local = FakeInfluxdb3Local()
    config = {"detectors": ["ThresholdAD"], "detector_params": {"ThresholdAD": [200]}}

    with pytest.raises(Exception, match="No applicable detectors"):
        plugin.parse_detectors(local, config, "tid")
    assert local.logged("must be a mapping", "warn")


# --- detection and consensus ------------------------------------------------


def test_detect_anomalies_requires_min_consensus_agreement():
    local = FakeInfluxdb3Local()
    series = plugin.validate_series(series_of([10.0, 11.0, 12.0, 300.0, 999.0]))
    detectors = ["QuantileAD", "ThresholdAD"]
    params = {"QuantileAD": {"high": 0.6}, "ThresholdAD": {"high": 500}}

    lenient = plugin.detect_anomalies(local, series, detectors, params, 1, "tid")
    strict = plugin.detect_anomalies(local, series, detectors, params, 2, "tid")

    assert lenient.sum() == 2
    assert strict.sum() == 1
    assert strict[strict].index == series.index[-1:]


def test_detect_anomalies_returns_none_when_every_detector_fails():
    local = FakeInfluxdb3Local()
    series = plugin.validate_series(series_of([1.0, 2.0, 3.0]))

    result = plugin.detect_anomalies(
        local, series, ["ThresholdAD"], {"ThresholdAD": {"nonsense": 1}}, 1, "tid"
    )

    assert result is None
    assert local.logged("Failed to apply detector ThresholdAD", "warn")


def test_only_trainable_detectors_are_fitted(monkeypatch):
    fitted = []

    class Recording:
        def __init__(self, **params):
            self.name = params["name"]

        def fit(self, series):
            fitted.append(self.name)

        def detect(self, series):
            return pd.Series(False, index=series.index)

    monkeypatch.setitem(plugin.AVAILABLE_DETECTORS, "ThresholdAD", Recording)
    monkeypatch.setitem(plugin.AVAILABLE_DETECTORS, "PersistAD", Recording)
    series = series_of([1.0, 2.0])

    plugin.detect_anomalies(
        FakeInfluxdb3Local(),
        series,
        ["ThresholdAD", "PersistAD"],
        {"ThresholdAD": {"name": "threshold"}, "PersistAD": {"name": "persist"}},
        1,
        "tid",
    )

    assert fitted == ["persist"]


def test_unreachable_min_consensus_warns(plugin_dir, sent):
    local = run({"min_consensus": "3"})

    assert local.logged("min_consensus=3 exceeds the 1 applicable detectors", "warn")
    assert sent == []


# --- series preparation -----------------------------------------------------


def test_split_by_tags_groups_only_when_enabled():
    frame = pd.DataFrame(merged(rows([1.0, 2.0]), rows([3.0, 4.0], host="server2")))

    assert len(plugin.split_by_tags(frame, ["host"], False)) == 1
    assert len(plugin.split_by_tags(frame, ["host"], True)) == 2
    assert len(plugin.split_by_tags(frame, [], True)) == 1


def test_null_values_are_dropped_before_detection(plugin_dir, sent):
    local = run(local=FakeInfluxdb3Local(rows=rows([10.0, None, 999.0, 10.0])))

    assert len(sent) == 1
    assert local.logged("Skipped 1 points without a 'usage' value", "info")
    assert local.messages("warn") == []


def test_series_without_any_value_is_skipped(plugin_dir, sent):
    local = run(local=FakeInfluxdb3Local(rows=rows([None, None])))

    assert sent == []
    assert local.logged("No values to analyze", "info")
    assert local.messages("error") == []


def test_duplicate_timestamps_keep_the_first_row(plugin_dir, sent):
    duplicated = rows([10.0, 999.0])
    duplicated.append({**duplicated[1], "usage": 10.0})

    local = run(local=FakeInfluxdb3Local(rows=duplicated))

    assert local.logged("Prepared time series data with 2 points", "info")
    assert len(sent) == 1
    assert (
        sent[0]["payload"]["notification_text"]
        == "Anomaly detected in cpu.usage with value 999.0 by ThresholdAD. Tags: host=server1"
    )


def test_cache_key_sorts_tags_and_marks_missing_ones():
    row = pd.Series({"region": "eu", "host": "server1"})

    key = plugin.generate_cache_key("cpu", "usage", ["region", "host", "rack"], row)

    assert key == "cpu:usage:host=server1:rack=None:region=eu"
    assert plugin.format_tags(row, ["host", "rack"]) == "host=server1, rack=None"


# --- senders ----------------------------------------------------------------


def test_senders_collect_channel_arguments():
    config = {
        "senders": "http.slack",
        "http_webhook_url": "https://example.com/hook",
        "slack_webhook_url": "https://hooks.slack.com/services/T",
        "slack_headers": "eyJhIjogMX0=",
    }

    senders = plugin.parse_senders(FakeInfluxdb3Local(), config, "tid")

    assert senders["http"] == {"http_webhook_url": "https://example.com/hook"}
    assert senders["slack"] == {
        "slack_webhook_url": "https://hooks.slack.com/services/T",
        "slack_headers": "eyJhIjogMX0=",
    }


def test_senders_drop_channel_without_required_argument():
    local = FakeInfluxdb3Local()
    config = {"senders": "http.sms", "http_webhook_url": "https://example.com/hook"}

    senders = plugin.parse_senders(local, config, "tid")

    assert set(senders) == {"http"}
    assert local.logged(
        "Required key 'twilio_to_number' missing for sender 'sms'", "warn"
    )


def test_senders_reject_unusable_configuration():
    local = FakeInfluxdb3Local()

    with pytest.raises(Exception, match="No valid senders configured"):
        plugin.parse_senders(local, {"senders": "carrier_pigeon"}, "tid")
    assert local.logged("Invalid sender type: carrier_pigeon", "warn")

    with pytest.raises(Exception, match="No valid senders configured"):
        plugin.parse_senders(
            FakeInfluxdb3Local(),
            {"senders": "http", "http_webhook_url": "ftp://x"},
            "tid",
        )


# --- notifications ----------------------------------------------------------


def test_notification_payload_carries_template_variables(plugin_dir, sent):
    run(
        {
            "notification_text": "$table.$field=$value at $timestamp by $detectors ($tags) $unknown"
        },
        local=FakeInfluxdb3Local(rows=rows([10.0, 999.0])),
    )

    text = sent[0]["payload"]["notification_text"]
    assert text == (
        "cpu.usage=999.0 at 2026-08-13T12:01:00 by ThresholdAD (host=server1) $unknown"
    )
    assert sent[0]["payload"]["senders_config"] == {
        "http": {"http_webhook_url": "https://example.com/hook"}
    }


def test_notification_cap_suppresses_the_rest(plugin_dir, sent):
    local = run(
        {"max_notifications_per_run": "2"},
        local=FakeInfluxdb3Local(rows=rows([999.0] * 5)),
    )

    assert len(sent) == 2
    assert local.logged("Suppressed 3 notifications", "warn")

    sent.clear()
    plugin.process_scheduled_call(
        local, CALL_TIME, {**ARGS, "max_notifications_per_run": "2"}
    )
    assert sent == []


@pytest.fixture
def failing_delivery(monkeypatch):
    """Make every notification attempt fail and count the attempts."""
    attempts = []

    def failing_post(url, headers=None, data=None, timeout=None):
        attempts.append(url)
        raise plugin.requests.ConnectionError("refused")

    monkeypatch.setattr(plugin.requests, "post", failing_post)
    monkeypatch.setattr(plugin.time, "sleep", lambda seconds: None)
    return attempts


def test_failed_delivery_is_retried_by_the_next_run(plugin_dir, failing_delivery):
    local = run(local=FakeInfluxdb3Local(rows=rows([999.0])))

    assert len(failing_delivery) == 3
    assert local.logged(
        "Failed to send alert to notification plugin after 3 attempts", "error"
    )
    assert local.logged("1 notifications could not be delivered", "warn")
    assert local.logged("0 notifications sent", "info")
    # the point stays unhandled, so a later run alerts on it again
    assert not any(key.endswith(":last_alert") for key in local.cache.store)

    plugin.process_scheduled_call(local, CALL_TIME, dict(ARGS))
    assert len(failing_delivery) == 6


def test_failed_deliveries_count_towards_the_cap(plugin_dir, failing_delivery):
    run(
        {"max_notifications_per_run": "2"},
        local=FakeInfluxdb3Local(rows=rows([999.0] * 6)),
    )

    assert len(failing_delivery) == 6  # two alerts, three attempts each


# --- debounce ---------------------------------------------------------------


def test_anomaly_alerts_immediately_without_debounce(plugin_dir, sent):
    local = run(local=FakeInfluxdb3Local(rows=rows([10.0, 999.0, 999.0])))

    assert len(sent) == 2
    assert local.logged("Anomaly detected for cpu.usage", "error")


def test_debounce_waits_for_the_configured_duration(plugin_dir, sent):
    local = run(
        {"min_condition_duration": "3min"},
        local=FakeInfluxdb3Local(rows=rows([10.0] + [999.0] * 5)),
    )

    assert len(sent) == 1
    assert local.logged("Anomaly started for cpu.usage", "info")
    assert local.logged("Anomaly ongoing for 0 days 00:01:00", "info")
    assert local.logged("Anomaly persisted for 0 days 00:03:00", "error")


def test_debounce_state_is_cleared_when_the_anomaly_stops(plugin_dir, sent):
    local = run(
        {"min_condition_duration": "1h"},
        local=FakeInfluxdb3Local(rows=rows([999.0, 999.0, 10.0])),
    )

    assert sent == []
    assert local.logged("Anomaly cleared for cpu.usage", "info")
    assert "cpu:usage:host=server1" not in local.cache.store


def test_debounce_longer_than_the_window_warns(plugin_dir, sent):
    local = run({"window": "10min", "min_condition_duration": "1h"})

    assert local.logged("is not shorter than window", "warn")


# --- overlapping windows ----------------------------------------------------


def test_same_anomaly_is_reported_once_across_runs(plugin_dir, sent):
    local = FakeInfluxdb3Local(rows=rows([10.0, 999.0, 10.0]))

    for _ in range(3):
        plugin.process_scheduled_call(local, CALL_TIME, dict(ARGS))

    assert len(sent) == 1
    assert (
        local.cache.store["cpu:usage:host=server1:last_alert"] == "2026-08-13T12:01:00"
    )


def test_nanosecond_timestamps_are_not_realerted(plugin_dir, sent):
    # datetime.fromisoformat truncates to microseconds, which would let a point
    # with nanosecond precision pass the "already alerted" check on every run
    nanos = [
        {
            "usage": 999.0 if index else 10.0,
            "time": int(START.timestamp() * 1_000_000_000)
            + index * 60_000_000_000
            + 678_851_840,
            "host": "server1",
        }
        for index in range(2)
    ]
    local = FakeInfluxdb3Local(rows=nanos)

    plugin.process_scheduled_call(local, CALL_TIME, dict(ARGS))
    assert len(sent) == 1

    plugin.process_scheduled_call(local, CALL_TIME, dict(ARGS))
    assert len(sent) == 1


def test_anomaly_after_the_last_alert_is_reported(plugin_dir, sent):
    local = FakeInfluxdb3Local(rows=rows([10.0, 999.0]))
    plugin.process_scheduled_call(local, CALL_TIME, dict(ARGS))

    local.rows = rows([10.0, 999.0, 10.0, 999.0])
    sent.clear()
    plugin.process_scheduled_call(local, CALL_TIME, dict(ARGS))

    assert len(sent) == 1
    assert (
        local.cache.store["cpu:usage:host=server1:last_alert"] == "2026-08-13T12:03:00"
    )


# --- tag grouping -----------------------------------------------------------


def test_without_grouping_only_the_first_series_is_analyzed(plugin_dir, sent):
    local = run(
        local=FakeInfluxdb3Local(
            rows=merged(rows([10.0, 10.0]), rows([20.0, 999.0], host="server2"))
        )
    )

    assert sent == []
    assert local.logged("Prepared time series data with 2 points", "info")


def test_grouping_analyzes_every_tag_combination(plugin_dir, sent):
    local = run(
        {"group_by_tags": "true"},
        local=FakeInfluxdb3Local(
            rows=merged(rows([10.0, 10.0]), rows([20.0, 999.0], host="server2"))
        ),
    )

    assert len(sent) == 1
    assert "host=server2" in sent[0]["payload"]["notification_text"]
    assert local.logged("on 2 series", "info")
    assert local.logged("(tags: host=server1)", "info")


def test_grouping_keeps_debounce_state_per_series(plugin_dir, sent):
    local = run(
        {"group_by_tags": "true", "min_condition_duration": "2min"},
        local=FakeInfluxdb3Local(
            rows=merged(rows([999.0] * 4), rows([999.0] * 4, host="server2"))
        ),
    )

    assert len(sent) == 2
    assert sorted(k for k in local.cache.store if k.endswith(":last_alert")) == [
        "cpu:usage:host=server1:last_alert",
        "cpu:usage:host=server2:last_alert",
    ]


# --- scheduled flow guards --------------------------------------------------


def test_unknown_measurement_is_reported(plugin_dir, sent):
    local = run(local=FakeInfluxdb3Local(tables=("memory",), rows=rows([999.0])))

    assert local.logged("Measurement 'cpu' not found", "error")
    assert sent == []


def test_empty_window_is_reported(plugin_dir, sent):
    local = run(local=FakeInfluxdb3Local(rows=[]))

    assert local.logged("No data found for cpu.usage", "info")
    assert sent == []


def test_missing_field_column_is_reported(plugin_dir, sent):
    local = run(
        local=FakeInfluxdb3Local(rows=[{"other": 1.0, "time": 0, "host": "server1"}])
    )

    assert local.logged("Field 'usage' or 'time' not found", "error")
    assert sent == []


def test_query_covers_the_window_and_quotes_identifiers(plugin_dir, sent):
    local = run(local=FakeInfluxdb3Local(rows=rows([10.0])))
    query, params = local.window_query()

    assert '"usage", "time", "host"' in query
    assert 'FROM "cpu"' in query
    assert params["start"] == (CALL_TIME - timedelta(hours=2)).isoformat()
    assert params["end"] == CALL_TIME.isoformat()
