import json
import re
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
import requests

import forecast_error_evaluator as plugin

TOKEN = "apiv3_secret_token_value"
WEBHOOK = "https://example.com/hook"
BASE_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)
CALL_TIME = BASE_TIME + timedelta(minutes=30)


class FakeCache:
    def __init__(self):
        self.store = {}

    def get(self, key, default=None, use_global=None):
        return self.store.get(key, default)

    def put(self, key, value, ttl=None, use_global=None):
        self.store[key] = value

    def delete(self, key, use_global=None):
        return self.store.pop(key, None) is not None


class FakeInfluxdb3Local:
    """Stub of the runtime client: logging, trigger-local cache and queries."""

    def __init__(self, tables=None, tags=None, rows=None):
        self.cache = FakeCache()
        self.logs = []
        self.tables = list(tables or ["temp_forecast", "temp_actual"])
        self.tags = dict(tags or {"temp_forecast": ["host"], "temp_actual": ["host"]})
        self.rows = dict(rows or {})
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
        if "information_schema.columns" in query:
            return [{"column_name": tag} for tag in self.tags.get(params["table"], [])]
        table = re.search(r'FROM "([^"]+)"', query).group(1)
        return list(self.rows.get(table, []))

    def messages(self, level=None):
        return [m for lvl, m in self.logs if level is None or lvl == level]


class FakeResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {"results": "recorded"}


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(plugin.time, "sleep", lambda seconds: None)


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
def failing_posts(monkeypatch):
    """Record every delivery attempt and fail it."""
    attempts = []

    def fake_post(url, headers=None, data=None, timeout=None):
        attempts.append(url)
        raise requests.RequestException("connection refused")

    monkeypatch.setattr(plugin.requests, "post", fake_post)
    return attempts


@pytest.fixture
def plugin_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    monkeypatch.delenv("INFLUXDB3_AUTH_TOKEN", raising=False)
    return tmp_path


SCHED_ARGS = {
    "forecast_measurement": "temp_forecast",
    "actual_measurement": "temp_actual",
    "forecast_field": "predicted",
    "actual_field": "temp",
    "error_metric": "mae",
    "error_thresholds": "WARN-1.0",
    "window": "1h",
    "senders": "http",
    "http_webhook_url": WEBHOOK,
    "influxdb3_auth_token": TOKEN,
}


def series(values, field, host="a", step_minutes=5, start_offset_minutes=0):
    return [
        {
            "time": int(
                (
                    BASE_TIME
                    + timedelta(minutes=start_offset_minutes + i * step_minutes)
                ).timestamp()
                * 1e9
            ),
            field: value,
            "host": host,
        }
        for i, value in enumerate(values)
    ]


def client(forecast, actual, **kwargs):
    """Build a client whose two measurements hold the given value sequences."""
    return FakeInfluxdb3Local(
        rows={
            "temp_forecast": series(forecast, "predicted"),
            "temp_actual": series(actual, "temp"),
        },
        **kwargs,
    )


# --- thresholds -------------------------------------------------------------


def test_thresholds_from_string():
    influxdb3_local = FakeInfluxdb3Local()

    thresholds = plugin.parse_error_thresholds(
        influxdb3_local, {"error_thresholds": "INFO-10:WARN-'20.5':ERROR-1e2"}, "tid"
    )

    assert thresholds == {"INFO": 10.0, "WARN": 20.5, "ERROR": 100.0}


def test_thresholds_from_toml_table_normalizes_levels_and_types():
    influxdb3_local = FakeInfluxdb3Local()

    thresholds = plugin.parse_error_thresholds(
        influxdb3_local,
        {"error_thresholds": {"INFO": 1.0, "warn": "2.5", "ERROR": 3}},
        "tid",
    )

    assert thresholds == {"INFO": 1.0, "WARN": 2.5, "ERROR": 3.0}


@pytest.mark.parametrize(
    "raw, reason",
    [
        ("INFO", "expected <level>-<value>"),
        ("DEBUG-1.0", "level must be one of"),
        ("INFO-abc", "is not a number"),
        ("INFO-0", "would flag every point"),
        ("INFO--5", "would flag every point"),
    ],
)
def test_thresholds_skip_invalid_segments(raw, reason):
    influxdb3_local = FakeInfluxdb3Local()

    thresholds = plugin.parse_error_thresholds(
        influxdb3_local, {"error_thresholds": raw}, "tid"
    )

    assert thresholds == {}
    assert any(reason in message for message in influxdb3_local.messages("warn"))


def test_thresholds_keep_the_first_of_a_repeated_level():
    influxdb3_local = FakeInfluxdb3Local()

    thresholds = plugin.parse_error_thresholds(
        influxdb3_local, {"error_thresholds": "INFO-1:INFO-2"}, "tid"
    )

    assert thresholds == {"INFO": 1.0}
    assert any(
        "duplicate threshold 'INFO'" in message
        for message in influxdb3_local.messages("warn")
    )


# --- configuration ----------------------------------------------------------


def test_load_config_applies_defaults(plugin_dir):
    influxdb3_local = FakeInfluxdb3Local()

    config = plugin._load_config(influxdb3_local, dict(SCHED_ARGS), "tid")

    assert config["window"] == timedelta(hours=1)
    assert config["min_condition_duration"] == timedelta(0)
    assert config["max_notifications_per_run"] == 20
    assert config["port_override"] == 8181
    assert config["rounding_freq"] == ""


@pytest.mark.parametrize(
    "overrides",
    [
        {"error_metric": "r2"},
        {"window": "0s"},
        {"window": "5m"},
        {"port_override": "70000"},
        {"rounding_freq": "1nonsense"},
        {"max_notifications_per_run": "0"},
    ],
)
def test_load_config_rejects_invalid_values(plugin_dir, overrides):
    influxdb3_local = FakeInfluxdb3Local()

    assert (
        plugin._load_config(influxdb3_local, {**SCHED_ARGS, **overrides}, "tid") is None
    )
    assert any(
        "Failed to load configuration" in message
        for message in influxdb3_local.messages("error")
    )


def test_load_config_rejects_non_toml_path(plugin_dir):
    influxdb3_local = FakeInfluxdb3Local()

    assert (
        plugin._load_config(influxdb3_local, {"config_file_path": "config.yaml"}, "tid")
        is None
    )
    assert any(
        "expected a .toml file" in message
        for message in influxdb3_local.messages("error")
    )


def test_toml_config_accepts_native_structures(plugin_dir, sent):
    (plugin_dir / "forecast.toml").write_text(
        "\n".join(
            [
                'forecast_measurement = "temp_forecast"',
                'actual_measurement = "temp_actual"',
                'forecast_field = "predicted"',
                'actual_field = "temp"',
                'error_metric = "mae"',
                "error_thresholds = {WARN = 1.0}",
                'window = "1h"',
                'senders = ["http"]',
                f'http_webhook_url = "{WEBHOOK}"',
                f'influxdb3_auth_token = "{TOKEN}"',
            ]
        )
    )
    influxdb3_local = client([10.0, 10.0], [10.0, 12.0])

    plugin.process_scheduled_call(
        influxdb3_local, CALL_TIME, {"config_file_path": "forecast.toml"}
    )

    assert len(sent) == 1


# --- senders ----------------------------------------------------------------


def test_senders_accept_string_and_list():
    influxdb3_local = FakeInfluxdb3Local()
    config = {
        "senders": "http.discord",
        "http_webhook_url": WEBHOOK,
        "discord_webhook_url": WEBHOOK,
    }

    from_string = plugin.parse_senders(influxdb3_local, config, "tid")
    from_list = plugin.parse_senders(
        influxdb3_local, {**config, "senders": ["http", "discord"]}, "tid"
    )

    assert from_string == from_list
    assert set(from_string) == {"http", "discord"}


@pytest.mark.parametrize(
    "config, reason",
    [
        ({"senders": "telegram"}, "Invalid sender type"),
        ({"senders": "http"}, "Required key 'http_webhook_url' missing"),
        ({"senders": "http", "http_webhook_url": "ftp://x"}, "must start with"),
    ],
)
def test_senders_reject_unusable_channels(config, reason):
    influxdb3_local = FakeInfluxdb3Local()

    with pytest.raises(Exception, match="No valid senders configured"):
        plugin.parse_senders(influxdb3_local, config, "tid")
    assert any(reason in message for message in influxdb3_local.messages())


# --- alignment and metrics --------------------------------------------------


def test_shared_tags_use_the_intersection():
    influxdb3_local = FakeInfluxdb3Local(
        tags={"temp_forecast": ["host", "model"], "temp_actual": ["host", "region"]}
    )

    tags = plugin.resolve_shared_tags(
        influxdb3_local, "temp_forecast", "temp_actual", "tid"
    )

    assert tags == ["host"]
    assert any("model, region" in m for m in influxdb3_local.messages("warn"))


def test_align_frames_collapses_rows_sharing_a_rounded_timestamp():
    influxdb3_local = FakeInfluxdb3Local()
    df_forecast = pd.DataFrame(series([10.0, 11.0, 12.0], "forecast", step_minutes=1))
    df_actual = pd.DataFrame(series([10.0, 11.0, 12.0], "actual", step_minutes=1))

    merged = plugin.align_frames(
        influxdb3_local, df_forecast, df_actual, ["host"], "1h", "tid"
    )

    assert len(merged) == 1
    assert any(
        "Collapsed 2 forecast and 2 actual rows" in m
        for m in influxdb3_local.messages("info")
    )


def test_align_frames_reports_missing_overlap():
    influxdb3_local = FakeInfluxdb3Local()
    df_forecast = pd.DataFrame(series([10.0], "forecast", host="a"))
    df_actual = pd.DataFrame(series([10.0], "actual", host="b"))

    merged = plugin.align_frames(
        influxdb3_local, df_forecast, df_actual, ["host"], "", "tid"
    )

    assert merged is None
    assert any(
        "No overlapping timestamps" in m for m in influxdb3_local.messages("error")
    )


def test_compute_error_skips_undefined_mape_rows():
    influxdb3_local = FakeInfluxdb3Local()
    merged = pd.DataFrame({"forecast": [10.0, 12.0], "actual": [0.0, 10.0]})

    result = plugin.compute_error(influxdb3_local, merged, "mape", "tid")

    assert result["error"].tolist() == [20.0]
    assert any("denominator is zero" in m for m in influxdb3_local.messages("warn"))


def test_compute_error_treats_rmse_per_point_as_absolute_difference():
    influxdb3_local = FakeInfluxdb3Local()
    merged = pd.DataFrame({"forecast": [10.0, 8.0], "actual": [12.0, 10.0]})

    rmse = plugin.compute_error(influxdb3_local, merged, "rmse", "tid")
    mae = plugin.compute_error(influxdb3_local, merged, "mae", "tid")

    assert rmse["error"].tolist() == mae["error"].tolist() == [2.0, 2.0]


# --- scheduled run ----------------------------------------------------------


def test_alerts_on_the_first_point_above_the_threshold(plugin_dir, sent):
    """Without a debounce duration the first outlier must alert, not arm a counter."""
    influxdb3_local = client([10.0, 10.0], [10.0, 12.0])

    plugin.process_scheduled_call(influxdb3_local, CALL_TIME, dict(SCHED_ARGS))

    assert len(sent) == 1
    text = sent[0]["payload"]["notification_text"]
    assert text.startswith("[WARN] Forecast error alert in temp_actual.temp")
    assert "host=a" in text


def test_second_run_does_not_realert_on_the_same_point(plugin_dir, sent):
    influxdb3_local = client([10.0, 10.0], [10.0, 12.0])

    plugin.process_scheduled_call(influxdb3_local, CALL_TIME, dict(SCHED_ARGS))
    plugin.process_scheduled_call(influxdb3_local, CALL_TIME, dict(SCHED_ARGS))

    assert len(sent) == 1


def test_debounce_waits_for_the_condition_to_persist(plugin_dir, sent):
    influxdb3_local = client([10.0, 10.0, 10.0], [12.0, 12.0, 12.0])
    args = {**SCHED_ARGS, "min_condition_duration": "5min"}

    plugin.process_scheduled_call(influxdb3_local, CALL_TIME, args)

    assert len(sent) == 1
    assert any("waiting for 0:05:00" in m for m in influxdb3_local.messages("info"))
    assert any(
        "alert triggered after 0:05:00" in m for m in influxdb3_local.messages("error")
    )


def test_every_level_alerts_separately(plugin_dir, sent):
    influxdb3_local = client([10.0], [12.0])
    args = {**SCHED_ARGS, "error_thresholds": "INFO-0.5:WARN-1.0"}

    plugin.process_scheduled_call(influxdb3_local, CALL_TIME, args)

    levels = [post["payload"]["notification_text"][:6] for post in sent]
    assert sorted(levels) == ["[INFO]", "[WARN]"]


def test_notifications_are_capped_per_run(plugin_dir, sent):
    influxdb3_local = client([10.0] * 4, [12.0, 13.0, 14.0, 15.0])
    args = {**SCHED_ARGS, "max_notifications_per_run": "2"}

    plugin.process_scheduled_call(influxdb3_local, CALL_TIME, args)

    assert len(sent) == 2
    assert any(
        "Suppressed 2 notifications" in m for m in influxdb3_local.messages("warn")
    )


def test_severe_levels_are_notified_before_lower_ones(plugin_dir, sent):
    """A cap must not be spent on INFO while a CRITICAL point goes unreported."""
    influxdb3_local = client([10.0] * 4, [11.0, 11.0, 11.0, 30.0])
    args = {
        **SCHED_ARGS,
        "error_thresholds": "INFO-0.5:CRITICAL-10",
        "max_notifications_per_run": "1",
    }

    plugin.process_scheduled_call(influxdb3_local, CALL_TIME, args)

    assert len(sent) == 1
    assert sent[0]["payload"]["notification_text"].startswith("[CRITICAL]")


def test_pending_state_from_outside_the_window_is_discarded(plugin_dir, sent):
    """A data gap must not stand in for an error that persisted."""
    influxdb3_local = client([10.0], [12.0])
    args = {**SCHED_ARGS, "min_condition_duration": "30min"}

    plugin.process_scheduled_call(influxdb3_local, CALL_TIME, args)
    assert not sent

    influxdb3_local.rows = {
        "temp_forecast": series([10.0], "predicted", start_offset_minutes=300),
        "temp_actual": series([12.0], "temp", start_offset_minutes=300),
    }
    plugin.process_scheduled_call(
        influxdb3_local, BASE_TIME + timedelta(hours=5, minutes=1), args
    )

    assert not sent
    assert (
        len([m for m in influxdb3_local.messages("info") if "waiting for 0:30:00" in m])
        == 2
    )


def test_window_bounds_keep_the_call_time_instant(plugin_dir):
    influxdb3_local = client([10.0], [10.0])
    aware_call_time = datetime(2026, 1, 1, 12, 0, tzinfo=timezone(timedelta(hours=3)))

    plugin.process_scheduled_call(influxdb3_local, aware_call_time, dict(SCHED_ARGS))

    bounds = [
        params for _, params in influxdb3_local.queries if params and "end" in params
    ]
    assert bounds and bounds[0]["end"] == aware_call_time.isoformat()


def test_failed_delivery_is_retried_on_the_next_run(plugin_dir, failing_posts):
    influxdb3_local = client([10.0], [12.0])

    plugin.process_scheduled_call(influxdb3_local, CALL_TIME, dict(SCHED_ARGS))
    assert any("could not be delivered" in m for m in influxdb3_local.messages("warn"))

    plugin.process_scheduled_call(influxdb3_local, CALL_TIME, dict(SCHED_ARGS))

    # three attempts per run: the undelivered alert was not marked as sent
    assert len(failing_posts) == 6


def test_missing_measurement_is_reported(plugin_dir, sent):
    influxdb3_local = client([10.0], [12.0], tables=["temp_actual"])

    plugin.process_scheduled_call(influxdb3_local, CALL_TIME, dict(SCHED_ARGS))

    assert not sent
    assert any(
        "Measurement 'temp_forecast' not found" in m
        for m in influxdb3_local.messages("error")
    )


def test_no_credentials_reach_the_logs(plugin_dir, sent):
    influxdb3_local = client([10.0], [12.0])

    plugin.process_scheduled_call(influxdb3_local, CALL_TIME, dict(SCHED_ARGS))

    assert len(sent) == 1
    assert not any(
        TOKEN in message or WEBHOOK in message for message in influxdb3_local.messages()
    )
