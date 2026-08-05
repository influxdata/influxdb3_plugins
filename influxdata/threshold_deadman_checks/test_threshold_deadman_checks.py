import json
import os
from datetime import datetime, timedelta, timezone

import pytest

import threshold_deadman_checks_plugin as plugin


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
        self.queries.append(query)
        if "SHOW TABLES" in query:
            return [{"table_name": t, "table_type": "BASE TABLE"} for t in self.tables]
        if "information_schema" in query:
            return [{"column_name": tag} for tag in self.tags]
        return self.rows

    def messages(self, level=None):
        return [m for lvl, m in self.logs if level is None or lvl == level]


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
def host_timezone():
    """Switch the process timezone and restore it afterwards."""
    original = os.environ.get("TZ")

    def use(name):
        os.environ["TZ"] = name
        plugin.time.tzset()

    yield use
    if original is None:
        os.environ.pop("TZ", None)
    else:
        os.environ["TZ"] = original
    plugin.time.tzset()


@pytest.fixture
def plugin_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    monkeypatch.delenv("INFLUXDB3_AUTH_TOKEN", raising=False)
    return tmp_path


WRITES_ARGS = {
    "measurement": "cpu",
    "field_conditions": "temp>30-WARN",
    "senders": "http",
    "http_webhook_url": "https://example.com/hook",
    "influxdb3_auth_token": "tok",
}
SCHEDULED_ARGS = {
    "measurement": "cpu",
    "senders": "http",
    "http_webhook_url": "https://example.com/hook",
    "influxdb3_auth_token": "tok",
    "window": "10min",
    "interval": "1min",
}


def batch(rows, table="cpu"):
    return [{"table_name": table, "rows": rows}]


# --- parsing ----------------------------------------------------------------


@pytest.mark.parametrize(
    "condition, actual, matches",
    [
        ("temp>30-WARN", 40.0, True),
        ("temp<30-WARN", 40.0, False),
        ("temp>=40-WARN", 40.0, True),
        ("temp<=40-WARN", 40.0, True),
        ("status=='ok'-INFO", "ok", True),
        ("status!='ok'-INFO", "ok", False),
    ],
)
def test_conditions_from_string_operators(condition, actual, matches):
    field, op_sym, compare_fn, value, level = plugin.parse_field_conditions(
        FakeInfluxdb3Local(), {"field_conditions": condition}, "tid"
    )[0]

    assert compare_fn(actual, value) is matches
    assert condition.startswith(field) and op_sym in condition


def test_conditions_from_entries_normalizes_level_and_keeps_symbol():
    conditions = plugin.parse_field_conditions(
        FakeInfluxdb3Local(),
        {"field_conditions": [["temp", ">", 30.0, "warn"]]},
        "tid",
    )

    assert conditions == [("temp", ">", plugin.operator.gt, 30.0, "WARN")]


def test_conditions_from_entries_skips_malformed_and_keeps_valid():
    client = FakeInfluxdb3Local()

    conditions = plugin.parse_field_conditions(
        client,
        {"field_conditions": [["temp", ">", 30.0, "WARN"], ["cpu", ">"], "junk"]},
        "tid",
    )

    assert [c[0] for c in conditions] == ["temp"]
    assert len([m for m in client.messages("warn") if "Invalid condition" in m]) == 2


def test_conditions_reject_unsupported_type():
    with pytest.raises(Exception, match="must be a list of entries or a string"):
        plugin.parse_field_conditions(
            FakeInfluxdb3Local(), {"field_conditions": 42}, "tid"
        )


def test_conditions_reject_when_nothing_valid():
    with pytest.raises(Exception, match="No valid field conditions"):
        plugin.parse_field_conditions(
            FakeInfluxdb3Local(), {"field_conditions": "temp>30-NOSUCH"}, "tid"
        )


def test_aggregations_from_string():
    parsed = plugin.parse_field_aggregation_values(
        FakeInfluxdb3Local(),
        {"field_aggregation_values": "temp:avg@>30-ERROR temp:max@<5.0-info"},
        "tid",
    )

    assert parsed == {
        "temp": [
            ["avg", ">", plugin.operator.gt, 30.0, "ERROR"],
            ["max", "<", plugin.operator.lt, 5.0, "INFO"],
        ]
    }


def test_aggregations_from_mapping_normalizes_level():
    parsed = plugin.parse_field_aggregation_values(
        FakeInfluxdb3Local(),
        {"field_aggregation_values": {"temp": [["max", ">", 30.0, "error"]]}},
        "tid",
    )

    assert parsed == {"temp": [["max", ">", plugin.operator.gt, 30.0, "ERROR"]]}


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_aggregations_absent_or_blank_is_empty(raw):
    config = {} if raw is None else {"field_aggregation_values": raw}

    assert (
        plugin.parse_field_aggregation_values(FakeInfluxdb3Local(), config, "tid") == {}
    )


def test_aggregations_reject_unsupported_type():
    with pytest.raises(Exception, match="must be a mapping or a string"):
        plugin.parse_field_aggregation_values(
            FakeInfluxdb3Local(),
            {"field_aggregation_values": [["temp", "avg", ">", 30, "ERROR"]]},
            "tid",
        )


@pytest.mark.parametrize(
    "condition, expected",
    [
        ("temp>30-WARN", 30),
        ("temp>30.5-WARN", 30.5),
        ("flag==true-WARN", True),
        ("status=='ok'-WARN", "ok"),
        ('status=="ok"-WARN', "ok"),
    ],
)
def test_conditions_coerce_value_types(condition, expected):
    value = plugin.parse_field_conditions(
        FakeInfluxdb3Local(), {"field_conditions": condition}, "tid"
    )[0][3]

    assert value == expected and isinstance(value, type(expected))


def test_senders_collects_channel_arguments():
    senders = plugin.parse_senders(
        FakeInfluxdb3Local(),
        {
            "senders": "http.whatsapp",
            "http_webhook_url": "https://example.com/hook",
            "twilio_sid": "ACdummy",
            "twilio_token": "dummy",
            "twilio_from_number": "+1234567890",
            "twilio_to_number": "+0987654321",
        },
        "tid",
    )

    assert sorted(senders) == ["http", "whatsapp"]
    assert sorted(senders["whatsapp"]) == [
        "twilio_from_number",
        "twilio_sid",
        "twilio_to_number",
        "twilio_token",
    ]


def test_senders_drops_channel_without_required_argument():
    client = FakeInfluxdb3Local()

    senders = plugin.parse_senders(
        client,
        {
            "senders": "slack.discord",
            "slack_webhook_url": "https://hooks.slack.com/services/TEST",
        },
        "tid",
    )

    assert list(senders) == ["slack"]
    assert any("discord_webhook_url" in m for m in client.messages("warn"))


def test_senders_reject_when_nothing_valid():
    with pytest.raises(Exception, match="No valid senders configured"):
        plugin.parse_senders(FakeInfluxdb3Local(), {"senders": "discord"}, "tid")


@pytest.mark.parametrize("raw, expected", [("10min", 600), ("2h", 7200), ("30s", 30)])
def test_parse_window_accepts_positive(raw, expected):
    assert plugin.parse_window(raw) == timedelta(seconds=expected)


@pytest.mark.parametrize("raw", ["0min", "0s"])
def test_parse_window_rejects_non_positive(raw):
    with pytest.raises(ValueError, match="must be a positive duration"):
        plugin.parse_window(raw)


# --- keys and counters ------------------------------------------------------


def test_row_identifier_includes_aggregation_and_sorted_tags():
    row = {"host": "a", "region": "eu"}

    assert (
        plugin.generate_cache_key("cpu", "temp", "WARN", row, ["region", "host"], "avg")
        == "cpu:temp:avg:WARN:host=a:region=eu"
    )


def test_row_identifier_skips_tag_without_value():
    row = {"host": None}

    assert (
        plugin.generate_cache_key("cpu", "temp", "WARN", row, ["host"])
        == "cpu:temp:WARN"
    )


def test_counter_key_separates_operator_and_threshold():
    row_id = "cpu:temp:WARN:host=a"

    keys = {
        plugin.generate_counter_key(row_id, ">", 30.0),
        plugin.generate_counter_key(row_id, ">", 20.0),
        plugin.generate_counter_key(row_id, ">=", 30.0),
    }

    assert len(keys) == 3
    assert all(key.startswith(row_id) for key in keys)


def test_record_breach_accumulates_then_alerts_and_resets():
    client = FakeInfluxdb3Local()

    assert plugin.record_breach(client, "k", 3) == (False, 1)
    assert plugin.record_breach(client, "k", 3) == (False, 2)
    assert plugin.record_breach(client, "k", 3) == (True, 3)
    assert client.cache.get("k") == "0"


def test_interpolate_notification_text_fills_all_variables():
    text = plugin.interpolate_notification_text(
        "[$level] $table $aggregation($field) $op_sym $compare_val actual=$actual "
        "count=$trigger_count row=$row",
        {
            "level": "WARN",
            "table": "cpu",
            "aggregation": "avg",
            "field": "temp",
            "op_sym": ">",
            "compare_val": 30.0,
            "actual": 40.0,
            "trigger_count": 2,
            "row": "cpu:temp:avg:WARN:host=a",
        },
    )

    assert text == (
        "[WARN] cpu avg(temp) > 30.0 actual=40.0 count=2 row=cpu:temp:avg:WARN:host=a"
    )


def test_interpolate_notification_text_keeps_unknown_variables():
    assert plugin.interpolate_notification_text(
        "$field $missing", {"field": "temp"}
    ) == ("temp $missing")


# --- SQL generation ---------------------------------------------------------


@pytest.mark.parametrize(
    "identifier, expected",
    [("temp", '"temp"'), ('te"mp', '"te""mp"'), ("ho st", '"ho st"')],
)
def test_quote_identifier(identifier, expected):
    assert plugin.quote_identifier(identifier) == expected


def test_interval_literal_rejects_sub_second():
    with pytest.raises(ValueError, match="at least 1 second"):
        plugin.interval_literal(timedelta(milliseconds=500))


def test_build_query_quotes_identifiers_dedupes_aliases_and_orders_bins():
    aggregations = {
        'te"mp': [
            ["first_value", ">", plugin.operator.gt, 30.0, "ERROR"],
            ["first_value", ">", plugin.operator.gt, 10.0, "WARN"],
        ]
    }

    query = plugin.build_query(
        aggregations,
        "cpu",
        ["ho st"],
        plugin.interval_literal(timedelta(minutes=1)),
        datetime(2026, 8, 5, 11, 50, tzinfo=timezone.utc),
        datetime(2026, 8, 5, 12, 0, tzinfo=timezone.utc),
    )

    assert query.count('as "te""mp_first_value"') == 1
    assert 'first_value("te""mp" ORDER BY time)' in query
    assert 'FROM\n            "cpu"' in query
    assert 'GROUP BY\n        _time, "ho st"' in query
    assert query.rstrip().endswith("ORDER BY\n            _time")
    assert "INTERVAL '60 seconds'" in query
    assert "time >= '2026-08-05T11:50:00.000000Z'" in query


# --- configuration ----------------------------------------------------------


def test_load_config_reports_missing_required_argument(plugin_dir):
    client = FakeInfluxdb3Local()

    config = plugin._load_config(
        client, {"senders": "http"}, plugin._WRITES_VALIDATORS, "tid"
    )

    assert config is None
    assert any("measurement is required" in m for m in client.messages("error"))


def test_load_config_rejects_non_toml_path(plugin_dir):
    client = FakeInfluxdb3Local()

    config = plugin._load_config(
        client, {"config_file_path": "conf.txt"}, plugin._WRITES_VALIDATORS, "tid"
    )

    assert config is None
    assert any("expected a .toml file" in m for m in client.messages("error"))


def test_load_config_from_toml_uses_native_structures(plugin_dir):
    (plugin_dir / "conf.toml").write_text(
        'measurement = "cpu"\n'
        'senders = ["http"]\n'
        'http_webhook_url = "https://example.com/hook"\n'
        'field_conditions = [["temp", ">", 30.0, "WARN"]]\n'
    )

    config = plugin._load_config(
        FakeInfluxdb3Local(),
        {"config_file_path": "conf.toml"},
        plugin._WRITES_VALIDATORS,
        "tid",
    )

    assert config["field_conditions"] == [["temp", ">", 30.0, "WARN"]]
    assert config["trigger_count"] == 1
    assert config["notification_path"] == "notify"


def test_blank_token_falls_back_to_environment(monkeypatch, plugin_dir, sent):
    monkeypatch.setenv("INFLUXDB3_AUTH_TOKEN", "env-tok")
    client = FakeInfluxdb3Local()

    plugin.process_writes(
        client,
        batch([{"host": "a", "temp": 40.0}]),
        {**WRITES_ARGS, "influxdb3_auth_token": ""},
    )

    assert sent[0]["headers"]["Authorization"] == "Bearer env-tok"


# --- data write flow --------------------------------------------------------


def test_writes_alerts_on_trigger_count_and_resets_on_non_breach(plugin_dir, sent):
    client = FakeInfluxdb3Local()
    args = {**WRITES_ARGS, "trigger_count": "2"}

    plugin.process_writes(client, batch([{"host": "a", "temp": 40.0}]), args)
    assert sent == []

    plugin.process_writes(client, batch([{"host": "a", "temp": 10.0}]), args)
    plugin.process_writes(client, batch([{"host": "a", "temp": 41.0}]), args)
    assert sent == []

    plugin.process_writes(client, batch([{"host": "a", "temp": 42.0}]), args)
    assert len(sent) == 1
    assert sent[0]["url"] == "http://localhost:8181/api/v3/engine/notify"
    assert (
        sent[0]["payload"]["notification_text"]
        == "[WARN] InfluxDB 3 alert triggered. Condition temp > 30 matched 2 times(42.0) "
        "— matched in row cpu:temp:WARN:host=a."
    )


def test_writes_evaluates_every_condition_and_row(plugin_dir, sent):
    client = FakeInfluxdb3Local()

    plugin.process_writes(
        client,
        batch([{"host": "a", "temp": 60.0}, {"host": "b", "temp": 40.0}]),
        {
            **WRITES_ARGS,
            "field_conditions": "temp>30-WARN:temp>50-ERROR",
            "notification_text": "$level $compare_val $row",
        },
    )

    assert [p["payload"]["notification_text"] for p in sent] == [
        "WARN 30 cpu:temp:WARN:host=a",
        "ERROR 50 cpu:temp:ERROR:host=a",
        "WARN 30 cpu:temp:WARN:host=b",
    ]


def test_writes_warn_when_field_missing_in_row(plugin_dir, sent):
    client = FakeInfluxdb3Local()

    plugin.process_writes(client, batch([{"host": "a", "other": 1.0}]), WRITES_ARGS)

    assert sent == []
    assert any("Field 'temp' not found" in m for m in client.messages("warn"))


def test_writes_respect_port_override_and_notification_path(plugin_dir, sent):
    client = FakeInfluxdb3Local()

    plugin.process_writes(
        client,
        batch([{"host": "a", "temp": 40.0}]),
        {**WRITES_ARGS, "port_override": "8182", "notification_path": "custom/path"},
    )

    assert sent[0]["url"] == "http://localhost:8182/api/v3/engine/custom/path"
    assert sent[0]["payload"]["senders_config"] == {
        "http": {"http_webhook_url": "https://example.com/hook"}
    }


def test_writes_exits_before_loading_config_for_other_tables(plugin_dir, sent):
    client = FakeInfluxdb3Local()

    plugin.process_writes(
        client, batch([{"host": "a", "temp": 40.0}], table="mem"), WRITES_ARGS
    )

    assert sent == []
    assert client.messages() == []


def test_writes_caches_config_between_invocations(plugin_dir, sent):
    client = FakeInfluxdb3Local()

    plugin.process_writes(client, batch([{"host": "a", "temp": 40.0}]), WRITES_ARGS)
    cached = client.cache.get(plugin._WRITES_CONFIG_CACHE_KEY)
    plugin.process_writes(
        client, batch([{"host": "a", "temp": 41.0}]), {"measurement": "cpu"}
    )

    assert cached["measurement"] == "cpu"
    assert (
        client.cache.ttls[plugin._WRITES_CONFIG_CACHE_KEY]
        == plugin._WRITES_CONFIG_TTL_SECONDS
    )
    assert len(sent) == 2


def test_writes_pick_up_tag_added_after_a_tagless_run(plugin_dir, sent):
    client = FakeInfluxdb3Local(tags=())

    plugin.process_writes(client, batch([{"temp": 40.0}]), WRITES_ARGS)
    client.tags = ["host"]
    plugin.process_writes(client, batch([{"host": "a", "temp": 41.0}]), WRITES_ARGS)

    rows = [p["payload"]["notification_text"].rsplit("row ", 1)[1] for p in sent]
    assert rows == ["cpu:temp:WARN.", "cpu:temp:WARN:host=a."]


def test_writes_report_unknown_measurement(plugin_dir, sent):
    client = FakeInfluxdb3Local(tables=("mem",))

    plugin.process_writes(client, batch([{"host": "a", "temp": 40.0}]), WRITES_ARGS)

    assert sent == []
    assert any("not found in database" in m for m in client.messages("error"))


def test_writes_drop_alert_after_failed_delivery(monkeypatch, plugin_dir):
    monkeypatch.setattr(plugin.requests, "post", lambda *a, **kw: FakeResponse(500))
    monkeypatch.setattr(plugin.time, "sleep", lambda seconds: None)
    client = FakeInfluxdb3Local()

    plugin.process_writes(client, batch([{"host": "a", "temp": 40.0}]), WRITES_ARGS)

    assert len([m for m in client.messages("warn") if "Error sending alert" in m]) == 3
    assert any("after 3 attempts" in m for m in client.messages("error"))


# --- scheduled flow ---------------------------------------------------------


def test_scheduled_window_bounds_treat_call_time_as_utc(
    plugin_dir, sent, host_timezone
):
    host_timezone("Europe/Warsaw")
    client = FakeInfluxdb3Local(rows=[])

    plugin.process_scheduled_call(
        client, datetime(2026, 8, 5, 12, 0), {**SCHEDULED_ARGS, "deadman_check": "true"}
    )

    assert any(
        "from 2026-08-05 11:50:00+00:00 to 2026-08-05 12:00:00+00:00" in m
        for m in client.messages("info")
    )


def test_scheduled_deadman_accumulates_then_resets_when_data_returns(plugin_dir, sent):
    client = FakeInfluxdb3Local(rows=[])
    args = {**SCHEDULED_ARGS, "deadman_check": "true", "trigger_count": "2"}

    plugin.process_scheduled_call(client, datetime(2026, 8, 5, 12, 0), args)
    assert sent == []

    plugin.process_scheduled_call(client, datetime(2026, 8, 5, 12, 10), args)
    assert sent[0]["payload"]["notification_text"].startswith(
        "Deadman Alert: No data received"
    )

    client.rows = [
        {"_time": datetime(2026, 8, 5, 12, 15), "host": "a", "temp_avg": 1.0}
    ]
    plugin.process_scheduled_call(client, datetime(2026, 8, 5, 12, 20), args)
    assert client.cache.get("cpu") == "0"
    assert len(sent) == 1


def test_scheduled_threshold_alert_reports_aggregation_and_row(plugin_dir, sent):
    client = FakeInfluxdb3Local(
        rows=[{"_time": datetime(2026, 8, 5, 12, 0), "host": "a", "temp_avg": 40.0}]
    )

    plugin.process_scheduled_call(
        client,
        datetime(2026, 8, 5, 12, 0),
        {**SCHEDULED_ARGS, "field_aggregation_values": "temp:avg@>30-ERROR"},
    )

    assert (
        sent[0]["payload"]["notification_text"]
        == "[ERROR] Threshold Alert on table cpu: avg of temp > 30.0 (actual: 40.0) "
        "— matched in row cpu:temp:avg:ERROR:host=a."
    )


def test_scheduled_uses_custom_template_and_counts_bins_of_one_run(plugin_dir, sent):
    client = FakeInfluxdb3Local(
        rows=[
            {"_time": datetime(2026, 8, 5, 12, 0), "host": "a", "temp_avg": 40.0},
            {"_time": datetime(2026, 8, 5, 12, 1), "host": "a", "temp_avg": 41.0},
        ]
    )

    plugin.process_scheduled_call(
        client,
        datetime(2026, 8, 5, 12, 2),
        {
            **SCHEDULED_ARGS,
            "trigger_count": "2",
            "field_aggregation_values": "temp:avg@>30-ERROR",
            "notification_threshold_text": "S $aggregation $actual $row",
        },
    )

    assert [p["payload"]["notification_text"] for p in sent] == [
        "S avg 41.0 cpu:temp:avg:ERROR:host=a"
    ]


def test_scheduled_skips_condition_when_aggregate_column_missing(plugin_dir, sent):
    client = FakeInfluxdb3Local(
        rows=[{"_time": datetime(2026, 8, 5, 12, 0), "host": "a", "temp_avg": 40.0}]
    )

    plugin.process_scheduled_call(
        client,
        datetime(2026, 8, 5, 12, 0),
        {**SCHEDULED_ARGS, "field_aggregation_values": "temp:max@>30-ERROR"},
    )

    assert sent == []
    assert any("'temp_max' not found" in m for m in client.messages("warn"))


def test_scheduled_requires_conditions_or_deadman(plugin_dir, sent):
    client = FakeInfluxdb3Local(rows=[])

    plugin.process_scheduled_call(client, datetime(2026, 8, 5, 12, 0), SCHEDULED_ARGS)

    assert sent == []
    assert any("deadman_check to True" in m for m in client.messages("error"))
