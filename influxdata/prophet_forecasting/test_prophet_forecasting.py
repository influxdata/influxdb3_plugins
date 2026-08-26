"""Unit and integration tests for the prophet_forecasting plugin.

Prophet is stubbed: these tests pin the plugin's own logic (configuration, tag
filters, query building, forecast grid, validation, writing, alerting), not the
model's math, and the suite stays fast without the heavy dependency.
"""

import ast
import json
import os
import sys
import types
from collections import namedtuple
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

CALL_TIME = datetime(2026, 8, 24, 12, 0)
HOUR = timedelta(hours=1)


class FakeProphet:
    """Stand-in for prophet.Prophet that records how it was used."""

    instances: list = []
    predict_values = None

    def __init__(
        self, seasonality_mode=None, changepoint_prior_scale=None, changepoints=None
    ):
        self.seasonality_mode = seasonality_mode
        self.changepoint_prior_scale = changepoint_prior_scale
        self.changepoints = changepoints
        self.holidays = None
        self.countries: list = []
        self.fitted = None
        self.predicted_ds = None
        self.loaded = False
        FakeProphet.instances.append(self)

    def add_country_holidays(self, country_name):
        self.countries.append(country_name)

    def fit(self, df):
        self.fitted = df.copy()

    def predict(self, future):
        self.predicted_ds = list(future["ds"])
        out = future.copy()
        if callable(FakeProphet.predict_values):
            out["yhat"] = [
                float(value) for value in FakeProphet.predict_values(out["ds"])
            ]
        else:
            out["yhat"] = 10.0
        out["yhat_lower"] = out["yhat"] - 1.0
        out["yhat_upper"] = out["yhat"] + 1.0
        return out


def _model_from_json(text):
    model = FakeProphet()
    model.loaded = True
    model.source = text
    return model


prophet_stub = types.ModuleType("prophet")
prophet_stub.Prophet = FakeProphet
serialize_stub = types.ModuleType("prophet.serialize")
serialize_stub.model_to_json = lambda model: '{"stub": true}'
serialize_stub.model_from_json = _model_from_json
prophet_stub.serialize = serialize_stub
sys.modules["prophet"] = prophet_stub
sys.modules["prophet.serialize"] = serialize_stub

sys.path.insert(0, os.path.dirname(__file__))
import prophet_forecasting as pf  # noqa: E402


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeCache:
    def __init__(self):
        self.store = {}

    def get(self, key, default=None, use_global=None):
        return self.store.get(key, default)

    def put(self, key, value, ttl=None, use_global=None):
        self.store[key] = value

    def delete(self, key, use_global=None):
        return self.store.pop(key, None) is not None


class FakeLineBuilder:
    def __init__(self, measurement):
        self.measurement = measurement
        self.tags: dict = {}
        self.fields: dict = {}
        self.timestamp = None

    def tag(self, key, value):
        self.tags[key] = value
        return self

    def int64_field(self, key, value):
        self.fields[key] = int(value)
        return self

    def uint64_field(self, key, value):
        self.fields[key] = int(value)
        return self

    def float64_field(self, key, value):
        self.fields[key] = float(value)
        return self

    def bool_field(self, key, value):
        self.fields[key] = bool(value)
        return self

    def string_field(self, key, value):
        self.fields[key] = str(value)
        return self

    def time_ns(self, timestamp_ns):
        self.timestamp = timestamp_ns
        return self

    def build(self):
        line = self.measurement
        if self.tags:
            line += "," + ",".join(f"{key}={value}" for key, value in self.tags.items())
        line += " " + ",".join(
            f"{key}={_encode_field(value)}" for key, value in self.fields.items()
        )
        return f"{line} {self.timestamp}"


def _encode_field(value):
    if isinstance(value, str):
        return f'"{value}"'
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return f"{value}i"
    return repr(value)


def _decode_field(raw):
    if raw.startswith('"'):
        return raw[1:-1]
    if raw in ("true", "false"):
        return raw == "true"
    if raw.endswith("i"):
        return int(raw[:-1])
    return float(raw)


Record = namedtuple("Record", ["measurement", "tags", "fields", "timestamp"])


def parse_line_protocol(line):
    """Parse one line-protocol record (sufficient for this plugin's output)."""
    head, fields, timestamp = line.rsplit(" ", 2)
    parts = head.split(",")
    return Record(
        parts[0],
        dict(pair.split("=", 1) for pair in parts[1:]),
        {
            key: _decode_field(value)
            for key, value in (pair.split("=", 1) for pair in fields.split(","))
        },
        int(timestamp),
    )


class FakeLocal:
    def __init__(self, rows=None, write_error=None):
        self.cache = FakeCache()
        self.rows = rows if rows is not None else []
        self.write_error = write_error
        self.queries: list = []
        self.writes: list = []  # (database, LineBuilder) per point
        self.infos: list = []
        self.warns: list = []
        self.errors: list = []

    def query(self, query, params=None):
        self.queries.append((query, params))
        start = pd.Timestamp(params["start_time"]).tz_convert("UTC").tz_localize(None)
        end = pd.Timestamp(params["end_time"]).tz_convert("UTC").tz_localize(None)
        # tag filters live in the SQL text, asserted separately; here only the
        # time window narrows the rows
        return [
            row
            for row in self.rows
            if start <= pd.Timestamp(row["time"], unit="ns") < end
        ]

    def write_to_db(self, database, batch):
        if self.write_error is not None:
            raise self.write_error
        for line in batch.build().split("\n"):
            self.writes.append((database, parse_line_protocol(line)))

    def write(self, batch):
        raise AssertionError("target database is always explicit")

    def info(self, *args):
        self.infos.append(" ".join(str(arg) for arg in args))

    def warn(self, *args):
        self.warns.append(" ".join(str(arg) for arg in args))

    def error(self, *args):
        self.errors.append(" ".join(str(arg) for arg in args))


class FakeResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {"results": "ok"}


class FakeRequests:
    """Stand-in for the `requests` module inside the plugin."""

    RequestException = Exception

    def __init__(self, error=None):
        self.error = error
        self.calls: list = []

    def post(self, url, headers=None, data=None, timeout=None):
        self.calls.append({"url": url, "headers": headers, "body": json.loads(data)})
        if self.error is not None:
            raise self.error
        return FakeResponse()


@pytest.fixture(autouse=True)
def plugin_env(monkeypatch, tmp_path):
    FakeProphet.instances = []
    FakeProphet.predict_values = None
    monkeypatch.setattr(pf, "LineBuilder", FakeLineBuilder, raising=False)
    monkeypatch.setattr(pf.time, "sleep", lambda _: None)
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    monkeypatch.delenv("INFLUXDB3_AUTH_TOKEN", raising=False)
    yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_ARGS = {
    "measurement": "temperature",
    "field": "value",
    "window": "2d",
    "forecast_horizont": "6h",
    "tag_values": "region:us-west",
    "target_measurement": "temperature_forecast",
    "model_mode": "train",
    "unique_suffix": "v1",
    "inferred_freq": "1h",
}


def rows(count=48, step=HOUR, end=CALL_TIME, value=lambda index: 20.0 + index % 5):
    """Regularly spaced source rows ending one step before `end`."""
    return [
        {
            "time": int(pd.Timestamp(end - step * (count - index)).value),
            "value": value(index),
            "region": "us-west",
        }
        for index in range(count)
    ]


def written_times(local):
    return [pd.Timestamp(record.timestamp) for _, record in local.writes]


def run_scheduled(local, **overrides):
    pf.process_scheduled_call(local, CALL_TIME, dict(BASE_ARGS, **overrides))
    return local


# ---------------------------------------------------------------------------
# Parsing and configuration
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("30s", timedelta(seconds=30)),
        ("500ms", timedelta(milliseconds=500)),
        (" 2 h ", timedelta(hours=2)),
        ("1m", timedelta(days=30)),
        ("2q", timedelta(days=182)),
        ("1y", timedelta(days=365)),
    ],
)
def test_parse_interval_accepts_fixed_and_calendar_units(raw, expected):
    assert pf.parse_interval(raw) == expected


@pytest.mark.parametrize("raw", ["6hours", "abc", "", "-1h"])
def test_parse_interval_rejects_garbage(raw):
    with pytest.raises(ValueError):
        pf.parse_interval(raw)


@pytest.mark.parametrize(
    "raw", ["../../etc/passwd", "a/b", "..", "", ".hidden", "x" * 65]
)
def test_parse_unique_suffix_rejects_unsafe_names(raw):
    with pytest.raises(ValueError):
        pf.parse_unique_suffix(raw)


def test_parse_unique_suffix_keeps_usual_versions():
    assert pf.parse_unique_suffix(" 20250619_v1.2-b ") == "20250619_v1.2-b"


def test_parse_tag_values_accepts_mapping_and_string():
    local = FakeLocal()
    assert pf.parse_tag_values(local, {"region": "us-west"}, "t") == {
        "region": "us-west"
    }
    assert pf.parse_tag_values(local, "region:us-west.device:sensor1", "t") == {
        "region": "us-west",
        "device": "sensor1",
    }
    assert pf.parse_tag_values(local, "region:us-west.broken", "t") == {
        "region": "us-west"
    }
    assert any("broken" in message for message in local.warns)


def test_parse_date_list_skips_invalid_entries():
    local = FakeLocal()
    assert pf.parse_date_list(
        local, "2025-01-01 nope 2025-06-01", "changepoints", "t"
    ) == [
        "2025-01-01",
        "2025-06-01",
    ]
    assert any("nope" in message for message in local.warns)
    assert pf.parse_date_list(local, "", "changepoints", "t") is None


def test_missing_required_argument_is_reported():
    local = FakeLocal(rows())
    args = dict(BASE_ARGS)
    args.pop("unique_suffix")
    pf.process_scheduled_call(local, CALL_TIME, args)
    assert local.errors and "unique_suffix is required" in local.errors[-1]
    assert not local.writes


def test_toml_config_replaces_trigger_arguments(tmp_path):
    (tmp_path / "cfg.toml").write_text(
        "\n".join(
            [
                'measurement = "temperature"',
                'field = "value"',
                'window = "2d"',
                'forecast_horizont = "6h"',
                'tag_values = { region = "us-west" }',
                'target_measurement = "temperature_forecast"',
                'model_mode = "train"',
                'unique_suffix = "toml_v1"',
                'inferred_freq = "1h"',
                'holiday_country_names = ["US"]',
                'target_database = "forecast_db"',
            ]
        )
    )
    local = FakeLocal(rows())
    pf.process_scheduled_call(local, CALL_TIME, {"config_file_path": "cfg.toml"})

    assert [database for database, _ in local.writes] == ["forecast_db"] * 6
    assert local.writes[0][1].tags["model_version"] == "toml_v1"
    assert FakeProphet.instances[0].countries == ["US"]


def test_non_toml_config_path_is_rejected():
    local = FakeLocal(rows())
    pf.process_scheduled_call(local, CALL_TIME, {"config_file_path": "cfg.txt"})
    assert "expected a .toml file" in local.errors[-1]


# ---------------------------------------------------------------------------
# Query building
# ---------------------------------------------------------------------------


def test_query_uses_bound_parameters_and_quoted_identifiers():
    local = run_scheduled(FakeLocal(rows()), tag_values="region:us-west.device:a'b")
    query, params = local.queries[0]

    assert 'SELECT time, "value" FROM "temperature"' in query
    assert '"region" = $tag0' in query and '"device" = $tag1' in query
    assert params["tag0"] == "us-west" and params["tag1"] == "a'b"
    assert (
        params["start_time"]
        == (CALL_TIME - timedelta(days=2)).replace(tzinfo=timezone.utc).isoformat()
    )
    assert params["end_time"] == CALL_TIME.replace(tzinfo=timezone.utc).isoformat()


def test_non_numeric_values_are_dropped_with_a_warning():
    source = rows(count=5)
    source[0]["value"] = None
    local = run_scheduled(FakeLocal(source), window="5h")

    assert any("Dropping 1 rows" in message for message in local.warns)
    assert local.writes


def test_empty_source_window_is_reported():
    local = run_scheduled(FakeLocal([]))
    assert "No data found from" in local.errors[-1]
    assert not local.writes


# ---------------------------------------------------------------------------
# Forecast grid
# ---------------------------------------------------------------------------


def test_grid_covers_validation_window_and_horizon():
    local = run_scheduled(FakeLocal(rows()), validation_window="6h")
    grid = FakeProphet.instances[0].predicted_ds

    assert grid[0] == pd.Timestamp("2026-08-24 06:00")
    assert grid[-1] == pd.Timestamp("2026-08-24 17:00")
    assert written_times(local) == list(
        pd.date_range("2026-08-24 12:00", periods=6, freq="1h")
    )


def test_lagging_data_still_covers_the_whole_horizon():
    local = run_scheduled(FakeLocal(rows(end=CALL_TIME - timedelta(hours=3))))
    grid = FakeProphet.instances[0].predicted_ds

    assert grid[0] == pd.Timestamp("2026-08-24 09:00")
    assert written_times(local) == list(
        pd.date_range("2026-08-24 12:00", periods=6, freq="1h")
    )


def test_saved_model_forecasts_the_requested_range(tmp_path):
    model_path = tmp_path / pf.MODEL_DIR_NAME / "prophet_model_saved.json"
    model_path.parent.mkdir(parents=True)
    model_path.write_text('{"trained": "long ago"}')

    local = run_scheduled(
        FakeLocal(rows()), model_mode="predict", unique_suffix="saved"
    )
    model = FakeProphet.instances[0]

    assert model.loaded and model.fitted is None
    assert model.predicted_ds[0] == pd.Timestamp("2026-08-24 12:00")
    assert written_times(local) == list(
        pd.date_range("2026-08-24 12:00", periods=6, freq="1h")
    )


def test_predict_mode_trains_and_saves_when_no_model_exists(tmp_path):
    local = run_scheduled(
        FakeLocal(rows()), model_mode="predict", unique_suffix="fresh"
    )
    model_path = tmp_path / pf.MODEL_DIR_NAME / "prophet_model_fresh.json"

    assert model_path.read_text() == '{"stub": true}'
    assert not list(model_path.parent.glob("*.tmp"))
    assert FakeProphet.instances[0].fitted is not None
    assert local.writes


def test_horizon_shorter_than_one_step_is_rejected():
    local = run_scheduled(FakeLocal(rows()), forecast_horizont="30min")
    assert "shorter than one '1h' step" in local.errors[-1]
    assert not local.writes


def test_forecast_point_cap_stops_the_run():
    local = run_scheduled(FakeLocal(rows()), max_forecast_points="3")
    assert "above max_forecast_points (3)" in local.errors[-1]
    assert not local.writes


def test_unknown_frequency_asks_for_the_argument():
    source = [
        {
            "time": int(pd.Timestamp(CALL_TIME - HOUR * step).value),
            "value": 1.0,
            "region": "us-west",
        }
        for step in (10, 7, 3)
    ]
    local = run_scheduled(FakeLocal(source), inferred_freq="")
    assert "Unable to infer frequency" in local.errors[-1]


def test_calendar_frequency_is_supported():
    # a daily grid: pandas 3 treats "D" as a calendar offset with no fixed duration
    daily = [
        {
            "time": int(pd.Timestamp(CALL_TIME - timedelta(days=count)).value),
            "value": 20.0 + count % 3,
            "region": "us-west",
        }
        for count in range(30, 0, -1)
    ]
    local = run_scheduled(
        FakeLocal(daily), window="30d", forecast_horizont="5d", inferred_freq="D"
    )

    assert pf.fixed_step("h") == HOUR
    assert written_times(local) == list(pd.date_range(CALL_TIME, periods=5, freq="D"))
    assert any("frequency: D" in message for message in local.infos)


def test_calendar_frequency_tolerance_follows_the_grid():
    grid = pd.date_range("2026-08-01", periods=4, freq="MS")
    assert pf.forecast_tolerance("MS", grid) == timedelta(days=15, hours=12)
    assert pf.forecast_tolerance("MS", grid[:1]) == timedelta(days=15, hours=12)


def test_wide_calendar_horizon_is_measured_in_steps_not_hours():
    local = run_scheduled(
        FakeLocal(rows()), inferred_freq="MS", forecast_horizont="30y"
    )
    written = written_times(local)

    assert not local.errors
    assert len(written) == 360
    assert {moment.day for moment in written} == {1}


@pytest.mark.parametrize("freq", ["0h", "-1h", "0D", "-1MS"])
def test_frequency_that_does_not_advance_is_rejected(freq):
    local = run_scheduled(FakeLocal(rows()), inferred_freq=freq)
    assert any("does not move time forward" in message for message in local.errors)
    assert not local.writes


def test_horizon_shorter_than_a_calendar_step_is_reported():
    # daily grid whose next point lands 12h after the horizon ends
    daily = [
        {
            "time": int(
                pd.Timestamp(CALL_TIME - timedelta(minutes=1, days=count)).value
            ),
            "value": 20.0 + count % 3,
            "region": "us-west",
        }
        for count in range(29, -1, -1)
    ]
    local = run_scheduled(
        FakeLocal(daily), window="30d", forecast_horizont="6h", inferred_freq="D"
    )

    assert "not before the end of the horizon" in local.errors[-1]
    assert not local.writes


@pytest.mark.parametrize("raw", ["nan", "inf", "-inf", "0"])
def test_non_finite_prior_scale_is_rejected(raw):
    with pytest.raises(ValueError):
        pf.parse_prior_scale(raw)


def test_tag_filter_with_several_values_is_skipped():
    local = FakeLocal()
    assert pf.parse_tag_values(local, {"region": ["a", "b"], "device": "x"}, "t") == {
        "device": "x"
    }
    assert any("one value per tag" in message for message in local.warns)


def test_holidays_need_both_dates_and_names():
    local = run_scheduled(FakeLocal(rows()), holiday_date_list="2026-08-20")
    assert any("holiday_names is not set" in message for message in local.warns)

    local = run_scheduled(FakeLocal(rows()), holiday_names="Only a name")
    assert any("holiday_date_list is not set" in message for message in local.warns)


def test_multiple_holiday_countries_warn_and_keep_the_first():
    local = run_scheduled(FakeLocal(rows()), holiday_country_names="US.UK")

    assert any("one country" in message for message in local.warns)
    assert FakeProphet.instances[0].countries == ["US"]


def test_alert_without_validation_window_warns():
    local = run_scheduled(FakeLocal(rows()), is_sending_alert="true", senders="slack")
    assert any(
        "is_sending_alert has no effect without validation_window" in message
        for message in local.warns
    )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validation_frames(actual_hours, forecast_hours):
    actual = pd.DataFrame(
        {
            "ds": [
                pd.Timestamp("2026-08-24 06:00") + HOUR * hour for hour in actual_hours
            ],
            "y": [float(hour) for hour in actual_hours],
        }
    )
    forecast = pd.DataFrame(
        {
            "ds": [
                pd.Timestamp("2026-08-24 06:00") + HOUR * hour
                for hour in forecast_hours
            ],
            "yhat": [float(hour) for hour in forecast_hours],
        }
    )
    return actual, forecast


def test_position_alignment_pairs_by_order_and_nearest_by_time():
    # actual points 2 and 3 are missing, so pairing by order compares 4 with 2
    actual, forecast = validation_frames([0, 1, 4, 5], [0, 1, 2, 3, 4, 5])
    local = FakeLocal()

    assert not pf.validate_forecast(
        local, actual, forecast, 0.01, "position", HOUR / 2, "t"
    )
    assert pf.validate_forecast(local, actual, forecast, 0.01, "nearest", HOUR / 2, "t")
    assert any("4 of 4 actual points matched within 0:30:00" in m for m in local.infos)


def test_nearest_alignment_tolerates_ingest_jitter():
    actual, forecast = validation_frames([1, 2, 3], [1, 2, 3])
    actual["ds"] = actual["ds"] + pd.Timedelta(seconds=17)
    local = FakeLocal()

    assert pf.validate_forecast(local, actual, forecast, 0.01, "nearest", HOUR / 2, "t")


def test_nearest_alignment_fails_when_nothing_overlaps():
    actual, forecast = validation_frames([0, 1], [40, 41])
    local = FakeLocal()

    assert not pf.validate_forecast(
        local, actual, forecast, 0.01, "nearest", HOUR / 2, "t"
    )
    assert any("treating validation as failed" in message for message in local.warns)


def test_all_zero_validation_window_fails_validation():
    actual, forecast = validation_frames([1, 2], [1, 2])
    actual["y"] = 0.0
    local = FakeLocal()

    assert not pf.validate_forecast(
        local, actual, forecast, 0.01, "position", HOUR / 2, "t"
    )
    assert any("are zero" in message for message in local.warns)


def test_nan_msre_fails_validation():
    actual, forecast = validation_frames([1, 2], [1, 2])
    forecast["yhat"] = float("nan")
    local = FakeLocal()

    assert not pf.validate_forecast(
        local, actual, forecast, 0.01, "position", HOUR / 2, "t"
    )
    assert any("not a number" in message for message in local.warns)


def test_validation_failure_withholds_the_forecast():
    FakeProphet.predict_values = lambda ds: [500.0] * len(ds)
    local = run_scheduled(
        FakeLocal(rows()), validation_window="6h", msre_threshold="0.01"
    )

    assert "Validation failed" in local.errors[-1]
    assert not local.writes


def test_missing_validation_data_fails_validation():
    source = [
        row
        for row in rows()
        if pd.Timestamp(row["time"], unit="ns") < pd.Timestamp("2026-08-24 06:00")
    ]
    local = run_scheduled(
        FakeLocal(source), validation_window="6h", msre_threshold="0.01"
    )

    assert "No data found for validation window" in local.errors[-1]
    assert not local.writes


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------


def test_points_carry_tags_fields_and_default_database():
    local = run_scheduled(FakeLocal(rows()), tag_values="region:us-west.device:sensor1")
    database, record = local.writes[0]

    assert database == pf.DEFAULT_TARGET_DATABASE
    assert record.measurement == "temperature_forecast"
    assert record.tags == {
        "model_version": "v1",
        "region": "us-west",
        "device": "sensor1",
    }
    assert record.fields["forecast"] == 10.0
    assert record.fields["yhat_lower"] == 9.0
    assert record.fields["yhat_upper"] == 11.0
    assert (
        record.fields["run_time"] == CALL_TIME.replace(tzinfo=timezone.utc).isoformat()
    )


def test_non_finite_points_are_skipped():
    FakeProphet.predict_values = lambda ds: [
        float("nan") if index == 0 else 10.0 for index in range(len(ds))
    ]
    local = run_scheduled(FakeLocal(rows()))

    assert len(local.writes) == 5
    assert any("1 forecast points with non-finite" in m for m in local.warns)


def test_write_failure_is_reported():
    local = FakeLocal(rows(), write_error=RuntimeError("table conflict"))
    run_scheduled(local)
    assert "table conflict" in local.errors[-1]
    assert not local.writes


# ---------------------------------------------------------------------------
# Alerting
# ---------------------------------------------------------------------------


def test_alert_reports_the_validated_window(monkeypatch):
    FakeProphet.predict_values = lambda ds: [500.0] * len(ds)
    stub = FakeRequests()
    monkeypatch.setattr(pf, "requests", stub)
    monkeypatch.setenv("INFLUXDB3_AUTH_TOKEN", "secret-token")

    local = run_scheduled(
        FakeLocal(rows()),
        validation_window="6h",
        msre_threshold="0.01",
        is_sending_alert="true",
        senders="slack",
        slack_webhook_url="https://hooks.slack.com/services/x",
        port_override="8183",
        notification_text="$measurement.$field from $start_time to $end_time",
    )

    assert not local.writes
    call = stub.calls[0]
    assert call["url"] == "http://localhost:8183/api/v3/engine/notify"
    assert call["headers"]["Authorization"] == "Bearer secret-token"
    assert call["body"]["notification_text"] == (
        "temperature.value from 2026-08-24T06:00:00+00:00 to 2026-08-24T12:00:00+00:00"
    )
    assert call["body"]["senders_config"] == {
        "slack": {"slack_webhook_url": "https://hooks.slack.com/services/x"}
    }


def test_alert_without_valid_senders_is_logged(monkeypatch):
    FakeProphet.predict_values = lambda ds: [500.0] * len(ds)
    stub = FakeRequests()
    monkeypatch.setattr(pf, "requests", stub)
    monkeypatch.setenv("INFLUXDB3_AUTH_TOKEN", "secret-token")

    local = run_scheduled(
        FakeLocal(rows()),
        validation_window="6h",
        msre_threshold="0.01",
        is_sending_alert="true",
        senders="carrier-pigeon",
    )

    assert not stub.calls
    assert (
        "Failed to send notification: No valid senders configured" in local.errors[-1]
    )


def test_configuration_values_stay_out_of_the_logs(monkeypatch):
    monkeypatch.setenv("INFLUXDB3_AUTH_TOKEN", "secret-token")
    local = run_scheduled(
        FakeLocal(rows()),
        senders="slack",
        slack_webhook_url="https://hooks.slack.com/services/x",
    )

    logged = " ".join(local.infos + local.warns + local.errors)
    assert "secret-token" not in logged
    assert "hooks.slack.com" not in logged


# ---------------------------------------------------------------------------
# HTTP entry point
# ---------------------------------------------------------------------------


def http_body(**overrides):
    body = {
        "measurement": "temperature",
        "field": "value",
        "forecast_horizont": "6h",
        "tag_values": {"region": "us-west"},
        "target_measurement": "temperature_forecast",
        "unique_suffix": "http_v1",
        "start_time": (CALL_TIME - timedelta(days=2))
        .replace(tzinfo=timezone.utc)
        .isoformat(),
        "end_time": CALL_TIME.replace(tzinfo=timezone.utc).isoformat(),
        "inferred_freq": "1h",
    }
    body.update(overrides)
    return body


def test_http_request_writes_the_forecast():
    local = FakeLocal(rows())
    response = pf.process_request(local, {}, {}, json.dumps(http_body()))

    assert "Forecast written to temperature_forecast" in response["message"]
    assert written_times(local) == list(
        pd.date_range("2026-08-24 12:00", periods=6, freq="1h")
    )


@pytest.mark.parametrize(
    "body, expected",
    [
        ({"unique_suffix": "../../etc/passwd"}, "Invalid unique_suffix"),
        ({"start_time": "2026-08-22T12:00:00"}, "must include timezone info"),
        ({"end_time": "2026-08-20T12:00:00+00:00"}, "must be earlier than end_time"),
        ({"validation_window": "2d"}, "Empty training window"),
    ],
)
def test_http_request_rejections(body, expected):
    local = FakeLocal(rows())
    response = pf.process_request(local, {}, {}, json.dumps(http_body(**body)))

    assert expected in response["message"]
    assert not local.writes


def test_http_null_means_not_set_and_body_is_not_a_file_path():
    local = FakeLocal(rows())
    body = http_body(validation_window=None, config_file_path="cfg.toml")
    response = pf.process_request(local, {}, {}, json.dumps(body))

    assert "Forecast written" in response["message"]


@pytest.mark.parametrize(
    "request_body, expected",
    [
        ("", "No request body provided"),
        ("not json", "Expecting value"),
        ('["a"]', "must be a JSON object"),
    ],
)
def test_http_bad_bodies_are_reported(request_body, expected):
    local = FakeLocal(rows())
    response = pf.process_request(local, {}, {}, request_body)

    assert expected in response["message"]
    assert not local.writes


def test_http_save_mode_loads_the_stored_model(tmp_path):
    model_path = tmp_path / pf.MODEL_DIR_NAME / "prophet_model_http_v1.json"
    model_path.parent.mkdir(parents=True)
    model_path.write_text('{"trained": "long ago"}')

    local = FakeLocal(rows())
    pf.process_request(local, {}, {}, json.dumps(http_body(save_mode=True)))

    assert FakeProphet.instances[0].loaded
    assert written_times(local) == list(
        pd.date_range("2026-08-24 12:00", periods=6, freq="1h")
    )


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


def test_metadata_docstring_is_valid_json():
    source = open(pf.__file__).read()
    metadata = json.loads(ast.get_docstring(ast.parse(source)))

    assert metadata["plugin_type"] == ["scheduled", "http"]
    for section in ("scheduled_args_config", "http_body_config"):
        names = [entry["name"] for entry in metadata[section]]
        assert len(names) == len(set(names))
        for entry in metadata[section]:
            assert set(entry) == {"name", "example", "description", "required"}
