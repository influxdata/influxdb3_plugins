"""Unit and integration tests for the synthefy_forecasting plugin."""

import json
import os
import sys
from collections import namedtuple

import pandas as pd
import pytest
from influxdata_plugin_utils import write as utils_write

sys.path.insert(0, os.path.dirname(__file__))
import synthefy_forecasting as sf

TAG_TYPE = "Dictionary(Int32, Utf8)"

COLUMNS = {
    "time": "Timestamp(Nanosecond, None)",
    "value": "Float64",
    "humidity": "Float64",
    "pressure": "Float64",
    "room": TAG_TYPE,
    "site": TAG_TYPE,
}


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
        self.tags = []
        self.fields = {}
        self.timestamp = None

    def tag(self, key, value):
        self.tags.append((key, value))
        return self

    def int64_field(self, key, value):
        self.fields[key] = f"{value}i"
        return self

    def uint64_field(self, key, value):
        self.fields[key] = f"{value}u"
        return self

    def float64_field(self, key, value):
        self.fields[key] = f"{int(value)}.0" if value % 1 == 0 else str(value)
        return self

    def bool_field(self, key, value):
        self.fields[key] = "true" if value else "false"
        return self

    def string_field(self, key, value):
        self.fields[key] = f'"{value}"'
        return self

    def time_ns(self, timestamp_ns):
        self.timestamp = timestamp_ns
        return self

    def build(self):
        line = self.measurement
        if self.tags:
            line += "," + ",".join(f"{k}={v}" for k, v in self.tags)
        line += " " + ",".join(f"{k}={v}" for k, v in self.fields.items())
        if self.timestamp is not None:
            line += f" {self.timestamp}"
        return line


Record = namedtuple("Record", ["measurement", "tags", "fields", "timestamp"])


def _parse_field(raw):
    if raw.startswith('"'):
        return raw[1:-1]
    if raw in ("true", "false"):
        return raw == "true"
    if raw[-1] in ("i", "u"):
        return int(raw[:-1])
    return float(raw)


def _parse_lp(line):
    """Parse one line-protocol record (sufficient for this plugin's output)."""
    head, fields_str, ts = line.rsplit(" ", 2)
    parts = head.split(",")
    tags = dict(kv.split("=", 1) for kv in parts[1:])
    fields = {k: _parse_field(v) for k, v in (kv.split("=", 1) for kv in fields_str.split(","))}
    return Record(parts[0], tags, fields, int(ts))


class FakeLocal:
    def __init__(self, columns=None, rows=None, write_failures=0):
        self.cache = FakeCache()
        self.columns = COLUMNS if columns is None else columns
        self.rows = [] if rows is None else rows
        self.write_failures = write_failures
        self.queries = []
        self.writes = []  # (db_name | None, Record) per emitted point
        self.infos = []
        self.warns = []
        self.errors = []

    def query(self, query, args=None):
        self.queries.append((query, args))
        if "information_schema.columns" not in query:
            return self.rows
        rows = [{"column_name": n, "data_type": t} for n, t in self.columns.items()]
        wanted = (args or {}).get("data_type")
        return [r for r in rows if wanted is None or r["data_type"] == wanted]

    def _record_batch(self, db_name, batch):
        # The plugin hands a BatchLines; the engine calls build(). Exercise that
        # path, then expand back to one Record per line for assertions.
        if self.write_failures:
            self.write_failures -= 1
            raise RuntimeError("simulated write failure")
        for lp in batch.build().split("\n"):
            self.writes.append((db_name, _parse_lp(lp)))

    def info(self, *args):
        self.infos.append(" ".join(str(a) for a in args))

    def warn(self, *args):
        self.warns.append(" ".join(str(a) for a in args))

    def error(self, *args):
        self.errors.append(" ".join(str(a) for a in args))

    def write(self, *args):
        raise AssertionError("buffered write must not be used")

    def write_sync(self, batch, no_sync=False):
        self._record_batch(None, batch)

    def write_sync_to_db(self, db_name, batch, no_sync=False):
        self._record_batch(db_name, batch)


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class FakeRequests:
    """Stand-in for the `requests` module inside the plugin."""

    def __init__(self, payload=None, error=None):
        self.payload = payload
        self.error = error
        self.calls = []

    def post(self, url, json=None, headers=None, timeout=None):
        self.calls.append({"url": url, "body": json, "headers": headers, "timeout": timeout})
        if self.error is not None:
            raise self.error
        return FakeResponse(self.payload)


@pytest.fixture(autouse=True)
def _plugin_env(monkeypatch):
    monkeypatch.setattr(sf, "LineBuilder", FakeLineBuilder, raising=False)
    monkeypatch.setattr(utils_write.time, "sleep", lambda _: None)
    yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

START_NS = 1_700_000_000_000_000_000
HOUR_NS = 3_600_000_000_000


def history_rows(count=5, field="value", step_ns=HOUR_NS, extra=None):
    rows = []
    for i in range(count):
        row = {"time": START_NS + i * step_ns, field: float(i)}
        row.update(extra or {})
        rows.append(row)
    return rows


def forecast_response(timestamps, values, quantiles=None, sample_id="value"):
    payload = {"sample_id": sample_id, "timestamps": timestamps, "values": values}
    if quantiles is not None:
        payload["quantiles"] = quantiles
    return {"forecasts": [[payload]]}


def run(local, body=None, args=None, headers=None, requests_stub=None, monkeypatch=None):
    if requests_stub is not None:
        monkeypatch.setattr(sf, "requests", requests_stub)
    return sf.process_request(
        local,
        {},
        {"X-Synthefy-Api-Key": "k"} if headers is None else headers,
        json.dumps(body or {}),
        args or {},
    )


# ---------------------------------------------------------------------------
# M1 — plugin metadata
# ---------------------------------------------------------------------------


def test_docstring_header_is_valid_json_with_expected_args():
    header = json.loads(sf.__doc__)
    assert header["plugin_type"] == ["http"]
    names = [arg["name"] for arg in header["http_args_config"]]
    assert set(names) == {
        "measurement", "field", "tags", "time_range", "forecast_horizon", "model",
        "output_measurement", "metadata_fields", "max_forecast_points", "database",
    }
    # every argument is also accepted in the request body, in the same order
    assert [arg["name"] for arg in header["http_body_config"]] == names
    assert [h["name"] for h in header["http_headers_config"]] == [sf.API_KEY_HEADER]
    for section in ("http_args_config", "http_body_config", "http_headers_config"):
        for entry in header[section]:
            assert set(entry) == {"name", "example", "description", "required"}


# ---------------------------------------------------------------------------
# M2 — configuration
# ---------------------------------------------------------------------------


def test_config_defaults():
    cfg = sf._load_config({"measurement": "t"}, {})
    assert cfg["field"] == "value"
    assert cfg["time_range"] == "30d"
    assert cfg["forecast_horizon"] == "7d"
    assert cfg["model"] == "sfm-tabular"
    assert cfg["output_measurement"] == ""
    assert cfg["database"] == ""
    assert cfg["max_forecast_points"] == sf.DEFAULT_MAX_FORECAST_POINTS


def test_config_body_overrides_args_and_null_falls_back():
    cfg = sf._load_config({"measurement": "t", "model": "from-args"}, {"model": "from-body"})
    assert cfg["model"] == "from-body"
    cfg = sf._load_config({"measurement": "t", "model": "from-args"}, {"model": None})
    assert cfg["model"] == "from-args"


def test_config_never_reads_a_toml_file():
    cfg = sf._load_config({"measurement": "t", "config_file_path": "/nonexistent.toml"}, {})
    assert "config_file_path" not in cfg
    assert cfg["measurement"] == "t"


def test_config_leaves_dynaconf_tokens_literal():
    cfg = sf._load_config({"measurement": "@format {env[HOME]}"}, {})
    assert cfg["measurement"] == "@format {env[HOME]}"


@pytest.mark.parametrize(
    "value, fragment",
    [("0", "below minimum 1"), ("-5", "below minimum 1"), ("junk", "Invalid integer")],
)
def test_config_rejects_bad_max_forecast_points(value, fragment):
    with pytest.raises(Exception) as excinfo:
        sf._load_config({"measurement": "t", "max_forecast_points": value}, {})
    assert "Invalid configuration" in str(excinfo.value)
    assert fragment in str(excinfo.value)


# ---------------------------------------------------------------------------
# M3 — interval parsing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, seconds",
    [
        ("500us", 0.0005),
        ("100ms", 0.1),
        ("30s", 30),
        ("10min", 600),
        ("2h", 7200),
        ("30d", 2_592_000),
        ("1w", 604_800),
        ("1m", 30 * 86_400),
        ("2q", 182 * 86_400),
        ("1y", 365 * 86_400),
    ],
)
def test_parse_time_interval_units(raw, seconds):
    assert sf.parse_time_interval(raw, "T").total_seconds() == pytest.approx(seconds)


@pytest.mark.parametrize(
    "raw, fragment",
    [
        ("5x", "Invalid interval format"),
        ("abc", "Invalid interval format"),
        ("", "Invalid interval format"),
        ("0y", "Computed days < 1"),
        (30, "Invalid interval type"),
    ],
)
def test_parse_time_interval_rejections(raw, fragment):
    with pytest.raises(Exception, match=fragment):
        sf.parse_time_interval(raw, "T")


# ---------------------------------------------------------------------------
# M4 — tag filters
# ---------------------------------------------------------------------------

TAG_NAMES = ["room", "site", "path"]


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("", {}),
        ("room:Bedroom", {"room": ["Bedroom"]}),
        ("room:Bedroom@Kitchen.site:north", {"room": ["Bedroom", "Kitchen"], "site": ["north"]}),
        ("room:'Some other room'@Bedroom", {"room": ["Some other room", "Bedroom"]}),
        ("room:A.room:B", {"room": ["A", "B"]}),
        # quoting protects every separator, as documented in the README
        ("path:'a:b'", {"path": ["a:b"]}),
        ("path:'a.b'", {"path": ["a.b"]}),
        ("path:'a@b'", {"path": ["a@b"]}),
        ('path:"a:b@c.d"', {"path": ["a:b@c.d"]}),
        ("path:Bob's.room:A", {"path": ["Bob's"], "room": ["A"]}),
        ('path:5".room:A', {"path": ['5"'], "room": ["A"]}),
    ],
)
def test_parse_tags_from_args(raw, expected):
    local = FakeLocal()
    assert sf.parse_tags_from_args(local, raw, "m", TAG_NAMES, "T") == expected


def test_parse_tags_from_args_rejects_ambiguous_pair():
    with pytest.raises(Exception, match="Invalid tag-value pair"):
        sf.parse_tags_from_args(FakeLocal(), "path:a:b:c", "m", TAG_NAMES, "T")
    with pytest.raises(Exception, match="expected string"):
        sf.parse_tags_from_args(FakeLocal(), ["room:A"], "m", TAG_NAMES, "T")
    with pytest.raises(Exception, match="unterminated ' quote"):
        sf.parse_tags_from_args(FakeLocal(), "room:'Living room", "m", TAG_NAMES, "T")


def test_parse_tags_from_args_warns_on_unknown_tag():
    local = FakeLocal()
    assert sf.parse_tags_from_args(local, "nope:x.room:A", "m", TAG_NAMES, "T") == {"room": ["A"]}
    assert any("Tag 'nope' does not exist" in w for w in local.warns)


@pytest.mark.parametrize(
    "raw, expected",
    [
        (None, {}),
        ({}, {}),
        ({"room": "Bedroom"}, {"room": ["Bedroom"]}),
        ({"room": ["Bedroom", "Kitchen"]}, {"room": ["Bedroom", "Kitchen"]}),
        ({"room": [1, 2]}, {"room": ["1", "2"]}),
        ({"room": []}, {}),
    ],
)
def test_parse_tags_from_body(raw, expected):
    assert sf.parse_tags_from_body(FakeLocal(), raw, "m", TAG_NAMES, "T") == expected


@pytest.mark.parametrize(
    "raw, fragment",
    [(["Bedroom"], "expected JSON object"), ({"room": 5}, "expected string or list")],
)
def test_parse_tags_from_body_rejections(raw, fragment):
    with pytest.raises(Exception, match=fragment):
        sf.parse_tags_from_body(FakeLocal(), raw, "m", TAG_NAMES, "T")


def test_parse_tags_dispatches_on_the_value_type():
    local = FakeLocal()
    assert sf.parse_tags(local, {"room": "A"}, "m", TAG_NAMES, "T") == {"room": ["A"]}
    assert sf.parse_tags(local, "room:A", "m", TAG_NAMES, "T") == {"room": ["A"]}
    assert sf.parse_tags(local, None, "m", TAG_NAMES, "T") == {}
    with pytest.raises(Exception, match="expected a string or JSON object"):
        sf.parse_tags(local, ["room:A"], "m", TAG_NAMES, "T")


# ---------------------------------------------------------------------------
# M5 — metadata fields
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("", []),
        ("humidity pressure", ["humidity", "pressure"]),
        (["humidity", "pressure"], ["humidity", "pressure"]),
        ("humidity   pressure", ["humidity", "pressure"]),
    ],
)
def test_parse_metadata_fields(raw, expected):
    names = ["humidity", "pressure"]
    assert sf.parse_metadata_fields(FakeLocal(), raw, "m", names, "T") == expected


def test_parse_metadata_fields_drops_unknown_with_warning():
    local = FakeLocal()
    result = sf.parse_metadata_fields(local, "humidity nope", "m", ["humidity"], "T")
    assert result == ["humidity"]
    assert any("Metadata field 'nope' does not exist" in w for w in local.warns)


def test_parse_metadata_fields_rejects_other_types():
    with pytest.raises(Exception, match="expected string or list"):
        sf.parse_metadata_fields(FakeLocal(), 5, "m", [], "T")


# ---------------------------------------------------------------------------
# M6 — history query
# ---------------------------------------------------------------------------


def test_history_query_binds_tag_values_and_quotes_identifiers():
    start = pd.Timestamp("2026-01-01T00:00:00Z").to_pydatetime()
    query, params = sf.build_history_query(
        "temp", "va\"lue", ["humidity"], {"room": ["A"], "site": ["x", "y"]}, start
    )
    assert '"va""lue"' in query and '"humidity"' in query
    assert '"room" = $tag_val_0' in query
    assert '"site" IN ($tag_val_1, $tag_val_2)' in query
    assert params == {"tag_val_0": "A", "tag_val_1": "x", "tag_val_2": "y"}
    assert "time >= '2026-01-01T00:00:00.000000Z'" in query
    assert "ORDER BY time" in query


# ---------------------------------------------------------------------------
# M7 — Synthefy request payload
# ---------------------------------------------------------------------------


def test_request_payload_derives_step_and_targets():
    df = pd.DataFrame(history_rows(4))
    request = sf.dataframe_to_synthefy_request(FakeLocal(), df, "value", "3h", [], "sfm-tabular", 10_000, "T")
    sample = request["samples"][0][0]
    assert request["model"] == "sfm-tabular"
    assert sample["forecast"] is True and sample["metadata"] is False
    assert len(sample["history_timestamps"]) == 4
    assert sample["history_timestamps"][-1] == "2023-11-15T01:13:20.000000Z"
    # the horizon continues the series at its own step, starting after the last point
    assert sample["target_timestamps"] == [
        "2023-11-15T02:13:20.000000Z",
        "2023-11-15T03:13:20.000000Z",
        "2023-11-15T04:13:20.000000Z",
    ]
    assert sample["target_values"] == [None, None, None]


def test_request_payload_point_form_and_covariates():
    rows = history_rows(3, extra={"humidity": 1.0})
    request = sf.dataframe_to_synthefy_request(
        FakeLocal(), pd.DataFrame(rows), "value", "2 points", ["humidity"], "m", 10_000, "T"
    )
    samples = request["samples"][0]
    assert len(samples[0]["target_timestamps"]) == 2
    assert len(samples) == 2
    assert samples[1]["sample_id"] == "humidity"
    assert samples[1]["metadata"] is True and samples[1]["forecast"] is False


@pytest.mark.parametrize(
    "horizon, cap, fragment",
    [
        ("7d", 10_000, "above the max_forecast_points limit"),
        ("50000 points", 10_000, "above the max_forecast_points limit"),
        ("0 points", 10_000, "below minimum 1"),
        ("many points", 10_000, "Invalid forecast_horizon"),
        ("2 points points", 10_000, "Invalid forecast_horizon"),
    ],
)
def test_request_payload_rejections(horizon, cap, fragment):
    df = pd.DataFrame(history_rows(3, step_ns=1_000_000_000))
    with pytest.raises(Exception, match=fragment):
        sf.dataframe_to_synthefy_request(FakeLocal(), df, "value", horizon, [], "m", cap, "T")


def test_request_payload_allows_a_raised_cap():
    df = pd.DataFrame(history_rows(3, step_ns=1_000_000_000))
    request = sf.dataframe_to_synthefy_request(FakeLocal(), df, "value", "1h", [], "m", 10_000, "T")
    assert len(request["samples"][0][0]["target_timestamps"]) == 3600


def test_request_payload_warns_when_the_window_holds_several_series():
    # two series without a tag filter: every timestamp appears twice
    rows = history_rows(3) + history_rows(3, field="value")
    local = FakeLocal()
    sf.dataframe_to_synthefy_request(local, pd.DataFrame(rows), "value", "1 points", [], "m", 10, "T")
    assert any("3 repeated timestamps" in w and "Set 'tags'" in w for w in local.warns)

    local = FakeLocal()
    sf.dataframe_to_synthefy_request(
        local, pd.DataFrame(history_rows(3)), "value", "1 points", [], "m", 10, "T"
    )
    assert local.warns == []


def test_request_payload_keeps_sub_second_steps():
    df = pd.DataFrame(history_rows(6, step_ns=100_000_000))
    sample = sf.dataframe_to_synthefy_request(
        FakeLocal(), df, "value", "500ms", [], "m", 10_000, "T"
    )["samples"][0][0]
    assert sample["history_timestamps"][1].endswith(".100000Z")
    assert len(set(sample["target_timestamps"])) == 5


def test_request_payload_rejects_steps_finer_than_a_microsecond():
    df = pd.DataFrame(history_rows(4, step_ns=500))
    with pytest.raises(Exception, match="less than a microsecond"):
        sf.dataframe_to_synthefy_request(FakeLocal(), df, "value", "2 points", [], "m", 10, "T")


# ---------------------------------------------------------------------------
# M8 — forecast response to line protocol
# ---------------------------------------------------------------------------


def test_response_writes_tags_quantiles_and_exact_nanoseconds():
    response = forecast_response(
        ["2026-01-01T00:00:00.123456789Z", "2026-01-01T01:00:00Z"],
        [1.5, 2.5],
        quantiles={"0.1": [1.0, 2.0], "0.9": [2.0, 3.0]},
    )
    builders = sf.forecast_response_to_line_builders(
        FakeLocal(), response, "temp_forecast", {"room": ["A"], "site": ["x", "y"]},
        "sfm-tabular", "temp", "T",
    )
    first = _parse_lp(builders[0].build())
    assert first.measurement == "temp_forecast"
    # a single-valued filter is written as a tag; a multi-valued one is not
    assert first.tags == {"room": "A", "model": "sfm-tabular"}
    assert first.fields == {"temp": 1.5, "value_0.1": 1.0, "value_0.9": 2.0}
    assert first.timestamp == 1767225600123456789
    assert len(builders) == 2


def test_response_treats_naive_timestamps_as_utc():
    aware = forecast_response(["2026-01-01T00:00:00Z"], [1.0])
    naive = forecast_response(["2026-01-01T00:00:00"], [1.0])
    build = lambda r: sf.forecast_response_to_line_builders(
        FakeLocal(), r, "m", {}, "mdl", "v", "T"
    )[0].timestamp
    assert build(aware) == build(naive)


def test_response_skips_unusable_points_but_keeps_the_rest():
    local = FakeLocal()
    response = forecast_response(
        ["2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z", "not-a-time", "2026-01-01T03:00:00Z"],
        [1.0, None, 3.0, float("nan")],
    )
    builders = sf.forecast_response_to_line_builders(
        local, response, "m", {}, "mdl", "v", "T"
    )
    assert len(builders) == 1
    assert any("Non-finite forecast value" in w for w in local.warns)
    assert any("Could not parse timestamp" in w for w in local.warns)


def test_response_drops_non_finite_quantiles_only():
    response = forecast_response(
        ["2026-01-01T00:00:00Z"], [1.0], quantiles={"0.1": [float("inf")], "0.9": [2.0]}
    )
    builders = sf.forecast_response_to_line_builders(
        FakeLocal(), response, "m", {}, "mdl", "v", "T"
    )
    assert _parse_lp(builders[0].build()).fields == {"v": 1.0, "value_0.9": 2.0}


@pytest.mark.parametrize(
    "response, fragment",
    [
        ({}, "missing 'forecasts' field"),
        ({"forecasts": []}, "No forecasts in response"),
        ({"forecasts": [[{"nope": 1}]]}, "No forecast payload"),
    ],
)
def test_response_rejections(response, fragment):
    with pytest.raises(ValueError, match=fragment):
        sf.forecast_response_to_line_builders(
            FakeLocal(), response, "m", {}, "mdl", "v", "T"
        )


# ---------------------------------------------------------------------------
# M9 — writes
# ---------------------------------------------------------------------------


def _two_builders():
    return sf.forecast_response_to_line_builders(
        FakeLocal(),
        forecast_response(["2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"], [1.0, 2.0]),
        "m", {}, "mdl", "v", "T",
    )


def test_write_batches_all_points_into_one_payload():
    local = FakeLocal()
    calls = []
    original = local._record_batch
    local._record_batch = lambda db, batch: (calls.append(db), original(db, batch))

    sf.write_forecasts_to_influxdb(local, _two_builders(), None, "T")
    assert calls == [None]  # a single batched write, not one call per point
    assert len(local.writes) == 2


def test_write_routes_to_the_override_database():
    local = FakeLocal()
    sf.write_forecasts_to_influxdb(local, _two_builders(), "other", "T")
    assert [db for db, _ in local.writes] == ["other", "other"]
    assert any("database other" in i for i in local.infos)


def test_write_retries_then_succeeds():
    local = FakeLocal(write_failures=2)
    sf.write_forecasts_to_influxdb(local, _two_builders(), None, "T")
    assert len(local.writes) == 2
    assert local.errors == []


def test_write_reports_and_reraises_after_exhausting_retries():
    local = FakeLocal(write_failures=3)
    with pytest.raises(RuntimeError):
        sf.write_forecasts_to_influxdb(local, _two_builders(), None, "T")
    assert any("Failed to write forecasts after 3 attempts" in e for e in local.errors)


def test_write_skips_an_empty_result():
    local = FakeLocal()
    sf.write_forecasts_to_influxdb(local, [], None, "T")
    assert local.writes == []
    assert any("No forecast points to write" in w for w in local.warns)


# ---------------------------------------------------------------------------
# M10 — request body decoding
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "body, expected",
    [
        (None, {}),
        ("", {}),
        (b"", {}),
        ({"a": 1}, {"a": 1}),
        ('{"a": 1}', {"a": 1}),
        (b'{"a": 1}', {"a": 1}),
    ],
)
def test_decode_request_body(body, expected):
    assert sf._decode_request_body(body, "T") == expected


def test_decode_request_body_rejects_unsupported_type():
    with pytest.raises(Exception, match="Unsupported request_body type"):
        sf._decode_request_body(42, "T")


# ---------------------------------------------------------------------------
# M11 — process_request
# ---------------------------------------------------------------------------


def test_full_flow_reads_forecasts_and_writes(monkeypatch):
    local = FakeLocal(rows=history_rows(5, extra={"humidity": 1.0}))
    stub = FakeRequests(
        forecast_response(["2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"], [10.0, 11.0])
    )
    result = run(
        local,
        body={
            "measurement": "sf_temp",
            "tags": {"room": "Bedroom"},
            "metadata_fields": ["humidity"],
            "forecast_horizon": "2 points",
        },
        requests_stub=stub,
        monkeypatch=monkeypatch,
    )

    assert result == {
        "message": "Forecast generated and written to InfluxDB. 2 forecast points written."
    }
    call = stub.calls[0]
    assert call["url"] == "https://forecast.synthefy.com/v2/forecast"
    assert call["headers"]["X-API-Key"] == "k"
    assert [s["sample_id"] for s in call["body"]["samples"][0]] == ["value", "humidity"]

    written = [record for _, record in local.writes]
    assert [r.measurement for r in written] == ["sf_temp_forecast"] * 2
    assert written[0].tags == {"room": "Bedroom", "model": "sfm-tabular"}
    assert [r.fields["value"] for r in written] == [10.0, 11.0]
    assert local.errors == []


def test_full_flow_honours_output_measurement_and_database(monkeypatch):
    local = FakeLocal(rows=history_rows(3))
    stub = FakeRequests(forecast_response(["2026-01-01T00:00:00Z"], [10.0]))
    run(
        local,
        args={"measurement": "sf_temp", "output_measurement": "my_fc", "database": "other"},
        body={"forecast_horizon": "1 points"},
        requests_stub=stub,
        monkeypatch=monkeypatch,
    )
    db, record = local.writes[0]
    assert (db, record.measurement) == ("other", "my_fc")


def test_body_overrides_trigger_arguments(monkeypatch):
    local = FakeLocal(rows=history_rows(3))
    stub = FakeRequests(forecast_response(["2026-01-01T00:00:00Z"], [10.0]))
    run(
        local,
        args={"measurement": "sf_temp", "model": "from-args"},
        body={"model": "from-body", "forecast_horizon": "1 points"},
        requests_stub=stub,
        monkeypatch=monkeypatch,
    )
    assert stub.calls[0]["body"]["model"] == "from-body"
    assert local.writes[0][1].tags["model"] == "from-body"


@pytest.mark.parametrize(
    "body_extra, expected_room, expected_samples",
    [
        ({}, "Bedroom", ["value", "humidity"]),
        # a null means "not set", so the trigger argument still applies
        ({"tags": None, "metadata_fields": None}, "Bedroom", ["value", "humidity"]),
        # an empty value clears the trigger argument
        ({"tags": {}, "metadata_fields": []}, None, ["value"]),
        ({"tags": {"room": "Hall"}}, "Hall", ["value", "humidity"]),
        # the body accepts the trigger-argument string form too
        ({"tags": "room:Hall"}, "Hall", ["value", "humidity"]),
    ],
)
def test_tags_and_covariates_merge_with_trigger_arguments(
    body_extra, expected_room, expected_samples, monkeypatch
):
    local = FakeLocal(rows=history_rows(3, extra={"humidity": 1.0}))
    stub = FakeRequests(forecast_response(["2026-01-01T00:00:00Z"], [10.0]))
    run(
        local,
        args={"measurement": "sf_temp", "tags": "room:Bedroom", "metadata_fields": "humidity"},
        body={"forecast_horizon": "1 points", **body_extra},
        requests_stub=stub,
        monkeypatch=monkeypatch,
    )
    assert local.writes[0][1].tags.get("room") == expected_room
    assert [s["sample_id"] for s in stub.calls[0]["body"]["samples"][0]] == expected_samples


@pytest.mark.parametrize(
    "body, message",
    [
        ({}, "'measurement' argument is required"),
        ({"measurement": "nope"}, "Measurement 'nope' not found"),
        ({"measurement": "sf_temp", "field": "nope"},
         "Field 'nope' does not exist in 'sf_temp'"),
    ],
)
def test_request_rejections_before_the_api_call(body, message, monkeypatch):
    columns = {} if body.get("measurement") == "nope" else COLUMNS
    local = FakeLocal(columns=columns, rows=history_rows(3))
    stub = FakeRequests(error=AssertionError("API must not be called"))
    assert run(local, body=body, requests_stub=stub, monkeypatch=monkeypatch) == {
        "message": message
    }
    assert stub.calls == []


def test_missing_api_key_stops_before_touching_the_database():
    local = FakeLocal(rows=history_rows(3))
    result = sf.process_request(local, {}, {}, '{"measurement": "sf_temp"}', {})
    assert result == {"message": "Missing API key"}
    assert local.queries == []


def test_api_key_falls_back_to_the_environment(monkeypatch):
    monkeypatch.setenv(sf.API_KEY_ENV_VAR, "env-key")
    local = FakeLocal(rows=history_rows(3))
    stub = FakeRequests(forecast_response(["2026-01-01T00:00:00Z"], [10.0]))
    run(
        local,
        body={"measurement": "sf_temp", "forecast_horizon": "1 points"},
        headers={},
        requests_stub=stub,
        monkeypatch=monkeypatch,
    )
    assert stub.calls[0]["headers"]["X-API-Key"] == "env-key"


def test_empty_history_returns_no_data_without_calling_the_api(monkeypatch):
    local = FakeLocal(rows=[])
    stub = FakeRequests(error=AssertionError("API must not be called"))
    result = run(local, body={"measurement": "sf_temp"}, requests_stub=stub, monkeypatch=monkeypatch)
    assert result == {"message": "No data found"}
    assert stub.calls == []


def test_invalid_json_body_is_reported():
    local = FakeLocal()
    result = sf.process_request(local, {}, {"X-Synthefy-Api-Key": "k"}, "{oops", {})
    assert result == {"message": "Invalid JSON in request body"}
    assert any("Invalid JSON in request body" in e for e in local.errors)


def test_api_failure_is_logged_and_returned(monkeypatch):
    local = FakeLocal(rows=history_rows(3))
    stub = FakeRequests(error=RuntimeError("503 Service Unavailable"))
    result = run(
        local,
        body={"measurement": "sf_temp", "forecast_horizon": "1 points"},
        requests_stub=stub,
        monkeypatch=monkeypatch,
    )
    assert result == {"message": "Error: 503 Service Unavailable"}
    assert any("Synthefy API call failed" in e for e in local.errors)
    assert local.writes == []


def test_configuration_error_is_returned_not_raised(monkeypatch):
    local = FakeLocal(rows=history_rows(3))
    stub = FakeRequests(error=AssertionError("API must not be called"))
    result = run(
        local,
        body={"measurement": "sf_temp", "max_forecast_points": "junk"},
        requests_stub=stub,
        monkeypatch=monkeypatch,
    )
    assert "Invalid configuration" in result["message"]
    assert any("HTTP request forecast failed" in e for e in local.errors)
