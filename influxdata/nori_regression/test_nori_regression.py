"""Unit tests for the nori_regression plugin.

Mirrors the mock-based approach used across this repo: a fake influxdb3_local, a fake LineBuilder
and a fake `requests` module, so no engine and no network are needed.

    pytest influxdata/nori_regression/test_nori_regression.py
"""

import json
import re
import math
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.dirname(__file__))
import nori_regression as nr  # noqa: E402

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
NS = 1_000_000_000

BASE_ARGS = {
    "measurement": "sensors",
    "field": "pressure",
    "feature_fields": "temp humidity",
    "model": "synthefy/nori-30m",
}

# `model` is accepted as a trigger argument only, so a request body carries everything but it.
MODEL_ARG = {"model": BASE_ARGS["model"]}
BASE_BODY = {k: v for k, v in BASE_ARGS.items() if k != "model"}

SENSOR_SCHEMA = [
    {"column_name": "time", "data_type": "Timestamp(Nanosecond, None)"},
    {"column_name": "pressure", "data_type": "Float64"},
    {"column_name": "temp", "data_type": "Float64"},
    {"column_name": "humidity", "data_type": "Float64"},
    {"column_name": "status", "data_type": "Utf8"},
    {"column_name": "site", "data_type": nr.TAG_DATA_TYPE},
]


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeLineBuilder:
    def __init__(self, measurement):
        self.measurement = measurement
        self.tags = []
        self.fields = {}
        self.timestamp = None

    def tag(self, key, value):
        self.tags.append((key, value))
        return self

    def float64_field(self, key, value):
        self.fields[key] = value
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


class FakeLocal:
    """Query responses are keyed by a SQL substring, longest key first."""

    def __init__(self, query_responses=None):
        self.query_responses = dict(query_responses or {})
        self.queries = []
        self.infos, self.warns, self.errors = [], [], []
        self.writes = []
        self.fail_writes = False

    def info(self, *a):
        self.infos.append(" ".join(str(x) for x in a))

    def warn(self, *a):
        self.warns.append(" ".join(str(x) for x in a))

    def error(self, *a):
        self.errors.append(" ".join(str(x) for x in a))

    def query(self, sql, args=None, database=None):
        self.queries.append({"sql": sql, "args": args, "database": database})
        for key in sorted(self.query_responses, key=len, reverse=True):
            if key in sql:
                rows = self.query_responses[key]
                if isinstance(rows, Exception):
                    raise rows
                return rows
        return []

    def write_sync(self, batch, no_sync=False):
        if self.fail_writes:
            raise RuntimeError("write refused")
        self.writes.append((None, batch.build()))

    def write_sync_to_db(self, database, batch, no_sync=False):
        if self.fail_writes:
            raise RuntimeError("write refused")
        self.writes.append((database, batch.build()))


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=None, headers=None):
        self.status_code = status_code
        self._payload = payload
        self.text = text if text is not None else json.dumps(payload)
        self.headers = headers or {"Content-Type": "application/json"}

    def json(self):
        if self._payload is None:
            raise ValueError("no JSON")
        return self._payload


class FakeRequests:
    """Stands in for the `requests` module: replays a script of responses/exceptions."""

    exceptions = __import__("requests").exceptions

    def __init__(self, script):
        self.script = list(script)
        self.calls = []

    def post(self, url, json=None, headers=None, timeout=None):
        self.calls.append(
            {"url": url, "json": json, "headers": headers, "timeout": timeout}
        )
        item = self.script.pop(0) if self.script else FakeResponse(200, {"predictions": []})
        if isinstance(item, Exception):
            raise item
        return item


@pytest.fixture(autouse=True)
def _plugin_env(monkeypatch):
    monkeypatch.setattr(nr, "LineBuilder", FakeLineBuilder, raising=False)
    monkeypatch.setattr(nr.time, "sleep", lambda _s: None)
    monkeypatch.delenv(nr.API_KEY_ENV_VAR, raising=False)
    monkeypatch.delenv(nr.GATEWAY_URL_ENV_VAR, raising=False)
    monkeypatch.setenv("PLUGIN_DIR", "/tmp")
    yield


def rows(specs, site="A"):
    """Build query rows: (offset_seconds, target_or_None, temp, humidity).

    Columns come back under their own names — the plugin does not alias the target to `y`, because
    a source tag column named `y` would collide with the alias and make the query unplannable.
    """
    return [
        {"time": t * NS, "pressure": y, "temp": temp, "humidity": hum, "site": site}
        for t, y, temp, hum in specs
    ]


def labeled(count, start=0, site="A"):
    return rows(
        [(start + i, 1000.0 + i, 20.0 + i % 5, 40.0 + i % 3) for i in range(count)],
        site=site,
    )


def unlabeled(count, start=10_000, site="A"):
    return rows(
        [(start + i, None, 21.0 + i % 5, 41.0 + i % 3) for i in range(count)], site=site
    )


def cfg_for(**overrides):
    args = dict(BASE_ARGS)
    args.update(overrides)
    return nr._load_config(args)


# ---------------------------------------------------------------------------
# Docstring metadata
# ---------------------------------------------------------------------------


def test_docstring_metadata_declares_every_accepted_parameter():
    """Explorer renders only declared parameters, so the declaration must not lag the code."""
    meta = json.loads(nr.__doc__)
    assert meta["plugin_type"] == ["scheduled", "http"]
    accepted = {
        "measurement",
        "field",
        "feature_fields",
        "window",
        "start_time",
        "end_time",
        "tags",
        "model",
        "output_measurement",
        "target_database",
        "dry_run",
        "skip_existing",
        "min_history",
        "max_train_rows",
        "max_predict_rows",
        "max_read_rows",
        "predict_batch_size",
        "request_timeout",
        "max_retries",
        "config_file_path",
    }
    for section in ("scheduled_args_config", "http_args_config"):
        declared = {a["name"] for a in meta[section]}
        assert declared == accepted, f"{section} drifted: {declared ^ accepted}"
        for arg in meta[section]:
            assert arg["description"] and arg["example"] is not None
            assert isinstance(arg["required"], bool)


def test_every_validator_key_is_declared():
    meta = json.loads(nr.__doc__)
    declared = {a["name"] for a in meta["scheduled_args_config"]}
    for validator in nr.VALIDATORS:
        for name in validator.names:
            assert name in declared


# ---------------------------------------------------------------------------
# Body-override allowlist (the security regression)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [
        {"measurement": "@format {env[SYNTHEFY_NORI_API_KEY]}"},
        {"field": "@read_file /etc/passwd"},
        {"measurement": "@jinja {{env.SYNTHEFY_NORI_API_KEY}}"},
        {"measurement": "@get other"},
        {"feature_fields": ["@format {env[SYNTHEFY_NORI_API_KEY]}", "temp"]},
        {"tags": {"site": "@read_file /etc/passwd"}},
        {"window": "@json [1]"},
    ],
)
def test_a_body_value_cannot_be_a_dynaconf_substitution_token(body, monkeypatch):
    """The allowlist gates key NAMES; without this the VALUES still reach dynaconf, which resolves
    @format/@read_file/@jinja and hands the host's environment or files back to the caller."""
    monkeypatch.setenv(nr.API_KEY_ENV_VAR, "SECRET-KEY-abc123")
    with pytest.raises(nr.ConfigError, match="may not begin with"):
        nr._load_config({}, {**BASE_ARGS, **body})


def test_a_trigger_argument_cannot_be_a_substitution_token_either():
    with pytest.raises(nr.ConfigError, match="trigger argument value"):
        cfg_for(output_measurement="@read_file /etc/passwd")


@pytest.mark.parametrize("value", ["a@b", "sensors@1", "user@example.com"])
def test_an_at_sign_that_is_not_a_leading_token_is_allowed(value):
    """Only a LEADING '@' triggers dynaconf, so a legitimate value containing one must still work.

    The guard is deliberately stricter than dynaconf at the boundary: any leading '@' is refused,
    including forms dynaconf would treat as inert, so the rule stays one sentence long.
    """
    cfg = nr._load_config(MODEL_ARG, {**BASE_BODY, "measurement": value})
    assert cfg["measurement"] == value


def test_the_leak_does_not_survive_end_to_end(monkeypatch):
    monkeypatch.setenv(nr.API_KEY_ENV_VAR, "SECRET-KEY-abc123")
    local = FakeLocal({"information_schema": SENSOR_SCHEMA})
    result, _ = _http(local, {"measurement": "@format {env[SYNTHEFY_NORI_API_KEY]}"})
    assert result["status"] == "failed"
    assert "SECRET-KEY-abc123" not in json.dumps(result)


@pytest.mark.parametrize(
    "key",
    [
        "MEASUREMENT",  # dynaconf keys are case-insensitive; the allowlist must still be closed
        "Target_Database",
        "CONFIG_FILE_PATH",
        " measurement ",
        "__class__",
        "measuremeｎt",  # unicode look-alike
    ],
)
def test_the_allowlist_is_closed_against_key_tricks(key):
    args = {**BASE_ARGS, "target_database": "opdb", "output_measurement": "op_out"}
    with pytest.raises(nr.ConfigError, match="may not set"):
        nr._load_config(args, {key: "evil"})


@pytest.mark.parametrize(
    "key, value",
    [
        ("gateway_url", "https://attacker.example/collect"),
        ("target_database", "someone_elses_db"),
        ("output_measurement", "victim"),
        ("model", "synthefy/nori-30m-thinking-medium"),
        ("min_history", "0"),
        ("max_train_rows", "1000000"),
        ("max_predict_rows", "1000000"),
        ("max_read_rows", "100000000"),
        ("predict_batch_size", "1"),
        ("request_timeout", "1s"),
        ("max_retries", "99"),
        ("skip_existing", "false"),
        ("config_file_path", "/etc/passwd"),
    ],
)
def test_request_body_cannot_set_operator_parameters(key, value):
    """A caller holding only a database token must not redirect the key, the write target, or cost."""
    with pytest.raises(nr.ConfigError) as excinfo:
        nr._load_config(dict(BASE_ARGS), {key: value})
    assert key in str(excinfo.value)


def test_gateway_url_is_not_a_parameter_at_all():
    """A trigger argument cannot move the endpoint either, and is rejected rather than ignored."""
    with pytest.raises(nr.ConfigError, match=nr.GATEWAY_URL_ENV_VAR):
        cfg_for(gateway_url="https://attacker.example/collect")
    assert nr._gateway_url() == nr.DEFAULT_GATEWAY_URL


@pytest.mark.parametrize("key", sorted(nr.REMOVED_PARAMETERS))
def test_a_removed_parameter_is_rejected_not_ignored(key):
    """An operator following the plugin's earlier documentation gets a message, not silence."""
    with pytest.raises(nr.ConfigError, match=f"`{key}` is not a parameter"):
        cfg_for(**{key: "1"})


def test_request_body_may_set_query_shape_keys_the_trigger_left_open():
    cfg = nr._load_config(
        MODEL_ARG,
        {
            "measurement": "other",
            "field": "flow",
            "feature_fields": ["a", "b"],
            "tags": {"site": "B"},
            "window": "2d",
            "start_time": "2026-01-01T00:00:00Z",
            "end_time": "2026-01-02T00:00:00Z",
            "dry_run": True,
        },
    )
    assert cfg["measurement"] == "other"
    assert cfg["field"] == "flow"
    assert cfg["feature_fields"] == ["a", "b"]
    assert cfg["tags"] == {"site": "B"}
    assert cfg["window"] == timedelta(days=2)
    assert cfg["dry_run"] is True
    assert cfg["output_measurement"] == "other_regressed"


@pytest.mark.parametrize("key", sorted(nr.BODY_OVERRIDABLE_KEYS))
def test_a_trigger_argument_pins_its_value_against_the_body(key):
    """`measurement` is body-settable and `output_measurement` defaults from it, so a body that
    could override a pinned `measurement` would re-point the operator's write target."""
    args = {**BASE_ARGS, "window": "1d", "tags": "site:A", "start_time": "2026-01-01T00:00:00Z",
            "end_time": "2026-01-02T00:00:00Z", "dry_run": "false"}
    with pytest.raises(nr.ConfigError, match=f"may not override \\['{key}'\\]"):
        nr._load_config(args, {key: "x" if key != "dry_run" else True})


def test_explicit_null_in_body_falls_back_to_the_default():
    cfg = nr._load_config({"measurement": "sensors", "field": "pressure",
                           "feature_fields": "temp humidity",
                           "model": "synthefy/nori-30m"}, {"window": None})
    assert cfg["window"] == timedelta(days=30)


def test_toml_config_and_request_body_are_mutually_exclusive(tmp_path):
    path = tmp_path / "cfg.toml"
    path.write_text('measurement = "sensors"\nfield = "pressure"\n')
    with pytest.raises(nr.ConfigError, match="must be empty"):
        nr._load_config({"config_file_path": str(path)}, {"window": "1d"})


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def test_toml_config_supplies_every_parameter(tmp_path):
    path = tmp_path / "cfg.toml"
    path.write_text(
        'measurement = "sensors"\n'
        'field = "pressure"\n'
        'feature_fields = ["air temp", "humidity"]\n'
        'model = "synthefy/nori-30m"\n'
        'window = "7d"\n'
        "min_history = 10\n"
    )
    cfg = nr._load_config({"config_file_path": str(path)})
    # A TOML list reaches the plugin natively, which is the only way to name a column
    # containing a space.
    assert cfg["feature_fields"] == ["air temp", "humidity"]
    assert cfg["window"] == timedelta(days=7)
    assert cfg["min_history"] == 10


def test_toml_config_cannot_be_combined_with_inline_arguments(tmp_path):
    """"Supplies all parameters" has to be enforced, or the inline ones vanish in silence."""
    path = tmp_path / "cfg.toml"
    path.write_text('measurement = "sensors"\nfield = "pressure"\n'
                    'feature_fields = ["temp"]\nmodel = "synthefy/nori-30m"\n')
    with pytest.raises(nr.ConfigError, match="cannot be combined with the inline"):
        nr._load_config({"config_file_path": str(path), "window": "99d"})


def test_the_shipped_toml_template_loads():
    """Catches drift between the template and the parameters the code accepts."""
    template = os.path.join(os.path.dirname(__file__), "nori_regression_config_scheduler.toml")
    cfg = nr._load_config({"config_file_path": template})
    assert cfg["measurement"] == "sensors"
    assert cfg["field"] == "pressure"
    assert cfg["feature_fields"] == ["temp", "humidity"]


def test_a_toml_failure_keeps_the_host_path_out_of_the_caller_message(tmp_path):
    """A caller reaches this by posting an empty body to a TOML-configured trigger."""
    missing = tmp_path / "nope.toml"
    with pytest.raises(nr.ConfigError) as excinfo:
        nr._load_config({"config_file_path": str(missing)})
    assert str(excinfo.value) == (
        "the trigger's TOML configuration could not be loaded; see the plugin logs for this task_id"
    )
    assert str(missing) not in str(excinfo.value)
    assert str(missing) in excinfo.value.log_text()


def test_a_malformed_toml_is_reported_the_same_way(tmp_path):
    path = tmp_path / "bad.toml"
    path.write_text('measurement = "a"\nmeasurement = "b"\n')
    with pytest.raises(nr.ConfigError, match="could not be loaded"):
        nr._load_config({"config_file_path": str(path)})


@pytest.mark.parametrize(
    "args, fragment",
    [
        ({"measurement": ""}, "`measurement` is required"),
        ({"field": ""}, "`field` is required"),
        ({"feature_fields": ""}, "`feature_fields` is required"),
        ({"feature_fields": "temp pressure"}, "cannot include the target field"),
        ({"feature_fields": "temp time"}, "cannot include the target field or 'time'"),
        ({"tags": "site=A"}, "invalid `tags`"),
        ({"tags": "site"}, "invalid `tags`"),
        ({"model": "synthefy/nori"}, "retired"),
        ({"output_measurement": "sensors"}, "must differ from measurement"),
        ({"min_history": "5000"}, "exceeds max_train_rows"),
        ({"window": "1.5h"}, "invalid configuration"),
        ({"window": "5m"}, "invalid configuration"),
        ({"min_history": "0"}, "invalid configuration"),
        ({"dry_run": "maybe"}, "invalid configuration"),
    ],
)
def test_rejected_configurations(args, fragment):
    merged = dict(BASE_ARGS)
    merged.update(args)
    with pytest.raises(nr.ConfigError) as excinfo:
        nr._load_config(merged)
    assert fragment in str(excinfo.value)


def test_tags_parse_into_a_filter_map():
    cfg = cfg_for(tags="site:A zone:north")
    assert cfg["tags"] == {"site": "A", "zone": "north"}


def test_feature_fields_are_deduplicated_in_order():
    cfg = cfg_for(feature_fields="temp humidity temp")
    assert cfg["feature_fields"] == ["temp", "humidity"]


def test_defaults():
    cfg = cfg_for()
    assert cfg["model"] == BASE_ARGS["model"]  # named by the caller; the plugin has no default
    assert cfg["output_measurement"] == "sensors_regressed"
    assert cfg["window"] == timedelta(days=30)
    assert cfg["skip_existing"] is True
    assert cfg["dry_run"] is False
    assert cfg["target_database"] is None
    # A cold start was measured at 125s on synthefy/nori-30m, so the default must clear it.
    assert cfg["request_timeout"] >= 300


# ---------------------------------------------------------------------------
# API key resolution
# ---------------------------------------------------------------------------


def test_header_key_wins_over_the_environment(monkeypatch):
    monkeypatch.setenv(nr.API_KEY_ENV_VAR, "from-env")
    assert nr._get_api_key({"X-Nori-Api-Key": " from-header "}) == "from-header"
    assert nr._get_api_key({"x-nori-api-key": "lowercased"}) == "lowercased"


def test_empty_header_value_falls_back_to_the_environment(monkeypatch):
    """An empty header used to win and send `Api-Key ` with no key at all."""
    monkeypatch.setenv(nr.API_KEY_ENV_VAR, "from-env")
    assert nr._get_api_key({"X-Nori-Api-Key": ""}) == "from-env"
    assert nr._get_api_key({"X-Nori-Api-Key": "   "}) == "from-env"


def test_non_string_header_values_do_not_crash(monkeypatch):
    monkeypatch.setenv(nr.API_KEY_ENV_VAR, "from-env")
    assert nr._get_api_key({"X-Nori-Api-Key": ["listed"]}) == "listed"
    assert nr._get_api_key({"X-Nori-Api-Key": []}) == "from-env"
    assert nr._get_api_key({"X-Nori-Api-Key": 1234}) == "from-env"
    assert nr._get_api_key({b"binary": b"key"}) == "from-env"


def test_model_is_required(monkeypatch):
    """There is no default model: the slug selects a priced variant, so the plugin refuses to pick
    one, the same way Synthefy's own client and local package do."""
    args = {k: v for k, v in BASE_ARGS.items() if k != "model"}
    with pytest.raises(nr.ConfigError, match="`model` is required"):
        nr._load_config(args, {})


def test_an_empty_model_is_treated_as_missing(monkeypatch):
    with pytest.raises(nr.ConfigError, match="`model` is required"):
        nr._load_config({**BASE_ARGS, "model": "   "}, {})


def test_the_model_requirement_names_the_published_list():
    args = {k: v for k, v in BASE_ARGS.items() if k != "model"}
    with pytest.raises(nr.ConfigError, match=re.escape(nr.MODEL_LIST_URL)):
        nr._load_config(args, {})


def test_the_api_key_environment_variable_is_vendor_prefixed():
    """The name is part of the plugin's public contract: operators set it on the InfluxDB host and
    the error message names it, so it must not drift silently."""
    assert nr.API_KEY_ENV_VAR == "SYNTHEFY_NORI_API_KEY"


def test_no_key_anywhere_is_a_config_error():
    with pytest.raises(nr.ConfigError, match=nr.API_KEY_ENV_VAR):
        nr._get_api_key({})


def test_authorization_header_is_never_read(monkeypatch):
    monkeypatch.setenv(nr.API_KEY_ENV_VAR, "from-env")
    assert nr._get_api_key({"Authorization": "Api-Key sneaky"}) == "from-env"


# ---------------------------------------------------------------------------
# Gateway URL
# ---------------------------------------------------------------------------


def test_gateway_url_env_override_requires_https(monkeypatch):
    monkeypatch.setenv(nr.GATEWAY_URL_ENV_VAR, "https://gateway.internal/predict")
    assert nr._gateway_url() == "https://gateway.internal/predict"

    monkeypatch.setenv(nr.GATEWAY_URL_ENV_VAR, "http://attacker.example/predict")
    with pytest.raises(nr.ConfigError, match="https"):
        nr._gateway_url()


def test_gateway_url_allows_plain_http_on_loopback_only(monkeypatch):
    monkeypatch.setenv(nr.GATEWAY_URL_ENV_VAR, "http://localhost:9000/predict")
    assert nr._gateway_url() == "http://localhost:9000/predict"
    monkeypatch.setenv(nr.GATEWAY_URL_ENV_VAR, "http://127.0.0.1:9000/predict")
    assert nr._gateway_url().startswith("http://127.0.0.1")


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost@evil.example/predict",  # userinfo, not a loopback host
        "http://[::1]@evil.example/predict",  # urlsplit itself raises on this one
        "http://127.0.0.1.evil.example/predict",
        "http://localhost.evil.example/predict",
        "http://evil.example/predict",
        "http://127.0.0.2/predict",  # not loopback
        "http://0.0.0.0/predict",
        "https:///predict",  # no host
        "http://",
        "ftp://localhost/predict",
        "file:///etc/passwd",
        "//evil.example/predict",
        "evil.example/predict",
        "not a url",
    ],
)
def test_gateway_url_rejects_a_disguised_host(monkeypatch, url):
    monkeypatch.setenv(nr.GATEWAY_URL_ENV_VAR, url)
    with pytest.raises(nr.ConfigError):
        nr._gateway_url()


@pytest.mark.parametrize(
    "url",
    [
        "https://gateway.internal/predict",
        " https://gateway.internal/predict ",  # trimmed
        "http://LOCALHOST/predict",  # the host is matched case-insensitively
        "http://[::1]:9000/predict",
        "http://127.0.0.1:9000/predict?x=1",
    ],
)
def test_gateway_url_accepts_the_legitimate_forms(monkeypatch, url):
    monkeypatch.setenv(nr.GATEWAY_URL_ENV_VAR, url)
    assert nr._gateway_url() == url.strip()


# ---------------------------------------------------------------------------
# Window resolution
# ---------------------------------------------------------------------------


NOW = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)


def test_window_uses_both_bounds_when_given():
    cfg = cfg_for(start_time="2026-01-01T00:00:00Z", end_time="2026-02-01T00:00:00Z")
    start, end = nr._resolve_window(cfg, NOW)
    assert start == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert end == datetime(2026, 2, 1, tzinfo=timezone.utc)


def test_start_time_alone_reads_up_to_now():
    """This used to fall back to the full 30-day window with no message."""
    cfg = cfg_for(start_time="2026-05-30T00:00:00Z")
    start, end = nr._resolve_window(cfg, NOW)
    assert start == datetime(2026, 5, 30, tzinfo=timezone.utc)
    assert end == NOW


def test_end_time_alone_reads_one_window_before_it():
    cfg = cfg_for(end_time="2026-03-01T00:00:00Z", window="2d")
    start, end = nr._resolve_window(cfg, NOW)
    assert end == datetime(2026, 3, 1, tzinfo=timezone.utc)
    assert start == datetime(2026, 2, 27, tzinfo=timezone.utc)


def test_no_bounds_uses_the_trailing_window():
    cfg = cfg_for(window="6h")
    start, end = nr._resolve_window(cfg, NOW)
    assert end == NOW
    assert start == NOW - timedelta(hours=6)


def test_naive_iso_bounds_are_utc():
    cfg = cfg_for(start_time="2026-01-01T00:00:00", end_time="2026-01-02T00:00:00")
    start, end = nr._resolve_window(cfg, NOW)
    assert start.tzinfo is not None and end.tzinfo is not None


@pytest.mark.parametrize(
    "args, fragment",
    [
        ({"start_time": "not-a-date"}, "ISO 8601"),
        (
            {"start_time": "2026-02-01T00:00:00Z", "end_time": "2026-01-01T00:00:00Z"},
            "empty time window",
        ),
        (
            {"start_time": "2026-01-01T00:00:00Z", "end_time": "2026-01-01T00:00:00Z"},
            "empty time window",
        ),
    ],
)
def test_rejected_windows(args, fragment):
    with pytest.raises(nr.ConfigError, match=fragment):
        nr._resolve_window(cfg_for(**args), NOW)


# ---------------------------------------------------------------------------
# WHERE clause
# ---------------------------------------------------------------------------


def test_the_read_query_is_bounded_by_max_read_rows():
    """The row caps bound what is SENT; without a LIMIT nothing bounds what is READ, and `window`
    is settable from the request body."""
    data = labeled(60) + unlabeled(2)
    local = FakeLocal({"information_schema": SENSOR_SCHEMA, 'FROM "sensors"': data})
    cfg = cfg_for(max_read_rows="62")
    schema = nr._resolve_schema(local, cfg)
    rows = nr._read_rows(local, cfg, schema, NOW - timedelta(days=30), NOW, "t")
    sql = [q for q in local.queries if "information_schema" not in q["sql"]][0]["sql"]
    assert "ORDER BY time DESC LIMIT 62" in sql
    # The DESC query keeps the newest rows; the ascending order downstream depends on is this
    # function's own guarantee, not the engine's.
    assert [r["time_ns"] for r in rows] == sorted(r["time_ns"] for r in rows)
    assert any("at least max_read_rows (62)" in w for w in local.warns)


def test_rows_are_sorted_regardless_of_the_order_the_engine_returns():
    data = labeled(5)
    local = FakeLocal(
        {"information_schema": SENSOR_SCHEMA, 'FROM "sensors"': list(reversed(data))}
    )
    cfg = cfg_for()
    rows = nr._read_rows(
        local, cfg, nr._resolve_schema(local, cfg), NOW - timedelta(days=30), NOW, "t"
    )
    assert [r["time_ns"] for r in rows] == sorted(r["time_ns"] for r in rows)


def test_a_read_under_the_ceiling_does_not_warn():
    local = FakeLocal({"information_schema": SENSOR_SCHEMA, 'FROM "sensors"': labeled(10)})
    cfg = cfg_for()
    nr._read_rows(local, cfg, nr._resolve_schema(local, cfg), NOW - timedelta(days=30), NOW, "t")
    assert not any("max_read_rows" in w for w in local.warns)


def test_where_clause_binds_values_and_escapes_identifiers():
    clause, params = nr._build_where(
        {'we"ird': "va'lue"}, NOW - timedelta(hours=1), NOW
    )
    assert clause == 'time >= $start_ts AND time < $end_ts AND "we""ird" = $tag0'
    assert params["tag0"] == "va'lue"
    assert params["start_ts"] == "2026-06-01T11:00:00Z"
    assert params["end_ts"] == "2026-06-01T12:00:00Z"


# ---------------------------------------------------------------------------
# Prediction validation
# ---------------------------------------------------------------------------


def test_valid_predictions_pass_through():
    assert nr._validate_predictions([1.0, 2, -3.5], 3) == [1.0, 2.0, -3.5]


def test_a_null_prediction_is_a_per_row_outcome():
    """The gateway emits JSON null for a row whose prediction is not finite."""
    assert nr._validate_predictions([1.0, None, 3.0], 3) == [1.0, None, 3.0]


@pytest.mark.parametrize(
    "preds, expected, fragment",
    [
        ([None, None], 2, "every one of the 2 predictions is null"),
        ([1.0], 2, "returned 1 predictions for 2 rows"),
        ({"predictions": []}, 1, "expected a list"),
        (None, 1, "expected a list"),
        (["1.5"], 1, "expected a number"),
        ([True], 1, "expected a number"),
        ([float("nan")], 1, "not finite"),
        ([float("inf")], 1, "not finite"),
    ],
)
def test_rejected_predictions(preds, expected, fragment):
    with pytest.raises(nr.GatewayError, match=fragment):
        nr._validate_predictions(preds, expected)


# ---------------------------------------------------------------------------
# Schema resolution
# ---------------------------------------------------------------------------


def test_schema_resolution_returns_tag_names():
    local = FakeLocal({"information_schema": SENSOR_SCHEMA})
    schema = nr._resolve_schema(local, cfg_for())
    assert schema["tag_names"] == ["site"]
    assert local.queries[0]["args"] == {"m": "sensors"}


@pytest.mark.parametrize(
    "args, fragment",
    [
        ({"feature_fields": "temp status"}, "is Utf8, not a numeric field"),
        ({"feature_fields": "temp site"}, "not a numeric field"),
        ({"feature_fields": "temp nope"}, "not found in 'sensors'"),
        ({"field": "status"}, "target field 'status' is Utf8"),
        ({"field": "nope"}, "target field 'nope' not found"),
        ({"tags": "nope:x"}, "tag column(s) ['nope'] not found"),
        ({"tags": "temp:1"}, "names field column(s) ['temp'], not tags"),
    ],
)
def test_schema_rejects_columns_it_cannot_serve(args, fragment):
    """A name-only check let a tag or string column through, then reported 'only 0 labeled rows'."""
    local = FakeLocal({"information_schema": SENSOR_SCHEMA})
    with pytest.raises(nr.ConfigError) as excinfo:
        nr._resolve_schema(local, cfg_for(**args))
    assert fragment in str(excinfo.value)


@pytest.mark.parametrize("clash", sorted(nr.PROVENANCE_TAGS))
def test_a_source_tag_clashing_with_a_provenance_tag_is_rejected(clash):
    """Such a tag would overwrite the provenance on write AND make the skip_existing lookup
    self-contradictory, so the run would re-pay for the same rows forever."""
    schema = SENSOR_SCHEMA + [{"column_name": clash, "data_type": nr.TAG_DATA_TYPE}]
    local = FakeLocal({"information_schema": schema})
    with pytest.raises(nr.ConfigError, match="collide with the provenance tags"):
        nr._resolve_schema(local, cfg_for())


def test_a_source_tag_named_y_does_not_break_the_query():
    """The target is not aliased to `y`, so a tag column of that name cannot collide with it."""
    schema = SENSOR_SCHEMA + [{"column_name": "y", "data_type": nr.TAG_DATA_TYPE}]
    data = [{**r, "y": "tagvalue"} for r in labeled(60) + unlabeled(1)]
    local = FakeLocal({"information_schema": schema, 'FROM "sensors"': data})
    cfg = cfg_for()
    resolved = nr._resolve_schema(local, cfg)
    assert "y" in resolved["tag_names"]
    read = nr._read_rows(local, cfg, resolved, NOW - timedelta(days=30), NOW, "t")
    sql = [q for q in local.queries if "information_schema" not in q["sql"]][0]["sql"]
    assert " AS y" not in sql
    assert read[0]["y"] == 1000.0  # the target value, read under its own column name
    assert ("y", "tagvalue") in read[0]["series"]


def test_unknown_measurement_is_reported_plainly():
    local = FakeLocal({"information_schema": []})
    with pytest.raises(nr.ConfigError, match="not found \\(no columns\\)"):
        nr._resolve_schema(local, cfg_for())


# ---------------------------------------------------------------------------
# Gateway transport
# ---------------------------------------------------------------------------


def _call(local, cfg, script, api_key="k"):
    fake = FakeRequests(script)
    original = nr.requests
    nr.requests = fake
    try:
        return nr._call_nori(local, cfg, [[1.0, 2.0]], [3.0], [[1.0, 2.0]], api_key, "t"), fake
    finally:
        nr.requests = original


def test_gateway_call_sends_the_documented_payload():
    local = FakeLocal()
    preds, fake = _call(
        local, cfg_for(), [FakeResponse(200, {"predictions": [9.5], "usage": {}})]
    )
    assert preds == [9.5]
    sent = fake.calls[0]
    assert sent["url"] == nr.DEFAULT_GATEWAY_URL
    assert sent["json"]["model"] == "synthefy/nori-30m"
    assert sent["json"]["task"] == "regression"
    assert sent["headers"]["Authorization"] == "Api-Key k"
    assert sent["timeout"] >= 300


def test_a_transient_fault_is_retried():
    local = FakeLocal()
    preds, fake = _call(
        local,
        cfg_for(max_retries="3"),
        [
            FakeResponse(503, {"detail": "model loading"}),
            FakeResponse(429, {"detail": "slow down"}),
            FakeResponse(200, {"predictions": [1.0]}),
        ],
    )
    assert preds == [1.0]
    assert len(fake.calls) == 3
    assert any("attempt 1 failed (HTTP 503)" in w for w in local.warns)


def test_retry_after_is_honoured(monkeypatch):
    slept = []
    monkeypatch.setattr(nr.time, "sleep", lambda s: slept.append(s))
    local = FakeLocal()
    _call(
        local,
        cfg_for(),
        [
            FakeResponse(429, {"detail": "slow"}, headers={"Retry-After": "7"}),
            FakeResponse(200, {"predictions": [1.0]}),
        ],
    )
    assert slept == [7.0]


def test_a_negative_retry_after_does_not_abort_the_run(monkeypatch):
    """time.sleep raises on a negative delay, which would throw away healthy attempts."""
    slept = []
    monkeypatch.setattr(nr.time, "sleep", lambda s: slept.append(s))
    local = FakeLocal()
    preds, _ = _call(
        local,
        cfg_for(),
        [
            FakeResponse(503, {"detail": "x"}, headers={"Retry-After": "-5"}),
            FakeResponse(200, {"predictions": [1.0]}),
        ],
    )
    assert preds == [1.0]
    assert slept == [0.0]


def test_an_http_date_retry_after_falls_back_to_backoff(monkeypatch):
    slept = []
    monkeypatch.setattr(nr.time, "sleep", lambda s: slept.append(s))
    local = FakeLocal()
    _call(
        local,
        cfg_for(),
        [
            FakeResponse(429, {}, headers={"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}),
            FakeResponse(200, {"predictions": [1.0]}),
        ],
    )
    assert 1.0 <= slept[0] <= 2.0


def test_backoff_is_capped(monkeypatch):
    slept = []
    monkeypatch.setattr(nr.time, "sleep", lambda s: slept.append(s))
    local = FakeLocal()
    _call(
        local,
        cfg_for(),
        [
            FakeResponse(503, {"detail": "x"}, headers={"Retry-After": "9999"}),
            FakeResponse(200, {"predictions": [1.0]}),
        ],
    )
    assert slept == [nr.MAX_BACKOFF_SECONDS]


@pytest.mark.parametrize("status", [400, 403, 404, 413, 422])
def test_a_permanent_fault_is_not_retried(status):
    local = FakeLocal()
    with pytest.raises(nr.GatewayError) as excinfo:
        _call(local, cfg_for(), [FakeResponse(status, {"error": "nope"})])
    assert f"HTTP {status}" in str(excinfo.value)
    # The gateway's echoed body goes to the log, not to the caller: a private NORI_GATEWAY_URL can
    # name its own host in it. The endpoint never appears either.
    assert "nope" not in str(excinfo.value)
    assert "nope" in excinfo.value.log_text()
    assert nr.DEFAULT_GATEWAY_URL not in excinfo.value.log_text()


def test_a_read_timeout_is_not_retried():
    """It has already spent the whole budget, and usually means an ungranted slug."""
    import requests as real_requests

    local = FakeLocal()
    with pytest.raises(nr.GatewayError, match="did not respond within"):
        _call(local, cfg_for(), [real_requests.exceptions.ReadTimeout("timed out")])


def test_a_connection_error_is_retried():
    import requests as real_requests

    local = FakeLocal()
    preds, fake = _call(
        local,
        cfg_for(),
        [
            real_requests.exceptions.ConnectionError("reset"),
            FakeResponse(200, {"predictions": [2.0]}),
        ],
    )
    assert preds == [2.0]
    assert len(fake.calls) == 2


def test_repeated_connection_errors_exhaust_the_attempts():
    import requests as real_requests

    local = FakeLocal()
    with pytest.raises(nr.GatewayError, match="could not reach the Nori gateway after 2"):
        _call(
            local,
            cfg_for(max_retries="2"),
            [real_requests.exceptions.ConnectionError("reset")] * 2,
        )


def test_a_non_json_200_blames_the_gateway_not_the_caller():
    """A proxy in front of a scaled-to-zero model can answer 200 with an HTML page."""
    local = FakeLocal()
    with pytest.raises(nr.GatewayError, match="non-JSON body"):
        _call(
            local,
            cfg_for(),
            [FakeResponse(200, None, text="<html>502</html>", headers={"Content-Type": "text/html"})],
        )


def test_gateway_error_text_survives_a_non_json_error_body():
    local = FakeLocal()
    with pytest.raises(nr.GatewayError) as excinfo:
        _call(
            local,
            cfg_for(max_retries="1"),
            [FakeResponse(502, None, text="Bad Gateway", headers={})],
        )
    assert "Bad Gateway" in excinfo.value.log_text()


# ---------------------------------------------------------------------------
# Regression flow
# ---------------------------------------------------------------------------


def _regress(local, cfg, script, api_key="k"):
    fake = FakeRequests(script)
    original = nr.requests
    nr.requests = fake
    try:
        schema = nr._resolve_schema(local, cfg)
        return (
            nr._regress(
                local, cfg, schema, NOW - timedelta(days=30), NOW, api_key, "t"
            ),
            fake,
        )
    finally:
        nr.requests = original


def test_two_series_in_the_window_fail_loud():
    """The old guard only checked duplicate timestamps, so disjoint series trained as one."""
    data = labeled(60, site="A") + labeled(60, start=5000, site="B") + unlabeled(2, site="A")
    local = FakeLocal({"information_schema": SENSOR_SCHEMA, 'FROM "sensors"': data})
    with pytest.raises(nr.ConfigError) as excinfo:
        _regress(local, cfg_for(), [])
    message = str(excinfo.value)
    assert "holds 2 series" in message
    assert "site=A" in message and "site=B" in message


def test_a_tags_filter_isolates_one_series():
    data = labeled(60, site="A") + unlabeled(2, site="A")
    local = FakeLocal({"information_schema": SENSOR_SCHEMA, 'FROM "sensors"': data})
    (times, preds, series, _unf), fake = _regress(
        local, cfg_for(tags="site:A"), [FakeResponse(200, {"predictions": [1.0, 2.0]})]
    )
    assert len(preds) == 2
    assert series == {"site": "A"}
    assert fake.calls[0]["json"]["X_train"][0] == [20.0, 40.0]


def test_too_little_history_skips_without_calling_the_gateway():
    local = FakeLocal(
        {"information_schema": SENSOR_SCHEMA, 'FROM "sensors"': labeled(10) + unlabeled(2)}
    )
    (times, preds, _, _unf), fake = _regress(local, cfg_for(), [])
    assert (times, preds) == ([], [])
    assert fake.calls == []
    assert any("only 10 labeled rows" in w for w in local.warns)


def test_nothing_to_predict_skips_without_calling_the_gateway():
    local = FakeLocal({"information_schema": SENSOR_SCHEMA, 'FROM "sensors"': labeled(60)})
    (times, preds, _, _unf), fake = _regress(local, cfg_for(), [])
    assert (times, preds) == ([], [])
    assert fake.calls == []
    assert any("no rows to predict" in w for w in local.warns)


def test_rows_missing_a_feature_are_dropped():
    data = labeled(60) + [
        {"time": 99_000 * NS, "pressure": None, "temp": 21.0, "humidity": None, "site": "A"}
    ]
    local = FakeLocal({"information_schema": SENSOR_SCHEMA, 'FROM "sensors"': data})
    (times, preds, _, _unf), fake = _regress(local, cfg_for(), [])
    assert fake.calls == []


def test_non_numeric_and_non_finite_values_never_reach_the_gateway():
    data = labeled(60) + rows([(20_000, None, 21.0, 41.0)])
    data[0]["temp"] = float("inf")  # dropped: a feature must be finite
    data[1]["pressure"] = "not a number"  # dropped from training, and not a prediction target
    local = FakeLocal({"information_schema": SENSOR_SCHEMA, 'FROM "sensors"': data})
    (times, preds, _, _unf), fake = _regress(
        local, cfg_for(min_history="1"), [FakeResponse(200, {"predictions": [1.0, 2.0]})]
    )
    sent = fake.calls[0]["json"]
    assert all(all(math.isfinite(v) for v in row) for row in sent["X_train"])
    assert all(math.isfinite(v) for v in sent["y_train"])
    # 60 labeled rows, less the one with a non-finite feature (dropped entirely) and the one whose
    # target will not parse (a prediction target instead of training data).
    assert len(sent["X_train"]) == 58
    assert len(sent["X_test"]) == 2


def test_skip_existing_drops_rows_that_already_hold_a_prediction():
    data = labeled(60) + unlabeled(3)
    already = [{"time": 10_000 * NS}, {"time": 10_001 * NS}]
    local = FakeLocal(
        {
            "information_schema": SENSOR_SCHEMA,
            'FROM "sensors"': data,
            'FROM "sensors_regressed"': already,
        }
    )
    (times, preds, _, _unf), fake = _regress(
        local, cfg_for(), [FakeResponse(200, {"predictions": [7.0]})]
    )
    assert times == [10_002 * NS]
    assert len(fake.calls[0]["json"]["X_test"]) == 1
    lookup = [q for q in local.queries if "sensors_regressed" in q["sql"]][0]
    assert lookup["args"]["src"] == "sensors"
    assert lookup["args"]["tgt"] == "pressure"


def test_skip_existing_filters_by_series_so_a_sibling_trigger_cannot_interfere():
    local = FakeLocal(
        {
            "information_schema": SENSOR_SCHEMA,
            'FROM "sensors"': labeled(60) + unlabeled(1),
            'FROM "sensors_regressed"': [],
        }
    )
    _regress(local, cfg_for(), [FakeResponse(200, {"predictions": [1.0]})])
    lookup = [q for q in local.queries if "sensors_regressed" in q["sql"]][0]
    assert '"site" = $tag0' in lookup["sql"]
    assert lookup["args"]["tag0"] == "A"


def test_all_rows_already_predicted_is_a_no_op():
    local = FakeLocal(
        {
            "information_schema": SENSOR_SCHEMA,
            'FROM "sensors"': labeled(60) + unlabeled(2),
            'FROM "sensors_regressed"': [{"time": 10_000 * NS}, {"time": 10_001 * NS}],
        }
    )
    (times, preds, _, _unf), fake = _regress(local, cfg_for(), [])
    assert (times, preds) == ([], [])
    assert fake.calls == []


def test_skip_existing_false_re_predicts():
    local = FakeLocal(
        {
            "information_schema": SENSOR_SCHEMA,
            'FROM "sensors"': labeled(60) + unlabeled(2),
            'FROM "sensors_regressed"': [{"time": 10_000 * NS}, {"time": 10_001 * NS}],
        }
    )
    (times, preds, _, _unf), fake = _regress(
        local, cfg_for(skip_existing="false"), [FakeResponse(200, {"predictions": [1.0, 2.0]})]
    )
    assert len(preds) == 2
    assert not any("sensors_regressed" in q["sql"] for q in local.queries)


def test_a_missing_output_measurement_is_not_an_error():
    local = FakeLocal(
        {
            "information_schema": SENSOR_SCHEMA,
            'FROM "sensors"': labeled(60) + unlabeled(1),
            'FROM "sensors_regressed"': RuntimeError("table not found"),
        }
    )
    (times, preds, _, _unf), fake = _regress(
        local, cfg_for(), [FakeResponse(200, {"predictions": [1.0]})]
    )
    assert len(preds) == 1


def test_row_caps_keep_the_most_recent_rows():
    """The query has no LIMIT, so a wide window on a fast series must not build one huge payload."""
    local = FakeLocal(
        {"information_schema": SENSOR_SCHEMA, 'FROM "sensors"': labeled(200) + unlabeled(50)}
    )
    (times, preds, _, _unf), fake = _regress(
        local,
        cfg_for(max_train_rows="20", max_predict_rows="5", min_history="10"),
        [FakeResponse(200, {"predictions": [1.0] * 5})],
    )
    sent = fake.calls[0]["json"]
    assert len(sent["X_train"]) == 20
    assert len(sent["X_test"]) == 5
    assert sent["y_train"][-1] == 1000.0 + 199  # the newest labeled row
    assert times == [(10_000 + i) * NS for i in range(45, 50)]  # the newest unlabeled rows
    assert any("exceed max_train_rows" in w for w in local.warns)
    assert any("exceed max_predict_rows" in w for w in local.warns)


def test_a_failed_later_batch_keeps_the_batches_already_paid_for():
    """Discarding them would make the next run buy the same predictions a second time."""
    local = FakeLocal(
        {"information_schema": SENSOR_SCHEMA, 'FROM "sensors"': labeled(60) + unlabeled(6)}
    )
    (times, preds, _, _unf), fake = _regress(
        local,
        cfg_for(predict_batch_size="2", max_retries="1"),
        [
            FakeResponse(200, {"predictions": [1.0, 2.0]}),
            FakeResponse(404, {"error": "gone"}),
        ],
    )
    assert preds == [1.0, 2.0]
    assert len(times) == 2
    assert any("keeping the 2 predictions already returned" in w for w in local.warns)


def test_a_partial_run_is_not_reported_as_success():
    """8 of 20 rows written after a mid-run gateway fault is not "success" to a caller who cannot
    read the log."""
    local = FakeLocal(
        {"information_schema": SENSOR_SCHEMA, 'FROM "sensors"': labeled(60) + unlabeled(6)}
    )
    result, _ = _http(
        local,
        {},
        args={**BASE_ARGS, "predict_batch_size": "2", "max_retries": "1"},
        script=[
            FakeResponse(200, {"predictions": [1.0, 2.0]}),
            FakeResponse(404, {"error": "gone"}),
        ],
    )
    assert result["status"] == "partial"
    assert result["result"]["written"] == 2
    assert result["result"]["remaining"] == 4


def test_a_failed_first_batch_still_raises():
    local = FakeLocal(
        {"information_schema": SENSOR_SCHEMA, 'FROM "sensors"': labeled(60) + unlabeled(4)}
    )
    with pytest.raises(nr.GatewayError):
        _regress(
            local,
            cfg_for(predict_batch_size="2", max_retries="1"),
            [FakeResponse(404, {"error": "gone"})],
        )


def test_batching_reuses_one_training_context_per_call():
    local = FakeLocal(
        {"information_schema": SENSOR_SCHEMA, 'FROM "sensors"': labeled(60) + unlabeled(5)}
    )
    (times, preds, _, _unf), fake = _regress(
        local,
        cfg_for(predict_batch_size="2"),
        [
            FakeResponse(200, {"predictions": [1.0, 2.0]}),
            FakeResponse(200, {"predictions": [3.0, 4.0]}),
            FakeResponse(200, {"predictions": [5.0]}),
        ],
    )
    assert preds == [1.0, 2.0, 3.0, 4.0, 5.0]
    assert len(times) == 5
    assert [len(c["json"]["X_test"]) for c in fake.calls] == [2, 2, 1]
    # Every batch re-sends the context and is billed again, so the log has to say so.
    assert all(len(c["json"]["X_train"]) == 60 for c in fake.calls)
    assert any("billed separately" in i for i in local.infos)


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------


def test_predictions_are_written_with_the_source_series_tags():
    local = FakeLocal()
    written = nr._write_predictions(
        local, cfg_for(), [1_000 * NS, 2_000 * NS], [1.5, 2.5], {"site": "A"}, "t"
    )
    assert written == 2
    database, payload = local.writes[0]
    assert database is None
    lines = payload.split("\n")
    assert lines[0].startswith("sensors_regressed,")
    for expected in ("model=synthefy/nori-30m", "source=sensors", "target=pressure", "site=A"):
        assert expected in lines[0]
    assert lines[0].endswith(" value=1.5 1000000000000")


def test_a_null_prediction_is_skipped_and_reported():
    local = FakeLocal()
    written = nr._write_predictions(
        local, cfg_for(), [1_000 * NS, 2_000 * NS], [None, 2.5], {}, "t"
    )
    assert written == 1
    assert any("had no finite prediction" in w for w in local.warns)


def test_writes_can_target_another_database():
    local = FakeLocal()
    nr._write_predictions(
        local, cfg_for(target_database="preds"), [1_000 * NS], [1.0], {}, "t"
    )
    assert local.writes[0][0] == "preds"


def test_a_failing_write_raises_after_the_configured_attempts():
    local = FakeLocal()
    local.fail_writes = True
    with pytest.raises(RuntimeError, match="write refused"):
        nr._write_predictions(
            local, cfg_for(max_retries="2"), [1_000 * NS], [1.0], {}, "t"
        )


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def _http(local, body, args=None, headers=None, script=None):
    """Drive process_request. By default the trigger carries no arguments and the body supplies
    everything, which is the documented on-demand shape (a trigger argument pins its value)."""
    if args is None:
        # `model` is a trigger argument only (it selects a priced model), so it goes to the
        # trigger side even when the body supplies everything else.
        args = {"model": BASE_ARGS["model"]}
        if isinstance(body, dict):
            body = {**{k: v for k, v in BASE_ARGS.items() if k != "model"}, **body}
    fake = FakeRequests(script or [])
    original = nr.requests
    nr.requests = fake
    try:
        return (
            nr.process_request(
                local,
                {},
                headers if headers is not None else {"X-Nori-Api-Key": "k"},
                body if isinstance(body, (str, bytes)) or body is None else json.dumps(body),
                args,
            ),
            fake,
        )
    finally:
        nr.requests = original


def test_http_success_reports_the_real_outcome():
    local = FakeLocal(
        {"information_schema": SENSOR_SCHEMA, 'FROM "sensors"': labeled(60) + unlabeled(2)}
    )
    result, _ = _http(local, {}, script=[FakeResponse(200, {"predictions": [1.0, 2.0]})])
    assert result["status"] == "success"
    assert result["result"]["written"] == 2
    assert result["task_id"]


def test_http_skipped_is_not_reported_as_success():
    local = FakeLocal({"information_schema": SENSOR_SCHEMA, 'FROM "sensors"': labeled(60)})
    result, _ = _http(local, {})
    assert result["status"] == "skipped"


def test_http_dry_run_does_not_write():
    local = FakeLocal(
        {"information_schema": SENSOR_SCHEMA, 'FROM "sensors"': labeled(60) + unlabeled(1)}
    )
    result, _ = _http(
        local, {"dry_run": True}, script=[FakeResponse(200, {"predictions": [1.0]})]
    )
    assert result["status"] == "dry_run"
    assert local.writes == []


@pytest.mark.parametrize("body", ["{not json", b"\xff\xfe", "[1,2,3]", '"a string"'])
def test_a_bad_request_body_is_reported_as_such(body):
    local = FakeLocal()
    result, _ = _http(local, body)
    assert result["status"] == "failed"
    assert "body" in result["message"]


@pytest.mark.parametrize("body", [12345, 3.5, ["a"], object()])
def test_an_unexpected_body_type_stays_inside_the_error_contract(body):
    """An unguarded len() would raise TypeError straight out of process_request."""
    local = FakeLocal()
    result = nr.process_request(local, {}, {"X-Nori-Api-Key": "k"}, body, dict(BASE_ARGS))
    assert result["status"] == "failed"
    assert "request body must be JSON text" in result["message"]


def test_rfc3339_treats_a_naive_datetime_as_utc():
    """astimezone() on a naive value would otherwise read the host's local timezone."""
    assert nr._rfc3339(datetime(2026, 6, 1, 12, 0)) == "2026-06-01T12:00:00Z"


def test_a_body_override_attempt_is_reported_to_the_caller():
    local = FakeLocal()
    result, _ = _http(local, {"gateway_url": "https://attacker.example/x"})
    assert result["status"] == "failed"
    assert "gateway_url" in result["message"]
    assert local.writes == []


def test_a_config_error_message_reaches_the_caller():
    local = FakeLocal({"information_schema": SENSOR_SCHEMA})
    result, _ = _http(local, {"field": "status"})
    assert result["status"] == "failed"
    assert "not a numeric field" in result["message"]


def test_an_unexpected_error_does_not_leak_internal_detail():
    """A storage error can name the target database and the endpoint, so it stays in the log."""
    local = FakeLocal(
        {
            "information_schema": SENSOR_SCHEMA,
            'FROM "sensors"': labeled(60) + unlabeled(1),
        }
    )
    local.fail_writes = True
    result, _ = _http(
        local,
        {},
        args={**BASE_ARGS, "target_database": "secret_db", "max_retries": "1"},
        script=[FakeResponse(200, {"predictions": [1.0]})],
    )
    assert result["status"] == "failed"
    assert result["message"] == "internal error; see the plugin logs for this task_id"
    assert "secret_db" not in result["message"]
    assert any("secret_db" in e or "write refused" in e for e in local.errors)


def test_a_missing_api_key_is_reported_before_any_gateway_call():
    local = FakeLocal({"information_schema": SENSOR_SCHEMA})
    result, fake = _http(local, {}, headers={})
    assert result["status"] == "failed"
    assert nr.API_KEY_ENV_VAR in result["message"]
    assert fake.calls == []


def test_scheduled_run_writes_predictions(monkeypatch):
    monkeypatch.setenv(nr.API_KEY_ENV_VAR, "k")
    local = FakeLocal(
        {"information_schema": SENSOR_SCHEMA, 'FROM "sensors"': labeled(60) + unlabeled(2)}
    )
    fake = FakeRequests([FakeResponse(200, {"predictions": [1.0, 2.0]})])
    original = nr.requests
    nr.requests = fake
    try:
        nr.process_scheduled_call(local, datetime(2026, 6, 1, 12, 0), dict(BASE_ARGS))
    finally:
        nr.requests = original
    assert local.errors == []
    assert len(local.writes) == 1


def test_scheduled_run_anchors_the_window_to_call_time(monkeypatch):
    monkeypatch.setenv(nr.API_KEY_ENV_VAR, "k")
    local = FakeLocal({"information_schema": SENSOR_SCHEMA})
    nr.process_scheduled_call(
        local, datetime(2026, 6, 1, 12, 0), {**BASE_ARGS, "window": "1h"}
    )
    data_query = [q for q in local.queries if "information_schema" not in q["sql"]][0]
    assert data_query["args"]["start_ts"] == "2026-06-01T11:00:00Z"
    assert data_query["args"]["end_ts"] == "2026-06-01T12:00:00Z"


def test_a_scheduled_failure_is_logged_not_raised():
    local = FakeLocal()
    nr.process_scheduled_call(local, datetime(2026, 6, 1, 12, 0), {"measurement": "sensors"})
    assert local.errors and "ConfigError" in local.errors[0]
