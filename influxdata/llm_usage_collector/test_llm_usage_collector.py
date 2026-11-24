import base64
import builtins
import copy
import json
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).parent))

import llm_usage_collector as plugin  # noqa: E402
from llm_usage_collector import (  # noqa: E402
    CONNECTORS,
    PROVIDERS,
    USAGE_UNITS,
    ConfigError,
    Connector,
    CostRecord,
    HttpClient,
    ProviderError,
    UsageRecord,
    collect,
    cost_window,
    load_settings,
    build_usage_line,
    prepare,
    process_request,
    read_secret,
    usage_window,
    write_records,
)

DEFAULT_SECRETS = {
    "ANTHROPIC_ADMIN_KEY": "sk-ant-admin01-secret",
    "OPENAI_ADMIN_KEY": "sk-admin-secret",
    "BEDROCK_AWS_ACCESS_KEY_ID": "AKIA-bedrock",
    "BEDROCK_AWS_SECRET_ACCESS_KEY": "bedrock-secret",
    "AZURE_CLIENT_SECRET": "azure-client-secret",
}


@pytest.fixture(autouse=True)
def default_secrets(monkeypatch):
    """The default secret variables are set; a test about a missing secret removes its variable."""
    for name, value in DEFAULT_SECRETS.items():
        monkeypatch.setenv(name, value)


class FakeCache:
    def __init__(self):
        self.values = {}

    def get(self, key, default=None):
        return self.values.get(key, default)

    def put(self, key, value, ttl=None):
        self.values[key] = value


class FakeLocal:
    def __init__(self):
        self.messages = {"info": [], "warn": [], "error": []}
        self.written = []
        self.rejected_text = None
        self.cache = FakeCache()

    def info(self, message):
        self.messages["info"].append(message)

    def warn(self, message):
        self.messages["warn"].append(message)

    def error(self, message):
        self.messages["error"].append(message)

    def write_sync(self, payload, no_sync=False):
        self.write_sync_to_db(None, payload, no_sync)

    def write_sync_to_db(self, database, payload, no_sync=False):
        built = payload.build()
        if self.rejected_text and self.rejected_text in built:
            raise RuntimeError("line rejected")
        self.written.append((database, built))


class FakeLineBuilder:
    def __init__(self, measurement):
        self.row = {"measurement": measurement, "tags": {}, "fields": {}, "time": None}

    def tag(self, key, value):
        self.row["tags"][key] = value
        return self

    def float64_field(self, key, value):
        self.row["fields"][key] = float(value)
        return self

    def string_field(self, key, value):
        self.row["fields"][key] = str(value)
        return self

    def time_ns(self, value):
        self.row["time"] = value
        return self

    def build(self):
        return json.dumps(self.row)


builtins.LineBuilder = FakeLineBuilder


def written_rows(local):
    """Split every recorded payload into lines; return (usage rows, cost rows)."""
    usage, cost = [], []
    for _database, payload in local.written:
        for line in payload.splitlines():
            row = json.loads(line)
            target = usage if row["measurement"] == "llm_usage" else cost
            target.append(
                {"tags": row["tags"], "fields": row["fields"], "time": row["time"]}
            )
    return usage, cost


def test_defaults_for_scheduled_trigger():
    s = load_settings(FakeLocal(), "t", {"providers": "anthropic openai"})
    assert s.providers == ["anthropic", "openai"]
    assert s.collect == ["usage", "cost"]
    assert s.usage_types == list(USAGE_UNITS)
    assert (s.usage_table, s.cost_table, s.bucket_width) == ("llm_usage", "llm_cost", "1h")
    assert s.exclude_tags == []
    assert (s.usage_lookback, s.cost_lookback, s.cost_interval) == (timedelta(hours=48), timedelta(days=40), timedelta(hours=6))
    assert s.provider["anthropic_admin_key_env"] == "ANTHROPIC_ADMIN_KEY"
    assert s.provider["anthropic_account"] == "default"
    assert s.provider["google_credentials_env"] == "GOOGLE_LLM_USAGE_CREDENTIALS"
    assert s.provider["google_account"] == "default"
    assert s.provider["google_sources"] == ["vertex_metrics", "gemini_api_metrics"]
    assert s.provider["google_billing_table"] is None


def test_list_settings_accept_string_or_list(tmp_path):
    toml = tmp_path / "c.toml"; toml.write_text('providers = ["bedrock"]\nbedrock_regions = ["us-east-1", "eu-west-1"]\n')
    s = load_settings(FakeLocal(), "t", {"config_file_path": str(toml)})
    assert s.provider["bedrock_regions"] == ["us-east-1", "eu-west-1"]


def test_metadata_lists_the_settings_each_source_accepts():
    meta = json.loads(plugin.__doc__)
    names = lambda section: [entry["name"] for entry in meta[section]]
    assert set(names("scheduled_args_config")) == set(plugin.SCHEDULED_NAMES)
    assert set(names("http_args_config")) == set(plugin.HTTP_NAMES)
    assert set(names("http_body_config")) == set(plugin.REQUEST_NAMES)


def test_unknown_setting_is_refused():
    with pytest.raises(ConfigError, match="nope"):
        load_settings(FakeLocal(), "t", {"providers": "openai", "nope": "1"})


@pytest.mark.parametrize("key", ["anthropic_admin_key_env", "google_credentials_env", "config_file_path", "bedrock_use_default_credentials"])
def test_request_cannot_set_secret_variable_names(key):
    with pytest.raises(ConfigError, match=key):
        load_settings(FakeLocal(), "t", {"providers": "anthropic"}, request_body=json.dumps({"start": "2026-10-01T00:00:00Z", key: "X"}), http=True)


def test_default_credentials_flag_comes_from_trigger_arguments_and_needs_no_secret(monkeypatch):
    monkeypatch.delenv("BEDROCK_AWS_ACCESS_KEY_ID")
    monkeypatch.delenv("BEDROCK_AWS_SECRET_ACCESS_KEY")
    args = {"providers": "bedrock", "bedrock_regions": "us-east-1", "bedrock_use_default_credentials": "true"}
    assert load_settings(FakeLocal(), "t", args).provider["bedrock_use_default_credentials"] is True


def test_request_providers_must_be_a_subset_of_the_trigger():
    with pytest.raises(ConfigError, match="azure"):
        load_settings(FakeLocal(), "t", {"providers": "openai"}, request_body=json.dumps({"providers": ["azure"], "start": "2026-10-01T00:00:00Z"}), http=True)


def test_http_period_parsing_and_errors():
    now = datetime(2026, 11, 14, 12, tzinfo=timezone.utc)
    s = load_settings(FakeLocal(), "t", {"providers": "openai"}, request_body=json.dumps({"start": "2026-10-01T00:00:00Z"}), http=True, now=now)
    assert (s.start, s.end) == (datetime(2026, 10, 1, tzinfo=timezone.utc), now)
    with pytest.raises(ConfigError, match="start"):
        load_settings(FakeLocal(), "t", {"providers": "openai"}, request_body="{}", http=True)
    with pytest.raises(ConfigError, match="end"):
        load_settings(FakeLocal(), "t", {"providers": "openai"}, request_body=json.dumps({"start": "2026-10-02T00:00:00Z", "end": "2026-10-01T00:00:00Z"}), http=True)


def accepted_keys(error) -> set[str]:
    """The keys an unknown-key error lists as accepted."""
    return set(str(error.value).split("accepted keys: ")[1].strip("[]").replace("'", "").split(", "))


def test_scheduled_run_reads_no_request_period(fake_connectors, monkeypatch, tmp_path):
    calls = fake_connectors(good=[])
    monkeypatch.setenv("INFLUXDB3_LLM_USAGE_START", "not a time")
    local = FakeLocal()
    plugin.process_scheduled_call(local, NOW, {"providers": "fake_ok"})
    assert local.messages["error"] == [] and len(calls["usage"]) == 1
    with pytest.raises(ConfigError, match="^Trigger arguments may not set 'start'; accepted keys: ") as error:
        load_settings(FakeLocal(), "t", {"providers": "openai", "start": "2026-10-01T00:00:00Z"})
    assert "usage_lookback" in accepted_keys(error) and not {"start", "end"} & accepted_keys(error)
    toml = tmp_path / "c.toml"; toml.write_text('end = "2026-10-02T00:00:00Z"\n')
    with pytest.raises(ConfigError, match="^Config file may not set 'end'"):
        load_settings(FakeLocal(), "t", {"providers": "openai", "config_file_path": str(toml)})


def test_request_reads_no_scheduled_window(fake_connectors, monkeypatch):
    fake_connectors(good=[])
    monkeypatch.setenv("INFLUXDB3_LLM_USAGE_USAGE_LOOKBACK", "not a duration")
    body = json.dumps({"start": "2026-10-01T00:00:00Z", "end": "2026-10-02T00:00:00Z"})
    assert process_request(FakeLocal(), {}, {}, body, {"providers": "fake_ok"})["status"] == "completed"
    with pytest.raises(ConfigError, match="^Trigger arguments may not set 'usage_lookback'; accepted keys: ") as error:
        load_settings(FakeLocal(), "t", {"providers": "openai", "usage_lookback": "12h"}, request_body=body, http=True)
    assert "start" in accepted_keys(error) and not {"usage_lookback", "cost_lookback", "cost_interval"} & accepted_keys(error)
    with pytest.raises(ConfigError, match="^Request body may not set 'cost_interval'"):
        load_settings(FakeLocal(), "t", {"providers": "openai"}, request_body=json.dumps({"start": "2026-10-01T00:00:00Z", "cost_interval": "1h"}), http=True)


@pytest.mark.parametrize("args,match", [
    ({"providers": "openai", "exclude_tags": "account model"}, "model"),
    ({"providers": "openai", "collect": "usage tokens"}, "collect: unknown value tokens"),
    ({"providers": "openai", "bucket_width": "5m"}, "bucket_width"),
    ({"providers": "openai", "usage_lookback": "30min"}, "usage_lookback"),
    ({"providers": "openai", "bucket_width": "1d", "usage_lookback": "12h"}, "usage_lookback"),
    ({"providers": "openai", "cost_lookback": "12h"}, "cost_lookback"),
    ({"providers": "vertex"}, "vertex"),
    ({"providers": "google", "google_sources": "vertex_metrics bigquery"}, "unknown source bigquery"),
])
def test_invalid_values_are_named(args, match):
    with pytest.raises(ConfigError, match=match):
        load_settings(FakeLocal(), "t", args)


def test_env_settings_and_secret(monkeypatch):
    monkeypatch.setenv("INFLUXDB3_LLM_USAGE_PROVIDERS", "openai")
    monkeypatch.setenv("MY_KEY", "sk-admin-x")
    s = load_settings(FakeLocal(), "t", {"openai_admin_key_env": "MY_KEY"})
    assert s.providers == ["openai"] and read_secret(s.provider["openai_admin_key_env"]) == "sk-admin-x"
    with pytest.raises(ConfigError, match="UNSET_VAR"):
        read_secret("UNSET_VAR")


def test_secret_whitespace_stripped_and_control_characters_refused(monkeypatch):
    monkeypatch.setenv("FROM_FILE", "sk-admin-x\n")
    assert read_secret("FROM_FILE") == "sk-admin-x"
    monkeypatch.setenv("BROKEN", "sk-ad\rmin-SECRET")
    with pytest.raises(ConfigError) as e:
        read_secret("BROKEN")
    assert "BROKEN" in str(e.value) and "SECRET" not in str(e.value)


T0 = datetime(2026, 11, 12, 16, tzinfo=timezone.utc)


def test_excluded_tag_sums_colliding_rows():
    rows = prepare(
        [
            UsageRecord(T0, "openai", "input_tokens", 100, {"api_key_id": "k1", "model": "m"}, "a"),
            UsageRecord(T0, "openai", "input_tokens", 50, {"api_key_id": "k2", "model": "m"}, "b"),
        ],
        ["api_key_id"],
    )
    assert len(rows) == 1 and rows[0].quantity == 150
    assert "api_key_id" not in rows[0].tags and rows[0].api_key_name is None


def test_identical_rows_from_two_sources_are_summed_and_zeros_dropped():
    rows = prepare(
        [
            UsageRecord(T0, "bedrock", "input_tokens", 30, {"model": "m", "region": "us-east-1"}),
            UsageRecord(T0, "bedrock", "input_tokens", 12, {"model": "m", "region": "us-east-1"}),
            UsageRecord(T0, "bedrock", "requests", 0, {"model": "m", "region": "us-east-1"}),
            CostRecord(T0, "openai", "x, cached input", "usd", 0.0, {}),
        ],
        [],
    )
    assert [(r.usage_type, r.quantity) for r in rows] == [("input_tokens", 42)]


def test_api_key_name_kept_only_when_unambiguous():
    rows = prepare(
        [
            UsageRecord(T0, "anthropic", "output_tokens", 1, {"api_key_id": "k", "user_id": "u1"}, "old"),
            UsageRecord(T0, "anthropic", "output_tokens", 2, {"api_key_id": "k", "user_id": "u2"}, "new"),
        ],
        ["user_id"],
    )
    assert rows[0].quantity == 3 and rows[0].api_key_name is None


def test_cent_sums_do_not_drift_and_empty_tags_are_dropped():
    rows = prepare(
        [CostRecord(T0, "openai", "x", "usd", 0.1, {"model": ""}) for _ in range(3)], []
    )
    assert rows[0].cost == 0.3 and rows[0].tags == {}


def test_currency_case_does_not_split_a_cost_row():
    rows = prepare([CostRecord(T0, "openai", "x", "usd", 0.1, {}), CostRecord(T0, "openai", "x", "USD", 0.2, {})], [])
    assert [(r.currency.upper(), r.cost) for r in rows] == [("USD", 0.3)]


def test_rows_are_written_in_batches_and_a_failed_batch_counts_what_landed(monkeypatch):
    monkeypatch.setattr(plugin, "WRITE_BATCH_LINES", 2)
    monkeypatch.setattr("influxdata_plugin_utils.write.time.sleep", lambda seconds: None)
    s = load_settings(FakeLocal(), "t", {"providers": "openai"})
    usage = [UsageRecord(T0 + timedelta(hours=i), "openai", "requests", 1, {"model": f"m{i}"}) for i in range(5)]
    local = FakeLocal()
    assert write_records(local, s, usage, []) == (5, 0) and len(local.written) == 3
    local = FakeLocal()
    local.rejected_text = '"model": "m2"'
    with pytest.raises(plugin.WriteFailed) as failed:
        write_records(local, s, usage, [])
    assert failed.value.written == 2 and len(local.written) == 1


def test_written_rows_have_the_design_layout():
    local = FakeLocal()
    s = load_settings(local, "t", {"providers": "openai", "target_database": "llm"})
    written = write_records(
        local,
        s,
        [UsageRecord(T0, "openai", "cache_read_tokens", 2048, {"account": "default", "model": "gpt-5-nano-2025-08-07", "project": "proj_1", "api_key_id": "key_1", "user_id": "user-1"}, "test")],
        [CostRecord(T0.replace(hour=0), "openai", "gpt-5-nano-2025-08-07, cached input", "usd", 1.024e-05, {"model": "gpt-5-nano-2025-08-07"})],
    )
    assert written == (1, 1)
    usage, cost = written_rows(local)
    assert usage[0]["tags"] == {"provider": "openai", "account": "default", "model": "gpt-5-nano-2025-08-07", "project": "proj_1", "api_key_id": "key_1", "user_id": "user-1", "usage_type": "cache_read_tokens", "unit": "tokens"}
    assert usage[0]["fields"] == {"quantity": 2048.0, "api_key_name": "test"}
    assert usage[0]["time"] == int(T0.timestamp()) * 10**9
    assert cost[0]["tags"]["currency"] == "USD" and cost[0]["fields"] == {"cost": 1.024e-05}
    assert local.written[0][0] == "llm"


NOW = datetime(2026, 11, 14, 10, 37, tzinfo=timezone.utc)


class FakeResponse:
    def __init__(self, status, body, headers=None):
        self.status_code, self.body, self.headers = status, body, headers or {}
        self.text = json.dumps(body)
        self.content = self.text.encode()

    def json(self):
        return self.body


class FakeSession:
    """Serves the routed responses in order and repeats the last one."""

    def __init__(self):
        self.routes, self.calls = {}, []

    def close(self):
        pass

    def route(self, method, url, responses):
        self.routes[(method, url)] = list(responses)
        return self

    def request(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        queue = self.routes[(method, url)]
        item = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(item, Exception):
            raise item
        return item if isinstance(item, FakeResponse) else FakeResponse(200, item)


def settings_for(providers, **overrides):
    return load_settings(FakeLocal(), "t", {"providers": providers, **overrides})


@pytest.fixture
def fake_connectors(monkeypatch):
    """Register fake_ok and fake_bad; an exception in a result list is raised when reached.

    `good` may be a function of the fetched window that returns its records.
    """

    def install(good, bad=None, cost=(), oldest_usage=None, collects_cost=True):
        calls = {"usage": [], "cost": []}

        def emit(items):
            if isinstance(items, Exception):
                raise items
            for item in items:
                if isinstance(item, Exception):
                    raise item
                yield item

        class FakeOk(Connector):
            name = "fake_ok"
            usage_types = frozenset(USAGE_UNITS)

            def oldest_usage(self, now):
                return oldest_usage

            def collects_cost(self):
                return collects_cost

            def fetch_usage(self, start, end, bucket_width, usage_types):
                calls["usage"].append((start, end, bucket_width, usage_types))
                yield from emit(good(start, end) if callable(good) else good)

            def fetch_cost(self, start_day, end_day):
                calls["cost"].append((start_day, end_day))
                yield from emit(cost)

        class FakeBad(FakeOk):
            name = "fake_bad"

            def fetch_usage(self, start, end, bucket_width, usage_types):
                yield from emit(bad)

            def fetch_cost(self, start_day, end_day):
                yield from ()

        monkeypatch.setattr(plugin, "PROVIDERS", PROVIDERS + ("fake_ok", "fake_bad"))
        monkeypatch.setitem(CONNECTORS, "fake_ok", FakeOk)
        monkeypatch.setitem(CONNECTORS, "fake_bad", FakeBad)
        return calls

    return install


def test_http_client_retries_and_honours_retry_after():
    session = FakeSession().route("GET", "https://x/a", [FakeResponse(429, {}, {"Retry-After": "7"}), FakeResponse(200, {"ok": 1})])
    slept = []
    assert HttpClient(session, sleep=slept.append).request_json("GET", "https://x/a") == {"ok": 1}
    assert slept == [7.0]


def test_http_client_error_never_contains_the_key():
    session = FakeSession().route("GET", "https://x/a", [FakeResponse(401, {"error": {"message": "bad key sk-ant-admin-SECRET"}})])
    with pytest.raises(ProviderError) as e:
        HttpClient(session).request_json("GET", "https://x/a", headers={"x-api-key": "sk-ant-admin-SECRET"})
    assert e.value.status == 401 and "SECRET" not in str(e.value) and "bad key" in str(e.value)


def test_http_client_masks_bearer_and_form_secrets_even_across_the_truncation_point():
    key, padding = "sk-proj-BEARERKEY1234", "x" * (plugin.MAX_ERROR_TEXT - 6)
    session = FakeSession().route("GET", "https://x/a", [FakeResponse(401, {"error": {"message": f"bad {key}"}})])
    session.route("GET", "https://x/b", [FakeResponse(401, {"error": {"message": f"{padding}{key}"}})])
    session.route("POST", "https://x/c", [FakeResponse(400, {"error": "bad client-secret-VALUE"})])
    client = HttpClient(session)
    with pytest.raises(ProviderError, match="bad \\*\\*\\*"):
        client.request_json("GET", "https://x/a", headers={"Authorization": f"Bearer {key}"})
    with pytest.raises(ProviderError) as straddling:
        client.request_json("GET", "https://x/b", headers={"Authorization": f"Bearer {key}"})
    assert "sk-" not in str(straddling.value)
    with pytest.raises(ProviderError) as form:
        client.request_json("POST", "https://x/c", data={"client_secret": "client-secret-VALUE"})
    assert "VALUE" not in str(form.value)


@pytest.mark.parametrize("header,expected", [("-5", 0.0), ("inf", 1.0), ("nan", 1.0), ("9999", 120.0), ("3", 3.0)])
def test_http_client_bounds_retry_after(header, expected):
    session = FakeSession().route("GET", "https://x/a", [FakeResponse(429, {}, {"Retry-After": header}), FakeResponse(200, {"ok": 1})])
    slept = []
    HttpClient(session, sleep=slept.append).request_json("GET", "https://x/a")
    assert slept == [expected]


def test_http_client_waits_for_the_longest_cost_management_quota():
    headers = {"x-ms-ratelimit-microsoft.costmanagement-clienttype-retry-after": "20",
               "x-ms-ratelimit-microsoft.costmanagement-entity-retry-after": "55",
               "x-ms-ratelimit-microsoft.costmanagement-qpu-remaining": "QueriesPerHour:521"}
    session = FakeSession().route("POST", "https://x/a", [FakeResponse(429, {}, headers), FakeResponse(200, {"ok": 1})])
    slept = []
    HttpClient(session, sleep=slept.append).request_json("POST", "https://x/a")
    assert slept == [55.0]


def test_http_client_hides_request_exception_text():
    session = FakeSession().route("GET", "https://x/a", [requests.exceptions.InvalidHeader("Invalid header value 'sk-ant-admin-SECRET\\n'")])
    with pytest.raises(ProviderError) as e:
        HttpClient(session).request_json("GET", "https://x/a")
    assert e.value.status is None and "SECRET" not in str(e.value)


def test_http_client_backs_off_exponentially_and_gives_up():
    session = FakeSession().route("GET", "https://x/a", [FakeResponse(529, {})])
    slept = []
    with pytest.raises(ProviderError) as e:
        HttpClient(session, sleep=slept.append, max_retries=3).request_json("GET", "https://x/a")
    assert slept == [1, 2, 4] and e.value.status == 529 and len(session.calls) == 4


def test_http_client_retries_transport_errors_three_times():
    session = FakeSession().route("GET", "https://x/a", [requests.ConnectionError("reset"), requests.ConnectionError("reset"), FakeResponse(200, {"ok": 1})])
    session.route("GET", "https://x/b", [requests.ReadTimeout("slow")])
    slept = []
    client = HttpClient(session, sleep=slept.append)
    assert client.request_json("GET", "https://x/a") == {"ok": 1} and slept == [1, 2]
    with pytest.raises(ProviderError) as e:
        client.request_json("GET", "https://x/b")
    assert (e.value.status, str(e.value)) == (None, "ReadTimeout") and slept == [1, 2, 1, 2, 4]
    assert len([c for c in session.calls if c["url"] == "https://x/b"]) == 4


def test_usage_and_cost_windows():
    assert usage_window(NOW, "1h", timedelta(hours=48)) == (datetime(2026, 11, 12, 10, tzinfo=timezone.utc), datetime(2026, 11, 14, 10, tzinfo=timezone.utc))
    assert usage_window(NOW, "1d", timedelta(days=3)) == (datetime(2026, 11, 11, tzinfo=timezone.utc), datetime(2026, 11, 14, tzinfo=timezone.utc))
    assert cost_window(NOW, timedelta(days=40)) == (date(2026, 10, 5), date(2026, 11, 15))
    assert cost_window(NOW, timedelta(hours=36)) == (date(2026, 11, 12), date(2026, 11, 15))


def test_backfill_reads_whole_complete_buckets_only(fake_connectors):
    fake_connectors(good=[])

    def period(**body):
        settings = load_settings(FakeLocal(), "t", {"providers": "fake_ok"}, request_body=json.dumps(
            {"start": "2026-11-14T02:20:00Z", "collect": ["usage"], **body}), http=True, now=NOW)
        return collect(FakeLocal(), settings, "t", now=NOW, backfill=True)["fake_ok"]["usage_period"]

    assert period(end="2026-11-14T05:30:00Z") == ["2026-11-14T02:00:00+00:00", "2026-11-14T06:00:00+00:00"]
    assert period() == ["2026-11-14T02:00:00+00:00", "2026-11-14T10:00:00+00:00"]
    assert period(end="2026-11-15T00:00:00Z") == ["2026-11-14T02:00:00+00:00", "2026-11-14T10:00:00+00:00"]


def test_backfill_cost_stops_at_today(fake_connectors):
    fake_connectors(good=[])
    settings = load_settings(FakeLocal(), "t", {"providers": "fake_ok"}, request_body=json.dumps(
        {"start": "2026-11-01T00:00:00Z", "end": "2027-01-01T00:00:00Z", "collect": ["cost"]}), http=True, now=NOW)
    assert collect(FakeLocal(), settings, "t", now=NOW, backfill=True)["fake_ok"]["cost_period"] == ["2026-11-01", "2026-11-15"]


def test_one_failing_connector_does_not_stop_the_others(fake_connectors):
    fake_connectors(good=[UsageRecord(NOW, "fake_ok", "requests", 3, {})], bad=RuntimeError("boom"))
    local = FakeLocal()
    result = collect(local, settings_for("fake_ok fake_bad"), "t", now=NOW, backfill=False)
    assert result["fake_ok"]["usage_rows"] == 1
    assert "boom" in result["fake_bad"]["errors"][0] and any("[t]" in e for e in local.messages["error"])


def test_cost_runs_only_after_the_interval(fake_connectors):
    fake_connectors(good=[], cost=[CostRecord(NOW, "fake_ok", "b", "USD", 1.0, {})])
    local, s = FakeLocal(), settings_for("fake_ok", cost_interval="6h")
    assert collect(local, s, "t", now=NOW, backfill=False)["fake_ok"]["cost_rows"] == 1
    assert collect(local, s, "t", now=NOW + timedelta(hours=1), backfill=False)["fake_ok"]["cost_period"] is None
    assert collect(local, s, "t", now=NOW + timedelta(hours=7), backfill=False)["fake_ok"]["cost_rows"] == 1


def test_provider_without_cost_source_skips_cost(fake_connectors):
    calls = fake_connectors(good=[], cost=[CostRecord(NOW, "fake_ok", "b", "USD", 1.0, {})], collects_cost=False)
    local = FakeLocal()
    result = collect(local, settings_for("fake_ok"), "t", now=NOW, backfill=False)["fake_ok"]
    assert calls["cost"] == [] and result["cost_period"] is None
    assert local.cache.get("cost_fetched_at:fake_ok") is None


def test_service_tag_written_after_region():
    tags = {"service": "vertex", "region": "us-central1", "project": "p", "model": "m", "account": "a"}
    line = build_usage_line("llm_usage", UsageRecord(NOW, "google", "requests", 1, tags))
    assert list(line.row["tags"]) == ["provider", "account", "model", "project", "region", "service", "usage_type", "unit"]


def test_backfill_clamps_to_oldest_history_and_says_so(fake_connectors):
    calls = fake_connectors(good=[], oldest_usage=datetime(2026, 1, 1, tzinfo=timezone.utc))
    body = json.dumps({"start": "2025-06-01T00:00:00Z", "end": "2026-02-01T00:00:00Z"})
    result = process_request(FakeLocal(), {}, {}, body, {"providers": "fake_ok"})
    assert result["status"] == "completed"
    assert result["providers"]["fake_ok"]["usage_period"][0] == "2026-01-01T00:00:00+00:00"
    assert result["providers"]["fake_ok"]["notes"] and calls["usage"][0][0] == datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_backfill_start_clamped_to_unaligned_history_is_aligned_to_the_bucket(fake_connectors):
    calls = fake_connectors(good=[], oldest_usage=datetime(2026, 1, 1, 10, 37, 12, tzinfo=timezone.utc))
    body = json.dumps({"start": "2025-06-01T00:00:00Z", "end": "2026-02-01T00:00:00Z"})
    result = process_request(FakeLocal(), {}, {}, body, {"providers": "fake_ok", "bucket_width": "1d"})["providers"]["fake_ok"]
    expected = datetime(2026, 1, 2, tzinfo=timezone.utc)
    assert calls["usage"][0][0] == expected
    assert expected.isoformat() in result["notes"][0] and result["usage_period"][0] == expected.isoformat()


def test_repeated_list_items_are_dropped_in_order():
    args = {
        "providers": "openai bedrock openai",
        "usage_types": "input_tokens output_tokens input_tokens",
        "bedrock_regions": "us-east-1 eu-west-1 us-east-1",
    }
    settings = load_settings(FakeLocal(), "t", args)
    assert settings.providers == ["openai", "bedrock"]
    assert settings.usage_types == ["input_tokens", "output_tokens"]
    assert settings.provider["bedrock_regions"] == ["us-east-1", "eu-west-1"]


def test_a_failed_write_is_recorded_for_its_provider_only(fake_connectors, monkeypatch):
    monkeypatch.setattr("influxdata_plugin_utils.write.time.sleep", lambda seconds: None)
    fake_connectors(good=[UsageRecord(NOW, "fake_ok", "requests", 3, {})], bad=[UsageRecord(NOW, "fake_bad", "requests", 1, {})])
    local = FakeLocal()
    local.rejected_text = "fake_bad"
    results = collect(local, settings_for("fake_ok fake_bad"), "t", now=NOW, backfill=False)
    assert results["fake_ok"]["errors"] == [] and results["fake_ok"]["usage_rows"] == 1
    assert "line rejected" in results["fake_bad"]["errors"][0] and results["fake_bad"]["usage_rows"] == 0
    assert len(written_rows(local)[0]) == 1


def test_request_with_bad_settings_returns_an_error():
    assert process_request(FakeLocal(), {}, {}, "{}", {"providers": "openai"})["status"] == "error"
    assert process_request(FakeLocal(), {}, {}, json.dumps({"start": "2026-10-01T00:00:00Z", "nope": 1}), {"providers": "openai"})["status"] == "error"


def test_backfill_aligns_to_buckets_and_days(fake_connectors):
    calls = fake_connectors(good=[])
    body = json.dumps({"start": "2026-11-12T10:37:00Z", "end": "2026-11-13T05:10:00Z"})
    process_request(FakeLocal(), {}, {}, body, {"providers": "fake_ok"})
    assert calls["usage"][0][:2] == (datetime(2026, 11, 12, 10, tzinfo=timezone.utc), datetime(2026, 11, 13, 6, tzinfo=timezone.utc))
    assert calls["cost"][0] == (date(2026, 11, 12), date(2026, 11, 14))


SLICED_BACKFILL = json.dumps({"start": "2026-10-01T00:00:00Z", "end": "2026-10-21T00:00:00Z"})


def test_backfill_is_written_slice_by_slice_and_the_slices_tile_the_period(fake_connectors):
    calls = fake_connectors(good=lambda start, end: [UsageRecord(start, "fake_ok", "requests", 1, {})])
    local = FakeLocal()
    result = process_request(local, {}, {}, SLICED_BACKFILL, {"providers": "fake_ok"})["providers"]["fake_ok"]
    starts = [datetime(2026, 10, day, tzinfo=timezone.utc) for day in (1, 8, 15, 21)]
    assert [window[:2] for window in calls["usage"]] == list(zip(starts, starts[1:]))
    assert result["usage_period"] == [starts[0].isoformat(), starts[-1].isoformat()]
    assert result["usage_rows"] == 3 and len(local.written) == 3


def test_failed_slice_writes_are_recorded_per_slice_and_the_other_slices_are_written(fake_connectors, monkeypatch):
    monkeypatch.setattr("influxdata_plugin_utils.write.time.sleep", lambda seconds: None)
    fake_connectors(good=lambda start, end: [UsageRecord(start, "fake_ok", "requests", 1, {"project": "ok" if start.day == 1 else "bad"})])
    local = FakeLocal()
    local.rejected_text = "bad"
    result = process_request(local, {}, {}, SLICED_BACKFILL, {"providers": "fake_ok"})["providers"]["fake_ok"]
    assert result["usage_rows"] == 1 and [r["tags"]["project"] for r in written_rows(local)[0]] == ["ok"]
    assert result["errors"] == ["usage of 2026-10-08T00:00:00Z to 2026-10-15T00:00:00Z not written: line rejected",
                                "usage of 2026-10-15T00:00:00Z to 2026-10-21T00:00:00Z not written: line rejected"]


def test_daily_usage_and_cost_are_sliced_by_92_days(fake_connectors):
    calls = fake_connectors(good=[])
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    body = json.dumps({"start": start.isoformat(), "end": (start + timedelta(days=200)).isoformat(), "bucket_width": "1d"})
    process_request(FakeLocal(), {}, {}, body, {"providers": "fake_ok"})
    edges = [start + timedelta(days=days) for days in (0, 92, 184, 200)]
    assert [window[:2] for window in calls["usage"]] == list(zip(edges, edges[1:]))
    assert calls["cost"] == [(first.date(), last.date()) for first, last in zip(edges, edges[1:])]


def test_scheduled_run_with_default_lookbacks_is_one_slice(fake_connectors):
    calls = fake_connectors(good=[])
    collect(FakeLocal(), settings_for("fake_ok"), "t", now=NOW, backfill=False)
    assert len(calls["usage"]) == 1 and len(calls["cost"]) == 1


def test_failing_cost_still_waits_for_the_interval(fake_connectors):
    calls = fake_connectors(good=[], cost=RuntimeError("billing down"))
    local, s = FakeLocal(), settings_for("fake_ok", cost_interval="6h")
    collect(local, s, "t", now=NOW, backfill=False)
    collect(local, s, "t", now=NOW + timedelta(hours=1), backfill=False)
    assert len(calls["cost"]) == 1


def test_rows_before_an_exception_are_written_and_cost_still_runs(fake_connectors):
    fake_connectors(good=[UsageRecord(NOW, "fake_ok", "requests", 3, {}), RuntimeError("mid-fetch")],
                    cost=[CostRecord(NOW, "fake_ok", "b", "USD", 1.0, {})])
    local = FakeLocal()
    result = collect(local, settings_for("fake_ok"), "t", now=NOW, backfill=False)["fake_ok"]
    assert (result["usage_rows"], result["cost_rows"]) == (1, 1)
    assert result["errors"] == ["usage of 2026-11-12T10:00:00Z to 2026-11-14T10:00:00Z not fully read: mid-fetch"]


def test_collect_usage_only_skips_cost_and_keeps_its_fetch_time(fake_connectors):
    calls = fake_connectors(good=[UsageRecord(NOW, "fake_ok", "requests", 3, {})],
                            cost=[CostRecord(NOW, "fake_ok", "b", "USD", 1.0, {})])
    local = FakeLocal()
    local.cache.put("cost_fetched_at:fake_ok", "2026-11-01T00:00:00+00:00")
    result = collect(local, settings_for("fake_ok", collect="usage"), "t", now=NOW, backfill=False)["fake_ok"]
    assert result["usage_rows"] == 1 and result["cost_period"] is None and calls["cost"] == []
    assert local.cache.get("cost_fetched_at:fake_ok") == "2026-11-01T00:00:00+00:00"


def test_backfill_collect_cost_only_skips_usage(fake_connectors):
    calls = fake_connectors(good=[UsageRecord(NOW, "fake_ok", "requests", 3, {})],
                            cost=[CostRecord(NOW, "fake_ok", "b", "USD", 1.0, {})])
    body = json.dumps({"start": "2026-11-12T00:00:00Z", "end": "2026-11-13T00:00:00Z"})
    headers = {"X-Influxdb3-Llm-Usage-Collect": "cost"}
    result = process_request(FakeLocal(), {}, headers, body, {"providers": "fake_ok"})["providers"]["fake_ok"]
    assert calls["usage"] == [] and result["usage_period"] is None and result["usage_rows"] == 0
    assert result["cost_period"] == ["2026-11-12", "2026-11-13"] and result["cost_rows"] == 1


ANTHROPIC_BASE = "https://api.anthropic.com/v1/organizations"


def one_usage_row(**group):
    row = {"starting_at": "2026-11-11T18:00:00Z", "ending_at": "2026-11-11T19:00:00Z",
           "group": {"model": "claude-opus-5-5", **group}, "usage": {"requests": 1}}
    return {"data": [row], "has_more": False, "next_page": None}


class AnthropicHarness:
    def __init__(self):
        self.session, self.local = FakeSession(), FakeLocal()
        self.usage_pages([{"data": [], "has_more": False}])
        self.cost_pages([{"data": [], "has_more": False}])
        self.key_pages([{"data": [], "has_more": False}])
        self.settings()

    def settings(self, **overrides):
        self.connector = plugin.AnthropicConnector(settings_for("anthropic", **overrides), HttpClient(self.session), self.local)
        return self

    def usage_pages(self, pages):
        self.session.route("GET", f"{ANTHROPIC_BASE}/analytics/usage", pages)

    def cost_pages(self, pages):
        self.session.route("GET", f"{ANTHROPIC_BASE}/analytics/cost", pages)

    def key_pages(self, pages):
        self.session.route("GET", f"{ANTHROPIC_BASE}/api_keys", pages)

    def fetch_usage(self):
        start = datetime(2026, 11, 11, 18, tzinfo=timezone.utc)
        return self.connector.fetch_usage(start, start + timedelta(hours=1), "1h", sorted(self.connector.usage_types))

    def fetch_cost(self):
        return self.connector.fetch_cost(date(2026, 11, 11), date(2026, 11, 12))

    def last_params(self):
        return [c for c in self.session.calls if c["url"].endswith(("/usage", "/cost"))][-1]["params"]


@pytest.fixture
def anthropic():
    return AnthropicHarness()


def test_anthropic_usage_maps_live_shaped_rows(anthropic):
    anthropic.usage_pages([{"data": [{"starting_at": "2026-11-11T18:00:00Z", "ending_at": "2026-11-11T19:00:00Z",
        "group": {"model": "claude-opus-5-5", "workspace_id": "wrkspc_1", "api_key_id": "apikey_1", "actor": {"type": "user_actor", "user_id": "user_1", "email_address": "a@b.c", "name": "N"}, "actor_type": "user_actor"},
        "usage": {"uncached_input_tokens": 19316, "cache_creation": {"ephemeral_5m_input_tokens": 3842, "ephemeral_1h_input_tokens": 0}, "cache_read_input_tokens": 3842, "output_tokens": 215, "server_tool_use": {"web_search_requests": 1}, "requests": 6}}],
        "has_more": False, "next_page": None}])
    rows = {r.usage_type: r for r in anthropic.fetch_usage()}
    assert {k: v.quantity for k, v in rows.items()} == {"input_tokens": 19316, "cache_write_tokens": 3842, "cache_read_tokens": 3842, "output_tokens": 215, "web_search_requests": 1, "requests": 6}
    assert rows["requests"].tags == {"account": "default", "model": "claude-opus-5-5", "project": "wrkspc_1", "api_key_id": "apikey_1", "user_id": "user_1"}
    assert "a@b.c" not in repr(rows)


def test_anthropic_service_account_user_id_and_request_shape(anthropic):
    anthropic.usage_pages([one_usage_row(actor={"type": "service_account", "id": "svac_1", "name": "N"}, actor_type="service_account")])
    assert {r.tags["user_id"] for r in anthropic.fetch_usage()} == {"svac_1"}
    call = anthropic.session.calls[-1]
    assert call["headers"] == {"x-api-key": "sk-ant-admin01-secret", "anthropic-version": "2026-10-21", "anthropic-beta": "analytics-usage-cost-2026-08-24"}
    assert call["params"] == {"starting_at": "2026-11-11T18:00:00Z", "ending_at": "2026-11-11T19:00:00Z", "time_bucket_width": "1h",
                              "group_by[]": ["model", "workspace_id", "api_key_id", "actor", "actor_type"], "order_by": "time", "order": "asc", "limit": 1000}


def test_anthropic_cost_in_cents_and_billing_item(anthropic):
    anthropic.cost_pages([{"data": [
        {"starting_at": "2026-11-11T00:00:00Z", "group": {"model": "claude-opus-5-5", "cost_type": "tokens", "token_type": "uncached_input_tokens"}, "cost": {"currency": "USD", "amount": "7.726400"}},
        {"starting_at": "2026-11-11T00:00:00Z", "group": {"model": "claude-opus-5-5", "cost_type": "web_search", "token_type": None}, "cost": {"currency": "USD", "amount": "1.000000"}}],
        "has_more": False}])
    assert [(c.billing_item, c.cost) for c in anthropic.fetch_cost()] == [("tokens/uncached_input_tokens", 0.077264), ("web_search", 0.01)]
    assert anthropic.last_params()["time_bucket_width"] == "1d" and anthropic.last_params()["group_by[]"][-2:] == ["cost_type", "token_type"]


def test_anthropic_excluded_tags_are_not_requested(anthropic):
    anthropic.settings(exclude_tags="api_key_id user_id project").usage_pages([{"data": [], "has_more": False}])
    list(anthropic.fetch_usage())
    assert anthropic.last_params()["group_by[]"] == ["model"]
    assert not any(c["url"].endswith("/api_keys") for c in anthropic.session.calls)


def test_anthropic_restarts_chunk_on_410_without_double_counting(anthropic):
    first = one_usage_row(api_key_id="apikey_1")
    first.update(has_more=True, next_page="p2")
    anthropic.usage_pages([first, FakeResponse(410, {}), dict(first, has_more=False, next_page=None)])
    rows = [r for r in anthropic.fetch_usage() if r.usage_type == "requests"]
    assert len(rows) == 1 and anthropic.connector.errors == []


def test_anthropic_gives_up_after_three_restarts(anthropic):
    anthropic.usage_pages([FakeResponse(410, {})])
    assert list(anthropic.fetch_usage()) == [] and len(anthropic.connector.errors) == 1
    assert len([c for c in anthropic.session.calls if c["url"].endswith("/usage")]) == 4


def test_anthropic_long_period_is_split_into_31_day_chunks_and_paged(anthropic):
    page = one_usage_row()
    more = dict(page, has_more=True, next_page="p2")
    anthropic.usage_pages([more, page, more, page])
    start = datetime(2026, 9, 1, tzinfo=timezone.utc)
    list(anthropic.connector.fetch_usage(start, start + timedelta(days=40), "1d", ["requests"]))
    calls = [c["params"] for c in anthropic.session.calls if c["url"].endswith("/usage")]
    assert [(p["starting_at"], p["ending_at"], p.get("page")) for p in calls] == [
        ("2026-09-01T00:00:00Z", "2026-10-02T00:00:00Z", None), ("2026-09-01T00:00:00Z", "2026-10-02T00:00:00Z", "p2"),
        ("2026-10-02T00:00:00Z", "2026-10-11T00:00:00Z", None), ("2026-10-02T00:00:00Z", "2026-10-11T00:00:00Z", "p2")]


def test_anthropic_oldest_dates(anthropic):
    assert anthropic.connector.oldest_usage(NOW) == datetime(2026, 1, 1, tzinfo=timezone.utc)
    later = datetime(2027, 6, 1, tzinfo=timezone.utc)
    assert anthropic.connector.oldest_usage(later) == later - timedelta(days=365)
    assert anthropic.connector.oldest_cost(NOW) == date(2026, 1, 1)


def test_anthropic_key_names_resolved_and_cached(anthropic):
    anthropic.key_pages([{"data": [{"id": "apikey_0", "name": "other"}], "has_more": True, "last_id": "apikey_0"},
                         {"data": [{"id": "apikey_1", "name": "default-key"}], "has_more": False, "last_id": "apikey_1"}])
    anthropic.usage_pages([one_usage_row(api_key_id="apikey_1")])
    assert {r.api_key_name for r in anthropic.fetch_usage()} == {"default-key"}
    key_calls = [c["params"] for c in anthropic.session.calls if c["url"].endswith("/api_keys")]
    assert key_calls == [{"limit": 100}, {"limit": 100, "after_id": "apikey_0"}]
    assert anthropic.local.cache.values["api_key_names:anthropic"] == {"apikey_0": "other", "apikey_1": "default-key"}
    anthropic.settings()
    assert {r.api_key_name for r in anthropic.fetch_usage()} == {"default-key"} and len(
        [c for c in anthropic.session.calls if c["url"].endswith("/api_keys")]) == 2


def test_anthropic_key_name_failure_warns_and_leaves_names_out(anthropic):
    anthropic.key_pages([FakeResponse(403, {})])
    anthropic.usage_pages([one_usage_row(api_key_id="apikey_1")])
    anthropic.connector.task_id = "task-42"
    assert {r.api_key_name for r in anthropic.fetch_usage()} == {None}
    assert len(anthropic.local.messages["warn"]) == 1 and "sk-ant" not in anthropic.local.messages["warn"][0]
    assert anthropic.local.messages["warn"][0].startswith("[task-42] anthropic:")


def test_collect_gives_connectors_the_task_id(all_providers):
    local = FakeLocal()
    all_providers.session.route("GET", f"{ANTHROPIC_BASE}/api_keys", [FakeResponse(403, {})])
    plugin.collect(local, settings_for("anthropic"), "task-7", now=NOW, backfill=False)
    assert local.messages["warn"] and all(m.startswith("[task-7] anthropic:") for m in local.messages["warn"])


@pytest.mark.parametrize("kind", ["usage", "cost"])
def test_anthropic_failed_chunk_is_recorded_and_skipped(anthropic, kind):
    row = {"starting_at": "2026-10-05T00:00:00Z", "group": {"model": "m"}, "usage": {"requests": 2}, "cost": {"currency": "USD", "amount": "100"}}
    ok = {"data": [row], "has_more": False}
    anthropic.session.route("GET", f"{ANTHROPIC_BASE}/analytics/{kind}", [FakeResponse(400, {}), ok])
    start = datetime(2026, 9, 1, tzinfo=timezone.utc)
    if kind == "usage":
        records = list(anthropic.connector.fetch_usage(start, start + timedelta(days=40), "1d", ["requests"]))
    else:
        records = list(anthropic.connector.fetch_cost(start.date(), start.date() + timedelta(days=40)))
    assert len(records) == 1
    assert len(anthropic.connector.errors) == 1 and "400" in anthropic.connector.errors[0] and "2026-09-01" in anthropic.connector.errors[0]


OPENAI_BASE = "https://api.openai.com/v1/organization"


def page(*results):
    return {"data": [{"start_time": 1794510960, "results": list(results)}] if results else [], "has_more": False}


def result(**fields):
    return {"model": "gpt-5-nano-2025-08-07", "project_id": "proj_1", **fields}


class OpenAIHarness:
    def __init__(self):
        self.session, self.local = FakeSession(), FakeLocal()
        for path in [*plugin.OPENAI_USAGE_ENDPOINTS, "costs", "projects"]:
            self.route(path, [{"data": [], "has_more": False}])
        self.settings()

    def settings(self, **overrides):
        self.connector = plugin.OpenAIConnector(settings_for("openai", **overrides), HttpClient(self.session), self.local)
        return self

    def route(self, path, responses):
        self.session.route("GET", f"{OPENAI_BASE}/{path}", responses)
        return self

    def fetch_usage(self, types):
        start = datetime(2026, 11, 12, 19, tzinfo=timezone.utc)
        return self.connector.fetch_usage(start, start + timedelta(hours=1), "1h", types)

    def fetch_cost(self):
        return self.connector.fetch_cost(date(2026, 11, 12), date(2026, 11, 13))

    def data_calls(self):
        return [c for c in self.session.calls if c["url"].startswith(f"{OPENAI_BASE}/usage/") or c["url"].endswith("/costs")]

    def called_usage_paths(self):
        return [c["url"].removeprefix(f"{OPENAI_BASE}/") for c in self.data_calls()]

    def last_params(self):
        return self.data_calls()[-1]["params"]

    def all_params(self, path):
        return [c["params"] for c in self.session.calls if c["url"] == f"{OPENAI_BASE}/{path}"]


@pytest.fixture
def openai():
    return OpenAIHarness()


def test_openai_completions_split_cached_and_reasoning_stays_in_output(openai):
    openai.route("usage/completions", [{"data": [{"start_time": 1794510960, "results": [
        {"project_id": "proj_1", "api_key_id": "key_1", "user_id": "user-1", "model": "gpt-5-nano-2025-08-07",
         "input_tokens": 4264, "input_uncached_tokens": 2216, "input_cached_tokens": 2048, "input_cache_write_tokens": 0, "output_tokens": 94, "num_model_requests": 3}]}],
        "has_more": False}])
    got = {r.usage_type: r.quantity for r in openai.fetch_usage(["input_tokens", "cache_read_tokens", "output_tokens", "requests"])}
    assert got == {"input_tokens": 2216, "cache_read_tokens": 2048, "output_tokens": 94, "requests": 3}
    assert openai.session.calls[-1]["headers"] == {"Authorization": "Bearer sk-admin-secret"}


def test_openai_derives_uncached_when_absent(openai):
    openai.route("usage/completions", [page(result(input_tokens=100, input_cached_tokens=30, input_cache_write_tokens=5, output_tokens=1, num_model_requests=1))])
    assert {r.usage_type: r.quantity for r in openai.fetch_usage(["input_tokens"])} == {"input_tokens": 65}


def test_openai_calls_only_needed_endpoints_and_drops_excluded_dimensions(openai):
    openai.settings(exclude_tags="user_id").route("usage/vector_stores", [page()])
    list(openai.fetch_usage(["vector_store_bytes"]))
    assert openai.called_usage_paths() == ["usage/vector_stores"]
    assert openai.last_params()["group_by"] == ["project_id"]


def test_openai_each_endpoint_reports_its_own_field(openai):
    openai.route("usage/embeddings", [page(result(input_tokens=10, num_model_requests=1))])
    openai.route("usage/audio_speeches", [page(result(characters=7))])
    openai.route("usage/web_search_calls", [page(result(num_requests=2))])
    got = {r.usage_type: r.quantity for r in openai.fetch_usage(["input_tokens", "requests", "tts_characters", "web_search_requests"])}
    assert got == {"input_tokens": 10, "requests": 1, "tts_characters": 7, "web_search_requests": 2}
    assert openai.called_usage_paths() == [
        "usage/completions", "usage/decisions", "usage/embeddings", "usage/moderations", "usage/audio_speeches", "usage/web_search_calls"]


def test_openai_cost_project_and_model(openai):
    openai.route("costs", [{"data": [{"start_time": 1794441600, "results": [
        {"line_item": "gpt-5-nano-2025-08-07, cached input", "project_id": "proj_1", "api_key_id": "key_1", "user_id": "user-1", "amount": {"value": 1.024e-05, "currency": "usd"}},
        {"line_item": "text-embedding-3-small", "project_id": "proj_1", "api_key_id": "key_1", "user_id": "user-1", "amount": {"value": 2e-07, "currency": "usd"}}]}],
        "has_more": False}])
    rows = list(openai.fetch_cost())
    assert [(r.billing_item, r.tags.get("model"), r.tags["project"], r.currency) for r in rows] == [
        ("gpt-5-nano-2025-08-07, cached input", "gpt-5-nano-2025-08-07", "proj_1", "USD"),
        ("text-embedding-3-small", None, "proj_1", "USD")]
    assert "project_id" not in openai.last_params()["group_by"]
    assert openai.last_params()["bucket_width"] == "1d" and openai.last_params()["limit"] == 180


def test_openai_cost_excluded_project_is_left_out(openai):
    openai.settings(exclude_tags="project user_id").route("costs", [{"data": [{"start_time": 1794441600, "results": [
        {"line_item": "m, input", "project_id": "proj_1", "amount": {"value": 1, "currency": "usd"}}]}], "has_more": False}])
    assert [r.tags for r in openai.fetch_cost()] == [{"account": "default", "model": "m"}]
    assert openai.last_params()["group_by"] == ["api_key_id", "line_item"]


def test_openai_key_names_across_projects(openai):
    openai.route("projects", [{"data": [{"id": "proj_1"}, {"id": "proj_2"}], "has_more": False}])
    openai.route("projects/proj_1/api_keys", [{"data": [{"id": "key_1", "name": "test", "owner": {"type": "user", "user": {"id": "user-1", "email": "x@y.z"}}}], "has_more": False}])
    openai.route("projects/proj_2/api_keys", [{"data": [{"id": "key_2", "name": "test-service", "owner": {"type": "service_account"}}], "has_more": False}])
    assert openai.connector.key_names() == {"key_1": "test", "key_2": "test-service"}
    assert openai.all_params("projects") == [{"limit": 100, "include_archived": "true"}]
    assert openai.local.cache.values["api_key_names:openai"] == {"key_1": "test", "key_2": "test-service"}


def test_openai_key_names_paginate_projects_with_after(openai):
    openai.route("projects", [{"data": [{"id": "proj_1"}], "has_more": True, "last_id": "proj_1"}, {"data": [], "has_more": False}])
    openai.connector.key_names()
    assert openai.all_params("projects")[1]["after"] == "proj_1"


def test_openai_key_name_failure_warns_and_usage_still_flows(openai):
    openai.route("projects", [FakeResponse(403, {})])
    openai.route("usage/completions", [page(result(api_key_id="key_1", output_tokens=4))])
    rows = list(openai.fetch_usage(["output_tokens"]))
    assert [(r.quantity, r.api_key_name) for r in rows] == [(4, None)]
    assert len(openai.local.messages["warn"]) == 1 and "sk-admin" not in openai.local.messages["warn"][0]


def test_openai_paginates_with_next_page(openai):
    openai.route("usage/completions", [{"data": [], "has_more": True, "next_page": "p2"}, {"data": [], "has_more": False}])
    list(openai.fetch_usage(["output_tokens"]))
    assert openai.all_params("usage/completions")[1]["page"] == "p2"


def test_missing_page_cursor_fails_the_chunk_and_the_key_names(openai):
    openai.route("projects", [{"data": [], "has_more": True}])
    openai.route("usage/completions", [{**page(result(api_key_id="key_1", output_tokens=4)), "has_more": True}])
    rows = list(openai.fetch_usage(["output_tokens"]))
    assert rows == [] and len(openai.connector.errors) == 1
    assert openai.connector.errors[0].endswith("skipped: page cursor missing: has_more without next_page")
    assert openai.connector.key_names() == {}
    [warning] = openai.local.messages["warn"]
    assert warning.endswith("openai: API key names unavailable (page cursor missing: has_more without last_id)")


def test_repeated_page_cursor_fails_instead_of_looping(openai):
    openai.route("projects", [{"data": [{"id": "proj_1"}], "has_more": True}])
    openai.route("usage/completions", [{**page(result(output_tokens=4)), "has_more": True, "next_page": "p1"}])
    assert list(openai.fetch_usage(["output_tokens"])) == []
    assert openai.connector.errors[0].endswith("skipped: page cursor repeated: has_more with a next_page already followed")
    assert openai.connector.key_names() == {}
    assert openai.local.messages["warn"][0].endswith("(page cursor repeated: has_more with a last_id already followed)")


def test_openai_failed_chunk_is_recorded_and_other_endpoints_continue(openai):
    openai.route("usage/completions", [FakeResponse(400, {})])
    openai.route("usage/embeddings", [page(result(input_tokens=10))])
    rows = list(openai.fetch_usage(["output_tokens", "input_tokens"]))
    assert [(r.usage_type, r.quantity) for r in rows] == [("input_tokens", 10)]
    assert len(openai.connector.errors) == 1
    assert all(word in openai.connector.errors[0] for word in ("usage/completions", "400", "2026-11-12"))
    assert "sk-admin" not in openai.connector.errors[0]


def test_provider_message_reaches_errors_and_log_with_the_key_masked(openai, monkeypatch):
    openai.route("usage/completions", [FakeResponse(403, {"error": {"message": "Missing scopes: api.usage.read (key sk-admin-secret)"}})])
    monkeypatch.setattr(plugin, "make_http_client", lambda: HttpClient(openai.session))
    local = FakeLocal()
    result = collect(local, settings_for("openai", collect="usage", usage_types="output_tokens"), "t", now=NOW, backfill=False)["openai"]
    expected = "skipped: HTTP 403: Missing scopes: api.usage.read (key ***)"
    assert len(result["errors"]) == 1 and result["errors"][0].endswith(expected)
    assert local.messages["error"] == [f"[t] openai: {result['errors'][0]}"]


def test_openai_oldest_dates(openai):
    now = datetime(2026, 11, 19, 16, 30, tzinfo=timezone.utc)
    assert openai.connector.oldest_usage(now) == datetime(2025, 11, 19, tzinfo=timezone.utc)
    assert openai.connector.oldest_cost(now) == date(2025, 11, 19)


def test_openai_long_period_is_chunked_by_bucket_limit(openai):
    start = datetime(2026, 11, 1, tzinfo=timezone.utc)
    list(openai.connector.fetch_usage(start, start + timedelta(hours=200), "1h", ["output_tokens"]))
    calls = [c for c in openai.all_params("usage/completions")]
    assert [(c["start_time"], c["end_time"], c["limit"]) for c in calls] == [
        (int(start.timestamp()), int((start + timedelta(hours=168)).timestamp()), 168),
        (int((start + timedelta(hours=168)).timestamp()), int((start + timedelta(hours=200)).timestamp()), 168)]


T0 = datetime(2026, 11, 12, 19, tzinfo=timezone.utc)
MANTLE_METRICS = set(plugin.BEDROCK_NAMESPACES["AWS/BedrockMantle"][1])


class BedrockHarness:
    """A BedrockConnector whose AWS clients are real boto3 clients driven by Stubbers."""

    def __init__(self, monkeypatch):
        boto3 = pytest.importorskip("boto3")
        from botocore.stub import Stubber

        self.boto3, self.Stubber, self.monkeypatch = boto3, Stubber, monkeypatch
        self.session = boto3.Session(aws_access_key_id="test", aws_secret_access_key="test")
        self.stubs, self.calls = {}, []
        self.settings()

    def settings(self, **overrides):
        self.connector = plugin.BedrockConnector(
            settings_for("bedrock", **{"bedrock_regions": "us-east-1", **overrides}), HttpClient(FakeSession()), FakeLocal()
        )
        self.connector._client = lambda service, region: self.stub(service, region)[0]
        self.stub("sts", "us-east-1")[1].add_response("get_caller_identity", {"Account": "123456789012"})
        return self

    def stub(self, service, region="us-east-1"):
        if (service, region) not in self.stubs:
            client = self.session.client(service, region_name=region)
            client.meta.events.register(
                "before-parameter-build.*.*", lambda params, model, **_: self.calls.append((model.name, copy.deepcopy(params)))
            )
            stubber = self.Stubber(client)
            stubber.activate()
            self.stubs[(service, region)] = client, stubber
        return self.stubs[(service, region)]

    def list_metrics(self, namespace, series, region="us-east-1"):
        metrics = [
            {"Namespace": namespace, "MetricName": name, "Dimensions": [{"Name": n, "Value": v} for n, v in dimensions]}
            for name, dimensions in series
        ]
        self.stub("cloudwatch", region)[1].add_response("list_metrics", {"Metrics": metrics})
        return self

    def metric_data(self, series, region="us-east-1", next_token=None):
        results = []
        for key, points in series.items():
            model, _, metric = key.rpartition("/")
            namespace, dimension = ("AWS/BedrockMantle", "Model") if metric in MANTLE_METRICS else ("AWS/Bedrock", "ModelId")
            results.append(
                {
                    "Id": plugin.metric_query_id(namespace, [(dimension, model)], metric),
                    "Label": key,
                    "Timestamps": [moment for moment, _ in points],
                    "Values": [value for _, value in points],
                    "StatusCode": "Complete",
                }
            )
        response = {"MetricDataResults": results, **({"NextToken": next_token} if next_token else {})}
        self.stub("cloudwatch", region)[1].add_response("get_metric_data", response)
        return self

    def fail(self, service, operation, region="us-east-1"):
        self.stub(service, region)[1].add_client_error(
            operation, "AccessDeniedException", "not authorized: AKIA-bedrock bedrock-secret"
        )
        return self

    def services(self, names):
        values = [{"Value": name, "Attributes": {}} for name in names]
        self.stub("ce")[1].add_response(
            "get_dimension_values", {"DimensionValues": values, "ReturnSize": len(values), "TotalSize": len(values)}
        )
        return self

    def cost_groups(self, day, groups, region="us-east-1"):
        entry = {
            "TimePeriod": {"Start": day.isoformat(), "End": (day + timedelta(days=1)).isoformat()},
            "Total": {},
            "Estimated": False,
            "Groups": [
                {"Keys": keys, "Metrics": {"UnblendedCost": {"Amount": amount, "Unit": "USD"}}} for keys, amount in groups
            ],
        }
        self.stub("ce")[1].add_response("get_cost_and_usage", {"ResultsByTime": [entry]})
        return self

    def requests(self, operation):
        return [params for name, params in self.calls if name == operation]

    def last_cost_request(self):
        return self.requests("GetCostAndUsage")[-1]

    def queried_dimension_sets(self):
        return [
            [(d["Name"], d["Value"]) for d in query["MetricStat"]["Metric"]["Dimensions"]]
            for query in self.requests("GetMetricData")[0]["MetricDataQueries"]
        ]

    def fetch_usage(self, types):
        return self.connector.fetch_usage(T0, T0 + timedelta(hours=1), "1h", types)

    def fetch_cost(self, start_day, end_day):
        return self.connector.fetch_cost(start_day, end_day)

    def session_kwargs(self):
        received = {}
        self.monkeypatch.setattr(self.boto3, "Session", lambda **kwargs: received.update(kwargs))
        plugin.BedrockConnector(settings_for("bedrock", bedrock_regions="us-east-1"), None, FakeLocal())._session()
        return received


@pytest.fixture
def bedrock(monkeypatch):
    return BedrockHarness(monkeypatch)


def test_bedrock_usage_exact_dimension_sets_and_two_namespaces(bedrock):
    bedrock.list_metrics("AWS/Bedrock", [("InputTokenCount", [("ModelId", "us.amazon.nova-micro-v1:0")]),
                                         ("InputTokenCount", [("ModelId", "m"), ("ContextWindow", "1M")]),
                                         ("InputTokenCount", [])])
    bedrock.list_metrics("AWS/BedrockMantle", [("TotalInputTokens", [("Model", "google.gemma-3-4b-it")]),
                                               ("TotalInputTokens", [("Model", "google.gemma-3-4b-it"), ("Project", "default")])])
    bedrock.metric_data({"us.amazon.nova-micro-v1:0/InputTokenCount": [(T0, 30.0)], "google.gemma-3-4b-it/TotalInputTokens": [(T0, 46.0)]})
    rows = sorted((r.tags["model"], r.usage_type, r.quantity) for r in bedrock.fetch_usage(["input_tokens"]))
    assert rows == [("google.gemma-3-4b-it", "input_tokens", 46.0), ("us.amazon.nova-micro-v1:0", "input_tokens", 30.0)]
    assert bedrock.queried_dimension_sets() == [[("ModelId", "us.amazon.nova-micro-v1:0")], [("Model", "google.gemma-3-4b-it")]]
    query = bedrock.requests("GetMetricData")[0]["MetricDataQueries"][0]["MetricStat"]
    assert (query["Stat"], query["Period"]) == ("Sum", 3600)


def test_bedrock_cost_after_september_uses_product_attribute_without_service_filter(bedrock):
    bedrock.cost_groups(date(2026, 11, 12), [(["model$Nova Micro", "USE1-NovaMicro-input-tokens"], "0.00000147"),
                                             (["model$Nova Micro", "USE1-NovaMicro-cache-write-input-token-count"], "0")])
    rows = list(bedrock.fetch_cost(date(2026, 11, 12), date(2026, 11, 13)))
    assert [(r.tags["model"], r.billing_item, r.cost, r.currency, r.tags["region"]) for r in rows] == [
        ("Nova Micro", "USE1-NovaMicro-input-tokens", 1.47e-06, "USD", "us-east-1"), ("Nova Micro", "USE1-NovaMicro-cache-write-input-token-count", 0.0, "USD", "us-east-1")]
    request = bedrock.last_cost_request()
    assert "SERVICE" not in json.dumps(request["Filter"]) and {"Dimensions": {"Key": "LINKED_ACCOUNT", "Values": ["123456789012"]}} in request["Filter"]["And"]
    assert (request["Granularity"], request["Metrics"]) == ("DAILY", ["UnblendedCost"])


def test_bedrock_cost_before_september_filters_bedrock_services(bedrock):
    bedrock.services(["Amazon Bedrock", "Claude Sonnet 5 (Amazon Bedrock Edition)", "Amazon Simple Storage Service"])
    bedrock.cost_groups(date(2026, 8, 1), [(["USE1-Claude5Sonnet-input-tokens"], "1.5")])
    rows = list(bedrock.fetch_cost(date(2026, 8, 1), date(2026, 8, 2)))
    assert rows[0].billing_item == "USE1-Claude5Sonnet-input-tokens" and "model" not in rows[0].tags
    service_filter = [e for e in bedrock.last_cost_request()["Filter"]["And"] if e["Dimensions"]["Key"] == "SERVICE"][0]
    assert service_filter["Dimensions"]["Values"] == ["Amazon Bedrock", "Claude Sonnet 5 (Amazon Bedrock Edition)"]


def test_bedrock_cost_period_is_split_at_september_and_skips_when_no_services(bedrock):
    bedrock.services(["Amazon Simple Storage Service"])
    bedrock.cost_groups(date(2026, 9, 1), [(["model$Nova Micro", "u"], "2")])
    rows = list(bedrock.fetch_cost(date(2026, 8, 31), date(2026, 9, 2)))
    assert len(rows) == 1 and bedrock.requests("GetCostAndUsage")[0]["TimePeriod"]["Start"] == "2026-09-01"


def test_bedrock_account_label_replaces_the_id_but_not_the_filter(bedrock):
    bedrock.settings(bedrock_account="prod").cost_groups(date(2026, 11, 12), [(["model$Nova Micro", "u"], "1")])
    rows = list(bedrock.fetch_cost(date(2026, 11, 12), date(2026, 11, 13)))
    assert rows[0].tags["account"] == "prod" and "123456789012" in json.dumps(bedrock.last_cost_request()["Filter"])


def test_bedrock_usage_with_account_excluded_makes_no_sts_call(bedrock):
    bedrock.settings(exclude_tags="account").list_metrics("AWS/Bedrock", [("Invocations", [("ModelId", "m")])])
    bedrock.list_metrics("AWS/BedrockMantle", []).metric_data({"m/Invocations": [(T0, 5.0)]})
    rows = list(bedrock.fetch_usage(["requests"]))
    assert [(r.quantity, r.tags) for r in rows] == [(5.0, {"model": "m", "region": "us-east-1"})]
    assert bedrock.requests("GetCallerIdentity") == []


def test_bedrock_notes_the_two_week_discovery_limit(bedrock):
    hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)  # the note is relative to the real clock
    for slice_start, slice_end in ((hour - timedelta(days=30), hour - timedelta(days=23)), (hour - timedelta(days=23), hour)):
        bedrock.list_metrics("AWS/Bedrock", []); bedrock.list_metrics("AWS/BedrockMantle", [])
        list(bedrock.connector.fetch_usage(slice_start, slice_end, "1d", ["input_tokens"]))
    assert len(bedrock.connector.notes) == 1 and "two weeks" in bedrock.connector.notes[0]
    assert not bedrock.requests("GetMetricData")
    bedrock.connector.notes.clear()
    bedrock.list_metrics("AWS/Bedrock", []); bedrock.list_metrics("AWS/BedrockMantle", [])
    list(bedrock.connector.fetch_usage(hour - timedelta(days=2), hour, "1h", ["input_tokens"]))
    assert bedrock.connector.notes == []


def test_bedrock_reads_its_own_variables_not_aws_defaults(bedrock, monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "server-own")
    assert bedrock.session_kwargs()["aws_access_key_id"] == "AKIA-bedrock"


def test_bedrock_queries_are_batched_by_500_and_follow_next_token(bedrock):
    models = [f"model-{index}" for index in range(501)]
    bedrock.list_metrics("AWS/Bedrock", [("Invocations", [("ModelId", model)]) for model in models])
    bedrock.list_metrics("AWS/BedrockMantle", [])
    bedrock.metric_data({f"{models[0]}/Invocations": [(T0, 1.0)]}, next_token="page-2")
    bedrock.metric_data({f"{models[1]}/Invocations": [(T0, 2.0)]})
    bedrock.metric_data({f"{models[500]}/Invocations": [(T0, 3.0)]})
    rows = sorted(r.quantity for r in bedrock.fetch_usage(["requests"]))
    calls = bedrock.requests("GetMetricData")
    assert rows == [1.0, 2.0, 3.0] and [len(c["MetricDataQueries"]) for c in calls] == [500, 500, 1]
    assert calls[1]["NextToken"] == "page-2"
    ids = [q["Id"] for q in calls[0]["MetricDataQueries"]]
    assert len(set(ids)) == 500 and all(re.fullmatch(r"[a-z][A-Za-z0-9_]*", i) for i in ids)


def test_bedrock_account_resolved_through_next_region_when_first_fails(bedrock):
    bedrock.settings(bedrock_regions="us-esat-1 us-east-1")
    bedrock.fail("sts", "get_caller_identity", "us-esat-1")
    for region in ("us-esat-1", "us-east-1"):
        bedrock.list_metrics("AWS/Bedrock", [("Invocations", [("ModelId", "m")])], region)
        bedrock.list_metrics("AWS/BedrockMantle", [], region)
    bedrock.fail("cloudwatch", "get_metric_data", "us-esat-1").metric_data({"m/Invocations": [(T0, 5.0)]}, "us-east-1")
    rows = list(bedrock.fetch_usage(["requests"]))
    assert bedrock.connector.account_id_or_record() == "123456789012"
    assert [(r.tags["region"], r.quantity) for r in rows] == [("us-east-1", 5.0)]
    assert len(bedrock.connector.errors) == 1 and "us-esat-1" in bedrock.connector.errors[0]


def test_bedrock_account_unresolved_when_no_region_answers(bedrock):
    bedrock.settings(bedrock_regions="us-esat-1 us-east-1")
    bedrock.stubs.pop(("sts", "us-east-1"))
    bedrock.fail("sts", "get_caller_identity", "us-esat-1").fail("sts", "get_caller_identity", "us-east-1")
    assert bedrock.connector.account_id_or_record() is None
    assert "GetCallerIdentity skipped" in bedrock.connector.errors[0]


def bedrock_backfill(bedrock, monkeypatch, region_clients=None, **args):
    """Collect a three-slice Bedrock backfill; `region_clients` replaces the AWS client of a region."""
    clients = region_clients or {}
    monkeypatch.setattr(plugin.BedrockConnector, "_client", lambda connector, service, region: clients.get(region) or bedrock.stub(service, region)[0])
    body = json.dumps({"start": "2026-10-20T00:00:00Z", "end": "2026-11-09T00:00:00Z"})
    settings = load_settings(FakeLocal(), "t", {"providers": "bedrock", "bedrock_regions": "us-east-1", **args}, request_body=body, http=True, now=NOW)
    return collect(FakeLocal(), settings, "t", now=NOW, backfill=True)["bedrock"]


def test_bedrock_failed_account_lookup_is_not_retried_in_later_slices(bedrock, monkeypatch):
    bedrock.stubs.pop(("sts", "us-east-1"))
    bedrock.fail("sts", "get_caller_identity")
    result = bedrock_backfill(bedrock, monkeypatch)
    assert len(bedrock.requests("GetCallerIdentity")) == 1
    assert result["errors"] == ["GetCallerIdentity skipped: AccessDeniedException"]


def test_bedrock_unreachable_region_is_not_retried_in_later_slices(bedrock, monkeypatch):
    from botocore.exceptions import EndpointConnectionError

    attempts = []

    class Unreachable:
        def __getattr__(self, operation):
            def call(**params):
                attempts.append(operation)
                raise EndpointConnectionError(endpoint_url="https://monitoring.us-esat-1.amazonaws.com/")
            return call

    for _ in range(3):
        bedrock.list_metrics("AWS/Bedrock", []).list_metrics("AWS/BedrockMantle", [])
    result = bedrock_backfill(bedrock, monkeypatch, {"us-esat-1": Unreachable()}, bedrock_regions="us-east-1 us-esat-1", collect="usage")
    assert attempts == ["list_metrics", "list_metrics"] and len(bedrock.requests("ListMetrics")) == 6
    assert result["errors"] == [f"ListMetrics {namespace} in us-esat-1 skipped: EndpointConnectionError"
                                for namespace in ("AWS/Bedrock", "AWS/BedrockMantle")]


def test_bedrock_failed_region_and_cost_call_are_recorded_and_skipped(bedrock):
    bedrock.settings(bedrock_regions="us-east-1 eu-west-1")
    for region in ("us-east-1", "eu-west-1"):
        bedrock.list_metrics("AWS/Bedrock", [("Invocations", [("ModelId", "m")])], region)
        bedrock.list_metrics("AWS/BedrockMantle", [], region)
    bedrock.fail("cloudwatch", "get_metric_data", "us-east-1").metric_data({"m/Invocations": [(T0, 5.0)]}, "eu-west-1")
    rows = list(bedrock.fetch_usage(["requests"]))
    assert [(r.tags["region"], r.quantity) for r in rows] == [("eu-west-1", 5.0)]
    bedrock.fail("ce", "get_cost_and_usage").cost_groups(date(2026, 11, 12), [(["model$M", "u"], "1")])
    assert [r.tags["region"] for r in bedrock.fetch_cost(date(2026, 11, 12), date(2026, 11, 13))] == ["eu-west-1"]
    errors = bedrock.connector.errors
    assert len(errors) == 2 and "us-east-1" in errors[0] and "AccessDeniedException" in errors[0]
    assert not any(secret in " ".join(errors) for secret in ("AKIA-bedrock", "bedrock-secret"))


def test_bedrock_oldest_dates(bedrock):
    now = datetime(2026, 11, 14, tzinfo=timezone.utc)
    assert bedrock.connector.oldest_usage(now) == now - timedelta(days=455)
    assert bedrock.connector.oldest_cost(now) == date(2025, 10, 1)
    assert bedrock.connector.oldest_cost(datetime(2026, 1, 5, tzinfo=timezone.utc)) == date(2024, 12, 1)


TENANT_ID = "11111111-1111-1111-1111-111111111111"
CLIENT_ID = "22222222-2222-2222-2222-222222222222"
SUBSCRIPTION_ID = "33333333-3333-3333-3333-333333333333"
AZURE_TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
ARM_ACCOUNTS = f"https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}/providers/Microsoft.CognitiveServices/accounts?api-version=2024-10-01"
METRICS_URL = f"https://eastus.metrics.monitor.azure.com/subscriptions/{SUBSCRIPTION_ID}/metrics:getBatch"
COST_URL = f"https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}/providers/Microsoft.CostManagement/query"


def account_id(name):
    return f"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/rg/providers/Microsoft.CognitiveServices/accounts/{name}"


def account(name, kind):
    return {"id": account_id(name), "name": name, "kind": kind, "location": "eastus"}


def batch_value(name, metrics):
    """One getBatch values[] entry; metrics: metric -> [(dimensions, [(timestamp, total)])]."""
    return {"resourceid": account_id(name).lower(), "resourceregion": "eastus", "value": [
        {"name": {"value": metric}, "timeseries": [
            {"metadatavalues": [{"name": {"value": key}, "value": value} for key, value in dimensions.items()],
             "data": [{"timeStamp": stamp} if total is None else {"timeStamp": stamp, "total": total} for stamp, total in points]}
            for dimensions, points in series]}
        for metric, series in metrics.items()]}


class AzureHarness:
    def __init__(self):
        self.session, self.local = FakeSession(), FakeLocal()
        self.session.route("POST", AZURE_TOKEN_URL, [{"access_token": "tok", "expires_in": 3600}])
        self.settings()

    def settings(self, **overrides):
        config = {"azure_tenant_id": TENANT_ID, "azure_client_id": CLIENT_ID, "azure_subscription_ids": SUBSCRIPTION_ID, **overrides}
        self.connector = plugin.AzureConnector(settings_for("azure", **config), HttpClient(self.session), self.local)
        return self

    def discovered(self, *accounts):
        found = [{"id": a["id"], "name": a["name"], "location": a["location"], "subscription": SUBSCRIPTION_ID} for a in accounts]
        self.connector.discover = lambda: found
        return self

    def route_arm_accounts(self, pages):
        self.session.route("GET", ARM_ACCOUNTS, [pages[0]])
        for previous, page in zip(pages, pages[1:]):
            self.session.route("GET", previous["nextLink"], [page])

    def route_metrics(self, values):
        self.session.route("POST", METRICS_URL, [{"values": [value]} for value in values])

    def route_cost(self, responses):
        self.session.route("POST", COST_URL, responses)

    def token_requests(self):
        return [c["data"]["scope"] for c in self.session.calls if c["url"] == AZURE_TOKEN_URL]

    def calls_to(self, url):
        return [c for c in self.session.calls if c["url"] == url]

    def fetch_usage(self, start=datetime(2026, 11, 12, 18, tzinfo=timezone.utc), end=datetime(2026, 11, 12, 20, tzinfo=timezone.utc), width="1h"):
        return self.connector.fetch_usage(start, end, width, sorted(USAGE_UNITS))

    def fetch_cost(self, start_day, end_day):
        return self.connector.fetch_cost(start_day, end_day)


@pytest.fixture
def azure():
    return AzureHarness()


def test_azure_discovery_follows_next_link_through_empty_pages(azure):
    azure.route_arm_accounts([{"value": [], "nextLink": ARM_ACCOUNTS + "&$skiptoken=2"},
                              {"value": [account("usage-test-aoai", "OpenAI"), account("usage-test-foundry", "AIServices"), account("speech", "SpeechServices")], "nextLink": ARM_ACCOUNTS + "&$skiptoken=3"},
                              {"value": []}])
    assert [a["name"] for a in azure.connector.discover()] == ["usage-test-aoai", "usage-test-foundry"]


def test_azure_usage_models_family_case_insensitive_and_input_as_is(azure):
    azure.discovered(account("usage-test-foundry", "AIServices"))
    azure.route_metrics([batch_value("usage-test-foundry", {
        "InputTokens": [({"modeldeploymentname": "gpt-4.1-mini", "modelname": "gpt-4.1-mini"}, [("2026-11-12T18:00:00Z", 4267), ("2026-11-12T19:00:00Z", None)])],
        "cacheReadInputTokens": [({"ModelDeploymentName": "claude", "ModelName": "claude-haiku-5-5"}, [("2026-11-12T18:00:00Z", 100)])],
        "ephemeral5mInputTokens": [({"modeldeploymentname": "claude", "modelname": "claude-haiku-5-5"}, [("2026-11-12T18:00:00Z", 7)])],
        "ephemeral1hInputTokens": [({"modeldeploymentname": "claude", "modelname": "claude-haiku-5-5"}, [("2026-11-12T18:00:00Z", 3)])]})])
    rows = sorted((r.tags["project"], r.usage_type, r.quantity) for r in prepare(azure.fetch_usage(), []))
    assert rows == [("usage-test-foundry/claude", "cache_read_tokens", 100), ("usage-test-foundry/claude", "cache_write_tokens", 10),
                    ("usage-test-foundry/gpt-4.1-mini", "input_tokens", 4267)]


def test_azure_usage_request_batches_by_fifty_and_chunks_by_seven_days(azure):
    azure.discovered(*[account(f"a{n}", "OpenAI") for n in range(51)])
    azure.route_metrics([batch_value("a0", {})])
    start = datetime(2026, 11, 1, tzinfo=timezone.utc)
    assert list(azure.fetch_usage(start, start + timedelta(days=8))) == []
    calls = azure.calls_to(METRICS_URL)
    assert [len(c["json"]["resourceids"]) for c in calls] == [50, 1, 50, 1]
    first = calls[0]["params"]
    assert (first["starttime"], first["endtime"], first["interval"]) == ("2026-11-01T00:00:00Z", "2026-11-08T00:00:00Z", "PT1H")
    assert calls[2]["params"]["starttime"] == "2026-11-08T00:00:00Z"
    assert first["api-version"] == "2023-10-01" and first["aggregation"] == "total" and first["top"] == 1000
    assert first["metricnamespace"] == "Microsoft.CognitiveServices/accounts"
    assert first["filter"] == "ModelDeploymentName eq '*' and ModelName eq '*'"
    assert first["metricnames"] == "InputTokens,OutputTokens,ModelRequests,GeneratedImages,cacheReadInputTokens,ephemeral5mInputTokens,ephemeral1hInputTokens"
    assert calls[0]["headers"]["Authorization"] == "Bearer tok"
    list(azure.fetch_usage(start, start + timedelta(days=40), "1d"))
    daily = azure.calls_to(METRICS_URL)[4:]
    assert {c["params"]["interval"] for c in daily} == {"P1D"} and daily[2]["params"]["starttime"] == "2026-12-02T00:00:00Z"


def test_azure_failed_batch_is_recorded_and_the_rest_continues(azure):
    azure.discovered(account("usage-test-aoai", "OpenAI"))
    good = {"values": [batch_value("usage-test-aoai", {"OutputTokens": [({"modelname": "m"}, [("2026-11-12T18:00:00Z", 3)])]})]}
    azure.session.route("POST", METRICS_URL, [FakeResponse(403, {"error": {"message": "AuthorizationFailed"}}), good])
    start = datetime(2026, 11, 1, tzinfo=timezone.utc)
    rows = list(azure.fetch_usage(start, start + timedelta(days=8)))
    assert [r.quantity for r in rows] == [3]
    assert len(azure.connector.errors) == 1 and "2026-11-01" in azure.connector.errors[0]
    assert azure.connector.errors[0].endswith("skipped: HTTP 403: AuthorizationFailed")


def test_azure_cost_rows_matched_case_insensitively(azure):
    azure.discovered(account("usage-test-aoai", "OpenAI"))
    azure.route_cost([{"properties": {"columns": [{"name": "Cost"}, {"name": "UsageDate"}, {"name": "ResourceId"}, {"name": "Meter"}, {"name": "Currency"}],
        "rows": [[0.000153328, 20261112, f"/subscriptions/{SUBSCRIPTION_ID}/resourcegroups/rg/providers/microsoft.cognitiveservices/accounts/usage-test-aoai", "gpt 4.1 mini Inp glbl Tokens", "EUR"],
                 [0.0, 20261112, f"/subscriptions/{SUBSCRIPTION_ID}/providers/microsoft.insights/pricings/metricsquery", "Native Metric Queries API Calls", "EUR"]],
        "nextLink": None}}])
    rows = list(azure.fetch_cost(date(2026, 11, 12), date(2026, 11, 13)))
    assert [(r.billing_item, r.tags["project"], r.tags["region"], r.currency, r.time.date()) for r in rows] == [
        ("gpt 4.1 mini Inp glbl Tokens", "usage-test-aoai", "eastus", "EUR", date(2026, 11, 12))]


def test_azure_cost_query_body_monthly_chunks_and_next_link(azure):
    azure.discovered(account("usage-test-aoai", "OpenAI"))
    columns = [{"name": "Cost"}, {"name": "UsageDate"}, {"name": "ResourceId"}, {"name": "Meter"}, {"name": "Currency"}]
    row = [1.5, 20261031, account_id("usage-test-aoai").lower(), "meter", "USD"]
    next_page = COST_URL + "?api-version=2025-03-01&$skiptoken=2"
    azure.route_cost([{"properties": {"columns": columns, "rows": [row], "nextLink": next_page}}, {"properties": {"columns": columns, "rows": [], "nextLink": None}}])
    azure.session.route("POST", next_page, [{"properties": {"columns": columns, "rows": [row[:1] + [20261101] + row[2:]], "nextLink": None}}])
    rows = list(azure.fetch_cost(date(2026, 10, 30), date(2026, 11, 2)))
    assert [r.time.date() for r in rows] == [date(2026, 10, 31), date(2026, 11, 1)]
    assert azure.calls_to(COST_URL)[0]["params"] == {"api-version": "2025-03-01"}
    bodies = [c["json"] for c in azure.session.calls if c["url"] in (COST_URL, next_page)]
    assert [b["timePeriod"] for b in bodies] == [
        {"from": "2026-10-30T00:00:00Z", "to": "2026-10-31T23:59:59Z"}, {"from": "2026-10-30T00:00:00Z", "to": "2026-10-31T23:59:59Z"},
        {"from": "2026-11-01T00:00:00Z", "to": "2026-11-01T23:59:59Z"}]
    assert bodies[0]["type"] == "ActualCost" and bodies[0]["timeframe"] == "Custom"
    assert bodies[0]["dataset"] == {"granularity": "Daily", "aggregation": {"cost": {"name": "Cost", "function": "Sum"}},
                                    "filter": {"dimensions": {"name": "ResourceType", "operator": "In",
                                                              "values": ["microsoft.cognitiveservices/accounts"]}},
                                    "grouping": [{"type": "Dimension", "name": "ResourceId"}, {"type": "Dimension", "name": "Meter"}]}


def test_azure_cost_of_resources_not_found_is_summed_in_one_note(azure, monkeypatch):
    azure.route_arm_accounts([{"value": [account("usage-test-aoai", "OpenAI"), account("speech", "SpeechServices")]}])
    columns = [{"name": "Cost"}, {"name": "UsageDate"}, {"name": "ResourceId"}, {"name": "Meter"}, {"name": "Currency"}]
    rows = [[1.5, 20261031, account_id("usage-test-aoai").lower(), "m", "EUR"], [0.25, 20261031, account_id("deleted").lower(), "m", "EUR"],
            [0.5, 20261031, account_id("deleted").lower(), "m", "EUR"], [0.0, 20261031, account_id("gone").lower(), "m", "EUR"],
            [2.0, 20261031, account_id("speech").lower(), "m", "EUR"]]
    azure.route_cost([{"properties": {"columns": columns, "rows": rows}}])
    monkeypatch.setattr(plugin, "make_http_client", lambda: HttpClient(azure.session))
    body = json.dumps({"start": "2026-08-01T00:00:00Z", "end": "2026-11-02T00:00:00Z", "collect": ["cost"]})
    args = {"providers": "azure", "azure_tenant_id": TENANT_ID, "azure_client_id": CLIENT_ID, "azure_subscription_ids": SUBSCRIPTION_ID}
    settings = load_settings(FakeLocal(), "t", args, request_body=body, http=True, now=NOW)
    result = collect(FakeLocal(), settings, "t", now=NOW, backfill=True)["azure"]
    # two cost slices (92 days, then 1): August, September, October, then November 1
    assert len(azure.calls_to(COST_URL)) == 4 and result["errors"] == []
    assert result["notes"] == [f"8 cost rows of 3.00 EUR skipped: their resources weren't found in subscription {SUBSCRIPTION_ID}"]


def test_azure_new_subscription_404_is_an_error_not_a_crash(azure):
    azure.discovered(account("usage-test-aoai", "OpenAI"))
    azure.route_cost([FakeResponse(404, {"error": {"code": "NotFound", "message": "doesn't have valid WebDirect/AIRS offer type"}})])
    assert list(azure.fetch_cost(date(2026, 11, 12), date(2026, 11, 13))) == [] and "404" in azure.connector.errors[0]


def test_azure_failed_discovery_is_recorded_per_subscription(azure):
    denied = "44444444-4444-4444-4444-444444444444"
    azure.settings(azure_subscription_ids=f"{denied} {SUBSCRIPTION_ID}")
    bad = f"https://management.azure.com/subscriptions/{denied}/providers/Microsoft.CognitiveServices/accounts?api-version=2024-10-01"
    azure.session.route("GET", bad, [FakeResponse(403, {})]).route("GET", ARM_ACCOUNTS, [{"value": [account("usage-test-aoai", "OpenAI")]}])
    assert [a["name"] for a in azure.connector.discover()] == ["usage-test-aoai"]
    assert denied in azure.connector.errors[0] and "403" in azure.connector.errors[0]


def test_azure_token_cached_per_scope(azure):
    azure.discovered(account("usage-test-foundry", "AIServices"))
    azure.route_metrics([batch_value("usage-test-foundry", {})] * 2)
    list(azure.fetch_usage()); list(azure.fetch_usage())
    assert azure.token_requests() == ["https://metrics.monitor.azure.com/.default"]


def test_azure_token_refreshed_shortly_before_expiry(azure, monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(plugin.time, "time", lambda: now[0])
    azure.discovered(account("usage-test-foundry", "AIServices"))
    azure.route_metrics([batch_value("usage-test-foundry", {})] * 3)
    list(azure.fetch_usage())
    now[0] += 3500
    list(azure.fetch_usage())
    now[0] += 100
    list(azure.fetch_usage())
    assert len(azure.token_requests()) == 2
    request = azure.calls_to(AZURE_TOKEN_URL)[0]["data"]
    assert request["grant_type"] == "client_credentials" and request["client_id"] == CLIENT_ID and request["client_secret"] == "azure-client-secret"


def test_azure_account_label(azure):
    azure.settings(azure_account="prod").discovered(account("usage-test-aoai", "OpenAI"))
    azure.route_metrics([batch_value("usage-test-aoai", {"OutputTokens": [({"modeldeploymentname": "d", "modelname": "m"}, [("2026-11-12T18:00:00Z", 3)])]})])
    assert {r.tags["account"] for r in azure.fetch_usage()} == {"prod"}
    azure.settings().discovered(account("usage-test-aoai", "OpenAI"))
    assert {r.tags["account"] for r in azure.fetch_usage()} == {SUBSCRIPTION_ID}


def test_azure_oldest_dates(azure):
    now = datetime(2026, 11, 14, tzinfo=timezone.utc)
    assert azure.connector.oldest_usage(now) == now - timedelta(days=93)
    assert azure.connector.oldest_cost(now) == date(2025, 10, 1)


GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"


@pytest.fixture(scope="module")
def google_key():
    """A service-account key with a freshly generated RSA private key."""
    rsa = pytest.importorskip("cryptography.hazmat.primitives.asymmetric.rsa")
    serialization = pytest.importorskip("cryptography.hazmat.primitives.serialization")
    pem = rsa.generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )
    return {
        "type": "service_account", "project_id": "p1", "private_key_id": "k1", "private_key": pem.decode(),
        "client_email": "reader@p1.iam.gserviceaccount.com", "client_id": "1", "token_uri": GOOGLE_TOKEN_URL,
    }


class GoogleHarness:
    def __init__(self, monkeypatch, key):
        self.monkeypatch, self.key = monkeypatch, key
        self.session, self.local = FakeSession(), FakeLocal()
        self.session.route("POST", key["token_uri"], [{"access_token": "tok", "expires_in": 3600, "token_type": "Bearer"}])
        self.set_credentials(json.dumps(key))
        self.series, self.queries = {}, []
        self.answer = self.session.request
        self.session.request = self.request
        self.settings()

    def route_series(self, project, metric_type, *pages):
        """Pages (or one failing FakeResponse) answered for a project's metric; paging uses `pageToken` page-N."""
        self.series[(project, metric_type)] = list(pages)

    def request(self, method, url, **kwargs):
        match = re.fullmatch(plugin.GOOGLE_TIME_SERIES_URL.format(project="(.+)"), url)
        if method != "GET" or not match:
            return self.answer(method, url, **kwargs)
        params = kwargs["params"]
        metric_type = re.search(r'metric\.type = "([^"]+)"', dict(params)["filter"]).group(1)
        self.queries.append({"project": match.group(1), "metric": metric_type, "params": params})
        token = dict(params).get("pageToken")
        pages = self.series.get((match.group(1), metric_type), [{}])
        item = pages[int(token.split("-")[1]) if token else 0]
        return item if isinstance(item, FakeResponse) else FakeResponse(200, item)

    def query(self, metric_type, project="usage-test-1"):
        return next(q for q in self.queries if q["project"] == project and q["metric"] == metric_type)["params"]

    def usage(self, usage_types=("input_tokens", "output_tokens", "requests"), bucket_width="1h"):
        start, end = datetime(2026, 11, 17, 16, tzinfo=timezone.utc), datetime(2026, 11, 17, 18, tzinfo=timezone.utc)
        return list(self.connector.fetch_usage(start, end, bucket_width, list(usage_types)))

    def set_credentials(self, value):
        self.monkeypatch.setenv("GOOGLE_LLM_USAGE_CREDENTIALS", value)

    def settings(self, **overrides):
        config = {"google_project_ids": "usage-test-1", **overrides}
        self.connector = plugin.GoogleConnector(settings_for("google", **config), HttpClient(self.session), self.local)
        return self

    def token_requests(self):
        return [c for c in self.session.calls if c["url"] == self.key["token_uri"]]


@pytest.fixture
def google(monkeypatch, google_key):
    return GoogleHarness(monkeypatch, google_key)


def assert_key_in_use(google, google_key):
    assert google.connector.auth_headers() == {"Authorization": "Bearer tok"}
    assert google.connector.credentials.service_account_email == google_key["client_email"]


def test_google_key_from_multiline_json_in_env(google, google_key):
    google.set_credentials(json.dumps(google_key, indent=2))
    assert_key_in_use(google.settings(), google_key)


def test_google_key_from_path_relative_to_plugin_dir(google, google_key, tmp_path, monkeypatch):
    (tmp_path / "keys").mkdir()
    (tmp_path / "keys" / "k.json").write_text(json.dumps(google_key))
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    google.set_credentials("keys/k.json")
    assert_key_in_use(google.settings(), google_key)


@pytest.mark.parametrize("value, message", [
    ("", "environment variable GOOGLE_LLM_USAGE_CREDENTIALS is not set"),
    ("missing/k.json", "environment variable GOOGLE_LLM_USAGE_CREDENTIALS names key file .*missing.k.json, which can't be read"),
    ("/abs/k.json", "environment variable GOOGLE_LLM_USAGE_CREDENTIALS names key file /abs/k.json, which can't be read"),
    ("C:\\keys\\k.json", r"names key file .*C:\\keys\\k\.json, which can't be read"),
    ("k.JSON", r"names key file .*k\.JSON, which can't be read"),
    ("keys/key", r"names key file .*keys.key, which can't be read"),
    ('{"type": "authorized_user"}', "environment variable GOOGLE_LLM_USAGE_CREDENTIALS does not hold a service account key"),
    ('{"type": "service_account", "client_email": "x"}', "environment variable GOOGLE_LLM_USAGE_CREDENTIALS does not hold a service account key"),
])
def test_google_key_errors_name_variable_and_path_only(google, google_key, tmp_path, monkeypatch, value, message):
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    (tmp_path / "keys").mkdir()
    google.set_credentials(value)
    with pytest.raises(ConfigError, match=message) as error:
        google.settings()
    assert "PRIVATE KEY" not in str(error.value) and google_key["client_email"] not in str(error.value)


@pytest.mark.parametrize("value", ["AIzaSyD-not-a-real-key-0123456789", "sk-admin-not-a-real-key", "credentials",
                                   "fakeAwsSecret/notReal0123/abcDEF+ghi", "/fakeAwsSecretNotReal0123abcDEF+ghi", "x" * 300 + "/k"])
def test_google_value_that_is_no_path_is_never_echoed(google, tmp_path, monkeypatch, value):
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    google.set_credentials(value)
    with pytest.raises(ConfigError, match="^environment variable GOOGLE_LLM_USAGE_CREDENTIALS does not hold a service account key$"):
        google.settings()


@pytest.mark.parametrize("wrap", [
    lambda text: f"'{text}'",
    lambda text: base64.b64encode(text.encode()).decode(),
    lambda text: "﻿" + text,
    lambda text: "{bad " + text,
    lambda text: "keys/" + text,
], ids=["quoted", "base64", "bom", "broken-inline", "multiline-path"])
def test_google_key_content_never_reaches_the_error(google, google_key, wrap):
    value = wrap(json.dumps(google_key, indent=2))
    google.set_credentials(value)
    with pytest.raises(ConfigError, match="environment variable GOOGLE_LLM_USAGE_CREDENTIALS does not hold a service account key") as error:
        google.settings()
    message = str(error.value)
    assert "PRIVATE KEY" not in message and google_key["client_email"] not in message and value not in message


def test_google_malformed_private_key_is_a_config_error(google, google_key):
    broken = {**google_key, "private_key": "-----BEGIN PRIVATE KEY-----\nnot a key\n-----END PRIVATE KEY-----\n"}
    google.set_credentials(json.dumps(broken))
    with pytest.raises(ConfigError, match="environment variable GOOGLE_LLM_USAGE_CREDENTIALS does not hold a service account key") as error:
        google.settings()
    assert "not a key" not in str(error.value)


def test_google_token_requested_once_through_the_plugin_session(google):
    assert google.connector.auth_headers() == {"Authorization": "Bearer tok"}
    assert google.connector.auth_headers() == {"Authorization": "Bearer tok"}
    assert len(google.token_requests()) == 1


def test_google_token_failure_hides_the_key(google, google_key):
    google.session.route("POST", google_key["token_uri"], [FakeResponse(400, {"error": "invalid_grant"})])
    with pytest.raises(ProviderError) as error:
        google.connector.auth_headers()
    assert str(error.value) == "token request failed: RefreshError"
    assert "invalid_grant" not in str(error.value) and "PRIVATE KEY" not in str(error.value)


def test_google_connector_shell(google):
    now = datetime(2026, 11, 14, tzinfo=timezone.utc)
    assert CONNECTORS["google"] is plugin.GoogleConnector
    assert google.connector.usage_types == {"input_tokens", "output_tokens", "requests"}
    assert google.connector.collects_cost() is False
    assert google.connector.oldest_usage(now) == now - timedelta(weeks=6)
    assert google.connector.account_label() == "default"
    assert google.settings(google_account="prod").connector.account_label() == "prod"


BQ_URL = "https://bigquery.googleapis.com/bigquery/v2/projects/billing-1"
BQ_FIELDS = ["day", "project_id", "location_name", "service_id", "sku_description", "currency", "cost"]
VERTEX_SERVICE, GEMINI_SERVICE = "C7E2-9256-1C43", "AEFD-7695-64FA"
EXPORT_DAY = (date(2026, 11, 16), date(2026, 11, 18))


def bq_page(rows, **extra):
    """A BigQuery result page: rows are lists of values in BQ_FIELDS order."""
    return {"jobComplete": True, "jobReference": {"jobId": "job-1", "location": "EU"},
            "schema": {"fields": [{"name": name} for name in BQ_FIELDS]},
            "rows": [{"f": [{"v": value} for value in row]} for row in rows], **extra}


def billing(google, *pages, **overrides):
    """Routes the dataset lookup and the query for table billing-1.ds.export."""
    google.session.route("GET", f"{BQ_URL}/datasets/ds", [{"location": "EU"}])
    google.session.route("POST", f"{BQ_URL}/queries", [pages[0]])
    google.session.route("GET", f"{BQ_URL}/queries/job-1", list(pages[1:]) or [pages[0]])
    return google.settings(google_sources="vertex_metrics billing_export", google_billing_table="billing-1.ds.export", **overrides)


def bq_calls(google, method):
    return [c for c in google.session.calls if c["method"] == method and c["url"].startswith(BQ_URL)]


VERTEX_ROW = ["2026-11-17", "usage-test-1", "europe-west4", VERTEX_SERVICE, "Gemini 3.5 Flash Lite Global Text Input - Predictions", "EUR", "0.01385"]


def test_google_billing_export_enables_cost_and_the_bigquery_scope(google):
    assert google.connector.collects_cost() is False
    billing(google, bq_page([]))
    assert google.connector.collects_cost() is True
    google.connector.auth_headers()
    assert set(google.connector.credentials.scopes) == {plugin.GOOGLE_MONITORING_SCOPE, plugin.GOOGLE_BIGQUERY_SCOPE}
    assert google.settings().connector.credentials is None
    google.connector.auth_headers()
    assert google.connector.credentials.scopes == [plugin.GOOGLE_MONITORING_SCOPE]
    billing(google, bq_page([]), collect="usage").connector.auth_headers()
    assert google.connector.credentials.scopes == [plugin.GOOGLE_MONITORING_SCOPE]


def test_google_billing_export_query(google):
    billing(google, bq_page([]))
    list(google.connector.fetch_cost(*EXPORT_DAY))
    list(google.connector.fetch_cost(*EXPORT_DAY))
    assert len(bq_calls(google, "GET")) == 1 and bq_calls(google, "GET")[0]["url"].endswith("/datasets/ds")
    body = bq_calls(google, "POST")[0]["json"]
    assert body["useLegacySql"] is False and body["parameterMode"] == "NAMED" and body["location"] == "EU"
    parameters = {p["name"]: p["parameterValue"] for p in body["queryParameters"]}
    assert parameters == {
        "first_day": {"value": "2026-11-16"}, "start": {"value": "2026-11-16 00:00:00+00"}, "end": {"value": "2026-11-18 00:00:00+00"},
        "projects": {"arrayValues": [{"value": "usage-test-1"}]},
        "services": {"arrayValues": [{"value": VERTEX_SERVICE}, {"value": GEMINI_SERVICE}]},
    }
    sql = " ".join(body["query"].split())
    assert "FROM `billing-1.ds.export`" in sql and "DATE(_PARTITIONTIME) >= DATE_SUB(@first_day, INTERVAL 1 DAY)" in sql
    assert "cost_type IN ('regular', 'adjustment')" in sql and "SUM(cost)" in sql
    assert "GROUP BY day, project_id, location_name, service_id, sku_description, currency" in sql
    assert "usage-test-1" not in sql and VERTEX_SERVICE not in sql


def test_google_billing_export_rows(google):
    billing(google, bq_page([
        VERTEX_ROW,
        ["2026-11-17", "usage-test-1", None, GEMINI_SERVICE, "Gemini 3.5 Flash Input", "USD", "1.5"],
    ]))
    first, second = list(google.connector.fetch_cost(*EXPORT_DAY))
    assert (first.time, first.provider, first.billing_item, first.currency, first.cost) == (
        datetime(2026, 11, 17, tzinfo=timezone.utc), "google", VERTEX_ROW[4], "EUR", 0.01385)
    assert first.tags == {"account": "default", "project": "usage-test-1", "region": "europe-west4", "service": "vertex"}
    assert second.tags == {"account": "default", "project": "usage-test-1", "service": "gemini_api"} and second.cost == 1.5


def test_google_billing_export_polls_and_pages(google):
    billing(google, {"jobComplete": False, "jobReference": {"jobId": "job-1", "location": "EU"}},
            {"jobComplete": False, "jobReference": {"jobId": "job-1", "location": "EU"}},
            bq_page([VERTEX_ROW], pageToken="p2"), bq_page([VERTEX_ROW[:4] + ["Other", "EUR", "2"]]))
    assert [r.cost for r in google.connector.fetch_cost(*EXPORT_DAY)] == [0.01385, 2.0]
    polls = bq_calls(google, "GET")[1:]
    assert len(polls) == 3 and all(p["url"].endswith("/queries/job-1") and p["params"]["location"] == "EU" for p in polls)
    assert [p["params"].get("pageToken") for p in polls] == [None, None, "p2"]


def test_google_billing_export_that_never_completes_is_recorded(google):
    billing(google, {"jobComplete": False, "jobReference": {"jobId": "job-1", "location": "EU"}})
    assert list(google.connector.fetch_cost(*EXPORT_DAY)) == []
    assert google.connector.errors == ["billing export chunk 2026-11-16 to 2026-11-17 skipped: query did not complete"]


def test_google_billing_export_failed_query_skips_only_its_slice(google, monkeypatch):
    billing(google, FakeResponse(403, {"error": {"message": "denied"}}))
    settings = google.connector.settings
    google.session.route("POST", f"{BQ_URL}/queries", [FakeResponse(403, {"error": "denied"}), bq_page([VERTEX_ROW])])
    monkeypatch.setattr(plugin, "COST_SLICE_SPAN", timedelta(days=1))
    result = {"cost_rows": 0, "errors": []}
    plugin.collect_in_slices(FakeLocal(), settings, result, result["errors"].append, "cost",
                             plugin.period_chunks(*EXPORT_DAY, plugin.COST_SLICE_SPAN), google.connector.fetch_cost)
    assert google.connector.errors == ["billing export chunk 2026-11-16 to 2026-11-16 skipped: HTTP 403: denied"]
    assert result["cost_rows"] == 1 and result["errors"] == []


def test_google_without_billing_export_makes_no_bigquery_call(google):
    list(google.connector.fetch_usage(NOW - timedelta(hours=2), NOW, "1h", ["requests"]))
    assert not bq_calls(google, "GET") and not bq_calls(google, "POST")
    result = {"cost_period": None, "notes": []}
    plugin.collect_cost(FakeLocal(), google.connector.settings, google.connector, result, print, NOW, True)
    assert result["cost_period"] is None and not bq_calls(google, "POST")


def vertex_series(value, *, model="gemini-3.5-flash-lite", location="global", type=None, start="2026-11-17T16:00:00Z"):
    labels = {"location": location, "model_user_id": model, "project_id": "usage-test-1"}
    metric = {"labels": {"type": type}} if type else {}
    return {"resource": {"labels": labels}, "metric": metric, "points": [{"interval": {"startTime": start}, "value": value}]}


def vertex_routes(google, project="usage-test-1"):
    google.route_series(project, plugin.VERTEX_TOKENS, {"timeSeries": [
        vertex_series({"int64Value": "40496"}, type="input"),
        vertex_series({"int64Value": "2871"}, type="output"),
        vertex_series({"int64Value": "99"}, type="other"),
    ]})
    google.route_series(project, plugin.VERTEX_INVOCATIONS, {"timeSeries": [vertex_series({"int64Value": "6"})]})


def test_vertex_tokens_and_requests_mapped_with_tags(google):
    vertex_routes(google)
    records = google.usage()
    assert {r.usage_type: r.quantity for r in records} == {"input_tokens": 40496, "output_tokens": 2871, "requests": 6}
    assert all(r.provider == "google" and r.time == datetime(2026, 11, 17, 16, tzinfo=timezone.utc) for r in records)
    assert all(r.tags == {"account": "default", "project": "usage-test-1", "region": "global",
                          "model": "gemini-3.5-flash-lite", "service": "vertex"} for r in records)


def test_vertex_request_params(google):
    google.usage()
    tokens = google.query(plugin.VERTEX_TOKENS)
    assert ("filter", f'metric.type = "{plugin.VERTEX_TOKENS}"') in tokens
    assert [v for k, v in tokens if k == "aggregation.groupByFields"] == [
        "resource.label.location", "resource.label.model_user_id", "metric.label.type"]
    assert dict(tokens) | {"aggregation.groupByFields": None} == {
        "filter": f'metric.type = "{plugin.VERTEX_TOKENS}"', "interval.startTime": "2026-11-17T16:00:00Z",
        "interval.endTime": "2026-11-17T18:00:00Z", "aggregation.alignmentPeriod": "3600s",
        "aggregation.perSeriesAligner": "ALIGN_SUM", "aggregation.crossSeriesReducer": "REDUCE_SUM",
        "aggregation.groupByFields": None}
    invocations = google.query(plugin.VERTEX_INVOCATIONS)
    assert dict(invocations)["filter"] == f'metric.type = "{plugin.VERTEX_INVOCATIONS}" AND metric.label.response_code = starts_with("2")'
    assert [v for k, v in invocations if k == "aggregation.groupByFields"] == [
        "resource.label.location", "resource.label.model_user_id"]
    google.queries.clear()
    google.usage(bucket_width="1d")
    assert dict(google.query(plugin.VERTEX_TOKENS))["aggregation.alignmentPeriod"] == "86400s"


def test_vertex_values_parse_int_strings_and_doubles_and_follow_pages(google):
    google.route_series("usage-test-1", plugin.VERTEX_INVOCATIONS,
                        {"timeSeries": [vertex_series({"int64Value": "5"})], "nextPageToken": "page-1"},
                        {"timeSeries": [vertex_series({"doubleValue": 2.0}, model="gemini-3.5-pro")]})
    assert sorted((r.tags["model"], r.quantity) for r in google.usage(["requests"])) == [
        ("gemini-3.5-flash-lite", 5.0), ("gemini-3.5-pro", 2.0)]
    assert dict(google.queries[1]["params"])["pageToken"] == "page-1"


def test_google_empty_project_gives_no_rows_and_no_error(google):
    assert google.usage() == [] and google.connector.errors == []


def test_google_failed_source_is_recorded_and_the_rest_continues(google):
    google.settings(google_project_ids="usage-test-1 usage-test-2")
    google.route_series("usage-test-1", plugin.VERTEX_TOKENS, FakeResponse(403, {"error": "denied"}))
    route_gemini(google, plugin.GEMINI_OUTPUT_TOKENS, gemini_series("m", None, (16, 5)))
    vertex_routes(google, "usage-test-2")
    records = google.usage()
    assert google.connector.errors == ["Vertex AI metrics of project usage-test-1 skipped: HTTP 403: denied"]
    assert {(r.tags["project"], r.tags["service"]) for r in records} == {("usage-test-1", "gemini_api"), ("usage-test-2", "vertex")}


def test_an_error_repeated_in_every_slice_is_recorded_and_logged_once(google, monkeypatch):
    google.route_series("usage-test-1", plugin.VERTEX_TOKENS, FakeResponse(403, {"error": "denied"}))
    monkeypatch.setattr(plugin, "make_http_client", lambda: HttpClient(google.session))
    body = json.dumps({"start": "2026-10-20T00:00:00Z", "end": "2026-11-09T00:00:00Z"})
    settings = load_settings(FakeLocal(), "t", {"providers": "google", "google_project_ids": "usage-test-1"}, request_body=body, http=True, now=NOW)
    local = FakeLocal()
    result = collect(local, settings, "t", now=NOW, backfill=True)["google"]
    assert len([q for q in google.queries if q["metric"] == plugin.VERTEX_TOKENS]) == 3
    assert result["errors"] == ["Vertex AI metrics of project usage-test-1 skipped: HTTP 403: denied"]
    assert local.messages["error"] == ["[t] google: Vertex AI metrics of project usage-test-1 skipped: HTTP 403: denied"]


def test_google_failed_token_is_recorded_for_every_project_and_source(google):
    google.settings(google_project_ids="usage-test-1 usage-test-2")
    google.session.route("POST", google.key["token_uri"], [FakeResponse(400, {"error": "invalid_grant"})])
    assert google.usage() == []
    assert google.connector.errors == [f"{source} metrics of project {p} skipped: token request failed: RefreshError"
                                       for p in ("usage-test-1", "usage-test-2") for source in ("Vertex AI", "Gemini API")]


def test_vertex_calls_only_for_selected_usage_types_and_sources(google):
    google.settings(google_sources="vertex_metrics").usage(["input_tokens"])
    assert [q["metric"] for q in google.queries] == [plugin.VERTEX_TOKENS]
    google.queries.clear()
    google.settings(google_sources="gemini_api_metrics").usage()
    assert not [q for q in google.queries if "aiplatform" in q["metric"]]


def gemini_series(model, limit, *points):
    """A quota series of one limit; `points` are (hour, value) pairs."""
    return {
        "resource": {"labels": {"project_id": "usage-test-1"}}, "metric": {"labels": {"model": model, "limit_name": limit}},
        "points": [{"interval": {"startTime": f"2026-11-17T{hour}:00:00Z"}, "value": {"int64Value": str(value)}}
                   for hour, value in points],
    }


def route_gemini(google, metric_type, *series, project="usage-test-1"):
    google.route_series(project, metric_type, {"timeSeries": list(series)})


FREE_INPUT, PAID_INPUT = plugin.GEMINI_INPUT_TOKEN_QUOTAS[0], plugin.GEMINI_INPUT_TOKEN_QUOTAS[1]
FREE_REQUESTS = plugin.GEMINI_REQUEST_QUOTAS[0]


def test_gemini_output_input_and_requests_mapped(google):
    google.settings(google_sources="gemini_api_metrics")
    route_gemini(google, plugin.GEMINI_OUTPUT_TOKENS, gemini_series("gemini-3.5-flash", None, (16, 1435)))
    route_gemini(google, FREE_INPUT, gemini_series("gemini-3.5-flash", "PerDay", (16, 38225)),
                 gemini_series("gemini-3.5-flash", "PerMinute", (16, 38225)))
    route_gemini(google, FREE_REQUESTS, gemini_series("gemini-3.5-flash", "PerDay", (16, 6)),
                 gemini_series("gemini-3.5-flash", "PerMinute", (16, 6)))
    records = google.usage()
    assert {r.usage_type: r.quantity for r in records} == {"output_tokens": 1435, "input_tokens": 38225, "requests": 6}
    assert len(records) == 3
    assert all(r.provider == "google" and r.time == datetime(2026, 11, 17, 16, tzinfo=timezone.utc) for r in records)
    assert all(r.tags == {"account": "default", "project": "usage-test-1", "model": "gemini-3.5-flash", "service": "gemini_api"}
               for r in records)


def test_gemini_one_limit_series_per_tier_even_across_an_hour_boundary(google):
    route_gemini(google, FREE_INPUT, gemini_series("m", "PerDay", (17, 5000), (18, 3000)),
                 gemini_series("m", "PerMinute", (17, 6000), (18, 2000)))
    records = google.usage(["input_tokens"])
    assert sorted((r.time.hour, r.quantity) for r in records) == [(17, 5000), (18, 3000)]


def test_gemini_tiers_are_summed(google):
    route_gemini(google, FREE_INPUT, gemini_series("m", "PerDay", (16, 300)))
    route_gemini(google, PAID_INPUT, gemini_series("m", "PerDay", (16, 700)))
    records = google.usage(["input_tokens"])
    assert sorted(r.quantity for r in records) == [300, 700]
    assert sum(r.quantity for r in records) == 1000


def test_gemini_smaller_limit_ignored_and_ties_deterministic(google):
    route_gemini(google, FREE_INPUT, gemini_series("m", "PerDay", (16, 10)), gemini_series("m", "PerMinute", (16, 90)),
                 gemini_series("n", "PerMinute", (16, 5)), gemini_series("n", "PerDay", (16, 5)))
    assert sorted((r.tags["model"], r.quantity) for r in google.usage(["input_tokens"])) == [("m", 90), ("n", 5)]
    series = [gemini_series("n", "PerMinute", (16, 5)), gemini_series("n", "PerDay", (16, 5))]
    assert plugin.largest_limit_series(series)["n"]["metric"]["labels"]["limit_name"] == "PerDay"


def test_gemini_series_split_over_pages_is_one_series(google):
    pages = [{"timeSeries": [gemini_series("m", "PerMinute", (17, 40))], "nextPageToken": "page-1"},
             {"timeSeries": [gemini_series("m", "PerMinute", (16, 30)), gemini_series("m", "PerDay", (16, 50))]}]
    google.route_series("usage-test-1", FREE_INPUT, *pages)
    assert sorted((r.time.hour, r.quantity) for r in google.usage(["input_tokens"])) == [(16, 30), (17, 40)]


def test_gemini_request_params(google):
    google.usage()
    for metric_type in (FREE_INPUT, FREE_REQUESTS):
        params = google.query(metric_type)
        assert [v for k, v in params if k == "aggregation.groupByFields"] == ["metric.label.model", "metric.label.limit_name"]
    output = google.query(plugin.GEMINI_OUTPUT_TOKENS)
    assert [v for k, v in output if k == "aggregation.groupByFields"] == ["metric.label.model"]


def test_gemini_calls_only_for_selected_usage_types_and_sources(google):
    google.settings(google_sources="gemini_api_metrics").usage(["output_tokens"])
    assert [q["metric"] for q in google.queries] == [plugin.GEMINI_OUTPUT_TOKENS]
    google.queries.clear()
    google.usage(["requests"])
    assert [q["metric"] for q in google.queries] == list(plugin.GEMINI_REQUEST_QUOTAS)
    google.queries.clear()
    google.settings(google_sources="vertex_metrics").usage()
    assert not [q for q in google.queries if "generativelanguage" in q["metric"]]


ALL_PROVIDERS = {"anthropic", "openai", "bedrock", "azure", "google"}


def all_provider_args(**overrides):
    """Trigger arguments that configure every provider."""
    return {"providers": "anthropic openai bedrock azure google", "bedrock_regions": "us-east-1", "azure_tenant_id": TENANT_ID,
            "azure_client_id": CLIENT_ID, "azure_subscription_ids": SUBSCRIPTION_ID, "google_project_ids": "usage-test-1", **overrides}


class AllProvidersHarness:
    """One FakeSession routed for Anthropic, OpenAI, Azure and Google; Bedrock answered by Stubbers."""

    def __init__(self, monkeypatch, google_key):
        monkeypatch.setenv("GOOGLE_LLM_USAGE_CREDENTIALS", json.dumps(google_key))
        self.session = FakeSession()
        monkeypatch.setattr(plugin, "make_http_client", lambda: HttpClient(self.session))
        self.route_anthropic()
        self.route_openai()
        self.route_azure()
        self.route_google(google_key)
        self.bedrock = BedrockHarness(monkeypatch)
        self.route_bedrock()
        monkeypatch.setattr(
            plugin.BedrockConnector, "_client", lambda connector, service, region: self.bedrock.stub(service, region)[0]
        )

    def route_anthropic(self):
        self.session.route("GET", f"{ANTHROPIC_BASE}/analytics/usage", [{"data": [{
            "starting_at": "2026-11-13T18:00:00Z", "ending_at": "2026-11-13T19:00:00Z",
            "group": {"model": "claude-opus-5-5", "workspace_id": "wrkspc_1", "api_key_id": "apikey_1",
                      "actor": {"type": "service_account", "id": "svac_1"}, "actor_type": "service_account"},
            "usage": {"uncached_input_tokens": 100, "output_tokens": 20, "requests": 2}}], "has_more": False}])
        self.session.route("GET", f"{ANTHROPIC_BASE}/analytics/cost", [{"data": [{
            "starting_at": "2026-11-13T00:00:00Z",
            "group": {"model": "claude-opus-5-5", "workspace_id": "wrkspc_1", "api_key_id": "apikey_1", "cost_type": "tokens", "token_type": "output_tokens"},
            "cost": {"currency": "USD", "amount": "250.0"}}], "has_more": False}])
        self.session.route("GET", f"{ANTHROPIC_BASE}/api_keys", [{"data": [{"id": "apikey_1", "name": "ci-key"}], "has_more": False}])

    def route_openai(self):
        for path in plugin.OPENAI_USAGE_ENDPOINTS:
            self.session.route("GET", f"{OPENAI_BASE}/{path}", [{"data": [], "has_more": False}])
        self.session.route("GET", f"{OPENAI_BASE}/usage/completions", [page(result(
            api_key_id="key_1", user_id="user-1", input_tokens=50, input_uncached_tokens=50, output_tokens=5, num_model_requests=1))])
        self.session.route("GET", f"{OPENAI_BASE}/costs", [{"data": [{"start_time": 1794510960, "results": [
            {"project_id": "proj_1", "api_key_id": "key_1", "line_item": "gpt-5-nano-2025-08-07, input", "amount": {"value": 0.25, "currency": "usd"}}]}], "has_more": False}])
        self.session.route("GET", f"{OPENAI_BASE}/projects", [{"data": [{"id": "proj_1"}], "has_more": False}])
        self.session.route("GET", f"{OPENAI_BASE}/projects/proj_1/api_keys", [{"data": [{"id": "key_1", "name": "batch-key"}], "has_more": False}])

    def route_azure(self):
        self.session.route("POST", AZURE_TOKEN_URL, [{"access_token": "tok", "expires_in": 3600}])
        self.session.route("GET", ARM_ACCOUNTS, [{"value": [account("usage-test-aoai", "OpenAI")]}])
        self.session.route("POST", METRICS_URL, [{"values": [batch_value("usage-test-aoai", {
            "InputTokens": [({"modeldeploymentname": "dep", "modelname": "gpt-4.1-mini"}, [("2026-11-13T18:00:00Z", 70)])]})]}])
        self.session.route("POST", COST_URL, [{"properties": {
            "columns": [{"name": "Cost"}, {"name": "UsageDate"}, {"name": "ResourceId"}, {"name": "Meter"}, {"name": "Currency"}],
            "rows": [[1.5, stamp, account_id("usage-test-aoai"), "gpt-4.1-mini Inp", "USD"]]}} for stamp in (20261020, 20261113)])

    def route_google(self, key):
        """The token POST, and timeSeries GETs answered by metric type: Vertex tokens carry one series, the rest are empty."""
        self.session.route("POST", key["token_uri"], [{"access_token": "tok", "expires_in": 3600, "token_type": "Bearer"}])
        answer_route = self.session.request

        def request(method, url, **kwargs):
            if method != "GET" or not url.endswith("/timeSeries"):
                return answer_route(method, url, **kwargs)
            self.session.calls.append({"method": method, "url": url, **kwargs})
            if f'metric.type = "{plugin.VERTEX_TOKENS}"' not in dict(kwargs["params"])["filter"]:
                return FakeResponse(200, {})
            return FakeResponse(200, {"timeSeries": [vertex_series({"int64Value": "400"}, type="input", start="2026-11-13T18:00:00Z")]})

        self.session.request = request

    def route_bedrock(self):
        self.bedrock.list_metrics("AWS/Bedrock", [("InputTokenCount", [("ModelId", "us.amazon.nova-micro-v1:0")])])
        self.bedrock.list_metrics("AWS/BedrockMantle", [])
        self.bedrock.metric_data({"us.amazon.nova-micro-v1:0/InputTokenCount": [(datetime(2026, 11, 13, 18, tzinfo=timezone.utc), 30.0)]})
        self.bedrock.cost_groups(date(2026, 11, 13), [(["model$Nova Micro", "USE1-NovaMicro-input-tokens"], "0.5")])

    def args(self, **overrides):
        return all_provider_args(**overrides)

    def key_name_requests(self):
        return len([c for c in self.session.calls if c["url"].endswith("/api_keys") or ("/projects" in c["url"] and "/organization/" in c["url"])])


@pytest.fixture
def all_providers(monkeypatch, google_key):
    return AllProvidersHarness(monkeypatch, google_key)


def test_scheduled_run_writes_every_provider(all_providers):
    local = FakeLocal()
    plugin.process_scheduled_call(local, NOW, all_providers.args())
    usage, cost = written_rows(local)
    assert {r["tags"]["provider"] for r in usage} == ALL_PROVIDERS
    assert {r["tags"]["provider"] for r in cost} == ALL_PROVIDERS - {"google"}
    for tag in ("account", "project", "api_key_id", "user_id"):
        assert any(tag in r["tags"] for r in usage), tag
    assert any("api_key_name" in r["fields"] for r in usage + cost)
    assert not local.messages["error"]


def test_scheduled_run_with_identifiers_excluded(all_providers):
    local = FakeLocal()
    plugin.process_scheduled_call(local, NOW, all_providers.args(exclude_tags="account project api_key_id user_id"))
    usage, cost = written_rows(local)
    assert {r["tags"]["provider"] for r in usage} == ALL_PROVIDERS
    assert {r["tags"]["provider"] for r in cost} == ALL_PROVIDERS - {"google"}
    assert not any(set(r["tags"]) & {"account", "project", "api_key_id", "user_id"} for r in usage + cost)
    assert not any("api_key_name" in r["fields"] for r in usage + cost)
    assert all_providers.key_name_requests() == 0


def test_scheduled_run_writes_google_billing_export_cost(all_providers):
    all_providers.session.route("GET", f"{BQ_URL}/datasets/ds", [{"location": "EU"}])
    all_providers.session.route("POST", f"{BQ_URL}/queries", [bq_page([["2026-11-13"] + VERTEX_ROW[1:]])])
    local = FakeLocal()
    args = all_providers.args(providers="google", google_sources="vertex_metrics billing_export", google_billing_table="billing-1.ds.export")
    plugin.process_scheduled_call(local, NOW, args)
    _usage, cost = written_rows(local)
    assert [(r["tags"]["service"], r["tags"]["region"], r["tags"]["billing_item"]) for r in cost] == [("vertex", "europe-west4", VERTEX_ROW[4])]
    assert not local.messages["error"]


def test_backfill_response_reports_each_provider(all_providers, monkeypatch):
    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW

    monkeypatch.setattr(plugin, "datetime", FrozenDatetime)
    body = json.dumps({"start": "2026-11-12T00:00:00Z", "end": "2026-11-13T00:00:00Z", "providers": ["openai", "azure", "google"]})
    result = process_request(FakeLocal(), {}, {}, body, all_providers.args())
    assert result["status"] == "completed" and set(result["providers"]) == {"openai", "azure", "google"}
    assert all(p["usage_rows"] > 0 and p["errors"] == [] for p in result["providers"].values())
    assert result["providers"]["google"]["cost_period"] is None


NAME_RULE = "use letters, digits, _ and -, starting with a letter or digit"
TABLE_RULE = "spaces, control characters and backslashes aren't allowed"
# trigger arguments, environment variables (None removes one), modules made unimportable, the error
CONFIG_ERRORS = [
    ({"bedrock_regions": ""}, {}, (), "bedrock_regions required when bedrock is listed in providers"),
    ({"azure_tenant_id": "", "azure_subscription_ids": ""}, {}, (),
     "azure_tenant_id azure_subscription_ids required when azure is listed in providers"),
    ({"google_project_ids": ""}, {}, (), "google_project_ids required when google is listed in providers"),
    ({"google_sources": "vertex_metrics billing_export"}, {}, (),
     "google_billing_table required when billing_export is listed in google_sources"),
    ({"bedrock_regions": "us-east-1 us_east_1"}, {}, (), "bedrock_regions: invalid region name us_east_1"),
    ({"azure_tenant_id": "contoso.onmicrosoft.com"}, {}, (), "azure_tenant_id: invalid GUID contoso.onmicrosoft.com"),
    ({"azure_client_id": "client"}, {}, (), "azure_client_id: invalid GUID client"),
    ({"azure_subscription_ids": f"{SUBSCRIPTION_ID} {SUBSCRIPTION_ID[:-1]}"}, {}, (),
     f"azure_subscription_ids: invalid GUID {SUBSCRIPTION_ID[:-1]}"),
    ({"google_project_ids": "usage-test-1 p1"}, {}, (), "google_project_ids: invalid project ID p1"),
    ({"google_billing_table": "usage-test-1.billing"}, {}, (),
     "google_billing_table: expected <project>.<dataset>.<table>, got usage-test-1.billing"),
    ({"google_billing_table": "usage-test-1.billing.export`x"}, {}, (),
     "google_billing_table: expected <project>.<dataset>.<table>, got usage-test-1.billing.export`x"),
    ({"google_billing_table": "usage-test-1.billing.export x"}, {}, (),
     "google_billing_table: expected <project>.<dataset>.<table>, got usage-test-1.billing.export x"),
    ({"google_billing_table": "usage-test-1.ds.t.x"}, {}, (),
     "google_billing_table: expected <project>.<dataset>.<table>, got usage-test-1.ds.t.x"),
    ({"usage_table": "llm\\usage"}, {}, (), f"usage_table: invalid name {'llm' + chr(92) + 'usage'!r}; {TABLE_RULE}"),
    ({"usage_table": "llm\x01usage"}, {}, (), f"usage_table: invalid name {'llm' + chr(1) + 'usage'!r}; {TABLE_RULE}"),
    ({"cost_table": "llm cost"}, {}, (), f"cost_table: invalid name 'llm cost'; {TABLE_RULE}"),
    ({"target_database": "llm.usage"}, {}, (), f"target_database: invalid name llm.usage; {NAME_RULE}"),
    ({"usage_table": "llm", "cost_table": "llm"}, {}, (), "usage_table and cost_table must differ"),
    ({}, {}, ("boto3",), "boto3 is not installed; install it with `influxdb3 install package boto3`"),
    ({}, {}, ("google.auth",), "google-auth is not installed; install it with `influxdb3 install package google-auth`"),
    ({}, {}, ("google.oauth2.service_account",),
     "google-auth is not installed; install it with `influxdb3 install package google-auth`"),
    ({}, {"ANTHROPIC_ADMIN_KEY": None}, (), "environment variable ANTHROPIC_ADMIN_KEY is not set"),
    ({}, {"BEDROCK_AWS_ACCESS_KEY_ID": None}, (), "environment variable BEDROCK_AWS_ACCESS_KEY_ID is not set"),
    ({"azure_client_secret_env": "MY_AZURE_SECRET"}, {}, (), "environment variable MY_AZURE_SECRET is not set"),
    ({}, {"OPENAI_ADMIN_KEY": "sk-ad\x07min"}, (), "environment variable OPENAI_ADMIN_KEY contains a control character"),
    ({}, {"BEDROCK_AWS_SESSION_TOKEN": "tok\nen"}, (),
     "environment variable BEDROCK_AWS_SESSION_TOKEN contains a control character"),
    ({}, {"GOOGLE_LLM_USAGE_CREDENTIALS": '{"type": "authorized_user"}'}, (),
     "environment variable GOOGLE_LLM_USAGE_CREDENTIALS does not hold a service account key"),
]


@pytest.mark.parametrize("http", [False, True], ids=["scheduled", "request"])
@pytest.mark.parametrize("args, env, unimportable, message", CONFIG_ERRORS)
def test_configuration_errors_refuse_loading(google_key, monkeypatch, http, args, env, unimportable, message):
    monkeypatch.setenv("GOOGLE_LLM_USAGE_CREDENTIALS", json.dumps(google_key))
    for name, value in env.items():
        if value is None:
            monkeypatch.delenv(name)
        else:
            monkeypatch.setenv(name, value)
    for module in unimportable:
        monkeypatch.setitem(sys.modules, module, None)
    body = json.dumps({"start": "2026-10-01T00:00:00Z"}) if http else None
    with pytest.raises(ConfigError) as error:
        load_settings(FakeLocal(), "t", all_provider_args(**args), request_body=body, http=http, now=NOW)
    assert str(error.value) == message


def test_table_names_take_what_the_server_takes(google_key, monkeypatch):
    monkeypatch.setenv("GOOGLE_LLM_USAGE_CREDENTIALS", json.dumps(google_key))
    args = all_provider_args(usage_table="_usage.v2", cost_table="стоимость,`x`")
    settings = load_settings(FakeLocal(), "t", args, http=False, now=NOW)
    assert (settings.usage_table, settings.cost_table) == ("_usage.v2", "стоимость,`x`")


def test_configuration_error_refuses_the_whole_run(all_providers):
    args = all_providers.args(bedrock_regions="us-east-1 us_east_1")
    local = FakeLocal()
    plugin.process_scheduled_call(local, NOW, args)
    body = json.dumps({"start": "2026-11-12T00:00:00Z", "end": "2026-11-13T00:00:00Z"})
    error = "bedrock_regions: invalid region name us_east_1"
    assert process_request(local, {}, {}, body, args) == {"status": "error", "error": error}
    assert local.written == [] and all_providers.session.calls == [] and all_providers.bedrock.calls == []
    assert [m.split("] ", 1)[1] for m in local.messages["error"]] == [f"Collection failed: {error}", f"Invalid request: {error}"]


def test_request_checks_only_the_providers_it_names(all_providers):
    body = json.dumps({"start": "2026-11-12T00:00:00Z", "end": "2026-11-13T00:00:00Z", "providers": ["openai"]})
    result = process_request(FakeLocal(), {}, {}, body, all_providers.args(bedrock_regions="us_east_1"))
    assert result["status"] == "completed" and list(result["providers"]) == ["openai"]
    assert result["providers"]["openai"]["errors"] == []


@pytest.mark.parametrize("region, valid", [
    ("us-east-1", True), ("us-esat-1", True), ("us-gov-west-1", True), ("cn-north-1", True), ("us-iso-east-1", True),
    ("us-isob-east-1", True), ("eusc-de-east-1", True), ("us_east_1", False), ("US-EAST-1", False), ("us-east", False),
])
def test_bedrock_region_names_must_be_region_codes(bedrock, region, valid):
    if valid:
        assert settings_for("bedrock", bedrock_regions=region).provider["bedrock_regions"] == [region]
    else:
        with pytest.raises(ConfigError, match=f"^bedrock_regions: invalid region name {region}$"):
            settings_for("bedrock", bedrock_regions=region)
