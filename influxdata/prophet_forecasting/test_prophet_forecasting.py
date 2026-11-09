"""Unit tests for the prophet_forecasting plugin.

Only the runtime is replaced, and the model is the real Prophet. FakeLocal answers the
plugin's queries from rows in memory: it recognises each query and applies the time
bounds passed as parameters, but does not run SQL, so what the server does with that
SQL is for the live test plan.
"""

import json
import logging
import math
import re
import tomllib
from collections import namedtuple
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import requests

import prophet_forecasting as plugin

logging.getLogger("cmdstanpy").setLevel(logging.WARNING)
logging.getLogger("prophet").setLevel(logging.WARNING)

PLUGIN_DIR = Path(__file__).parent
TAG = "Dictionary(Int32, Utf8)"
SCHEMA = {
    "time": "Timestamp(Nanosecond, None)",
    "host": TAG,
    "region": TAG,
    "usage": "Float64",
    "temp": "Float64",
    "status": "Utf8",
}
SECOND = 10**9
MINUTE = 60 * SECOND
HOUR = 60 * MINUTE
DAY = 24 * HOUR
# 2027-01-15T08:00:00Z, a whole hour, so raw points and sampling buckets line up.
NOW = 1_800_000_000 * SECOND
CALL_TIME = datetime.fromtimestamp(NOW // SECOND, timezone.utc).replace(tzinfo=None)

REQUEST = {
    "source_measurement": "cpu",
    "field": "usage",
    "horizon_steps": 3,
    "window": "30d",
}
QUERY = "SELECT time, host, usage FROM cpu"


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


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

    def float64_field(self, key, value):
        self.fields[key] = float(value)
        return self

    def string_field(self, key, value):
        self.fields[key] = str(value)
        return self

    def time_ns(self, timestamp_ns):
        self.timestamp = timestamp_ns
        return self

    def build(self):
        return json.dumps(
            {
                "measurement": self.measurement,
                "tags": self.tags,
                "fields": self.fields,
                "time": self.timestamp,
            }
        )


Written = namedtuple("Written", ["database", "measurement", "tags", "fields", "time"])


def missing_last(key: tuple) -> tuple:
    return tuple((value is None, str(value)) for value in key)


def aliased(sql: str, pattern: str) -> list:
    """The column names a query aliases with a numbered alias, in alias order."""
    found = re.findall(pattern, sql)
    return [name for name, _ in sorted(found, key=lambda match: int(match[1]))]


class FakeLocal:
    """Stub of the runtime client, answering the plugin's queries from rows in memory."""

    def __init__(
        self, rows=(), schema=None, query_rows=(), explain=("cpu",), fail=None, write_error=None
    ):
        self.rows = list(rows)
        self.schema = SCHEMA if schema is None else schema
        self.query_rows = list(query_rows)
        self.explain = explain
        self.fail = fail or {}  # SQL fragment -> what a query holding it raises
        self.write_error = write_error
        self.queries: list = []
        self.written: list = []
        self.write_calls: list = []
        self.logs: list = []

    def query(self, sql, params=None, database=None):
        self.queries.append(SimpleNamespace(sql=sql, params=params or {}, database=database))
        for fragment, error in self.fail.items():
            if fragment in sql:
                raise error
        if "information_schema.columns" in sql:
            if params["table"] != "cpu":
                return []
            return [{"column_name": n, "data_type": t} for n, t in self.schema.items()]
        if sql.startswith("EXPLAIN"):
            plan = "\n".join(f"  TableScan: {name} projection=[time]" for name in self.explain)
            return [
                {"plan_type": "logical_plan", "plan": plan},
                {"plan_type": "physical_plan", "plan": "TableScan: not_this one"},
            ]
        if "count(*) AS __rows" in sql:
            return self._series(sql, params or {})
        if "AS __first" in sql:
            return self._tag_sets(sql, params or {})
        if " AS __t," in sql:
            return self._history(sql, params or {})
        return [dict(row) for row in self.query_rows]

    def _matching(self, sql: str, params: dict) -> list:
        """The rows with the forecast field, within the time bounds passed as parameters."""
        field = re.search(r'"(\w+)" IS NOT NULL', sql).group(1)
        rows = [row for row in self.rows if row.get(field) is not None]

        def moment(name):
            return plugin._to_ns(datetime.fromisoformat(params[name]))

        if "time >= $history_start" in sql:
            rows = [row for row in rows if row["time"] >= moment("history_start")]
        if "time < $history_end" in sql:
            rows = [row for row in rows if row["time"] < moment("history_end")]
        if "time < $cut" in sql:
            rows = [row for row in rows if row["time"] < moment("cut")]
        if "time >= $cut" in sql:
            rows = [row for row in rows if row["time"] >= moment("cut")]
        return rows

    def _series(self, sql: str, params: dict) -> list:
        groups = aliased(sql, r'"(\w+)" AS __g(\d+)')
        others = aliased(sql, r'count\(CAST\("(\w+)" AS VARCHAR\)\) AS __c(\d+)')
        found: dict = {}
        for row in self._matching(sql, params):
            found.setdefault(tuple(row.get(group) for group in groups), []).append(row)
        if not groups and not found:
            found[()] = []  # an aggregate without GROUP BY answers one row
        answer = []
        for key in sorted(found, key=missing_last):
            rows = found[key]
            out = {f"__g{i}": value for i, value in enumerate(key)}
            out["__rows"] = len(rows)
            for i, tag in enumerate(others):
                present = [str(row[tag]) for row in rows if row.get(tag) is not None]
                out[f"__c{i}"] = len(present)
                out[f"__n{i}"] = len(set(present))
                out[f"__v{i}"] = min(present, default=None)
            if "__total" in sql:
                out["__total"] = len(found)
            answer.append(out)
        return answer[: int(re.search(r"LIMIT (\d+)", sql).group(1))] if "LIMIT" in sql else answer

    def _tag_sets(self, sql: str, params: dict) -> list:
        groups = aliased(sql, r'"(\w+)" AS __g(\d+)')
        others = aliased(sql, r'"(\w+)" AS __o(\d+)')
        rank = re.search(r"__rank <= (\d+)", sql)
        by_group: dict = {}
        for row in self._matching(sql, params):
            by_group.setdefault(tuple(row.get(group) for group in groups), []).append(row)
        spans: dict = {}
        for group, rows in by_group.items():
            if rank:
                recent = sorted({row["time"] for row in rows}, reverse=True)[: int(rank.group(1))]
                rows = [row for row in rows if row["time"] in recent]
            for row in rows:
                key = (group, tuple(row.get(tag) for tag in others))
                first, last = spans.get(key, (row["time"], row["time"]))
                spans[key] = (min(first, row["time"]), max(last, row["time"]))
        answer = []
        for (group, tag_values), (first, last) in spans.items():
            out = {f"__g{i}": value for i, value in enumerate(group)}
            out |= {f"__o{i}": value for i, value in enumerate(tag_values)}
            answer.append({**out, "__first": first, "__last": last})
        return answer[: int(re.search(r"LIMIT (\d+)", sql).group(1))]

    def _history(self, sql: str, params: dict) -> list:
        groups = aliased(sql, r'"(\w+)" AS __g(\d+)')
        field = re.search(r'avg\("(\w+)"\) AS __f0', sql).group(1)
        binned = re.search(r"date_bin\(INTERVAL '(\d+) (seconds|nanoseconds)', time\)", sql)
        step = None
        if binned:
            step = int(binned.group(1)) * (SECOND if binned.group(2) == "seconds" else 1)
        cells: dict = {}
        for row in self._matching(sql, params):
            moment = row["time"] - row["time"] % step if step else row["time"]
            key = (tuple(row.get(group) for group in groups), moment)
            cells.setdefault(key, []).append(row[field])
        answer = []
        for (group, moment), values in cells.items():
            out = {"__t": moment, **{f"__g{i}": value for i, value in enumerate(group)}}
            answer.append({**out, "__f0": sum(values) / len(values)})
        answer.sort(key=lambda row: row["__t"], reverse=True)
        limit = re.search(r"LIMIT (\d+)", sql)
        if limit:
            return answer[: int(limit.group(1))]
        rank = re.search(r"__rn <= (\d+)", sql)
        if not rank:
            return answer[::-1]
        kept: dict = {}
        for row in answer:
            kept.setdefault(tuple(row.get(f"__g{i}") for i in range(len(groups))), []).append(row)
        return sorted(
            (row for rows in kept.values() for row in rows[: int(rank.group(1))]),
            key=lambda row: row["__t"],
        )

    def sent(self, fragment: str) -> list:
        return [query for query in self.queries if fragment in query.sql]

    def history_query(self):
        return self.sent(" AS __t,")[0]

    def _record(self, database, batch, no_sync):
        if self.write_error is not None:
            raise self.write_error
        self.write_calls.append(SimpleNamespace(database=database, no_sync=no_sync))
        for line in batch.build().split("\n"):
            self.written.append(Written(database, **json.loads(line)))

    def write_sync(self, batch, no_sync=False):
        self._record(None, batch, no_sync)

    def write_sync_to_db(self, database, batch, no_sync=False):
        self._record(database, batch, no_sync)

    def info(self, message):
        self.logs.append(("info", message))

    def warn(self, message):
        self.logs.append(("warn", message))

    def error(self, message):
        self.logs.append(("error", message))

    def messages(self, level=None):
        return [text for lvl, text in self.logs if level in (None, lvl)]


@pytest.fixture(autouse=True)
def runtime(monkeypatch):
    """The runtime global, a fixed clock and no settings from the environment."""
    monkeypatch.setattr(plugin, "LineBuilder", FakeLineBuilder, raising=False)
    monkeypatch.setattr(
        plugin,
        "_time",
        SimpleNamespace(time_ns=lambda: NOW, time=lambda: NOW / SECOND, sleep=lambda _: None),
    )
    for name in list(plugin.os.environ):
        if name.startswith(plugin.ENV_PREFIX) or name == "INFLUXDB3_AUTH_TOKEN":
            monkeypatch.delenv(name)


@pytest.fixture
def models_dir(tmp_path, monkeypatch):
    """A plugin directory for saved models."""
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    return tmp_path / plugin.MODEL_DIR_NAME


@pytest.fixture
def fits(monkeypatch):
    """Every series a model was trained on, while still training for real."""
    trained = []
    original = plugin.Prophet.fit

    def fit(self, df, **kwargs):
        trained.append(df)
        return original(self, df, **kwargs)

    monkeypatch.setattr(plugin.Prophet, "fit", fit)
    return trained


def history(count=40, step=HOUR, end=NOW - HOUR, field="usage", value=float, **tags) -> list:
    """count rows one step apart, the last at end, valued value(0), value(1), ..."""
    first = end - (count - 1) * step
    return [{"time": first + i * step, field: value(i), **tags} for i in range(count)]


def forecast(local, **keys):
    """An on-demand request for cpu.usage, 3 points ahead, with keys added; None removes one."""
    body = {key: value for key, value in {**REQUEST, **keys}.items() if value is not None}
    return plugin.process_request(local, {}, {}, json.dumps(body).encode(), None)


def forecast_query(local, query=QUERY, **keys):
    return forecast(
        local, source_measurement=None, field=None, window=keys.pop("window", None), query=query, **keys
    )


def answered(response) -> dict:
    body, status = response
    assert status == 200, body
    assert body["status"] == "ok"
    return body


def refused(response) -> str:
    """The message of a bad_request; every request-level error has this body."""
    body, status = response
    assert (status, body["status"], body["code"]) == (400, "error", "bad_request"), body
    assert set(body) == {"status", "code", "message"}
    return body["message"]


def failed(response, code: str, status: int) -> str:
    body, actual = response
    assert (actual, body["status"], body["code"]) == (status, "error", code), body
    return body["message"]


def result_of(response) -> dict:
    return answered(response)["results"][0]


def times_of(result: dict) -> list:
    return [point["timestamp"] * 10**6 for point in result["forecast"]]


def run_scheduled(local, **args):
    base = {
        "source_measurement": "cpu",
        "field": "usage",
        "horizon_steps": "3",
        "window": "30d",
    }
    settings = {key: value for key, value in {**base, **args}.items() if value is not None}
    return plugin.process_scheduled_call(local, CALL_TIME, settings)


def write_toml(tmp_path, text: str) -> str:
    path = tmp_path / "settings.toml"
    path.write_text(text)
    return str(path)


# ---------------------------------------------------------------------------
# Value formats
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, ns",
    [
        ("250us", 250_000),
        ("500ms", 500_000_000),
        ("30s", 30 * SECOND),
        ("5min", 5 * MINUTE),
        ("6h", 6 * HOUR),
        ("7d", 7 * DAY),
        ("1w", 7 * DAY),
        ("1m", int(30.42 * 86400) * SECOND),
        ("1q", int(91.25 * 86400) * SECOND),
        ("1y", 365 * DAY),
        (" 2h ", 2 * HOUR),
    ],
)
def test_durations_of_the_contract_and_beyond(raw, ns):
    assert plugin._duration_ns(raw) == ns


@pytest.mark.parametrize("raw", ["1ns", "0s", "0ms", "1.5h", "5", "", "h", 5, None])
def test_duration_refuses_anything_else(raw):
    with pytest.raises(ValueError):
        plugin._duration_ns(raw)


@pytest.mark.parametrize(
    "raw, levels",
    [("0.5 0.8", [0.5, 0.8]), ([0.8, 0.5, 0.8], [0.5, 0.8]), (["0.9"], [0.9]), ([], [])],
)
def test_levels_are_sorted_and_unique(raw, levels):
    assert plugin._levels(raw) == levels


@pytest.mark.parametrize("raw", [0.5, "0 0.5", "1", [1.5], [True], ["a"], [None], {"a": 1}])
def test_levels_outside_zero_to_one_are_refused(raw):
    with pytest.raises(ValueError):
        plugin._levels(raw)


@pytest.mark.parametrize(
    "level, key", [(0.5, "50"), (0.8, "80"), (0.975, "97.5"), (0.999, "99.9"), (0.05, "5")]
)
def test_level_key_is_the_shortest_percent(level, key):
    assert plugin._level_key(level) == key


@pytest.mark.parametrize(
    "raw, parsed",
    [
        ("host=a region=eu", {"host": ["a"], "region": ["eu"]}),
        ({"host": "a"}, {"host": ["a"]}),
        ({"host": ["a", "b", "a"]}, {"host": ["a", "b"]}),
        ({"rack": 7}, {"rack": ["7"]}),
    ],
)
def test_tag_filter_forms(raw, parsed):
    assert plugin._tag_filter(raw) == parsed


@pytest.mark.parametrize(
    "raw",
    ["host", "=a", "host=", "host=a host=b", {"host": []}, {"host": True}, {"host": ""}, 5],
)
def test_tag_filter_refuses_malformed_input(raw):
    with pytest.raises(ValueError):
        plugin._tag_filter(raw)


@pytest.mark.parametrize(
    "cast, good, value",
    [
        (plugin._names, "a b a", ["a", "b"]),
        (plugin._items, "a a", ["a", "a"]),
        (plugin._unique_suffix, " v1.2_a-b ", "v1.2_a-b"),
        (plugin._msre_threshold, "inf", math.inf),
        (plugin._dates, "2025-01-01 2025-01-01", [pd.Timestamp("2025-01-01")] * 2),
        (
            plugin._dates,
            ["2025-01-01", "2025-06-01T12:00:00"],
            [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-06-01 12:00")],
        ),
        (plugin._dates, "2025-W01-1", [pd.Timestamp("2024-12-30")]),
        (plugin._dates, "2025-01-01T00:00:00Z", [pd.Timestamp("2025-01-01")]),
        (plugin._dates, "2025-01-01T02:00:00+02:00", [pd.Timestamp("2025-01-01")]),
        (plugin._choice(("a", "b")), " B ", "b"),
        (plugin._moment_ns, "1970-01-01T03:00:01+03:00", SECOND),
    ],
)
def test_casts_accept(cast, good, value):
    assert cast(good) == value


@pytest.mark.parametrize(
    "raw, ns",
    [
        (None, None),
        (123, 123),
        (datetime(1970, 1, 1, 0, 0, 1), SECOND),
        (datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc), SECOND),
        ("1970-01-01T00:00:01Z", SECOND),
        ("not a time", None),
    ],
)
def test_query_timestamps_become_nanoseconds(raw, ns):
    assert plugin._to_ns(raw) == ns


# ---------------------------------------------------------------------------
# Settings sources
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sources, winner",
    [
        (("env",), 0.1),
        (("env", "args"), 0.2),
        (("env", "args", "toml"), 0.3),
        (("env", "args", "toml", "body"), 0.4),
        (("env", "args", "toml", "body", "header"), 0.5),
        (("env", "args", "toml", "body", "header", "query"), 0.6),
    ],
)
def test_each_source_overrides_the_ones_before_it(sources, winner, tmp_path, monkeypatch):
    if "env" in sources:
        monkeypatch.setenv("INFLUXDB3_PROPHET_FORECASTING_LEVELS", "0.1")
    args = {"levels": "0.2"} if "args" in sources else {}
    if "toml" in sources:
        args["config_file_path"] = write_toml(tmp_path, "levels = [0.3]\n")
    body = dict(REQUEST)
    if "body" in sources:
        body["levels"] = [0.4]
    headers = {"X-Influxdb3-Prophet-Forecasting-levels": "0.5"} if "header" in sources else {}
    query = {"levels": "0.6"} if "query" in sources else {}

    response = plugin.process_request(FakeLocal(history()), query, headers, json.dumps(body), args)

    assert answered(response)["levels"] == [winner]


def test_any_key_can_come_from_a_header_or_the_query_string():
    headers = {"x-influxdb3-prophet-forecasting-horizon-steps": "2"}
    query = {"source_measurement": "cpu", "field": "usage", "window": "30d", "levels": "0.5"}

    body = answered(plugin.process_request(FakeLocal(history()), query, headers, None, None))

    assert len(body["results"][0]["forecast"]) == 2
    assert body["levels"] == [0.5]


def test_config_file_path_comes_from_the_environment(tmp_path, monkeypatch):
    path = write_toml(tmp_path, "levels = [0.3]\n")
    monkeypatch.setenv("INFLUXDB3_PROPHET_FORECASTING_CONFIG_FILE_PATH", path)

    assert answered(forecast(FakeLocal(history())))["levels"] == [0.3]


TRIGGER_DEFAULTS = {"tag_filter": "region=eu", "group_by": "host", "levels": "0.9"}


def test_empty_list_or_object_replaces_a_lower_source():
    body = json.dumps({**REQUEST, "tag_filter": {}, "group_by": [], "levels": []})
    local = FakeLocal(history(host="a", region="eu"))

    response = plugin.process_request(local, {}, {}, body, TRIGGER_DEFAULTS)

    assert answered(response)["levels"] == []
    sql = local.history_query().sql
    assert "region" not in sql and "__g0" not in sql


@pytest.mark.parametrize("blank", ["", None])
def test_empty_text_or_null_keeps_a_lower_source(blank):
    body = json.dumps({**REQUEST, "tag_filter": blank, "group_by": blank, "levels": blank})
    local = FakeLocal(history(host="a", region="eu"))

    response = plugin.process_request(local, {}, {}, body, TRIGGER_DEFAULTS)

    assert answered(response)["levels"] == [0.9]
    sql = local.history_query().sql
    assert "region" in sql and "__g0" in sql


def test_request_query_replaces_the_triggers_measurement_keys():
    args = {
        "source_measurement": "cpu",
        "field": "usage",
        "group_by": "host",
        "horizon_steps": "2",
        "window": "30d",
    }
    local = FakeLocal(query_rows=history())

    response = plugin.process_request(local, {}, {}, json.dumps({"query": QUERY}), args)

    assert answered(response)["results"][0]["field"] == "usage"
    assert local.sent(QUERY)


def test_empty_list_or_object_is_not_a_side_of_the_choice():
    args = {"query": QUERY, "horizon_steps": "2"}
    body = json.dumps({"group_by": [], "tag_filter": {}})
    local = FakeLocal(query_rows=history())

    answered(plugin.process_request(local, {}, {}, body, args))

    assert local.sent(QUERY)


ABSOLUTE = {"start_time": "2027-01-01T00:00:00Z", "end_time": "2027-01-10T00:00:00Z"}


@pytest.mark.parametrize(
    "lower, upper",
    [
        ({"source_measurement": "cpu", "field": "usage", "group_by": ["host"]}, {"query": QUERY}),
        ({"query": QUERY}, {"source_measurement": "cpu", "field": "usage"}),
        ({"horizon_steps": 9}, {"horizon_duration": "2h"}),
        ({"horizon_duration": "2h"}, {"horizon_steps": 9}),
        ({"window": "1d", "point_limit": 5}, ABSOLUTE),
        (ABSOLUTE, {"window": "6h"}),
        ({"sampling_interval": "1h"}, {"forecast_freq": "D"}),
        ({"forecast_freq": "MS"}, {"sampling_interval": "1h"}),
    ],
)
def test_the_highest_source_makes_a_choice_between_keys(lower, upper):
    assert plugin._drop_overridden([lower, upper]) == [{}, upper]


@pytest.mark.parametrize(
    "keys, fragment",
    [
        ({"query": QUERY}, "either query"),
        ({"source_measurement": None}, "required"),
        ({"horizon_steps": None}, "exactly one"),
        ({"horizon_duration": "1h"}, "exactly one"),
        ({"window": None}, "set the history to read"),
        ({"forecast_freq": "h", "sampling_interval": "1h"}, "sampling_interval and forecast_freq"),
        ({"model_mode": "predict"}, "requires unique_suffix"),
        ({"holiday_names": ["a"]}, "holiday_date_list and holiday_names"),
        ({"series_limit": 101}, "series_limit is 101, more than max_series=100"),
        ({"horizon_steps": 4, "max_forecast_points": 3}, "max_forecast_points=3"),
        ({"group_by": ["model"], "write_results": True}, "clashes"),
        ({"group_by": ["lower_80"], "write_results": True}, "clashes"),
        (ABSOLUTE, "not both"),
        ({"window": None, "start_time": ABSOLUTE["start_time"]}, "together"),
        (
            {"window": None, "start_time": ABSOLUTE["end_time"], "end_time": ABSOLUTE["start_time"]},
            "earlier than",
        ),
        ({"window": "1d", "validation_window": "1d"}, "no history to train on"),
    ],
)
def test_requests_refused_before_any_query_or_training(keys, fragment, fits):
    local = FakeLocal(history())

    assert fragment in refused(forecast(local, **keys))

    assert local.queries == []
    assert fits == []


@pytest.mark.parametrize(
    "keys",
    [
        {"source_measurement": 5},
        {"point_limit": 2},
        {"series_limit": 0},
        {"horizon_steps": 0},
        {"max_series": 0},
        {"backtest_points": 0},
        {"window": "1ns"},
        {"validation_window": "soon"},
        {"levels": [1.5]},
        {"write_results": "maybe"},
        {"group_by": 5},
        {"tag_filter": "host"},
        {"model": "other"},
        {"model_mode": "serve"},
        {"seasonality_mode": "linear"},
        {"validation_alignment": "best"},
        {"changepoints": ["not a date"]},
        {"forecast_freq": "nope"},
        {"unique_suffix": "../up"},
        {"unique_suffix": "a" * 65},
        {"msre_threshold": -1},
        {"changepoint_prior_scale": 0},
        {"start_time": "yesterday"},
        {"start_time": "2025-06-19T00:00:00"},
    ],
)
def test_invalid_values_are_bad_requests(keys):
    assert refused(forecast(FakeLocal(history()), **keys))


@pytest.mark.parametrize(
    "keys",
    [
        {"group_by": ["host"]},
        {"tag_filter": {"host": "a"}},
        {"sql_filter": "usage > 0"},
        {"start_time": ABSOLUTE["start_time"]},
        {"end_time": ABSOLUTE["end_time"]},
    ],
)
def test_measurement_keys_do_not_apply_to_a_query(keys):
    assert "does not apply" in refused(forecast_query(FakeLocal(), **keys))


@pytest.mark.parametrize(
    "query, headers, extra, args",
    [
        ({"nope": "1"}, {}, {}, None),
        ({}, {"x-INFLUXDB3-prophet-forecasting-nope": "1"}, {}, None),
        ({}, {}, {"nope": 1}, None),
        ({}, {}, {}, {"nope": "1"}),
        ({}, {}, {"config_file_path": "a.toml"}, None),
        ({}, {}, {"senders": ["slack"]}, None),
    ],
    ids=["query string", "header", "body", "trigger args", "config_file_path in the body", "scheduled-only"],
)
def test_unknown_keys_are_refused_in_every_source(query, headers, extra, args):
    body = json.dumps({**REQUEST, **extra})

    assert refused(plugin.process_request(FakeLocal(history()), query, headers, body, args))


def test_unknown_key_in_toml_is_refused(tmp_path):
    args = {"config_file_path": write_toml(tmp_path, "nope = 1\n")}

    assert refused(plugin.process_request(FakeLocal(), {}, {}, json.dumps(REQUEST), args))


def test_other_headers_are_ignored():
    headers = {"Host": "x", "User-Agent": "curl"}

    answered(plugin.process_request(FakeLocal(history()), {}, headers, json.dumps(REQUEST), None))


@pytest.mark.parametrize("body", [b"{not json", b"[1, 2]", b"\xff\xfe"])
def test_unreadable_body_is_a_bad_request(body):
    assert refused(plugin.process_request(FakeLocal(), {}, {}, body, None))


# ---------------------------------------------------------------------------
# The response
# ---------------------------------------------------------------------------


def test_response_of_one_series():
    body = answered(forecast(FakeLocal(history())))

    assert set(body) == {"status", "model", "levels", "series_total", "results"}
    assert body["model"] == "prophet"
    assert body["levels"] == [0.5, 0.8]
    assert body["series_total"] == 1
    [result] = body["results"]
    assert set(result) == {"field", "tags", "points_read", "step_ms", "forecast"}
    assert result["field"] == "usage"
    assert result["tags"] == {}
    assert result["points_read"] == 40
    assert result["step_ms"] == HOUR // 10**6
    # The forecast starts one step after the last point, in milliseconds.
    assert times_of(result) == [NOW, NOW + HOUR, NOW + 2 * HOUR]
    for point in result["forecast"]:
        assert set(point) == {"timestamp", "forecast", "lower_50", "upper_50", "lower_80", "upper_80"}


def test_the_forecast_follows_the_data():
    result = result_of(forecast(FakeLocal(history(value=lambda i: 2.0 * i))))

    # A straight line at +2 per step, whose next value is 80.
    assert [point["forecast"] for point in result["forecast"]] == pytest.approx([80, 82, 84], abs=1)


def test_intervals_widen_with_the_level_and_hold_the_forecast():
    def noisy(i):
        return i + math.sin(i * 7.3) * 3

    levels = [0.5, 0.8, 0.9, 0.95]
    point = result_of(forecast(FakeLocal(history(value=noisy)), levels=levels))["forecast"][0]

    assert point["lower_95"] < point["lower_90"] < point["lower_80"] < point["lower_50"]
    assert point["lower_50"] <= point["forecast"] <= point["upper_50"]
    assert point["upper_50"] < point["upper_80"] < point["upper_90"] < point["upper_95"]


def test_a_narrow_interval_still_holds_the_forecast(monkeypatch):
    # Samples all above the central value, as a skewed trend may leave them.
    monkeypatch.setattr(
        plugin.Prophet,
        "predictive_samples",
        lambda self, df: {"yhat": np.full((len(df), 10), 1000.0)},
    )

    for point in result_of(forecast(FakeLocal(history()), levels=[0.5]))["forecast"]:
        assert point["lower_50"] == pytest.approx(point["forecast"])
        assert point["upper_50"] == 1000.0


def test_a_level_is_the_central_share_of_the_samples(monkeypatch):
    monkeypatch.setattr(
        plugin.Prophet,
        "predictive_samples",
        lambda self, df: {"yhat": np.tile(np.linspace(0.0, 100.0, 101), (len(df), 1))},
    )

    point = result_of(forecast(FakeLocal(history(value=lambda i: 50.0)), levels=[0.5, 0.8]))[
        "forecast"
    ][0]

    assert point["lower_50"] == pytest.approx(25.0)
    assert point["upper_50"] == pytest.approx(75.0)
    assert point["lower_80"] == pytest.approx(10.0)
    assert point["upper_80"] == pytest.approx(90.0)


def test_empty_levels_give_only_the_forecast_without_sampling(monkeypatch):
    def refuse(self, df):
        raise AssertionError("no level needs samples")

    monkeypatch.setattr(plugin.Prophet, "predictive_samples", refuse)

    point = result_of(forecast(FakeLocal(history()), levels=[]))["forecast"][0]

    assert set(point) == {"timestamp", "forecast"}


def test_non_finite_values_are_null_and_not_written(monkeypatch):
    def predict(model, times, levels):
        nan = np.full(len(times), math.nan)
        return nan, {level: (nan, nan) for level in levels}

    monkeypatch.setattr(plugin, "_predict", predict)
    local = FakeLocal(history())

    point = result_of(forecast(local, write_results=True))["forecast"][0]

    assert point["forecast"] is None and point["lower_50"] is None
    assert set(local.written[0].fields) == {"step", "points_read", "run_time"}


# ---------------------------------------------------------------------------
# History selection
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "keys, seconds",
    [
        ({"window": "6h"}, 6 * 3600),
        ({"window": "6h", "point_limit": 10}, 6 * 3600),
        ({"window": None, "point_limit": 10}, 30 * 86400),
        ({"window": None, "point_limit": 10, "sampling_interval": "1h"}, 11 * 3600),
        ({"window": None, "point_limit": 1000, "sampling_interval": "1d"}, 365 * 86400),
        ({"window": None, "point_limit": 10, "validation_window": "1d"}, 31 * 86400),
        (
            {"window": None, "point_limit": 10, "sampling_interval": "1h",
             "validation_window": "1h"},
            12 * 3600,
        ),
    ],
)
def test_every_measurement_read_is_bounded_by_time(keys, seconds):
    local = FakeLocal(history())

    forecast(local, **keys)

    for query in (local.history_query(), local.sent("count(*) AS __rows")[0]):
        assert "time >= $history_start AND time < $history_end" in query.sql
        assert query.params["history_start"] == plugin._iso(NOW - seconds * SECOND)
    # NOW sits on the edge of an hour, not of a day.
    day_bucket = keys.get("sampling_interval") == "1d"
    assert query.params["history_end"] == plugin._iso(NOW - NOW % DAY if day_bucket else NOW)


@pytest.mark.parametrize(
    "keys, fragment",
    [
        ({"point_limit": 30}, "LIMIT 30"),
        ({"point_limit": 30, "sampling_interval": "1h"}, "LIMIT 30"),
        ({"point_limit": 30, "group_by": ["host"]}, "__rn <= 30"),
    ],
)
def test_history_read_is_limited_per_series(keys, fragment):
    local = FakeLocal(history(host="a"))

    answered(forecast(local, **keys))

    assert fragment in local.history_query().sql


def test_window_alone_reads_every_point_oldest_first():
    local = FakeLocal(history(40))

    assert result_of(forecast(local))["points_read"] == 40

    sql = local.history_query().sql
    assert "LIMIT" not in sql and "__rn" not in sql
    assert sql.endswith("ORDER BY __t")


def test_the_last_points_reach_the_model(fits):
    answered(forecast(FakeLocal(history(30)), point_limit=10))

    assert fits[0]["y"].tolist() == [float(i) for i in range(20, 30)]


def test_absolute_history_bounds_the_reads_by_parameters():
    start, end = NOW - 10 * DAY, NOW - 5 * DAY
    rows = history(24 * 20, end=NOW - HOUR)
    local = FakeLocal(rows)

    result = result_of(
        forecast(local, window=None, start_time=plugin._iso(start), end_time=plugin._iso(end))
    )

    query = local.history_query()
    assert "time >= $history_start AND time < $history_end" in query.sql
    assert "INTERVAL" not in query.sql
    assert query.params["history_start"] == plugin._iso(start)
    assert result["points_read"] == 5 * 24
    # The forecast continues from the last point before end_time.
    assert times_of(result)[0] == end


# ---------------------------------------------------------------------------
# Where history ends
# ---------------------------------------------------------------------------


NOW_0840 = NOW + 40 * MINUTE


@pytest.fixture
def at_0840(monkeypatch):
    """A clock inside a bucket, as NOW sits on the edge of an hour."""
    monkeypatch.setattr(plugin._time, "time_ns", lambda: NOW_0840)


def rows_with_a_fast_clock() -> list:
    """Rows every 5 minutes up to 08:35, and one at 09:10 from a device whose clock runs fast."""
    rows = history(12 * 24, step=5 * MINUTE, end=NOW + 35 * MINUTE, value=lambda i: 20.0)
    return [*rows, {"time": NOW + 70 * MINUTE, "usage": 500.0}]


def trained_until(df) -> int:
    return int(df["ds"].dt.as_unit("ns").iloc[-1].value)


def test_a_row_from_the_future_is_not_history(at_0840, fits):
    local = FakeLocal(rows_with_a_fast_clock())

    result = result_of(forecast(local))

    assert local.history_query().params["history_end"] == plugin._iso(NOW_0840)
    assert trained_until(fits[0]) == NOW + 35 * MINUTE
    assert fits[0]["y"].max() == 20.0
    assert times_of(result)[0] == NOW + 40 * MINUTE


def test_history_with_sampling_ends_at_the_last_full_bucket(at_0840, fits):
    local = FakeLocal(rows_with_a_fast_clock())

    result = result_of(forecast(local, sampling_interval="1h"))

    assert local.history_query().params["history_end"] == plugin._iso(NOW)
    assert trained_until(fits[0]) == NOW - HOUR
    assert times_of(result)[0] == NOW


@pytest.mark.parametrize("end_time", ["2027-01-15T09:00:10Z", "2030-01-01T00:00:00Z"])
def test_end_time_after_now_still_ends_at_the_last_full_bucket(end_time, at_0840, fits):
    local = FakeLocal(rows_with_a_fast_clock())

    result = result_of(
        forecast(
            local,
            window=None,
            start_time="2027-01-01T00:00:00Z",
            end_time=end_time,
            sampling_interval="1h",
        )
    )

    assert local.history_query().params["history_end"] == plugin._iso(NOW)
    assert trained_until(fits[0]) == NOW - HOUR
    assert times_of(result)[0] == NOW


def test_end_time_after_now_reads_later_rows_without_sampling(at_0840, fits):
    end_time = "2030-01-01T00:00:00Z"
    local = FakeLocal(rows_with_a_fast_clock())

    answered(
        forecast(local, window=None, start_time="2027-01-01T00:00:00Z", end_time=end_time)
    )

    assert local.history_query().params["history_end"] == plugin._iso(plugin._moment_ns(end_time))
    assert trained_until(fits[0]) == NOW + 70 * MINUTE
    assert fits[0]["y"].max() == 500.0


def test_mixed_tag_sets_in_the_future_do_not_count():
    rows = history(10, host="a") + history(5, end=NOW + 10 * HOUR, host="b")
    rows += history(5, end=NOW + 10 * HOUR, host="a")

    assert result_of(forecast(FakeLocal(rows)))["points_read"] == 10


@pytest.mark.parametrize("keys", [{}, {"window": "1d"}], ids=["no window", "window"])
def test_a_query_keeps_its_rows_from_the_future(keys, at_0840, fits):
    result = result_of(forecast_query(FakeLocal(query_rows=rows_with_a_fast_clock()), **keys))

    assert trained_until(fits[0]) == NOW + 70 * MINUTE
    assert times_of(result)[0] > NOW + 70 * MINUTE


def test_a_query_with_sampling_ends_at_the_last_full_bucket(at_0840, fits):
    rows = rows_with_a_fast_clock()

    result = result_of(forecast_query(FakeLocal(query_rows=rows), sampling_interval="1h"))

    assert trained_until(fits[0]) == NOW - HOUR
    assert fits[0]["y"].max() == 20.0
    assert times_of(result)[0] == NOW


@pytest.mark.parametrize(
    "keys, fragment",
    [
        ({"window": "30min", "sampling_interval": "1h"}, "shorter than one sampling_interval"),
        (
            {"window": None, "point_limit": 5, "sampling_interval": "2y"},
            "shorter than one sampling_interval",
        ),
        (
            {
                "window": None,
                "start_time": "2027-01-15T09:00:00Z",
                "end_time": "2030-01-01T00:00:00Z",
                "sampling_interval": "1h",
            },
            "no full sampling_interval",
        ),
    ],
)
def test_a_range_the_server_would_refuse_is_refused_first(keys, fragment, at_0840):
    local = FakeLocal(history())

    assert fragment in refused(forecast(local, **keys))

    assert local.queries == []


# ---------------------------------------------------------------------------
# Series
# ---------------------------------------------------------------------------


def test_no_history_without_group_by_answers_its_one_series(fits):
    body = answered(forecast(FakeLocal()))

    assert body["series_total"] == 1
    assert body["results"] == [
        {
            "field": "usage",
            "tags": {},
            "status": "error",
            "code": "too_few_points",
            "message": "need >= 3 points, got 0 in the last 30d",
        }
    ]
    assert fits == []


def test_no_history_with_group_by_has_no_series(fits):
    local = FakeLocal()

    body = answered(forecast(local, group_by=["host"]))

    assert body["series_total"] == 0
    assert body["results"] == []
    assert fits == []
    assert not local.sent(" AS __t,")


@pytest.mark.parametrize(
    "points, keys, message",
    [
        (2, {}, "need >= 3 points, got 2 in the last 30d"),
        (
            2,
            {"window": None, "point_limit": 10},
            "need >= 3 points, got 2 in the last 30d; set window or sampling_interval",
        ),
        (
            2,
            {"window": None, "point_limit": 10, "sampling_interval": "1h"},
            "need >= 3 points, got 2 in the last 11h; "
            "set window, raise point_limit or shorten sampling_interval",
        ),
        (
            2,
            {"window": None, "start_time": "1970-01-01T00:00:01Z", "end_time": "1970-01-01T00:00:02Z"},
            "need >= 3 points, got 0 from 1970-01-01T00:00:01+00:00 to 1970-01-01T00:00:02+00:00",
        ),
        (
            4,
            {"validation_window": "3h"},
            "need >= 3 points, got 1 in the last 30d, before the validation window",
        ),
    ],
)
def test_too_few_points_says_what_was_read(points, keys, message, fits):
    result = result_of(forecast(FakeLocal(history(points)), **keys))

    assert result["code"] == "too_few_points"
    assert result["message"] == message
    assert fits == []


def test_three_points_are_enough():
    assert "forecast" in result_of(forecast(FakeLocal(history(3))))


def test_a_short_series_does_not_blank_the_others(fits):
    rows = history(host="a") + history(2, host="b")

    good, short = answered(forecast(FakeLocal(rows), group_by=["host"]))["results"]

    assert "forecast" in good
    assert (short["status"], short["code"]) == ("error", "too_few_points")
    assert set(short) == {"field", "tags", "status", "code", "message"}
    assert len(fits) == 1


def test_each_series_is_trained_on_its_own(fits):
    rows = history(value=lambda i: 1.0, host="a") + history(value=lambda i: 50.0, host="b")

    body = answered(forecast(FakeLocal(rows), group_by=["host"]))

    assert len(fits) == 2
    assert body["results"][0]["forecast"][0]["forecast"] == pytest.approx(1.0, abs=0.5)
    assert body["results"][1]["forecast"][0]["forecast"] == pytest.approx(50.0, abs=0.5)


def test_results_are_ordered_with_a_missing_value_last():
    rows = (
        history(10, host="b", region="eu")
        + history(10, region="eu")
        + history(10, host="a", region="us")
        + history(10, host="a", region="eu")
    )

    body = answered(forecast(FakeLocal(rows), group_by=["host", "region"]))

    assert [r["tags"] for r in body["results"]] == [
        {"host": "a", "region": "eu"},
        {"host": "a", "region": "us"},
        {"host": "b", "region": "eu"},
        {"region": "eu"},
    ]


def test_series_tags_are_those_with_one_value_on_every_row():
    half = NOW - 11 * HOUR
    rows = (
        history(10, host="a", region="eu")
        # region changes value, one after the other in time
        + history(10, end=half, host="b", region="eu")
        + history(10, host="b", region="us")
        # region is missing from some rows
        + history(10, end=half, host="c")
        + history(10, host="c", region="eu")
    )

    body = answered(forecast(FakeLocal(rows), group_by=["host"]))

    assert [r["tags"] for r in body["results"]] == [
        {"host": "a", "region": "eu"},
        {"host": "b"},
        {"host": "c"},
    ]


def test_series_limit_keeps_the_first_series_and_counts_them_all(fits):
    rows = history(10, host="c") + history(10, host="a") + history(10, host="b")
    local = FakeLocal(rows)

    body = answered(forecast(local, group_by=["host"], series_limit=2))

    assert [r["tags"]["host"] for r in body["results"]] == ["a", "b"]
    assert body["series_total"] == 3
    assert len(fits) == 2
    assert any("series_limit=2 keeps 2 of 3" in text for text in local.messages("info"))


def test_series_limit_narrows_the_following_reads():
    rows = history(10, host="c") + history(10, host="a") + history(10, host="b")
    local = FakeLocal(rows)

    answered(forecast(local, group_by=["host"], series_limit=2))

    series = local.sent("count(*) AS __rows")[0].sql
    assert "LIMIT 2" in series and "count(*) OVER () AS __total" in series
    for query in (local.sent("AS __first")[0], local.history_query()):
        assert '"host" = $sl0_0' in query.sql and '"host" = $sl1_0' in query.sql
        assert (query.params["sl0_0"], query.params["sl1_0"]) == ("a", "b")


def test_chosen_series_matches_a_missing_value():
    req = SimpleNamespace(group_by=["host", "region"])
    params: dict = {}

    condition = plugin._chosen_series(req, [(("a", None), {}, 1)], params)

    assert condition == '(("host" = $sl0_0 AND "region" IS NULL))'
    assert params == {"sl0_0": "a"}


def test_more_series_than_the_cap_are_refused_before_training(fits):
    rows = history(10, host="a") + history(10, host="b") + history(10, host="c")
    local = FakeLocal(rows)

    assert "max_series=2" in refused(forecast(local, group_by=["host"], max_series=2))

    assert fits == []
    assert not local.sent(" AS __t,")


def test_series_limit_never_hits_the_cap():
    rows = history(10, host="a") + history(10, host="b") + history(10, host="c")

    body = answered(forecast(FakeLocal(rows), group_by=["host"], max_series=2, series_limit=2))

    assert len(body["results"]) == 2
    assert body["series_total"] == 3


@pytest.mark.parametrize(
    "keys, fragment",
    [
        ({"source_measurement": "absent"}, "cannot read measurement"),
        ({"field": "absent"}, "not a numeric field"),
        ({"field": "status"}, "not a numeric field"),
        ({"group_by": ["usage"]}, "not a tag"),
        ({"tag_filter": {"usage": "1"}}, "not a tag"),
    ],
)
def test_columns_are_checked_against_the_schema(keys, fragment, fits):
    assert fragment in refused(forecast(FakeLocal(history()), **keys))

    assert fits == []


@pytest.mark.parametrize(
    "sql, keys",
    [
        ("information_schema", {"source_database": "absent"}),
        ("count(*) AS __rows", {"sql_filter": "x > 0"}),
        ("AS __first", {}),
    ],
    ids=["schema", "series", "tag sets"],
)
def test_a_query_failing_before_the_history_read_is_a_bad_request(sql, keys, fits):
    local = FakeLocal(history(), fail={sql: RuntimeError("boom")})

    assert refused(forecast(local, **keys))
    assert fits == []


def test_tag_filter_is_bound_as_parameters():
    local = FakeLocal(history())

    answered(forecast(local, tag_filter={"host": "a' OR 1=1", "region": ["eu", "us"]}))

    query = local.history_query()
    assert '"host" = $tf0_0' in query.sql
    assert '"region" IN ($tf1_0, $tf1_1)' in query.sql
    assert (query.params["tf0_0"], query.params["tf1_0"], query.params["tf1_1"]) == (
        "a' OR 1=1",
        "eu",
        "us",
    )
    assert "OR 1=1" not in query.sql


def test_sql_filter_is_anded_as_is():
    local = FakeLocal(history())

    answered(forecast(local, tag_filter={"host": "a"}, sql_filter="usage > 0 OR temp > 1"))

    assert '"host" = $tf0_0 AND (usage > 0 OR temp > 1)' in local.history_query().sql


@pytest.mark.parametrize(
    "keys",
    [
        {"validation_window": "1h"},
        {"source_measurement": None, "field": None, "query": QUERY, "window": None},
    ],
    ids=["measurement", "query"],
)
def test_every_read_goes_to_the_source_database(keys):
    local = FakeLocal(history(), query_rows=history())

    forecast(local, source_database="other", **keys)

    assert local.queries
    assert {query.database for query in local.queries} == {"other"}


def test_failed_history_query_is_internal():
    local = FakeLocal(history(), fail={" AS __t,": RuntimeError("boom")})

    assert "history query failed: boom" in failed(forecast(local), "internal", 500)


# ---------------------------------------------------------------------------
# Mixed series
# ---------------------------------------------------------------------------


def test_tag_sets_writing_at_the_same_time_are_refused(fits):
    local = FakeLocal(history(10, host="a") + history(10, host="b"))

    assert "differ in host" in refused(forecast(local))
    assert fits == []


def test_a_tag_set_following_another_is_one_series():
    rows = history(10, end=NOW - 11 * HOUR, host="a") + history(10, host="b")

    result = result_of(forecast(FakeLocal(rows)))

    assert result["points_read"] == 20
    assert result["tags"] == {}


def test_sampling_interval_averages_tag_sets_instead(fits):
    rows = history(10, value=lambda i: 1.0, host="a") + history(10, value=lambda i: 3.0, host="b")
    local = FakeLocal(rows)

    answered(forecast(local, sampling_interval="1h"))

    assert not local.sent("AS __first")
    assert fits[0]["y"].tolist()[-1] == pytest.approx(2.0)


def test_overlap_older_than_the_history_read_does_not_count():
    old = NOW - 60 * HOUR
    rows = history(10, host="a") + history(5, end=old, host="a") + history(5, end=old, host="b")

    answered(forecast(FakeLocal(rows), point_limit=10))
    assert refused(forecast(FakeLocal(rows)))


def two_hosts(mixed) -> list:
    """Minute rows of host a at 10.0, and of host b at 100.0 between them where mixed(moment)."""
    rows = []
    for i in range(500):
        moment = NOW - MINUTE - i * MINUTE
        rows.append({"time": moment, "usage": 10.0, "host": "a"})
        if mixed(moment):
            rows.append({"time": moment + 30 * SECOND, "usage": 100.0, "host": "b"})
    return rows


CUT = NOW - 3 * HOUR
TRAINING_MIXED = {"validation_window": "3h", "msre_threshold": 1000}


@pytest.mark.parametrize("keys", [{"point_limit": 100}, {}], ids=["point_limit", "window alone"])
def test_mixed_tag_sets_in_the_training_part_are_refused(keys, fits):
    rows = two_hosts(lambda moment: moment < CUT)

    assert "differ in host" in refused(forecast(FakeLocal(rows), **TRAINING_MIXED, **keys))

    assert fits == []


@pytest.mark.parametrize("keys", [{"point_limit": 100}, {}], ids=["point_limit", "window alone"])
def test_mixed_tag_sets_only_in_the_validation_window_are_not_refused(keys, fits):
    rows = two_hosts(lambda moment: moment >= CUT)

    result = result_of(forecast(FakeLocal(rows), **TRAINING_MIXED, **keys))

    assert "forecast" in result
    assert set(fits[0]["y"]) == {10.0}


# ---------------------------------------------------------------------------
# The forecast grid
# ---------------------------------------------------------------------------


def month_starts(*months) -> list:
    return [plugin._to_ns(datetime(2026, month, 1)) for month in months]


def test_calendar_frequency_is_inferred():
    rows = [{"time": moment, "usage": float(i)} for i, moment in enumerate(month_starts(3, 4, 5, 6, 7))]

    result = result_of(forecast(FakeLocal(rows), window="1y", horizon_steps=2))

    assert times_of(result) == month_starts(8, 9)
    assert result["step_ms"] == 31 * DAY // 10**6


def test_forecast_freq_sets_a_calendar_grid_over_raw_points(fits):
    result = result_of(
        forecast(
            FakeLocal(history(40, step=DAY, end=plugin._to_ns(datetime(2026, 7, 10)))),
            window="1y",
            forecast_freq="MS",
            horizon_steps=2,
        )
    )

    assert times_of(result) == month_starts(8, 9)
    assert len(fits[0]) == 40


def irregular(count=40) -> list:
    # Mostly hourly, with a stray hole every few points, so pandas infers nothing.
    moments = [NOW - HOUR - i * HOUR for i in range(count) if i % 7 != 3]
    return [{"time": moment, "usage": 1.0} for moment in reversed(moments)]


def test_irregular_data_falls_back_to_the_median_spacing():
    result = result_of(forecast(FakeLocal(irregular())))

    assert result["step_ms"] == HOUR // 10**6
    assert times_of(result) == [NOW, NOW + HOUR, NOW + 2 * HOUR]


def test_jitter_does_not_move_the_grid_between_runs():
    grid = [NOW - HOUR - i * HOUR for i in reversed(range(10))]
    runs = (
        [30, -20, 10, -40, 20, 0, -10, 40, -30, 15],
        [-40, 20, -5, 30, -25, 35, 5, -15, 25, -35],
    )

    for jitter_ms in runs:
        rows = [
            {"time": moment + jitter * 10**6, "usage": 1.0}
            for moment, jitter in zip(grid, jitter_ms)
        ]
        assert times_of(result_of(forecast(FakeLocal(rows)))) == [NOW, NOW + HOUR, NOW + 2 * HOUR]


def test_sampling_trains_on_bucket_means_without_filling_gaps(fits):
    rows = history(40, step=30 * MINUTE)
    rows = [row for row in rows if not NOW - 8 * HOUR <= row["time"] < NOW - 6 * HOUR]

    result = result_of(forecast(FakeLocal(rows), sampling_interval="1h"))

    assert result["step_ms"] == HOUR // 10**6
    assert result["points_read"] == len(fits[0]) == 19
    # The first bucket holds point 0 alone, the next the points 1 and 2.
    assert fits[0]["y"].iloc[0] == 0.0
    assert fits[0]["y"].iloc[1] == 1.5
    assert not fits[0]["y"].isna().any()
    assert times_of(result)[0] == NOW


def test_buckets_below_a_second():
    local = FakeLocal(history(40, step=100 * 10**6, end=NOW - SECOND))

    result = result_of(
        forecast(local, sampling_interval="500ms", horizon_steps=None, horizon_duration="1s")
    )

    assert "date_bin(INTERVAL '500000000 nanoseconds', time)" in local.history_query().sql
    assert result["step_ms"] == 500
    assert times_of(result) == [NOW - 500 * 10**6, NOW]


# ---------------------------------------------------------------------------
# The horizon
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "duration, points",
    [("2h", 2), ("90min", 2), ("10min", 1), ("1h", 1), ("61min", 2)],
)
def test_horizon_duration_rounds_up_to_whole_steps(duration, points):
    result = result_of(forecast(FakeLocal(history()), horizon_steps=None, horizon_duration=duration))

    assert len(result["forecast"]) == points


@pytest.mark.parametrize(
    "duration, points", [("45d", 2), ("1d", 1), ("30d", 1), ("31d", 2)]
)
def test_calendar_horizon_rounds_up_to_whole_steps(duration, points):
    rows = [{"time": moment, "usage": float(i)} for i, moment in enumerate(month_starts(3, 4, 5, 6))]

    result = result_of(
        forecast(FakeLocal(rows), window="1y", horizon_steps=None, horizon_duration=duration)
    )

    assert len(result["forecast"]) == points


def test_the_horizon_counts_from_the_last_point_not_from_now():
    local = FakeLocal(history(end=NOW - 5 * HOUR))

    result = result_of(forecast(local, horizon_steps=None, horizon_duration="2h"))

    assert times_of(result) == [NOW - 4 * HOUR, NOW - 3 * HOUR]


def test_a_horizon_over_the_cap_at_the_datas_spacing_is_refused_before_training(fits):
    response = forecast(
        FakeLocal(history()), horizon_steps=None, horizon_duration="3d", max_forecast_points=30
    )

    assert "the horizon is 72 points" in refused(response)
    assert fits == []


def test_a_horizon_past_the_timestamp_range_is_refused():
    response = forecast(FakeLocal(history()), forecast_freq="YS", horizon_steps=500)

    assert "timestamps InfluxDB can hold" in refused(response)


@pytest.mark.parametrize(
    "last, keys, warns",
    [
        (NOW - 10 * DAY, {}, True),
        (NOW - HOUR, {}, False),
        (
            NOW - 10 * DAY,
            {"window": None, "start_time": plugin._iso(NOW - 12 * DAY), "end_time": plugin._iso(NOW - 9 * DAY)},
            False,
        ),
    ],
    ids=["old data", "fresh data", "asked for the past"],
)
def test_a_forecast_wholly_in_the_past_warns_unless_asked_for(last, keys, warns):
    local = FakeLocal(history(end=last))

    answered(forecast(local, **keys))

    warned = any("whole forecast lies in the past" in text for text in local.messages("warn"))
    assert warned is warns


# ---------------------------------------------------------------------------
# The model
# ---------------------------------------------------------------------------


def test_settings_reach_the_model():
    rows = history(60, step=DAY)
    series = plugin.Series(
        field="usage",
        tags={},
        times=[row["time"] for row in rows],
        values=[row["usage"] for row in rows],
    )
    changepoint = pd.Timestamp(series.times[30], unit="ns").strftime("%Y-%m-%d")
    req = SimpleNamespace(
        seasonality_mode="multiplicative",
        changepoint_prior_scale=0.5,
        changepoints=[changepoint],
        holiday_dates=["2026-12-25", "2026-12-26"],
        holiday_names=["Xmas", "Xmas"],
        holiday_country_names=["US"],
    )

    model = plugin._train(req, series)

    assert model.seasonality_mode == "multiplicative"
    assert model.changepoint_prior_scale == 0.5
    assert model.changepoints.dt.strftime("%Y-%m-%d").tolist() == [changepoint]
    assert model.holidays["holiday"].tolist() == ["Xmas", "Xmas"]
    assert model.holidays["ds"].dt.strftime("%Y-%m-%d").tolist() == ["2026-12-25", "2026-12-26"]
    assert model.country_holidays == "US"
    assert len(model.history) == 60


def test_built_in_holidays_use_the_first_country_with_a_warning():
    local = FakeLocal(history())

    answered(forecast(local, holiday_country_names=["US", "DE"]))

    assert sum("one country, using 'US'" in text for text in local.messages("warn")) == 1


def test_dates_in_any_iso_form_reach_the_model():
    result = result_of(
        forecast(
            FakeLocal(history(60, step=DAY)),
            window="1y",
            changepoints=["2026-12-01", "2026-12-15T12:00:00Z"],
            holiday_date_list=["2026-12-25T00:00:00Z", "2026-12-26"],
            holiday_names=["Xmas", "Xmas"],
        )
    )

    assert len(result["forecast"]) == 3


@pytest.mark.parametrize(
    "keys, fragment",
    [
        ({"holiday_date_list": ["2026-12-25"], "holiday_names": ["trend"]}, "'trend' is reserved"),
        ({"holiday_country_names": ["XX"]}, "Holidays in XX"),
    ],
)
def test_holidays_prophet_refuses_are_bad_requests_before_any_read(keys, fragment, fits):
    local = FakeLocal(history())

    message = refused(forecast(local, **keys))

    assert "invalid holidays" in message and fragment in message
    assert local.queries == []
    assert fits == []


def test_train_mode_saves_nothing(models_dir, fits):
    answered(forecast(FakeLocal(history()), unique_suffix="v1"))
    answered(forecast(FakeLocal(history()), unique_suffix="v1"))

    assert len(fits) == 2
    assert not models_dir.exists()


def test_predict_trains_once_and_then_loads_the_saved_model(models_dir, fits):
    first = result_of(forecast(FakeLocal(history()), model_mode="predict", unique_suffix="v1"))
    second = result_of(forecast(FakeLocal(history(50)), model_mode="predict", unique_suffix="v1"))

    assert len(fits) == 1
    assert len(list(models_dir.iterdir())) == 1
    assert first["points_read"] == 40
    # The model read the 40 points it was trained on, not today's 50.
    assert second["points_read"] == 40


def test_a_loaded_model_forecasts_after_todays_data(models_dir):
    answered(forecast(FakeLocal(history(end=NOW - 5 * HOUR)), model_mode="predict", unique_suffix="v1"))

    result = result_of(forecast(FakeLocal(history()), model_mode="predict", unique_suffix="v1"))

    assert times_of(result)[0] == NOW


def test_each_series_and_version_has_its_own_saved_model(models_dir, fits):
    rows = history(10, host="a") + history(10, host="b")

    answered(forecast(FakeLocal(rows), group_by=["host"], model_mode="predict", unique_suffix="v1"))
    answered(forecast(FakeLocal(rows), group_by=["host"], model_mode="predict", unique_suffix="v1"))
    assert len(fits) == 2
    assert len(list(models_dir.iterdir())) == 2

    answered(forecast(FakeLocal(rows), group_by=["host"], model_mode="predict", unique_suffix="v2"))
    assert len(fits) == 4


def model_file(field="usage", key=None, **keys):
    req = {
        "unique_suffix": "v1",
        "source_database": None,
        "source_measurement": "cpu",
        "query": None,
        "tag_filter": {},
        "sql_filter": None,
        "sampling_ns": None,
    }
    series = plugin.Series(field=field, tags={"ignored": "tag"}, key=key or {})
    return plugin._model_path(SimpleNamespace(**{**req, **keys}), series).name


@pytest.mark.parametrize(
    "keys",
    [
        {"unique_suffix": "v2"},
        {"source_database": "eu"},
        {"source_measurement": "mem"},
        {"query": QUERY},
        {"tag_filter": {"host": ["a"]}},
        {"sql_filter": "usage > 0"},
        {"sampling_ns": HOUR},
        {"field": "temp"},
        {"key": {"host": "a"}},
    ],
)
def test_what_a_model_trains_on_names_its_file(keys, models_dir):
    assert model_file(**keys) != model_file()


PREDICT = {"model_mode": "predict", "unique_suffix": "v1"}


@pytest.mark.parametrize(
    "keys",
    [
        {"seasonality_mode": "multiplicative"},
        {"holiday_country_names": ["US"]},
        {"window": "20d"},
        {"point_limit": 30},
    ],
)
def test_settings_and_history_bounds_keep_the_saved_model(keys, models_dir, fits):
    answered(forecast(FakeLocal(history()), **PREDICT))

    answered(forecast(FakeLocal(history()), **PREDICT, **keys))

    assert len(fits) == 1
    assert len(list(models_dir.iterdir())) == 1


def test_a_tag_outside_group_by_does_not_change_the_model(models_dir, fits):
    schema = {**SCHEMA, "room": TAG, "fw": TAG}
    half = NOW - 21 * HOUR
    windows = (
        history(room="k", fw="1.0"),
        history(20, end=half, room="k", fw="1.0") + history(20, room="k", fw="2.0"),
        history(room="k", fw="2.0"),
        history(room="k", fw="1.0"),
    )

    for rows in windows:
        answered(forecast(FakeLocal(rows, schema=schema), group_by=["room"], **PREDICT))

    assert len(fits) == 1
    assert len(list(models_dir.iterdir())) == 1


def test_each_query_series_has_its_own_model(models_dir, fits):
    answered(forecast_query(FakeLocal(query_rows=query_rows()), **PREDICT))

    assert len(fits) == 4
    assert len(list(models_dir.iterdir())) == 4


def test_any_tag_value_makes_a_safe_file_name(models_dir):
    rows = history(10, host="../../etc/passwd")

    answered(forecast(FakeLocal(rows), group_by=["host"], model_mode="predict", unique_suffix="v1"))

    [saved] = models_dir.iterdir()
    assert re.fullmatch(r"prophet_model_v1_[0-9a-f]{16}\.json", saved.name)


def test_a_model_that_cannot_be_saved_warns_and_leaves_no_temporary_file(models_dir, monkeypatch):
    def broken(source, target):
        raise OSError("disk full")

    monkeypatch.setattr(plugin.os, "replace", broken)
    local = FakeLocal(history())

    result = result_of(forecast(local, model_mode="predict", unique_suffix="v1"))

    assert "forecast" in result
    assert list(models_dir.iterdir()) == []
    assert any("could not be saved" in text for text in local.messages("warn"))


def test_a_models_directory_that_cannot_be_made_warns_and_still_forecasts(models_dir, fits):
    models_dir.write_text("a file where the directory should be")
    local = FakeLocal(history())

    result = result_of(forecast(local, model_mode="predict", unique_suffix="v1"))

    assert "forecast" in result
    assert len(fits) == 1
    assert any("could not be saved" in text for text in local.messages("warn"))


def test_an_unreadable_saved_model_costs_only_its_series(models_dir, fits):
    rows = [row for host in "abc" for row in history(20, host=host)]
    keys = {"group_by": ["host"], **PREDICT}
    answered(forecast(FakeLocal(rows), **keys))
    sorted(models_dir.iterdir())[0].write_text("{not json")
    fits.clear()

    results = answered(forecast(FakeLocal(rows), **keys))["results"]

    broken = [result for result in results if result.get("code") == "model_unavailable"]
    assert len(broken) == 1
    assert broken[0]["status"] == "error"
    assert "cannot read the saved model" in broken[0]["message"]
    assert sum("forecast" in result for result in results) == 2
    assert fits == []


def test_no_plugin_directory_is_model_unavailable(monkeypatch):
    for name in ("PLUGIN_DIR", "INFLUXDB3_PLUGIN_DIR", "VIRTUAL_ENV"):
        monkeypatch.delenv(name, raising=False)

    result = result_of(forecast(FakeLocal(history()), model_mode="predict", unique_suffix="v1"))

    assert result["code"] == "model_unavailable"
    assert "plugin directory" in result["message"]


def test_a_failed_fit_is_internal_for_that_series_only(monkeypatch):
    original = plugin.Prophet.fit

    def fit(self, df, **kwargs):
        if df["y"].iloc[0] == 99.0:
            raise RuntimeError("no optimum")
        return original(self, df, **kwargs)

    monkeypatch.setattr(plugin.Prophet, "fit", fit)
    rows = history(10, value=lambda i: 1.0, host="a") + history(10, value=lambda i: 99.0, host="b")

    good, bad = answered(forecast(FakeLocal(rows), group_by=["host"]))["results"]

    assert "forecast" in good
    assert (bad["status"], bad["code"]) == ("error", "internal")
    assert "no optimum" in bad["message"]


def test_a_changepoint_outside_the_history_fails_that_series():
    result = result_of(forecast(FakeLocal(history()), changepoints=["2000-01-01"]))

    assert result["code"] == "internal"
    assert "Changepoints must fall within training data" in result["message"]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_validation_trains_before_the_cut_and_answers_after_the_data(fits):
    local = FakeLocal(history(40))

    result = result_of(forecast(local, validation_window="5h", msre_threshold=1))

    assert result["points_read"] == 35
    assert len(fits[0]) == 35
    # Measured up to NOW - 1h, so the horizon starts at NOW.
    assert times_of(result) == [NOW, NOW + HOUR, NOW + 2 * HOUR]
    cut = plugin._iso(NOW - 5 * HOUR)
    assert local.sent("time < $cut")[0].params["cut"] == cut
    assert local.sent("time >= $cut")[0].params["cut"] == cut


def test_the_cap_counts_the_validation_window(fits):
    response = forecast(
        FakeLocal(history(40)),
        validation_window="5h",
        horizon_steps=3,
        max_forecast_points=5,
    )

    assert "the horizon is 8 points" in refused(response)
    assert fits == []


def test_failed_validation_answers_other_without_a_forecast_or_a_write():
    local = FakeLocal(history(40, value=lambda i: 100.0 if i >= 35 else 1.0))

    result = result_of(
        forecast(local, validation_window="5h", msre_threshold=0.01, write_results=True)
    )

    assert (result["status"], result["code"]) == ("error", "other")
    assert "exceeds msre_threshold 0.01" in result["message"]
    assert "forecast" not in result
    assert local.write_calls == []


def test_no_data_in_the_validation_window_fails_validation(fits):
    local = FakeLocal(history(end=NOW - 10 * HOUR))

    result = result_of(forecast(local, validation_window="5h"))

    assert result["code"] == "other"
    assert "no data from" in result["message"]
    assert fits == []


def test_validation_works_on_a_calendar_grid(fits):
    rows = history(40, step=DAY, end=plugin._to_ns(datetime(2027, 1, 15)))
    local = FakeLocal(rows)

    result = result_of(
        forecast(local, window="1y", forecast_freq="D", validation_window="5d", msre_threshold=1)
    )

    assert len(fits[0]) < 40
    assert times_of(result)[0] == plugin._to_ns(datetime(2027, 1, 16))


@pytest.mark.parametrize("minute", [10, 59])
def test_a_validation_window_below_a_bucket_checks_one_full_bucket_at_any_minute(
    minute, monkeypatch, fits
):
    monkeypatch.setattr(plugin._time, "time_ns", lambda: NOW + minute * MINUTE)
    rows = history(12 * 24, step=5 * MINUTE, end=NOW + HOUR, value=lambda i: 20.0)
    local = FakeLocal(rows)

    result = result_of(
        forecast(
            local,
            sampling_interval="1h",
            validation_window="30min",
            msre_threshold=1,
            horizon_steps=1,
        )
    )

    assert local.sent("time < $cut")[0].params["cut"] == plugin._iso(NOW - HOUR)
    assert trained_until(fits[0]) == NOW - 2 * HOUR
    assert times_of(result) == [NOW]


def validation_problem(actual, central, alignment="position", threshold=math.inf, spacing=HOUR):
    series = plugin.Series(
        field="usage",
        tags={},
        actual_times=[moment for moment, _ in actual],
        actual_values=[value for _, value in actual],
    )
    grid = plugin.Grid(
        times=plugin._timestamps([moment for moment, _ in central]),
        first_new=0,
        spacing=pd.Timedelta(spacing, "ns"),
    )
    values = np.array([value for _, value in central])
    req = SimpleNamespace(validation_alignment=alignment, msre_threshold=threshold)
    return plugin._validation_problem(FakeLocal(), series, grid, values, req, "t")


def test_position_pairs_in_time_order():
    actual = [(HOUR, 2.0), (2 * HOUR, 4.0)]
    central = [(HOUR + HOUR // 2, 1.0), (3 * HOUR, 4.0), (4 * HOUR, 99.0)]

    # MSRE = ((2-1)^2/2^2 + 0) / 2 = 0.125; the third point has no pair.
    assert validation_problem(actual, central, threshold=0.125) is None
    assert "MSRE 0.125" in validation_problem(actual, central, threshold=0.12)


def test_nearest_pairs_within_half_a_step():
    actual = [(HOUR + 10, 2.0), (5 * HOUR, 100.0)]
    central = [(HOUR, 2.0), (2 * HOUR, 50.0)]

    # The second measured point is too far from any forecast point to be compared.
    assert validation_problem(actual, central, "nearest", threshold=0.0) is None


def test_nearest_with_nothing_in_reach_fails():
    actual = [(10 * HOUR, 2.0)]
    central = [(HOUR, 2.0)]

    assert "no measured point is within" in validation_problem(actual, central, "nearest")


def test_only_zero_actuals_fail_and_some_zero_actuals_are_skipped():
    central = [(HOUR, 1.0), (2 * HOUR, 2.0)]

    assert "every measured value is zero" in validation_problem([(HOUR, 0.0)], central)
    assert validation_problem([(HOUR, 0.0), (2 * HOUR, 2.0)], central, threshold=0.0) is None


def test_a_query_is_split_at_the_cut(fits):
    local = FakeLocal(query_rows=history(40))

    result = result_of(forecast_query(local, validation_window="5h", msre_threshold=1))

    assert len(fits[0]) == 35
    assert result["points_read"] == 35
    assert times_of(result)[0] == NOW


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------


ALERTS = {
    "is_sending_alert": "true",
    "senders": "slack",
    "slack_webhook_url": "https://hooks.slack.example/x",
    "validation_window": "5h",
    "msre_threshold": "0.01",
}


@pytest.fixture
def posts(monkeypatch):
    """Every notification POST, answered with success, with a token in the environment."""
    sent = []

    def post(url, headers=None, data=None, timeout=None):
        sent.append(SimpleNamespace(url=url, headers=headers, body=json.loads(data)))
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"results": "ok"})

    monkeypatch.setattr(plugin.requests, "post", post)
    monkeypatch.setenv("INFLUXDB3_AUTH_TOKEN", "secret")
    return sent


def failing_rows() -> list:
    def bad(i):
        return 100.0 if i >= 35 else 1.0

    return (
        history(40, value=bad, host="a")
        + history(40, value=lambda i: 1.0, host="b")
        + history(40, value=bad, host="c")
    )


def test_one_alert_per_run_lists_the_failed_series(posts):
    local = FakeLocal(failing_rows())

    run_scheduled(local, group_by="host", **ALERTS)

    [post] = posts
    assert post.url == "http://localhost:8181/api/v3/engine/notify"
    assert post.headers["Authorization"] == "Bearer secret"
    assert post.body["senders_config"] == {"slack": {"slack_webhook_url": ALERTS["slack_webhook_url"]}}
    text = post.body["notification_text"]
    assert "usage host=a, usage host=c" in text
    assert "host=b" not in text
    assert "table:cpu" in text and "field:usage" in text
    assert "table:_forecasts.cpu" in text
    assert plugin._iso(NOW - 5 * HOUR) in text and plugin._iso(NOW) in text
    assert {row.tags["host"] for row in local.written} == {"b"}


def test_a_failed_write_costs_neither_the_errors_nor_the_alert(posts):
    rows = failing_rows() + history(4, host="d")
    local = FakeLocal(rows, write_error=RuntimeError("disk full"))

    run_scheduled(local, group_by="host", **ALERTS)

    [post] = posts
    assert "usage host=a, usage host=c" in post.body["notification_text"]
    errors = local.messages("error")
    assert any("other" in text and "'host': 'a'" in text for text in errors)
    assert any("internal" in text and "disk full" in text for text in errors)
    assert any("too_few_points" in text and "'d'" in text for text in local.messages("warn"))
    assert local.written == []


def test_the_alert_text_and_its_address_can_be_set(posts):
    run_scheduled(
        FakeLocal(failing_rows()),
        group_by="host",
        notification_text="$version|$measurement|$field|$series|$output_measurement|$other",
        unique_suffix="v1",
        port_override="8182",
        notification_path="custom/path",
        **ALERTS,
    )

    assert posts[0].body["notification_text"] == (
        "v1|cpu|usage|usage host=a, usage host=c|_forecasts.cpu|$other"
    )
    assert posts[0].url == "http://localhost:8182/api/v3/engine/custom/path"


@pytest.mark.parametrize("overrides", [{"is_sending_alert": "false"}, {"msre_threshold": "1000000"}])
def test_no_alert_without_the_flag_or_a_failure(overrides, posts):
    run_scheduled(FakeLocal(failing_rows()), group_by="host", **{**ALERTS, **overrides})

    assert posts == []


def test_a_too_short_series_is_not_an_alert(posts):
    run_scheduled(FakeLocal(history(4)), **ALERTS)

    assert posts == []


def test_the_token_comes_from_the_toml_file(posts, tmp_path):
    path = write_toml(tmp_path, 'influxdb3_auth_token = "from-toml"\n')

    run_scheduled(FakeLocal(failing_rows()), group_by="host", config_file_path=path, **ALERTS)

    assert posts[0].headers["Authorization"] == "Bearer from-toml"


def test_an_alert_without_a_token_is_logged(posts, monkeypatch):
    monkeypatch.delenv("INFLUXDB3_AUTH_TOKEN")
    local = FakeLocal(failing_rows())

    run_scheduled(local, group_by="host", **ALERTS)

    assert posts == []
    assert any("INFLUXDB3_AUTH_TOKEN not found" in text for text in local.messages("error"))


SMS = {**ALERTS, "senders": "sms", "twilio_to_number": "+1", "twilio_from_number": "+2"}


def test_the_twilio_token_comes_from_the_environment(posts, monkeypatch):
    monkeypatch.setenv("INFLUXDB3_PROPHET_FORECASTING_TWILIO_TOKEN", "from-env")

    run_scheduled(FakeLocal(failing_rows()), group_by="host", **SMS)

    assert posts[0].body["senders_config"]["sms"]["twilio_token"] == "from-env"


@pytest.mark.parametrize("key", plugin.SECRET_KEYS)
def test_a_secret_is_refused_as_a_trigger_argument(key, posts, fits):
    local = FakeLocal(failing_rows())

    run_scheduled(local, group_by="host", **ALERTS, **{key: "hunter2"})

    [message] = local.messages("error")
    assert "bad_request" in message and f"{key} cannot be a trigger argument" in message
    assert "TOML file" in message and f"INFLUXDB3_PROPHET_FORECASTING_{key.upper()}" in message
    assert not any("hunter2" in text for text in local.messages())
    assert local.queries == [] and fits == [] and posts == []


@pytest.mark.parametrize("key", plugin.SECRET_KEYS)
def test_a_secret_is_refused_on_an_http_trigger_without_showing_it(key):
    response = plugin.process_request(
        FakeLocal(history()), {}, {}, json.dumps(REQUEST), {key: "hunter2"}
    )

    message = refused(response)
    assert key in message and "hunter2" not in message
    # An HTTP trigger has no use for it, so the message must not send it to the TOML file.
    assert "TOML file" not in message


@pytest.mark.parametrize(
    "config, fragment",
    [
        ({}, "No senders provided"),
        ({"senders": "pigeon"}, "No valid senders configured"),
        ({"senders": "slack", "slack_webhook_url": "ftp://x"}, "No valid senders configured"),
    ],
)
def test_senders_without_a_valid_one_are_refused(config, fragment):
    with pytest.raises(Exception, match=fragment):
        plugin.parse_senders(FakeLocal(), config, "t")


def test_several_senders_take_a_space_separated_list():
    config = {
        "senders": "discord http",
        "discord_webhook_url": "https://d",
        "http_webhook_url": "https://h",
        "http_headers": "abc",
    }

    assert plugin.parse_senders(FakeLocal(), config, "t") == {
        "discord": {"discord_webhook_url": "https://d"},
        "http": {"http_webhook_url": "https://h", "http_headers": "abc"},
    }


def test_a_failed_notification_is_retried_and_logged(monkeypatch):
    calls = []

    def post(*args, **kwargs):
        calls.append(args)
        raise requests.ConnectionError("refused")

    monkeypatch.setattr(plugin.requests, "post", post)
    local = FakeLocal()

    plugin.send_notification(local, 8181, "notify", "token", {}, "t")

    assert len(calls) == 3
    assert any("after 3 attempts" in text for text in local.messages("error"))


# ---------------------------------------------------------------------------
# Backtest
# ---------------------------------------------------------------------------


def known_predict(model, times, levels):
    """A forecast flat at 10.0: 9.5-10.5 at the 50% level and 9.2-10.8 at the 80%."""
    central = np.full(len(times), 10.0)
    widths = {0.5: 0.5, 0.8: 0.8}
    return central, {
        level: (central - widths.get(level, 2.0), central + widths.get(level, 2.0))
        for level in levels
    }


def held_out_history(actual: list, before=27) -> list:
    rows = history(before + len(actual), value=lambda i: 10.0)
    for row, value in zip(rows[-len(actual):], actual):
        row["usage"] = value
    return rows


def test_backtest_compares_the_held_out_points(monkeypatch):
    monkeypatch.setattr(plugin, "_predict", known_predict)
    rows = held_out_history([10.2, 10.6, 11.0])

    result = result_of(forecast(FakeLocal(rows), backtest_points=3))

    assert result["backtest"]["points"] == 3
    assert result["backtest"]["coverage"] == pytest.approx({"50": 1 / 3, "80": 2 / 3})
    assert result["backtest"]["mae"] == pytest.approx(0.6)


def test_backtest_on_the_real_model_tracks_a_line():
    result = result_of(forecast(FakeLocal(history(60, value=lambda i: 2.0 * i)), backtest_points=5))

    backtest = result["backtest"]
    assert backtest["points"] == 5
    assert set(backtest["coverage"]) == {"50", "80"}
    assert all(0 <= share <= 1 for share in backtest["coverage"].values())
    assert backtest["mae"] < 1


def test_backtest_trains_a_second_model_and_leaves_the_forecast_on_all_points(fits):
    result = result_of(forecast(FakeLocal(history(30)), backtest_points=5))

    assert [len(df) for df in fits] == [30, 25]
    assert fits[1]["y"].tolist() == [float(i) for i in range(25)]
    assert result["points_read"] == 30


def test_backtest_gets_the_same_history_length_as_the_forecast(fits):
    local = FakeLocal(history(40))

    answered(forecast(local, point_limit=20, backtest_points=5))

    assert [len(df) for df in fits] == [20, 20]
    assert fits[0]["y"].iloc[-1] == 39.0
    assert fits[1]["y"].iloc[-1] == 34.0
    assert "LIMIT 25" in local.history_query().sql


@pytest.mark.parametrize("points", [2, 4], ids=["too short", "as long as the series"])
def test_a_series_too_short_for_the_backtest_answers_null_with_its_forecast(points, fits):
    result = result_of(forecast(FakeLocal(history(4)), backtest_points=points))

    assert result["backtest"] is None
    assert "forecast" in result
    assert len(fits) == 1


def test_a_failed_backtest_leaves_the_forecast():
    rows = history(40, step=DAY, end=plugin._to_ns(datetime(2027, 1, 10)))
    local = FakeLocal(rows)

    # After the cut, so only the backtest's shorter history lacks this date.
    result = result_of(forecast(local, window="1y", changepoints=["2027-01-08"], backtest_points=5))

    assert result["backtest"] is None
    assert "forecast" in result
    assert any("the backtest failed" in text for text in local.messages("warn"))


def test_a_saved_model_still_gets_a_fresh_backtest_that_is_not_saved(models_dir, fits):
    keys = {"model_mode": "predict", "unique_suffix": "v1", "backtest_points": 3}

    first = result_of(forecast(FakeLocal(history(30)), **keys))
    second = result_of(forecast(FakeLocal(history(30)), **keys))

    # The forecast's model once, and a backtest's model on each request.
    assert [len(df) for df in fits] == [30, 27, 27]
    assert len(list(models_dir.iterdir())) == 1
    assert first["backtest"]["points"] == second["backtest"]["points"] == 3


def test_a_query_gets_a_backtest_and_reads_enough_for_it(fits):
    result = result_of(
        forecast_query(FakeLocal(query_rows=history(40)), point_limit=20, backtest_points=5)
    )

    assert result["backtest"]["points"] == 5
    assert [len(df) for df in fits] == [20, 20]


def test_the_backtest_does_not_change_the_written_rows():
    local = FakeLocal(history(30))

    answered(forecast(local, write_results=True, backtest_points=3))

    assert len(local.written) == 3
    assert not any("backtest" in name for name in local.written[0].fields)
    assert local.written[0].fields["points_read"] == 30


@pytest.mark.parametrize(
    "keys, fragment",
    [
        ({"validation_window": "1h"}, "one of backtest_points and validation_window"),
        ({"max_forecast_points": 4}, "backtest_points is 5, more than max_forecast_points=4"),
    ],
)
def test_backtest_points_refused_before_any_query(keys, fragment, fits):
    local = FakeLocal(history())
    # Within the cap, so that only the backtest can break it.
    horizon = {"horizon_steps": 1}

    assert fragment in refused(forecast(local, backtest_points=5, **horizon, **keys))

    assert local.queries == []
    assert fits == []


def test_a_request_backtest_refuses_a_validation_window_on_the_trigger():
    args = {"validation_window": "1h"}
    body = json.dumps({**REQUEST, "backtest_points": 3})

    assert "not both" in refused(plugin.process_request(FakeLocal(history()), {}, {}, body, args))


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------


def test_on_demand_writes_nothing_by_default():
    local = FakeLocal(history())

    answered(forecast(local))

    assert local.written == []


def test_written_rows():
    local = FakeLocal(history(host="a"))

    body = answered(forecast(local, write_results=True, group_by=["host"], forecast_id="ops"))

    assert len(local.written) == 3
    first = local.written[0]
    assert first.measurement == "_forecasts.cpu"
    assert first.database is None
    assert first.tags == {
        "field": "usage",
        "model": "prophet",
        "source_measurement": "cpu",
        "forecast_id": "ops",
        "host": "a",
    }
    assert [row.time for row in local.written] == [NOW, NOW + HOUR, NOW + 2 * HOUR]
    assert [row.fields["step"] for row in local.written] == [1, 2, 3]
    assert set(first.fields) == {
        "forecast",
        "lower_50",
        "upper_50",
        "lower_80",
        "upper_80",
        "step",
        "points_read",
        "run_time",
    }
    assert first.fields["points_read"] == 40
    assert first.fields["run_time"] == "2027-01-15T08:00:00Z"
    point = body["results"][0]["forecast"][0]
    assert point["timestamp"] * 10**6 == first.time
    assert point["forecast"] == first.fields["forecast"]
    assert [call.no_sync for call in local.write_calls] == [True]


def test_target_measurement_and_database():
    local = FakeLocal(history())

    answered(
        forecast(local, write_results=True, target_measurement="fc", target_database="archive")
    )

    assert {row.measurement for row in local.written} == {"fc"}
    assert {row.database for row in local.written} == {"archive"}


def test_a_series_tag_named_like_a_row_column_is_left_out():
    schema = {**SCHEMA, "model": TAG, "step": TAG}
    rows = [{**row, "model": "other", "step": "x"} for row in history(host="a")]
    local = FakeLocal(rows, schema=schema)

    result = result_of(forecast(local, write_results=True))

    assert result["tags"] == {"host": "a", "model": "other", "step": "x"}
    assert local.written[0].tags["model"] == "prophet"
    assert local.written[0].tags["host"] == "a"
    assert "step" not in local.written[0].tags


def test_only_series_with_a_forecast_are_written():
    local = FakeLocal(history(10, host="a") + history(2, host="b"))

    answered(forecast(local, write_results=True, group_by=["host"]))

    assert {row.tags["host"] for row in local.written} == {"a"}


def test_a_failed_write_fails_the_request():
    local = FakeLocal(history(), write_error=RuntimeError("disk full"))

    message = failed(forecast(local, write_results=True), "internal", 500)

    assert "the write failed: disk full" in message


def test_rows_that_cannot_be_built_fail_only_a_request_that_writes(monkeypatch):
    class Refusing(FakeLineBuilder):
        def tag(self, key, value):
            raise ValueError("invalid tag")

    monkeypatch.setattr(plugin, "LineBuilder", Refusing, raising=False)

    message = failed(forecast(FakeLocal(history()), write_results=True), "internal", 500)

    assert "rows cannot be built: invalid tag" in message
    answered(forecast(FakeLocal(history())))


# ---------------------------------------------------------------------------
# Query mode
# ---------------------------------------------------------------------------


def query_rows(count=10, hosts=("a", "b")) -> list:
    return [
        {**row, "host": host, "load": row["usage"] * 2}
        for host in hosts
        for row in history(count)
    ]


def test_each_numeric_column_of_each_series_is_a_forecast(fits):
    body = answered(forecast_query(FakeLocal(query_rows=query_rows())))

    assert [(r["tags"]["host"], r["field"]) for r in body["results"]] == [
        ("a", "load"),
        ("a", "usage"),
        ("b", "load"),
        ("b", "usage"),
    ]
    assert body["series_total"] == 4
    assert len(fits) == 4


def test_query_results_are_ordered_with_a_missing_key_last():
    rows = [{**row, "host": host} for host in ("b", None, "a") for row in history(10)]

    body = answered(forecast_query(FakeLocal(query_rows=rows)))

    assert [r["tags"] for r in body["results"]] == [{"host": "a"}, {"host": "b"}, {}]


def test_query_rows_sharing_a_timestamp_are_averaged(fits):
    rows = history(10, value=lambda i: 1.0) + history(10, value=lambda i: 3.0)

    result = result_of(forecast_query(FakeLocal(query_rows=rows)))

    assert result["points_read"] == 10
    assert fits[0]["y"].tolist() == [2.0] * 10


def test_query_series_limit_cuts_after_sorting():
    body = answered(forecast_query(FakeLocal(query_rows=query_rows()), series_limit=3))

    assert [(r["tags"]["host"], r["field"]) for r in body["results"]] == [
        ("a", "load"),
        ("a", "usage"),
        ("b", "load"),
    ]
    assert body["series_total"] == 4


def test_query_cap_counts_forecasts_not_series():
    assert "max_series=3" in refused(
        forecast_query(FakeLocal(query_rows=query_rows()), max_series=3)
    )


def test_no_rows_means_no_series(fits):
    body = answered(forecast_query(FakeLocal()))

    assert body["series_total"] == 0
    assert body["results"] == []
    assert fits == []


@pytest.mark.parametrize(
    "row, fragment",
    [({"host": "a", "usage": 1.0}, "no time column"), ({"time": NOW, "host": "a"}, "no numeric column")],
)
def test_a_query_without_a_time_or_numeric_column_is_a_bad_request(row, fragment):
    assert fragment in refused(forecast_query(FakeLocal(query_rows=[row])))


def test_a_boolean_column_is_a_key_and_an_empty_column_is_nothing():
    rows = [{**row, "up": True, "empty": None} for row in history(10)]

    body = answered(forecast_query(FakeLocal(query_rows=rows)))

    assert [r["field"] for r in body["results"]] == ["usage"]
    assert body["results"][0]["tags"] == {"up": "True"}


def test_point_limit_keeps_the_latest_points_of_an_unsorted_query(fits):
    rows = list(reversed(history(30)))

    result = result_of(forecast_query(FakeLocal(query_rows=rows), point_limit=15))

    assert result["points_read"] == 15
    assert fits[0]["y"].tolist() == [float(i) for i in range(15, 30)]


def test_query_needs_no_history_keys_and_has_no_default_window():
    local = FakeLocal(query_rows=history(10))

    answered(forecast_query(local))

    assert local.sent(QUERY)[-1].sql == QUERY


def test_window_wraps_the_query_and_drops_a_series_without_rows_in_it():
    rows = [{**row, "host": "old"} for row in history(10, end=NOW - 120 * HOUR)]
    rows += [{**row, "host": "new"} for row in history(10)]
    local = FakeLocal(query_rows=rows)

    body = answered(forecast_query(local, window="1d", max_series=1))

    assert [r["tags"]["host"] for r in body["results"]] == ["new"]
    assert (
        "AS __history WHERE time > (now() AT TIME ZONE 'UTC') - INTERVAL '86400 seconds'"
        in local.sent(QUERY)[-1].sql
    )


def test_a_query_that_cannot_be_wrapped_is_filtered_by_the_plugin():
    rows = history(10, end=NOW - 120 * HOUR, host="old") + history(10, host="new")
    local = FakeLocal(query_rows=rows, fail={"AS __history": RuntimeError("cannot compare")})

    body = answered(forecast_query(local, window="1d"))

    assert [r["tags"]["host"] for r in body["results"]] == ["new"]
    assert any("Window applied in the plugin" in text for text in local.messages("warn"))
    assert local.queries[-1].sql == QUERY


def test_a_wrapped_query_out_of_resources_is_not_retried():
    local = FakeLocal(fail={"AS __history": RuntimeError("Resources exhausted: too big")})

    assert "Resources exhausted" in failed(forecast_query(local, window="1d"), "internal", 500)
    assert local.queries[-1].sql.startswith("SELECT * FROM")


@pytest.mark.parametrize(
    "query, body",
    [
        ("SELECT 1  ;  ", "SELECT 1"),
        ("SELECT 1 -- note", "SELECT 1 -- note"),
    ],
)
def test_windowed_query_keeps_a_trailing_comment_inside(query, body):
    wrapped = plugin._windowed_query(query, 3600 * SECOND)

    assert wrapped.startswith(f"SELECT * FROM (\n{body}\n) AS __history WHERE")


def test_a_query_failing_only_when_run_is_internal():
    class FailsWhenRun(FakeLocal):
        def query(self, sql, params=None, database=None):
            if sql == QUERY:
                raise RuntimeError("boom")
            return super().query(sql, params, database)

    assert "the query failed: boom" in failed(forecast_query(FailsWhenRun()), "internal", 500)


def test_query_sampling_averages_in_the_plugin(fits):
    rows = history(40, step=30 * MINUTE, end=NOW - 30 * MINUTE)

    result = result_of(forecast_query(FakeLocal(query_rows=rows), sampling_interval="1h"))

    assert result["step_ms"] == HOUR // 10**6
    assert result["points_read"] == 20
    assert fits[0]["y"].iloc[0] == pytest.approx(0.5)


def test_query_writes_use_the_measurement_from_its_plan():
    local = FakeLocal(query_rows=history(10, host="a"))

    answered(forecast_query(local, write_results=True))

    assert local.written[0].measurement == "_forecasts.cpu"
    assert local.written[0].tags["source_measurement"] == "cpu"
    assert local.written[0].tags["host"] == "a"


def test_query_over_several_measurements_needs_a_target_to_write():
    local = FakeLocal(query_rows=history(10), explain=("cpu", "mem"))

    assert "target_measurement" in refused(forecast_query(local, write_results=True))
    assert local.queries[-1].sql.startswith("EXPLAIN")


def test_query_over_several_measurements_writes_without_the_tag():
    local = FakeLocal(query_rows=history(10), explain=("cpu", "mem"))

    answered(forecast_query(local, write_results=True, target_measurement="fc"))

    assert local.written[0].measurement == "fc"
    assert "source_measurement" not in local.written[0].tags


def test_a_query_that_does_not_plan_is_a_bad_request(fits):
    local = FakeLocal(fail={"EXPLAIN": RuntimeError("no such table")})

    assert "does not plan: no such table" in refused(forecast_query(local))
    assert fits == []


def test_a_query_key_named_like_a_row_column_is_refused_when_writing():
    rows = history(10, model="m")

    assert "clashes" in refused(forecast_query(FakeLocal(query_rows=rows), write_results=True))
    answered(forecast_query(FakeLocal(query_rows=rows)))


@pytest.mark.parametrize(
    "plan, names",
    [
        ("TableScan: cpu projection=[time, usage]", ["cpu"]),
        ("TableScan: cpu load projection=[time]", ["cpu load"]),
        ("TableScan: public.iox.cpu projection=[time]", ["cpu"]),
        ("TableScan: cpu, full_filters=[usage > 0]", ["cpu"]),
        ("TableScan: cpu", ["cpu"]),
        (
            "Join\n  TableScan: mem projection=[a]\n  TableScan: cpu projection=[a]\n"
            "  TableScan: cpu projection=[b]",
            ["cpu", "mem"],
        ),
    ],
)
def test_measurements_come_from_the_logical_plan(plan, names):
    class Planned(FakeLocal):
        def query(self, sql, params=None, database=None):
            return [
                {"plan_type": "logical_plan", "plan": plan},
                {"plan_type": "physical_plan", "plan": "TableScan: ignored projection=[a]"},
            ]

    req = SimpleNamespace(query=QUERY, source_database=None)

    assert plugin._query_measurements(Planned(), req) == names


# ---------------------------------------------------------------------------
# HTTP statuses
# ---------------------------------------------------------------------------


def test_an_unexpected_failure_is_internal(monkeypatch):
    def explode(*args):
        raise KeyError("surprise")

    monkeypatch.setattr(plugin, "_run_forecast", explode)
    local = FakeLocal(history())

    assert "surprise" in failed(forecast(local), "internal", 500)
    assert local.messages("error")


def test_a_bad_request_logs_a_warning_and_other_codes_an_error():
    local = FakeLocal(history())
    refused(forecast(local, horizon_steps=None))
    assert local.messages("error") == []
    assert any("bad_request" in text for text in local.messages("warn"))

    local = FakeLocal(history(), write_error=RuntimeError("disk full"))
    failed(forecast(local, write_results=True), "internal", 500)
    assert any("internal" in text for text in local.messages("error"))


@pytest.mark.parametrize(
    "code, status",
    [
        ("bad_request", 400),
        ("unauthorized", 401),
        ("model_unavailable", 503),
        ("timeout", 504),
        ("internal", 500),
        ("other", 400),
    ],
)
def test_each_contract_code_has_its_status(code, status, monkeypatch):
    def explode(*args):
        raise plugin.ForecastError(code, "why")

    monkeypatch.setattr(plugin, "_run_forecast", explode)

    assert failed(forecast(FakeLocal()), code, status) == "why"


# ---------------------------------------------------------------------------
# Scheduled runs
# ---------------------------------------------------------------------------


def test_a_scheduled_run_writes_and_returns_nothing():
    local = FakeLocal(history())

    assert run_scheduled(local) is None

    assert len(local.written) == 3
    assert local.written[0].measurement == "_forecasts.cpu"


def test_a_scheduled_run_takes_its_times_from_the_trigger():
    local = FakeLocal(history())

    plugin.process_scheduled_call(
        local,
        CALL_TIME + timedelta(minutes=40),
        {
            "source_measurement": "cpu",
            "field": "usage",
            "horizon_steps": "1",
            "window": "6h",
            "validation_window": "2h",
        },
    )

    assert local.written[0].fields["run_time"] == "2027-01-15T08:40:00Z"
    query = local.history_query()
    assert query.params["history_start"] == plugin._iso(NOW_0840 - 6 * HOUR)
    assert query.params["history_end"] == plugin._iso(NOW_0840)
    assert query.params["cut"] == plugin._iso(NOW_0840 - 2 * HOUR)


def test_scheduled_arguments_are_strings():
    rows = history(10, host="a", region="eu") + history(10, host="b", region="eu")
    local = FakeLocal(rows)

    run_scheduled(
        local,
        group_by="host region",
        tag_filter="region=eu",
        levels="0.5 0.8",
        point_limit="15",
        window="1d",
        series_limit="1",
        forecast_id="ops",
        target_measurement="fc",
        target_database="archive",
        changepoint_prior_scale="0.1",
        holiday_country_names="US",
    )

    assert {row.tags["host"] for row in local.written} == {"a"}
    assert {row.tags["forecast_id"] for row in local.written} == {"ops"}
    assert {row.measurement for row in local.written} == {"fc"}
    assert {row.database for row in local.written} == {"archive"}
    assert '"region" = $tf0_0' in local.history_query().sql


def test_toml_overrides_trigger_arguments(tmp_path):
    path = write_toml(
        tmp_path, 'horizon_steps = 2\nlevels = [0.5]\n[tag_filter]\nhost = ["a", "b"]\n'
    )
    local = FakeLocal(history(host="a"))

    run_scheduled(local, config_file_path=path)

    assert len(local.written) == 2
    assert "lower_50" in local.written[0].fields
    assert "lower_80" not in local.written[0].fields
    assert '"host" IN ($tf0_0, $tf0_1)' in local.history_query().sql


def test_a_scheduled_query_writes():
    local = FakeLocal(query_rows=history(10))

    plugin.process_scheduled_call(local, CALL_TIME, {"query": QUERY, "horizon_steps": "2"})

    assert len(local.written) == 2


def test_a_schedule_needs_a_history():
    local = FakeLocal(history())

    run_scheduled(local, window=None)

    assert any("window or point_limit" in text for text in local.messages("error"))
    assert not any("start_time" in text for text in local.messages("error"))


@pytest.mark.parametrize(
    "key", ["write_results", "start_time", "end_time", "backtest_points", "nope"]
)
def test_keys_a_scheduled_run_does_not_take_are_logged_as_bad_requests(key):
    local = FakeLocal(history())

    run_scheduled(local, **{key: "1"})

    assert local.written == []
    assert any("bad_request" in text and key in text for text in local.messages("error"))


def test_a_short_series_in_a_scheduled_run_is_a_warning():
    local = FakeLocal(history(10, host="a") + history(2, host="b"))

    run_scheduled(local, group_by="host")

    assert {row.tags["host"] for row in local.written} == {"a"}
    assert any("too_few_points" in text and "'b'" in text for text in local.messages("warn"))
    assert local.messages("error") == []


def test_an_unexpected_scheduled_failure_is_logged_and_does_not_raise(monkeypatch):
    def explode(*args):
        raise KeyError("surprise")

    monkeypatch.setattr(plugin, "_run_forecast", explode)
    local = FakeLocal(history())

    assert run_scheduled(local) is None
    assert any("internal" in text and "surprise" in text for text in local.messages("error"))


# ---------------------------------------------------------------------------
# What ships beside the code
# ---------------------------------------------------------------------------


def metadata() -> dict:
    return json.loads(plugin.__doc__)


def active_keys(name: str) -> set:
    return set(tomllib.loads((PLUGIN_DIR / name).read_text()))


def test_metadata_names_only_keys_the_plugin_takes():
    meta = metadata()

    assert meta["plugin_type"] == ["scheduled", "http"]
    scheduled = {entry["name"] for entry in meta["scheduled_args_config"]}
    assert scheduled == {*plugin.SCHEDULED_NAMES, "config_file_path"} - set(plugin.SECRET_KEYS)
    http_args = {entry["name"] for entry in meta["http_args_config"]}
    assert http_args == {*plugin.REQUEST_NAMES, "config_file_path"}
    assert {entry["name"] for entry in meta["http_body_config"]} == set(plugin.REQUEST_NAMES)


def test_toml_templates_set_only_keys_the_plugin_takes():
    assert active_keys("prophet_forecasting_scheduler.toml") <= set(plugin.SCHEDULED_NAMES)
    assert active_keys("prophet_forecasting_http.toml") <= set(plugin.REQUEST_NAMES)


def test_manifest_matches_the_plugin():
    manifest = json.loads((PLUGIN_DIR / "forecast_interface.json").read_text())

    assert manifest["forecast_contract_version"] == 1
    assert set(manifest["filter"]) == {"tag_filter", "sql_filter"}
    assert manifest["backtest"] is True
    assert manifest["accepts_query"] is True
    [model] = manifest["models"]
    assert model["id"] == plugin.MODEL_ID and model["default"] is True
    assert model["min_points"] == plugin.MIN_POINTS
    assert set(plugin.DEFAULT_LEVELS) <= set(model["levels"])
    for param in manifest["advanced_params"]:
        assert param["name"] in plugin.REQUEST_NAMES
        assert param["name"] not in plugin.CONTRACT_KEYS


def test_the_manifest_ships_and_the_tests_do_not():
    plugin_table = tomllib.loads((PLUGIN_DIR / "manifest.toml").read_text())["plugin"]

    assert "!forecast_interface.json" in plugin_table["exclude"]
    assert "*.py" in plugin_table["exclude"]
    assert "!prophet_forecasting.py" in plugin_table["exclude"]
    assert f"!{Path(__file__).name}" not in plugin_table["exclude"]
