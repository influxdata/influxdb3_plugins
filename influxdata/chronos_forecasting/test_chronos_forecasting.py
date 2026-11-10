"""Unit tests for the chronos_forecasting plugin.

Only the runtime and the model are replaced: FakeLocal answers the plugin's queries
from rows in memory, and FakePipeline stands in for the downloaded weights. FakeLocal
recognises each query, it does not run SQL, so what the server does with that SQL is
for the live test plan.
"""

import itertools
import json
import math
import re
import sys
import tomllib
import types
from collections import namedtuple
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
import torch

import chronos_forecasting as plugin

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
BOLT_QUANTILES = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
SECOND = 10**9
MINUTE = 60 * SECOND
# A whole minute, so raw points and sampling buckets line up.
NOW = 1_800_000_000 * SECOND

REQUEST = {"source_measurement": "cpu", "field": "usage", "horizon_steps": 3}
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
        self,
        rows=(),
        schema=None,
        query_rows=(),
        explain=("cpu",),
        fail=None,
        write_error=None,
        before_history=None,
    ):
        self.rows = list(rows)
        self.schema = SCHEMA if schema is None else schema
        self.query_rows = list(query_rows)
        self.explain = explain
        self.fail = fail or {}  # SQL fragment -> what a query holding it raises
        self.write_error = write_error
        self.before_history = before_history
        self.queries: list = []
        self.written: list = []
        self.write_calls: list = []
        self.logs: list = []

    def query(self, sql, params=None, database=None):
        self.queries.append(SimpleNamespace(sql=sql, params=params or {}, database=database))
        self.params = params or {}
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
            return self._series(sql)
        if "AS __first" in sql:
            return self._tag_sets(sql)
        if " AS __t," in sql:
            if self.before_history:
                self.before_history()
            return self._history(sql)
        return [dict(row) for row in self.query_rows]

    def _with_field(self, sql: str) -> list:
        """The rows with the forecast field, within the time bounds passed as parameters."""
        field = re.search(r'"(\w+)" IS NOT NULL', sql).group(1)
        start = plugin._to_ns(self.params["history_start"])
        end = plugin._to_ns(self.params["history_end"]) if "history_end" in self.params else math.inf
        return [
            row
            for row in self.rows
            if row.get(field) is not None and start <= row["time"] < end
        ]

    def _series(self, sql: str) -> list:
        groups = aliased(sql, r'"(\w+)" AS __g(\d+)')
        others = aliased(sql, r'count\(CAST\("(\w+)" AS VARCHAR\)\) AS __c(\d+)')
        found: dict = {}
        for row in self._with_field(sql):
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

    def _tag_sets(self, sql: str) -> list:
        groups = aliased(sql, r'"(\w+)" AS __g(\d+)')
        others = aliased(sql, r'"(\w+)" AS __o(\d+)')
        rank = re.search(r"__rank <= (\d+)", sql)
        by_group: dict = {}
        for row in self._with_field(sql):
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

    def _history(self, sql: str) -> list:
        groups = aliased(sql, r'"(\w+)" AS __g(\d+)')
        fields = aliased(sql, r'avg\("(\w+)"\) AS __f(\d+)')
        binned = re.search(r"date_bin\(INTERVAL '(\d+) seconds', time\)", sql)
        step = int(binned.group(1)) * SECOND if binned else None
        cells: dict = {}
        for row in self._with_field(sql):
            moment = row["time"] - row["time"] % step if step else row["time"]
            key = (tuple(row.get(group) for group in groups), moment)
            cells.setdefault(key, []).append(row)
        answer = []
        for (group, moment), rows in cells.items():
            out = {"__t": moment, **{f"__g{i}": value for i, value in enumerate(group)}}
            for i, field in enumerate(fields):
                values = [row[field] for row in rows if row.get(field) is not None]
                out[f"__f{i}"] = sum(values) / len(values) if values else None
            answer.append(out)
        answer.sort(key=lambda row: row["__t"], reverse=True)
        limit = re.search(r"LIMIT (\d+)", sql)
        if limit:
            return answer[: int(limit.group(1))]
        keep = int(re.search(r"__rn <= (\d+)", sql).group(1))
        kept: dict = {}
        for row in answer:
            kept.setdefault(tuple(row.get(f"__g{i}") for i in range(len(groups))), []).append(row)
        return sorted(
            (row for rows in kept.values() for row in rows[:keep]), key=lambda row: row["__t"]
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


def flat_forecast(values, length: int, levels: list, crossing: bool):
    """A forecast flat at the last observed value, each quantile 2 * (q - 0.5) from it."""
    last = float(values[~values.isnan()][-1])
    row = [last + 2 * (level - 0.5) for level in levels]
    return torch.tensor([row[::-1] if crossing else row] * length, dtype=torch.float32)


class FakePipeline:
    """A Bolt pipeline without weights: one (series, horizon, quantiles) tensor."""

    quantiles = BOLT_QUANTILES
    model_context_length = 2048
    model_prediction_length = 64

    def __init__(self, **attributes):
        self.calls: list = []
        self.model = SimpleNamespace(parameters=lambda: [torch.zeros(1)])
        self.crossing = False
        self.error = None
        self.__dict__.update(attributes)

    def _record(self, inputs, length, levels):
        self.calls.append(
            SimpleNamespace(
                inputs=inputs,
                length=length,
                quantiles=levels,
                locked=plugin._inference_lock(self).locked(),
            )
        )
        if self.error:
            raise self.error

    def predict_quantiles(self, inputs, prediction_length, quantile_levels):
        self._record(inputs, prediction_length, quantile_levels)
        output = torch.stack(
            [flat_forecast(v, prediction_length, quantile_levels, self.crossing) for v in inputs]
        )
        return output, output[..., 0]


class FakeChronos2(FakePipeline, plugin.Chronos2Pipeline):
    """A Chronos-2 pipeline without weights: a (variates, horizon, quantiles) tensor per input."""

    quantiles = [0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99]
    model_context_length = 8192
    model_prediction_length = 1024

    def predict_quantiles(self, inputs, prediction_length, quantile_levels):
        self._record(inputs, prediction_length, quantile_levels)
        matrices = []
        for item in inputs:
            target = item["target"] if isinstance(item, dict) else item
            matrices.append(
                flat_forecast(target, prediction_length, quantile_levels, False).unsqueeze(0)
            )
        return matrices, [matrix[..., 0] for matrix in matrices]


class Hub:
    """Replaces from_pretrained: hands out a pipeline and records what was loaded."""

    def __init__(self):
        self.pipeline = FakePipeline()
        self.loads: list = []
        self.error = None

    def from_pretrained(self, model_id, **kwargs):
        self.loads.append(model_id)
        if self.error:
            raise self.error
        return self.pipeline


@pytest.fixture(autouse=True)
def hub(monkeypatch):
    """The runtime globals, a fixed clock and no state, environment or weights from outside."""
    fake = Hub()
    ticks = itertools.count()
    monkeypatch.setattr(plugin.BaseChronosPipeline, "from_pretrained", fake.from_pretrained)
    monkeypatch.setattr(plugin, "LineBuilder", FakeLineBuilder, raising=False)
    monkeypatch.setattr(
        plugin,
        "_time",
        SimpleNamespace(
            time_ns=lambda: NOW, time=lambda: NOW / SECOND, monotonic=lambda: next(ticks)
        ),
    )
    for name in list(plugin.os.environ):
        if name.startswith(plugin.ENV_PREFIX):
            monkeypatch.delenv(name)
    sys.modules.pop(plugin._STATE_MODULE, None)
    yield fake
    sys.modules.pop(plugin._STATE_MODULE, None)


def history(count=20, step=MINUTE, end=NOW - MINUTE, field="usage", value=float, **tags) -> list:
    """count rows one step apart, the last at end, valued value(0), value(1), ..."""
    first = end - (count - 1) * step
    return [{"time": first + i * step, field: value(i), **tags} for i in range(count)]


def forecast(local, **keys):
    """An on-demand request for cpu.usage, 3 points ahead, with keys added; None removes one."""
    body = {key: value for key, value in {**REQUEST, **keys}.items() if value is not None}
    return plugin.process_request(local, {}, {}, json.dumps(body).encode(), None)


def forecast_query(local, query=QUERY, **keys):
    return forecast(local, source_measurement=None, field=None, query=query, **keys)


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


def model_input(pipeline, index=0, call=0) -> list:
    return pipeline.calls[call].inputs[index].tolist()


def run_scheduled(local, **args):
    base = {"source_measurement": "cpu", "field": "usage", "horizon_steps": "3"}
    return plugin.process_scheduled_call(local, datetime(2027, 1, 15), {**base, **args})


# ---------------------------------------------------------------------------
# Value formats
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, ns",
    [
        ("30s", 30 * SECOND),
        ("5min", 5 * MINUTE),
        ("6h", 6 * 60 * MINUTE),
        ("7d", 7 * 1440 * MINUTE),
        ("1w", 7 * 1440 * MINUTE),
        (" 2h ", 2 * 60 * MINUTE),
    ],
)
def test_duration_units_of_the_contract(raw, ns):
    assert plugin._duration_ns(raw) == ns


@pytest.mark.parametrize("raw", ["1m", "1q", "1y", "0s", "1.5h", "5", "", "h", 5, None])
def test_duration_refuses_anything_else(raw):
    with pytest.raises(ValueError):
        plugin._duration_ns(raw)


@pytest.mark.parametrize(
    "raw, offset",
    [("latest", None), ("now", 0), ("now+1h", 3600 * SECOND), (" now+5min ", 5 * MINUTE)],
)
def test_history_end_forms(raw, offset):
    assert plugin._history_end(raw) == offset


@pytest.mark.parametrize(
    "raw", ["", "Now", "now-1h", "now+", "now+0s", "now+1m", "1h", "tomorrow", 5, None]
)
def test_history_end_refuses_anything_else(raw):
    with pytest.raises(ValueError):
        plugin._history_end(raw)


@pytest.mark.parametrize(
    "ns, text",
    [(86400 * SECOND, "1d"), (90 * SECOND, "90s"), (7 * 86400 * SECOND, "1w"), (5, "5ns")],
)
def test_duration_text_uses_the_largest_unit(ns, text):
    assert plugin._duration_text(ns) == text


@pytest.mark.parametrize(
    "raw, levels",
    [
        ("0.5 0.8", [0.5, 0.8]),
        ([0.8, 0.5, 0.8], [0.5, 0.8]),
        (["0.9"], [0.9]),
        ([], []),
    ],
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
    "raw, names",
    [("a b a", ["a", "b"]), (["a", " b "], ["a", "b"]), ("", [])],
)
def test_names_forms(raw, names):
    assert plugin._names(raw) == names


@pytest.mark.parametrize("raw", [5, ["a", 1], [""], {"a": 1}])
def test_names_refuse_anything_else(raw):
    with pytest.raises(ValueError):
        plugin._names(raw)


@pytest.mark.parametrize("raw", [5, "", "   ", ["a"], None])
def test_text_refuses_anything_but_text(raw):
    with pytest.raises(ValueError):
        plugin._text(raw)


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


def test_quote_ident_escapes_double_quotes():
    assert plugin._quote_ident('a"b') == '"a""b"'


# ---------------------------------------------------------------------------
# Settings sources
# ---------------------------------------------------------------------------


def write_toml(tmp_path, text: str) -> str:
    path = tmp_path / "settings.toml"
    path.write_text(text)
    return str(path)


@pytest.mark.parametrize(
    "sources, winner",
    [
        (("env",), "env"),
        (("env", "args"), "args"),
        (("env", "args", "toml"), "toml"),
        (("env", "args", "toml", "body"), "body"),
        (("env", "args", "toml", "body", "header"), "header"),
        (("env", "args", "toml", "body", "header", "query"), "query"),
    ],
)
def test_each_source_overrides_the_ones_before_it(sources, winner, tmp_path, monkeypatch):
    if "env" in sources:
        monkeypatch.setenv("INFLUXDB3_CHRONOS_FORECASTING_MODEL", "env")
    args = {"model": "args"} if "args" in sources else {}
    if "toml" in sources:
        args["config_file_path"] = write_toml(tmp_path, 'model = "toml"\n')
    body = dict(REQUEST)
    if "body" in sources:
        body["model"] = "body"
    headers = {"X-Influxdb3-Chronos-Forecasting-model": "header"} if "header" in sources else {}
    query = {"model": "query"} if "query" in sources else {}

    response = plugin.process_request(FakeLocal(history()), query, headers, json.dumps(body), args)

    assert answered(response)["model"] == winner


def test_any_key_can_come_from_a_header_or_the_query_string():
    headers = {"x-influxdb3-chronos-forecasting-horizon-steps": "2"}
    query = {"source_measurement": "cpu", "field": "usage", "levels": "0.5"}

    body = answered(plugin.process_request(FakeLocal(history()), query, headers, None, None))

    assert len(body["results"][0]["forecast"]) == 2
    assert body["levels"] == [0.5]


def test_config_file_path_comes_from_the_environment(tmp_path, monkeypatch):
    path = write_toml(tmp_path, 'model = "from-toml"\n')
    monkeypatch.setenv("INFLUXDB3_CHRONOS_FORECASTING_CONFIG_FILE_PATH", path)

    assert answered(forecast(FakeLocal(history())))["model"] == "from-toml"


def test_blank_value_does_not_override_a_lower_source():
    response = plugin.process_request(
        FakeLocal(history()), {}, {}, json.dumps({**REQUEST, "model": " "}), {"model": "args"}
    )

    assert answered(response)["model"] == "args"


def test_request_query_replaces_the_triggers_measurement_keys():
    args = {"source_measurement": "cpu", "field": "usage", "group_by": "host", "horizon_steps": "2"}
    local = FakeLocal(query_rows=history())

    body = answered(plugin.process_request(local, {}, {}, json.dumps({"query": QUERY}), args))

    assert body["results"][0]["field"] == "usage"
    assert local.sent(QUERY)


def test_request_measurement_replaces_the_triggers_query():
    args = {"query": QUERY, "horizon_steps": "2"}
    body = json.dumps({"source_measurement": "cpu", "field": "usage"})
    local = FakeLocal(history())

    answered(plugin.process_request(local, {}, {}, body, args))

    assert not local.sent(QUERY)


def test_request_horizon_replaces_the_triggers_other_horizon_key():
    args = {"horizon_steps": "9", "source_measurement": "cpu", "field": "usage"}
    body = json.dumps({"horizon_duration": "2min"})

    response = plugin.process_request(FakeLocal(history()), {}, {}, body, args)

    assert len(answered(response)["results"][0]["forecast"]) == 2


def test_blank_side_of_a_choice_does_not_win():
    args = {"source_measurement": "cpu", "field": "usage", "horizon_steps": "2"}
    local = FakeLocal(history())

    answered(plugin.process_request(local, {}, {}, json.dumps({"query": " "}), args))

    assert local.history_query()


@pytest.mark.parametrize(
    "value, given",
    [
        ("", False),
        ("  ", False),
        ([], False),
        ({}, False),
        (None, False),
        ("a", True),
        (["a"], True),
        ({"a": 1}, True),
        (0, True),
        (False, True),
    ],
)
def test_what_counts_as_set_in_a_choice(value, given):
    assert plugin._given(value) is given


@pytest.mark.parametrize(
    "keys, fragment",
    [
        ({"query": QUERY}, "either query"),
        ({"source_measurement": None}, "required"),
        ({"field": None}, "required"),
        ({"horizon_steps": None}, "exactly one"),
        ({"horizon_duration": "1h"}, "exactly one"),
        ({"covariate_fields": ["usage"]}, "must not include"),
        ({"series_limit": 101}, "series_limit is 101, more than max_series=100"),
        ({"series_limit": 3, "max_series": 2}, "more than max_series=2"),
        ({"horizon_steps": 1025}, "max_forecast_points=1024"),
        ({"horizon_steps": 4, "max_forecast_points": 3}, "max_forecast_points=3"),
        (
            {"horizon_steps": None, "horizon_duration": "2h", "sampling_interval": "1min",
             "max_forecast_points": 60},
            "the horizon is 120 points",
        ),
        ({"backtest_points": 1025}, "backtest_points is 1025"),
        ({"window": "200000w"}, "years 1 to 9999"),
        ({"history_end": "now+500000w"}, "years 1 to 9999"),
        ({"source_measurement": None, "field": None, "query": QUERY, "history_end": "now+500000w"}, "years 1 to 9999"),
        ({"group_by": ["model"], "write_results": True}, "clashes"),
        ({"group_by": ["forecast"], "write_results": True}, "clashes"),
        ({"group_by": ["forecast_id"], "write_results": True}, "clashes"),
        ({"group_by": ["lower_80"], "write_results": True}, "clashes"),
    ],
)
def test_requests_refused_before_any_query_or_model(keys, fragment, hub):
    local = FakeLocal(history())

    assert fragment in refused(forecast(local, **keys))

    assert local.queries == []
    assert hub.loads == []


@pytest.mark.parametrize(
    "keys",
    [
        {"point_limit": 9},
        {"series_limit": 0},
        {"horizon_steps": 0},
        {"backtest_points": 0},
        {"max_series": 0},
        {"max_forecast_points": 0},
        {"window": "1m"},
        {"sampling_interval": "1q"},
        {"history_end": "tomorrow"},
        {"levels": [1.5]},
        {"write_results": "maybe"},
        {"group_by": 5},
        {"tag_filter": "host"},
        {"model": 5},
    ],
)
def test_invalid_values_are_bad_requests(keys):
    assert refused(forecast(FakeLocal(history()), **keys))


@pytest.mark.parametrize("key", ["group_by", "covariate_fields"])
def test_measurement_keys_do_not_apply_to_a_query(key):
    assert f"{key} does not apply" in refused(forecast_query(FakeLocal(), **{key: ["host"]}))


@pytest.mark.parametrize(
    "keys", [{"tag_filter": {"host": "a"}}, {"sql_filter": "usage > 0"}]
)
def test_filters_do_not_apply_to_a_query(keys):
    assert "does not apply" in refused(forecast_query(FakeLocal(), **keys))


def test_both_series_modes_in_one_source_are_refused():
    args = {"query": QUERY, "horizon_steps": "2"}
    body = json.dumps({"source_measurement": "cpu", "field": "usage", "query": QUERY})

    assert "either query" in refused(plugin.process_request(FakeLocal(), {}, {}, body, args))


def test_empty_list_or_object_is_not_a_side_of_the_choice():
    args = {"query": QUERY, "horizon_steps": "2"}
    body = json.dumps({"group_by": [], "tag_filter": {}})
    local = FakeLocal(query_rows=history())

    answered(plugin.process_request(local, {}, {}, body, args))

    assert local.sent(QUERY)


@pytest.mark.parametrize(
    "call",
    [
        lambda local: plugin.process_request(local, {"nope": "1"}, {}, json.dumps(REQUEST), None),
        lambda local: plugin.process_request(local, {}, {}, json.dumps({**REQUEST, "nope": 1}), None),
        lambda local: plugin.process_request(local, {}, {}, json.dumps(REQUEST), {"nope": "1"}),
        lambda local: plugin.process_request(
            local, {}, {"X-Influxdb3-Chronos-Forecasting-nope": "1"}, json.dumps(REQUEST), None
        ),
        lambda local: plugin.process_request(
            local, {}, {}, json.dumps({**REQUEST, "config_file_path": "a.toml"}), None
        ),
    ],
    ids=["query string", "body", "trigger args", "header", "config_file_path in the body"],
)
def test_unknown_keys_are_refused_in_every_source(call):
    assert refused(call(FakeLocal(history())))


def test_unknown_key_in_toml_is_refused(tmp_path):
    args = {"config_file_path": write_toml(tmp_path, "nope = 1\n")}

    assert refused(plugin.process_request(FakeLocal(), {}, {}, json.dumps(REQUEST), args))


def test_header_with_the_plugins_prefix_in_any_case_must_name_a_key():
    headers = {"x-INFLUXDB3-chronos-forecasting-nope": "1"}

    message = refused(plugin.process_request(FakeLocal(), {}, headers, json.dumps(REQUEST), None))

    assert "unknown header" in message


def test_other_headers_are_ignored():
    headers = {"Host": "x", "User-Agent": "curl"}

    answered(plugin.process_request(FakeLocal(history()), {}, headers, json.dumps(REQUEST), None))


@pytest.mark.parametrize("body", [b"{not json", b"[1, 2]", b"\xff\xfe"])
def test_unreadable_body_is_a_bad_request(body):
    assert refused(plugin.process_request(FakeLocal(), {}, {}, body, None))


def test_missing_toml_file_is_a_bad_request(tmp_path):
    args = {"config_file_path": str(tmp_path / "absent.toml")}

    assert refused(plugin.process_request(FakeLocal(), {}, {}, json.dumps(REQUEST), args))


# ---------------------------------------------------------------------------
# The response
# ---------------------------------------------------------------------------


def test_response_of_one_series():
    local = FakeLocal(history(value=lambda i: 10.0))

    body = answered(forecast(local))

    assert set(body) == {"status", "model", "levels", "series_total", "results"}
    assert body["model"] == plugin.DEFAULT_MODEL
    assert body["levels"] == [0.5, 0.8]
    assert body["series_total"] == 1
    [result] = body["results"]
    assert set(result) == {"field", "tags", "points_read", "step_ms", "forecast"}
    assert result["field"] == "usage"
    assert result["tags"] == {}
    assert result["points_read"] == 20
    assert result["step_ms"] == 60_000
    # The forecast starts one step after the last point, in milliseconds.
    assert [p["timestamp"] for p in result["forecast"]] == [
        NOW // 10**6,
        (NOW + MINUTE) // 10**6,
        (NOW + 2 * MINUTE) // 10**6,
    ]
    point = result["forecast"][0]
    assert set(point) == {"timestamp", "forecast", "lower_50", "upper_50", "lower_80", "upper_80"}
    assert point["forecast"] == pytest.approx(10.0)
    assert point["lower_50"] == pytest.approx(9.5)
    assert point["upper_50"] == pytest.approx(10.5)
    assert point["lower_80"] == pytest.approx(9.2)
    assert point["upper_80"] == pytest.approx(10.8)


def test_all_series_go_to_one_model_call(hub):
    rows = history(value=lambda i: 1.0, host="a") + history(value=lambda i: 2.0, host="b")

    body = answered(forecast(FakeLocal(rows), group_by=["host"]))

    assert len(hub.pipeline.calls) == 1
    assert len(hub.pipeline.calls[0].inputs) == 2
    assert [r["forecast"][0]["forecast"] for r in body["results"]] == [1.0, 2.0]


def test_model_gets_the_quantiles_of_the_levels(hub):
    answered(forecast(FakeLocal(history()), levels=[0.5, 0.8]))

    assert hub.pipeline.calls[0].quantiles == [0.1, 0.25, 0.5, 0.75, 0.9]
    assert hub.pipeline.calls[0].length == 3


def test_model_call_holds_the_inference_lock(hub):
    answered(forecast(FakeLocal(history())))

    assert hub.pipeline.calls[0].locked
    assert not plugin._inference_lock(hub.pipeline).locked()


def test_level_outside_the_trained_quantiles_is_null(hub):
    body = answered(forecast(FakeLocal(history()), levels=[0.8, 0.9]))

    point = body["results"][0]["forecast"][0]
    assert point["lower_90"] is None and point["upper_90"] is None
    assert point["lower_80"] is not None
    assert 0.05 not in hub.pipeline.calls[0].quantiles
    assert body["levels"] == [0.8, 0.9]


def test_a_model_trained_on_wider_quantiles_gives_the_level(hub):
    hub.pipeline = FakeChronos2()

    point = answered(forecast(FakeLocal(history()), levels=[0.975]))["results"][0]["forecast"][0]

    assert point["lower_97.5"] is not None


def test_a_sampling_model_accepts_any_level(hub):
    hub.pipeline = FakePipeline(quantiles=None)

    point = answered(forecast(FakeLocal(history()), levels=[0.99]))["results"][0]["forecast"][0]

    assert point["lower_99"] is not None


def test_empty_levels_give_only_the_forecast():
    point = answered(forecast(FakeLocal(history()), levels=[]))["results"][0]["forecast"][0]

    assert set(point) == {"timestamp", "forecast"}


def test_crossing_quantiles_are_sorted(hub):
    hub.pipeline.crossing = True

    point = answered(forecast(FakeLocal(history())))["results"][0]["forecast"][0]

    assert point["lower_80"] <= point["lower_50"] <= point["forecast"]
    assert point["forecast"] <= point["upper_50"] <= point["upper_80"]


def test_non_finite_values_are_null_and_not_written(hub, monkeypatch):
    def predict(pipeline, inputs, horizon, quantiles):
        return [[[math.nan] * len(quantiles)] * horizon for _ in inputs], 1.0

    monkeypatch.setattr(plugin, "_predict", predict)
    local = FakeLocal(history())

    point = answered(forecast(local, write_results=True))["results"][0]["forecast"][0]

    assert point["forecast"] is None and point["lower_50"] is None
    assert set(local.written[0].fields) == {"step", "points_read", "run_time"}


# ---------------------------------------------------------------------------
# History selection
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "keys, seconds",
    [
        ({}, 86400),
        ({"point_limit": 100}, 86400),
        ({"sampling_interval": "1min"}, 513 * 60),
        ({"sampling_interval": "1min", "point_limit": 100}, 101 * 60),
        ({"sampling_interval": "1min", "backtest_points": 12}, 525 * 60),
        # NOW sits on the edge of a minute, not of a day: the start moves up to the next day's edge.
        ({"sampling_interval": "1d"}, 365 * 86400 - (-(NOW - 365 * 86400 * SECOND) % (86400 * SECOND)) // SECOND),
        ({"window": "6h"}, 6 * 3600),
        ({"window": "6h", "sampling_interval": "1min"}, 6 * 3600),
    ],
)
def test_every_measurement_read_is_bounded_by_time(keys, seconds):
    local = FakeLocal(history())

    answered(forecast(local, **keys))

    for query in (local.history_query(), local.sent("count(*) AS __rows")[0]):
        assert "time >= $history_start" in query.sql
        assert query.params["history_start"] == plugin._iso(NOW - seconds * SECOND)
        # history_end latest leaves the end open.
        assert "$history_end" not in query.sql


@pytest.mark.parametrize(
    "keys, limit",
    [
        ({}, 512),
        ({"point_limit": 30}, 30),
        ({"point_limit": 5000}, 2048),
        ({"window": "1d"}, 2048),
        # One bucket more, as a last bucket still filling is dropped.
        ({"point_limit": 30, "sampling_interval": "1min"}, 31),
        ({"point_limit": 30, "sampling_interval": "1min", "history_end": "now+1h"}, 31),
        ({"point_limit": 30, "sampling_interval": "1min", "history_end": "now"}, 31),
        ({"point_limit": 30, "backtest_points": 5}, 35),
    ],
)
def test_history_read_is_limited_per_series(keys, limit):
    local = FakeLocal(history())

    answered(forecast(local, **keys))

    assert f"LIMIT {limit}" in local.history_query().sql


def test_group_by_limits_each_series_on_its_own():
    rows = history(40, host="a") + history(40, host="b")
    local = FakeLocal(rows)

    body = answered(forecast(local, group_by=["host"], point_limit=15))

    assert "__rn <= 15" in local.history_query().sql
    assert [r["points_read"] for r in body["results"]] == [15, 15]


def test_a_model_without_a_known_context_reads_the_fallback(hub):
    hub.pipeline = FakePipeline(model_context_length=None)
    local = FakeLocal(history())

    answered(forecast(local, window="1d"))

    assert f"LIMIT {plugin.FALLBACK_CONTEXT}" in local.history_query().sql


def test_the_last_points_reach_the_model(hub):
    answered(forecast(FakeLocal(history(30)), point_limit=10))

    assert model_input(hub.pipeline) == [float(i) for i in range(20, 30)]


# ---------------------------------------------------------------------------
# Where history ends
# ---------------------------------------------------------------------------


NOW_0840 = NOW + 40 * MINUTE
HOUR = 60 * MINUTE


@pytest.fixture
def at_0840(monkeypatch):
    """A clock inside an hour's bucket, as NOW sits on its edge."""
    monkeypatch.setattr(plugin._time, "time_ns", lambda: NOW_0840)


def rows_with_a_fast_clock() -> list:
    """Rows every 5 minutes up to 08:35, and one at 09:10 from a device whose clock runs fast."""
    rows = history(288, step=5 * MINUTE, end=NOW + 35 * MINUTE, value=lambda i: 20.0)
    return [*rows, {"time": NOW + 70 * MINUTE, "usage": 500.0}]


def first_time(result: dict) -> int:
    return result["forecast"][0]["timestamp"] * 10**6


def test_iso_is_a_utc_time_the_timestamps_read_back():
    assert plugin._iso(SECOND) == "1970-01-01T00:00:01+00:00"
    assert plugin._iso(SECOND + 5000) == "1970-01-01T00:00:01.000005+00:00"
    assert plugin._to_ns(plugin._iso(NOW)) == NOW


def test_by_default_history_reaches_the_latest_row(at_0840, hub):
    local = FakeLocal(rows_with_a_fast_clock())

    result = answered(forecast(local))["results"][0]

    assert "$history_end" not in local.history_query().sql
    assert max(model_input(hub.pipeline)) == 500.0
    assert first_time(result) == NOW + 75 * MINUTE


def test_history_end_now_leaves_out_a_row_from_the_future(at_0840, hub):
    local = FakeLocal(rows_with_a_fast_clock())

    result = answered(forecast(local, history_end="now"))["results"][0]

    assert local.history_query().params["history_end"] == plugin._iso(NOW_0840)
    assert max(model_input(hub.pipeline)) == 20.0
    assert first_time(result) == NOW + 40 * MINUTE


@pytest.mark.parametrize(
    "history_end, end, peak",
    [("now+15min", NOW + 55 * MINUTE, 20.0), ("now+1h", NOW + 100 * MINUTE, 500.0)],
)
def test_history_end_reaches_that_long_past_now(history_end, end, peak, at_0840, hub):
    local = FakeLocal(rows_with_a_fast_clock())

    answered(forecast(local, history_end=history_end))

    assert local.history_query().params["history_end"] == plugin._iso(end)
    assert max(model_input(hub.pipeline)) == peak


@pytest.mark.parametrize(
    "history_end, end, first",
    [
        # The read reaches the 09:00 bucket, which ends after now and is dropped; 08:00 stays.
        (None, None, NOW + HOUR),
        ("now+2h", NOW + 2 * HOUR, NOW + HOUR),
        # The read ends at 09:00 or earlier; the 08:00 bucket is the last and ends after now.
        ("now+1h", NOW + HOUR, NOW),
        # The read ends at 08:00, so every bucket read has ended.
        ("now", NOW, NOW),
    ],
)
def test_with_sampling_a_last_bucket_ending_after_now_is_left_out(
    history_end, end, first, at_0840, hub
):
    local = FakeLocal(rows_with_a_fast_clock())

    body = answered(forecast(local, sampling_interval="1h", history_end=history_end))

    assert local.history_query().params.get("history_end") == (plugin._iso(end) if end else None)
    assert max(model_input(hub.pipeline)) == 20.0
    assert first_time(body["results"][0]) == first


def test_a_window_starts_at_a_bucket_edge_inside_it(at_0840, hub):
    """window=24h at 08:40 reads from 09:00 yesterday, not 08:40, so the first bucket is whole."""
    rows = history(300, step=5 * MINUTE, end=NOW + 35 * MINUTE, value=lambda i: 20.0)
    rows += [{"time": NOW - 23 * HOUR - 10 * MINUTE, "usage": 500.0}]  # 08:50 yesterday, inside 24h
    local = FakeLocal(rows)

    result = answered(forecast(local, window="24h", sampling_interval="1h"))["results"][0]

    assert local.history_query().params["history_start"] == plugin._iso(NOW - 23 * HOUR)
    assert result["points_read"] == 23
    assert max(model_input(hub.pipeline)) == 20.0


def test_only_the_last_bucket_is_dropped(at_0840, hub):
    """A bucket holding now stays when a later one follows: a fast clock has filled it."""
    rows = history(144, step=5 * MINUTE, end=NOW + 55 * MINUTE, value=lambda i: 20.0)
    rows += [{"time": NOW + 65 * MINUTE, "usage": 500.0}]

    result = answered(forecast(FakeLocal(rows), sampling_interval="1h"))["results"][0]

    assert max(model_input(hub.pipeline)) == 20.0
    assert first_time(result) == NOW + HOUR
    assert result["points_read"] == 12


def test_a_bucket_that_closed_while_the_model_loaded_is_kept(hub):
    """The clock is read when the buckets are checked, not when the request was resolved."""
    clocks = itertools.chain([NOW - 10 * SECOND], itertools.repeat(NOW + 10 * SECOND))
    plugin._time.time_ns = lambda: next(clocks)
    rows = history(144, step=5 * MINUTE, end=NOW - 5 * MINUTE, value=lambda i: 20.0)

    result = answered(forecast(FakeLocal(rows), sampling_interval="1h"))["results"][0]

    # Requested at 07:59:50 and read at 08:00:10: the 07:00 bucket has ended by then.
    assert first_time(result) == NOW
    assert result["points_read"] == 12


def test_history_end_now_holds_out_no_row_from_the_future_for_the_backtest(at_0840, hub):
    local = FakeLocal(rows_with_a_fast_clock())

    result = answered(forecast(local, history_end="now", backtest_points=3))["results"][0]

    assert result["backtest"]["points"] == 3
    assert result["backtest"]["mae"] == pytest.approx(0.0)
    assert max(model_input(hub.pipeline)) == 20.0


@pytest.mark.parametrize("history_end, hosts", [(None, ["a", "b"]), ("now", ["a"])])
def test_a_series_wholly_in_the_future_exists_only_when_read(history_end, hosts):
    rows = history(host="a") + history(end=NOW + 20 * HOUR, host="b")

    body = answered(forecast(FakeLocal(rows), group_by=["host"], history_end=history_end))

    assert [r["tags"]["host"] for r in body["results"]] == hosts
    assert body["series_total"] == len(hosts)


def test_mixed_tag_sets_in_the_future_count_only_when_read():
    rows = history(host="a") + history(5, end=NOW + 10 * HOUR, host="a")
    rows += history(5, end=NOW + 10 * HOUR, host="b")

    assert "several tag sets" in refused(forecast(FakeLocal(rows)))
    assert answered(forecast(FakeLocal(rows), history_end="now"))["results"][0]["points_read"] == 20


def test_a_query_keeps_its_rows_from_the_future(at_0840, hub):
    result = answered(forecast_query(FakeLocal(query_rows=rows_with_a_fast_clock())))["results"][0]

    assert max(model_input(hub.pipeline)) == 500.0
    assert first_time(result) > NOW + 70 * MINUTE


def test_a_query_with_a_window_keeps_its_rows_from_the_future(at_0840, hub):
    answered(forecast_query(FakeLocal(query_rows=rows_with_a_fast_clock()), window="1d"))

    assert max(model_input(hub.pipeline)) == 500.0


def test_history_end_now_leaves_out_a_querys_rows_from_the_future(at_0840, hub):
    local = FakeLocal(query_rows=rows_with_a_fast_clock())

    result = answered(forecast_query(local, history_end="now"))["results"][0]

    assert max(model_input(hub.pipeline)) == 20.0
    assert first_time(result) == NOW + 40 * MINUTE


@pytest.mark.parametrize("history_end, first", [(None, NOW + HOUR), ("now", NOW)])
def test_a_query_with_sampling_leaves_out_the_bucket_still_filling(
    history_end, first, at_0840, hub
):
    local = FakeLocal(query_rows=rows_with_a_fast_clock())

    body = answered(forecast_query(local, sampling_interval="1h", history_end=history_end))

    assert max(model_input(hub.pipeline)) == 20.0
    assert first_time(body["results"][0]) == first


def test_a_query_series_wholly_after_the_history_end_does_not_exist(at_0840):
    rows = history(end=NOW + 10 * HOUR, host="b")

    body = answered(forecast_query(FakeLocal(query_rows=rows), history_end="now"))

    assert body["series_total"] == 0


def test_a_window_inside_the_bucket_still_filling_finds_no_rows(at_0840, hub):
    local = FakeLocal(rows_with_a_fast_clock())

    keys = {"window": "30min", "sampling_interval": "1h", "history_end": "now"}
    result = answered(forecast(local, **keys))["results"][0]

    assert result["code"] == "too_few_points"
    assert hub.loads == []
    params = local.sent("count(*) AS __rows")[0].params
    assert plugin._to_ns(params["history_start"]) < plugin._to_ns(params["history_end"])


@pytest.mark.parametrize(
    "keys",
    [
        {"window": "1s"},
        {"window": "1min", "sampling_interval": "1min"},
        {"window": "30min", "sampling_interval": "1h"},
        {"window": "1h", "sampling_interval": "1h"},
        {"point_limit": 10, "sampling_interval": "1d"},
        {"sampling_interval": "2w"},
    ],
)
@pytest.mark.parametrize("history_end", ["now", "now+1min"])
def test_every_read_range_holds_something(keys, history_end, at_0840):
    """The server answers an error to a range whose start is not before its end."""
    local = FakeLocal(history())

    forecast(local, history_end=history_end, **keys)

    for query in local.sent("$history_start"):
        start, end = query.params["history_start"], query.params["history_end"]
        assert plugin._to_ns(start) < plugin._to_ns(end)


# ---------------------------------------------------------------------------
# Series
# ---------------------------------------------------------------------------


def test_no_history_without_group_by_answers_its_one_series(hub):
    body = answered(forecast(FakeLocal()))

    assert body["series_total"] == 1
    assert body["results"] == [
        {
            "field": "usage",
            "tags": {},
            "status": "error",
            "code": "too_few_points",
            "message": "need >= 10 points, got 0 in the last 1d; set window or sampling_interval",
        }
    ]
    assert hub.loads == []


def test_no_history_with_group_by_has_no_series(hub):
    local = FakeLocal()

    body = answered(forecast(local, group_by=["host"]))

    assert body["series_total"] == 0
    assert body["results"] == []
    assert hub.loads == []
    assert not local.sent(" AS __t,")


@pytest.mark.parametrize(
    "keys, message",
    [
        ({"window": "6h"}, "need >= 10 points, got 4 in the last 6h"),
        ({}, "need >= 10 points, got 4 in the last 1d; set window or sampling_interval"),
        (
            {"sampling_interval": "1min"},
            "need >= 10 points, got 4 in the last 513min; "
            "set window, raise point_limit or shorten sampling_interval",
        ),
        # The window is too short for the buckets, whatever the data; NOW is on an hour's edge.
        (
            {"window": "30min", "sampling_interval": "1h", "history_end": "now"},
            "need >= 10 points, got 0 in the last 30min; the window fits 0 whole 1h buckets",
        ),
        (
            {"window": "3h", "sampling_interval": "1h"},
            "need >= 10 points, got 1 in the last 3h; the window fits 3 whole 1h buckets",
        ),
        (
            {"window": "1h", "sampling_interval": "1h"},
            "need >= 10 points, got 1 in the last 1h; the window fits 1 whole 1h bucket",
        ),
        # A window that holds enough buckets gets no hint about them.
        ({"window": "1d", "sampling_interval": "1h"}, "need >= 10 points, got 1 in the last 1d"),
    ],
)
def test_too_few_points_says_what_was_read(keys, message, hub):
    body = answered(forecast(FakeLocal(history(4)), **keys))

    assert body["results"][0]["message"] == message
    assert hub.loads == []


def test_a_short_series_does_not_blank_the_others(hub):
    rows = history(20, host="a") + history(4, host="b")

    body = answered(forecast(FakeLocal(rows), group_by=["host"]))

    good, short = body["results"]
    assert "forecast" in good
    assert (short["status"], short["code"]) == ("error", "too_few_points")
    assert set(short) == {"field", "tags", "status", "code", "message"}
    assert len(hub.pipeline.calls[0].inputs) == 1


def test_results_are_ordered_with_a_missing_value_last():
    rows = (
        history(host="b", region="eu")
        + history(region="eu")
        + history(host="a", region="us")
        + history(host="a", region="eu")
    )

    body = answered(forecast(FakeLocal(rows), group_by=["host", "region"]))

    assert [r["tags"] for r in body["results"]] == [
        {"host": "a", "region": "eu"},
        {"host": "a", "region": "us"},
        {"host": "b", "region": "eu"},
        {"region": "eu"},
    ]


def test_series_tags_are_those_with_one_value_on_every_row():
    half = NOW - 11 * MINUTE
    rows = (
        history(host="a", region="eu")
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


def test_tag_pinned_by_a_filter_is_a_series_tag():
    body = answered(forecast(FakeLocal(history(host="a")), tag_filter={"host": "a"}))

    assert body["results"][0]["tags"] == {"host": "a"}


def test_series_limit_keeps_the_first_series_and_counts_them_all(hub):
    rows = history(host="c") + history(host="a") + history(host="b")
    local = FakeLocal(rows)

    body = answered(forecast(local, group_by=["host"], series_limit=2))

    assert [r["tags"]["host"] for r in body["results"]] == ["a", "b"]
    assert body["series_total"] == 3
    assert len(hub.pipeline.calls[0].inputs) == 2
    assert any("series_limit=2 keeps 2 of 3" in text for text in local.messages("info"))


def test_series_limit_narrows_the_following_reads():
    rows = history(host="c") + history(host="a") + history(host="b")
    local = FakeLocal(rows)

    answered(forecast(local, group_by=["host"], series_limit=2))

    series = local.sent("count(*) AS __rows")[0].sql
    assert "LIMIT 2" in series and "count(*) OVER () AS __total" in series
    for query in (local.sent("AS __first")[0], local.history_query()):
        assert '"host" = $sl0_0' in query.sql and '"host" = $sl1_0' in query.sql
        assert (query.params["sl0_0"], query.params["sl1_0"]) == ("a", "b")


def test_series_limit_that_leaves_nothing_out_does_not_narrow_the_reads():
    local = FakeLocal(history(host="a") + history())

    answered(forecast(local, group_by=["host"], series_limit=2))

    assert "$sl" not in local.history_query().sql


def test_chosen_series_matches_a_missing_value():
    req = SimpleNamespace(group_by=["host", "region"])
    params: dict = {}

    condition = plugin._chosen_series(req, [(("a", None), {}, 1)], params)

    assert condition == '(("host" = $sl0_0 AND "region" IS NULL))'
    assert params == {"sl0_0": "a"}


def test_series_limit_above_the_series_count_changes_nothing():
    body = answered(forecast(FakeLocal(history(host="a")), group_by=["host"], series_limit=5))

    assert body["series_total"] == 1
    assert len(body["results"]) == 1


def test_series_limit_without_group_by_keeps_the_one_series():
    body = answered(forecast(FakeLocal(history()), series_limit=1))

    assert body["series_total"] == 1
    assert "forecast" in body["results"][0]


def test_too_short_series_keeps_its_slot_under_series_limit():
    rows = history(4, host="a") + history(host="b") + history(host="c")

    body = answered(forecast(FakeLocal(rows), group_by=["host"], series_limit=2))

    assert [r["tags"]["host"] for r in body["results"]] == ["a", "b"]
    assert body["results"][0]["code"] == "too_few_points"


def test_more_series_than_the_cap_are_refused_before_the_model(hub):
    rows = history(host="a") + history(host="b") + history(host="c")
    local = FakeLocal(rows)

    assert "max_series=2" in refused(forecast(local, group_by=["host"], max_series=2))

    assert hub.loads == []
    assert not local.sent(" AS __t,")


def test_series_limit_never_hits_the_cap():
    rows = history(host="a") + history(host="b") + history(host="c")

    body = answered(forecast(FakeLocal(rows), group_by=["host"], max_series=2, series_limit=2))

    assert len(body["results"]) == 2
    assert body["series_total"] == 3


@pytest.mark.parametrize(
    "keys, fragment",
    [
        ({"source_measurement": "absent"}, "cannot read measurement"),
        ({"field": "absent"}, "not a numeric field"),
        ({"field": "status"}, "not a numeric field"),
        ({"field": "host"}, "not a numeric field"),
        ({"group_by": ["usage"]}, "not a tag"),
        ({"group_by": ["absent"]}, "not a tag"),
        ({"tag_filter": {"usage": "1"}}, "not a tag"),
        ({"covariate_fields": ["status"]}, "not a numeric field"),
    ],
)
def test_columns_are_checked_against_the_schema(keys, fragment, hub):
    local = FakeLocal(history())

    assert fragment in refused(forecast(local, **keys))

    assert hub.loads == []


def test_schema_failure_is_a_bad_request():
    local = FakeLocal(fail={"information_schema": RuntimeError("no such database")})

    assert "no such database" in refused(forecast(local, source_database="absent"))


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

    where = local.history_query().sql
    assert '"host" = $tf0_0 AND (usage > 0 OR temp > 1)' in where


@pytest.mark.parametrize(
    "keys",
    [{}, {"source_measurement": None, "field": None, "query": QUERY}],
    ids=["measurement", "query"],
)
def test_every_read_goes_to_the_source_database(keys):
    local = FakeLocal(history(), query_rows=history())

    answered(forecast(local, source_database="other", **keys))

    assert local.queries
    assert {query.database for query in local.queries} == {"other"}


def test_without_source_database_reads_go_to_the_triggers():
    local = FakeLocal(history())

    answered(forecast(local))

    assert {query.database for query in local.queries} == {None}


def test_bad_sql_filter_is_a_bad_request(hub):
    local = FakeLocal(history(), fail={"count(*) AS __rows": RuntimeError("no column x")})

    assert "no column x" in refused(forecast(local, sql_filter="x > 0"))
    assert hub.loads == []


def test_failed_history_query_is_internal():
    local = FakeLocal(history(), fail={" AS __t,": RuntimeError("boom")})

    assert "history query failed: boom" in failed(forecast(local), "internal", 500)


def test_a_history_row_without_a_value_or_a_time_is_skipped():
    class Holes(FakeLocal):
        def _history(self, sql):
            holes = [{"__t": NOW, "__f0": None}, {"__t": None, "__f0": 1.0}]
            return super()._history(sql) + holes

    assert answered(forecast(Holes(history())))["results"][0]["points_read"] == 20


# ---------------------------------------------------------------------------
# Mixed series
# ---------------------------------------------------------------------------


def test_tag_sets_writing_at_the_same_time_are_refused(hub):
    local = FakeLocal(history(host="a") + history(host="b"))

    message = refused(forecast(local))

    assert "differ in host" in message
    assert hub.loads == []


def test_the_message_names_only_the_tags_that_differ_and_the_series():
    rows = history(host="a", region="eu") + history(host="a", region="us")

    message = refused(forecast(FakeLocal(rows), group_by=["host"]))

    assert "differ in region" in message
    assert "{'host': 'a'}" in message


def test_a_tag_set_following_another_is_one_series():
    rows = history(10, end=NOW - 11 * MINUTE, host="a") + history(10, host="b")

    body = answered(forecast(FakeLocal(rows)))

    assert body["results"][0]["points_read"] == 20
    assert body["results"][0]["tags"] == {}


def test_a_tag_set_starting_where_another_ends_is_one_series():
    rows = history(10, end=NOW - 10 * MINUTE, host="a") + history(10, host="b")

    assert answered(forecast(FakeLocal(rows)))["results"][0]["points_read"] == 19


def test_group_by_separates_tag_sets():
    answered(forecast(FakeLocal(history(host="a") + history(host="b")), group_by=["host"]))


def test_sampling_interval_averages_tag_sets_instead(hub):
    rows = history(value=lambda i: 1.0, host="a") + history(value=lambda i: 3.0, host="b")
    local = FakeLocal(rows)

    answered(forecast(local, sampling_interval="1min"))

    assert not local.sent("AS __first")
    assert model_input(hub.pipeline)[-1] == pytest.approx(2.0)


def test_overlap_older_than_the_history_read_does_not_count():
    old = NOW - 60 * MINUTE
    rows = history(10, host="a") + history(5, end=old, host="a") + history(5, end=old, host="b")

    answered(forecast(FakeLocal(rows), point_limit=10))
    assert refused(forecast(FakeLocal(rows), window="1d"))


def test_a_measurement_without_other_tags_has_nothing_to_check():
    schema = {"time": SCHEMA["time"], "host": TAG, "usage": "Float64"}
    local = FakeLocal(history(host="a"), schema=schema)

    answered(forecast(local, group_by=["host"]))

    assert not local.sent("AS __first")


def test_too_many_tag_sets_are_refused(monkeypatch):
    monkeypatch.setattr(plugin, "_MAX_TAG_SETS", 2)
    rows = [row for host in "abc" for row in history(host=host)]

    assert "more than 2 tag sets" in refused(forecast(FakeLocal(rows)))


def test_failed_tag_set_query_is_a_bad_request():
    local = FakeLocal(history(), fail={"AS __first": RuntimeError("boom")})

    assert "cannot read the series" in refused(forecast(local))


# ---------------------------------------------------------------------------
# The grid
# ---------------------------------------------------------------------------


def regularized(times, values=None, limit=100, sampling_ns=None):
    values = [float(i) for i in range(len(times))] if values is None else values
    series = plugin.Series(field="usage", tags={}, times=list(times), values=list(values))
    plugin._regularize(series, SimpleNamespace(sampling_ns=sampling_ns), limit)
    return series


def test_even_points_keep_their_times():
    times = [NOW + i * MINUTE for i in range(5)]

    series = regularized(times)

    assert series.times == times
    assert series.step_ns == MINUTE
    assert series.values == [0.0, 1.0, 2.0, 3.0, 4.0]


def test_jitter_does_not_move_the_grid_between_runs():
    grid = [NOW + i * MINUTE for i in range(10)]
    runs = (
        [30, -20, 10, -40, 20, 0, -10, 40, -30, 15],
        [-40, 20, -5, 30, -25, 35, 5, -15, 25, -35],
    )

    for jitter_ms in runs:
        times = [moment + jitter * 10**6 for moment, jitter in zip(grid, jitter_ms)]
        assert regularized(times).times == grid


def test_a_stray_timestamp_does_not_shift_the_grid():
    grid = [NOW + i * MINUTE for i in range(20)]
    stray = grid[10] + 17 * SECOND
    times = sorted([*grid, stray])
    values = [100.0 if moment == stray else 1.0 for moment in times]

    series = regularized(times, values)

    assert series.times == grid
    assert series.step_ns == MINUTE
    # It lands in the nearest cell and is averaged there.
    assert series.values[10] == pytest.approx(50.5)
    assert series.values[11] == pytest.approx(1.0)


def test_a_stray_last_timestamp_does_not_shift_the_grid():
    grid = [NOW + i * MINUTE for i in range(20)]

    series = regularized([*grid[:-1], grid[-1] + 17 * SECOND])

    assert series.times == grid


def test_points_in_one_cell_are_averaged():
    series = regularized([NOW, NOW, NOW + MINUTE, NOW + 2 * MINUTE], [1.0, 3.0, 5.0, 7.0])

    assert series.values == [2.0, 5.0, 7.0]


def test_an_empty_cell_is_a_gap():
    times = [NOW + i * MINUTE for i in (0, 1, 2, 5, 6, 7, 8)]

    series = regularized(times)

    assert len(series.times) == 9
    assert math.isnan(series.values[3]) and math.isnan(series.values[4])
    assert series.values[5] == 3.0


def test_only_the_last_cells_are_kept():
    series = regularized([NOW + i * MINUTE for i in range(10)], limit=3)

    assert series.values == [7.0, 8.0, 9.0]


def test_a_single_point_gets_the_fallback_step():
    series = regularized([NOW])

    assert series.times == [NOW]
    assert series.step_ns == plugin.FALLBACK_STEP_NS


def test_a_series_without_points_gets_a_step():
    assert regularized([]).step_ns == plugin.FALLBACK_STEP_NS
    assert regularized([], sampling_ns=MINUTE).step_ns == MINUTE


def test_sampling_grid_is_its_buckets_with_gaps():
    times = [NOW - 10 * MINUTE, NOW - 9 * MINUTE, NOW - 6 * MINUTE]

    series = regularized(times, sampling_ns=MINUTE)

    assert series.times == [NOW - (10 - i) * MINUTE for i in range(5)]
    assert series.step_ns == MINUTE
    assert math.isnan(series.values[2]) and math.isnan(series.values[3])


def test_a_bucket_just_finished_is_kept():
    result = answered(forecast(FakeLocal(history(12)), sampling_interval="1min"))["results"][0]

    assert result["points_read"] == 12
    assert result["forecast"][0]["timestamp"] == NOW // 10**6


@pytest.mark.parametrize(
    "times, step",
    [
        ([0, 10, 20, 100], 10),
        ([0, 10, 30, 60, 100], 25),
        ([5, 5, 5], plugin.FALLBACK_STEP_NS),
        ([5], plugin.FALLBACK_STEP_NS),
    ],
)
def test_median_step(times, step):
    assert plugin._median_step_ns(times) == step


@pytest.mark.parametrize(
    "gap, step",
    [
        (10_020_000_000, 10 * SECOND),
        (604800 * SECOND, 604800 * SECOND),
        (604800 * SECOND + 1, 604800 * SECOND),
        (37, 37),
    ],
)
def test_round_step_absorbs_jitter_about_one_percent(gap, step):
    assert plugin._round_step(gap) == step


def test_bucket_means_are_aligned_to_the_epoch():
    series = plugin.Series(
        field="usage", tags={}, times=[61 * SECOND, 62 * SECOND, 125 * SECOND], values=[1.0, 3.0, 5.0]
    )

    plugin._bucket(series, MINUTE)

    assert series.times == [60 * SECOND, 120 * SECOND]
    assert series.values == [2.0, 5.0]


def test_mean_skips_gaps_and_answers_nan_when_empty():
    assert plugin._mean([1.0, math.nan, None, 3.0], [0, 1, 2, 3]) == 2.0
    assert math.isnan(plugin._mean([math.nan], [0]))
    assert math.isnan(plugin._mean([1.0], ()))


def test_sampling_reaches_the_model_as_means_and_gaps(hub):
    rows = history(40, step=30 * SECOND, value=lambda i: float(i))
    rows = [row for row in rows if not NOW - 8 * MINUTE <= row["time"] < NOW - 6 * MINUTE]

    result = answered(forecast(FakeLocal(rows), sampling_interval="1min"))["results"][0]

    values = model_input(hub.pipeline)
    assert sum(math.isnan(value) for value in values) == 2
    assert result["step_ms"] == 60_000
    assert result["points_read"] == len(values) - 2
    assert result["forecast"][0]["timestamp"] == NOW // 10**6


def test_the_forecast_after_a_dropped_bucket_starts_at_that_bucket():
    result = answered(forecast(FakeLocal(history(12, end=NOW)), sampling_interval="1min"))[
        "results"
    ][0]

    assert result["points_read"] == 11
    assert result["forecast"][0]["timestamp"] == NOW // 10**6


def test_points_read_counts_cells_that_hold_data():
    rows = [row for i, row in enumerate(history(20)) if i not in (5, 6)]

    result = answered(forecast(FakeLocal(rows)))["results"][0]

    assert result["points_read"] == 18


# ---------------------------------------------------------------------------
# The horizon
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "duration, points", [("10min", 2), ("7min", 2), ("1min", 1), ("5min", 1), ("6min", 2)]
)
def test_horizon_duration_rounds_up_to_whole_steps(duration, points):
    rows = history(step=5 * MINUTE)

    body = answered(forecast(FakeLocal(rows), horizon_steps=None, horizon_duration=duration))

    assert len(body["results"][0]["forecast"]) == points


def test_horizon_points_stay_exact_at_large_values():
    req = SimpleNamespace(horizon_steps=None, horizon_ns=(2**53 + 1) * 1000)

    assert plugin._horizon_points(req, 1000) == 2**53 + 1


def test_each_series_gets_the_horizon_at_its_own_spacing(hub):
    rows = history(host="a") + history(step=5 * MINUTE, host="b")

    body = answered(
        forecast(FakeLocal(rows), group_by=["host"], horizon_steps=None, horizon_duration="10min")
    )

    assert [len(r["forecast"]) for r in body["results"]] == [10, 2]
    assert hub.pipeline.calls[0].length == 10


def test_a_horizon_over_the_cap_at_the_datas_spacing_is_refused():
    response = forecast(
        FakeLocal(history()),
        horizon_steps=None,
        horizon_duration="1h",
        max_forecast_points=30,
    )

    assert "the horizon is 60 points" in refused(response)


def test_horizon_past_the_native_length_warns():
    local = FakeLocal(history())

    answered(forecast(local, horizon_steps=65))

    assert any("past the model's native 64" in text for text in local.messages("warn"))


def test_horizon_within_the_native_length_does_not_warn():
    local = FakeLocal(history())

    answered(forecast(local, horizon_steps=64))

    assert local.messages("warn") == []


def test_a_forecast_wholly_in_the_past_warns():
    local = FakeLocal(history(end=NOW - 10 * 1440 * MINUTE))

    answered(forecast(local, window="30d"))

    assert any("whole forecast lies in the past" in text for text in local.messages("warn"))


# ---------------------------------------------------------------------------
# Loading the model
# ---------------------------------------------------------------------------


def test_a_loaded_model_is_reused(hub):
    answered(forecast(FakeLocal(history())))
    answered(forecast(FakeLocal(history())))

    assert hub.loads == [plugin.DEFAULT_MODEL]


def test_the_least_recently_used_model_is_dropped(hub):
    def use(model):
        answered(forecast(FakeLocal(history()), model=model))

    use("a")
    use("b")
    use("a")
    use("c")

    assert set(plugin._shared_state().pipelines) == {"a", "c"}
    assert "b" not in plugin._shared_state().last_used
    use("a")
    assert hub.loads == ["a", "b", "c"]


def test_state_created_without_last_used_is_completed():
    state = types.ModuleType(plugin._STATE_MODULE)
    state.load_lock, state.pipelines, state.inference_locks = None, {}, {}
    sys.modules[plugin._STATE_MODULE] = state

    assert plugin._shared_state().last_used == {}


def test_a_failed_load_is_model_unavailable_and_not_remembered(hub):
    hub.error = OSError("no such repo")

    message = failed(forecast(FakeLocal(history()), model="nope"), "model_unavailable", 503)

    assert "no such repo" in message
    assert plugin._shared_state().pipelines == {}
    assert plugin._shared_state().last_used == {}

    hub.error = None
    answered(forecast(FakeLocal(history()), model="nope"))
    assert hub.loads == ["nope", "nope"]


def test_weights_left_on_the_meta_device_are_model_unavailable(hub):
    hub.pipeline.model = SimpleNamespace(parameters=lambda: [torch.zeros(1, device="meta")])

    assert "restart InfluxDB" in failed(forecast(FakeLocal(history())), "model_unavailable", 503)
    assert plugin._shared_state().pipelines == {}


def test_failed_inference_is_internal(hub):
    hub.pipeline.error = RuntimeError("out of memory")

    message = failed(forecast(FakeLocal(history())), "internal", 500)

    assert "inference failed: out of memory" in message


def test_the_model_loads_only_when_a_series_has_enough_rows(hub):
    answered(forecast(FakeLocal(history(9))))
    assert hub.loads == []

    answered(forecast(FakeLocal(history(10))))
    assert hub.loads == [plugin.DEFAULT_MODEL]


def test_rows_arriving_after_the_count_still_forecast(hub):
    local = FakeLocal(history(5, end=NOW - 60 * MINUTE))
    local.before_history = lambda: local.rows.extend(history(20))

    result = answered(forecast(local))["results"][0]

    assert hub.loads == [plugin.DEFAULT_MODEL]
    assert "forecast" in result


def test_a_model_loaded_after_the_read_cuts_the_history_to_its_context(hub):
    hub.pipeline = FakePipeline(model_context_length=15)
    local = FakeLocal(history(5, end=NOW - 60 * MINUTE))
    local.before_history = lambda: local.rows.extend(history(40))

    result = answered(forecast(local, window="1d"))["results"][0]

    assert result["points_read"] == 15
    assert len(model_input(hub.pipeline)) == 15


def test_a_query_loads_the_model_only_when_a_series_has_enough_rows(hub):
    answered(forecast_query(FakeLocal(query_rows=history(9))))
    assert hub.loads == []

    answered(forecast_query(FakeLocal(query_rows=history(10))))
    assert hub.loads == [plugin.DEFAULT_MODEL]


# ---------------------------------------------------------------------------
# Covariates
# ---------------------------------------------------------------------------


def test_covariates_need_a_chronos2_model(hub):
    message = refused(forecast(FakeLocal(history()), covariate_fields=["temp"]))

    assert "requires a Chronos-2 model" in message


def test_covariates_reach_chronos2_beside_the_target(hub):
    hub.pipeline = FakeChronos2()
    rows = [{**row, "temp": row["usage"] * 2} for row in history(12)]
    local = FakeLocal(rows)

    result = answered(forecast(local, covariate_fields=["temp"]))["results"][0]

    item = hub.pipeline.calls[0].inputs[0]
    assert item["target"].tolist() == [float(i) for i in range(12)]
    assert item["past_covariates"]["temp"].tolist() == [float(i * 2) for i in range(12)]
    assert 'avg("temp") AS __f1' in local.history_query().sql
    assert result["forecast"][0]["forecast"] == pytest.approx(11.0)


def test_a_covariate_without_data_is_passed_as_missing_with_a_warning(hub):
    hub.pipeline = FakeChronos2()
    local = FakeLocal(history(12))

    answered(forecast(local, covariate_fields=["temp"]))

    values = hub.pipeline.calls[0].inputs[0]["past_covariates"]["temp"].tolist()
    assert all(math.isnan(value) for value in values)
    assert any("Covariate 'temp' has no data" in text for text in local.messages("warn"))


def test_covariates_are_cut_with_the_backtest(hub):
    hub.pipeline = FakeChronos2()
    rows = [{**row, "temp": 1.0} for row in history(30)]

    answered(forecast(FakeLocal(rows), covariate_fields=["temp"], backtest_points=5))

    forecast_input, check_input = hub.pipeline.calls[0].inputs
    assert len(forecast_input["past_covariates"]["temp"]) == 30
    assert len(check_input["target"]) == 25
    assert len(check_input["past_covariates"]["temp"]) == 25


# ---------------------------------------------------------------------------
# Backtest
# ---------------------------------------------------------------------------


def held_out_history(actual: list, before=27) -> list:
    rows = history(before + len(actual), value=lambda i: 10.0)
    for row, value in zip(rows[-len(actual):], actual):
        row["usage"] = value
    return rows


def test_backtest_compares_the_held_out_points(hub):
    # The fake's forecast stays at 10.0: 50% is 9.5-10.5 and 80% is 9.2-10.8.
    rows = held_out_history([10.2, 10.6, 11.0])

    result = answered(forecast(FakeLocal(rows), backtest_points=3))["results"][0]

    assert result["backtest"]["points"] == 3
    assert result["backtest"]["coverage"] == pytest.approx({"50": 1 / 3, "80": 2 / 3})
    assert result["backtest"]["mae"] == pytest.approx(0.6)


def test_backtest_goes_in_the_same_model_call_and_leaves_the_forecast_on_all_points(hub):
    rows = held_out_history([10.2, 10.6, 11.0])

    result = answered(forecast(FakeLocal(rows), backtest_points=3))["results"][0]

    call = hub.pipeline.calls[0]
    assert len(hub.pipeline.calls) == 1
    assert [len(item) for item in call.inputs] == [30, 27]
    assert result["forecast"][0]["forecast"] == pytest.approx(11.0)
    assert result["points_read"] == 30


@pytest.mark.parametrize("horizon, length", [(3, 5), (8, 8)])
def test_backtest_model_call_runs_to_the_longer_of_horizon_and_points(horizon, length, hub):
    result = answered(
        forecast(FakeLocal(history(30)), horizon_steps=horizon, backtest_points=5)
    )["results"][0]

    assert hub.pipeline.calls[0].length == length
    assert len(result["forecast"]) == horizon
    assert result["backtest"]["points"] == 5


def test_without_backtest_points_there_is_no_backtest_key(hub):
    assert "backtest" not in answered(forecast(FakeLocal(history())))["results"][0]
    assert len(hub.pipeline.calls[0].inputs) == 1


def test_backtest_gets_the_same_history_length_as_the_forecast(hub):
    answered(forecast(FakeLocal(history(40)), point_limit=20, backtest_points=5))

    assert [len(item) for item in hub.pipeline.calls[0].inputs] == [20, 20]


def test_a_series_too_short_for_the_backtest_answers_null_with_its_forecast():
    result = answered(forecast(FakeLocal(history(12)), backtest_points=5))["results"][0]

    assert result["backtest"] is None
    assert "forecast" in result


def test_a_gap_in_the_held_out_points_is_skipped():
    rows = [row for row in history(30) if row["time"] != NOW - 3 * MINUTE]

    result = answered(forecast(FakeLocal(rows), backtest_points=5))["results"][0]

    assert result["backtest"]["points"] == 4


def test_backtest_coverage_of_a_level_the_model_cannot_produce_is_null():
    result = answered(forecast(FakeLocal(history(30)), levels=[0.8, 0.9], backtest_points=5))[
        "results"
    ][0]

    assert result["backtest"]["coverage"]["90"] is None
    assert result["backtest"]["coverage"]["80"] is not None


def test_split_backtest_keeps_limit_cells_on_each_side_of_the_cut():
    values = [float(i) for i in range(40)]
    series = plugin.Series(field="usage", tags={}, times=list(range(40)), values=values)

    check, actual = plugin._split_backtest(series, limit=20, held_out=5)

    assert check.values == values[15:35]
    assert check.observed == 20
    assert actual == values[35:]
    assert series.values == values[20:]


def test_a_too_short_series_answers_too_few_points_under_a_backtest():
    result = answered(forecast(FakeLocal(history(4)), backtest_points=2))["results"][0]

    assert result["code"] == "too_few_points"
    assert "backtest" not in result


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------


def test_on_demand_writes_nothing_by_default():
    local = FakeLocal(history())

    answered(forecast(local))

    assert local.written == []


def test_written_rows():
    local = FakeLocal(history(value=lambda i: 10.0, host="a"))

    body = answered(forecast(local, write_results=True, group_by=["host"], forecast_id="ops"))

    assert len(local.written) == 3
    first = local.written[0]
    assert first.measurement == "_forecasts.cpu"
    assert first.database is None
    assert first.tags == {
        "field": "usage",
        "model": plugin.DEFAULT_MODEL,
        "source_measurement": "cpu",
        "forecast_id": "ops",
        "host": "a",
    }
    assert first.time == NOW
    assert [row.time for row in local.written] == [NOW, NOW + MINUTE, NOW + 2 * MINUTE]
    assert [row.fields["step"] for row in local.written] == [1, 2, 3]
    assert first.fields["points_read"] == 20
    assert first.fields["forecast"] == pytest.approx(10.0)
    assert first.fields["lower_80"] == pytest.approx(9.2)
    assert first.fields["upper_50"] == pytest.approx(10.5)
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", first.fields["run_time"])
    assert body["results"][0]["forecast"][0]["timestamp"] * 10**6 == first.time


def test_run_time_is_the_current_utc_time():
    local = FakeLocal(history())

    answered(forecast(local, write_results=True))

    stamp = datetime.strptime(local.written[0].fields["run_time"], "%Y-%m-%dT%H:%M:%SZ")
    age = datetime.now(timezone.utc) - stamp.replace(tzinfo=timezone.utc)
    assert abs(age) < timedelta(minutes=1)


def test_rows_go_in_one_durable_write():
    local = FakeLocal(history())

    answered(forecast(local, write_results=True))

    assert [call.no_sync for call in local.write_calls] == [True]


def test_target_measurement_and_database():
    local = FakeLocal(history())

    answered(
        forecast(local, write_results=True, target_measurement="fc", target_database="archive")
    )

    assert {row.measurement for row in local.written} == {"fc"}
    assert {row.database for row in local.written} == {"archive"}


def test_without_forecast_id_there_is_no_such_tag():
    local = FakeLocal(history())

    answered(forecast(local, write_results=True))

    assert "forecast_id" not in local.written[0].tags


def test_a_level_the_model_cannot_produce_is_not_written():
    local = FakeLocal(history())

    answered(forecast(local, write_results=True, levels=[0.8, 0.9]))

    assert "lower_80" in local.written[0].fields
    assert "lower_90" not in local.written[0].fields


def test_a_series_tag_named_like_a_row_column_is_left_out():
    schema = {**SCHEMA, "model": TAG, "step": TAG}
    rows = [{**row, "model": "other", "step": "x"} for row in history(host="a")]
    local = FakeLocal(rows, schema=schema)

    result = answered(forecast(local, write_results=True))["results"][0]

    assert result["tags"] == {"host": "a", "model": "other", "step": "x"}
    assert local.written[0].tags["model"] == plugin.DEFAULT_MODEL
    assert local.written[0].tags["host"] == "a"
    assert "step" not in local.written[0].tags


def test_a_too_short_series_writes_nothing():
    local = FakeLocal(history(4))

    answered(forecast(local, write_results=True))

    assert local.write_calls == []


def test_only_series_with_a_forecast_are_written():
    local = FakeLocal(history(host="a") + history(4, host="b"))

    answered(forecast(local, write_results=True, group_by=["host"]))

    assert {row.tags["host"] for row in local.written} == {"a"}


def test_a_failed_write_fails_the_request():
    local = FakeLocal(history(), write_error=RuntimeError("disk full"))

    message = failed(forecast(local, write_results=True), "internal", 500)

    assert "the write failed: disk full" in message


def test_rows_that_cannot_be_built_fail_the_request(monkeypatch):
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


def query_rows(count=20, hosts=("a", "b")) -> list:
    return [
        {**row, "host": host, "load": row["usage"] * 2}
        for host in hosts
        for row in history(count)
    ]


def test_each_numeric_column_of_each_series_is_a_forecast():
    body = answered(forecast_query(FakeLocal(query_rows=query_rows())))

    assert [(r["tags"]["host"], r["field"]) for r in body["results"]] == [
        ("a", "load"),
        ("a", "usage"),
        ("b", "load"),
        ("b", "usage"),
    ]
    assert body["series_total"] == 4


def test_query_results_are_ordered_with_a_missing_key_last():
    rows = [
        {**row, "host": host}
        for host in ("b", None, "a")
        for row in history()
    ]

    body = answered(forecast_query(FakeLocal(query_rows=rows)))

    assert [r["tags"] for r in body["results"]] == [{"host": "a"}, {"host": "b"}, {}]


def test_query_without_a_key_column_is_one_series(hub):
    body = answered(forecast_query(FakeLocal(query_rows=history())))

    assert body["series_total"] == 1
    assert body["results"][0]["tags"] == {}
    assert model_input(hub.pipeline) == [float(i) for i in range(20)]


def test_query_rows_need_not_be_sorted(hub):
    answered(forecast_query(FakeLocal(query_rows=list(reversed(history())))))

    assert model_input(hub.pipeline) == [float(i) for i in range(20)]


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


def test_query_series_limit_never_hits_the_cap():
    answered(forecast_query(FakeLocal(query_rows=query_rows()), max_series=3, series_limit=3))


def test_no_rows_means_no_series(hub):
    body = answered(forecast_query(FakeLocal()))

    assert body["series_total"] == 0
    assert body["results"] == []
    assert hub.loads == []


def test_no_time_column_is_a_bad_request():
    rows = [{"host": "a", "usage": 1.0}]

    assert "no time column" in refused(forecast_query(FakeLocal(query_rows=rows)))


def test_no_numeric_column_is_a_bad_request():
    rows = [{"time": NOW, "host": "a"}]

    assert "no numeric column" in refused(forecast_query(FakeLocal(query_rows=rows)))


def test_a_boolean_column_is_a_key_and_an_empty_column_is_nothing():
    rows = [{**row, "up": True, "empty": None} for row in history()]

    body = answered(forecast_query(FakeLocal(query_rows=rows)))

    assert [r["field"] for r in body["results"]] == ["usage"]
    assert body["results"][0]["tags"] == {"up": "True"}


def test_a_null_value_is_not_a_point():
    rows = history()
    rows[-1]["usage"] = None

    result = answered(forecast_query(FakeLocal(query_rows=rows)))["results"][0]

    assert result["points_read"] == 19


def test_too_few_query_points_name_no_window():
    result = answered(forecast_query(FakeLocal(query_rows=history(4))))["results"][0]

    assert result["message"] == "need >= 10 points, got 4"


def test_point_limit_cuts_each_query_series(hub):
    body = answered(forecast_query(FakeLocal(query_rows=history(30)), point_limit=15))

    assert body["results"][0]["points_read"] == 15
    assert model_input(hub.pipeline) == [float(i) for i in range(15, 30)]


def test_query_has_no_default_window():
    local = FakeLocal(query_rows=history())

    answered(forecast_query(local))

    assert local.sent(QUERY)[-1].sql == QUERY


def test_window_wraps_the_query_and_drops_a_series_without_rows_in_it():
    old = history(end=NOW - 120 * MINUTE)
    rows = [{**row, "host": "old"} for row in old] + [{**row, "host": "new"} for row in history()]
    local = FakeLocal(query_rows=rows)

    body = answered(forecast_query(local, window="1h", max_series=1))

    assert [r["tags"]["host"] for r in body["results"]] == ["new"]
    assert (
        "AS __history WHERE time > (now() AT TIME ZONE 'UTC') - INTERVAL '3600 seconds'"
        in local.sent(QUERY)[-1].sql
    )


def test_a_query_that_cannot_be_wrapped_is_filtered_by_the_plugin():
    rows = history(end=NOW - 120 * MINUTE, host="old") + history(host="new")
    local = FakeLocal(query_rows=rows, fail={"AS __history": RuntimeError("cannot compare")})

    body = answered(forecast_query(local, window="1h"))

    assert [r["tags"]["host"] for r in body["results"]] == ["new"]
    assert any("Window applied in the plugin" in text for text in local.messages("warn"))
    assert local.queries[-1].sql == QUERY


def test_a_wrapped_query_out_of_resources_is_not_retried():
    local = FakeLocal(fail={"AS __history": RuntimeError("Resources exhausted: too big")})

    assert "Resources exhausted" in failed(forecast_query(local, window="1h"), "internal", 500)
    assert local.sent("AS __history") and local.queries[-1].sql.startswith("SELECT * FROM")


@pytest.mark.parametrize(
    "query, body",
    [
        ("SELECT 1;", "SELECT 1"),
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


def test_query_sampling_averages_in_the_plugin(hub):
    rows = history(40, step=30 * SECOND, end=NOW - 30 * SECOND)

    result = answered(forecast_query(FakeLocal(query_rows=rows), sampling_interval="1min"))[
        "results"
    ][0]

    assert result["step_ms"] == 60_000
    assert result["points_read"] == 20
    assert model_input(hub.pipeline)[0] == pytest.approx(0.5)


def test_query_writes_use_the_measurement_from_its_plan():
    local = FakeLocal(query_rows=history(host="a"))

    answered(forecast_query(local, write_results=True))

    assert local.written[0].measurement == "_forecasts.cpu"
    assert local.written[0].tags["source_measurement"] == "cpu"
    assert local.written[0].tags["host"] == "a"


def test_query_over_several_measurements_needs_a_target_to_write():
    local = FakeLocal(query_rows=history(), explain=("cpu", "mem"))

    assert "target_measurement" in refused(forecast_query(local, write_results=True))
    assert local.queries[-1].sql.startswith("EXPLAIN")


def test_query_over_several_measurements_writes_without_the_tag():
    local = FakeLocal(query_rows=history(), explain=("cpu", "mem"))

    answered(forecast_query(local, write_results=True, target_measurement="fc"))

    assert local.written[0].measurement == "fc"
    assert "source_measurement" not in local.written[0].tags


def test_query_over_several_measurements_answers_without_writing():
    answered(forecast_query(FakeLocal(query_rows=history(), explain=("cpu", "mem"))))


def test_a_query_that_does_not_plan_is_a_bad_request(hub):
    local = FakeLocal(fail={"EXPLAIN": RuntimeError("no such table")})

    assert "does not plan: no such table" in refused(forecast_query(local))
    assert hub.loads == []


def test_a_query_key_named_like_a_row_column_is_refused_when_writing():
    rows = history(model="m")

    assert "clashes" in refused(forecast_query(FakeLocal(query_rows=rows), write_results=True))
    answered(forecast_query(FakeLocal(query_rows=rows)))


@pytest.mark.parametrize(
    "plan, names",
    [
        ("TableScan: cpu projection=[time, usage]", ["cpu"]),
        ("TableScan: cpu load projection=[time]", ["cpu load"]),
        ("TableScan: iox.cpu projection=[time]", ["cpu"]),
        ("TableScan: public.iox.cpu projection=[time]", ["cpu"]),
        ("TableScan: cpu, full_filters=[usage > 0]", ["cpu"]),
        ("TableScan: cpu, partial_filters=[usage > 0]", ["cpu"]),
        ("TableScan: cpu, fetch=5", ["cpu"]),
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


def test_a_bad_request_logs_a_warning_and_other_codes_an_error(hub):
    local = FakeLocal(history())
    refused(forecast(local, horizon_steps=None))
    assert local.messages("error") == []
    assert any("bad_request" in text for text in local.messages("warn"))

    hub.error = OSError("gone")
    local = FakeLocal(history())
    failed(forecast(local), "model_unavailable", 503)
    assert any("model_unavailable" in text for text in local.messages("error"))


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


def test_scheduled_arguments_are_strings():
    rows = history(host="a", region="eu") + history(host="b", region="eu")
    local = FakeLocal(rows)

    run_scheduled(
        local,
        group_by="host region",
        tag_filter="region=eu",
        levels="0.5 0.8",
        point_limit="15",
        window="1d",
        history_end="now+1h",
        series_limit="1",
        forecast_id="ops",
        target_measurement="fc",
        target_database="archive",
    )

    assert {row.tags["host"] for row in local.written} == {"a"}
    assert {row.tags["forecast_id"] for row in local.written} == {"ops"}
    assert {row.measurement for row in local.written} == {"fc"}
    assert {row.database for row in local.written} == {"archive"}
    assert '"region" = $tf0_0' in local.history_query().sql
    assert local.history_query().params["history_end"] == plugin._iso(NOW + 60 * MINUTE)


def test_toml_overrides_trigger_arguments(tmp_path):
    path = write_toml(
        tmp_path, 'horizon_steps = 2\nlevels = [0.5]\ntag_filter = { host = ["a", "b"] }\n'
    )
    local = FakeLocal(history(host="a"))

    run_scheduled(local, config_file_path=path)

    assert len(local.written) == 2
    assert "lower_50" in local.written[0].fields
    assert "lower_80" not in local.written[0].fields
    assert '"host" IN ($tf0_0, $tf0_1)' in local.history_query().sql


def test_a_scheduled_query_writes():
    local = FakeLocal(query_rows=history())

    plugin.process_scheduled_call(
        local, datetime(2027, 1, 15), {"query": QUERY, "horizon_steps": "2"}
    )

    assert len(local.written) == 2


@pytest.mark.parametrize("key", ["write_results", "backtest_points", "nope"])
def test_keys_a_scheduled_run_does_not_take_are_logged_as_bad_requests(key):
    local = FakeLocal(history())

    run_scheduled(local, **{key: "1"})

    assert local.written == []
    assert any("bad_request" in text and key in text for text in local.messages("error"))


def test_a_failed_scheduled_run_logs_its_code_and_does_not_raise(hub):
    hub.error = OSError("gone")
    local = FakeLocal(history())

    assert run_scheduled(local) is None

    assert any(text.split("] ")[1].startswith("model_unavailable") for text in local.messages("error"))


def test_a_failed_scheduled_write_logs_internal():
    local = FakeLocal(history(), write_error=RuntimeError("disk full"))

    run_scheduled(local)

    assert any("internal" in text and "disk full" in text for text in local.messages("error"))


def test_a_short_series_in_a_scheduled_run_is_a_warning():
    local = FakeLocal(history(host="a") + history(4, host="b"))

    run_scheduled(local, group_by="host")

    assert {row.tags["host"] for row in local.written} == {"a"}
    assert any("too_few_points" in text and "'b'" in text for text in local.messages("warn"))


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
    assert scheduled == {*plugin.SCHEDULED_NAMES, "config_file_path"}
    http_args = {entry["name"] for entry in meta["http_args_config"]}
    assert http_args == {*plugin.SETTING_NAMES, "config_file_path"}
    assert {entry["name"] for entry in meta["http_body_config"]} == set(plugin.SETTING_NAMES)


def test_toml_templates_set_only_keys_the_plugin_takes():
    assert active_keys("chronos_forecasting_scheduler.toml") <= set(plugin.SCHEDULED_NAMES)
    assert active_keys("chronos_forecasting_http.toml") <= set(plugin.SETTING_NAMES)


def test_manifest_matches_the_plugin():
    manifest = json.loads((PLUGIN_DIR / "forecast_interface.json").read_text())

    assert manifest["forecast_contract_version"] == 1
    assert set(manifest["filter"]) == {"tag_filter", "sql_filter"}
    assert manifest["backtest"] is True
    assert manifest["accepts_query"] is True
    defaults = [model for model in manifest["models"] if model["default"]]
    assert [model["id"] for model in defaults] == [plugin.DEFAULT_MODEL]
    assert set(plugin.DEFAULT_LEVELS) <= set(defaults[0]["levels"])
    for model in manifest["models"]:
        assert model["min_points"] == plugin.MIN_POINTS
    for param in manifest["advanced_params"]:
        assert param["name"] in plugin.PLUGIN_KEYS
        assert param["name"] not in plugin.CONTRACT_KEYS
    param_defaults = {param["name"]: param.get("default") for param in manifest["advanced_params"]}
    assert param_defaults["history_end"] == "latest"


def test_the_manifest_ships_and_the_tests_do_not():
    plugin_table = tomllib.loads((PLUGIN_DIR / "manifest.toml").read_text())["plugin"]

    assert "!forecast_interface.json" in plugin_table["exclude"]
    assert "*.py" in plugin_table["exclude"]
    assert "!chronos_forecasting.py" in plugin_table["exclude"]
    assert f"!{Path(__file__).name}" not in plugin_table["exclude"]
