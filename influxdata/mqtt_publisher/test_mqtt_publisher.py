import json
import re
import time
from types import SimpleNamespace

import pytest

import mqtt_publisher as plugin

TAG_TYPE = "Dictionary(Int32, Utf8)"

# get_schema reports tags and fields and leaves `time` out, as the real one does
PRESS_COLUMNS = {
    "machine": TAG_TYPE,
    "line": TAG_TYPE,
    "temp": "Float64",
    "count": "Int64",
    "state": "Utf8",
}

BASE_ARGS = {
    "broker_url": "mqtt://broker:1883",
    "source_measurements": "press_line",
    "topic": "plant/{machine}/{_table}",
}


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

    def tag(self, key, value):
        self.tags[key] = value
        return self

    def int64_field(self, key, value):
        self.fields[key] = int(value)
        return self

    def string_field(self, key, value):
        self.fields[key] = str(value)
        return self


class FakeLocal:
    """Stub of the runtime: logging, the trigger-local cache, queries, writes."""

    def __init__(self, columns=None, rows=None):
        self.cache = FakeCache()
        self.columns = dict(columns or {"press_line": PRESS_COLUMNS})
        self.rows = dict(rows or {})
        self.queries: list = []
        self.writes: list = []
        self.logs: list = []

    def query(self, query, params=None):
        self.queries.append((query, params))
        # The runtime binds only string parameters; model that so a numeric
        # value reaching the query as a parameter fails here as it would there.
        for value in (params or {}).values():
            if not isinstance(value, str):
                raise TypeError(
                    f"'{type(value).__name__}' object is not an instance of 'str'"
                )
        if "information_schema.columns" in query:
            table = params["table"] if params else None
            if table not in self.columns:
                return []
            return [
                {"column_name": name, "data_type": data_type}
                for name, data_type in self.columns[table].items()
            ]

        table = re.search(r'FROM "([^"]+)"', query).group(1)
        rows = sorted(self.rows.get(table, []), key=lambda row: row["time"])

        exact = re.search(r"time = to_timestamp_nanos\((-?\d+)\)", query)
        if exact:
            selected = [row for row in rows if row["time"] == int(exact.group(1))]
        else:
            lower = int(re.search(r"time > to_timestamp_nanos\((-?\d+)\)", query).group(1))
            upper = int(re.search(r"time <= to_timestamp_nanos\((-?\d+)\)", query).group(1))
            selected = [row for row in rows if lower < row["time"] <= upper]
            if "ORDER BY time DESC" in query:
                selected = list(reversed(selected))
        limit = re.search(r"LIMIT (\d+)", query)
        if limit:
            selected = selected[: int(limit.group(1))]
        return [dict(row) for row in selected]

    def write(self, line):
        self.writes.append(line)

    def info(self, message):
        self.logs.append(("info", message))

    def warn(self, message):
        self.logs.append(("warn", message))

    def error(self, message):
        self.logs.append(("error", message))

    def messages(self, level=None):
        return [text for lvl, text in self.logs if level is None or lvl == level]


class FakeInfo:
    def __init__(self, mid, published, rc=0):
        self.mid = mid
        self.rc = rc
        self._published = published

    def is_published(self):
        return self._published


class FakeReason:
    def __init__(self, value=0, name="Success"):
        self.value = value
        self._name = name

    def __str__(self):
        return self._name


class FakeClient:
    """Stand-in for paho's client: records publishes, drives the callbacks.

    It reproduces the one behaviour the plugin is built around — a QoS 0
    publish reaches the socket inside ``publish()``, so ``on_publish`` has run
    by the time it returns, while QoS >= 1 waits for an acknowledgement.
    """

    instances: list = []

    def __init__(self, *args, **kwargs):
        self.callbacks = kwargs["userdata"]
        self.client_id = kwargs.get("client_id")
        self.sent: list = []
        self.will = None
        self.connect_timeout = 2
        self.max_inflight = None
        self.disconnected = False
        self._connected = False
        self._mid = 0
        self.connack = FakeReason()
        self.connack_properties = None
        self.publish_rc = 0
        self.auto_ack = True
        FakeClient.instances.append(self)

    # paho exposes many setters the plugin may touch; none of them matter here
    def __getattr__(self, name):
        return lambda *args, **kwargs: None

    def max_inflight_messages_set(self, value):
        self.max_inflight = value

    def will_set(self, topic, payload=None, qos=0, retain=False):
        self.will = (topic, payload, qos, retain)

    def connect(self, host, port, keepalive=60, **kwargs):
        self.host, self.port, self.keepalive = host, port, keepalive
        self._connected = self.connack.value == 0
        plugin.on_connect(self, self.callbacks, {}, self.connack, self.connack_properties)

    def is_connected(self):
        return self._connected and not self.disconnected

    def loop(self, timeout=0.0):
        return 0

    def socket(self):
        return None

    def disconnect(self):
        self._connected = False

    def publish(self, topic, payload, qos=0, retain=False, properties=None):
        self._mid += 1
        self.sent.append(
            {
                "topic": topic,
                "payload": payload,
                "qos": qos,
                "retain": retain,
                "properties": properties,
            }
        )
        if self.publish_rc:
            return FakeInfo(self._mid, False, rc=self.publish_rc)
        if qos == 0 and self.auto_ack:
            plugin.on_publish(self, self.callbacks, self._mid, None, None)
            return FakeInfo(self._mid, True)
        return FakeInfo(self._mid, False)

    def ack(self, mid, reason=None):
        plugin.on_publish(self, self.callbacks, mid, reason, None)

    def drop(self, reason=None, from_broker=True):
        """Lose the connection, with or without a DISCONNECT from the broker.

        A reason code can only reach the client in a DISCONNECT packet; a
        socket that simply dies carries one paho synthesised, which says
        nothing about the message that was in flight.
        """
        self.disconnected = True
        self._connected = False
        flags = SimpleNamespace(is_disconnect_packet_from_server=from_broker)
        plugin.on_disconnect(self, self.callbacks, flags, reason, None)


@pytest.fixture(autouse=True)
def runtime(monkeypatch):
    monkeypatch.setattr(plugin, "LineBuilder", FakeLineBuilder, raising=False)
    monkeypatch.setattr(plugin.mqtt, "Client", FakeClient)
    FakeClient.instances.clear()


@pytest.fixture
def local():
    return FakeLocal()


def rows_of(*values, base_ns=None):
    """Rows timestamped around now, so an age check sees them as fresh."""
    base = base_ns if base_ns is not None else time.time_ns()
    return [
        {
            "time": base + index,
            "machine": machine,
            "line": "l1",
            "temp": temp,
            "count": count,
            "state": "run",
        }
        for index, (machine, temp, count) in enumerate(values)
    ]


def publish(local, args, rows=None, table="press_line"):
    batches = [{"table_name": table, "rows": rows if rows is not None else rows_of(("press7", 21.5, 7))}]
    plugin.process_writes(local, batches, dict(BASE_ARGS, **args))
    return FakeClient.instances[-1] if FakeClient.instances else None


def state_of(local):
    return local.cache.get(plugin.STATE_CACHE_KEY)


# --- templates and topics ---------------------------------------------------


def test_placeholder_grammar_leaves_json_alone():
    """A brace group that is not a placeholder is literal, so JSON needs no escaping."""
    rendered = plugin.render_template(
        '{"cmd":"stop","temp":{temp},"note":"{{raw}}"}', {"temp": 187.2}, None
    )
    assert rendered == '{"cmd":"stop","temp":187.2,"note":"{raw}"}'
    assert plugin.placeholder_names('{"a":1} {machine} {a-b}') == ["machine"]


@pytest.mark.parametrize(
    "value, refused",
    [("press7", False), ("../cmd", True), ("a+b", True), ("a#b", True)],
)
def test_substituted_value_may_not_reshape_the_topic(value, refused):
    topic, reason = plugin.render_topic("plant/{machine}/x", {"machine": value}, None)
    assert (topic is None) is refused
    assert (reason is not None) is refused


def test_missing_placeholder_value_skips_or_substitutes():
    assert plugin.render_topic("plant/{machine}", {}, None)[0] is None
    assert plugin.render_topic("plant/{machine}", {}, "unknown")[0] == "plant/unknown"


# --- payload encoders -------------------------------------------------------


def test_line_protocol_carries_types_and_escapes():
    """A bare number is a float in line protocol, so integers need their suffix."""
    line = plugin.encode_line_protocol(
        "press,line",
        {"machine": "press 7"},
        {"count": 7, "ratio": 1.5, "ok": True, "note": 'say "hi"\\'},
        1_791_700_000_000_000_000,
        {"count": "int", "ratio": "float", "ok": "bool", "note": "string"},
    )
    assert line == (
        'press\\,line,machine=press\\ 7 count=7i,note="say \\"hi\\"\\\\",ok=t,ratio=1.5 '
        "1791700000000000000"
    )


def test_unsigned_needs_the_catalog_not_the_python_value():
    """int64 and uint64 are both Python ints; only the schema tells them apart."""
    assert plugin.encode_field_value(7, "uint") == "7u"
    assert plugin.encode_field_value(7, "int") == "7i"
    assert plugin.encode_field_value(7, None) == "7i"
    assert plugin.encode_field_value(True, None) == "t"


@pytest.mark.parametrize(
    "time_format, expected",
    [
        ("rfc3339", "2026-10-11T06:26:40.000000000Z"),
        ("ms", 1_791_700_000_000),
        ("ns", 1_791_700_000_000_000_000),
    ],
)
def test_time_format(time_format, expected):
    assert plugin.format_timestamp(1_791_700_000_000_000_000, time_format) == expected


def test_rfc3339_keeps_the_second_on_a_boundary():
    assert plugin.format_timestamp(1_791_699_999_999_999_999, "rfc3339").endswith(
        "39.999999999Z"
    )


# --- filtering --------------------------------------------------------------


def test_absent_column_is_judged_per_operator():
    """A row cannot equal a value it lacks, and cannot be shown to match one."""
    cfg = {
        "filters": plugin.parse_filters("temp:'>=185' state:'!=error'"),
        "allow_values": plugin.parse_value_lists("machine:'press7'"),
        "deny_values": plugin.parse_value_lists("line:'l9'"),
    }
    assert plugin.row_passes({"temp": 200, "state": "run", "machine": "press7"}, cfg)
    # no state: passes "!="; no line: passes deny
    assert plugin.row_passes({"temp": 200, "machine": "press7"}, cfg)
    # no temp: fails a positive test
    assert not plugin.row_passes({"state": "run", "machine": "press7"}, cfg)
    # no machine: fails allow
    assert not plugin.row_passes({"temp": 200, "state": "run"}, cfg)
    # denied wins over allowed
    assert not plugin.row_passes(
        {"temp": 200, "state": "run", "machine": "press7", "line": "l9"}, cfg
    )


def test_pairs_are_space_separated_and_quote_aware():
    """One separator serves both levels: quotes say which space is which."""
    assert plugin.parse_filters("temp:'>=1.5' state:'!=in progress'") == [
        ("temp", ">=", 1.5),
        ("state", "!=", "in progress"),
    ]
    assert plugin.parse_value_lists("machine:'press7 press8' line:'l1'") == {
        "machine": ["press7", "press8"],
        "line": ["l1"],
    }
    with pytest.raises(ValueError, match="unbalanced"):
        plugin.parse_filters("temp:'>=185")
    with pytest.raises(ValueError, match="separated by spaces"):
        plugin.parse_filters("temp>=185")


def test_the_structured_form_says_the_same_as_the_compact_one(local):
    """A TOML file and a request body carry data structures; use them."""
    compact = plugin.load_settings(
        local,
        dict(BASE_ARGS, filters="temp:'>=185' state:'!=error'",
             allow_values="machine:'press7 press8'"),
        "t",
    )
    structured = plugin.load_settings(
        local,
        dict(BASE_ARGS),
        "t",
        request_layers=(plugin.parse_json_body(
            {
                "filters": [["temp", ">=", 185], ["state", "!=", "error"]],
                "allow_values": {"machine": ["press7", "press8"]},
            },
            plugin.BODY_KEYS,
        ),),
    )
    assert structured["filters"] == compact["filters"]
    assert structured["allow_values"] == compact["allow_values"]
    assert plugin.parse_filters([{"column": "temp", "operator": ">=", "value": 185}]) == [
        ("temp", ">=", 185)
    ]
    assert plugin.parse_header_map({"Authorization": "Bearer abc"}) == {
        "Authorization": "Bearer abc"
    }
    with pytest.raises(ValueError, match="triple"):
        plugin.parse_filters([["temp", ">="]])
    with pytest.raises(ValueError, match="expected one of"):
        plugin.parse_filters([["temp", "~=", 1]])


def test_a_filter_is_checked_against_the_schema(local):
    state = plugin.get_state(local, plugin.load_settings(local, dict(BASE_ARGS), "t"))

    cfg = plugin.load_settings(local, dict(BASE_ARGS, filters="nosuch:'>=1'"), "t")
    with pytest.raises(ValueError, match="no source table has"):
        plugin.validate_filters(local, state, cfg, "t")

    cfg = plugin.load_settings(local, dict(BASE_ARGS, filters="state:'>=1'"), "t")
    with pytest.raises(ValueError, match="needs a numeric column"):
        plugin.validate_filters(local, state, cfg, "t")

    cfg = plugin.load_settings(local, dict(BASE_ARGS, filters="state:'=7'"), "t")
    with pytest.raises(ValueError, match="quote it to compare as text"):
        plugin.validate_filters(local, state, cfg, "t")

    cfg = plugin.load_settings(local, dict(BASE_ARGS, filters="temp:'>=185'"), "t")
    plugin.validate_filters(local, state, cfg, "t")


def test_negative_tests_compile_with_an_is_null_arm():
    """A bare NOT IN or != drops the rows the Python path keeps."""
    cfg = {
        "filters": plugin.parse_filters("temp:'>=185' state:'!=error'"),
        "allow_values": plugin.parse_value_lists("machine:'press7 press8'"),
        "deny_values": plugin.parse_value_lists("line:'l9'"),
    }
    clauses, params = plugin.filter_clauses(cfg)
    assert clauses == [
        '"temp" >= 185',
        '("state" IS NULL OR "state" != $filter_1)',
        '"machine" IN ($allow_0_0, $allow_0_1)',
        '("line" IS NULL OR "line" NOT IN ($deny_0_0))',
    ]
    assert params == {
        "filter_1": "error",
        "allow_0_0": "press7",
        "allow_0_1": "press8",
        "deny_0_0": "l9",
    }


def test_numeric_filter_values_are_inlined_not_bound(local):
    """The runtime binds only string parameters, so a numeric filter value must
    reach the SQL as a literal, never as a parameter the query would reject."""
    cfg = {
        "filters": plugin.parse_filters("temp:'>=1.5' count:'=7' state:'!=error'"),
        "allow_values": {},
        "deny_values": {},
    }
    clauses, params = plugin.filter_clauses(cfg)
    assert clauses[0] == '"temp" >= 1.5'
    assert clauses[1] == '"count" = 7'
    assert '"state" != $filter_2' in clauses[2]
    # only the string value is a bound parameter, and every bound value is a str
    assert params == {"filter_2": "error"}
    assert all(isinstance(v, str) for v in params.values())


def test_filter_values_are_bound_not_interpolated(local):
    """A value carrying a quote must not reach the statement as text."""
    args = dict(BASE_ARGS, filters="state:'=o''brien'", config_file_path="")
    cfg = plugin.load_settings(local, args, "t")
    clauses, params = plugin.filter_clauses(cfg)
    assert "o''brien" not in "".join(clauses)
    assert params["filter_0"] == "o''brien"


# --- payload and layout -----------------------------------------------------


def test_each_layout_produces_its_own_shape():
    rows = rows_of(("press7", 21.5, 7), ("press8", 22.0, 8))

    client = publish(FakeLocal(), {"payload_fields": "temp"}, rows)
    assert [message["topic"] for message in client.sent] == [
        "plant/press7/press_line",
        "plant/press8/press_line",
    ]
    # payload_fields narrows the payload without narrowing the topic
    assert json.loads(client.sent[0]["payload"]) == {"temp": 21.5}

    client = publish(FakeLocal(), {"layout": "batch", "topic": "plant/all"}, rows)
    assert len(client.sent) == 1
    assert len(json.loads(client.sent[0]["payload"])) == 2

    client = publish(
        FakeLocal(),
        {"layout": "field", "topic": "plant/{machine}", "payload_fields": "temp count"},
        rows,
    )
    assert [(m["topic"], m["payload"]) for m in client.sent] == [
        ("plant/press7/temp", b"21.5"),
        ("plant/press7/count", b"7"),
        ("plant/press8/temp", b"22.0"),
        ("plant/press8/count", b"8"),
    ]


def test_field_layout_ignores_payload_format(local):
    client = publish(
        local,
        {
            "layout": "field",
            "topic": "plant/{machine}/{_field}",
            "payload_fields": "temp",
            "payload_format": "lineprotocol",
        },
    )
    assert client.sent[0]["payload"] == b"21.5"


def test_empty_columns_leave_the_payload_and_empty_rows_are_skipped(local):
    """A query fills gaps with None where a write row simply has no key."""
    rows = [
        {"time": 1, "machine": "press7", "line": None, "temp": 21.5, "count": None, "state": ""},
        {"time": 2, "machine": "press8", "line": None, "temp": None, "count": None, "state": ""},
    ]
    client = publish(local, {"payload_fields": "temp count"}, rows)
    assert len(client.sent) == 1
    assert json.loads(client.sent[0]["payload"]) == {"temp": 21.5}


def test_batch_is_split_at_the_payload_limit(local):
    rows = rows_of(*[("press7", float(index), index) for index in range(40)])
    client = publish(
        local, {"layout": "batch", "topic": "plant/all", "max_payload_bytes": 1200}, rows
    )
    assert len(client.sent) > 1
    assert all(len(message["payload"]) <= 1200 for message in client.sent)
    published = [row for message in client.sent for row in json.loads(message["payload"])]
    assert len(published) == 40


def test_oversized_single_message_is_dropped_and_counted(local):
    rows = [dict(rows_of(("press7", 1.0, 1))[0], state="x" * 5000)]
    client = publish(local, {"max_payload_bytes": 1024}, rows)
    assert client.sent == []
    assert state_of(local).callbacks.counters["dropped_oversized"] == 1


def test_a_publish_that_raises_drops_the_message_not_the_queue(local, monkeypatch):
    """A topic the client rejects outright is dead-lettered, so the messages
    behind it still go out instead of waiting behind a poison one for ever."""

    class Rejecting(FakeClient):
        def publish(self, topic, payload, qos=0, retain=False, properties=None):
            if "poison" in topic:
                raise ValueError("Publish topic cannot contain wildcards")
            return super().publish(
                topic, payload, qos=qos, retain=retain, properties=properties
            )

    monkeypatch.setattr(plugin.mqtt, "Client", Rejecting)
    rows = [
        {"time": 1, "machine": "poison", "line": "l1", "temp": 1.0, "count": 1, "state": "run"},
        {"time": 2, "machine": "ok", "line": "l1", "temp": 2.0, "count": 2, "state": "run"},
    ]
    client = publish(local, {"topic": "plant/{machine}"}, rows)
    assert state_of(local).callbacks.counters["dropped_undeliverable"] == 1
    assert [m["topic"] for m in client.sent] == ["plant/ok"]
    assert len(state_of(local).pending) == 0


# --- the client and its life ------------------------------------------------


def test_the_connection_survives_and_no_threads_are_started(local):
    publish(local, {})
    publish(local, {})
    assert len(FakeClient.instances) == 1, "the cached client was rebuilt"
    assert FakeClient.instances[0].sent and len(FakeClient.instances[0].sent) == 2


def test_qos0_is_confirmed_from_the_message_info_not_the_callback(local):
    """on_publish has already run when publish() returns, so an id recorded
    afterwards would never clear."""
    publish(local, {"qos": "0"})
    callbacks = state_of(local).callbacks
    assert callbacks.unconfirmed == {}
    assert callbacks.counters["confirmed"] == 1


def test_qos1_waits_for_the_acknowledgement(local):
    client = publish(local, {"qos": "1"})
    callbacks = state_of(local).callbacks
    assert len(callbacks.unconfirmed) == 1
    client.ack(1)
    assert callbacks.unconfirmed == {}
    assert callbacks.counters["confirmed"] == 1


def test_a_refusal_drops_the_message_without_spending_attempts(local):
    client = publish(local, {"qos": "1", "mqtt_version": "5"})
    client.ack(1, FakeReason(0x87, "Not authorized"))
    counters = state_of(local).callbacks.counters
    assert counters["dropped_undeliverable"] == 1
    assert len(state_of(local).pending) == 0


def test_no_matching_subscribers_counts_as_delivered(local):
    client = publish(local, {"qos": "1", "mqtt_version": "5"})
    client.ack(1, FakeReason(0x10, "No matching subscribers"))
    assert state_of(local).callbacks.counters["confirmed"] == 1


def test_a_dropped_connection_returns_its_messages_to_the_head(local):
    client = publish(local, {"qos": "1"})
    state = state_of(local)
    assert len(state.callbacks.unconfirmed) == 1
    client.drop(FakeReason(0x8E, "Session taken over"))
    publish(local, {"qos": "1"})
    assert len(FakeClient.instances) == 2, "the client is replaced, not repaired"
    # the returned message goes out first on the new client
    assert FakeClient.instances[1].sent[0]["payload"] == FakeClient.instances[0].sent[0]["payload"]


def test_only_the_oldest_unconfirmed_is_charged_an_attempt(local):
    rows = rows_of(("press7", 1.0, 1), ("press8", 2.0, 2), ("press9", 3.0, 3))
    client = publish(local, {"qos": "1", "max_delivery_attempts": "1"}, rows)
    assert len(state_of(local).callbacks.unconfirmed) == 3
    client.drop(FakeReason(0x95, "Packet too large"))
    state = state_of(local)
    # the rebuild, and with it the attempt accounting, happens on the next call
    publish(local, {"qos": "1", "max_delivery_attempts": "1"}, [])
    counters = state.callbacks.counters
    assert counters["dropped_undeliverable"] == 1, "only the first in line retires"


def test_a_broker_disconnect_that_blames_nothing_charges_nothing(local):
    settings = {"qos": "1", "mqtt_version": "5", "max_delivery_attempts": "1"}
    client = publish(local, settings)
    client.drop(FakeReason(0x8B, "Server shutting down"))
    publish(local, settings, [])
    assert state_of(local).callbacks.counters["dropped_undeliverable"] == 0


def test_a_silent_close_under_mqtt5_still_charges(local):
    """MQTT 5 lets a server close without a DISCONNECT, and several do on an
    oversized packet; treating that as a network event would leave the message
    that provokes it retrying for ever."""
    settings = {"qos": "1", "mqtt_version": "5", "max_delivery_attempts": "1"}
    client = publish(local, settings)
    client.drop(FakeReason(7, "Connection lost"), from_broker=False)
    publish(local, settings, [])
    assert state_of(local).callbacks.counters["dropped_undeliverable"] == 1


def test_quota_exceeded_stops_the_invocation_without_dead_lettering(local):
    client = publish(local, {"qos": "1", "mqtt_version": "5"})
    client.ack(1, FakeReason(0x97, "Quota exceeded"))
    callbacks = state_of(local).callbacks
    assert callbacks.quota_refused is True
    assert callbacks.counters["dropped_undeliverable"] == 0


def test_a_quota_refusal_returns_the_message_to_the_buffer(local):
    """0x97 is about the rate, so the refused message leaves the in-flight map
    for the head of the buffer, where the next invocation retries it."""
    settings = {"qos": "1", "mqtt_version": "5"}
    client = publish(local, settings)
    callbacks = state_of(local).callbacks
    assert len(callbacks.unconfirmed) == 1
    client.ack(1, FakeReason(0x97, "Quota exceeded"))
    # out of the in-flight window, held for re-queue, not dropped
    assert len(callbacks.unconfirmed) == 0
    assert len(callbacks.quota_returned) == 1
    assert callbacks.counters["dropped_undeliverable"] == 0
    # the next invocation drains, which returns it to the buffer
    publish(local, settings, [])
    assert len(callbacks.quota_returned) == 0
    assert len(state_of(local).pending) == 1
    assert callbacks.counters["dropped_undeliverable"] == 0


def test_the_silence_timer_tracks_acks_not_qos0_writes(local):
    """on_publish fires on a local QoS 0 write too, which is not inbound data,
    so only a matched PUBACK refreshes the silence timer."""
    client = publish(local, {"qos": "1", "mqtt_version": "5"})
    callbacks = state_of(local).callbacks
    callbacks.last_inbound = 0.0
    plugin.on_publish(client, callbacks, 999, None, None)  # a QoS 0-style callback
    assert callbacks.last_inbound == 0.0
    plugin.on_publish(client, callbacks, 1, FakeReason(0, "Success"), None)  # a real PUBACK
    assert callbacks.last_inbound > 0.0


def test_the_status_pair_is_the_will_and_the_online_payload(local):
    client = publish(local, {"status_topic": "plant/status"})
    assert client.will == ("plant/status", "offline", 1, True)
    assert client.sent[0]["topic"] == "plant/status"
    assert client.sent[0]["payload"] == "online"


def test_a_changed_connection_setting_rebuilds_the_client(local):
    publish(local, {})
    publish(local, {"keep_alive": "60"})
    assert len(FakeClient.instances) == 2
    assert FakeClient.instances[1].keepalive == 60


def test_the_callback_object_holds_no_handle_on_the_state(local):
    """The no-cycle rule: nothing reachable from the client leads back."""
    publish(local, {})
    callbacks = state_of(local).callbacks
    assert all(
        not isinstance(getattr(callbacks, name), plugin.PublisherState)
        for name in plugin.CallbackState.__slots__
    )
    assert "unconfirmed" in plugin.CallbackState.__slots__


# --- buffering --------------------------------------------------------------


def test_nothing_is_handed_over_while_disconnected(local, monkeypatch):
    monkeypatch.setattr(plugin.mqtt, "Client", _refusing_client())
    publish(local, {})
    state = state_of(local)
    assert len(state.pending) == 1, "the row waits in the plugin's own buffer"
    assert local.messages("warn")


def _refusing_client():
    class Refusing(FakeClient):
        def connect(self, *args, **kwargs):
            raise ConnectionRefusedError(111, "Connection refused")

    return Refusing


def test_the_byte_budget_counts_more_than_the_payload(local, monkeypatch):
    monkeypatch.setattr(plugin.mqtt, "Client", _refusing_client())
    rows = rows_of(*[("press7", float(index), index) for index in range(50)])
    publish(local, {"max_queue_bytes": "4096"}, rows)
    state = state_of(local)
    assert state.pending_bytes <= 4096
    assert state.callbacks.counters["dropped_policy"] > 0
    # every entry is charged its topic and the Python object overhead
    entry = state.pending[0]
    assert entry.size == len(entry.payload) + len(entry.topic) + plugin.MESSAGE_OVERHEAD_BYTES


def test_drop_newest_refuses_the_append_instead(local, monkeypatch):
    monkeypatch.setattr(plugin.mqtt, "Client", _refusing_client())
    rows = rows_of(*[("press7", float(index), index) for index in range(20)])
    publish(local, {"max_queue_bytes": "2048", "drop_policy": "drop_newest"}, rows)
    state = state_of(local)
    assert json.loads(state.pending[0].payload)["temp"] == 0.0, "the earliest survives"


def test_a_stale_row_is_discarded_on_both_paths(local):
    """Checking only on drain would make the verdict depend on back-pressure."""
    rows = [dict(rows_of(("press7", 21.5, 7))[0], time=1)]
    client = publish(local, {"max_message_age": "5min"}, rows)
    assert client.sent == []
    assert state_of(local).callbacks.counters["dropped_age"] == 1


def test_mqtt5_expiry_is_the_remaining_life(local):
    rows = rows_of(("press7", 21.5, 7))
    client = publish(local, {"mqtt_version": "5", "max_message_age": "1h"}, rows)
    expiry = client.sent[0]["properties"].MessageExpiryInterval
    assert 0 < expiry <= 3600


def test_mqtt5_expiry_is_reduced_by_the_time_waited(local):
    """The broker counts expiry from receipt, so a buffered message must carry
    what is left of its life, not the whole limit."""
    message = plugin.Message("t", b"x", 1, False, time.time_ns() - 600 * 1_000_000_000)
    cfg = plugin.load_settings(
        local, dict(BASE_ARGS, mqtt_version="5", max_message_age="1h"), "t"
    )
    properties = plugin.publish_properties(cfg, message, time.time_ns())
    # Waited ~600s of a 3600s life, so ~3000s remain — never the full limit.
    assert 2900 < properties.MessageExpiryInterval < 3100


# --- configuration ----------------------------------------------------------


def test_layer_precedence_toml_over_arguments(local, tmp_path, monkeypatch):
    config = tmp_path / "publisher.toml"
    config.write_text('topic = "from-file"\nqos = 1\n')
    monkeypatch.setenv("INFLUXDB3_MQTT_PUBLISHER_RETAIN", "true")
    args = dict(BASE_ARGS, config_file_path=str(config), topic="from-args")
    cfg = plugin.load_settings(local, args, "t")
    assert cfg["topic"] == "from-file"
    assert cfg["qos"] == 1
    assert cfg["retain"] is True


def test_a_request_may_shape_the_message_but_not_the_connection(local):
    cfg = plugin.load_settings(
        local,
        dict(BASE_ARGS),
        "t",
        request_layers=(plugin.parse_json_body(
            {"topic": "replay/{machine}", "layout": "batch"}, plugin.BODY_KEYS
        ),),
    )
    assert cfg["topic"] == "replay/{machine}"
    assert cfg["layout"] == "batch"
    with pytest.raises(ValueError):
        plugin.parse_json_body({"broker_url": "mqtt://elsewhere"}, plugin.BODY_KEYS)
    with pytest.raises(ValueError):
        plugin.parse_json_body({"qos": "2"}, plugin.BODY_KEYS)


def test_a_misspelled_setting_is_refused_rather_than_ignored(local, tmp_path):
    with pytest.raises(ValueError, match="qso"):
        plugin.load_settings(local, dict(BASE_ARGS, qso="1"), "t")

    config = tmp_path / "publisher.toml"
    config.write_text('tpoic = "typo"\n')
    with pytest.raises(ValueError, match="tpoic"):
        plugin.load_settings(local, dict(BASE_ARGS, config_file_path=str(config)), "t")


def test_a_config_file_cannot_point_at_another_one(local, tmp_path):
    """The path names a layer, so only a trigger argument may set it."""
    config = tmp_path / "publisher.toml"
    config.write_text('config_file_path = "elsewhere.toml"\n')
    with pytest.raises(ValueError, match="config_file_path"):
        plugin.load_settings(local, dict(BASE_ARGS, config_file_path=str(config)), "t")


def test_max_message_age_below_settle_delay_is_refused(local):
    args = dict(BASE_ARGS, settle_delay="90s", max_message_age="60s")
    with pytest.raises(ValueError, match="max_message_age"):
        plugin.load_settings(local, args, "t")


@pytest.mark.parametrize("url", ["ftp://host:21", "mqtt://", "mqtt://:1883"])
def test_an_unusable_broker_url_is_a_configuration_error(local, url):
    """It will not start parsing after a backoff, so it is refused up front."""
    with pytest.raises(ValueError, match="broker_url"):
        plugin.load_settings(local, dict(BASE_ARGS, broker_url=url), "t")


def test_unreadable_tls_material_is_a_configuration_error(local, tmp_path):
    """Retrying never makes a missing file appear, so it is not a failed connect."""
    with pytest.raises(ValueError, match="tls_ca_cert: cannot read"):
        plugin.load_settings(local, dict(BASE_ARGS, tls_ca_cert=str(tmp_path / "no.pem")), "t")

    certificate = tmp_path / "client.pem"
    certificate.write_text("not really a certificate")
    with pytest.raises(ValueError, match="needs both"):
        plugin.load_settings(local, dict(BASE_ARGS, tls_cert=str(certificate)), "t")


def test_time_reaches_a_template_but_never_a_topic(local):
    """A topic per timestamp is a new topic per row; no consumer can use it."""
    client = publish(
        local,
        {"time_format": "ms", "payload_format": "template", "payload_template": "{time}"},
    )
    assert client.sent[0]["payload"].isdigit()

    with pytest.raises(ValueError, match=r"\{time\}"):
        plugin.load_settings(local, dict(BASE_ARGS, topic="plant/{machine}/{time}"), "t")


def test_a_row_encodes_the_same_whichever_trigger_read_it(local):
    """A write trigger sees the engine's column order, a query the statement's."""
    engine_order = {"machine": "m1", "line": "A", "temp": 1.5}
    query_order = {"line": "A", "machine": "m1", "temp": 1.5}
    assert plugin.encode_json(engine_order, 1, "ns", True) == plugin.encode_json(
        query_order, 1, "ns", True
    )
    assert plugin.encode_line_protocol(
        "t", {"b": "2", "a": "1"}, {"y": 2, "x": 1}, 1, {}
    ) == plugin.encode_line_protocol("t", {"a": "1", "b": "2"}, {"x": 1, "y": 2}, 1, {})


def test_a_topic_that_can_never_be_published_is_refused(local):
    """A placeholder-free topic is decided once, so it is decided with the settings."""
    for topic in ("$SYS/publisher", "", "plant//press7"):
        with pytest.raises(ValueError, match="topic"):
            plugin.load_settings(local, dict(BASE_ARGS, topic=topic), "t")

    # A value is still data, so the per-row check stays.
    assert plugin.render_topic("{machine}/x", {"machine": "$SYS"}, None)[0] is None


def test_a_template_without_its_format_is_refused(local):
    with pytest.raises(ValueError, match="payload_template"):
        plugin.load_settings(local, dict(BASE_ARGS, payload_format="template"), "t")


def test_a_placeholder_naming_no_column_is_refused(local):
    cfg = plugin.load_settings(local, dict(BASE_ARGS, topic="plant/{machne}"), "t")
    state = plugin.get_state(local, cfg)
    with pytest.raises(ValueError, match="machne"):
        plugin.validate_placeholders(local, state, cfg, "t")


def test_a_table_that_does_not_exist_yet_is_a_warning(local):
    cfg = plugin.load_settings(local, dict(BASE_ARGS, source_measurements="later"), "t")
    state = plugin.get_state(local, cfg)
    plugin.validate_placeholders(local, state, cfg, "t")
    assert any("no schema yet" in message for message in local.messages("warn"))


@pytest.mark.parametrize(
    "url, expected",
    [
        ("broker:1883", ("tcp", False, "broker", 1883)),
        ("mqtts://b.example.com", ("tcp", True, "b.example.com", 8883)),
        ("wss://b.example.com/ws", ("websockets", True, "b.example.com", 443)),
    ],
)
def test_broker_url_chooses_transport_and_port(url, expected):
    address = plugin.parse_broker_url(url)
    assert (address["transport"], address["tls"], address["host"], address["port"]) == expected


def test_instance_name_defaults_to_a_stable_hash(local):
    first = plugin.load_settings(local, dict(BASE_ARGS), "t")["instance_name"]
    second = plugin.load_settings(local, dict(BASE_ARGS), "t")["instance_name"]
    other = plugin.load_settings(local, dict(BASE_ARGS, topic="other"), "t")["instance_name"]
    assert first == second != other


# --- the scheduled trigger --------------------------------------------------


def scheduled_local(rows):
    return FakeLocal(rows={"press_line": rows})


def run_tick(local, args=None):
    plugin.process_scheduled_call(local, None, dict(BASE_ARGS, **(args or {})))
    return FakeClient.instances[-1] if FakeClient.instances else None


def test_a_cold_tick_publishes_nothing_and_anchors_the_watermark(local):
    local.rows["press_line"] = rows_of(("press7", 1.0, 1))
    client = run_tick(local)
    assert client is None or client.sent == []
    assert state_of(local).watermark["press_line"] is not None


def test_the_second_tick_publishes_what_appeared_since(local, monkeypatch):
    local.rows["press_line"] = []
    run_tick(local)
    state = state_of(local)
    watermark = state.watermark["press_line"]
    local.rows["press_line"] = [
        dict(row, time=watermark + 1 + index)
        for index, row in enumerate(rows_of(("press7", 1.0, 1), ("press8", 2.0, 2)))
    ]
    client = run_tick(local)
    assert len(client.sent) == 2
    assert state.watermark["press_line"] > watermark
    # nothing is published twice
    client = run_tick(local)
    assert len(client.sent) == 2


def test_a_timestamp_group_larger_than_the_window_still_advances(local):
    """The deadlock the page-at-a-time rule exists to prevent."""
    settings = {"qos": "1", "max_inflight": "2", "max_messages": "3", "tick_budget": "200ms"}
    local.rows["press_line"] = []
    run_tick(local, settings)
    state = state_of(local)
    base = state.watermark["press_line"] + 1
    local.rows["press_line"] = [
        dict(row, time=base) for row in rows_of(*[("press7", float(n), n) for n in range(5)])
    ]
    client = run_tick(local, settings)
    # the whole group is read and buffered rather than re-read every tick
    assert len(client.sent) + len(state.pending) == 5
    assert state.watermark["press_line"] == base or state.pending


def test_the_tick_does_not_read_while_the_broker_is_unreachable(local, monkeypatch):
    local.rows["press_line"] = rows_of(("press7", 1.0, 1))
    monkeypatch.setattr(plugin.mqtt, "Client", _refusing_client())
    run_tick(local)
    state = state_of(local)
    assert len(state.pending) == 0, "the database is the buffer, not memory"
    assert state.callbacks.counters["skipped_reads"] >= 1
    assert "press_line" not in state.watermark


def test_the_schema_cache_expires_after_its_ttl(local, monkeypatch):
    """Past the TTL the cache is a miss, so a scheduled read re-reads the catalog
    and picks up a column added after the first read; within it, it does not."""
    state = plugin.PublisherState("fp", "cid", 60)
    clock = [1000.0]
    monkeypatch.setattr(plugin.time, "monotonic", lambda: clock[0])
    plugin.cache_schema(state, "press_line", {"columns": ["temp"]})
    assert plugin.cached_schema(state, "press_line") is not None

    clock[0] += plugin.SCHEMA_TTL_SECONDS - 1
    assert plugin.cached_schema(state, "press_line") is not None, "still fresh under the TTL"

    clock[0] += 2
    assert plugin.cached_schema(state, "press_line") is None, "a miss past the TTL"


def _raise_schema(*args, **kwargs):
    raise RuntimeError("catalog temporarily unavailable")


def test_a_schema_error_leaves_the_scheduled_watermark_put(local, monkeypatch):
    """A failed schema read retries the whole page next tick rather than
    dropping its tail and advancing past it."""
    local.rows["press_line"] = []
    run_tick(local)
    watermark = state_of(local).watermark["press_line"]
    local.rows["press_line"] = [
        dict(row, time=watermark + 1 + index)
        for index, row in enumerate(rows_of(("press7", 1.0, 1), ("press8", 2.0, 2)))
    ]
    monkeypatch.setattr(plugin, "known_schema", _raise_schema)
    run_tick(local)
    assert state_of(local).watermark["press_line"] == watermark
    assert len(state_of(local).pending) == 0


def test_a_schema_error_on_a_write_counts_the_batch(local, monkeypatch):
    """A write batch cannot be re-read, so rows it cannot build are counted."""
    monkeypatch.setattr(plugin, "known_schema", _raise_schema)
    publish(local, {}, rows_of(("press7", 1.0, 1), ("press8", 2.0, 2)))
    assert state_of(local).callbacks.counters["failed"] == 2


def test_the_timestamp_is_always_selected(local):
    """`get_schema` leaves `time` out, so filtering the column list through it
    would drop the timestamp from the statement — no cursor, no payload time."""
    cfg = plugin.load_settings(local, dict(BASE_ARGS, payload_fields="temp"), "t")
    schema = plugin.table_schema(local, "press_line")
    assert "time" not in schema["columns"], "the fake must match the real get_schema"
    assert plugin.select_columns(cfg, schema).startswith('"time"')


def test_filters_reach_the_query_rather_than_the_rows(local):
    local.rows["press_line"] = []
    run_tick(local, {"filters": "temp:'>=185'"})  # cold tick only anchors the watermark
    local.rows["press_line"] = rows_of(
        ("press7", 200.0, 1), base_ns=state_of(local).watermark["press_line"] + 1
    )
    run_tick(local, {"filters": "temp:'>=185'"})
    page_queries = [query for query, _ in local.queries if 'FROM "press_line"' in query]
    assert page_queries, "a page was read"
    # a numeric value is inlined (the runtime binds only string parameters)
    assert any('"temp" >= 185' in query for query in page_queries)
    assert all("SELECT *" not in query for query, _ in local.queries)


# --- the HTTP trigger -------------------------------------------------------


@pytest.mark.parametrize(
    "nanos",
    [1_791_700_004_000_000_000, 1_791_700_004_000_000_001, 1_791_699_999_999_999_999],
)
def test_a_timestamp_survives_the_round_trip(nanos):
    """`resume_from` is `reached + 1ns`; a parser holding microseconds would
    hand back `reached` and republish the row on the seam."""
    rendered = plugin.format_timestamp(nanos, "rfc3339")
    assert plugin.parse_timestamp_argument(rendered, "t") == nanos


def test_a_backfill_pages_a_range_and_reports_where_to_resume(local):
    rows = rows_of(*[("press7", float(n), n) for n in range(5)])
    local.rows["press_line"] = rows
    answer, status = plugin.process_request(
        local,
        None,
        None,
        {"backfill_start": "2026-10-11T00:00:00Z"},
        dict(BASE_ARGS, max_messages="2"),
    )
    assert status == 200
    assert answer["complete"] is True
    assert answer["published"] == 5
    assert answer["resume_from"] > answer["reached"]


def test_a_malformed_timestamp_is_a_configuration_error(local):
    """It must be refused before the call starts, not half way through it."""
    with pytest.raises(ValueError, match="backfill_start is not an RFC 3339"):
        plugin.load_settings(local, dict(BASE_ARGS, backfill_start="not-a-time"), "t")
    answer, status = plugin.process_request(
        local, None, None, {"backfill_start": "not-a-time"}, dict(BASE_ARGS)
    )
    assert status == 400


def test_an_inverted_range_publishes_nothing_without_querying(local):
    """The server refuses an inverted time predicate outright, so never send one."""
    local.rows["press_line"] = rows_of(("press7", 1.0, 1))
    before = len(local.queries)
    answer, status = plugin.process_request(
        local,
        None,
        None,
        {"backfill_start": "2026-10-11T06:26:49Z", "backfill_end": "2026-10-11T06:26:41Z"},
        dict(BASE_ARGS),
    )
    assert (status, answer["published"], answer["complete"]) == (200, 0, True)
    assert not any(
        "to_timestamp_nanos" in query for query, _ in local.queries[before:]
    ), "no query may be issued for an empty range"


def test_a_backfill_stops_at_its_cap(local):
    local.rows["press_line"] = rows_of(*[("press7", float(n), n) for n in range(10)])
    answer, _ = plugin.process_request(
        local, None, None, {}, dict(BASE_ARGS, max_messages="2", max_backfill_messages="4")
    )
    assert answer["complete"] is False
    assert answer["published"] == 4
    assert answer["resume_from"]


def test_a_backfill_leaves_the_watermark_alone(local):
    local.rows["press_line"] = rows_of(("press7", 1.0, 1))
    run_tick(local)
    state = state_of(local)
    before = state.watermark["press_line"]
    plugin.process_request(local, None, None, {}, dict(BASE_ARGS))
    assert state.watermark["press_line"] == before


def test_a_request_naming_a_quoted_table_fails_cleanly(local):
    answer, status = plugin.process_request(
        local, None, None, {"source_measurements": 'we"rd'}, dict(BASE_ARGS)
    )
    assert status == 400
    assert "error" in answer


# --- logging ----------------------------------------------------------------


def test_a_declared_limit_outlives_the_invocation_that_learned_it(local, monkeypatch):
    """`cfg` is rebuilt every invocation while the client is cached across
    them, so a clamp applied only at connect time is gone by the next one. The
    packet cap also reserves the protocol overhead below the broker's limit."""

    class Strict(FakeClient):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.connack_properties = SimpleNamespace(
                ServerKeepAlive=None,
                AssignedClientIdentifier=None,
                ReceiveMaximum=5,
                MaximumPacketSize=1024,
            )

    payload_cap = 1024 - plugin.PACKET_OVERHEAD_RESERVE
    monkeypatch.setattr(plugin.mqtt, "Client", Strict)
    publish(local, {})
    state = state_of(local)
    assert (state.packet_size_limit, state.inflight_limit) == (payload_cap, 5)

    # a second invocation starts from fresh settings; the limits must still hold
    cfg = plugin.load_settings(local, dict(BASE_ARGS), "t")
    assert cfg["max_payload_bytes"] > payload_cap
    plugin.apply_declared_limits(state, cfg)
    assert (cfg["max_payload_bytes"], cfg["max_inflight"]) == (payload_cap, 5)


def test_a_refused_connack_names_its_reason_code(local, monkeypatch):
    """The code is the diagnosis, so it is logged beside the exception rather
    than inside a message `enable_full_logging` withholds."""

    class Refusing(FakeClient):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.connack = FakeReason(0x87, "Not authorized")

    monkeypatch.setattr(plugin.mqtt, "Client", Refusing)
    publish(local, {})
    assert any(
        "0x87" in message and "Not authorized" in message
        for message in local.messages("warn")
    )


def test_the_diagnosis_is_logged_even_without_full_logging():
    failure = ConnectionRefusedError(111, "Connection refused")
    quiet = plugin.describe_error(failure, False)
    assert "ConnectionRefusedError" in quiet and "111" in quiet
    assert "mqtt://secret" not in plugin.describe_error(
        ValueError("mqtt://secret:pass@host"), False
    )
    assert "mqtt://secret" in plugin.describe_error(
        ValueError("mqtt://secret:pass@host"), True
    )


def test_statistics_tag_the_broker_and_keep_client_id_a_field(local):
    publish(local, {"stats_interval": "1s"})
    assert local.writes, "a summary row is written"
    line = local.writes[-1]
    assert set(line.tags) == {"broker", "instance_name"}
    assert "client_id" in line.fields


def test_stats_interval_zero_silences_the_table_not_the_log(local):
    publish(local, {"stats_interval": "0"})
    assert local.writes == [], "no measurement is written"
    assert any("published=" in message for message in local.messages("info"))
