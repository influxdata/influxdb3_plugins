import pytest

# Vendored from influxdb_pro/ent/influxdb3_py_api/src/system_py.rs:493-614
# (Python class definitions injected into the plugin runtime at server startup.)

from collections import OrderedDict
from typing import Optional


class InfluxDBError(Exception):
    pass


class InvalidMeasurementError(InfluxDBError):
    pass


class InvalidKeyError(InfluxDBError):
    pass


class InvalidLineError(InfluxDBError):
    pass


class LineBuilder:
    def __init__(self, measurement: str):
        if " " in measurement:
            raise InvalidMeasurementError("Measurement name cannot contain spaces")
        self.measurement = measurement
        self.tags: OrderedDict[str, str] = OrderedDict()
        self.fields: OrderedDict[str, str] = OrderedDict()
        self._timestamp_ns: Optional[int] = None

    def _validate_key(self, key: str, key_type: str) -> None:
        """Validate that a key does not contain spaces, commas, or equals signs."""
        if not key:
            raise InvalidKeyError(f"{key_type} key cannot be empty")
        if " " in key:
            raise InvalidKeyError(f"{key_type} key '{key}' cannot contain spaces")
        if "," in key:
            raise InvalidKeyError(f"{key_type} key '{key}' cannot contain commas")
        if "=" in key:
            raise InvalidKeyError(f"{key_type} key '{key}' cannot contain equals signs")

    def _escape_measurement(self, value: str) -> str:
        """Escape characters in measurement names according to line protocol."""
        return value.replace(",", "\\,").replace(" ", "\\ ")

    def _escape_tag_value(self, value: str) -> str:
        """Escape characters in tag values according to line protocol."""
        return (
            value.replace("\\", "\\\\")
            .replace(",", "\\,")
            .replace("=", "\\=")
            .replace(" ", "\\ ")
        )

    def _escape_field_key(self, value: str) -> str:
        """Escape characters in field keys according to line protocol."""
        return (
            value.replace("\\", "\\\\")
            .replace(",", "\\,")
            .replace("=", "\\=")
            .replace(" ", "\\ ")
        )

    def tag(self, key: str, value: str) -> "LineBuilder":
        """Add a tag to the line protocol."""
        self._validate_key(key, "tag")
        self.tags[key] = str(value)
        return self

    def uint64_field(self, key: str, value: int) -> "LineBuilder":
        """Add an unsigned integer field to the line protocol."""
        self._validate_key(key, "field")
        if value < 0:
            raise ValueError(f"uint64 field '{key}' cannot be negative")
        self.fields[key] = f"{value}u"
        return self

    def int64_field(self, key: str, value: int) -> "LineBuilder":
        """Add an integer field to the line protocol."""
        self._validate_key(key, "field")
        self.fields[key] = f"{value}i"
        return self

    def float64_field(self, key: str, value: float) -> "LineBuilder":
        """Add a float field to the line protocol."""
        self._validate_key(key, "field")
        # Check if value has no decimal component
        self.fields[key] = f"{int(value)}.0" if value % 1 == 0 else str(value)
        return self

    def string_field(self, key: str, value: str) -> "LineBuilder":
        """Add a string field to the line protocol."""
        self._validate_key(key, "field")
        # Escape quotes and backslashes in string values
        escaped_value = value.replace("\\", "\\\\").replace('"', '\\"')
        self.fields[key] = f'"{escaped_value}"'
        return self

    def bool_field(self, key: str, value: bool) -> "LineBuilder":
        """Add a boolean field to the line protocol."""
        self._validate_key(key, "field")
        self.fields[key] = "t" if value else "f"
        return self

    def time_ns(self, timestamp_ns: int) -> "LineBuilder":
        """Set the timestamp in nanoseconds."""
        self._timestamp_ns = timestamp_ns
        return self

    def build(self) -> str:
        """Build the line protocol string."""
        # Start with measurement name (escape commas and spaces)
        line = self._escape_measurement(self.measurement)

        # Add tags if present
        if self.tags:
            tags_str = ",".join(
                f"{key}={self._escape_tag_value(value)}"
                for key, value in self.tags.items()
            )
            line += f",{tags_str}"

        # Add fields (required)
        if not self.fields:
            raise InvalidLineError(f"At least one field is required: {line}")

        fields_str = ",".join(
            f"{self._escape_field_key(key)}={value}"
            for key, value in self.fields.items()
        )
        line += f" {fields_str}"

        # Add timestamp if present
        if self._timestamp_ns is not None:
            line += f" {self._timestamp_ns}"

        return line


class FakeCache:
    def __init__(self):
        self._d = {}
        self._ttls = {}

    def get(self, key, default=None, use_global=None):
        return self._d.get(key, default)

    def put(self, key, value, ttl=None, use_global=None):
        self._d[key] = value
        self._ttls[key] = ttl

    def delete(self, key, use_global=None):
        return self._d.pop(key, None) is not None


class FakeInfluxdb3Local:
    def __init__(self):
        self.cache = FakeCache()
        self.logs = []
        self.writes = []
        self.cross_db_writes = []
        self.fail_write = False
        self.query_responses = {}
        self.queries_run = []

    def info(self, m):
        self.logs.append(("info", m))

    def warn(self, m):
        self.logs.append(("warn", m))

    def error(self, m):
        self.logs.append(("error", m))

    def write_sync(self, line, no_sync=False):
        if self.fail_write:
            raise RuntimeError("simulated write failure")
        self.writes.append(line.build())

    def write_sync_to_db(self, db_name, line, no_sync=False):
        if self.fail_write:
            raise RuntimeError("simulated write failure")
        self.cross_db_writes.append((db_name, line.build()))

    def query(self, sql, params=None):
        self.queries_run.append((sql, params))
        for substr, rows in self.query_responses.items():
            if substr in sql:
                return rows
        return []


def test_vendored_line_builder_basic():
    lb = LineBuilder("m")
    lb.tag("host", "a").int64_field("n", 3).time_ns(1731000000000000000)
    assert lb.build() == "m,host=a n=3i 1731000000000000000"


from valuecounter import _stringify_value


def test_stringify_int():
    assert _stringify_value(200) == "200"


def test_stringify_float():
    assert _stringify_value(1.5) == "1.5"


def test_stringify_str():
    assert _stringify_value("OK") == "OK"


def test_stringify_bool_true():
    assert _stringify_value(True) == "true"  # telegraf-style lowercase


def test_stringify_bool_false():
    assert _stringify_value(False) == "false"


def test_stringify_none():
    assert _stringify_value(None) is None  # signals "skip"


def test_stringify_unknown_falls_back_to_repr():
    class Weird: ...

    out = _stringify_value(Weird())
    assert isinstance(out, str)
    assert "Weird" in out


from valuecounter import _sanitize_field_name


def test_sanitize_no_change_for_plain():
    assert _sanitize_field_name("status_200") == "status_200"


def test_sanitize_spaces():
    assert _sanitize_field_name("not found") == "not_found"


def test_sanitize_hyphen():
    assert _sanitize_field_name("User-Agent") == "User_Agent"


def test_sanitize_punctuation():
    assert (
        _sanitize_field_name("500 Internal Server Error") == "500_Internal_Server_Error"
    )


def test_sanitize_unicode_replaced():
    assert _sanitize_field_name("café") == "caf_"


from valuecounter import _validate_identifier


def test_validate_accepts_plain():
    _validate_identifier("http_requests", "table")
    _validate_identifier("status", "field")


def test_validate_accepts_leading_underscore():
    _validate_identifier("_internal", "table")


def test_validate_rejects_leading_digit():
    with pytest.raises(ValueError):
        _validate_identifier("1status", "field")


def test_validate_rejects_space():
    with pytest.raises(ValueError):
        _validate_identifier("status code", "field")


def test_validate_rejects_dash():
    with pytest.raises(ValueError):
        _validate_identifier("user-agent", "field")


def test_validate_rejects_semicolon():
    with pytest.raises(ValueError):
        _validate_identifier("status;DROP TABLE users", "table")


def test_validate_rejects_quote():
    with pytest.raises(ValueError):
        _validate_identifier('status"', "field")


def test_validate_rejects_too_long():
    with pytest.raises(ValueError):
        _validate_identifier("a" * 129, "table")


def test_validate_message_includes_what_and_value():
    with pytest.raises(ValueError, match="invalid table:.*'bad name'"):
        _validate_identifier("bad name", "table")


from valuecounter import _parse_period_seconds


def test_parse_seconds():
    assert _parse_period_seconds("60s") == 60


def test_parse_legacy_bare_minutes():
    assert _parse_period_seconds("5m") == 300


def test_parse_minutes():
    assert _parse_period_seconds("5min") == 300


def test_parse_hours():
    assert _parse_period_seconds("1h") == 3600


def test_parse_days():
    assert _parse_period_seconds("1d") == 86400


def test_parse_with_whitespace():
    assert _parse_period_seconds("  30 s  ") == 30


def test_parse_rejects_missing_unit():
    with pytest.raises(ValueError, match="Invalid duration"):
        _parse_period_seconds("60")


def test_parse_rejects_sub_second():
    with pytest.raises(ValueError, match="at least 1 second"):
        _parse_period_seconds("60ms")


def test_parse_rejects_empty():
    with pytest.raises(ValueError):
        _parse_period_seconds("")


from valuecounter import _resolve_config


def _write_tmp_toml(tmp_path, content):
    path = tmp_path / "vc.toml"
    path.write_text(content)
    return path


def test_resolve_inline_mode_a_minimal():
    cfg = _resolve_config({"fields": "status method"}, mode="wal")
    assert cfg.fields == ["status", "method"]
    assert cfg.output_suffix == "_valuecounts"
    assert cfg.dest_database == ""
    assert cfg.period_seconds == 60
    assert cfg.table == ""


def test_resolve_inline_mode_b_minimal():
    cfg = _resolve_config(
        {"table": "http_requests", "fields": "status"}, mode="scheduled"
    )
    assert cfg.table == "http_requests"
    assert cfg.fields == ["status"]


def test_resolve_inline_mode_a_full():
    cfg = _resolve_config(
        {
            "fields": "status method",
            "output_suffix": "_vc",
            "period_seconds": "30",
            "dest_database": "rollups",
        },
        mode="wal",
    )
    assert cfg.period_seconds == 30
    assert cfg.output_suffix == "_vc"
    assert cfg.dest_database == "rollups"


def test_resolve_period_seconds_overrides_period():
    cfg = _resolve_config(
        {"fields": "a", "period": "5m", "period_seconds": "30"}, mode="wal"
    )
    assert cfg.period_seconds == 30


def test_resolve_rejects_unknown_arg():
    with pytest.raises(ValueError, match="unknown arg"):
        _resolve_config({"fields": "a", "bogus": "x"}, mode="wal")


def test_resolve_mode_a_rejects_table():
    with pytest.raises(ValueError, match="trigger-spec.*inline args"):
        _resolve_config({"fields": "a", "table": "http_requests"}, mode="wal")


def test_resolve_mode_b_rejects_period_seconds():
    with pytest.raises(ValueError, match="drift-based"):
        _resolve_config(
            {"table": "t", "fields": "a", "period_seconds": "60"}, mode="scheduled"
        )


def test_resolve_mode_b_rejects_period():
    with pytest.raises(ValueError, match="drift-based"):
        _resolve_config(
            {"table": "t", "fields": "a", "period": "60s"}, mode="scheduled"
        )


def test_resolve_rejects_unknown_mode():
    with pytest.raises(ValueError, match="unknown mode"):
        _resolve_config({"fields": "a"}, mode="bogus")


def test_resolve_toml_mode_a(monkeypatch, tmp_path):
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    _write_tmp_toml(tmp_path, 'fields = ["status", "method"]\nperiod = "30s"\n')
    cfg = _resolve_config({"config_file_path": "vc.toml"}, mode="wal")
    assert cfg.fields == ["status", "method"]
    assert cfg.period_seconds == 30


def test_resolve_toml_mode_b(monkeypatch, tmp_path):
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    _write_tmp_toml(tmp_path, 'table = "http_requests"\nfields = ["status"]\n')
    cfg = _resolve_config({"config_file_path": "vc.toml"}, mode="scheduled")
    assert cfg.table == "http_requests"
    assert cfg.fields == ["status"]


def test_resolve_toml_rejects_unknown_key(monkeypatch, tmp_path):
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    _write_tmp_toml(tmp_path, 'fields = ["a"]\nbogus = 1\n')
    with pytest.raises(ValueError, match="unknown TOML key"):
        _resolve_config({"config_file_path": "vc.toml"}, mode="wal")


def test_resolve_toml_parse_error(monkeypatch, tmp_path):
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    _write_tmp_toml(tmp_path, "this is not valid toml = =")
    with pytest.raises(ValueError):
        _resolve_config({"config_file_path": "vc.toml"}, mode="wal")


def test_resolve_toml_missing_file(monkeypatch, tmp_path):
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    with pytest.raises(FileNotFoundError):
        _resolve_config({"config_file_path": "definitely_not_here.toml"}, mode="wal")


def test_resolve_toml_path_needs_plugin_dir(monkeypatch):
    for name in ("PLUGIN_DIR", "INFLUXDB3_PLUGIN_DIR", "VIRTUAL_ENV"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(ValueError, match="Cannot resolve plugin directory"):
        _resolve_config({"config_file_path": "vc.toml"}, mode="wal")


def test_resolve_neither_fields_nor_toml_raises():
    with pytest.raises(ValueError, match="fields"):
        _resolve_config({}, mode="wal")


def test_resolve_empty_fields_raises():
    with pytest.raises(ValueError, match="empty"):
        _resolve_config({"fields": ""}, mode="wal")


def test_resolve_mode_b_without_table_raises():
    with pytest.raises(ValueError, match="table"):
        _resolve_config({"fields": "status"}, mode="scheduled")


def test_resolve_both_inline_and_toml_raises(monkeypatch, tmp_path):
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    _write_tmp_toml(tmp_path, 'fields = ["status"]\n')
    with pytest.raises(ValueError, match="either.*both"):
        _resolve_config({"config_file_path": "vc.toml", "fields": "status"}, mode="wal")


def test_resolve_validates_identifier_table():
    with pytest.raises(ValueError, match="invalid table"):
        _resolve_config(
            {"table": "http requests", "fields": "status"}, mode="scheduled"
        )


def test_resolve_validates_identifier_field():
    with pytest.raises(ValueError, match="invalid field"):
        _resolve_config({"fields": "status method-name"}, mode="wal")


def test_resolve_rejects_empty_output_suffix():
    with pytest.raises(ValueError, match="output_suffix"):
        _resolve_config({"fields": "status", "output_suffix": ""}, mode="wal")


from valuecounter import _series_key


def test_series_key_stable_across_tag_order():
    k1 = _series_key("http_requests", {"host": "web-1", "endpoint": "/api"})
    k2 = _series_key("http_requests", {"endpoint": "/api", "host": "web-1"})
    assert k1 == k2


def test_series_key_distinct_for_distinct_tags():
    k1 = _series_key("http_requests", {"host": "web-1"})
    k2 = _series_key("http_requests", {"host": "web-2"})
    assert k1 != k2


def test_series_key_includes_table():
    k1 = _series_key("table_a", {"host": "x"})
    k2 = _series_key("table_b", {"host": "x"})
    assert k1 != k2
    assert "table_a" in k1
    assert "table_b" in k2


def test_series_key_empty_tags():
    k = _series_key("http_requests", {})
    assert k.startswith("http_requests:")


from valuecounter import _extract_tags


def test_extract_tags_subset():
    row = {"host": "web-1", "endpoint": "/api", "status": 200, "time": 123}
    out = _extract_tags(row, ["host", "endpoint"])
    assert out == {"host": "web-1", "endpoint": "/api"}


def test_extract_tags_missing_tag_returns_partial():
    row = {"host": "web-1", "status": 200}
    out = _extract_tags(row, ["host", "endpoint"])
    assert out == {"host": "web-1"}


def test_extract_tags_empty_list():
    row = {"status": 200}
    assert _extract_tags(row, []) == {}


def test_extract_tags_null_tag_returns_none():
    row = {"host": "web-1", "endpoint": None}
    assert _extract_tags(row, ["host", "endpoint"]) is None


import valuecounter
from valuecounter import _build_rollup_line


@pytest.fixture(autouse=True)
def _inject_line_builder(monkeypatch):
    """The engine injects LineBuilder as a global; tests use the vendored copy."""
    monkeypatch.setattr(valuecounter, "LineBuilder", LineBuilder, raising=False)


def test_build_line_basic():
    lb = _build_rollup_line(
        "http_requests",
        {"host": "web-1"},
        {"status_200": 3, "status_500": 1},
        "_valuecounts",
        1731000060000000000,
    )
    out = lb.build()
    assert out.startswith("http_requests_valuecounts,host=web-1 ")
    assert "status_200=3i" in out
    assert "status_500=1i" in out
    assert out.endswith(" 1731000060000000000")


def test_build_line_tagless():
    lb = _build_rollup_line("t", {}, {"a_1": 2}, "_vc", 1)
    assert lb.build() == "t_vc a_1=2i 1"


def test_build_line_empty_counts_raises():
    with pytest.raises(ValueError, match="no fields"):
        _build_rollup_line("t", {}, {}, "_vc", 1)


from valuecounter import _build_scheduled_query


def test_build_query_with_tags():
    sql = _build_scheduled_query("http_requests", ["host", "endpoint"], "status")
    # Identifiers double-quoted; time predicate uses parameters
    assert '"http_requests"' in sql
    assert '"host"' in sql and '"endpoint"' in sql
    assert '"status"' in sql
    assert "$start_ns" in sql and "$end_ns" in sql
    assert "GROUP BY" in sql
    assert "COUNT(*)" in sql.upper() or "count(*)" in sql


def test_build_query_tagless():
    sql = _build_scheduled_query("t", [], "field_a")
    # No tag projection prefix or GROUP BY tag prefix; only the watched field
    assert '"t"' in sql
    assert '"field_a"' in sql
    assert "GROUP BY" in sql
    # No leading-comma artifacts
    assert ",  " not in sql
    assert "SELECT ," not in sql.replace(" ", "")[:8]


from valuecounter import _query_field_distribution


def test_query_field_distribution_calls_query_with_string_params():
    fake = FakeInfluxdb3Local()
    fake.query_responses['"http_requests"'] = [
        {"host": "a", "status": "200", "cnt": 3},
    ]
    rows = _query_field_distribution(
        fake,
        "http_requests",
        ["host"],
        "status",
        start_ns=1_000_000_000,
        end_ns=2_000_000_000,
    )
    assert rows == [{"host": "a", "status": "200", "cnt": 3}]
    # Verify params dict was decimal strings
    sql, params = fake.queries_run[-1]
    assert params == {"start_ns": "1000000000", "end_ns": "2000000000"}
    assert '"http_requests"' in sql
    assert '"host"' in sql and '"status"' in sql


from datetime import datetime, timezone
from valuecounter import process_scheduled_call


def _dt_ns(ns):
    """Return a datetime that corresponds to the given ns timestamp."""
    return datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc)


def test_scheduled_first_fire_sets_anchor_no_emit():
    fake = FakeInfluxdb3Local()

    call_time = _dt_ns(1_000_000_000_000_000_000)
    process_scheduled_call(
        fake,
        call_time,
        args={"table": "http_requests", "fields": "status"},
    )

    # Anchor written, no rollup emitted
    assert (
        fake.cache._d.get("vc:scheduled:last_call_ns:http_requests")
        == 1_000_000_000_000_000_000
    )
    assert fake.writes == []
    # Info log emitted
    assert any("first fire" in m for _, m in fake.logs)


def test_scheduled_second_fire_emits_and_advances_anchor():
    fake = FakeInfluxdb3Local()

    # Pre-seed anchor and tag-name cache
    fake.cache.put(
        "vc:scheduled:last_call_ns:http_requests", 1_000_000_000_000_000_000, ttl=None
    )
    fake.cache.put("shared:tags:http_requests", ["host"], ttl=3600)
    fake.query_responses['"status"'] = [
        {"host": "a", "status": "200", "cnt": 3},
        {"host": "a", "status": "500", "cnt": 1},
    ]

    call_time = _dt_ns(2_000_000_000_000_000_000)
    process_scheduled_call(
        fake,
        call_time,
        args={"table": "http_requests", "fields": "status"},
    )

    assert len(fake.writes) == 1
    out = fake.writes[0]
    assert "http_requests_valuecounts,host=a status_200=3i,status_500=1i" in out
    assert out.endswith(" 2000000000000000000")
    # Anchor advanced
    assert (
        fake.cache._d["vc:scheduled:last_call_ns:http_requests"]
        == 2_000_000_000_000_000_000
    )


def test_scheduled_write_failure_does_not_advance_anchor():
    fake = FakeInfluxdb3Local()

    fake.cache.put("vc:scheduled:last_call_ns:t", 1_000_000_000_000_000_000, ttl=None)
    fake.cache.put("shared:tags:t", ["host"], ttl=3600)
    fake.query_responses['"f"'] = [{"host": "a", "f": "x", "cnt": 1}]
    fake.fail_write = True

    process_scheduled_call(
        fake,
        _dt_ns(2_000_000_000_000_000_000),
        args={"table": "t", "fields": "f"},
    )

    assert fake.cache._d["vc:scheduled:last_call_ns:t"] == 1_000_000_000_000_000_000
    assert any("write failed" in m for _, m in fake.logs)


def test_scheduled_empty_window_advances_anchor():
    fake = FakeInfluxdb3Local()

    fake.cache.put("vc:scheduled:last_call_ns:t", 1_000_000_000_000_000_000, ttl=None)
    fake.cache.put("shared:tags:t", ["host"], ttl=3600)
    # query_responses empty → no rows in window

    process_scheduled_call(
        fake,
        _dt_ns(2_000_000_000_000_000_000),
        args={"table": "t", "fields": "f"},
    )

    assert fake.writes == []
    assert fake.cache._d["vc:scheduled:last_call_ns:t"] == 2_000_000_000_000_000_000
    assert any("no rows" in m for _, m in fake.logs)


def test_scheduled_cross_db_write():
    fake = FakeInfluxdb3Local()

    fake.cache.put("vc:scheduled:last_call_ns:t", 1_000_000_000_000_000_000, ttl=None)
    fake.cache.put("shared:tags:t", [], ttl=3600)  # tagless
    fake.query_responses['"f"'] = [{"f": "x", "cnt": 5}]

    process_scheduled_call(
        fake,
        _dt_ns(2_000_000_000_000_000_000),
        args={"table": "t", "fields": "f", "dest_database": "rollups"},
    )

    assert fake.writes == []
    assert len(fake.cross_db_writes) == 1
    db, lp = fake.cross_db_writes[0]
    assert db == "rollups"
    assert lp.startswith("t_valuecounts f_x=5i")


def test_scheduled_sanitization_collision_sums_and_warns():
    fake = FakeInfluxdb3Local()

    fake.cache.put("vc:scheduled:last_call_ns:t", 1_000_000_000_000_000_000, ttl=None)
    fake.cache.put("shared:tags:t", ["host"], ttl=3600)
    fake.query_responses['"f"'] = [
        {"host": "a", "f": "not found", "cnt": 2},  # sanitized: f_not_found
        {"host": "a", "f": "not_found", "cnt": 3},  # already plain: f_not_found
    ]

    process_scheduled_call(
        fake,
        _dt_ns(2_000_000_000_000_000_000),
        args={"table": "t", "fields": "f"},
    )

    out = fake.writes[0]
    assert "f_not_found=5i" in out
    assert any("collision" in m for lvl, m in fake.logs if lvl == "warn")


def test_scheduled_call_time_ns_preserves_microseconds():
    fake = FakeInfluxdb3Local()

    fake.cache.put("vc:scheduled:last_call_ns:t", 1_700_000_000_000_000_000, ttl=None)
    fake.cache.put("shared:tags:t", ["host"], ttl=3600)
    fake.query_responses['"f"'] = [{"host": "a", "f": "x", "cnt": 1}]

    # datetime with microseconds = 123456 -> expected ns suffix carries it exactly
    from datetime import datetime, timezone

    call_time = datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=timezone.utc)
    expected_ns = int(call_time.timestamp()) * 1_000_000_000 + 123456 * 1000

    process_scheduled_call(fake, call_time, args={"table": "t", "fields": "f"})

    assert fake.writes, "expected one rollup write"
    assert fake.writes[0].endswith(f" {expected_ns}")


from valuecounter import process_writes


def test_wal_first_batch_accumulates_no_emit(monkeypatch):
    fake = FakeInfluxdb3Local()
    fake.cache.put("shared:tags:http_requests", ["host"], ttl=3600)

    # Freeze time so period gate cannot fire
    fixed_now = 1_000_000_000_000_000_000
    monkeypatch.setattr("valuecounter.time.time_ns", lambda: fixed_now)

    table_batches = [
        {
            "table_name": "http_requests",
            "rows": [
                {"host": "web-1", "status": 200, "time": fixed_now - 1000},
                {"host": "web-1", "status": 500, "time": fixed_now - 500},
                {"host": "web-1", "status": 200, "time": fixed_now - 100},
            ],
        }
    ]

    process_writes(
        fake, table_batches, args={"fields": "status", "period_seconds": "60"}
    )

    # No emit yet — last_emit_ns == now_ns means period not elapsed
    assert fake.writes == []
    # Index has one series hash; series entry has counts
    idx_key = "vc:wal:_index:http_requests"
    sh_list = fake.cache._d[idx_key]
    assert len(sh_list) == 1
    series_entry = fake.cache._d[f"vc:wal:{sh_list[0]}"]
    assert series_entry["tags"] == {"host": "web-1"}
    assert series_entry["counts"] == {"status_200": 2, "status_500": 1}
    assert series_entry["last_emit_ns"] == fixed_now


def test_wal_period_elapsed_emits_and_subtracts(monkeypatch):
    fake = FakeInfluxdb3Local()
    fake.cache.put("shared:tags:t", ["host"], ttl=3600)

    # Pre-seed series state with last_emit_ns far in the past
    sh = _series_key("t", {"host": "a"})
    fake.cache.put(
        f"vc:wal:{sh}",
        {
            "table": "t",
            "tags": {"host": "a"},
            "counts": {"status_200": 5},
            "last_emit_ns": 0,
        },
        ttl=120,
    )
    fake.cache.put("vc:wal:_index:t", [sh], ttl=120)

    fixed_now = 1_000_000_000_000_000_000
    monkeypatch.setattr("valuecounter.time.time_ns", lambda: fixed_now)

    # New batch adds two more 200s (so during snapshot counts=5, after add counts=7)
    table_batches = [
        {
            "table_name": "t",
            "rows": [
                {"host": "a", "status": 200, "time": fixed_now - 100},
                {"host": "a", "status": 200, "time": fixed_now - 50},
            ],
        }
    ]

    process_writes(
        fake, table_batches, args={"fields": "status", "period_seconds": "60"}
    )

    # One emit with counts=7 (5 pre + 2 new)
    assert len(fake.writes) == 1
    assert "status_200=7i" in fake.writes[0]
    # Subtract leaves counts = max(7-7, 0) = removed
    new_state = fake.cache._d[f"vc:wal:{sh}"]
    assert new_state["counts"] == {}
    assert new_state["last_emit_ns"] == fixed_now


def test_wal_write_failure_retains_counts(monkeypatch):
    fake = FakeInfluxdb3Local()
    fake.cache.put("shared:tags:t", ["host"], ttl=3600)

    sh = _series_key("t", {"host": "a"})
    fake.cache.put(
        f"vc:wal:{sh}",
        {
            "table": "t",
            "tags": {"host": "a"},
            "counts": {"status_200": 3},
            "last_emit_ns": 0,
        },
        ttl=120,
    )
    fake.cache.put("vc:wal:_index:t", [sh], ttl=120)

    fixed_now = 1_000_000_000_000_000_000
    monkeypatch.setattr("valuecounter.time.time_ns", lambda: fixed_now)
    fake.fail_write = True

    process_writes(
        fake,
        [{"table_name": "t", "rows": []}],
        args={"fields": "status", "period_seconds": "60"},
    )

    # Counts retained for retry; last_emit_ns NOT advanced
    state = fake.cache._d[f"vc:wal:{sh}"]
    assert state["counts"] == {"status_200": 3}
    assert state["last_emit_ns"] == 0
    assert any("emit failed" in m for _, m in fake.logs)


def test_wal_index_pruning_removes_expired_hashes(monkeypatch):
    fake = FakeInfluxdb3Local()
    fake.cache.put("shared:tags:t", ["host"], ttl=3600)

    sh_live = _series_key("t", {"host": "a"})
    sh_dead = _series_key("t", {"host": "b"})
    fake.cache.put(
        f"vc:wal:{sh_live}",
        {
            "table": "t",
            "tags": {"host": "a"},
            "counts": {},
            "last_emit_ns": 0,
        },
        ttl=120,
    )
    # sh_dead deliberately NOT in the cache (simulating TTL expiry)
    fake.cache.put("vc:wal:_index:t", [sh_live, sh_dead], ttl=120)

    fixed_now = 1_000_000_000_000_000_000
    monkeypatch.setattr("valuecounter.time.time_ns", lambda: fixed_now)

    process_writes(
        fake,
        [{"table_name": "t", "rows": []}],
        args={"fields": "status", "period_seconds": "60"},
    )

    pruned = fake.cache._d["vc:wal:_index:t"]
    assert pruned == [sh_live]


def test_wal_cross_db_write(monkeypatch):
    fake = FakeInfluxdb3Local()
    fake.cache.put("shared:tags:t", ["host"], ttl=3600)

    sh = _series_key("t", {"host": "a"})
    fake.cache.put(
        f"vc:wal:{sh}",
        {
            "table": "t",
            "tags": {"host": "a"},
            "counts": {"status_200": 1},
            "last_emit_ns": 0,
        },
        ttl=120,
    )
    fake.cache.put("vc:wal:_index:t", [sh], ttl=120)

    fixed_now = 1_000_000_000_000_000_000
    monkeypatch.setattr("valuecounter.time.time_ns", lambda: fixed_now)

    process_writes(
        fake,
        [{"table_name": "t", "rows": []}],
        args={"fields": "status", "period_seconds": "60", "dest_database": "rollups"},
    )

    assert fake.writes == []
    assert len(fake.cross_db_writes) == 1
    db, lp = fake.cross_db_writes[0]
    assert db == "rollups"
    assert "t_valuecounts" in lp
