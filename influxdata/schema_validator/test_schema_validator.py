"""Tests for the schema_validator plugin.

The runtime is faked: `LineBuilder` records a structured point and serializes it
as JSON, so assertions read the written tags, fields and timestamp directly
instead of parsing line protocol.
"""

import ast
import copy
import json
import math
import os
import sys

import pytest
from influxdata_plugin_utils import write as utils_write

sys.path.insert(0, os.path.dirname(__file__))
import schema_validator as sv  # noqa: E402


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
        self.point = {
            "measurement": measurement,
            "tags": {},
            "fields": {},
            "types": {},
            "time": None,
        }

    def _field(self, key, value, field_type):
        self.point["fields"][key] = value
        self.point["types"][key] = field_type
        return self

    def tag(self, key, value):
        self.point["tags"][key] = value
        return self

    def int64_field(self, key, value):
        return self._field(key, int(value), "int")

    def uint64_field(self, key, value):
        return self._field(key, int(value), "uint")

    def float64_field(self, key, value):
        return self._field(key, float(value), "float")

    def bool_field(self, key, value):
        return self._field(key, bool(value), "bool")

    def string_field(self, key, value):
        return self._field(key, str(value), "string")

    def time_ns(self, timestamp_ns):
        self.point["time"] = timestamp_ns
        return self

    def build(self):
        return json.dumps(self.point)


class FakeLocal:
    def __init__(self, write_error=None):
        self.cache = FakeCache()
        self.write_error = write_error
        self.writes = []  # (database, point) per written point
        self.infos = []
        self.warns = []
        self.errors = []

    def _record(self, database, payload):
        if self.write_error is not None:
            raise self.write_error
        for line in payload.build().split("\n"):
            self.writes.append((database, json.loads(line)))

    def write_sync(self, payload, no_sync=None):
        self._record(None, payload)

    def write_sync_to_db(self, database, payload, no_sync=None):
        self._record(database, payload)

    def info(self, message):
        self.infos.append(message)

    def warn(self, message):
        self.warns.append(message)

    def error(self, message):
        self.errors.append(message)


@pytest.fixture(autouse=True)
def plugin_env(monkeypatch, tmp_path):
    monkeypatch.setattr(sv, "LineBuilder", FakeLineBuilder, raising=False)
    monkeypatch.setattr(utils_write.time, "sleep", lambda _: None)
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    monkeypatch.delenv("INFLUXDB3_PLUGIN_DIR", raising=False)
    yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SCHEMA = {
    "allowed_measurements": ["weather"],
    "tables": {
        "weather": {
            "target_table": "weather_clean",
            "tags": {
                "location": {"required": True, "allowed_values": ["us-east", "us-west"]},
                "station_id": {"required": True},
                "region": {"required": False},
            },
            "fields": {
                "temperature": {"required": True, "type": "float"},
                "condition": {
                    "required": False,
                    "type": "string",
                    "allowed_values": ["sunny", "rain"],
                },
                "reading": {"required": False},
            },
        }
    },
}

VALID_ROW = {
    "time": 1_700_000_000_000_000_000,
    "location": "us-east",
    "station_id": "ST001",
    "temperature": 20.5,
}


def schema(**overrides):
    result = copy.deepcopy(SCHEMA)
    table = result["tables"]["weather"]
    for key, value in overrides.items():
        if value is None:
            table.pop(key, None)
        else:
            table[key] = value
    return result


def write_schema(tmp_path, payload, name="schema.json"):
    (tmp_path / name).write_text(json.dumps(payload))
    return name


def run(local, tmp_path, rows, schema_payload=None, table="weather", **args):
    """Run one WAL flush with the given rows against a schema file."""
    schema_file = args.pop("schema_file", None) or write_schema(
        tmp_path, schema_payload if schema_payload is not None else SCHEMA
    )
    sv.process_writes(
        local, [{"table_name": table, "rows": rows}], {"schema_file": schema_file, **args}
    )
    return local


def points(local):
    return [point for _database, point in local.writes]


def rejections(local):
    return [
        point for point in points(local) if point["measurement"] == "_schema_rejections"
    ]


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def test_missing_schema_file_is_reported():
    local = FakeLocal()
    sv.process_writes(local, [], {})

    assert local.errors == [
        error for error in local.errors if "Failed to load configuration" in error
    ]
    assert "schema_file" in local.errors[0]


@pytest.mark.parametrize(
    "args, expected",
    [
        ({"schema_file": "schema.yaml"}, "schema_file must end with '.json'"),
        (
            {"schema_file": "schema.json", "config_file_path": "trigger.yaml"},
            "expected a .toml file",
        ),
        (
            {"schema_file": "schema.json", "target_databse": "clean_db"},
            "may not set 'target_databse'",
        ),
        ({"schema_file": "schema.json", "log_rejected": "maybe"}, "Invalid boolean"),
    ],
)
def test_configuration_errors_are_reported(args, expected):
    local = FakeLocal()
    sv.process_writes(local, [], args)

    assert len(local.errors) == 1
    assert expected in local.errors[0]
    assert not local.writes


@pytest.mark.parametrize("raw", ["true", "1", "yes", "on", True])
def test_boolean_arguments_accept_common_spellings(tmp_path, raw):
    local = run(FakeLocal(), tmp_path, [VALID_ROW], log_accepted=raw)

    assert any("ACCEPTED" in info for info in local.infos)


def test_environment_is_the_lowest_layer(tmp_path, monkeypatch):
    schema_file = write_schema(tmp_path, SCHEMA)
    monkeypatch.setenv("INFLUXDB3_SCHEMA_VALIDATOR_SCHEMA_FILE", schema_file)
    monkeypatch.setenv("INFLUXDB3_SCHEMA_VALIDATOR_TARGET_DATABASE", "env_db")

    local = FakeLocal()
    sv.process_writes(local, [{"table_name": "weather", "rows": [VALID_ROW]}], {})
    assert [database for database, _point in local.writes] == ["env_db"]

    # an inline argument overrides the environment; an untouched variable stands
    overridden = FakeLocal()
    sv.process_writes(
        overridden,
        [{"table_name": "weather", "rows": [VALID_ROW]}],
        {"target_database": "args_db"},
    )
    assert [database for database, _point in overridden.writes] == ["args_db"]


def test_config_file_path_comes_from_the_environment(tmp_path, monkeypatch):
    schema_file = write_schema(tmp_path, SCHEMA)
    (tmp_path / "from_env.toml").write_text(
        f'schema_file = "{schema_file}"\ntarget_database = "env_toml_db"\n'
    )
    (tmp_path / "from_args.toml").write_text(
        f'schema_file = "{schema_file}"\ntarget_database = "args_toml_db"\n'
    )
    monkeypatch.setenv("INFLUXDB3_SCHEMA_VALIDATOR_CONFIG_FILE_PATH", "from_env.toml")

    local = FakeLocal()
    sv.process_writes(local, [{"table_name": "weather", "rows": [VALID_ROW]}], {})
    assert [database for database, _point in local.writes] == ["env_toml_db"]

    # a config_file_path argument names the file instead
    from_args = FakeLocal()
    sv.process_writes(
        from_args,
        [{"table_name": "weather", "rows": [VALID_ROW]}],
        {"config_file_path": "from_args.toml"},
    )
    assert [database for database, _point in from_args.writes] == ["args_toml_db"]


def test_toml_config_overrides_trigger_arguments(tmp_path):
    schema_file = write_schema(tmp_path, SCHEMA)
    (tmp_path / "trigger.toml").write_text(
        f'schema_file = "{schema_file}"\ntarget_database = "clean_db"\n'
    )
    local = FakeLocal()

    sv.process_writes(
        local,
        [{"table_name": "weather", "rows": [VALID_ROW]}],
        {
            "config_file_path": "trigger.toml",
            "target_database": "overridden_db",
            "log_accepted": "true",
        },
    )

    assert [database for database, _point in local.writes] == ["clean_db"]
    # a key the file leaves out keeps the value the trigger arguments gave it
    assert any("ACCEPTED" in info for info in local.infos)


def test_unknown_key_in_toml_config_is_reported(tmp_path):
    schema_file = write_schema(tmp_path, SCHEMA)
    (tmp_path / "trigger.toml").write_text(
        f'schema_file = "{schema_file}"\ntarget_databse = "clean_db"\n'
    )
    local = FakeLocal()

    sv.process_writes(
        local,
        [{"table_name": "weather", "rows": [VALID_ROW]}],
        {"config_file_path": "trigger.toml"},
    )

    assert "Config file may not set 'target_databse'" in local.errors[0]
    assert not local.writes


# ---------------------------------------------------------------------------
# Schema loading
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "payload, expected",
    [
        ({"tables": {}}, "'tables' must be a non-empty dict"),
        ({"tables": ["weather"]}, "'tables' must be a non-empty dict"),
        (
            {"allowed_measurements": "weather", "tables": {"weather": {"fields": {"a": {}}}}},
            "'allowed_measurements' must be a list",
        ),
        ({"tables": {"weather": []}}, "definition must be a dict"),
        ({"tables": {"weather": {"fields": {}}}}, "fields must be a non-empty dict"),
        (
            {"tables": {"weather": {"tags": [], "fields": {"a": {}}}}},
            "tags must be a dict",
        ),
        (
            {"tables": {"weather": {"fields": {"a": {"type": "floatt"}}}}},
            "field 'a' has unknown type 'floatt'",
        ),
    ],
)
def test_invalid_schema_is_reported(tmp_path, payload, expected):
    local = run(FakeLocal(), tmp_path, [VALID_ROW], schema_payload=payload)

    assert len(local.errors) == 1
    assert expected in local.errors[0]
    assert not local.writes


def test_schema_is_cached_between_flushes(tmp_path):
    local = FakeLocal()
    run(local, tmp_path, [VALID_ROW])
    write_schema(tmp_path, schema(target_table="renamed"))

    run(local, tmp_path, [VALID_ROW])

    assert {point["measurement"] for point in points(local)} == {"weather_clean"}


# ---------------------------------------------------------------------------
# Target resolution
# ---------------------------------------------------------------------------


def test_writing_a_table_into_itself_is_rejected(tmp_path):
    local = run(FakeLocal(), tmp_path, [VALID_ROW], schema_payload=schema(target_table=None))

    assert "would be written back into itself" in local.errors[0]
    assert not local.writes


def test_self_mapping_table_outside_the_batch_is_ignored(tmp_path):
    payload = schema()
    payload["allowed_measurements"].append("cpu")
    payload["tables"]["cpu"] = {"fields": {"usage": {"required": True, "type": "float"}}}

    local = run(FakeLocal(), tmp_path, [VALID_ROW], schema_payload=payload)

    assert not local.errors
    assert {point["measurement"] for point in points(local)} == {"weather_clean"}


@pytest.mark.parametrize(
    "args, expected",
    [
        ({"target_table_suffix": "_clean"}, "weather_clean"),
        ({"target_table_prefix": "validated_"}, "validated_weather"),
        ({"target_database": "clean_db"}, "weather"),
    ],
)
def test_target_measurement_comes_from_prefix_suffix_or_database(tmp_path, args, expected):
    local = run(
        FakeLocal(),
        tmp_path,
        [VALID_ROW],
        schema_payload=schema(target_table=None),
        **args,
    )

    assert [point["measurement"] for point in points(local)] == [expected]
    assert local.writes[0][0] == args.get("target_database")


def test_per_table_target_table_wins_over_suffix(tmp_path):
    local = run(FakeLocal(), tmp_path, [VALID_ROW], target_table_suffix="_ignored")

    assert [point["measurement"] for point in points(local)] == ["weather_clean"]


# ---------------------------------------------------------------------------
# Row validation
# ---------------------------------------------------------------------------


def test_valid_row_keeps_only_schema_columns(tmp_path):
    row = dict(VALID_ROW, condition="sunny", region=None, junk_tag="x", junk_field=1)
    local = run(FakeLocal(), tmp_path, [row])

    (point,) = points(local)
    assert point["tags"] == {"location": "us-east", "station_id": "ST001"}
    assert point["fields"] == {"temperature": 20.5, "condition": "sunny"}
    assert point["time"] == VALID_ROW["time"]


@pytest.mark.parametrize(
    "row, expected",
    [
        ({"station_id": None}, "Required tag 'station_id' is missing"),
        ({"location": "eu-west"}, "Tag 'location' value 'eu-west' is not in allowed"),
        ({"temperature": None}, "Required field 'temperature' is missing"),
        ({"temperature": "warm"}, "Field 'temperature' expected type 'float'"),
        ({"condition": "fog"}, "Field 'condition' value 'fog' is not in allowed"),
        ({"temperature": True}, "Field 'temperature' expected type 'float'"),
        ({"temperature": math.nan}, "float field 'temperature' is not finite"),
    ],
)
def test_rows_are_rejected_with_a_reason(tmp_path, row, expected):
    local = run(FakeLocal(), tmp_path, [dict(VALID_ROW, **row)])

    assert not local.writes
    assert expected in local.warns[0]


@pytest.mark.parametrize(
    "field_type, value, valid",
    [
        ("integer", 7, True),
        ("integer", 7.5, False),
        ("integer", True, False),
        ("uint64", 7, True),
        ("uint64", -7, False),
        ("boolean", True, True),
        ("boolean", 1, False),
        ("string", "ok", True),
        ("string", 1, False),
    ],
)
def test_declared_types_are_enforced(tmp_path, field_type, value, valid):
    payload = schema()
    payload["tables"]["weather"]["fields"]["reading"] = {
        "required": True,
        "type": field_type,
    }
    local = run(
        FakeLocal(), tmp_path, [dict(VALID_ROW, reading=value)], schema_payload=payload
    )

    assert bool(local.writes) is valid
    if valid:
        assert points(local)[0]["types"]["reading"] == sv.FIELD_TYPES[field_type]


@pytest.mark.parametrize(
    "value, expected_type",
    [(True, "bool"), (7, "int"), (7.5, "float"), ("ok", "string")],
)
def test_untyped_fields_infer_their_type(tmp_path, value, expected_type):
    local = run(FakeLocal(), tmp_path, [dict(VALID_ROW, reading=value)])

    assert points(local)[0]["types"]["reading"] == expected_type


def test_row_without_schema_fields_is_rejected(tmp_path):
    payload = schema(fields={"temperature": {"required": False, "type": "float"}})
    local = run(FakeLocal(), tmp_path, [{"time": 1, "location": "us-east", "station_id": "A"}], schema_payload=payload)

    assert not local.writes
    assert "No fields matched the schema definition" in local.warns[0]


def test_one_bad_row_does_not_stop_the_others(tmp_path):
    rows = [
        VALID_ROW,
        dict(VALID_ROW, time=2, temperature=math.inf),
        dict(VALID_ROW, time=3, temperature=21.5),
    ]
    local = run(FakeLocal(), tmp_path, rows)

    assert [point["time"] for point in points(local)] == [VALID_ROW["time"], 3]
    assert any("is not finite" in warn for warn in local.warns)


# ---------------------------------------------------------------------------
# Table selection
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "table, expected",
    [
        ("cpu", "not in allowed_measurements"),
        ("_schema_rejections", "not in allowed_measurements"),
    ],
)
def test_unrelated_table_batches_are_skipped(tmp_path, table, expected):
    local = run(FakeLocal(), tmp_path, [VALID_ROW], table=table)

    assert not local.writes
    assert not local.warns
    assert any(expected in info for info in local.infos)


def test_table_without_a_schema_entry_is_skipped(tmp_path):
    payload = copy.deepcopy(SCHEMA)
    payload["allowed_measurements"] = []
    local = run(FakeLocal(), tmp_path, [VALID_ROW], schema_payload=payload, table="cpu")

    assert not local.writes
    assert any("No schema defined for table 'cpu'" in info for info in local.infos)


# ---------------------------------------------------------------------------
# Rejection log and write failures
# ---------------------------------------------------------------------------


def test_rejection_log_entries_are_batched_and_distinct(tmp_path):
    rows = [dict(VALID_ROW, time=index, location="eu-west") for index in range(3)]
    local = run(FakeLocal(), tmp_path, rows, write_rejection_log="true")

    entries = rejections(local)
    assert len(entries) == 3
    assert [entry["tags"] for entry in entries] == [{"source_table": "weather"}] * 3
    timestamps = [entry["time"] for entry in entries]
    assert timestamps == sorted(set(timestamps))
    assert "not in allowed" in entries[0]["fields"]["reason"]
    assert entries[0]["fields"]["row_data"].startswith("{'time': 0")


def test_rejection_row_data_is_truncated(tmp_path):
    row = dict(VALID_ROW, location="eu-west", junk="x" * 4096)
    local = run(FakeLocal(), tmp_path, [row], write_rejection_log=True)

    assert len(rejections(local)[0]["fields"]["row_data"]) == sv.ROW_DATA_LIMIT


def test_rejection_log_follows_the_target_database(tmp_path):
    local = run(
        FakeLocal(),
        tmp_path,
        [dict(VALID_ROW, location="eu-west")],
        target_database="clean_db",
        write_rejection_log=True,
    )

    assert [database for database, _point in local.writes] == ["clean_db"]


def test_write_failure_is_reported_per_table(tmp_path):
    schema_file = write_schema(tmp_path, SCHEMA)
    local = FakeLocal(write_error=RuntimeError("boom"))

    sv.process_writes(
        local,
        [
            {"table_name": "weather", "rows": [VALID_ROW]},
            {"table_name": "weather", "rows": [dict(VALID_ROW, time=2)]},
        ],
        {"schema_file": schema_file},
    )

    assert len(local.errors) == 2
    assert all("Failed to write 1 validated rows" in error for error in local.errors)
    assert local.infos[-1].endswith("0 total accepted, 0 total rejected, 2 total dropped")


def test_logging_toggles(tmp_path):
    rows = [VALID_ROW, dict(VALID_ROW, time=2, location="eu-west")]
    local = run(FakeLocal(), tmp_path, rows, log_rejected="false", log_accepted="false")

    assert not local.warns
    assert not any("ACCEPTED" in info for info in local.infos)
    assert local.infos[-1].endswith("1 total accepted, 1 total rejected, 0 total dropped")


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


def test_metadata_docstring_is_valid_json():
    source = open(sv.__file__).read()
    metadata = json.loads(ast.get_docstring(ast.parse(source)))

    assert metadata["plugin_type"] == ["onwrite"]
    names = [entry["name"] for entry in metadata["onwrite_args_config"]]
    assert len(names) == len(set(names))
    for entry in metadata["onwrite_args_config"]:
        assert set(entry) == {"name", "example", "description", "required"}