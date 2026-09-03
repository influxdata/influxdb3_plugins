import json
import math
import re
from collections import OrderedDict, namedtuple

import pytest

import geo_enrichment as plugin

TAG_TYPE = "Dictionary(Int32, Utf8)"


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
        return json.dumps(
            {
                "measurement": self.measurement,
                "tags": self.tags,
                "fields": self.fields,
                "time": self.timestamp,
            }
        )


Record = namedtuple("Record", ["measurement", "tags", "fields", "time"])


def parse_written(payload):
    return [Record(**json.loads(line)) for line in payload.split("\n")]


class FakeResolver:
    """Resolver stand-in: a lookup table keyed by rounded coordinates."""

    def __init__(self, places=None, attributes=("country", "city")):
        self.attributes = set(attributes)
        self.warnings: list = []
        self.places = places or {
            (55.75, 37.62): {"country": "Russia", "city": "Moscow"},
            (48.86, 2.35): {"country": "France", "city": "Paris"},
        }
        self.calls: list = []

    def resolve(self, lat, lon):
        self.calls.append((lat, lon))
        return self.places.get((round(lat, 2), round(lon, 2)))


class FakeLocal:
    """Stub of the runtime client: logging, trigger-local cache, query, writes."""

    def __init__(self, columns=None, rows=None, write_error=None):
        self.cache = FakeCache()
        self.columns = dict(columns or {})
        self.rows = dict(rows or {})
        self.write_error = write_error
        self.queries: list = []
        self.writes: list = []
        self.logs: list = []

    # --- queries ---------------------------------------------------------
    def query(self, query, params=None):
        self.queries.append((query, params))
        if "information_schema.columns" in query:
            return [
                {"column_name": name, "data_type": data_type}
                for name, data_type in self.columns.get(params["table"], {}).items()
            ]

        table = re.search(r'FROM "([^"]+)"', query).group(1)
        rows = sorted(self.rows.get(table, []), key=lambda row: row["time"])
        if params and "ts" in params:
            target = rfc3339_to_ns(params["ts"])
            return [dict(row) for row in rows if row["time"] == target]
        if params and "cursor" in params:
            cursor = rfc3339_to_ns(params["cursor"])
            rows = [row for row in rows if row["time"] >= cursor]
        if params and "end" in params:
            end = rfc3339_to_ns(params["end"])
            rows = [row for row in rows if row["time"] < end]
        limit = re.search(r"LIMIT (\d+)", query)
        if limit:
            rows = rows[: int(limit.group(1))]
        return [dict(row) for row in rows]

    # --- writes ----------------------------------------------------------
    def write_sync(self, batch, no_sync=False):
        if self.write_error is not None:
            raise self.write_error
        self.writes.extend((None, record) for record in parse_written(batch.build()))

    def write_sync_to_db(self, database, batch, no_sync=False):
        if self.write_error is not None:
            raise self.write_error
        self.writes.extend(
            (database, record) for record in parse_written(batch.build())
        )

    # --- logging ---------------------------------------------------------
    def info(self, message):
        self.logs.append(("info", message))

    def warn(self, message):
        self.logs.append(("warn", message))

    def error(self, message):
        self.logs.append(("error", message))

    def messages(self, level=None):
        return [text for lvl, text in self.logs if level is None or lvl == level]

    def records(self):
        return [record for _, record in self.writes]


def rfc3339_to_ns(text):
    """Inverse of the plugin's ns_to_rfc3339, for the fake query engine."""
    stamp, nanos = text.rstrip("Z").split(".")
    from datetime import datetime, timezone

    seconds = int(
        datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%S")
        .replace(tzinfo=timezone.utc)
        .timestamp()
    )
    return seconds * 1_000_000_000 + int(nanos)


@pytest.fixture(autouse=True)
def line_builder(monkeypatch):
    monkeypatch.setattr(plugin, "LineBuilder", FakeLineBuilder, raising=False)


@pytest.fixture
def resolver(monkeypatch):
    """Replace index construction, so no reference dataset is needed."""
    instance = FakeResolver()
    monkeypatch.setattr(plugin, "build_resolver", lambda cfg: instance)
    return instance


BASE_ARGS = {
    "source_measurements": "gps",
    "output_columns": "country:geo_country city:geo_city",
}

GPS_COLUMNS = {
    "device": TAG_TYPE,
    "lat": "Float64",
    "lon": "Float64",
    "speed": "Float64",
    "time": "Timestamp(Nanosecond, None)",
}

ENRICHED_COLUMNS = {**GPS_COLUMNS, "geo_country": "Utf8", "geo_city": "Utf8"}


def config(influxdb3_local=None, **overrides):
    return plugin.normalize_config(
        influxdb3_local or FakeLocal(), {**BASE_ARGS, **overrides}, "tid"
    )


def gps_row(time_ns, lat=55.7512, lon=37.6184, device="A", **extra):
    return {"device": device, "lat": lat, "lon": lon, "speed": 60.0,
            "time": time_ns, **extra}


# --- configuration ----------------------------------------------------------


def test_config_defaults_grid_precision_per_grid_type():
    assert config(strategy="grid", grid_type="h3")["grid_precision"] == 7
    assert config(strategy="grid", grid_type="geohash")["grid_precision"] == 6
    assert config(strategy="grid", grid_type="s2")["grid_precision"] == 9
    # 0 is a valid h3 resolution, so it must survive as itself
    assert config(strategy="grid", grid_type="h3", grid_precision="0")["grid_precision"] == 0


@pytest.mark.parametrize(
    "overrides, reason",
    [
        ({"output_mode": "tag"}, "needs 'target_measurement'"),
        ({"output_mode": "tag", "target_measurement": "gps"}, "must differ"),
        ({"point_field": "p", "h3_field": "cell"}, "exactly one coordinate input"),
        ({"strategy": "nope"}, "Unknown strategy"),
        ({"strategy": "polygon"}, "needs 'reference_file'"),
        ({"strategy": "nearest"}, "needs 'reference_file'"),
        ({"strategy": "grid", "grid_type": "geohash", "grid_precision": "20"}, "out of range"),
        ({"max_radius_m": "-0.5"}, "must be greater than 0"),
        ({"overlap_policy": "priority"}, "needs 'priority_attribute'"),
        ({"nearest_count": "2"}, "needs strategy='nearest'"),
        ({"source_measurements": " "}, "'source_measurements' is empty"),
    ],
)
def test_config_rejects_contradictory_settings(overrides, reason):
    with pytest.raises(Exception, match=re.escape(reason)):
        config(**overrides)


@pytest.mark.parametrize(
    "overrides, reason",
    [
        ({"config_file_path": "settings.yaml"}, "must be a .toml file"),
        (
            {"strategy": "polygon", "reference_file": "/etc/passwd"},
            r"must be a .geojson or .json or .csv file",
        ),
        (
            {"strategy": "nearest", "reference_file": "zones.shp"},
            r"must be a .geojson or .json or .csv file",
        ),
    ],
)
def test_reference_files_must_carry_a_known_format(overrides, reason):
    """A path is read verbatim, so the name is checked before anything opens it."""
    with pytest.raises(Exception, match=reason):
        config(**overrides)


def test_reference_file_format_check_ignores_case():
    cfg = config(strategy="polygon", reference_file="/plugins/data/Zones.GeoJSON")

    assert cfg["reference_file"] == "/plugins/data/Zones.GeoJSON"


def test_config_rejects_zero_coord_scale():
    """coord_scale divides every coordinate, so 0 has to fail at load time."""
    with pytest.raises(Exception):
        config(coord_scale="0")


def test_config_rejects_attributes_the_strategy_cannot_produce(resolver):
    cfg = config(output_columns="country:geo_country postcode:geo_postcode")

    with pytest.raises(Exception, match="cannot produce postcode"):
        plugin.validate_attributes(cfg, resolver, "tid")


# --- coordinate extraction --------------------------------------------------


@pytest.mark.parametrize(
    "point_format, raw, expected",
    [
        ("lat_lon", "55.75, 37.62", (55.75, 37.62)),
        ("lon_lat", "37.62 ; 55.75", (55.75, 37.62)),
        ("wkt", "POINT (37.62 55.75)", (55.75, 37.62)),
        ("geojson", '{"type":"Point","coordinates":[37.62,55.75]}', (55.75, 37.62)),
    ],
)
def test_parse_point_reads_every_supported_format(point_format, raw, expected):
    assert plugin.parse_point(raw, point_format) == expected


@pytest.mark.parametrize(
    "point_format, raw",
    [
        ("geojson", '{"coordinates": []}'),
        ("geojson", '{"coordinates": [1]}'),
        ("geojson", '{"type": "Point"}'),
        ("geojson", "not json"),
        ("wkt", "LINESTRING (1 2, 3 4)"),
        ("wkt", "POINT (37.62)"),
        ("lat_lon", "55.75"),
        ("lat_lon", "north, east"),
    ],
)
def test_parse_point_returns_none_for_malformed_input(point_format, raw):
    """Malformed points are skipped, never raised: one bad row must not stop a batch."""
    assert plugin.parse_point(raw, point_format) is None


def test_extract_coordinates_applies_coord_scale():
    cfg = config(coord_scale="1e7")

    assert plugin.extract_coordinates(
        {"lat": 557512000, "lon": 376184000}, cfg
    ) == pytest.approx((55.7512, 37.6184))


def test_extract_coordinates_reads_string_tags():
    """Coordinates carried as tags arrive as strings."""
    assert plugin.extract_coordinates(
        {"lat": "55.7512", "lon": "37.6184"}, config()
    ) == (55.7512, 37.6184)


@pytest.mark.parametrize(
    "lat, lon", [(91.0, 0.0), (0.0, 181.0), (float("nan"), 0.0), (float("inf"), 0.0)]
)
def test_coordinates_out_of_range_are_invalid(lat, lon):
    assert plugin.coordinates_valid(lat, lon) is False


# --- enrichment guard -------------------------------------------------------


@pytest.mark.parametrize(
    "row, enriched",
    [
        # backfill reads SELECT *, so unset columns are present as None
        ({"geo_country": None, "geo_city": None}, False),
        ({"geo_country": "Russia", "geo_city": None}, False),
        ({"geo_country": "Russia", "geo_city": "Moscow"}, True),
        # a write batch carries only the columns actually written
        ({"lat": 1.0, "lon": 2.0}, False),
    ],
)
def test_already_enriched_requires_a_value_not_just_the_column(row, enriched):
    assert plugin.already_enriched(row, config()) is enriched


def test_retry_unknown_sees_a_float_column_too():
    """distance_m carries -1, not unknown_value, so a distance-only column map
    must not make the flag a silent no-op."""
    cfg = config(output_columns=f"{plugin.DISTANCE_ATTRIBUTE}:geo_dist")

    assert plugin.needs_reresolve({"geo_dist": plugin.UNRESOLVED_DISTANCE}, cfg, True)
    assert not plugin.needs_reresolve({"geo_dist": 12.5}, cfg, True)


def test_retry_unknown_only_targets_rows_the_resolver_failed_on():
    cfg = config()
    unknown = {"geo_country": "UNKNOWN", "geo_city": "UNKNOWN"}
    resolved = {"geo_country": "Russia", "geo_city": "Moscow"}

    assert plugin.needs_reresolve(unknown, cfg, True) is True
    assert plugin.needs_reresolve(resolved, cfg, True) is False
    assert plugin.needs_reresolve(unknown, cfg, False) is False


# --- caching ----------------------------------------------------------------


@pytest.mark.parametrize(
    "overrides",
    [
        {"grid_precision": "8"},
        {"grid_type": "geohash"},
        {"min_population": "1000"},
        {"max_radius_m": "5000"},
        {"strategy": "polygon", "reference_file": "zones.geojson"},
        {"strategy": "polygon", "reference_file": "zones.csv",
         "reference_geometry_column": "shape"},
        {"strategy": "nearest", "reference_file": "s.csv",
         "reference_encoding": "cp1251"},
        {"strategy": "nearest", "reference_file": "s.csv", "nearest_count": "3"},
    ],
)
def test_resolver_cache_key_tracks_every_build_parameter(overrides):
    """An edited parameter must rebuild the index, not reuse a frozen resolver."""
    base = config(strategy="grid", grid_type="h3", grid_precision="7")

    assert plugin.resolver_cache_key(config(**{
        "strategy": "grid", "grid_type": "h3", "grid_precision": "7", **overrides
    })) != plugin.resolver_cache_key(base)


def test_rebuilding_the_resolver_drops_memoized_results(monkeypatch):
    """Memoized attributes came from the old index and must not outlive it."""
    influxdb3_local = FakeLocal()
    cfg = config()
    monkeypatch.setattr(plugin, "build_resolver", lambda _: FakeResolver())
    plugin.get_memo(influxdb3_local)[(55.75, 37.62)] = {"country": "Russia"}

    plugin.get_resolver(influxdb3_local, cfg, rebuild=True)

    assert plugin.get_memo(influxdb3_local) == {}


def test_resolver_is_reused_from_the_cache_between_invocations(resolver):
    influxdb3_local = FakeLocal()
    cfg = config()

    first = plugin.get_resolver(influxdb3_local, cfg)
    second = plugin.get_resolver(influxdb3_local, cfg)

    assert first is second is resolver


def test_schema_is_reread_when_a_row_shows_an_unknown_column():
    """A tag added by a client must reach the line, or the write lands on a
    different series than the source row."""
    influxdb3_local = FakeLocal(columns={"gps": dict(GPS_COLUMNS)})
    cache: dict = {}

    plugin.schema_for(influxdb3_local, "gps", [gps_row(1)], cache, "tid")
    influxdb3_local.columns["gps"]["site" ] = TAG_TYPE
    schema = plugin.schema_for(
        influxdb3_local, "gps", [gps_row(2, site="north")], cache, "tid"
    )

    assert "site" in schema["tags"]


def test_schema_is_not_requeried_while_columns_stay_the_same():
    influxdb3_local = FakeLocal(columns={"gps": dict(GPS_COLUMNS)})
    cache: dict = {}

    for time_ns in range(3):
        plugin.schema_for(influxdb3_local, "gps", [gps_row(time_ns)], cache, "tid")

    assert len(influxdb3_local.queries) == 1


# --- process_writes ---------------------------------------------------------


def write_client(rows, columns=None):
    influxdb3_local = FakeLocal(columns={"gps": dict(columns or GPS_COLUMNS)})
    return influxdb3_local, [{"table_name": "gps", "rows": rows}]


def test_in_place_write_carries_only_tags_and_geo_fields(resolver):
    """Fields merge into the existing row, so re-sending them would be waste;
    tags are the primary key and must be reproduced exactly."""
    influxdb3_local, batches = write_client([gps_row(1_000)])

    plugin.process_writes(influxdb3_local, batches, dict(BASE_ARGS))

    (database, record), = influxdb3_local.writes
    assert database is None
    assert record.measurement == "gps"
    assert record.tags == {"device": "A"}
    assert record.fields == {"geo_country": "Russia", "geo_city": "Moscow"}
    assert record.time == 1_000
    assert "speed" not in record.fields


def test_echo_batch_is_skipped_so_the_trigger_terminates(resolver):
    """An in-place write feeds the plugin its own rows back."""
    influxdb3_local, batches = write_client(
        [gps_row(1_000, geo_country="Russia", geo_city="Moscow")],
        columns=ENRICHED_COLUMNS,
    )

    plugin.process_writes(influxdb3_local, batches, dict(BASE_ARGS))

    assert influxdb3_local.writes == []
    assert "already_enriched=1" in influxdb3_local.messages("info")[0]


def test_unresolved_point_fills_every_column_with_the_unknown_value(resolver):
    """A partial row would fail the enriched check and be resolved again forever."""
    influxdb3_local, batches = write_client([gps_row(1_000, lat=10.0, lon=10.0)])

    plugin.process_writes(influxdb3_local, batches, dict(BASE_ARGS))

    (_, record), = influxdb3_local.writes
    assert record.fields == {"geo_country": "UNKNOWN", "geo_city": "UNKNOWN"}


def test_rows_without_coordinates_are_counted_and_produce_no_line(resolver):
    influxdb3_local, batches = write_client(
        [{"device": "A", "speed": 60.0, "time": 1_000}]
    )

    plugin.process_writes(influxdb3_local, batches, dict(BASE_ARGS))

    assert influxdb3_local.writes == []
    assert "no_coordinates=1" in influxdb3_local.messages("info")[0]


def test_tables_outside_source_measurements_are_ignored(resolver):
    influxdb3_local = FakeLocal(columns={"gps": dict(GPS_COLUMNS)})

    plugin.process_writes(
        influxdb3_local,
        [{"table_name": "weather", "rows": [gps_row(1_000)]}],
        dict(BASE_ARGS),
    )

    assert influxdb3_local.writes == []


def test_copy_to_a_target_measurement_carries_the_source_fields(resolver):
    """A separate table has no row to merge into, so the whole row is copied."""
    influxdb3_local, batches = write_client([gps_row(1_000)])

    plugin.process_writes(
        influxdb3_local,
        batches,
        {**BASE_ARGS, "output_mode": "tag", "target_measurement": "gps_geo"},
    )

    (_, record), = influxdb3_local.writes
    assert record.measurement == "gps_geo"
    assert record.tags == {"device": "A", "geo_country": "Russia", "geo_city": "Moscow"}
    assert record.fields == {"lat": 55.7512, "lon": 37.6184, "speed": 60.0}


def test_nearby_points_share_one_resolver_call(resolver):
    """The memo is keyed on quantized coordinates, at ~11 m by default."""
    influxdb3_local, batches = write_client(
        [gps_row(1_000), gps_row(2_000, lat=55.75121, lon=37.61841)]
    )

    plugin.process_writes(influxdb3_local, batches, dict(BASE_ARGS))

    assert len(resolver.calls) == 1
    assert len(influxdb3_local.writes) == 2


def test_one_failing_row_does_not_take_down_the_batch(resolver, monkeypatch):
    """A resolver failure is counted and logged, never swallowed into UNKNOWN."""
    def explode(lat, lon):
        if lat > 50:
            raise RuntimeError("index corrupted")
        return {"country": "France", "city": "Paris"}

    monkeypatch.setattr(resolver, "resolve", explode)
    influxdb3_local, batches = write_client(
        [gps_row(1_000), gps_row(2_000, lat=48.8566, lon=2.3522)]
    )

    plugin.process_writes(influxdb3_local, batches, dict(BASE_ARGS))

    assert [record.time for record in influxdb3_local.records()] == [2_000]
    assert "errors=1" in influxdb3_local.messages("info")[0]
    assert any("index corrupted" in message for message in influxdb3_local.messages("warn"))


def test_a_failed_write_is_reported_and_not_retried(resolver):
    """The WAL trigger runs inline with ingestion, so it must not sleep on retry."""
    influxdb3_local, batches = write_client([gps_row(1_000)])
    influxdb3_local.write_error = Exception("write buffer full")

    plugin.process_writes(influxdb3_local, batches, dict(BASE_ARGS))

    assert influxdb3_local.writes == []
    assert any(
        "write buffer full" in message for message in influxdb3_local.messages("error")
    )


def test_a_trigger_without_arguments_reports_the_missing_setting(resolver):
    """The engine passes None, which must not surface as an AttributeError."""
    influxdb3_local, batches = write_client([gps_row(1_000)])

    plugin.process_writes(influxdb3_local, batches, None)

    assert influxdb3_local.writes == []
    assert any(
        "source_measurements" in message
        for message in influxdb3_local.messages("error")
    )


def test_configuration_error_stops_before_any_write():
    influxdb3_local, batches = write_client([gps_row(1_000)])

    plugin.process_writes(
        influxdb3_local, batches, {**BASE_ARGS, "strategy": "polygon"}
    )

    assert influxdb3_local.writes == []
    assert any(
        "Configuration error" in message
        for message in influxdb3_local.messages("error")
    )


# --- process_request --------------------------------------------------------


def backfill_client(rows, columns=None):
    return FakeLocal(
        columns={"gps": dict(columns or ENRICHED_COLUMNS)}, rows={"gps": rows}
    )


BASE_BODY = dict(BASE_ARGS)


def backfill(influxdb3_local, **body):
    """The endpoint is configured from the body alone; args are not read."""
    return plugin.process_request(
        influxdb3_local, None, None, json.dumps({**BASE_BODY, **body}), None
    )


def unenriched(time_ns, **extra):
    return gps_row(time_ns, geo_country=None, geo_city=None, **extra)


def test_backfill_enriches_rows_left_empty_by_the_live_trigger(resolver):
    influxdb3_local = backfill_client([unenriched(1_000), unenriched(2_000)])

    body, status = backfill(influxdb3_local)

    assert status == 200
    assert body["stats"]["written"] == 2
    assert body["stats"]["skipped_enriched"] == 0
    assert {record.time for record in influxdb3_local.records()} == {1_000, 2_000}


@pytest.mark.parametrize("batch_size", [1, 2, 3, 7])
def test_paging_covers_tied_timestamps_without_duplicates(resolver, batch_size):
    """Rows sharing a timestamp have no stable order across queries, so a page
    boundary must not fall inside one timestamp."""
    rows = [unenriched(1_000, device=f"d{index}") for index in range(5)]
    rows += [unenriched(2_000, device=f"d{index}") for index in range(4)]
    influxdb3_local = backfill_client(rows)

    body, status = backfill(influxdb3_local, batch_size=batch_size)

    written = [(record.tags["device"], record.time) for record in influxdb3_local.records()]
    assert status == 200
    assert len(written) == len(set(written)) == len(rows)
    assert body["stats"]["rows"] == len(rows)


def test_backfill_honours_the_time_range(resolver):
    influxdb3_local = backfill_client(
        [unenriched(1_000), unenriched(2_000), unenriched(3_000)]
    )

    backfill(
        influxdb3_local,
        start="1970-01-01T00:00:00.000002000Z",
        end="1970-01-01T00:00:00.000003000Z",
    )

    assert [record.time for record in influxdb3_local.records()] == [2_000]


def test_backfill_skips_rows_that_already_resolved(resolver):
    influxdb3_local = backfill_client(
        [unenriched(1_000), gps_row(2_000, geo_country="Russia", geo_city="Moscow")]
    )

    body, _ = backfill(influxdb3_local)

    assert body["stats"]["skipped_enriched"] == 1
    assert [record.time for record in influxdb3_local.records()] == [1_000]


def test_retry_unknown_reresolves_only_the_failed_rows(resolver):
    influxdb3_local = backfill_client(
        [
            gps_row(1_000, geo_country="UNKNOWN", geo_city="UNKNOWN"),
            gps_row(2_000, geo_country="Russia", geo_city="Moscow"),
        ]
    )

    body, _ = backfill(influxdb3_local, retry_unknown=True)

    assert body["stats"]["skipped_enriched"] == 1
    assert [record.time for record in influxdb3_local.records()] == [1_000]


def test_force_reresolves_rows_that_already_carry_values(resolver):
    """How a corrected boundary file reaches rows that resolved successfully."""
    influxdb3_local = backfill_client(
        [gps_row(1_000, geo_country="Stale", geo_city="Stale")]
    )

    backfill(influxdb3_local, force=True)

    (record,) = influxdb3_local.records()
    assert record.fields == {"geo_country": "Russia", "geo_city": "Moscow"}


@pytest.mark.parametrize(
    "body, reason",
    [
        ({"source_measurements": " "}, "'source_measurements' is empty"),
        ({"start": "2026-01-01T00:00:00Z"}, "must be given together"),
        ({"batch_size": "many"}, "'batch_size' must be an integer"),
        ({"force": "yes please"}, "Invalid boolean"),
    ],
)
def test_backfill_reports_bad_request_bodies_as_400(resolver, body, reason):
    influxdb3_local = backfill_client([unenriched(1_000)])

    response, status = backfill(influxdb3_local, **body)

    assert status == 400
    assert reason in response["error"]


def test_trigger_arguments_are_ignored_and_reported(resolver):
    """Merging args into the body would make the same request behave differently
    on two triggers; the endpoint is configured from the body alone."""
    influxdb3_local = backfill_client([unenriched(1_000)])

    body, status = plugin.process_request(
        influxdb3_local,
        None,
        None,
        json.dumps(BASE_BODY),
        {"unknown_value": "from-args", "strategy": "grid"},
    )

    assert status == 200
    assert body["stats"]["written"] == 1
    assert influxdb3_local.records()[0].fields["geo_country"] == "Russia"
    assert any(
        "Trigger arguments are ignored" in message
        for message in influxdb3_local.messages("warn")
    )


def test_only_the_first_of_several_tables_is_backfilled(resolver):
    influxdb3_local = backfill_client([unenriched(1_000)])

    body, status = backfill(influxdb3_local, source_measurements="gps fleet_pos")

    assert status == 200
    assert body["measurement"] == "gps"
    assert any(
        "ignoring fleet_pos" in message for message in influxdb3_local.messages("warn")
    )


def test_body_alone_configures_the_whole_run(resolver):
    influxdb3_local = backfill_client([unenriched(1_000)])

    backfill(influxdb3_local, unknown_value="n/a", lat_field="lat", quantize_decimals=2)

    assert influxdb3_local.records()[0].fields["geo_city"] == "Moscow"


def test_config_file_in_the_body_replaces_the_settings_around_it(
    resolver, monkeypatch, tmp_path
):
    """A TOML path is how a long setup is reused; letting the body override
    parts of it would make the effective configuration hard to reason about."""
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    (tmp_path / "geo.toml").write_text(
        'source_measurements = "gps"\n'
        'output_columns = "country:geo_country city:geo_city"\n'
        'unknown_value = "from-toml"\n'
    )
    influxdb3_local = backfill_client([unenriched(1_000), unenriched(2_000, lat=10.0)])

    body, status = backfill(
        influxdb3_local, config_file_path="geo.toml", unknown_value="from-body"
    )

    assert status == 200
    unresolved = [r for r in influxdb3_local.records() if r.time == 2_000][0]
    assert unresolved.fields["geo_country"] == "from-toml"


def test_backfill_fields_still_come_from_the_body_beside_a_config_file(
    resolver, monkeypatch, tmp_path
):
    """start/end/force are per-call, so a TOML never carries them."""
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    (tmp_path / "geo.toml").write_text(
        'source_measurements = "gps"\noutput_columns = "country:geo_country"\n'
    )
    influxdb3_local = backfill_client([unenriched(1_000), unenriched(2_000)])

    backfill(
        influxdb3_local,
        config_file_path="geo.toml",
        start="1970-01-01T00:00:00.000002000Z",
        end="1970-01-01T00:00:00.000003000Z",
    )

    assert [record.time for record in influxdb3_local.records()] == [2_000]


def test_json_null_in_the_body_means_not_provided(resolver):
    influxdb3_local = backfill_client([unenriched(1_000)])

    body, status = backfill(influxdb3_local, start=None, end=None, batch_size=None)

    assert status == 200
    assert body["stats"]["written"] == 1


def test_backfill_of_an_unknown_table_is_a_bad_request(resolver):
    """The caller named the table, so this is a 400 and not the planner error
    the first page query would otherwise raise."""
    influxdb3_local = backfill_client([unenriched(1_000)])

    response, status = backfill(influxdb3_local, source_measurements="ghost")

    assert status == 400
    assert "not found" in response["error"]
    assert influxdb3_local.writes == []


def test_backfill_reports_a_write_failure_as_500(resolver):
    influxdb3_local = backfill_client([unenriched(1_000)])
    influxdb3_local.write_error = Exception("disk full")

    response, status = backfill(influxdb3_local)

    assert status == 500
    assert "disk full" in response["error"]


# --- builtin strategy -------------------------------------------------------


MOSCOW = (55.7512, 37.6184)


def builtin(min_population=0, max_radius_m=math.inf):
    return plugin.BuiltinResolver(min_population, max_radius_m)


def test_builtin_resolver_names_the_settlement_at_a_point():
    attributes = builtin().resolve(*MOSCOW)

    assert attributes["country_code"] == "RU"
    assert attributes["city"] == "Moscow"
    assert set(attributes) == set(plugin.BUILTIN_ATTRIBUTES) | {
        plugin.DISTANCE_ATTRIBUTE
    }


def test_min_population_zooms_out_to_the_larger_settlement():
    """The knob that turns suburb-level answers into city-level ones."""
    suburb = (55.9, 37.5)

    assert builtin().resolve(*suburb)["city"] != "Moscow"
    assert builtin(min_population=5_000_000).resolve(*suburb)["city"] == "Moscow"


def test_builtin_resolver_reports_how_far_the_settlement_is():
    """The dataset holds city centers, so a valid point is kilometers away."""
    outskirts = (55.9, 37.5)

    assert builtin().resolve(*MOSCOW)[plugin.DISTANCE_ATTRIBUTE] < 1_000
    assert builtin(min_population=5_000_000).resolve(*outskirts)[
        plugin.DISTANCE_ATTRIBUTE
    ] == pytest.approx(19_000, abs=3_000)


def test_max_radius_turns_a_far_fetched_match_into_no_match():
    """Without a limit every point on Earth belongs to some settlement."""
    antarctica = (-82.0, 25.0)

    assert builtin().resolve(*antarctica)["country_code"] == "ZA"
    assert builtin(max_radius_m=50_000).resolve(*antarctica) is None
    assert builtin(max_radius_m=50_000).resolve(*MOSCOW) is not None


@pytest.mark.parametrize(
    "overrides, expected",
    [
        ({"strategy": "builtin"}, math.inf),
        ({"strategy": "nearest", "reference_file": "s.csv"}, 1000.0),
        ({"strategy": "builtin", "max_radius_m": "250"}, 250.0),
    ],
)
def test_max_radius_default_follows_the_strategy(overrides, expected):
    """1000 m is right for a site a vehicle parks at, useless for a city center."""
    assert config(**overrides)["max_radius_m"] == expected


# --- polygon strategy -------------------------------------------------------


def square(west, south, size):
    corners = [
        [west, south],
        [west + size, south],
        [west + size, south + size],
        [west, south + size],
        [west, south],
    ]
    return {"type": "Polygon", "coordinates": [corners]}


@pytest.fixture
def boundaries(tmp_path):
    """Two zones, the smaller one wholly inside the larger."""

    def write(features):
        path = tmp_path / "zones.geojson"
        path.write_text(json.dumps({"type": "FeatureCollection", "features": features}))
        return str(path)

    return write


OUTER = {"geometry": square(37.0, 55.0, 2.0), "properties": {"zone": "outer", "rank": 1}}
INNER = {"geometry": square(37.5, 55.5, 0.5), "properties": {"zone": "inner", "rank": 9}}


def reference(path, attributes=("zone",), priority_attribute="", **overrides):
    """Read a reference file the way build_resolver() does."""
    cfg = {
        "reference_file": path,
        "reference_encoding": "utf-8-sig",
        "reference_lat_column": "",
        "reference_lon_column": "",
        "reference_geometry_column": "",
        **overrides,
    }
    labeled = {name: "output_columns attribute" for name in attributes}
    if priority_attribute:
        labeled.setdefault(priority_attribute, "priority_attribute")
    return plugin.read_reference(cfg, labeled)


def polygon(path, policy="smallest", priority_attribute="", paths=("zone",)):
    return plugin.PolygonResolver(
        reference(path, paths, priority_attribute), policy, priority_attribute
    )


@pytest.mark.parametrize(
    "policy, priority_attribute, expected",
    [
        ("smallest", "", "inner"),
        ("largest", "", "outer"),
        ("first", "", "outer"),
        ("priority", "rank", "inner"),
    ],
)
def test_overlapping_zones_are_resolved_by_policy(
    boundaries, policy, priority_attribute, expected
):
    resolver = polygon(boundaries([OUTER, INNER]), policy, priority_attribute)

    assert resolver.resolve(55.6, 37.6)["zone"] == expected


@pytest.mark.parametrize(
    "policy, expected", [("smallest", "inner"), ("largest", "outer"), ("first", "inner")]
)
def test_area_policies_do_not_fall_back_to_file_order(boundaries, policy, expected):
    """The nested zone is listed first here, so area and file order disagree."""
    resolver = polygon(boundaries([INNER, OUTER]), policy)

    assert resolver.resolve(55.6, 37.6)["zone"] == expected


def test_polygon_resolver_reports_a_point_outside_every_zone(boundaries):
    resolver = polygon(boundaries([OUTER]))

    assert resolver.resolve(*MOSCOW) is not None
    assert resolver.resolve(-33.9, 151.2) is None


def test_polygon_resolver_binds_only_the_requested_paths(boundaries):
    """Properties are arbitrary, so what a zone can produce is what was asked for."""
    resolver = polygon(boundaries([OUTER, INNER]), paths=("zone", "rank"))

    assert resolver.attributes == {"zone", "rank"}
    assert resolver.resolve(55.6, 37.6) == {"zone": "inner", "rank": 9}


# --- nested zone properties -------------------------------------------------

NESTED = {
    "geometry": square(37.0, 55.0, 2.0),
    "properties": {
        "zone": "plant-A",
        "owner": {"name": "ACME", "contact": {"email": "ops@acme.io"}},
        "codes": ["A1", "A2"],
        "odd.name": "dotted",
    },
}


@pytest.mark.parametrize(
    "path, expected",
    [
        ("zone", "plant-A"),
        ("owner.name", "ACME"),
        ("owner.contact.email", "ops@acme.io"),
        ("codes[0]", "A1"),
        ('"odd.name"', "dotted"),
    ],
)
def test_nested_properties_are_addressed_by_path(boundaries, path, expected):
    resolver = polygon(boundaries([NESTED]), paths=(path,))

    assert resolver.resolve(55.5, 37.5)[path] == expected


@pytest.mark.parametrize("path", ["owner", "owner.contact", "codes"])
def test_a_path_that_stops_at_a_container_is_refused(boundaries, path):
    """Writing a nested object into a column produces an unusable value."""
    with pytest.raises(Exception, match="only single values can be written"):
        polygon(boundaries([NESTED]), paths=(path,))


def test_a_path_matching_nothing_is_refused(boundaries):
    with pytest.raises(Exception, match="output_columns attribute 'owner.phone'"):
        polygon(boundaries([NESTED]), paths=("owner.phone",))


def test_a_bad_priority_attribute_names_its_own_parameter(boundaries):
    """Reporting it as an output_columns fault sends the operator to the wrong knob."""
    with pytest.raises(Exception, match="priority_attribute 'rnak' matches no feature"):
        polygon(boundaries([NESTED]), "priority", "rnak")


def test_a_path_matching_several_values_is_refused(boundaries):
    with pytest.raises(Exception, match="matches 2 values"):
        polygon(boundaries([NESTED]), paths=("codes[*]",))


def test_a_malformed_path_is_refused(boundaries):
    with pytest.raises(Exception, match="is not a valid JSONPath"):
        polygon(boundaries([NESTED]), paths=("owner[",))


def test_a_property_missing_from_one_zone_only_is_allowed(boundaries):
    """A zone that simply lacks the property resolves to unknown_value."""
    plain = {"geometry": square(40.0, 10.0, 1.0), "properties": {"zone": "plain"}}
    resolver = polygon(boundaries([NESTED, plain]), paths=("zone", "owner.name"))

    assert resolver.resolve(55.5, 37.5)["owner.name"] == "ACME"
    assert "owner.name" not in resolver.resolve(10.5, 40.5)


def test_priority_attribute_may_be_a_nested_path(boundaries):
    low = {"geometry": square(37.0, 55.0, 2.0), "properties": {"zone": "low", "meta": {"rank": 1}}}
    high = {"geometry": square(37.5, 55.5, 0.5), "properties": {"zone": "high", "meta": {"rank": 9}}}
    resolver = polygon(boundaries([low, high]), "priority", "meta.rank", paths=("zone",))

    assert resolver.resolve(55.6, 37.6)["zone"] == "high"


@pytest.mark.parametrize(
    "features, reason",
    [
        ([], "contains no usable features"),
        ([{"geometry": square(37.0, 55.0, 1.0), "properties": {}}], "no properties"),
        (
            [{"geometry": {"type": "Polygon", "coordinates": "bad"},
              "properties": {"zone": "broken"}}],
            "needs Polygon or MultiPolygon geometries",
        ),
    ],
)
def test_unusable_reference_file_fails_at_load_time(boundaries, features, reason):
    """A broken reference file is a configuration error, not a per-row warning."""
    with pytest.raises(Exception, match=reason):
        polygon(boundaries(features))


def test_missing_reference_file_is_reported_with_its_path(tmp_path):
    with pytest.raises(Exception, match="cannot be read"):
        polygon(str(tmp_path / "absent.geojson"))


# --- nearest strategy -------------------------------------------------------


@pytest.fixture
def sites(tmp_path):
    def write(text):
        path = tmp_path / "sites.csv"
        path.write_text(text)
        return str(path)

    return write


DEPOTS = "name,latitude,longitude\ncenter,55.7512,37.6184\nnorth,55.9000,37.5000\n"


def nearest(path, max_radius_m=1_000, nearest_count=1, attributes=("name",), **overrides):
    return plugin.NearestResolver(
        reference(path, attributes, **overrides), max_radius_m, nearest_count
    )


def test_nearest_resolver_picks_the_closest_site_and_reports_the_distance(sites):
    resolver = nearest(sites(DEPOTS), max_radius_m=50_000)

    attributes = resolver.resolve(55.7600, 37.6200)

    assert attributes["name"] == "center"
    assert attributes[plugin.DISTANCE_ATTRIBUTE] == pytest.approx(980, abs=50)
    assert resolver.attributes == {"name", plugin.DISTANCE_ATTRIBUTE}


def test_sites_beyond_max_radius_do_not_match(sites):
    resolver = nearest(sites(DEPOTS), max_radius_m=500)

    assert resolver.resolve(55.7512, 37.6184) is not None
    assert resolver.resolve(55.7600, 37.6200) is None


def test_nearest_resolver_accepts_the_common_column_spellings(sites):
    resolver = nearest(sites("name,lat,lng\ncenter,55.7512,37.6184\n"))

    assert resolver.resolve(*MOSCOW)["name"] == "center"


def test_site_coordinate_columns_can_be_named_explicitly(sites):
    """The reference file may come from a system that names columns its own way."""
    resolver = nearest(
        sites("name,Y_Coord,X_Coord\ncenter,55.7512,37.6184\n"),
        reference_lat_column="y_coord",
        reference_lon_column="x_coord",
    )

    assert resolver.resolve(*MOSCOW) == {"name": "center", "distance_m": pytest.approx(0)}


def test_a_named_site_column_that_is_absent_is_refused(sites):
    with pytest.raises(Exception, match=r"no latitude column \(tried y_coord\)"):
        nearest(
            sites("name,lat,lon\ncenter,55.7512,37.6184\n"),
            reference_lat_column="y_coord",
        )


def test_rows_with_unusable_coordinates_are_dropped_from_the_index(sites):
    resolver = nearest(
        sites("name,lat,lon\nbroken,,\nfar,91.0,0.0\ncenter,55.7512,37.6184\n")
    )

    assert resolver.resolve(*MOSCOW)["name"] == "center"


@pytest.mark.parametrize(
    "text, reason",
    [
        ("name,lat,lon\n", "contains no rows"),
        ("name,x,y\ncenter,1,2\n", "carries no geometry"),
        ("name,lat,lon\nbroken,,\n", "no rows with usable geometry"),
    ],
)
def test_unusable_reference_csv_fails_at_load_time(sites, text, reason):
    with pytest.raises(Exception, match=reason):
        nearest(sites(text))


def test_further_ranks_describe_the_next_sites(sites):
    resolver = nearest(sites(DEPOTS), max_radius_m=50_000, nearest_count=2)

    attributes = resolver.resolve(55.7600, 37.6200)

    assert attributes["name"] == "center" and attributes["name_2"] == "north"
    assert attributes["distance_m_2"] > attributes["distance_m"]
    assert resolver.attributes == {"name", "name_2", "distance_m", "distance_m_2"}


def test_a_rank_beyond_the_radius_is_dropped_while_the_first_stays(sites):
    resolver = nearest(sites(DEPOTS), max_radius_m=2_000, nearest_count=2)

    assert resolver.resolve(55.7600, 37.6200) == {
        "name": "center", "distance_m": pytest.approx(980, abs=50)
    }


def test_every_rank_keeps_the_distance_column_typed_as_a_float(sites):
    """A ranked distance is still a number, so it cannot fall back to unknown_value."""
    cfg = config(
        strategy="nearest", reference_file=sites(DEPOTS), nearest_count="2",
        output_columns="name:geo_site distance_m:geo_dist", max_radius_m="50000",
    )

    assert cfg["unresolved_markers"] == {
        "geo_site": "UNKNOWN", "geo_dist": plugin.UNRESOLVED_DISTANCE,
        "geo_site_2": "UNKNOWN", "geo_dist_2": plugin.UNRESOLVED_DISTANCE,
    }


def test_asking_for_more_sites_than_the_file_holds_only_warns(sites):
    resolver = nearest(
        sites("name,lat,lon\ncenter,55.7512,37.6184\n"), nearest_count=3
    )

    assert resolver.resolve(*MOSCOW)["name"] == "center"
    assert any("nearest_count=3" in message for message in resolver.warnings)


# --- reference file: formats and reading ------------------------------------


def test_either_strategy_reads_either_format(sites, boundaries):
    """Format decides how the file is read, strategy decides how it is matched."""
    points = [{"geometry": {"type": "Point", "coordinates": [37.6184, 55.7512]},
               "properties": {"name": "center"}}]
    wkt_zones = sites(
        'zone,geometry\n'
        'outer,"POLYGON((37.0 55.0, 39.0 55.0, 39.0 57.0, 37.0 57.0, 37.0 55.0))"\n'
    )

    assert nearest(boundaries(points), max_radius_m=500).resolve(*MOSCOW)["name"] == "center"
    assert polygon(wkt_zones).resolve(*MOSCOW)["zone"] == "outer"


@pytest.mark.parametrize(
    "label, text, written_as, read_as, expected",
    [
        ("plain", "name,lat,lon\ncenter,55.7512,37.6184\n", "utf-8", "utf-8-sig", "center"),
        # Excel writes a byte-order mark that would otherwise rename the first column
        ("bom", "name,lat,lon\ncenter,55.7512,37.6184\n", "utf-8-sig", "utf-8-sig", "center"),
        ("semicolon", 'name;lat;lon\ncenter;"55,7512";"37,6184"\n', "utf-8", "utf-8-sig", "center"),
        ("cp1251", "name,lat,lon\nцентр,55.7512,37.6184\n", "cp1251", "cp1251", "центр"),
    ],
)
def test_csv_dialects_are_read(tmp_path, label, text, written_as, read_as, expected):
    path = tmp_path / "sites.csv"
    path.write_bytes(text.encode(written_as))
    resolver = nearest(str(path), reference_encoding=read_as)

    assert resolver.resolve(*MOSCOW)["name"] == expected


def test_a_comma_inside_a_quoted_value_stays_part_of_it(sites):
    """The decimal-comma reading must not fire when the delimiter is a comma."""
    resolver = nearest(
        sites('name,label,lat,lon\nc,"Moscow, RU",55.7512,37.6184\n'),
        attributes=("name", "label"),
    )

    assert resolver.resolve(*MOSCOW)["label"] == "Moscow, RU"


def test_geometry_column_and_coordinate_columns_are_alternatives(sites):
    with pytest.raises(Exception, match="set only one of them"):
        nearest(
            sites("name,lat,lon\nc,55.7,37.6\n"),
            reference_geometry_column="geom",
            reference_lat_column="lat",
        )


@pytest.mark.parametrize(
    "builder, reason",
    [
        ("nearest", "needs Point geometries"),
        ("polygon", "needs Polygon or MultiPolygon geometries"),
    ],
)
def test_geometry_the_strategy_cannot_use_is_refused(boundaries, sites, builder, reason):
    """A file of the wrong shape must fail loudly, not enrich every row with UNKNOWN."""
    if builder == "nearest":
        path, build = boundaries([OUTER]), lambda: nearest(path, attributes=("zone",))
    else:
        path, build = sites("zone,lat,lon\na,55.7,37.6\n"), lambda: polygon(path)

    with pytest.raises(Exception, match=reason):
        build()


def test_one_unusable_entry_does_not_stop_the_others(boundaries):
    label = {"geometry": {"type": "Point", "coordinates": [37.6, 55.6]},
             "properties": {"zone": "label"}}
    resolver = polygon(boundaries([OUTER, label]))

    assert resolver.resolve(55.6, 37.6)["zone"] == "outer"
    assert any("1 entries skipped" in message for message in resolver.warnings)


def test_a_zone_spanning_the_antimeridian_is_flagged(boundaries):
    """An unsplit ring matches the far side of the globe instead of its interior."""
    fiji = {"geometry": {"type": "Polygon", "coordinates":
            [[[179.0, -17.0], [-179.0, -17.0], [-179.0, -18.0], [179.0, -18.0],
              [179.0, -17.0]]]},
            "properties": {"zone": "fiji"}}

    resolver = polygon(boundaries([fiji]))

    assert any("antimeridian" in message for message in resolver.warnings)


def test_great_circle_distance_matches_haversine():
    """The index matches on the unit sphere, so chord length has to convert back."""
    for lat, lon in [(55.7512, 37.6184), (-33.87, 151.21), (0.0, 179.0)]:
        chord = math.dist(plugin.unit_vector(*MOSCOW), plugin.unit_vector(lat, lon))
        phi1, phi2 = math.radians(MOSCOW[0]), math.radians(lat)
        dphi, dlambda = phi2 - phi1, math.radians(lon - MOSCOW[1])
        haversine = 2 * plugin.EARTH_RADIUS_M * math.asin(
            math.sqrt(
                math.sin(dphi / 2) ** 2
                + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
            )
        )

        assert plugin.arc_length(chord) == pytest.approx(haversine, rel=1e-9)


def test_max_radius_beyond_half_the_globe_never_excludes_a_site():
    assert plugin.chord_length(math.pi * plugin.EARTH_RADIUS_M) == 2.0


# --- grid strategy ----------------------------------------------------------


@pytest.mark.parametrize(
    "grid_type, precision, level_of",
    [
        ("h3", 7, lambda cell: __import__("h3").get_resolution(cell)),
        ("geohash", 6, len),
        (
            "s2",
            9,
            lambda cell: __import__("s2sphere").CellId.from_token(cell).level(),
        ),
    ],
)
def test_grid_resolver_emits_a_cell_at_the_configured_precision(
    grid_type, precision, level_of
):
    cell = plugin.GridResolver(grid_type, precision).resolve(*MOSCOW)["cell"]

    assert level_of(cell) == precision


@pytest.mark.parametrize(
    "grid_type, coarse, fine", [("h3", 5, 9), ("geohash", 3, 8), ("s2", 6, 18)]
)
def test_finer_precision_separates_points_a_coarse_cell_shares(grid_type, coarse, fine):
    nearby = (55.7530, 37.6200)

    assert plugin.GridResolver(grid_type, coarse).resolve(*MOSCOW) == plugin.GridResolver(
        grid_type, coarse
    ).resolve(*nearby)
    assert plugin.GridResolver(grid_type, fine).resolve(*MOSCOW) != plugin.GridResolver(
        grid_type, fine
    ).resolve(*nearby)


def test_each_strategy_builds_its_own_resolver(boundaries, sites):
    columns = {
        "builtin": "country:c", "polygon": "zone:c", "nearest": "name:c", "grid": "cell:c",
    }
    built = {
        strategy: type(
            plugin.build_resolver(
                config(
                    strategy=strategy,
                    output_columns=columns[strategy],
                    reference_file=(
                        boundaries([OUTER]) if strategy == "polygon" else sites(DEPOTS)
                    ),
                )
            )
        )
        for strategy in plugin.STRATEGIES
    }

    assert built == {
        "builtin": plugin.BuiltinResolver,
        "polygon": plugin.PolygonResolver,
        "nearest": plugin.NearestResolver,
        "grid": plugin.GridResolver,
    }


# --- grid cells as input ----------------------------------------------------


def test_geohash_input_is_decoded_back_to_the_point():
    import pygeohash

    cfg = config(geohash_field="cell")
    row = {"cell": pygeohash.encode(*MOSCOW, precision=9)}

    lat, lon = plugin.extract_coordinates(row, cfg)

    assert (lat, lon) == pytest.approx(MOSCOW, abs=0.001)


def test_h3_input_is_decoded_back_to_the_point():
    import h3

    cfg = config(h3_field="cell")
    row = {"cell": h3.latlng_to_cell(*MOSCOW, 12)}

    lat, lon = plugin.extract_coordinates(row, cfg)

    assert (lat, lon) == pytest.approx(MOSCOW, abs=0.001)


@pytest.mark.parametrize(
    "overrides, row",
    [
        ({"geohash_field": "cell"}, {"cell": "not a geohash!"}),
        ({"h3_field": "cell"}, {"cell": "deadbeef"}),
        ({"geohash_field": "cell"}, {}),
    ],
)
def test_unusable_grid_input_yields_no_coordinates(overrides, row):
    assert plugin.extract_coordinates(row, config(**overrides)) is None


# --- line construction ------------------------------------------------------


def test_unresolved_distance_is_written_as_a_negative_marker():
    """distance_m is a float column, so it cannot carry the unknown string."""
    cfg = config(output_columns=f"name:site {plugin.DISTANCE_ATTRIBUTE}:site_distance")

    resolved = plugin.output_values(cfg, {"name": "center", "distance_m": 12.5})
    unresolved = plugin.output_values(cfg, None)

    assert resolved == {"site": ("center", "string"), "site_distance": (12.5, "float")}
    assert unresolved == {
        "site": ("UNKNOWN", "string"),
        "site_distance": (plugin.UNRESOLVED_DISTANCE, "float"),
    }


def test_attributes_are_written_as_strings_with_json_booleans():
    """Every column must be able to hold unknown_value, so only distance_m is
    typed. A boolean still has to read as JSON, not as a Python repr."""
    cfg = config(output_columns="population:geo_pop active:geo_active zone:geo_zone")

    values = plugin.output_values(
        cfg, {"population": 10381222, "active": True, "zone": "plant-A"}
    )

    assert values == {
        "geo_pop": ("10381222", "string"),
        "geo_active": ("true", "string"),
        "geo_zone": ("plant-A", "string"),
    }
    assert plugin.output_values(cfg, {"active": False})["geo_active"] == (
        "false",
        "string",
    )


def test_unknown_value_is_configurable(resolver):
    influxdb3_local, batches = write_client([gps_row(1_000, lat=10.0, lon=10.0)])

    plugin.process_writes(
        influxdb3_local, batches, {**BASE_ARGS, "unknown_value": "n/a"}
    )

    (_, record), = influxdb3_local.writes
    assert record.fields == {"geo_country": "n/a", "geo_city": "n/a"}


def test_tag_output_without_a_single_field_is_refused():
    """Line protocol has no field to carry, so the write would be rejected."""
    cfg = config(output_mode="tag", target_measurement="gps_geo")
    schema = {"tags": ["device"], "fields": {}}

    with pytest.raises(Exception, match="Nothing to write"):
        plugin.build_enrichment_line(
            {"device": "A", "time": 1},
            "gps",
            plugin.output_values(cfg, {"country": "Russia", "city": "Moscow"}),
            cfg,
            schema,
        )


def test_columns_of_an_unmapped_catalog_type_keep_their_python_type():
    """A data type outside LINE_TYPES still has to be copied to the target table."""
    cfg = config(output_mode="tag", target_measurement="gps_geo")
    schema = {"tags": ["device"], "fields": {"lat": "float", "note": None}}

    line = plugin.build_enrichment_line(
        {"device": "A", "lat": 55.7, "note": "checked", "time": 1},
        "gps",
        plugin.output_values(cfg, {"country": "Russia", "city": "Moscow"}),
        cfg,
        schema,
    )

    assert json.loads(line.build())["fields"] == {"lat": 55.7, "note": "checked"}


def test_enrichment_can_be_written_to_another_database(resolver):
    influxdb3_local, batches = write_client([gps_row(1_000)])

    plugin.process_writes(
        influxdb3_local, batches, {**BASE_ARGS, "target_database": "geo"}
    )

    (database, record), = influxdb3_local.writes
    assert database == "geo"
    assert record.fields == {
        "geo_country": "Russia",
        "geo_city": "Moscow",
        "lat": 55.7512,
        "lon": 37.6184,
        "speed": 60.0,
    }


# --- plumbing ---------------------------------------------------------------


def test_memo_evicts_the_least_recently_used_entry():
    memo = OrderedDict()
    for index in range(3):
        plugin.memo_store(memo, (index, index), index, cache_size=2)
    plugin.memo_lookup(memo, (1, 1))
    plugin.memo_store(memo, (9, 9), 9, cache_size=2)

    assert list(memo) == [(1, 1), (9, 9)]


@pytest.mark.parametrize(
    "body, expected",
    [
        (None, {}),
        ("", {}),
        ({"force": True}, {"force": True}),
        ('{"force": true}', {"force": True}),
    ],
)
def test_request_body_accepts_json_text_and_dicts(body, expected):
    assert plugin.parse_request_body(body) == expected


@pytest.mark.parametrize("body", ["[1, 2]", "not json"])
def test_unusable_request_body_is_rejected(body):
    with pytest.raises(ValueError):
        plugin.parse_request_body(body)


def test_absent_package_names_the_install_command():
    with pytest.raises(Exception, match="influxdb3 install package no_such_geo_lib"):
        plugin.require_package("no_such_geo_lib", "strategy=example")


def test_unknown_table_is_reported_by_name():
    with pytest.raises(Exception, match="Table 'ghost' not found"):
        plugin.resolve_schema(FakeLocal(), "ghost", "tid")


def test_a_table_created_after_the_first_miss_is_picked_up():
    """An empty catalog means 'not there yet'; caching it would brick the trigger."""
    influxdb3_local = FakeLocal(columns={})

    with pytest.raises(Exception, match="not found"):
        plugin.resolve_schema(influxdb3_local, "gps", "tid")
    influxdb3_local.columns["gps"] = dict(GPS_COLUMNS)

    assert "device" in plugin.resolve_schema(influxdb3_local, "gps", "tid")["tags"]


def test_table_and_column_names_are_quoted_for_sql():
    assert plugin.quote_identifier('we"ird') == '"we""ird"'


def test_timestamps_keep_nanosecond_precision_in_queries():
    assert plugin.ns_to_rfc3339(1_700_000_000_123_456_789) == (
        "2023-11-14T22:13:20.123456789Z"
    )


# --- packaging --------------------------------------------------------------


def test_docstring_header_is_valid_json_matching_the_entry_points():
    header = json.loads(plugin.__doc__)
    write_args = {arg["name"] for arg in header["onwrite_args_config"]}
    body_fields = {field["name"] for field in header["http_body_config"]}

    assert header["plugin_type"] == ["onwrite", "http"]
    assert write_args >= set(BASE_ARGS)
    # the endpoint reads no trigger arguments, so the body must declare every
    # setting the write trigger accepts, plus the backfill-only fields
    assert body_fields - write_args == {
        "start", "end", "batch_size", "retry_unknown", "force",
    }
    assert write_args - body_fields == set()


def test_settings_can_come_from_a_toml_file(monkeypatch, tmp_path):
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    (tmp_path / "geo.toml").write_text(
        'source_measurements = "gps"\n'
        'output_columns = "country:geo_country"\n'
        'strategy = "grid"\n'
        'grid_type = "geohash"\n'
        "grid_precision = 8\n"
    )

    cfg = plugin.normalize_config(
        FakeLocal(), {"config_file_path": "geo.toml"}, "tid"
    )

    assert cfg["sources"] == ["gps"]
    assert cfg["grid_type"] == "geohash"
    assert cfg["grid_precision"] == 8
