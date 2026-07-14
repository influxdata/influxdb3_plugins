"""Unit and integration tests for the signal_filter plugin.

Mirrors the mock-based approach used across this repo: fake influxdb3_local,
cache, LineBuilder, and TableBatch; no running engine required.
"""

import json
import math
import os
import sys
from collections import namedtuple

import numpy as np
import pytest
from scipy import signal as sp_signal

sys.path.insert(0, os.path.dirname(__file__))
import signal_filter as sf

AttrTableBatch = namedtuple("AttrTableBatch", ["table_name", "rows"])


def TableBatch(table_name, rows):
    """Live-engine WAL batch shape (verified on 3.10.2): a plain dict."""
    return {"table_name": table_name, "rows": rows}


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


def _fmt_float(value):
    # Mirror the real LineBuilder.float64_field rendering.
    return f"{int(value)}.0" if value % 1 == 0 else str(value)


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
        line += " " + ",".join(f"{k}={_fmt_float(v)}" for k, v in self.fields.items())
        if self.timestamp is not None:
            line += f" {self.timestamp}"
        return line


Record = namedtuple("Record", ["measurement", "tags", "fields", "timestamp"])


def _parse_lp(line):
    """Parse one line-protocol record (sufficient for this plugin's output)."""
    head, fields_str, ts = line.rsplit(" ", 2)
    parts = head.split(",")
    tags = dict(kv.split("=", 1) for kv in parts[1:])
    fields = {k: float(v) for k, v in (kv.split("=", 1) for kv in fields_str.split(","))}
    return Record(parts[0], tags, fields, int(ts))


class FakeLocal:
    def __init__(self, cache=None):
        self.cache = cache if cache is not None else FakeCache()
        self.infos = []
        self.warns = []
        self.errors = []
        self.writes = []  # (db_name | None, Record) per emitted point
        self.fail_writes = False

    def _record_batch(self, db_name, batch):
        # The plugin hands a _BatchLines; the engine calls build(). Exercise
        # that path, then expand back to one Record per line for assertions.
        for lp in batch.build().split("\n"):
            self.writes.append((db_name, _parse_lp(lp)))

    def info(self, *args):
        self.infos.append(" ".join(str(a) for a in args))

    def warn(self, *args):
        self.warns.append(" ".join(str(a) for a in args))

    def error(self, *args):
        self.errors.append(" ".join(str(a) for a in args))

    def write_sync(self, batch, no_sync=False):
        if self.fail_writes:
            raise RuntimeError("simulated write failure")
        self._record_batch(None, batch)

    def write_sync_to_db(self, db_name, batch, no_sync=False):
        if self.fail_writes:
            raise RuntimeError("simulated write failure")
        self._record_batch(db_name, batch)


@pytest.fixture(autouse=True)
def _plugin_env(monkeypatch):
    monkeypatch.setattr(sf, "LineBuilder", FakeLineBuilder, raising=False)
    sf._DESIGN_CACHE.clear()
    yield


def make_rows(times, values, tags=None, field="value", extra=None):
    rows = []
    for t, v in zip(times, values):
        row = {"time": t, field: v}
        row.update(tags or {})
        row.update(extra or {})
        rows.append(row)
    return rows


def written_values(local, field="value_filtered"):
    return [line.fields[field] for _, line in local.writes]


def written_times(local):
    return [line.timestamp for _, line in local.writes]


BASE_ARGS = {"fc2": "5.0", "sample_rate": "100.0"}


def run(local, rows, args=None, table="signal"):
    sf.process_writes(local, [TableBatch(table, rows)], dict(BASE_ARGS if args is None else args))


# ---------------------------------------------------------------------------
# M1 — skeleton
# ---------------------------------------------------------------------------


def test_docstring_header_is_valid_json_with_expected_args():
    header = json.loads(sf.__doc__)
    assert header["plugin_type"] == ["onwrite"]
    names = {arg["name"] for arg in header["onwrite_args_config"]}
    assert names == {
        "input_measurement", "input_fields", "tag_keys", "design_type", "prototype",
        "order", "ripple", "filter_type", "fc", "fc1", "fc2", "bessel_norm", "sos",
        "sample_rate", "init_from_first_sample", "output_target_database",
        "output_measurement", "output_field", "field_prefix", "field_suffix",
        "config_file_path",
    }


def test_missing_scipy_reports_install_command(monkeypatch):
    monkeypatch.setattr(sf, "np", None)
    monkeypatch.setattr(sf, "sp_signal", None)
    local = FakeLocal()
    sf.process_writes(local, [], {})
    assert len(local.errors) == 1
    assert "influxdb3 install package numpy scipy" in local.errors[0]


# ---------------------------------------------------------------------------
# M2 — config parsing + validation
# ---------------------------------------------------------------------------


def test_defaults():
    cfg = sf.parse_config({"fc2": "5.0"})
    assert cfg.input_fields == ("value",)
    assert cfg.design_type == "preset"
    assert cfg.prototype == "butter"
    assert cfg.order == 4
    assert cfg.filter_type == "lowpass"
    assert cfg.fc2 == 5.0
    assert cfg.init_from_first_sample is True
    assert cfg.field_prefix == ""
    assert cfg.field_suffix == "_filtered"
    assert cfg.sample_rate is None


@pytest.mark.parametrize(
    "args, fragment",
    [
        ({"design_type": "zap"}, "design_type"),
        ({"prototype": "ellip", "fc2": "5"}, "prototype"),
        ({"order": "0", "fc2": "5"}, "order"),
        ({"order": "13", "fc2": "5"}, "order"),
        ({"order": "four", "fc2": "5"}, "order"),
        ({"prototype": "cheby1", "fc2": "5"}, "ripple"),
        ({"prototype": "cheby1", "ripple": "0.001", "fc2": "5"}, "ripple"),
        ({"prototype": "cheby1", "ripple": "90", "fc2": "5"}, "ripple"),
        ({"ripple": "1", "fc2": "5"}, "ripple"),
        ({"filter_type": "notch"}, "filter_type"),
        ({"filter_type": "lowpass"}, "cutoff"),
        ({"filter_type": "highpass"}, "cutoff"),
        ({"filter_type": "bandpass", "fc1": "1"}, "fc2"),
        ({"filter_type": "bandpass", "fc": "3"}, "band"),
        ({"fc": "3", "fc2": "5"}, "both"),
        ({"filter_type": "highpass", "fc": "3", "fc1": "5"}, "both"),
        ({"fc1": "1", "fc2": "5"}, "fc1"),
        ({"filter_type": "highpass", "fc1": "1", "fc2": "5"}, "fc2"),
        ({"filter_type": "bandpass", "fc1": "5", "fc2": "5"}, "fc1"),
        ({"fc2": "-5"}, "fc2"),
        ({"fc2": "5", "sample_rate": "0"}, "sample_rate"),
        ({"fc2": "5", "bessel_norm": "weird"}, "bessel_norm"),
        ({"design_type": "manual"}, "sos"),
        ({"design_type": "manual", "sos": "not json"}, "JSON"),
        ({"design_type": "manual", "sos": "[[1, 2, 3]]"}, "6 coefficients"),
        ({"design_type": "manual", "sos": "[[1, 0, 0, 0, 0, 0]]"}, "a0"),
        ({"sos": "[[1,0,0,1,0,0]]", "fc2": "5"}, "manual"),
        ({"fc2": "5", "output_field": "x", "input_fields": "a b"}, "output_field"),
        ({"fc2": "5", "init_from_first_sample": "maybe"}, "init_from_first_sample"),
    ],
)
def test_rejections(args, fragment):
    with pytest.raises(sf.ConfigError) as excinfo:
        sf.parse_config(args)
    assert fragment.lower() in str(excinfo.value).lower()


def test_fc_alias_lowpass_and_highpass():
    assert sf.parse_config({"fc": "5"}).fc2 == 5.0
    cfg = sf.parse_config({"filter_type": "highpass", "fc": "2"})
    assert cfg.fc1 == 2.0
    assert cfg.fc2 is None


def test_manual_sos_a0_normalization():
    cfg = sf.parse_config({"design_type": "manual", "sos": "[[2, 0, 0, 2, 1, 0.5]]"})
    assert cfg.sos == ((1.0, 0.0, 0.0, 1.0, 0.5, 0.25),)


def test_toml_config_supplies_all_params(tmp_path):
    toml = tmp_path / "cfg.toml"
    toml.write_text('fc2 = 9.0\norder = 2\n')
    cfg = sf.parse_config({"config_file_path": str(toml)})
    assert cfg.fc2 == 9.0
    assert cfg.order == 2


def test_config_file_path_exclusive_with_inline_args(tmp_path):
    toml = tmp_path / "cfg.toml"
    toml.write_text("fc2 = 9.0\n")
    with pytest.raises(sf.ConfigError, match="mutually exclusive"):
        sf.parse_config({"config_file_path": str(toml), "fc2": "5"})


def test_toml_relative_path_requires_plugin_dir(tmp_path, monkeypatch):
    monkeypatch.delenv("PLUGIN_DIR", raising=False)
    monkeypatch.delenv("INFLUXDB3_PLUGIN_DIR", raising=False)
    monkeypatch.delenv("VIRTUAL_ENV", raising=False)
    with pytest.raises(sf.ConfigError, match="PLUGIN_DIR"):
        sf.parse_config({"config_file_path": "cfg.toml"})
    (tmp_path / "cfg.toml").write_text("fc2 = 7.0\n")
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    assert sf.parse_config({"config_file_path": "cfg.toml"}).fc2 == 7.0


def test_toml_relative_path_virtual_env_fallback(tmp_path, monkeypatch):
    monkeypatch.delenv("PLUGIN_DIR", raising=False)
    monkeypatch.delenv("INFLUXDB3_PLUGIN_DIR", raising=False)
    venv = tmp_path / "venv"
    venv.mkdir()
    (tmp_path / "cfg.toml").write_text("fc2 = 3.0\n")  # parent of the venv
    monkeypatch.setenv("VIRTUAL_ENV", str(venv))
    assert sf.parse_config({"config_file_path": "cfg.toml"}).fc2 == 3.0


def test_loop_hazard_detection():
    hazard = sf.parse_config({"fc2": "5", "field_suffix": ""})
    assert sf.loop_hazard_fields(hazard) == ["value"]
    assert sf.loop_hazard_fields(sf.parse_config({"fc2": "5"})) == []
    other_db = sf.parse_config(
        {"fc2": "5", "field_suffix": "", "output_target_database": "elsewhere"}
    )
    assert sf.loop_hazard_fields(other_db) == []
    other_meas = sf.parse_config(
        {"fc2": "5", "field_suffix": "", "input_measurement": "signal",
         "output_measurement": "signal_out"}
    )
    assert sf.loop_hazard_fields(other_meas) == []
    # output_measurement set but input unrestricted: rows in the output
    # measurement still loop back, so the hazard stands.
    all_tables = sf.parse_config(
        {"fc2": "5", "field_suffix": "", "output_measurement": "signal_out"}
    )
    assert sf.loop_hazard_fields(all_tables) == ["value"]


# ---------------------------------------------------------------------------
# M3 — sample rate
# ---------------------------------------------------------------------------


def test_infer_uniform():
    times = [i * 100_000_000 for i in range(10)]  # 10 Hz
    assert sf.infer_sample_rate(times) == pytest.approx(10.0)


def test_infer_jittered_median_robust():
    base = [i * 100_000_000 for i in range(20)]
    base[5] += 30_000_000  # jitter one point
    base[13] -= 20_000_000
    assert sf.infer_sample_rate(base) == pytest.approx(10.0, rel=0.01)


def test_infer_insufficient():
    assert sf.infer_sample_rate([]) is None
    assert sf.infer_sample_rate([123]) is None
    assert sf.infer_sample_rate([123, 123]) is None


def test_merge_warmup_accumulates_and_caps():
    state = {"warmup_times": [1, 2, 3]}
    merged = sf.merge_warmup_times(state, [3, 4, 5])
    assert merged == [1, 2, 3, 4, 5]
    big = sf.merge_warmup_times({"warmup_times": list(range(100))}, [200])
    assert len(big) == sf.WARMUP_MAX_TIMES
    assert big[-1] == 200


# ---------------------------------------------------------------------------
# M4 — filter design
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("prototype", ["butter", "cheby1", "bessel"])
@pytest.mark.parametrize("filter_type", ["lowpass", "highpass", "bandpass", "bandstop"])
def test_preset_matrix_matches_scipy(prototype, filter_type):
    args = {"prototype": prototype, "filter_type": filter_type, "order": "4"}
    if prototype == "cheby1":
        args["ripple"] = "1.0"
    if filter_type == "lowpass":
        args["fc2"] = "5.0"
        wn = 5.0
    elif filter_type == "highpass":
        args["fc1"] = "5.0"
        wn = 5.0
    else:
        args["fc1"], args["fc2"] = "2.0", "8.0"
        wn = [2.0, 8.0]
    cfg = sf.parse_config(args)
    sos, coeff_hash = sf.design_filter(cfg, 100.0)
    if prototype == "butter":
        expected = sp_signal.butter(4, wn, btype=filter_type, output="sos", fs=100.0)
    elif prototype == "cheby1":
        expected = sp_signal.cheby1(4, 1.0, wn, btype=filter_type, output="sos", fs=100.0)
    else:
        expected = sp_signal.bessel(4, wn, btype=filter_type, norm="phase", output="sos", fs=100.0)
    np.testing.assert_array_equal(sos, expected)
    assert isinstance(coeff_hash, str) and len(coeff_hash) == 64


def test_design_memoized():
    cfg = sf.parse_config(BASE_ARGS)
    sos_a, hash_a = sf.design_filter(cfg, 100.0)
    sos_b, hash_b = sf.design_filter(cfg, 100.0)
    assert sos_a is sos_b
    assert hash_a == hash_b


def test_coeff_hash_changes_with_fs_and_params():
    cfg = sf.parse_config(BASE_ARGS)
    _, hash_100 = sf.design_filter(cfg, 100.0)
    _, hash_50 = sf.design_filter(cfg, 50.0)
    assert hash_100 != hash_50
    cfg2 = sf.parse_config({**BASE_ARGS, "order": "2"})
    _, hash_o2 = sf.design_filter(cfg2, 100.0)
    assert hash_o2 != hash_100


def test_nyquist_rejected():
    cfg = sf.parse_config({"fc2": "6.0"})
    with pytest.raises(ValueError, match="fs/2"):
        sf.design_iir(cfg, 10.0)


def test_unstable_manual_sos_rejected():
    # z^2 - 2.5z + 1 has poles at 2.0 and 0.5 -> unstable
    cfg = sf.parse_config({"design_type": "manual", "sos": "[[1, 0, 0, 1, -2.5, 1]]"})
    with pytest.raises(ValueError, match="unstable"):
        sf.design_iir(cfg, None)


def test_stable_manual_sos_accepted():
    cfg = sf.parse_config({"design_type": "manual", "sos": "[[0.2, 0.4, 0.2, 1, -0.4, 0.2]]"})
    sos = sf.design_iir(cfg, None)
    assert sos.shape == (1, 6)


# ---------------------------------------------------------------------------
# M5 — streaming runtime
# ---------------------------------------------------------------------------


def test_chunked_apply_equals_whole_signal():
    rng = np.random.default_rng(42)
    x = rng.normal(size=200)
    sos = sp_signal.butter(4, 5.0, output="sos", fs=100.0)
    zi = sf.init_zi(sos, x[0], True)
    whole, _ = sf.apply_filter(sos, x, zi.copy())
    parts = []
    zi_run = zi.copy()
    for chunk in np.array_split(x, 7):
        y, zi_run = sf.apply_filter(sos, chunk, zi_run)
        parts.append(y)
    np.testing.assert_array_equal(np.concatenate(parts), whole)


def test_init_from_first_sample_suppresses_transient():
    sos = sp_signal.butter(4, 5.0, output="sos", fs=100.0)
    dc = np.full(50, 7.5)
    primed, _ = sf.apply_filter(sos, dc, sf.init_zi(sos, 7.5, True))
    np.testing.assert_allclose(primed, 7.5, rtol=1e-9)
    cold, _ = sf.apply_filter(sos, dc, sf.init_zi(sos, 7.5, False))
    assert abs(cold[0] - 7.5) > 1.0  # visible startup transient


# ---------------------------------------------------------------------------
# M6 — extraction
# ---------------------------------------------------------------------------


def test_tag_heuristic_strings_minus_time_and_inputs():
    rows = [{"time": 1, "value": 1.0, "host": "a", "count": 3}]
    groups, dropped = sf.extract_series(rows, ("value",), None)
    assert list(groups) == [("value", (("host", "a"),))]
    assert dropped == 0


def test_tag_keys_override():
    rows = [{"time": 1, "value": 1.0, "host": "a", "note": "x"}]
    groups, _ = sf.extract_series(rows, ("value",), ("host",))
    assert list(groups) == [("value", (("host", "a"),))]


def test_nonfinite_dropped_and_counted():
    rows = make_rows([1, 2, 3, 4], [1.0, float("nan"), float("inf"), 2.0])
    groups, dropped = sf.extract_series(rows, ("value",), None)
    assert dropped == 2
    assert groups[("value", ())] == [(1, 1.0), (4, 2.0)]


def test_duplicate_timestamps_last_wins():
    rows = make_rows([1, 2, 2, 3], [1.0, 5.0, 9.0, 3.0])
    groups, _ = sf.extract_series(rows, ("value",), None)
    assert groups[("value", ())] == [(1, 1.0), (2, 9.0), (3, 3.0)]


def test_unsorted_times_sorted():
    rows = make_rows([3, 1, 2], [3.0, 1.0, 2.0])
    groups, _ = sf.extract_series(rows, ("value",), None)
    assert groups[("value", ())] == [(1, 1.0), (2, 2.0), (3, 3.0)]


def test_null_bool_and_string_values_skipped():
    rows = [
        {"time": 1, "value": None},
        {"time": 2, "value": True},
        {"time": 3, "value": "text"},
    ]
    groups, dropped = sf.extract_series(rows, ("value",), None)
    assert groups == {}
    assert dropped == 0


# ---------------------------------------------------------------------------
# M7 — per-series state
# ---------------------------------------------------------------------------


def uniform_times(n, start=0, fs=100.0):
    step = int(1e9 / fs)
    return [start + i * step for i in range(n)]


def test_state_saved_after_run():
    local = FakeLocal()
    times = uniform_times(20)
    run(local, make_rows(times, np.sin(np.arange(20)).tolist(), tags={"host": "a"}))
    assert len(local.writes) == 20
    (key, state), = local.cache.store.items()
    assert key.startswith("signal_filter:signal:value:")
    assert state["fs"] == 100.0
    assert state["last_time_ns"] == times[-1]
    assert state["warmup_times"] == []
    assert isinstance(state["zi"], list)
    np.asarray(state["zi"], dtype=np.float64)  # round-trips


def test_out_of_order_samples_dropped_with_warning():
    local = FakeLocal()
    times = uniform_times(20)
    values = np.sin(np.arange(20)).tolist()
    run(local, make_rows(times[:10], values[:10]))
    first_writes = len(local.writes)
    # second commit replays old timestamps plus new ones
    run(local, make_rows(times[5:], values[5:]))
    assert any("out-of-order" in w for w in local.warns)
    assert len(local.writes) == first_writes + 10  # only times[10:] filtered
    assert written_times(local) == times


def test_param_change_resets_state():
    local = FakeLocal()
    times = uniform_times(20)
    values = np.sin(np.arange(20)).tolist()
    run(local, make_rows(times[:10], values[:10]))
    hash_before = list(local.cache.store.values())[0]["coeff_hash"]
    run(local, make_rows(times[10:], values[10:]), args={**BASE_ARGS, "fc2": "8.0"})
    hash_after = list(local.cache.store.values())[0]["coeff_hash"]
    assert hash_before != hash_after


def test_corrupt_cached_zi_shape_reinitializes():
    local = FakeLocal()
    times = uniform_times(20)
    values = np.sin(np.arange(20)).tolist()
    run(local, make_rows(times[:10], values[:10]))
    key, state = next(iter(local.cache.store.items()))
    state["zi"] = [[0.0, 0.0]]  # wrong section count for a 4th-order filter
    run(local, make_rows(times[10:], values[10:]))
    assert any("re-initializing" in w for w in local.warns)
    assert len(local.writes) == 20  # second batch still filtered
    healed = local.cache.store[key]["zi"]
    assert np.asarray(healed).shape != (1, 2)


def test_explicit_sample_rate_beats_frozen_state():
    local = FakeLocal()
    times = uniform_times(20)
    values = list(range(20))
    run(local, make_rows(times[:10], [float(v) for v in values[:10]]), args={"fc2": "5.0"})
    state = list(local.cache.store.values())[0]
    assert state["fs"] == pytest.approx(100.0)  # inferred and frozen
    run(
        local,
        make_rows(times[10:], [float(v) for v in values[10:]]),
        args={"fc2": "5.0", "sample_rate": "50.0"},
    )
    state = list(local.cache.store.values())[0]
    assert state["fs"] == 50.0


# ---------------------------------------------------------------------------
# M8 — adapter integration
# ---------------------------------------------------------------------------


def test_split_commits_equal_single_commit():
    n = 100
    times = uniform_times(n)
    rng = np.random.default_rng(7)
    values = (np.sin(2 * np.pi * 1.0 * np.arange(n) / 100.0) + 0.1 * rng.normal(size=n)).tolist()
    rows = make_rows(times, values, tags={"host": "a"})

    single = FakeLocal()
    run(single, rows)

    split = FakeLocal()
    for i in range(0, n, 20):
        run(split, rows[i : i + 20])

    assert written_times(single) == written_times(split)
    assert written_values(single) == written_values(split)  # float-exact


def test_lowpass_attenuates_high_tone_passes_low_tone():
    fs, n = 100.0, 400
    t = np.arange(n) / fs
    low = np.sin(2 * np.pi * 1.0 * t)
    high = np.sin(2 * np.pi * 30.0 * t)
    rows = make_rows(uniform_times(n), (low + high).tolist())
    local = FakeLocal()
    run(local, rows)
    out = np.asarray(written_values(local))[100:]  # skip settling
    t_s = t[100:]

    def amplitude(sig, freq):
        c = np.cos(2 * np.pi * freq * t_s)
        s = np.sin(2 * np.pi * freq * t_s)
        return 2 * math.hypot(np.dot(sig, c) / len(sig), np.dot(sig, s) / len(sig))

    assert amplitude(out, 30.0) < 0.1  # >= 20 dB down
    assert amplitude(out, 1.0) > 0.7


def test_write_failure_leaves_state_unsaved_and_retry_is_clean():
    n = 40
    times = uniform_times(n)
    values = np.sin(np.arange(n) * 0.3).tolist()
    rows = make_rows(times, values)

    reference = FakeLocal()
    run(reference, rows[:20])
    run(reference, rows[20:])

    local = FakeLocal()
    run(local, rows[:20])
    state_before = json.dumps(local.cache.store, sort_keys=True, default=str)
    local.fail_writes = True
    with pytest.raises(RuntimeError):
        run(local, rows[20:])
    assert json.dumps(local.cache.store, sort_keys=True, default=str) == state_before
    local.fail_writes = False
    run(local, rows[20:])  # retry
    assert written_values(local) == written_values(reference)
    assert written_times(local) == written_times(reference)


def test_refired_rows_with_only_output_field_are_skipped():
    local = FakeLocal()
    rows = [
        {"time": t, "value": None, "value_filtered": 1.0, "host": "a"}
        for t in uniform_times(10)
    ]
    run(local, rows)
    assert local.writes == []
    assert local.errors == []


def test_warmup_accumulates_across_commits_then_filters():
    local = FakeLocal()
    times = uniform_times(9)
    values = [float(i) for i in range(9)]
    args = {"fc2": "5.0"}  # no sample_rate -> inference with warm-up
    run(local, make_rows(times[:3], values[:3]), args=args)
    assert local.writes == []
    run(local, make_rows(times[3:6], values[3:6]), args=args)
    assert local.writes == []
    assert any("warm" in i or "inferring" in i for i in local.infos)
    run(local, make_rows(times[6:], values[6:]), args=args)
    assert len(local.writes) == 3  # only the freezing batch is emitted
    state = list(local.cache.store.values())[0]
    assert state["fs"] == pytest.approx(100.0)
    assert state["warmup_times"] == []


def test_single_sample_commits_never_freeze_bad_fs():
    local = FakeLocal()
    args = {"fc2": "5.0"}
    for i in range(5):
        run(local, make_rows([uniform_times(9)[i]], [float(i)]), args=args)
    assert local.writes == []
    state = list(local.cache.store.values())[0]
    assert "fs" not in state or state.get("fs") is None


def test_multiple_input_fields_filtered_independently():
    local = FakeLocal()
    times = uniform_times(20)
    rows = []
    for i, t in enumerate(times):
        rows.append({"time": t, "a": float(i), "b": float(-i), "host": "x"})
    run(local, rows, args={**BASE_ARGS, "input_fields": "a b"})
    fields = {name for _, line in local.writes for name in line.fields}
    assert fields == {"a_filtered", "b_filtered"}
    assert len(local.writes) == 40


def test_series_isolation_by_tags():
    local = FakeLocal()
    times = uniform_times(20)
    rows_a = make_rows(times, [1.0] * 20, tags={"host": "a"})
    rows_b = make_rows(times, [100.0] * 20, tags={"host": "b"})
    run(local, rows_a + rows_b)
    by_host = {}
    for _, line in local.writes:
        by_host.setdefault(dict(line.tags)["host"], []).append(line.fields["value_filtered"])
    np.testing.assert_allclose(by_host["a"], 1.0, rtol=1e-9)
    np.testing.assert_allclose(by_host["b"], 100.0, rtol=1e-9)
    assert len(local.cache.store) == 2


def test_output_naming_and_routing():
    local = FakeLocal()
    times = uniform_times(20)
    run(
        local,
        make_rows(times, [1.0] * 20),
        args={
            **BASE_ARGS,
            "output_field": "smooth",
            "field_prefix": "f_",
            "field_suffix": "_out",
            "output_measurement": "clean",
            "output_target_database": "otherdb",
        },
    )
    db, line = local.writes[0]
    assert db == "otherdb"
    assert line.measurement == "clean"
    assert "f_smooth_out" in line.fields


def test_input_measurement_filters_tables():
    local = FakeLocal()
    rows = make_rows(uniform_times(20), [1.0] * 20)
    sf.process_writes(
        local,
        [TableBatch("noise", rows), TableBatch("signal", rows)],
        {**BASE_ARGS, "input_measurement": "signal"},
    )
    assert len(local.writes) == 20
    assert all(line.measurement == "signal" for _, line in local.writes)


def test_config_error_logged_not_raised():
    local = FakeLocal()
    sf.process_writes(local, [], {"design_type": "bogus"})
    assert len(local.errors) == 1
    assert "invalid configuration" in local.errors[0]


def test_loop_hazard_warning_emitted():
    local = FakeLocal()
    run(local, make_rows(uniform_times(20), [1.0] * 20), args={**BASE_ARGS, "field_suffix": ""})
    assert any("write loop" in w for w in local.warns)


def test_summary_logged():
    local = FakeLocal()
    run(local, make_rows(uniform_times(20), [1.0] * 20))
    assert any("points written" in i for i in local.infos)


def test_attribute_style_table_batch_also_accepted():
    local = FakeLocal()
    rows = make_rows(uniform_times(20), [1.0] * 20)
    sf.process_writes(local, [AttrTableBatch("signal", rows)], dict(BASE_ARGS))
    assert len(local.writes) == 20


def test_manual_mode_needs_no_sample_rate():
    local = FakeLocal()
    times = uniform_times(3)  # far below warm-up threshold
    run(
        local,
        make_rows(times, [1.0, 2.0, 3.0]),
        args={"design_type": "manual", "sos": "[[0.5, 0, 0, 1, 0, 0]]"},
    )
    assert written_values(local) == [0.5, 1.0, 1.5]


# ---------------------------------------------------------------------------
# M9 — batched writes
# ---------------------------------------------------------------------------


def test_series_written_as_single_batch():
    # One write_sync call per series (batched), not one per point.
    calls = []
    local = FakeLocal()
    orig = local._record_batch

    def counting(db_name, batch):
        calls.append(db_name)
        orig(db_name, batch)

    local._record_batch = counting
    run(local, make_rows(uniform_times(20), [1.0] * 20, tags={"host": "a"}))
    assert calls == [None]  # single batched write for the one series
    assert len(local.writes) == 20  # expanded back to 20 points
