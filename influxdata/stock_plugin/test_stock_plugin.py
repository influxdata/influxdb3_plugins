"""Unit and integration tests for the stock_plugin plugin."""

import json
import os
import sys
from collections import namedtuple
from datetime import datetime
from textwrap import dedent

import pytest
from influxdata_plugin_utils import write as utils_write

sys.path.insert(0, os.path.dirname(__file__))
import stock_plugin as sp

# Captured before the fixtures patch the module attribute.
REAL_IS_MARKET_OPEN = sp._is_market_open


def ns(iso: str) -> int:
    return int(datetime.fromisoformat(iso).timestamp() * 1_000_000_000)


# 2023-11-15 is a Wednesday; the market timezone default is America/New_York.
BEFORE_NAV = ns("2023-11-15T15:00:00-05:00")
AFTER_NAV = ns("2023-11-15T19:00:00-05:00")
TODAY_LOCAL = "2023-11-15"


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeCache:
    def __init__(self, initial=None):
        self.store = dict(initial or {})

    def get(self, key, default=None, use_global=None):
        return self.store.get(key, default)

    def put(self, key, value, ttl=None, use_global=None):
        self.store[key] = value


class FakeLineBuilder:
    def __init__(self, measurement):
        self.measurement = measurement
        self.tags = {}
        self.fields = {}
        self.timestamp = None

    def tag(self, key, value):
        self.tags[key] = value
        return self

    def int64_field(self, key, value):
        self.fields[key] = f"{value}i"
        return self

    def uint64_field(self, key, value):
        self.fields[key] = f"{value}u"
        return self

    def float64_field(self, key, value):
        self.fields[key] = repr(float(value))
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
            line += "," + ",".join(f"{k}={v}" for k, v in self.tags.items())
        line += " " + ",".join(f"{k}={v}" for k, v in self.fields.items())
        return f"{line} {self.timestamp}"


Record = namedtuple("Record", ["measurement", "tags", "fields", "timestamp"])


def _parse_field(raw):
    if raw.startswith('"'):
        return raw[1:-1]
    if raw[-1] in ("i", "u"):
        return int(raw[:-1])
    return float(raw)


def _parse_lp(line):
    head, fields_str, timestamp = line.rsplit(" ", 2)
    parts = head.split(",")
    tags = dict(kv.split("=", 1) for kv in parts[1:])
    fields = {
        key: _parse_field(value)
        for key, value in (kv.split("=", 1) for kv in fields_str.split(","))
    }
    return Record(parts[0], tags, fields, int(timestamp))


class FakeLocal:
    def __init__(self, cache=None, write_error=None):
        self.cache = FakeCache(cache)
        self.write_error = write_error
        self.writes = []  # one (database, [Record]) per write call
        self.infos = []
        self.warns = []
        self.errors = []

    def write_to_db(self, database, batch):
        if self.write_error is not None:
            raise self.write_error
        lines = [_parse_lp(lp) for lp in batch.build().split("\n")]
        self.writes.append((database, lines))

    def write(self, batch):
        raise AssertionError("writes must target an explicit database")

    def info(self, message):
        self.infos.append(message)

    def warn(self, message):
        self.warns.append(message)

    def error(self, message):
        self.errors.append(message)


@pytest.fixture(autouse=True)
def plugin_dir(tmp_path, monkeypatch):
    """Point config resolution at an empty directory, never the real plugin dir."""
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    monkeypatch.delenv("INFLUXDB3_PLUGIN_DIR", raising=False)
    monkeypatch.setattr(sp, "LineBuilder", FakeLineBuilder, raising=False)
    monkeypatch.setattr(utils_write.time, "sleep", lambda _: None)
    return tmp_path


@pytest.fixture(autouse=True)
def market(monkeypatch):
    """Market state, so gating tests do not depend on the real calendar or clock."""
    state = {"open": True}
    monkeypatch.setattr(sp, "_is_market_open", lambda *_: state["open"])
    return state


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def write_toml(plugin_dir, body, name=sp.DEFAULT_CONFIG_FILE):
    path = plugin_dir / name
    path.write_text(dedent(body))
    return path


def quote_fetcher(prices=None, asset_types=None, failures=(), currency="USD"):
    """A fetcher returning deterministic quotes and raising for `failures`."""

    def fetch(symbol):
        if symbol in failures:
            raise RuntimeError(f"no data for {symbol}")
        return sp.Quote(
            symbol=symbol,
            price=(prices or {}).get(symbol, 100.0),
            currency=currency,
            asset_type=(asset_types or {}).get(symbol, "equity"),
            previous_close=99.0,
            day_open=None,
            day_high=101.0,
            day_low=None,
        )

    return fetch


def run(args=None, cache=None, now_ns=AFTER_NAV, fetcher=None, local=None):
    local = local or FakeLocal(cache)
    sp._main(
        local=local,
        args=args or {},
        fetcher=fetcher or quote_fetcher(),
        line_builder_cls=FakeLineBuilder,
        now_ns=now_ns,
        task_id="test",
    )
    return local


def records(local, measurement=None):
    rows = [row for _, lines in local.writes for row in lines]
    if measurement:
        rows = [row for row in rows if row.measurement == measurement]
    return rows


def resolve_config(args=None, local=None):
    return sp.resolve_config(local or FakeLocal(), args or {}, "test")


def holdings_of(config):
    return {
        portfolio: [(h.symbol, h.quantity) for h in holdings]
        for portfolio, holdings in config.holdings_by_portfolio.items()
    }


# ---------------------------------------------------------------------------
# M1 — plugin metadata
# ---------------------------------------------------------------------------


def test_docstring_metadata_covers_every_supported_argument():
    header = json.loads(sp.__doc__)
    assert header["plugin_type"] == ["scheduled"]
    names = [arg["name"] for arg in header["scheduled_args_config"]]
    validated = {validator.names[0] for validator in sp.VALIDATORS}
    # every validated key is documented, plus the TOML path itself
    assert set(names) == validated | {"config_path"}
    for entry in header["scheduled_args_config"]:
        assert set(entry) == {"name", "example", "description", "required"}


# ---------------------------------------------------------------------------
# M2 — configuration resolution
# ---------------------------------------------------------------------------


def test_defaults_apply_without_any_configuration():
    config = resolve_config({})
    assert config.database == "stocks"
    assert config.write_during_closed_hours is True
    assert config.mutual_fund_check_time == "18:00"
    assert config.market_calendar == "NYSE"
    assert config.market_timezone == "America/New_York"
    assert holdings_of(config) == {"main": [("AAPL", 1.0), ("MSFT", 1.0), ("GOOG", 1.0)]}
    assert config.categories == {}


def test_toml_supplies_holdings_categories_and_scalars(plugin_dir):
    write_toml(
        plugin_dir,
        """
        database = "portfolio"
        write_during_closed_hours = false
        mutual_fund_check_time = "20:30"
        market_calendar = "LSE"
        market_timezone = "Europe/London"

        [portfolio_categories]
        "401k" = "Retirement"

        [holdings.401k]
        AAPL = 10
        "VOD.L" = 2.5
        """,
    )
    config = resolve_config({})
    assert config.database == "portfolio"
    assert config.write_during_closed_hours is False
    assert config.mutual_fund_check_time == "20:30"
    assert config.market_calendar == "LSE"
    assert config.market_timezone == "Europe/London"
    assert holdings_of(config) == {"401k": [("AAPL", 10.0), ("VOD.L", 2.5)]}
    assert config.categories == {"401k": "Retirement"}


def test_trigger_arguments_override_toml_keys(plugin_dir):
    write_toml(
        plugin_dir,
        """
        database = "from_toml"
        write_during_closed_hours = false
        market_calendar = "LSE"

        [holdings.401k]
        AAPL = 10
        """,
    )
    config = resolve_config(
        {
            "database": "from_args",
            "write_during_closed_hours": "true",
            "market_calendar": "NYSE",
        }
    )
    assert config.database == "from_args"
    assert config.write_during_closed_hours is True
    assert config.market_calendar == "NYSE"
    # holdings still come from the file
    assert holdings_of(config) == {"401k": [("AAPL", 10.0)]}


def test_inline_holdings_replace_the_toml_tables_but_keep_toml_scalars(plugin_dir):
    write_toml(
        plugin_dir,
        """
        database = "portfolio"

        [portfolio_categories]
        "401k" = "Retirement"

        [holdings.brokerage]
        GOOG = 5
        """,
    )
    config = resolve_config({"portfolio": "AAPL:2:401k"})
    assert holdings_of(config) == {"401k": [("AAPL", 2.0)]}
    assert config.database == "portfolio"
    assert config.categories == {"401k": "Retirement"}


def test_explicit_config_path_must_exist(plugin_dir):
    write_toml(plugin_dir, '[holdings.main]\nAAPL = 1\n', name="custom.toml")
    assert holdings_of(resolve_config({"config_path": "custom.toml"})) == {
        "main": [("AAPL", 1.0)]
    }
    with pytest.raises(ValueError, match="no TOML config found"):
        resolve_config({"config_path": "absent.toml"})


@pytest.mark.parametrize(
    "args, fragment",
    [
        ({"mutual_fund_check_time": "25:00"}, "out of range"),
        ({"mutual_fund_check_time": "noon"}, "expected HH:MM"),
        ({"write_during_closed_hours": "maybe"}, "Invalid boolean"),
        ({"market_timezone": "Mars/Olympus"}, "not a valid IANA timezone"),
        ({"market_calendar": "NOPE"}, "not accepted by pandas_market_calendars"),
        ({"portfolio": "AAPL:1:_total"}, "'_total' is reserved"),
    ],
)
def test_invalid_configuration_is_rejected(args, fragment):
    with pytest.raises(ValueError, match=fragment):
        resolve_config(args)


def test_duplicate_symbols_in_one_portfolio_are_aggregated():
    config = resolve_config({"portfolio": "AAPL:2:401k|MSFT:1:401k|AAPL:3:401k"})
    assert holdings_of(config) == {"401k": [("AAPL", 5.0), ("MSFT", 1.0)]}


# ---------------------------------------------------------------------------
# M3 — inline argument parsing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value, expected",
    [
        ("AAPL:10", {"main": [("AAPL", 10.0)]}),
        ("AAPL:10|MSFT:5", {"main": [("AAPL", 10.0), ("MSFT", 5.0)]}),
        ("aapl:2.5:401k", {"401k": [("AAPL", 2.5)]}),
        ("AAPL:1||MSFT:2", {"main": [("AAPL", 1.0), ("MSFT", 2.0)]}),
        (" AAPL:1 | MSFT:2 ", {"main": [("AAPL", 1.0), ("MSFT", 2.0)]}),
        ("AAPL:-1", {"main": [("AAPL", -1.0)]}),
    ],
)
def test_parse_inline_portfolio(value, expected):
    parsed = sp.parse_inline_portfolio(value)
    assert {p: [(h.symbol, h.quantity) for h in hs] for p, hs in parsed.items()} == expected


@pytest.mark.parametrize(
    "value, fragment",
    [
        ("", "is empty"),
        ("AAPL", "expected SYMBOL:QUANTITY"),
        ("AAPL:1:401k:extra", "expected SYMBOL:QUANTITY"),
        (":1", "empty symbol"),
        ("AAPL:many", "invalid quantity"),
        ("AAPL:inf", "must be a finite number"),
        ("AAPL:nan", "must be a finite number"),
        ("AAPL:1e400", "must be a finite number"),
    ],
)
def test_parse_inline_portfolio_rejects_malformed_input(value, fragment):
    with pytest.raises(ValueError, match=fragment):
        sp.parse_inline_portfolio(value)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("401k:Retirement", {"401k": "Retirement"}),
        ("401k:Retirement|brokerage:Investment",
         {"401k": "Retirement", "brokerage": "Investment"}),
        ("401k:Retirement||brokerage:Investment",
         {"401k": "Retirement", "brokerage": "Investment"}),
    ],
)
def test_parse_inline_categories(value, expected):
    assert sp.parse_inline_categories(value) == expected


@pytest.mark.parametrize(
    "value, fragment",
    [
        ("|", "is empty"),
        ("401k", "expected PORTFOLIO:CATEGORY"),
        ("401k:", "empty portfolio or category name"),
        (":Retirement", "empty portfolio or category name"),
        ("401k:Retirement:extra", "expected PORTFOLIO:CATEGORY"),
        # the message quotes the user's own token, not the parsed halves
        ("401k : Retirement : extra", r"'401k : Retirement : extra'"),
    ],
)
def test_parse_inline_categories_rejects_malformed_input(value, fragment):
    with pytest.raises(ValueError, match=fragment):
        sp.parse_inline_categories(value)


# ---------------------------------------------------------------------------
# M4 — TOML holdings parsing
# ---------------------------------------------------------------------------


def test_unquoted_dotted_symbol_reports_how_to_fix_it(plugin_dir):
    write_toml(plugin_dir, "[holdings.main]\nVOD.L = 10\n")
    with pytest.raises(ValueError, match=r'"VOD.L" = 1'):
        resolve_config({})


@pytest.mark.parametrize(
    "body, fragment",
    [
        ('holdings = "AAPL"', "must be a table of portfolios"),
        ("[holdings.main]\nAAPL = nan\n", "must be a finite number"),
        ('[holdings.main]\nAAPL = "ten"\n', "invalid quantity"),
    ],
)
def test_invalid_toml_holdings_are_rejected(plugin_dir, body, fragment):
    write_toml(plugin_dir, body)
    with pytest.raises(ValueError, match=fragment):
        resolve_config({})


# ---------------------------------------------------------------------------
# M5 — market and mutual-fund gating
# ---------------------------------------------------------------------------

WARM_EQUITY = {"asset_type:AAPL": "equity", "last_price:AAPL": 90.0}
WARM_FUND = {"asset_type:VFIAX": "mutualfund", "last_price:VFIAX": 40.0}


def test_closed_market_skips_equities_when_closed_hour_writes_are_disabled(market):
    market["open"] = False
    local = run(
        args={"portfolio": "AAPL:2", "write_during_closed_hours": "false"},
        cache=WARM_EQUITY,
    )
    assert records(local, "stock_holdings") == []
    total = records(local, "portfolio_totals")[0]
    # value carried forward from the cached last price
    assert total.fields["value"] == 180.0
    assert total.fields["skipped_symbols"] == 1
    assert total.fields["carried_symbols"] == 1
    assert total.fields["missing_symbols"] == 0


def test_closed_market_still_fetches_when_closed_hour_writes_are_enabled(market):
    market["open"] = False
    local = run(args={"portfolio": "AAPL:2"}, cache=WARM_EQUITY)
    assert [r.tags["symbol"] for r in records(local, "stock_holdings")] == ["AAPL"]
    assert records(local, "portfolio_totals")[0].fields["skipped_symbols"] == 0


@pytest.mark.parametrize(
    "cache, now_ns, expected_skip",
    [
        ({**WARM_FUND, "last_mf_date:VFIAX": TODAY_LOCAL}, AFTER_NAV, "already-today"),
        (WARM_FUND, BEFORE_NAV, "too-early"),
    ],
)
def test_mutual_fund_is_fetched_once_a_day_after_the_check_time(
    cache, now_ns, expected_skip
):
    local = run(args={"portfolio": "VFIAX:3"}, cache=cache, now_ns=now_ns)
    assert records(local, "stock_holdings") == []
    assert records(local, "portfolio_totals")[0].fields["carried_symbols"] == 1
    assert expected_skip in local.infos[-1]


def test_mutual_fund_is_fetched_once_the_check_time_has_passed():
    local = run(
        args={"portfolio": "VFIAX:3"},
        cache={**WARM_FUND, "last_mf_date:VFIAX": "2023-11-14"},
        fetcher=quote_fetcher(asset_types={"VFIAX": "mutualfund"}),
    )
    assert [r.tags["symbol"] for r in records(local, "stock_holdings")] == ["VFIAX"]
    assert local.cache.get("last_mf_date:VFIAX") == TODAY_LOCAL


def test_a_symbol_that_would_be_skipped_is_fetched_while_its_price_is_uncached(market):
    market["open"] = False
    local = run(
        args={"portfolio": "AAPL:2", "write_during_closed_hours": "false"},
        cache={"asset_type:AAPL": "equity"},
    )
    assert [r.tags["symbol"] for r in records(local, "stock_holdings")] == ["AAPL"]
    assert "cold-cache bootstrap" in local.infos[-1]


def test_asset_type_and_last_price_are_cached_for_later_runs():
    local = run(args={"portfolio": "AAPL:2"}, fetcher=quote_fetcher(prices={"AAPL": 12.5}))
    assert local.cache.get("asset_type:AAPL") == "equity"
    assert local.cache.get("last_price:AAPL") == 12.5
    assert local.cache.get("last_mf_date:AAPL") is None


@pytest.mark.parametrize(
    "moment, expected",
    [
        ("2023-11-15T10:00:00-05:00", True),  # Wednesday, mid-session
        ("2023-11-15T20:00:00-05:00", False),  # Wednesday, after the close
        ("2023-11-11T10:00:00-05:00", False),  # Saturday
        ("2023-11-23T10:00:00-05:00", False),  # Thanksgiving
    ],
)
def test_real_nyse_calendar_decides_whether_the_session_is_open(moment, expected):
    from zoneinfo import ZoneInfo

    now_utc = datetime.fromisoformat(moment).astimezone(ZoneInfo("UTC"))
    tz = ZoneInfo("America/New_York")
    assert REAL_IS_MARKET_OPEN(now_utc, "NYSE", tz) is expected


# ---------------------------------------------------------------------------
# M6 — totals and category roll-ups
# ---------------------------------------------------------------------------


def test_totals_roll_up_per_portfolio_then_into_a_grand_total():
    local = run(
        args={
            "portfolio": "AAPL:2:401k|MSFT:1:401k|GOOG:1:brokerage",
            "categories": "401k:Retirement",
        },
        fetcher=quote_fetcher(prices={"AAPL": 10.0, "MSFT": 20.0, "GOOG": 30.0}),
    )
    totals = {r.tags["portfolio"]: r for r in records(local, "portfolio_totals")}
    assert totals["401k"].fields["value"] == 40.0
    assert totals["401k"].tags["category"] == "Retirement"
    assert totals["brokerage"].fields["value"] == 30.0
    assert "category" not in totals["brokerage"].tags
    assert totals["_total"].fields["value"] == 70.0
    assert totals["_total"].fields["symbol_count"] == 3
    assert "category" not in totals["_total"].tags


def test_category_totals_exclude_uncategorized_portfolios_and_the_grand_total():
    local = run(
        args={
            "portfolio": "AAPL:1:401k|MSFT:1:ira|GOOG:1:brokerage",
            "categories": "401k:Retirement|ira:Retirement",
        },
        fetcher=quote_fetcher(prices={"AAPL": 10.0, "MSFT": 20.0, "GOOG": 30.0}),
    )
    rows = records(local, "category_totals")
    assert [r.tags["category"] for r in rows] == ["Retirement"]
    assert rows[0].fields["value"] == 30.0
    assert rows[0].fields["portfolio_count"] == 2
    assert rows[0].fields["symbol_count"] == 2


# ---------------------------------------------------------------------------
# M7 — line protocol output
# ---------------------------------------------------------------------------


def test_a_run_emits_one_batched_write_with_a_single_timestamp():
    local = run(args={"portfolio": "AAPL:1:401k", "categories": "401k:Retirement",
                      "database": "portfolio"})
    assert len(local.writes) == 1
    database, lines = local.writes[0]
    assert database == "portfolio"
    assert [r.measurement for r in lines] == [
        "stock_holdings",
        "portfolio_totals",
        "portfolio_totals",
        "category_totals",
    ]
    assert {r.timestamp for r in lines} == {AFTER_NAV}


def test_holding_row_carries_the_quote_and_omits_unavailable_fields():
    local = run(args={"portfolio": "AAPL:2"}, fetcher=quote_fetcher(prices={"AAPL": 10.0}))
    row = records(local, "stock_holdings")[0]
    assert row.tags == {"symbol": "AAPL", "portfolio": "main", "asset_type": "equity"}
    assert row.fields == {
        "price": 10.0,
        "quantity": 2.0,
        "value": 20.0,
        "currency": "USD",
        "previous_close": 99.0,
        "day_high": 101.0,
    }


def test_counts_are_written_as_integer_fields():
    local = run(args={"portfolio": "AAPL:1"})
    total = records(local, "portfolio_totals")[0]
    for name in ("symbol_count", "missing_symbols", "skipped_symbols", "carried_symbols"):
        assert isinstance(total.fields[name], int)
    assert isinstance(total.fields["value"], float)


# ---------------------------------------------------------------------------
# M8 — failure handling
# ---------------------------------------------------------------------------


def test_a_fetch_failure_is_reported_and_counted_as_missing():
    local = run(
        args={"portfolio": "AAPL:1|BOOM:1"}, fetcher=quote_fetcher(failures={"BOOM"})
    )
    assert [r.tags["symbol"] for r in records(local, "stock_holdings")] == ["AAPL"]
    total = records(local, "portfolio_totals")[0]
    assert total.fields["missing_symbols"] == 1
    assert total.fields["carried_symbols"] == 0
    assert any("failed to fetch BOOM" in w for w in local.warns)
    assert "Failed: BOOM" in local.infos[-1]


def test_calendar_failure_aborts_only_when_closed_hour_writes_are_disabled(monkeypatch):
    def boom(*_):
        raise RuntimeError("calendar unavailable")

    monkeypatch.setattr(sp, "_is_market_open", boom)

    aborted = run(args={"portfolio": "AAPL:1", "write_during_closed_hours": "false"})
    assert aborted.writes == []
    assert any("skipping run" in e for e in aborted.errors)

    continued = run(args={"portfolio": "AAPL:1"})
    assert len(continued.writes) == 1
    assert any("continuing because" in w for w in continued.warns)


def test_a_configuration_error_is_logged_and_nothing_is_written():
    local = run(args={"portfolio": "AAPL:inf"})
    assert local.writes == []
    assert len(local.errors) == 1
    assert "configuration error" in local.errors[0]
    assert "must be a finite number" in local.errors[0]


def test_a_row_that_cannot_be_built_is_logged_instead_of_raising():
    # quantity and price are both finite, but their product overflows to inf
    local = run(
        args={"portfolio": "AAPL:1e200"}, fetcher=quote_fetcher(prices={"AAPL": 1e200})
    )
    assert local.writes == []
    assert any("failed to write" in e and "not finite" in e for e in local.errors)


def test_a_write_failure_is_logged_instead_of_raising():
    local = FakeLocal(write_error=RuntimeError("database gone"))
    run(args={"portfolio": "AAPL:1"}, local=local)
    assert local.writes == []
    assert any("failed to write" in e and "database gone" in e for e in local.errors)


def test_process_scheduled_call_uses_the_runtime_line_builder(monkeypatch):
    captured = {}

    def fake_main(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(sp, "_main", fake_main)
    sp.process_scheduled_call(FakeLocal(), "ignored call_time", {"portfolio": "AAPL:1"})

    assert captured["line_builder_cls"] is FakeLineBuilder
    assert captured["fetcher"] is sp.fetch_quote
    assert captured["args"] == {"portfolio": "AAPL:1"}
    assert captured["now_ns"] > 0
    assert len(captured["task_id"]) == 8


# ---------------------------------------------------------------------------
# M9 — quote fetching
# ---------------------------------------------------------------------------


class FakeFastInfo:
    """Stand-in for yfinance fast_info, which raises KeyError for keys it cannot fill."""

    def __init__(self, values):
        self._values = values

    def __getattr__(self, name):
        if name not in self._values:
            raise KeyError(name)
        return self._values[name]


@pytest.fixture
def fake_yfinance(monkeypatch):
    def install(values):
        module = type(sys)("yfinance")
        module.Ticker = lambda symbol: type(
            "Ticker", (), {"fast_info": FakeFastInfo(values)}
        )()
        monkeypatch.setitem(sys.modules, "yfinance", module)

    return install


def test_a_quote_survives_optional_fields_the_api_cannot_fill(fake_yfinance):
    fake_yfinance({"last_price": 10.0, "currency": "GBP", "quote_type": "ETF"})
    quote = sp.fetch_quote("VOD.L")
    assert (quote.price, quote.currency, quote.asset_type) == (10.0, "GBP", "etf")
    assert (quote.previous_close, quote.day_open, quote.day_high, quote.day_low) == (
        None,
        None,
        None,
        None,
    )


@pytest.mark.parametrize("last_price", [None, float("nan"), float("inf")])
def test_an_unusable_last_price_fails_the_fetch(fake_yfinance, last_price):
    fake_yfinance({"last_price": last_price})
    with pytest.raises(ValueError, match="no usable last_price"):
        sp.fetch_quote("AAPL")


def test_non_finite_optional_fields_are_dropped(fake_yfinance):
    fake_yfinance(
        {"last_price": 10.0, "previous_close": float("nan"), "day_high": 11.0}
    )
    quote = sp.fetch_quote("AAPL")
    assert quote.previous_close is None
    assert quote.day_high == 11.0
    # no currency or quote_type in the payload
    assert (quote.currency, quote.asset_type) == ("USD", "other")