"""
{
    "plugin_type": ["scheduled"],
    "scheduled_args_config": [
        {
            "name": "database",
            "example": "stocks",
            "description": "Target database for writes. Overrides the TOML database key if both are set. Defaults to stocks.",
            "required": false
        },
        {
            "name": "portfolio",
            "example": "AAPL:10:401k|MSFT:5:401k|GOOG:2.5:brokerage",
            "description": "Inline holdings as pipe-separated SYMBOL:QUANTITY[:PORTFOLIO_NAME] entries. Required unless config_path points to a TOML file with holdings.",
            "required": false
        },
        {
            "name": "categories",
            "example": "401k:Retirement|brokerage:Investment",
            "description": "Inline category map as pipe-separated PORTFOLIO:CATEGORY entries.",
            "required": false
        },
        {
            "name": "config_path",
            "example": "stock_plugin.toml",
            "description": "Path to TOML configuration file. Supports absolute paths or relative paths resolved from INFLUXDB3_PLUGIN_DIR, PLUGIN_DIR, VIRTUAL_ENV parent, or the plugin directory.",
            "required": false
        }
    ]
}
"""

from __future__ import annotations

import os
import tomllib
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo


def _parse_hhmm(s: str) -> tuple[int, int]:
    """Parse a HH:MM time string. Raises ValueError on bad input."""
    parts = s.strip().split(":")
    if len(parts) != 2:
        raise ValueError(f"invalid HH:MM time {s!r}: expected HH:MM")
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError as e:
        raise ValueError(f"invalid HH:MM time {s!r}: non-numeric") from e
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"invalid HH:MM time {s!r}: out of range")
    return hour, minute


def _normalize_quote_type(qt: Optional[str]) -> str:
    """Map yfinance fast_info.quote_type to our normalized asset_type tag."""
    if not qt:
        return "other"
    qt = qt.upper()
    if qt == "EQUITY":
        return "equity"
    if qt == "ETF":
        return "etf"
    if qt == "MUTUALFUND":
        return "mutualfund"
    return "other"


def _is_market_open(
    now_utc: datetime, calendar_name: str, tz: ZoneInfo
) -> bool:
    """Return True if the given exchange's regular session is currently open.

    `calendar_name` is any name accepted by pandas_market_calendars
    (e.g. "NYSE", "LSE", "TSX", "JPX", "XETR"). `tz` is the local
    timezone of the exchange — used to determine "today" relative to
    the local market day, not UTC.

    Imported lazily so the module remains AST-parseable without the
    dependency.
    """
    import pandas_market_calendars as mcal
    cal = mcal.get_calendar(calendar_name)
    today_local = now_utc.astimezone(tz).date()
    schedule = cal.schedule(start_date=today_local, end_date=today_local)
    if schedule.empty:
        return False
    market_open = schedule.iloc[0]["market_open"].to_pydatetime()
    market_close = schedule.iloc[0]["market_close"].to_pydatetime()
    return market_open <= now_utc <= market_close


@dataclass
class Holding:
    """A configured holding: symbol + quantity + portfolio name."""
    symbol: str
    quantity: float
    portfolio: str


@dataclass
class Quote:
    """A successful price fetch result for a single symbol."""
    symbol: str
    price: float
    currency: str
    asset_type: str
    previous_close: Optional[float]
    day_open: Optional[float]
    day_high: Optional[float]
    day_low: Optional[float]


@dataclass
class HoldingRow:
    """A row of the stock_holdings measurement, ready to write."""
    symbol: str
    portfolio: str
    asset_type: str
    category: str  # empty string = no category configured
    price: float
    quantity: float
    value: float
    currency: str
    previous_close: Optional[float]
    day_open: Optional[float]
    day_high: Optional[float]
    day_low: Optional[float]
    timestamp_ns: int


@dataclass
class TotalRow:
    """A row of the portfolio_totals measurement.

    value = sum of (fresh fetches + carry-forward from last known price)
    symbol_count = configured holdings
    missing_symbols = fetch failures (no value available)
    skipped_symbols = intentionally not fetched (mutual-fund off-time or
                      market closed); includes carried symbols
    carried_symbols = subset of skipped that contributed a cached
                      last-known price to `value`
    category = user-defined portfolio category (empty for `_total`)
    """
    portfolio: str
    category: str  # empty string = no category configured
    value: float
    symbol_count: int
    missing_symbols: int
    skipped_symbols: int
    carried_symbols: int
    timestamp_ns: int


@dataclass
class CategoryRow:
    """A row of the category_totals measurement (rolled up across portfolios)."""
    category: str
    value: float
    symbol_count: int
    portfolio_count: int
    missing_symbols: int
    skipped_symbols: int
    carried_symbols: int
    timestamp_ns: int


@dataclass
class ResolvedConfig:
    """Final, validated configuration after merging trigger args and TOML."""
    database: str
    holdings_by_portfolio: dict[str, list[Holding]]
    categories: dict[str, str]  # portfolio name -> category name
    write_during_closed_hours: bool = True
    mutual_fund_check_time: str = "18:00"
    market_calendar: str = "NYSE"
    market_timezone: str = "America/New_York"


def parse_inline_portfolio(value: str) -> dict[str, list[Holding]]:
    """Parse the inline `portfolio=` trigger argument.

    Format: pipe-separated holdings. Each holding is `SYMBOL:QUANTITY` or
    `SYMBOL:QUANTITY:PORTFOLIO_NAME`. Defaults to portfolio "main" if the
    portfolio name is omitted.

    Examples:
        "AAPL:10"                            -> {"main": [Holding(AAPL, 10, main)]}
        "AAPL:10|MSFT:5"                     -> two holdings, both in "main"
        "AAPL:10:401k|GOOG:2.5:brokerage"    -> across two portfolios

    Raises ValueError on any malformed input.
    """
    if not value or not value.strip():
        raise ValueError("portfolio argument is empty")
    result: dict[str, list[Holding]] = {}
    for raw in value.split("|"):
        raw = raw.strip()
        if not raw:
            raise ValueError(
                f"invalid portfolio argument {value!r}: empty token "
                f"(check for leading, trailing, or doubled '|')"
            )
        parts = raw.split(":")
        if len(parts) not in (2, 3):
            raise ValueError(
                f"invalid holding spec {raw!r}: expected SYMBOL:QUANTITY[:PORTFOLIO]"
            )
        symbol = parts[0].strip().upper()
        if not symbol:
            raise ValueError(f"invalid holding spec {raw!r}: empty symbol")
        try:
            quantity = float(parts[1])
        except ValueError as e:
            raise ValueError(
                f"invalid quantity {parts[1]!r} in {raw!r}"
            ) from e
        if len(parts) == 3 and parts[2].strip():
            portfolio = parts[2].strip()
        else:
            portfolio = "main"
        result.setdefault(portfolio, []).append(
            Holding(symbol=symbol, quantity=quantity, portfolio=portfolio)
        )
    return result


def parse_inline_categories(value: str) -> dict[str, str]:
    """Parse the inline `categories=` trigger argument.

    Format: pipe-separated entries, each `PORTFOLIO_NAME:CATEGORY_NAME`.
    Example: "401k:Retirement|brokerage:Investment"

    Raises ValueError on any malformed input.
    """
    if not value or not value.strip():
        raise ValueError("categories argument is empty")
    result: dict[str, str] = {}
    for raw in value.split("|"):
        raw = raw.strip()
        if not raw:
            raise ValueError(
                f"invalid categories argument {value!r}: empty token "
                f"(check for leading, trailing, or doubled '|')"
            )
        parts = raw.split(":")
        if len(parts) != 2:
            raise ValueError(
                f"invalid category spec {raw!r}: expected PORTFOLIO:CATEGORY"
            )
        portfolio_name = parts[0].strip()
        category_name = parts[1].strip()
        if not portfolio_name or not category_name:
            raise ValueError(
                f"invalid category spec {raw!r}: empty portfolio or category name"
            )
        result[portfolio_name] = category_name
    return result


def _aggregate_duplicate_holdings(
    holdings_by_portfolio: dict[str, list[Holding]]
) -> dict[str, list[Holding]]:
    """Combine duplicate symbols within each portfolio into one holding."""
    result: dict[str, list[Holding]] = {}
    for portfolio, holdings in holdings_by_portfolio.items():
        order: list[str] = []
        by_symbol: dict[str, Holding] = {}
        for holding in holdings:
            existing = by_symbol.get(holding.symbol)
            if existing is None:
                order.append(holding.symbol)
                by_symbol[holding.symbol] = Holding(
                    symbol=holding.symbol,
                    quantity=holding.quantity,
                    portfolio=holding.portfolio,
                )
            else:
                by_symbol[holding.symbol] = Holding(
                    symbol=holding.symbol,
                    quantity=existing.quantity + holding.quantity,
                    portfolio=holding.portfolio,
                )
        result[portfolio] = [by_symbol[symbol] for symbol in order]
    return result


def _validate_market_calendar(calendar_name: str) -> None:
    """Fail fast if pandas_market_calendars does not know this calendar."""
    try:
        import pandas_market_calendars as mcal
    except ImportError as e:
        raise ValueError(
            "pandas_market_calendars is required to validate market_calendar; "
            "install it in the plugin environment"
        ) from e

    try:
        mcal.get_calendar(calendar_name)
    except Exception as e:
        raise ValueError(
            f"market_calendar {calendar_name!r} is not accepted by "
            f"pandas_market_calendars: {e}"
        ) from e


def load_toml_config(path: Path) -> tuple[dict, dict[str, list[Holding]]]:
    """Load TOML config from `path`.

    Returns (top_level_data_dict, holdings_by_portfolio). The full raw
    TOML top-level dict is returned so resolve_config can pick out
    optional scalar keys (database, market_calendar, etc).

    Expected TOML shape:
        database = "stocks"
        write_during_closed_hours = true
        mutual_fund_check_time = "18:00"

        [holdings.401k]
        AAPL = 10
        MSFT = 5

        [holdings.brokerage]
        GOOG = 2.5

    Raises:
        FileNotFoundError: path does not exist
        tomllib.TOMLDecodeError: file is not valid TOML
        ValueError: no usable [holdings.<portfolio>] sections
    """
    if not path.exists():
        raise FileNotFoundError(f"TOML config not found: {path}")
    with open(path, "rb") as f:
        data = tomllib.load(f)
    holdings_section = data.get("holdings", {})
    if not isinstance(holdings_section, dict) or not holdings_section:
        raise ValueError(
            f"TOML config at {path} has no [holdings.<portfolio>] sections"
        )
    result: dict[str, list[Holding]] = {}
    for portfolio_name, mapping in holdings_section.items():
        if not isinstance(mapping, dict):
            continue
        for symbol, quantity in mapping.items():
            if isinstance(quantity, dict):
                dotted_hint = symbol
                if quantity:
                    dotted_hint = f"{symbol}.{next(iter(quantity))}"
                raise ValueError(
                    f"invalid quantity {quantity!r} for symbol {symbol!r} "
                    f"in [holdings.{portfolio_name}]. If this is a dotted "
                    f"ticker symbol, quote it in TOML, for example "
                    f'"{dotted_hint}" = 1'
                )
            try:
                qty = float(quantity)
            except (TypeError, ValueError) as e:
                raise ValueError(
                    f"invalid quantity {quantity!r} for symbol {symbol!r} "
                    f"in [holdings.{portfolio_name}]"
                ) from e
            result.setdefault(portfolio_name, []).append(
                Holding(
                    symbol=symbol.upper(),
                    quantity=qty,
                    portfolio=portfolio_name,
                )
            )
    if not result:
        raise ValueError(
            f"TOML config at {path} has empty [holdings.<portfolio>] sections"
        )
    return data, result


def resolve_config_path(path: str, default_toml_path: Path) -> Path:
    """Resolve TOML config path using the plugin-dir fallbacks used by plugins.

    Absolute paths are used as-is. Relative paths are resolved from
    INFLUXDB3_PLUGIN_DIR or PLUGIN_DIR when available, then VIRTUAL_ENV's
    parent directory, and finally the supplied default TOML directory.
    """
    raw_path = Path(path)
    if raw_path.is_absolute():
        return raw_path

    candidates: list[Path] = []
    if influxdb3_plugin_dir := os.environ.get("INFLUXDB3_PLUGIN_DIR"):
        candidates.append(Path(influxdb3_plugin_dir))
    if plugin_dir := os.environ.get("PLUGIN_DIR"):
        candidates.append(Path(plugin_dir))
    if virtual_env := os.environ.get("VIRTUAL_ENV"):
        candidates.append(Path(virtual_env).parent)
    candidates.append(default_toml_path.parent)

    for base in candidates:
        candidate = base / raw_path
        if candidate.exists():
            return candidate
    return candidates[0] / raw_path


def resolve_config(
    args: dict[str, str], default_toml_path: Path
) -> ResolvedConfig:
    """Resolve final config from trigger args + TOML.

    Precedence:
      Holdings: inline `portfolio=` arg > TOML [holdings.*] (one or the other)
      Database: `database=` arg > TOML `database` > default "stocks"
      Config path: `config_path=` arg > default_toml_path. Relative paths
      resolve from INFLUXDB3_PLUGIN_DIR, PLUGIN_DIR, VIRTUAL_ENV's parent,
      or default_toml_path.parent.

    Other TOML scalars (write_during_closed_hours, mutual_fund_check_time,
    market_calendar, market_timezone) come only from TOML — there is no
    inline-arg override for them. Defaults apply when the key is absent.

    Raises ValueError if neither inline holdings nor a usable TOML file is
    found, or if any TOML scalar has an invalid value.
    """
    inline_portfolio = args.get("portfolio")
    config_path = resolve_config_path(
        args.get("config_path") or str(default_toml_path),
        default_toml_path,
    )

    toml_data: dict = {}
    if inline_portfolio:
        holdings = parse_inline_portfolio(inline_portfolio)
    else:
        if not config_path.exists():
            raise ValueError(
                f"No 'portfolio' trigger arg provided and no TOML config "
                f"at {config_path}. Either pass --trigger-arguments "
                f"'portfolio=...' or create the TOML file."
            )
        toml_data, holdings = load_toml_config(config_path)

    if "_total" in holdings:
        raise ValueError(
            "portfolio name '_total' is reserved for the grand total row; "
            "please use a different name"
        )
    holdings = _aggregate_duplicate_holdings(holdings)

    if args.get("database"):
        database = args["database"]
    elif toml_data.get("database"):
        database = toml_data["database"]
    else:
        database = "stocks"

    write_closed = toml_data.get("write_during_closed_hours", True)
    if not isinstance(write_closed, bool):
        raise ValueError(
            f"write_during_closed_hours must be true or false; got {write_closed!r}"
        )

    mf_check = toml_data.get("mutual_fund_check_time", "18:00")
    if not isinstance(mf_check, str):
        raise ValueError(
            f"mutual_fund_check_time must be a HH:MM string; got {mf_check!r}"
        )
    _parse_hhmm(mf_check)  # validate format, raises ValueError on bad input

    market_calendar = toml_data.get("market_calendar", "NYSE")
    if not isinstance(market_calendar, str) or not market_calendar.strip():
        raise ValueError(
            f"market_calendar must be a non-empty exchange name string; got {market_calendar!r}"
        )
    market_calendar = market_calendar.strip()
    _validate_market_calendar(market_calendar)

    market_timezone = toml_data.get("market_timezone", "America/New_York")
    if not isinstance(market_timezone, str) or not market_timezone.strip():
        raise ValueError(
            f"market_timezone must be a non-empty IANA timezone string; got {market_timezone!r}"
        )
    try:
        ZoneInfo(market_timezone)
    except Exception as e:
        raise ValueError(
            f"market_timezone {market_timezone!r} is not a valid IANA timezone: {e}"
        ) from e

    # Resolve categories: trigger arg > TOML > {}
    inline_categories = args.get("categories")
    if inline_categories:
        categories = parse_inline_categories(inline_categories)
    else:
        toml_cats = toml_data.get("portfolio_categories", {})
        if not isinstance(toml_cats, dict):
            raise ValueError(
                f"[portfolio_categories] must be a TOML table; got {type(toml_cats).__name__}"
            )
        categories = {str(k): str(v) for k, v in toml_cats.items()}

    return ResolvedConfig(
        database=database,
        holdings_by_portfolio=holdings,
        categories=categories,
        write_during_closed_hours=write_closed,
        mutual_fund_check_time=mf_check,
        market_calendar=market_calendar,
        market_timezone=market_timezone,
    )


def compute_totals(
    holdings_by_portfolio: dict[str, list[Holding]],
    categories: dict[str, str],
    fresh_rows: list[HoldingRow],
    carried_rows: list[HoldingRow],
    skipped_by_portfolio: dict[str, int],
    timestamp_ns: int,
) -> list[TotalRow]:
    """Aggregate per-portfolio totals + a grand `_total` row.

    `fresh_rows` are rows from a successful fetch this tick. They are also
    written to stock_holdings (caller's responsibility).
    `carried_rows` are synthetic rows for skipped symbols whose last
    known price was carried forward from cache. They contribute to
    `value` but are NOT written to stock_holdings.
    `skipped_by_portfolio` is the total intentional-skip count per
    portfolio (includes carried).
    `categories` maps portfolio name -> category string (empty if not set).
    """
    fresh_by_portfolio: dict[str, list[HoldingRow]] = {}
    for r in fresh_rows:
        fresh_by_portfolio.setdefault(r.portfolio, []).append(r)
    carried_by_portfolio: dict[str, list[HoldingRow]] = {}
    for r in carried_rows:
        carried_by_portfolio.setdefault(r.portfolio, []).append(r)

    totals: list[TotalRow] = []
    grand_value = 0.0
    grand_configured = 0
    grand_fetched = 0
    grand_skipped = 0
    grand_carried = 0
    for portfolio, holdings in holdings_by_portfolio.items():
        fresh = fresh_by_portfolio.get(portfolio, [])
        carried = carried_by_portfolio.get(portfolio, [])
        skipped = skipped_by_portfolio.get(portfolio, 0)
        value = float(sum(r.value for r in fresh) + sum(r.value for r in carried))
        configured = len(holdings)
        missing = configured - len(fresh) - skipped
        totals.append(
            TotalRow(
                portfolio=portfolio,
                category=categories.get(portfolio, ""),
                value=value,
                symbol_count=configured,
                missing_symbols=missing,
                skipped_symbols=skipped,
                carried_symbols=len(carried),
                timestamp_ns=timestamp_ns,
            )
        )
        grand_value += value
        grand_configured += configured
        grand_fetched += len(fresh)
        grand_skipped += skipped
        grand_carried += len(carried)

    totals.append(
        TotalRow(
            portfolio="_total",
            category="",
            value=grand_value,
            symbol_count=grand_configured,
            missing_symbols=grand_configured - grand_fetched - grand_skipped,
            skipped_symbols=grand_skipped,
            carried_symbols=grand_carried,
            timestamp_ns=timestamp_ns,
        )
    )
    return totals


def compute_category_totals(
    portfolio_totals: list[TotalRow],
    timestamp_ns: int,
) -> list[CategoryRow]:
    """Roll up per-portfolio TotalRows into per-category sums.

    Skips:
      * the `_total` grand-total row (avoids double-counting)
      * portfolios with no category configured (empty category)
    """
    by_category: dict[str, list[TotalRow]] = {}
    for r in portfolio_totals:
        if r.portfolio == "_total":
            continue
        if not r.category:
            continue
        by_category.setdefault(r.category, []).append(r)

    rows: list[CategoryRow] = []
    for category, ports in by_category.items():
        rows.append(
            CategoryRow(
                category=category,
                value=float(sum(r.value for r in ports)),
                symbol_count=sum(r.symbol_count for r in ports),
                portfolio_count=len(ports),
                missing_symbols=sum(r.missing_symbols for r in ports),
                skipped_symbols=sum(r.skipped_symbols for r in ports),
                carried_symbols=sum(r.carried_symbols for r in ports),
                timestamp_ns=timestamp_ns,
            )
        )
    return rows


def fetch_quote(symbol: str) -> Quote:
    """Fetch current price + day OHL + previous close + currency for `symbol`.

    Uses yfinance.Ticker(symbol).fast_info — a single HTTP call that returns
    all the fields the plugin needs. Optional fields default to None when
    yfinance returns None for them; the caller decides how to handle missing
    fields when building line protocol.

    Raises ValueError if no last_price is returned. Raises whatever
    yfinance raises on network/other failures (e.g. requests exceptions).

    yfinance is imported lazily so this module is importable / AST-parseable
    in environments where yfinance is not installed.
    """
    import yfinance as yf
    ticker = yf.Ticker(symbol)
    fi = ticker.fast_info
    if fi.last_price is None:
        raise ValueError(f"no last_price returned for {symbol}")

    def _opt(attr: str) -> Optional[float]:
        v = getattr(fi, attr, None)
        return float(v) if v is not None else None

    try:
        raw_currency = fi.currency
    except (KeyError, AttributeError):
        raw_currency = None

    try:
        raw_quote_type = fi.quote_type
    except (KeyError, AttributeError):
        raw_quote_type = None

    return Quote(
        symbol=symbol,
        price=float(fi.last_price),
        currency=str(raw_currency) if raw_currency else "USD",
        asset_type=_normalize_quote_type(raw_quote_type),
        previous_close=_opt("previous_close"),
        day_open=_opt("open"),
        day_high=_opt("day_high"),
        day_low=_opt("day_low"),
    )


def _main(
    local,
    args: dict[str, str],
    fetcher,
    line_builder_cls,
    plugin_dir: Path,
    now_ns: int,
    task_id: str,
) -> None:
    """The core plugin flow. process_scheduled_call delegates here.

    Gating rules applied per symbol:
      * mutual funds: fetch at most once per configured market calendar day, at the
        first tick at or after mutual_fund_check_time. Bootstrap exception:
        if no asset_type has been cached yet, fetch so we can learn the
        type and record an initial row.
      * stocks/etfs/other: fetch every tick UNLESS the configured market
        is closed AND write_during_closed_hours is False.
      * cold-cache bootstrap: if a symbol would be skipped but has no
        last_price cached, fetch once so later skipped ticks can carry it.

    Per-symbol asset_type is auto-detected via yfinance.fast_info.quote_type
    on first fetch and cached (no TTL) so we don't re-query for type.
    """
    default_toml = plugin_dir / "stock_plugin.toml"
    try:
        config = resolve_config(args, default_toml)
    except (ValueError, OSError, tomllib.TOMLDecodeError) as e:
        local.error(f"[{task_id}] stock_plugin: configuration error: {e}")
        return

    market_tz = ZoneInfo(config.market_timezone)
    now_utc = datetime.fromtimestamp(now_ns / 1_000_000_000, tz=timezone.utc)
    now_local = now_utc.astimezone(market_tz)
    today_local = now_local.date().isoformat()
    try:
        market_open_now = _is_market_open(
            now_utc, config.market_calendar, market_tz
        )
    except Exception as e:
        if not config.write_during_closed_hours:
            local.error(
                f"[{task_id}] stock_plugin: market-calendar lookup failed for "
                f"{config.market_calendar!r} ({e}); skipping run to avoid "
                f"fetching during possible closed hours"
            )
            return
        local.warn(
            f"[{task_id}] stock_plugin: market-calendar lookup failed for "
            f"{config.market_calendar!r} ({e}); continuing because "
            f"write_during_closed_hours=true"
        )
        market_open_now = True
    mf_check_h, mf_check_m = _parse_hhmm(config.mutual_fund_check_time)
    mf_check_passed = (now_local.hour, now_local.minute) >= (mf_check_h, mf_check_m)

    total_symbols = sum(len(h) for h in config.holdings_by_portfolio.values())
    local.info(
        f"[{task_id}] stock_plugin: starting run with {total_symbols} symbol(s) "
        f"across {len(config.holdings_by_portfolio)} portfolio(s) "
        f"→ database={config.database}, "
        f"calendar={config.market_calendar}, market_open={market_open_now}, "
        f"mf_check_passed={mf_check_passed} (now_local={now_local.isoformat()})"
    )

    successful: list[HoldingRow] = []
    failures: list[tuple[str, str]] = []
    skipped_holdings: list[tuple[Holding, str, str]] = []  # (holding, portfolio, reason)
    skipped_by_portfolio: dict[str, int] = {}
    cold_bootstrap_fetches: list[str] = []
    skip_reasons: dict[str, list[str]] = {
        "market_closed": [],
        "mf_already_today": [],
        "mf_too_early": [],
    }

    def _record_skip(holding: Holding, portfolio: str, reason: str) -> None:
        skipped_holdings.append((holding, portfolio, reason))
        skipped_by_portfolio[portfolio] = (
            skipped_by_portfolio.get(portfolio, 0) + 1
        )
        skip_reasons[reason].append(holding.symbol)

    for portfolio, holdings in config.holdings_by_portfolio.items():
        for holding in holdings:
            cached_type = local.cache.get(f"asset_type:{holding.symbol}")
            skip_reason: Optional[str] = None

            if cached_type == "mutualfund":
                last_mf_date = local.cache.get(f"last_mf_date:{holding.symbol}")
                if last_mf_date == today_local:
                    skip_reason = "mf_already_today"
                elif not mf_check_passed:
                    skip_reason = "mf_too_early"
            elif cached_type in ("equity", "etf", "other"):
                if not market_open_now and not config.write_during_closed_hours:
                    skip_reason = "market_closed"

            if (
                cached_type is None
                and not market_open_now
                and not config.write_during_closed_hours
            ):
                cold_bootstrap_fetches.append(
                    f"{holding.symbol}:asset_type_unknown"
                )

            # Bootstrap: if we would skip but have no cached last_price,
            # force a fetch this tick so carry-forward will work next time.
            if (
                skip_reason is not None
                and local.cache.get(f"last_price:{holding.symbol}") is None
            ):
                cold_bootstrap_fetches.append(f"{holding.symbol}:{skip_reason}")
                skip_reason = None

            if skip_reason is not None:
                _record_skip(holding, portfolio, skip_reason)
                continue

            try:
                quote = fetcher(holding.symbol)
            except Exception as e:
                local.warn(
                    f"[{task_id}] stock_plugin: failed to fetch {holding.symbol}: {e}"
                )
                failures.append((holding.symbol, str(e)))
                continue

            if cached_type != quote.asset_type:
                local.cache.put(f"asset_type:{holding.symbol}", quote.asset_type)
            if quote.asset_type == "mutualfund":
                local.cache.put(f"last_mf_date:{holding.symbol}", today_local)
            local.cache.put(f"last_price:{holding.symbol}", float(quote.price))

            successful.append(
                HoldingRow(
                    symbol=holding.symbol,
                    portfolio=portfolio,
                    asset_type=quote.asset_type,
                    category=config.categories.get(portfolio, ""),
                    price=quote.price,
                    quantity=holding.quantity,
                    value=quote.price * holding.quantity,
                    currency=quote.currency,
                    previous_close=quote.previous_close,
                    day_open=quote.day_open,
                    day_high=quote.day_high,
                    day_low=quote.day_low,
                    timestamp_ns=now_ns,
                )
            )

    carried: list[HoldingRow] = []
    for holding, portfolio, _reason in skipped_holdings:
        last_price = local.cache.get(f"last_price:{holding.symbol}")
        if last_price is None:
            continue
        last_price = float(last_price)
        cached_type = local.cache.get(f"asset_type:{holding.symbol}") or "other"
        carried.append(
            HoldingRow(
                symbol=holding.symbol,
                portfolio=portfolio,
                asset_type=cached_type,
                category=config.categories.get(portfolio, ""),
                price=last_price,
                quantity=holding.quantity,
                value=last_price * holding.quantity,
                currency="USD",
                previous_close=None,
                day_open=None,
                day_high=None,
                day_low=None,
                timestamp_ns=now_ns,
            )
        )

    totals = compute_totals(
        config.holdings_by_portfolio,
        config.categories,
        successful,
        carried,
        skipped_by_portfolio,
        now_ns,
    )
    category_totals = compute_category_totals(totals, now_ns)

    for r in successful:
        lb = line_builder_cls("stock_holdings")
        lb.tag("symbol", r.symbol)
        lb.tag("portfolio", r.portfolio)
        lb.tag("asset_type", r.asset_type)
        if r.category:
            lb.tag("category", r.category)
        lb.float64_field("price", r.price)
        lb.float64_field("quantity", r.quantity)
        lb.float64_field("value", r.value)
        lb.string_field("currency", r.currency)
        if r.previous_close is not None:
            lb.float64_field("previous_close", r.previous_close)
        if r.day_open is not None:
            lb.float64_field("day_open", r.day_open)
        if r.day_high is not None:
            lb.float64_field("day_high", r.day_high)
        if r.day_low is not None:
            lb.float64_field("day_low", r.day_low)
        lb.time_ns(r.timestamp_ns)
        local.write_to_db(config.database, lb)

    for t in totals:
        lb = line_builder_cls("portfolio_totals")
        lb.tag("portfolio", t.portfolio)
        if t.category:
            lb.tag("category", t.category)
        lb.float64_field("value", t.value)
        lb.int64_field("symbol_count", t.symbol_count)
        lb.int64_field("missing_symbols", t.missing_symbols)
        lb.int64_field("skipped_symbols", t.skipped_symbols)
        lb.int64_field("carried_symbols", t.carried_symbols)
        lb.time_ns(t.timestamp_ns)
        local.write_to_db(config.database, lb)

    for c in category_totals:
        lb = line_builder_cls("category_totals")
        lb.tag("category", c.category)
        lb.float64_field("value", c.value)
        lb.int64_field("symbol_count", c.symbol_count)
        lb.int64_field("portfolio_count", c.portfolio_count)
        lb.int64_field("missing_symbols", c.missing_symbols)
        lb.int64_field("skipped_symbols", c.skipped_symbols)
        lb.int64_field("carried_symbols", c.carried_symbols)
        lb.time_ns(c.timestamp_ns)
        local.write_to_db(config.database, lb)

    skipped_total = sum(skipped_by_portfolio.values())
    parts = [f"fetched {len(successful)}/{total_symbols} symbols"]
    if skipped_total:
        skip_bits = []
        if skip_reasons["market_closed"]:
            skip_bits.append(f"{len(skip_reasons['market_closed'])} market-closed")
        if skip_reasons["mf_already_today"]:
            skip_bits.append(
                f"{len(skip_reasons['mf_already_today'])} mutual-fund-already-today"
            )
        if skip_reasons["mf_too_early"]:
            skip_bits.append(
                f"{len(skip_reasons['mf_too_early'])} mutual-fund-too-early"
            )
        parts.append(f"skipped {skipped_total} ({', '.join(skip_bits)})")
    if carried:
        parts.append(f"carried last-known value for {len(carried)} skipped")
    if cold_bootstrap_fetches:
        parts.append(
            f"attempted {len(cold_bootstrap_fetches)} cold-cache bootstrap fetches"
        )
    parts.append(
        f"wrote {len(successful)} holding rows + {len(totals)} portfolio_total rows "
        f"({len(totals) - 1} portfolios + grand total) + "
        f"{len(category_totals)} category_total rows to {config.database}"
    )
    if failures:
        parts.append(
            "Failed: " + ", ".join(f"{s} ({e})" for s, e in failures)
        )
    local.info(f"[{task_id}] stock_plugin: " + ". ".join(parts))


def process_scheduled_call(influxdb3_local, call_time, args):
    """Scheduled plugin entry point invoked by InfluxDB 3.

    We stamp every row in this run with one consistent UTC timestamp so
    per-symbol holdings, portfolio_totals, and category_totals line up
    cleanly when joined in queries.

    We deliberately ignore the passed-in `call_time` for timestamping.
    Empirically, `call_time` can arrive as a naive datetime in server
    local time (not UTC) on some deployments, which would silently
    shift every row by the server's UTC offset. `datetime.now(timezone.utc)`
    is unambiguous regardless of host timezone; the few-seconds offset
    from the scheduled tick boundary is negligible for a 15-minute
    polling cadence.

    The InfluxDB runtime executes plugins via exec(), so __file__ is not
    defined. INFLUXDB3_PLUGIN_DIR is the env var the server itself uses
    to locate the plugin directory.
    """
    plugin_dir = Path(os.environ.get("INFLUXDB3_PLUGIN_DIR", "."))
    now_ns = int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)
    task_id = uuid.uuid4().hex[:8]
    _main(
        local=influxdb3_local,
        args=args or {},
        fetcher=fetch_quote,
        line_builder_cls=LineBuilder,  # runtime-injected global
        plugin_dir=plugin_dir,
        now_ns=now_ns,
        task_id=task_id,
    )
