from . import financials, pricing, company
import pathlib
from dataclasses import dataclass
import polars as pl


def store_universe(symbols: list, path: pathlib.Path) -> None:
    if not path.exists():
        path.mkdir(parents=True)

    income_statements = financials.multi_income_statements(symbols)
    income_statements.write_parquet(path / "income_statements.parquet")
    del income_statements
    balance_sheets = financials.multi_balance_sheets(symbols)
    balance_sheets.write_parquet(path / "balance_sheets.parquet")
    del balance_sheets
    cashflow_statements = financials.multi_cashflow_statements(symbols)
    cashflow_statements.write_parquet(path / "cashflow_statements.parquet")
    del cashflow_statements
    key_metrics = financials.multi_key_metrics(symbols)
    key_metrics.write_parquet(path / "key_metrics.parquet")
    del key_metrics
    prices = pricing.multi_historical_prices(symbols)
    prices.write_parquet(path / "prices.parquet")
    del prices
    market_caps = pricing.multi_market_caps(symbols)
    market_caps.write_parquet(path / "market_caps.parquet")
    del market_caps
    company_profiles = company.multi_company_profiles(symbols)
    company_profiles.write_parquet(path / "company_profiles.parquet")
    del company_profiles


@dataclass
class FmpUniverse:
    income_statements: pl.LazyFrame
    balance_sheets: pl.LazyFrame
    cashflow_statements: pl.LazyFrame
    key_metrics: pl.LazyFrame
    prices: pl.LazyFrame
    market_caps: pl.LazyFrame
    company_profiles: pl.LazyFrame


def access_universe(path: pathlib.Path) -> FmpUniverse:
    universe = FmpUniverse(
        income_statements=pl.scan_parquet(path / "income_statements.parquet"),
        balance_sheets=pl.scan_parquet(path / "balance_sheets.parquet"),
        cashflow_statements=pl.scan_parquet(path / "cashflow_statements.parquet"),
        key_metrics=pl.scan_parquet(path / "key_metrics.parquet"),
        prices=pl.scan_parquet(path / "prices.parquet"),
        market_caps=pl.scan_parquet(path / "market_caps.parquet"),
        company_profiles=pl.scan_parquet(path / "company_profiles.parquet")
    )

    return universe

def split_universe(universe: FmpUniverse, date: pl.Date) -> tuple[FmpUniverse, FmpUniverse]:
    past = FmpUniverse(
        income_statements=universe.income_statements.filter(pl.col('date') <= date),
        balance_sheets=universe.balance_sheets.filter(pl.col('date') <= date),
        cashflow_statements=universe.cashflow_statements.filter(pl.col('date') <= date),
        key_metrics=universe.key_metrics.filter(pl.col('date') <= date),
        prices=universe.prices.filter(pl.col('date') <= date),
        market_caps=universe.market_caps.filter(pl.col('date') <= date),
        company_profiles=universe.company_profiles
    )
    future = FmpUniverse(
        income_statements=universe.income_statements.filter(pl.col('date') > date),
        balance_sheets=universe.balance_sheets.filter(pl.col('date') > date),
        cashflow_statements=universe.cashflow_statements.filter(pl.col('date') > date),
        key_metrics=universe.key_metrics.filter(pl.col('date') > date),
        prices=universe.prices.filter(pl.col('date') > date),
        market_caps=universe.market_caps.filter(pl.col('date') > date),
        company_profiles=universe.company_profiles
    )
    return past, future 
