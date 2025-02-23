from . import financials, pricing, company
import pathlib
from dataclasses import dataclass
import polars as pl
import typing as t
import timastock as tm

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
    company_profiles: pl.LazyFrame


def access_universe(path: pathlib.Path) -> FmpUniverse:
    universe = FmpUniverse(
        income_statements=pl.scan_parquet(path / "income_statements.parquet").select(financials.INCOME_STATEMENT_VALIDATED_FIELDS),
        balance_sheets=pl.scan_parquet(path / "balance_sheets.parquet").select(financials.BALANCE_SHEET_VALIDATED_FIELDS),
        cashflow_statements=pl.scan_parquet(path / "cashflow_statements.parquet").select(financials.CASHFLOW_STATEMENT_VALIDATED_FIELDS),
        key_metrics=pl.scan_parquet(path / "key_metrics.parquet").select(financials.KEY_METRICS_VALIDATED_FIELDS),
        prices=pl.scan_parquet(path / "prices.parquet").select(pricing.HISORICAL_PRICES_VALIDATED_FIELDS),
        company_profiles=pl.scan_parquet(path / "company_profiles.parquet").select(company.COMPANY_PROFILE_VALIDATED_FIELDS)
    )

    return universe

def split_universe(universe: FmpUniverse, date: pl.Date) -> tuple[FmpUniverse, FmpUniverse]:
    past = FmpUniverse(
        income_statements=universe.income_statements.filter(pl.col('date') <= date),
        balance_sheets=universe.balance_sheets.filter(pl.col('date') <= date),
        cashflow_statements=universe.cashflow_statements.filter(pl.col('date') <= date),
        key_metrics=universe.key_metrics.filter(pl.col('date') <= date),
        prices=universe.prices.filter(pl.col('date') <= date),
        company_profiles=universe.company_profiles
    )
    future = FmpUniverse(
        income_statements=universe.income_statements.filter(pl.col('date') > date),
        balance_sheets=universe.balance_sheets.filter(pl.col('date') > date),
        cashflow_statements=universe.cashflow_statements.filter(pl.col('date') > date),
        key_metrics=universe.key_metrics.filter(pl.col('date') > date),
        prices=universe.prices.filter(pl.col('date') > date),
        company_profiles=universe.company_profiles
    )
    return past, future

def sort_universe(universe: FmpUniverse) -> FmpUniverse:
    universe = FmpUniverse(
        income_statements=universe.income_statements.sort("date"),
        balance_sheets=universe.balance_sheets.sort("date"),
        cashflow_statements=universe.cashflow_statements.sort("date"),
        key_metrics=universe.key_metrics.sort("date"),
        prices=universe.prices.sort("date"),
        company_profiles=universe.company_profiles
    )
    return universe

def concat_universes(universes: t.Iterable[FmpUniverse]) -> FmpUniverse:
    universe = FmpUniverse(
        income_statements=pl.concat([u.income_statements for u in universes]),
        balance_sheets=pl.concat([u.balance_sheets for u in universes]),
        cashflow_statements=pl.concat([u.cashflow_statements for u in universes]),
        key_metrics=pl.concat([u.key_metrics for u in universes]),
        prices=pl.concat([u.prices for u in universes]),
        company_profiles=pl.concat([u.company_profiles for u in universes])
    )
    return universe

def adjust_universe_by_rates(universe: FmpUniverse, rates: pl.DataFrame, ) -> FmpUniverse:
    currencies = universe.company_profiles.select("symbol", "currency")

    universe = FmpUniverse(
        income_statements=tm.forex.adjust_by_rates(
            universe.income_statements, rates, curr="reportedCurrency",
            columns=financials.INCOME_STATEMENT_CURRENCY_FIELDS),
        balance_sheets=tm.forex.adjust_by_rates(
            universe.balance_sheets, rates, curr="reportedCurrency",
            columns=financials.BALANCE_SHEET_CURRENCY_FIELDS),
        cashflow_statements=tm.forex.adjust_by_rates(
            universe.cashflow_statements, rates, curr="reportedCurrency",
            columns=financials.CASHFLOW_STATEMENT_VALIDATED_FIELDS),
        key_metrics=tm.forex.adjust_by_rates(
            universe.key_metrics.join(currencies, on="symbol", how="left"), rates, curr="currency",
            columns=financials.KEY_METRICS_CURRENCY_FIELDS),
        prices=tm.forex.adjust_by_rates(
            universe.prices.join(currencies, on="symbol", how="left"), rates, curr="currency",
            columns=pricing.HISORICAL_PRICES_CURRENCY_FIELDS).select(pl.exclude("currency")),
        company_profiles=universe.company_profiles
    )

    return universe