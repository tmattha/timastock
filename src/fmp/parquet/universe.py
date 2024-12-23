from . import financials
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
    cashflow_statements = financials.multi_cashflow_statement(symbols)
    cashflow_statements.write_parquet(path / "cashflow_statements.parquet")
    del cashflow_statements
    key_metrics = financials.multi_key_metrics(symbols)
    key_metrics.write_parquet(path / "key_metrics.parquet")
    del key_metrics

@dataclass
class FmpUniverse:
    income_statements: pl.LazyFrame
    balance_sheets: pl.LazyFrame
    cashflow_statements: pl.LazyFrame
    key_metrics: pl.LazyFrame

def access_universe(path: pathlib.Path) -> FmpUniverse:
    universe = FmpUniverse(
        income_statements=pl.scan_parquet(path / "income_statements.parquet"),
        balance_sheets=pl.scan_parquet(path / "balance_sheets.parquet"),
        cashflow_statements=pl.scan_parquet(path / "cashflow_statements.parquet"),
        key_metrics=pl.scan_parquet(path / "key_metrics.parquet"),
    )

    return universe
