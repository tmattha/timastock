import pathlib
import polars as pl
from .misc import AnyPolarsFrame
import typing as t

def load_ecb_csv(path: pathlib.Path | str) -> pl.DataFrame:
    input = pl.read_csv(path, ignore_errors=True)
    input = input.rename({c: c.lower() for c in input.columns})
    input = input.unpivot(on=pl.selectors.exclude("date"), index="date", variable_name="currency", value_name="exchangeRate")
    input = input.filter(pl.col("exchangeRate") != "N/A")
    # input = input.drop_nulls()
    input = input.with_columns(
        pl.col("date").str.strptime(pl.Date), pl.col("exchangeRate").cast(pl.Float32))
    input = input.sort("date")

    tautology = pl.date_range(
        input.get_column("date").min(),
        input.get_column("date").max(),
        interval="1d", eager=True
        ).alias("date")
    tautology = pl.DataFrame(tautology)
    tautology = tautology.with_columns(pl.lit("eur").alias("currency"), pl.lit(1.0, dtype=pl.Float32).alias("exchangeRate"))

    return input.extend(tautology)

@t.overload
def adjust_by_rates(data: pl.DataFrame, rates: pl.DataFrame, curr: str, columns: t.Iterable[str]) -> pl.DataFrame: ...
@t.overload
def adjust_by_rates(data: pl.LazyFrame, rates: pl.DataFrame, curr: str, columns: t.Iterable[str]) -> pl.LazyFrame: ...

def adjust_by_rates(data: AnyPolarsFrame, rates: pl.DataFrame, curr: str, columns: t.Iterable[str]) -> AnyPolarsFrame:
    rates = rates.lazy()
    data = data.sort("date")
    rates = rates.sort("date")
    data = data.with_columns(pl.col(curr).str.to_lowercase().alias("currLowercase"))
    data = data.join_asof(rates, on="date", by_left="currLowercase", by_right="currency", strategy="nearest")
    data = data.with_columns([pl.col(c) / pl.col("exchangeRate") for c in columns])

    return data.select(pl.exclude("currLowercase", "exchangeRate"))

@t.overload
def adjust_by_latest_rate(data: pl.DataFrame, rates: pl.DataFrame, curr: str, columns: t.Iterable[str]) -> pl.DataFrame: ...
@t.overload
def adjust_by_latest_rate(data: pl.LazyFrame, rates: pl.DataFrame, curr: str, columns: t.Iterable[str]) -> pl.LazyFrame: ...

def adjust_by_latest_rate(data: AnyPolarsFrame, rates: pl.DataFrame, curr: str, columns: t.Iterable[str]) -> AnyPolarsFrame:
    rates = rates.lazy()
    rates = rates.sort("date").group_by("currency").agg(pl.last("exchangeRate"))
    data = data.with_columns(pl.col(curr).str.to_lowercase().alias("currLowercase"))
    data = data.join(rates, left_on="currLowercase", right_on="currency")
    data = data.with_columns([pl.col(c) / pl.col("exchangeRate") for c in columns])

    return data.select(pl.exclude("currLowercase", "exchangeRate"))