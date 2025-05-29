import pandas as pd
import polars as pl

AnyPolarsFrame = pl.DataFrame | pl.LazyFrame

def interquartile_range(column: str | pl.Expr):
    if not isinstance(column, pl.Expr):
        column = pl.col(column)
    return (column.quantile(0.75) - column.quantile(0.25))

def series_common_index(series_a: pd.Series, series_b: pd.Series) -> tuple[pd.Series, pd.Series]:
    intersection = series_a.index.intersection(series_b.index)
    return series_a.loc[intersection], series_b.loc[intersection]
