import pandas as pd
import polars as pl

def series_common_index(series_a: pd.Series, series_b: pd.Series) -> tuple[pd.Series, pd.Series]:
    intersection = series_a.index.intersection(series_b.index)
    return series_a.loc[intersection], series_b.loc[intersection]

def pairwise_avg(frame: pl.DataFrame | pl.LazyFrame, target: str) -> pl.DataFrame | pl.LazyFrame:
    return frame.with_columns(pl.col(target).rolling_mean(2).shift(-1))

def yearpair_avg(frame: pl.DataFrame | pl.LazyFrame, index: str, target: str) -> pl.DataFrame | pl.LazyFrame:
    return frame.with_columns(pl.col(target).rolling_mean_by(index, "1y", closed="both", min_periods=2).shift(-1))
