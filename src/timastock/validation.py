import matplotlib.pyplot as plt
import pandas as pd
import polars as pl
import scipy.stats as sps
import seaborn as sns
from collections import namedtuple
from .misc import AnyPolarsFrame

import typing as t

IndicatorTtestResult = namedtuple("IndicatorTtestResult", ["result", "median", "high_mean", "low_mean"])
IndicatorBandTtestResult = namedtuple("IndicatorTtestResult", ["result", "outside_mean", "inside_mean"])

def indicator_ttest(data: AnyPolarsFrame, indicator: str, target: str, split_on: t.Iterable[str] | str | None, alternative: str = "two-sided", with_plot: bool = True) -> IndicatorTtestResult:
    if isinstance(data, pl.LazyFrame):
        data = data.collect()
    median = data.select(pl.median(indicator)).item() # data[indicator].median()
    if split_on is None:
        low_indicator = data.filter(pl.col(indicator) < pl.median(indicator))
        high_indicator = data.filter(pl.col(indicator) > pl.median(indicator))
    else:
        low_indicator = data.filter(pl.col(indicator) < pl.median(indicator).over(split_on))
        high_indicator = data.filter(pl.col(indicator) > pl.median(indicator).over(split_on))
    low_mean = low_indicator.select(pl.mean(target)).item()
    high_mean = high_indicator.select(pl.mean(target)).item()
    if with_plot:
        sns.histplot(
            {
                f"High {indicator}: {high_mean:0.3f}": high_indicator.get_column(target),
                f"Low {indicator}: {low_mean:0.3f}": low_indicator.get_column(target)
            },
            kde=tuple, element="step", bins=30)
    result = sps.ttest_ind(high_indicator.get_column(target), low_indicator.get_column(target), equal_var=False, alternative=alternative)
    return IndicatorTtestResult(result, median, high_mean, low_mean)

def indicator_banded_ttest(data: AnyPolarsFrame, indicator: str, target: str, split_on: t.Iterable[str] | str | None, alternative: str = "two-sided", with_plot: bool = True, quantiles: float = 0.25) -> IndicatorBandTtestResult:
    if isinstance(data, pl.LazyFrame):
        data = data.collect()
    outside = data.filter(
        (pl.col(indicator) < pl.col(indicator).quantile(quantiles).over(split_on)) |
        (pl.col(indicator) > pl.col(indicator).quantile(1 - quantiles).over(split_on))
    )
    inside = data.filter(
        pl.col(indicator) > pl.col(indicator).quantile(quantiles).over(split_on),
        pl.col(indicator) < pl.col(indicator).quantile(1 - quantiles).over(split_on)
    )
    outside_mean = outside.select(pl.mean(target)).item()
    inside_mean = inside.select(pl.mean(target)).item()
    if with_plot:
        sns.histplot(
            {
                f"Outside Band {indicator}: {outside_mean:0.3f}": outside.get_column(target), 
                f"Inside Band {indicator}: {inside_mean:0.3f}": inside.get_column(target)
            },
            kde=tuple, element="step", bins=30)
    result = sps.ttest_ind(outside.get_column(target), inside.get_column(target), equal_var=False, alternative=alternative)
    return IndicatorBandTtestResult(result, outside_mean, inside_mean)

def quantile_elimination(data: AnyPolarsFrame, column: str, quantile: float):
    return data.filter(
        pl.col(column) > pl.col(column).quantile(quantile),
        pl.col(column) < pl.col(column).quantile(1 - quantile))


def indicator_overlap(data: pl.DataFrame, indicator1: str, indicator2: str):
    both_high = data.filter(
        pl.col(indicator1) > pl.median(indicator1),
        pl.col(indicator2) > pl.median(indicator2))
    return (2 * len(both_high)) / len(data)